package com.linkedin.venice.router.api;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;

import com.linkedin.alpini.router.api.RouterException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Right now, there is no simple way to capture all the exception cases with DDS Router.
 * So we temporarily track all the error cases when throwing a exception.
 *
 * TODO: If later on DDS router could support a better way to register a handler to handle the exceptional cases,
 * we should update the logic here.
 */

public class RouterExceptionAndTrackingUtils {
  public enum FailureType {
    REGULAR, SMART_RETRY_ABORTED_BY_SLOW_ROUTE, SMART_RETRY_ABORTED_BY_DELAY_CONSTRAINT,
    SMART_RETRY_ABORTED_BY_MAX_RETRY_ROUTE_LIMIT, RESOURCE_NOT_FOUND, RETRY_ABORTED_BY_NO_AVAILABLE_REPLICA
  }

  private static final StackTraceElement[] emptyStackTrace = new StackTraceElement[0];

  private static RouterStats<AggRouterHttpRequestStats> ROUTER_STATS;

  private static final Logger LOGGER = LogManager.getLogger(RouterExceptionAndTrackingUtils.class);

  private static final RedundantExceptionFilter EXCEPTION_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();

  public static void setRouterStats(RouterStats<AggRouterHttpRequestStats> routerStats) {
    ROUTER_STATS = routerStats;
  }

  public static RouterException newRouterExceptionAndTracking(
      Optional<String> storeName,
      Optional<RequestType> requestType,
      HttpResponseStatus responseStatus,
      String msg,
      FailureType failureType) {
    metricTracking(storeName, requestType, responseStatus, failureType);
    RouterException e =
        new RouterException(HttpResponseStatus.class, responseStatus, responseStatus.code(), msg, false);
    // Do not dump stack-trace for Quota exceed exception as it might blow up memory on high load
    if (responseStatus.equals(TOO_MANY_REQUESTS) || responseStatus.equals(SERVICE_UNAVAILABLE)
        || failureType == FailureType.RESOURCE_NOT_FOUND) {
      e.setStackTrace(emptyStackTrace);
    }
    String name = storeName.isPresent() ? storeName.get() : "";
    if (!EXCEPTION_FILTER.isRedundantException(name, String.valueOf(e.code()))) {
      if (responseStatus == BAD_REQUEST) {
        LOGGER.debug("{} for store: {}", BAD_REQUEST, name, e);
      } else if (failureType == FailureType.RESOURCE_NOT_FOUND) {
        LOGGER.error("Could not find resources for store: {} ", name, e);
      } else {
        LOGGER.warn("Got an exception for store: {} ", name, e);
      }
    }
    return e;
  }

  public static RouterException newRouterExceptionAndTracking(
      Optional<String> storeName,
      Optional<RequestType> requestType,
      HttpResponseStatus responseStatus,
      String msg) {
    return newRouterExceptionAndTracking(storeName, requestType, responseStatus, msg, FailureType.REGULAR);
  }

  public static RouterException newRouterExceptionAndTrackingResourceNotFound(
      Optional<String> storeName,
      Optional<RequestType> requestType,
      HttpResponseStatus responseStatus,
      String msg) {
    return newRouterExceptionAndTracking(storeName, requestType, responseStatus, msg, FailureType.RESOURCE_NOT_FOUND);
  }

  @Deprecated
  public static VeniceException newVeniceExceptionAndTracking(
      Optional<String> storeName,
      Optional<RequestType> requestType,
      HttpResponseStatus responseStatus,
      String msg,
      FailureType failureType) {
    metricTracking(storeName, requestType, responseStatus, failureType);
    String name = storeName.isPresent() ? storeName.get() : "";
    VeniceException e = new VeniceException(msg);

    // Do not dump stack-trace for Quota exceed exception as it might blow up memory on high load
    if (responseStatus.equals(TOO_MANY_REQUESTS) || responseStatus.equals(SERVICE_UNAVAILABLE)) {
      e.setStackTrace(emptyStackTrace);
    }
    if (!EXCEPTION_FILTER.isRedundantException(name, e)) {
      LOGGER.warn("Got an exception for store: {}", name, e);
    }
    return e;
  }

  @Deprecated
  public static VeniceException newVeniceExceptionAndTracking(
      Optional<String> storeName,
      Optional<RequestType> requestType,
      HttpResponseStatus responseStatus,
      String msg) {
    return newVeniceExceptionAndTracking(storeName, requestType, responseStatus, msg, FailureType.REGULAR);
  }

  public static void recordUnavailableReplicaStreamingRequest(String storeName, RequestType requestType) {
    AggRouterHttpRequestStats stats = ROUTER_STATS.getStatsByType(requestType);
    stats.recordUnavailableReplicaStreamingRequest(storeName);
  }

  private static void metricTracking(
      Optional<String> storeName,
      Optional<RequestType> requestType,
      HttpResponseStatus responseStatus,
      FailureType failureType) {
    if (ROUTER_STATS == null) {
      // defensive code
      throw new VeniceException("'ROUTER_STATS' hasn't been setup yet, so there must be some bug causing this.");
    }
    AggRouterHttpRequestStats stats =
        ROUTER_STATS.getStatsByType(requestType.isPresent() ? requestType.get() : RequestType.SINGLE_GET);
    // If we don't know the actual store name, this error will only be aggregated in server level, but not
    // in store level
    if (responseStatus.equals(BAD_REQUEST)) {
      stats.recordBadRequest(storeName.orElse(null));
    } else if (responseStatus.equals(TOO_MANY_REQUESTS)) {
      if (storeName.isPresent()) {
        if (requestType.isPresent()) {
          /**
           * Once we stop throwing quota exceptions from within the {@link VeniceDelegateMode} then we can
           * process everything through {@link VeniceResponseAggregator} and remove the metric tracking
           * from here.
           *
           * TODO: Remove this metric after the above work is done...
           */
          stats.recordThrottledRequest(storeName.get());
        }
      } else {
        // not possible to have empty store name in this scenario
        throw new VeniceException("Received a TOO_MANY_REQUESTS error without store name present");
      }
    } else {
      /**
       * It is on purpose that here doesn't record retry request abort as unhealthy request.
       */
      switch (failureType) {
        case SMART_RETRY_ABORTED_BY_SLOW_ROUTE:
          if (storeName.isPresent()) {
            stats.recordSlowRouteAbortedRetryRequest(storeName.get());
          }
          return;
        case SMART_RETRY_ABORTED_BY_DELAY_CONSTRAINT:
          if (storeName.isPresent()) {
            stats.recordDelayConstraintAbortedRetryRequest(storeName.get());
          }
          return;
        case SMART_RETRY_ABORTED_BY_MAX_RETRY_ROUTE_LIMIT:
          if (storeName.isPresent()) {
            stats.recordRetryRouteLimitAbortedRetryRequest(storeName.get());
          }
          return;
        case RETRY_ABORTED_BY_NO_AVAILABLE_REPLICA:
          if (storeName.isPresent()) {
            stats.recordNoAvailableReplicaAbortedRetryRequest(storeName.get());
          }
          return;
      }

      stats.recordUnhealthyRequest(storeName.orElse(null));

      if (responseStatus.equals(SERVICE_UNAVAILABLE)) {
        if (storeName.isPresent()) {
          stats.recordUnavailableRequest(storeName.get());
        } else {
          throw new VeniceException("Received a SERVICE_UNAVAILABLE error without store name present");
        }
      }
    }
  }
}
