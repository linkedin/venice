package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.Optional;

import static io.netty.handler.codec.http.HttpResponseStatus.*;


/**
 * Right now, there is no simple way to capture all the exception cases with DDS Router.
 * So we temporarily track all the error cases when throwing a exception.
 *
 * TODO: If later on DDS router could support a better way to register a handler to handle the exceptional cases,
 * we should update the logic here.
 */
@Deprecated
public class RouterExceptionAndTrackingUtils {
  private static AggRouterHttpRequestStats STATS_FOR_SINGLE_GET;
  private static AggRouterHttpRequestStats STATS_FOR_MULTI_GET;

  @Deprecated
  public static void setStatsForSingleGet(AggRouterHttpRequestStats stats) {
    STATS_FOR_SINGLE_GET = stats;
  }

  @Deprecated
  public static void setStatsForMultiGet(AggRouterHttpRequestStats stats) {
    STATS_FOR_MULTI_GET = stats;
  }

  @Deprecated
  public static RouterException newRouterExceptionAndTracking(Optional<String> storeName,
      Optional<RequestType> requestType, HttpResponseStatus responseStatus, String msg) {
    metricTracking(storeName, requestType, responseStatus);
    return new RouterException(HttpResponseStatus.class, responseStatus, responseStatus.code(), msg, true);
  }

  @Deprecated
  public static VeniceException newVeniceExceptionAndTracking(Optional<String> storeName,
      Optional<RequestType> requestType, HttpResponseStatus responseStatus, String msg) {
    metricTracking(storeName, requestType, responseStatus);
    return new VeniceException(msg);
  }

  private static void metricTracking(Optional<String> storeName, Optional<RequestType> requestType,
      HttpResponseStatus responseStatus) {
    AggRouterHttpRequestStats stats = STATS_FOR_SINGLE_GET;
    if (requestType.isPresent() && requestType.equals(RequestType.MULTI_GET)) {
      stats= STATS_FOR_MULTI_GET;
    }
    if (null == stats) {
      return;
    }
    // If we don't know the actual store name, this error will only be aggregated in server level, but not
    // in store level
    if (responseStatus.equals(BAD_REQUEST)) {
      if (storeName.isPresent()) {
        stats.recordBadRequest(storeName.get());
      } else {
        stats.recordBadRequest();
      }
    } else if (responseStatus.equals(TOO_MANY_REQUESTS)) {
      if (storeName.isPresent()) {
        stats.recordThrottledRequest(storeName.get());
      } else {
        // not possible to have empty store name in this scenario
        throw new VeniceException("Received a TOO_MANY_REQUESTS error without store name present");
      }
    } else {
      if (storeName.isPresent()) {
        stats.recordUnhealthyRequest(storeName.get());
      } else {
        stats.recordUnhealthyRequest();
      }
    }
  }

}
