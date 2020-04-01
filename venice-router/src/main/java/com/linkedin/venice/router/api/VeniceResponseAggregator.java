package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.base.misc.HeaderNames;
import com.linkedin.ddsstorage.base.misc.Metrics;
import com.linkedin.ddsstorage.base.misc.TimeValue;
import com.linkedin.ddsstorage.netty4.misc.BasicFullHttpRequest;
import com.linkedin.ddsstorage.router.api.ResponseAggregatorFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceStoreIsMigratedException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.router.streaming.SuccessfulStreamingResponse;
import com.linkedin.venice.router.streaming.VeniceChunkedResponse;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Utils;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.log4j.Logger;

import static com.linkedin.ddsstorage.router.api.MetricNames.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class VeniceResponseAggregator implements ResponseAggregatorFactory<BasicFullHttpRequest, FullHttpResponse> {
  private static final List<HttpResponseStatus> HEALTHY_STATUSES = Arrays.asList(OK, NOT_FOUND);

  private static final Logger LOGGER = Logger.getLogger(VeniceResponseAggregator.class);

  private final RouterStats<AggRouterHttpRequestStats> routerStats;

  private static final RecordSerializer<MultiGetResponseRecordV1> recordSerializer =
      SerializerDeserializerFactory.getAvroGenericSerializer(MultiGetResponseRecordV1.SCHEMA$);;
  private static final RecordDeserializer<MultiGetResponseRecordV1> recordDeserializer =
      SerializerDeserializerFactory.getAvroSpecificDeserializer(MultiGetResponseRecordV1.class);

  //timeout is configurable and should be overwritten elsewhere
  private long singleGetTardyThresholdInMs = TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);
  private long multiGetTardyThresholdInMs = TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);
  private long computeTardyThresholdInMs = TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);

  public VeniceResponseAggregator(RouterStats<AggRouterHttpRequestStats> routerStats) {
    this.routerStats = routerStats;
  }

  public VeniceResponseAggregator withSingleGetTardyThreshold(long timeout, TimeUnit unit){
    this.singleGetTardyThresholdInMs = unit.toMillis(timeout);
    return this;
  }

  public VeniceResponseAggregator withMultiGetTardyThreshold(long timeout, TimeUnit unit){
    this.multiGetTardyThresholdInMs = unit.toMillis(timeout);
    return this;
  }

  public VeniceResponseAggregator withComputeTardyThreshold(long timeout, TimeUnit unit) {
    this.computeTardyThresholdInMs = unit.toMillis(timeout);
    return this;
  }

  @Nonnull
  @Override
  public FullHttpResponse buildResponse(@Nonnull BasicFullHttpRequest request, Metrics metrics,
      @Nonnull List<FullHttpResponse> gatheredResponses) {
    if (gatheredResponses.isEmpty()) {
      throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.empty(), Optional.empty(),
          BAD_GATEWAY, "Received empty response!");
    }
    if (null == metrics) {
      throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.empty(), Optional.empty(),
          INTERNAL_SERVER_ERROR, "'metrics' should not be null");
    }
    VenicePath venicePath = metrics.getPath();
    if (null == venicePath) {
      /**
       * This is necessary since the exception could be thrown when parsing request path.
       * If it happens, here will just return the response, which contains exception stacktrace.
       */
      FullHttpResponse response = gatheredResponses.get(0);
      try {
        if (response.status().equals(MOVED_PERMANENTLY)) {
          String errorMsg = response.headers().get(HeaderNames.X_ERROR_MESSAGE);
          String d2Service = VeniceStoreIsMigratedException.getD2ServiceName(errorMsg);
          if (!Utils.isNullOrEmpty(d2Service)) {
            URI uri = new URI(request.uri());
            uri = new URI("d2", d2Service, uri.getPath(), uri.getQuery(), uri.getFragment());
            String redirectUri = uri.toString();
            LOGGER.info("redirect the request to " + redirectUri);
            response.setStatus(MOVED_PERMANENTLY);
            response.headers().set(HttpHeaderNames.LOCATION, redirectUri);
          } else {
            LOGGER.error("D2 service name is not available for request redirection");
          }
        }
      } catch (URISyntaxException e) {
        throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.empty(), Optional.empty(),
            BAD_REQUEST, "Failed to parse uri");
      }
      return response;
    }

    // TODO: Need to investigate if any of the early terminations above could cause the in-flight request sensor to "leak"

    RequestType requestType = venicePath.getRequestType();
    AggRouterHttpRequestStats stats = routerStats.getStatsByType(requestType);
    String storeName = venicePath.getStoreName();

    VeniceResponseDecompressor responseDecompressor = venicePath.getResponseDecompressor();
    FullHttpResponse finalResponse = null;
    if (venicePath.isStreamingRequest()) {
      /**
       * All the request with type: {@link RequestType.MULTI_GET_STREAMING} and {@link RequestType.COMPUTE_STREAMING}
       * will be handled here.
       */
      finalResponse = buildStreamingResponse(gatheredResponses, responseDecompressor);
    } else {
      switch (requestType) {
        case SINGLE_GET:
          finalResponse = responseDecompressor.processSingleGetResponse(gatheredResponses.get(0));
          break;
        case MULTI_GET:
          finalResponse = responseDecompressor.processMultiGetResponses(gatheredResponses);
          break;
        case COMPUTE:
          finalResponse = responseDecompressor.processComputeResponses(gatheredResponses);
          break;
        default:
          throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.empty(), Optional.empty(),
              INTERNAL_SERVER_ERROR, "Unknown request type: " + requestType);
      }
    }
    stats.recordFanoutRequestCount(storeName, gatheredResponses.size());

    HttpResponseStatus responseStatus = finalResponse.status();
    Map<String, TimeValue> allMetrics = metrics.getMetrics();
    /**
     * All the metrics in {@link com.linkedin.ddsstorage.router.api.MetricNames} are supported in {@link Metrics}.
     * We are not exposing the following metrics inside Venice right now.
     * 1. {@link ROUTER_PARSE_URI}
     * 2. {@link ROUTER_ROUTING_TIME}
     */
    if (allMetrics.containsKey(ROUTER_SERVER_TIME.name())) {
      // TODO: When a batch get throws a quota exception, the ROUTER_SERVER_TIME is missing, so we can't record anything here...
      double latency = LatencyUtils.convertLatencyFromNSToMS(allMetrics.get(ROUTER_SERVER_TIME.name()).getRawValue(TimeUnit.NANOSECONDS));
      stats.recordLatency(storeName, latency);
      if (HEALTHY_STATUSES.contains(responseStatus)) {
        routerStats.getStatsByType(RequestType.SINGLE_GET).recordReadQuotaUsage(storeName, venicePath.getPartitionKeys().size());
        if (isFastRequest(latency, requestType)) {
          stats.recordHealthyRequest(storeName, latency);
        } else {
          stats.recordTardyRequest(storeName, latency);
        }
      } else if (responseStatus.equals(TOO_MANY_REQUESTS)) {
        LOGGER.debug("request is rejected by storage node because quota is exceeded");
        stats.recordThrottledRequest(storeName, latency);
      } else {
        LOGGER.debug("Unhealthy request detected, latency: " + latency + "ms, response status: " + responseStatus);
        stats.recordUnhealthyRequest(storeName, latency);
      }
    }
    if (allMetrics.containsKey(ROUTER_RESPONSE_WAIT_TIME.name())) {
      double waitingTime = LatencyUtils.convertLatencyFromNSToMS(allMetrics.get(ROUTER_RESPONSE_WAIT_TIME.name()).getRawValue(TimeUnit.NANOSECONDS));
      stats.recordResponseWaitingTime(storeName, waitingTime);
    }
    if (allMetrics.containsKey(ROUTER_PARSE_URI.name())) {
      double parsingTime = LatencyUtils.convertLatencyFromNSToMS(allMetrics.get(ROUTER_PARSE_URI.name()).getRawValue(TimeUnit.NANOSECONDS));
      stats.recordRequestParsingLatency(storeName, parsingTime);
    }
    if (allMetrics.containsKey(ROUTER_ROUTING_TIME.name())) {
      double routingTime = LatencyUtils.convertLatencyFromNSToMS(allMetrics.get(ROUTER_ROUTING_TIME.name()).getRawValue(TimeUnit.NANOSECONDS));
      stats.recordRequestRoutingLatency(storeName, routingTime);
    }
    if (HEALTHY_STATUSES.contains(responseStatus) && !venicePath.isStreamingRequest()) {
      // Only record successful response
      stats.recordResponseSize(storeName, finalResponse.content().readableBytes());
    }
    stats.recordResponse(storeName);

    return finalResponse;
  }

  private FullHttpResponse buildStreamingResponse(List<FullHttpResponse> gatheredResponses, VeniceResponseDecompressor responseDecompressor) {
    // Validate the consistency of compression strategy across all the gathered responses
    responseDecompressor.validateAndExtractCompressionStrategy(gatheredResponses);
    /**
     * If every sub-response is good, here will return {@link VeniceChunkedResponse.DummyFullHttpResponse} to
     * indicate that, otherwise, here will return the first error response.
     */
    for (FullHttpResponse subResponse : gatheredResponses) {
      if (! subResponse.status().equals(OK)) {
        return subResponse;
      }
    }
    return new SuccessfulStreamingResponse();
  }

  private boolean isFastRequest(double requestLatencyMs, RequestType requestType){
    switch (requestType) {
      case SINGLE_GET:
        return requestLatencyMs < singleGetTardyThresholdInMs;
      case MULTI_GET_STREAMING:
      case MULTI_GET:
        return requestLatencyMs < multiGetTardyThresholdInMs;
      case COMPUTE_STREAMING:
      case COMPUTE:
        return requestLatencyMs < computeTardyThresholdInMs;
      default:
        throw new VeniceException("Unknown request type: " + requestType);
    }
  }
}
