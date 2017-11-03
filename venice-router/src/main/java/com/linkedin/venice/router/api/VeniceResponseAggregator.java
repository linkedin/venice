package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.base.misc.Metrics;
import com.linkedin.ddsstorage.base.misc.TimeValue;
import com.linkedin.ddsstorage.netty4.misc.BasicFullHttpRequest;
import com.linkedin.ddsstorage.router.api.ResponseAggregatorFactory;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Time;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.log4j.Logger;

import static com.linkedin.ddsstorage.router.api.MetricNames.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;


public class VeniceResponseAggregator implements ResponseAggregatorFactory<BasicFullHttpRequest, FullHttpResponse> {
  //TODO: timeout should be configurable and be defined by the HttpAysncClient
  private static final int TIMEOUT_THRESHOLD_IN_MS = 50 * Time.MS_PER_SECOND;

  private static final List<HttpResponseStatus> HEALTHY_STATUSES = Arrays.asList(OK, NOT_FOUND);

  private static final Logger LOGGER = Logger.getLogger(VeniceResponseAggregator.class);

  // Headers expected in each storage node multi-get response
  private static final Map<CharSequence, String> MULTI_GET_VALID_HEADER_MAP = new HashMap<>();
  static {
    MULTI_GET_VALID_HEADER_MAP.put(HttpHeaderNames.CONTENT_TYPE, HttpConstants.AVRO_BINARY);
    //TODO: If the response version for multi-get changes in the future, we need to update the following protocol version mapping.
    MULTI_GET_VALID_HEADER_MAP.put(HttpConstants.VENICE_SCHEMA_ID,
        Integer.toString(ReadAvroProtocolDefinition.MULTI_GET_RESPONSE_V1.getProtocolVersion()));
  }

  private final AggRouterHttpRequestStats statsForSingleGet;
  private final AggRouterHttpRequestStats statsForMultiGet;

  public VeniceResponseAggregator(AggRouterHttpRequestStats statsForSingleGet, AggRouterHttpRequestStats statsForMultiGet) {
    this.statsForSingleGet = statsForSingleGet;
    this.statsForMultiGet = statsForMultiGet;
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
    RequestType requestType = venicePath.getRequestType();
    String storeName = venicePath.getStoreName();
    FullHttpResponse finalResponse = null;
    AggRouterHttpRequestStats stats = null;
    switch (requestType) {
      case SINGLE_GET:
        finalResponse = gatheredResponses.get(0);
        stats = statsForSingleGet;
        break;
      case MULTI_GET:
        finalResponse = buildMultiGetResponse(storeName, gatheredResponses);
        stats = statsForMultiGet;
        break;
      default:
        throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.empty(), Optional.empty(),
            INTERNAL_SERVER_ERROR, "Unknown request type: " + requestType);
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
      double latency = LatencyUtils.convertLatencyFromNSToMS(allMetrics.get(ROUTER_SERVER_TIME.name()).getRawValue(TimeUnit.NANOSECONDS));
      stats.recordLatency(storeName, latency);
      if (latency <= TIMEOUT_THRESHOLD_IN_MS && HEALTHY_STATUSES.contains(responseStatus)) {
        stats.recordHealthyRequest(storeName);
      } else {
        LOGGER.error("Unhealthy request detected, latency: " + latency + "ms, response status: " + responseStatus);
        stats.recordUnhealthyRequest(storeName);
      }
    }
    if (allMetrics.containsKey(ROUTER_RESPONSE_WAIT_TIME.name())) {
      double waitingTime = LatencyUtils.convertLatencyFromNSToMS(allMetrics.get(ROUTER_RESPONSE_WAIT_TIME.name()).getRawValue(TimeUnit.NANOSECONDS));
      stats.recordResponseWaitingTime(storeName, waitingTime);
    }
    if (HEALTHY_STATUSES.contains(responseStatus)) {
      // Only record successful response
      stats.recordResponseSize(storeName, finalResponse.content().readableBytes());
    }

    return finalResponse;
  }

  private FullHttpResponse buildMultiGetResponse(String storeName, List<FullHttpResponse> responses) {
    int responseNum = responses.size();
    if (1 == responseNum) {
      return responses.get(0);
    }
    /**
     * Here we will check the consistency of the following headers among all the responses:
     * 1. {@link HttpHeaderNames.CONTENT_TYPE}
     * 2. {@link HttpConstants.VENICE_SCHEMA_ID}
     */
    List<byte[]> contentList = new ArrayList<>();
    int resultLen = 0;
    for (FullHttpResponse response : responses) {
      if (response.status() != OK) {
        // Return error response directly for now.
        return response;
      }
      MULTI_GET_VALID_HEADER_MAP.forEach((headerName, headerValue) -> {
        String currentValue = response.headers().get(headerName);
        if (null == currentValue) {
          throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.of(storeName), Optional.of(RequestType.MULTI_GET),
              BAD_GATEWAY, "Header: " + headerName + " is expected in multi-get sub-response");
        }
        if (!headerValue.equals(currentValue)) {
          throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.of(storeName), Optional.of(RequestType.MULTI_GET),
              BAD_GATEWAY, "Incompatible header received for " + headerName + ", values: " + headerValue + ", " +  currentValue);
        }
      });
      byte[] content = response.content().array();
      contentList.add(content);
      resultLen += content.length;
    }

    // Concat all the responses
    // TODO: explore how to reuse the buffer: Pooled??
    ByteBuf result = Unpooled.buffer(resultLen);
    contentList.stream().forEach(content -> result.writeBytes(content));

    FullHttpResponse multiGetResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK, result);
    MULTI_GET_VALID_HEADER_MAP.forEach((headerName, headerValue) -> {
      multiGetResponse.headers().add(headerName, headerValue);
    });
    multiGetResponse.headers().add(HttpHeaderNames.CONTENT_LENGTH, result.readableBytes());

    return multiGetResponse;
  }
}
