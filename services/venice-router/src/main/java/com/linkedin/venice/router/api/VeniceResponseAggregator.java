package com.linkedin.venice.router.api;

import static com.linkedin.alpini.base.misc.MetricNames.ROUTER_DISPATCH_LATENCY;
import static com.linkedin.alpini.base.misc.MetricNames.ROUTER_PARSE_URI;
import static com.linkedin.alpini.base.misc.MetricNames.ROUTER_PIPELINE_LATENCY;
import static com.linkedin.alpini.base.misc.MetricNames.ROUTER_QUEUE_LATENCY;
import static com.linkedin.alpini.base.misc.MetricNames.ROUTER_RESPONSE_WAIT_TIME;
import static com.linkedin.alpini.base.misc.MetricNames.ROUTER_ROUTING_TIME;
import static com.linkedin.alpini.base.misc.MetricNames.ROUTER_SCATTER_LATENCY;
import static com.linkedin.alpini.base.misc.MetricNames.ROUTER_SERVER_TIME;
import static com.linkedin.venice.HttpConstants.VENICE_CLIENT_COMPUTE;
import static com.linkedin.venice.HttpConstants.VENICE_COMPRESSION_STRATEGY;
import static com.linkedin.venice.HttpConstants.VENICE_REQUEST_RCU;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.MOVED_PERMANENTLY;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;

import com.linkedin.alpini.base.misc.HeaderNames;
import com.linkedin.alpini.base.misc.Metrics;
import com.linkedin.alpini.netty4.misc.BasicHttpRequest;
import com.linkedin.alpini.router.api.ResponseAggregatorFactory;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceStoreIsMigratedException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.api.routing.helix.HelixGroupSelector;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.router.streaming.SuccessfulStreamingResponse;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.utils.LatencyUtils;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * {@code VeniceResponseAggregator} aggregates the sub-responses returned by storage node for a multi-get request.
 */
public class VeniceResponseAggregator implements ResponseAggregatorFactory<BasicHttpRequest, FullHttpResponse> {
  private static final List<HttpResponseStatus> HEALTHY_STATUSES = Arrays.asList(OK, NOT_FOUND);

  private static final Logger LOGGER = LogManager.getLogger(VeniceResponseAggregator.class);

  private final RouterStats<AggRouterHttpRequestStats> routerStats;
  private final Optional<MetaStoreShadowReader> metaStoreShadowReaderOptional;

  private HelixGroupSelector helixGroupSelector;

  // timeout is configurable and should be overwritten elsewhere
  private long singleGetTardyThresholdInMs = TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);
  private long multiGetTardyThresholdInMs = TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);
  private long computeTardyThresholdInMs = TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);

  // Headers expected in each storage node multi-get response
  public static final Map<CharSequence, String> MULTI_GET_VALID_HEADER_MAP;
  public static final Map<CharSequence, String> COMPUTE_VALID_HEADER_MAP;
  static {
    Map<CharSequence, String> multiGetValidHeaderMap = new HashMap<>();
    multiGetValidHeaderMap.put(HttpHeaderNames.CONTENT_TYPE, HttpConstants.AVRO_BINARY);
    /**
     * TODO: need to revisit this logic if there are multiple response versions for batch-get are available.
     */
    multiGetValidHeaderMap.put(
        HttpConstants.VENICE_SCHEMA_ID,
        Integer.toString(ReadAvroProtocolDefinition.MULTI_GET_RESPONSE_V1.getProtocolVersion()));
    MULTI_GET_VALID_HEADER_MAP = Collections.unmodifiableMap(multiGetValidHeaderMap);

    Map<CharSequence, String> computeValidHeaderMap = new HashMap<>();
    computeValidHeaderMap.put(HttpHeaderNames.CONTENT_TYPE, HttpConstants.AVRO_BINARY);
    /**
     * TODO: need to revisit this logic if there are multiple response versions for read compute are available.
     */
    computeValidHeaderMap.put(
        HttpConstants.VENICE_SCHEMA_ID,
        Integer.toString(ReadAvroProtocolDefinition.COMPUTE_RESPONSE_V1.getProtocolVersion()));
    COMPUTE_VALID_HEADER_MAP = Collections.unmodifiableMap(computeValidHeaderMap);
  }

  public VeniceResponseAggregator(
      RouterStats<AggRouterHttpRequestStats> routerStats,
      Optional<MetaStoreShadowReader> metaStoreShadowReaderOptional) {
    this.routerStats = routerStats;
    this.metaStoreShadowReaderOptional = metaStoreShadowReaderOptional;
  }

  public VeniceResponseAggregator withSingleGetTardyThreshold(long timeout, TimeUnit unit) {
    this.singleGetTardyThresholdInMs = unit.toMillis(timeout);
    return this;
  }

  public VeniceResponseAggregator withMultiGetTardyThreshold(long timeout, TimeUnit unit) {
    this.multiGetTardyThresholdInMs = unit.toMillis(timeout);
    return this;
  }

  public VeniceResponseAggregator withComputeTardyThreshold(long timeout, TimeUnit unit) {
    this.computeTardyThresholdInMs = unit.toMillis(timeout);
    return this;
  }

  public void initHelixGroupSelector(HelixGroupSelector helixGroupSelector) {
    if (this.helixGroupSelector != null) {
      throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(
          null,
          null,
          INTERNAL_SERVER_ERROR,
          "HelixGroupSelector has already been initialized before, and no further update expected!");
    }
    this.helixGroupSelector = helixGroupSelector;
  }

  @Nonnull
  @Override
  public FullHttpResponse buildResponse(
      @Nonnull BasicHttpRequest request,
      Metrics metrics,
      @Nonnull List<FullHttpResponse> gatheredResponses) {
    if (gatheredResponses.isEmpty()) {
      throw RouterExceptionAndTrackingUtils
          .newVeniceExceptionAndTracking(null, null, BAD_GATEWAY, "Received empty response!");
    }
    if (metrics == null) {
      throw RouterExceptionAndTrackingUtils
          .newVeniceExceptionAndTracking(null, null, INTERNAL_SERVER_ERROR, "'metrics' should not be null");
    }
    VenicePath venicePath = metrics.getPath();
    if (venicePath == null) {
      /**
       * This is necessary since the exception could be thrown when parsing request path.
       * If it happens, here will just return the response, which contains exception stacktrace.
       */
      FullHttpResponse response = gatheredResponses.get(0);
      try {
        if (response.status().equals(MOVED_PERMANENTLY)) {
          String errorMsg = response.headers().get(HeaderNames.X_ERROR_MESSAGE);
          String d2Service = VeniceStoreIsMigratedException.getD2ServiceName(errorMsg);
          if (!StringUtils.isEmpty(d2Service)) {
            URI uri = new URI(request.uri());
            uri = new URI("d2", d2Service, uri.getPath(), uri.getQuery(), uri.getFragment());
            String redirectUri = uri.toString();
            LOGGER.info("redirect the request to {}", redirectUri);
            response.setStatus(MOVED_PERMANENTLY);
            response.headers().set(HttpHeaderNames.LOCATION, redirectUri);
          } else {
            LOGGER.error("D2 service name is not available for request redirection");
          }
        }
      } catch (URISyntaxException e) {
        throw RouterExceptionAndTrackingUtils
            .newVeniceExceptionAndTracking(null, null, BAD_REQUEST, "Failed to parse uri");
      }
      return response;
    }

    // TODO: Need to investigate if any of the early terminations above could cause the in-flight request sensor to
    // "leak"

    /**
     * Decrease the group counter in the following conditions:
     * 1. The request is an original request (not retry requests).
     * 2. {@link #helixGroupSelector} is not null.
     * 3. HelixGroupId is valid since Helix-assisted routing is only enabled for multi-key request.
      */
    if (!venicePath.isRetryRequest() && helixGroupSelector != null && venicePath.getHelixGroupId() >= 0) {
      helixGroupSelector.finishRequest(
          venicePath.getRequestId(),
          venicePath.getHelixGroupId(),
          LatencyUtils.getElapsedTimeFromMsToMs(venicePath.getOriginalRequestStartTs()));
    }
    RequestType requestType = venicePath.getRequestType();
    AggRouterHttpRequestStats stats = routerStats.getStatsByType(requestType);
    String storeName = venicePath.getStoreName();
    int versionNumber = venicePath.getVersionNumber();

    FullHttpResponse finalResponse;
    if (venicePath.isStreamingRequest()) {
      /**
       * All the request with type: {@link RequestType.MULTI_GET_STREAMING} and {@link RequestType.COMPUTE_STREAMING}
       * will be handled here.
       */
      finalResponse = buildStreamingResponse(gatheredResponses, storeName, versionNumber);
    } else {
      switch (requestType) {
        case SINGLE_GET:
          finalResponse = gatheredResponses.get(0);
          break;
        case MULTI_GET:
          finalResponse = processMultiGetResponses(
              gatheredResponses,
              storeName,
              versionNumber,
              venicePath.getClientComputeHeader());
          break;
        case COMPUTE:
          finalResponse = processComputeResponses(gatheredResponses, storeName, venicePath.getClientComputeHeader());
          break;
        default:
          throw RouterExceptionAndTrackingUtils
              .newVeniceExceptionAndTracking(null, null, INTERNAL_SERVER_ERROR, "Unknown request type: " + requestType);
      }
    }
    stats.recordFanoutRequestCount(storeName, gatheredResponses.size());

    if (metaStoreShadowReaderOptional.isPresent()) {
      MetaStoreShadowReader metaStoreShadowReader = metaStoreShadowReaderOptional.get();
      if (metaStoreShadowReader.shouldPerformShadowRead(venicePath, finalResponse)) {
        // Record meta store shadow read for the user store.
        String metaStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
        routerStats.getStatsByType(RequestType.SINGLE_GET).recordMetaStoreShadowRead(metaStoreName);
        finalResponse = metaStoreShadowReader.shadowReadMetaStore(venicePath, finalResponse);
      }
    }

    HttpResponseStatus httpResponseStatus = finalResponse.status();
    /**
     * All the metrics in {@link com.linkedin.ddsstorage.router.api.MetricNames} are supported in {@link Metrics}.
     * We are not exposing the following metrics inside Venice right now.
     * 1. {@link ROUTER_PARSE_URI}
     * 2. {@link ROUTER_ROUTING_TIME}
     */
    long timeValue = metrics.get(ROUTER_SERVER_TIME);
    if (timeValue != Metrics.UNSET_VALUE) {
      // TODO: When a batch get throws a quota exception, the ROUTER_SERVER_TIME is missing, so we can't record anything
      // here...
      double latency = LatencyUtils.convertNSToMS(timeValue);
      stats.recordLatency(storeName, latency);
      int keyNum = venicePath.getPartitionKeys().size();
      if (HEALTHY_STATUSES.contains(httpResponseStatus)) {
        routerStats.getStatsByType(RequestType.SINGLE_GET).recordReadQuotaUsage(storeName, keyNum);
        if (isFastRequest(latency, requestType)) {
          stats.recordHealthyRequest(storeName, latency, httpResponseStatus, keyNum);
        } else {
          stats.recordTardyRequest(storeName, latency, httpResponseStatus, keyNum);
        }
      } else if (httpResponseStatus.equals(TOO_MANY_REQUESTS)) {
        LOGGER.debug("request is rejected by storage node because quota is exceeded");
        stats.recordThrottledRequest(storeName, latency, httpResponseStatus, keyNum);
      } else {
        LOGGER.debug("Unhealthy request detected, latency: {}ms, response status: {}", latency, httpResponseStatus);
        stats.recordUnhealthyRequest(storeName, latency, httpResponseStatus, keyNum);
      }
    }
    timeValue = metrics.get(ROUTER_RESPONSE_WAIT_TIME);
    if (timeValue != Metrics.UNSET_VALUE) {
      double waitingTime = LatencyUtils.convertNSToMS(timeValue);
      stats.recordResponseWaitingTime(storeName, waitingTime);
    }
    timeValue = metrics.get(ROUTER_PARSE_URI);
    if (timeValue != Metrics.UNSET_VALUE) {
      double parsingTime = LatencyUtils.convertNSToMS(timeValue);
      stats.recordRequestParsingLatency(storeName, parsingTime);
    }
    timeValue = metrics.get(ROUTER_ROUTING_TIME);
    if (timeValue != Metrics.UNSET_VALUE) {
      double routingTime = LatencyUtils.convertNSToMS(timeValue);
      stats.recordRequestRoutingLatency(storeName, routingTime);
    }
    timeValue = metrics.get(ROUTER_PIPELINE_LATENCY);
    if (timeValue != Metrics.UNSET_VALUE) {
      stats.recordPipelineLatency(storeName, LatencyUtils.convertNSToMS(timeValue));
    }
    timeValue = metrics.get(ROUTER_SCATTER_LATENCY);
    if (timeValue != Metrics.UNSET_VALUE) {
      stats.recordScatterLatency(storeName, LatencyUtils.convertNSToMS(timeValue));
    }
    timeValue = metrics.get(ROUTER_QUEUE_LATENCY);
    if (timeValue != Metrics.UNSET_VALUE) {
      stats.recordQueueLatency(storeName, LatencyUtils.convertNSToMS(timeValue));
    }
    timeValue = metrics.get(ROUTER_DISPATCH_LATENCY);
    if (timeValue != Metrics.UNSET_VALUE) {
      stats.recordDispatchLatency(storeName, LatencyUtils.convertNSToMS(timeValue));
    }
    if (HEALTHY_STATUSES.contains(httpResponseStatus) && !venicePath.isStreamingRequest()) {
      // Only record successful response
      stats.recordResponseSize(storeName, finalResponse.content().readableBytes());
    }
    stats.recordResponse(storeName);

    return finalResponse;
  }

  private FullHttpResponse buildStreamingResponse(
      List<FullHttpResponse> gatheredResponses,
      String storeName,
      int version) {
    CompressionStrategy compressionStrategy = null;

    /**
     * If every sub-response is good, return {@link SuccessfulStreamingResponse} to indicate that,
     * otherwise, return the first error response.
     */
    for (FullHttpResponse subResponse: gatheredResponses) {
      if (!subResponse.status().equals(OK)) {
        return subResponse;
      }
      compressionStrategy = validateAndExtractCompressionStrategy(storeName, version, compressionStrategy, subResponse);
    }

    return new SuccessfulStreamingResponse();
  }

  private boolean isFastRequest(double requestLatencyMs, RequestType requestType) {
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

  private static CompressionStrategy getResponseCompressionStrategy(HttpResponse response) {
    return VeniceResponseDecompressor.getCompressionStrategy(response.headers().get(VENICE_COMPRESSION_STRATEGY));
  }

  /** Compression strategy should be consistent across all records for a specific store version */
  private CompressionStrategy validateAndExtractCompressionStrategy(
      String storeName,
      int version,
      CompressionStrategy compressionStrategy,
      HttpResponse response) {
    CompressionStrategy responseCompression = getResponseCompressionStrategy(response);
    if (responseCompression == compressionStrategy) {
      return responseCompression;
    } else if (compressionStrategy == null) {
      return responseCompression;
    } else {
      String errorMsg = String.format(
          "Inconsistent compression strategy returned. Store: %s; Version: %d, ExpectedCompression: %d, ResponseCompression: %d, All headers: %s",
          storeName,
          version,
          compressionStrategy.getValue(),
          responseCompression.getValue(),
          response.headers().toString());
      throw RouterExceptionAndTrackingUtils
          .newVeniceExceptionAndTracking(storeName, RequestType.MULTI_GET, BAD_GATEWAY, errorMsg);
    }
  }

  protected FullHttpResponse processComputeResponses(
      List<FullHttpResponse> responses,
      String storeName,
      String clientComputeHeader) {
    /**
     * Here we will check the consistency of the following headers among all the responses:
     * 1. {@link HttpHeaderNames.CONTENT_TYPE}
     * 2. {@link HttpConstants.VENICE_SCHEMA_ID}
     */
    CompositeByteBuf content = Unpooled.compositeBuffer();
    int totalRequestRcu = 0;
    for (FullHttpResponse response: responses) {
      if (response.status() != OK) {
        // Return error response directly.
        return response;
      }
      COMPUTE_VALID_HEADER_MAP.forEach((headerName, headerValue) -> {
        String currentValue = response.headers().get(headerName);
        if (currentValue == null) {
          throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(
              storeName,
              RequestType.COMPUTE,
              BAD_GATEWAY,
              "Header: " + headerName + " is expected in compute sub-response");
        }
        if (!headerValue.equals(currentValue)) {
          throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(
              storeName,
              RequestType.COMPUTE,
              BAD_GATEWAY,
              "Incompatible header received for " + headerName + ", values: " + headerValue + ", " + currentValue);
        }
      });

      totalRequestRcu += getRCU(response);
      content.addComponent(true, response.content());
    }

    FullHttpResponse computeResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK, content);
    COMPUTE_VALID_HEADER_MAP.forEach(computeResponse.headers()::set);
    if (clientComputeHeader != null) {
      computeResponse.headers().set(VENICE_CLIENT_COMPUTE, clientComputeHeader);
    }
    computeResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
    computeResponse.headers().set(VENICE_COMPRESSION_STRATEGY, CompressionStrategy.NO_OP.getValue());
    computeResponse.headers().set(VENICE_REQUEST_RCU, totalRequestRcu);
    return computeResponse;
  }

  private int getRCU(FullHttpResponse response) {
    String rcuHeader = response.headers().get(VENICE_REQUEST_RCU);
    if (NumberUtils.isCreatable(rcuHeader)) {
      return Integer.parseInt(rcuHeader);
    } else {
      return 1;
    }
  }

  /**
   * If a part of a multi-get request fails, the entire request should fail from the client's perspective.
   * @param responses Subset of responses from the SN to be concatenated to form the response to the client (guaranteed not empty).
   * @return The concatenated response that should be sent to the client along with some content-related headers.
   */
  protected FullHttpResponse processMultiGetResponses(
      List<FullHttpResponse> responses,
      String storeName,
      int version,
      String clientComputeHeader) {
    long decompressedSize = 0;
    long decompressionTimeInNs = 0;
    int totalRequestRcu = 0;
    CompositeByteBuf content = Unpooled.compositeBuffer();
    CompressionStrategy compressionStrategy = null;

    for (FullHttpResponse response: responses) {
      if (response.status() != OK) {
        response.headers().set(HttpConstants.VENICE_COMPRESSION_STRATEGY, CompressionStrategy.NO_OP.getValue());
        // Return error response directly for now.
        return response;
      }
      compressionStrategy = validateAndExtractCompressionStrategy(storeName, version, compressionStrategy, response);

      content.addComponent(true, response.content());

      /**
       * Here we will check the consistency of the following headers among all the responses:
       * 1. {@link HttpHeaderNames.CONTENT_TYPE}
       * 2. {@link HttpConstants.VENICE_SCHEMA_ID}
       */
      MULTI_GET_VALID_HEADER_MAP.forEach((headerName, headerValue) -> {
        String currentValue = response.headers().get(headerName);
        if (currentValue == null) {
          throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(
              storeName,
              RequestType.MULTI_GET,
              BAD_GATEWAY,
              "Header: " + headerName + " is expected in multi-get sub-response");
        }
        if (!headerValue.equals(currentValue)) {
          throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(
              storeName,
              RequestType.MULTI_GET,
              BAD_GATEWAY,
              "Incompatible header received for " + headerName + ", values: " + headerValue + ", " + currentValue);
        }
      });

      totalRequestRcu += getRCU(response);

      decompressedSize += response.content().readableBytes();
      if (response instanceof VeniceFullHttpResponse) {
        decompressionTimeInNs += ((VeniceFullHttpResponse) response).getDecompressionTimeInNs();
      }
    }

    if (decompressedSize > 0 && decompressionTimeInNs > 0) {
      AggRouterHttpRequestStats stats = routerStats.getStatsByType(RequestType.MULTI_GET);
      stats.recordDecompressedResponseSize(storeName, decompressedSize);
      /**
       * The following metric is actually measuring the deserialization/decompression/re-serialization.
       * Since all the overhead is introduced by the value compression, it might be fine to track them altogether.
       */
      stats.recordDecompressionTime(storeName, LatencyUtils.convertNSToMS(decompressionTimeInNs));
    }

    FullHttpResponse multiGetResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK, content);
    MULTI_GET_VALID_HEADER_MAP.forEach(multiGetResponse.headers()::set);
    if (clientComputeHeader != null) {
      multiGetResponse.headers().set(VENICE_CLIENT_COMPUTE, clientComputeHeader);
    }
    multiGetResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
    multiGetResponse.headers().set(VENICE_COMPRESSION_STRATEGY, compressionStrategy.getValue());
    multiGetResponse.headers().set(VENICE_REQUEST_RCU, totalRequestRcu);
    return multiGetResponse;
  }
}
