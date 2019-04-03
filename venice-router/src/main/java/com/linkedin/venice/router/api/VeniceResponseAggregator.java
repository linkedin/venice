package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.base.misc.Metrics;
import com.linkedin.ddsstorage.base.misc.TimeValue;
import com.linkedin.ddsstorage.netty4.misc.BasicFullHttpRequest;
import com.linkedin.ddsstorage.router.api.ResponseAggregatorFactory;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.LatencyUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

import org.apache.avro.io.OptimizedBinaryDecoderFactory;
import org.apache.log4j.Logger;

import static com.linkedin.ddsstorage.router.api.MetricNames.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static com.linkedin.venice.HttpConstants.VENICE_COMPRESSION_STRATEGY;

public class VeniceResponseAggregator implements ResponseAggregatorFactory<BasicFullHttpRequest, FullHttpResponse> {

  private static final List<HttpResponseStatus> HEALTHY_STATUSES = Arrays.asList(OK, NOT_FOUND);

  private static final Logger LOGGER = Logger.getLogger(VeniceResponseAggregator.class);

  // Headers expected in each storage node multi-get response
  private static final Map<CharSequence, String> MULTI_GET_VALID_HEADER_MAP = new HashMap<>();
  private static final Map<CharSequence, String> COMPUTE_VALID_HEADER_MAP = new HashMap<>();
  static {
    MULTI_GET_VALID_HEADER_MAP.put(HttpHeaderNames.CONTENT_TYPE, HttpConstants.AVRO_BINARY);
    //TODO: If the response version for multi-get changes in the future, we need to update the following protocol version mapping.
    MULTI_GET_VALID_HEADER_MAP.put(HttpConstants.VENICE_SCHEMA_ID,
        Integer.toString(ReadAvroProtocolDefinition.MULTI_GET_RESPONSE_V1.getProtocolVersion()));

    COMPUTE_VALID_HEADER_MAP.put(HttpHeaderNames.CONTENT_TYPE, HttpConstants.AVRO_BINARY);
    // If the response version for read compute changes in the future, we need to update the following protocol version mapping
    COMPUTE_VALID_HEADER_MAP.put(HttpConstants.VENICE_SCHEMA_ID,
        Integer.toString(ReadAvroProtocolDefinition.COMPUTE_RESPONSE_V1.getProtocolVersion()));
  }

  private final AggRouterHttpRequestStats statsForSingleGet;
  private final AggRouterHttpRequestStats statsForMultiGet;
  private final AggRouterHttpRequestStats statsForCompute;

  private final RecordSerializer<MultiGetResponseRecordV1> recordSerializer;
  private final RecordDeserializer<MultiGetResponseRecordV1> recordDeserializer;

  //timeout is configurable and should be overwritten elsewhere
  private long singleGetTardyThresholdInMs = TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);
  private long multiGetTardyThresholdInMs = TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);
  private long computeTardyThresholdInMs = TimeUnit.MILLISECONDS.convert(10, TimeUnit.SECONDS);

  public VeniceResponseAggregator(AggRouterHttpRequestStats statsForSingleGet,
      AggRouterHttpRequestStats statsForMultiGet, AggRouterHttpRequestStats statsForCompute) {
    this.statsForSingleGet = statsForSingleGet;
    this.statsForMultiGet = statsForMultiGet;
    this.statsForCompute = statsForCompute;

    this.recordSerializer = SerializerDeserializerFactory.getAvroGenericSerializer(MultiGetResponseRecordV1.SCHEMA$);
    this.recordDeserializer = SerializerDeserializerFactory.getAvroSpecificDeserializer(MultiGetResponseRecordV1.class);
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
      return gatheredResponses.get(0);
    }
    RequestType requestType = venicePath.getRequestType();
    String storeName = venicePath.getStoreName();
    int version = venicePath.getVersionNumber();
    FullHttpResponse finalResponse = null;
    AggRouterHttpRequestStats stats = null;
    switch (requestType) {
      case SINGLE_GET:
        stats = statsForSingleGet;
        finalResponse = buildSingleGetResponse(storeName, version, gatheredResponses.get(0), stats);
        break;
      case MULTI_GET:
        stats = statsForMultiGet;
        finalResponse = buildMultiKeyResponse(storeName, version, gatheredResponses, stats, MULTI_GET_VALID_HEADER_MAP, requestType);
        break;
      case COMPUTE:
        stats = statsForCompute;
        finalResponse = buildMultiKeyResponse(storeName, version, gatheredResponses, stats, COMPUTE_VALID_HEADER_MAP, requestType);
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
      if (HEALTHY_STATUSES.contains(responseStatus)) {
        statsForSingleGet.recordReadQuotaUsage(storeName, venicePath.getPartitionKeys().size());
        if (isFastRequest(latency, requestType)) {
          stats.recordHealthyRequest(storeName);
        } else {
          stats.recordTardyRequest(storeName);
        }
      } else if (responseStatus.equals(TOO_MANY_REQUESTS)) {
        LOGGER.debug("request is rejected by storage node because quota is exceeded");
        stats.recordThrottledRequest(storeName);
      } else {
        LOGGER.debug("Unhealthy request detected, latency: " + latency + "ms, response status: " + responseStatus);
        stats.recordUnhealthyRequest(storeName);
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
    if (HEALTHY_STATUSES.contains(responseStatus)) {
      // Only record successful response
      stats.recordResponseSize(storeName, finalResponse.content().readableBytes());
    }

    return finalResponse;
  }

  private boolean isFastRequest(double requestLatencyMs, RequestType requestType){
    switch (requestType) {
      case SINGLE_GET:
        return requestLatencyMs < singleGetTardyThresholdInMs;
      case MULTI_GET:
        return requestLatencyMs < multiGetTardyThresholdInMs;
      case COMPUTE:
        return requestLatencyMs < computeTardyThresholdInMs;
      default:
        throw new VeniceException("Unknown request type: " + requestType);
    }
  }

  private FullHttpResponse buildSingleGetResponse(String storeName, int version, FullHttpResponse response, AggRouterHttpRequestStats stats) {
    if (response.status() != OK) {
      return response;
    }

    CompressionStrategy compressionStrategy = response.headers().contains(VENICE_COMPRESSION_STRATEGY) ?
        CompressionStrategy.valueOf(Integer.valueOf(response.headers().get(VENICE_COMPRESSION_STRATEGY))) : CompressionStrategy.NO_OP;

    ByteBuf decompressedData = Unpooled.wrappedBuffer(decompressRecord(storeName, version, compressionStrategy,
        response.content().nioBuffer(), stats));

    if (compressionStrategy != CompressionStrategy.NO_OP) {
      /**
       * When using compression, the data in response is already copied to `decompressedData`, so we can explicitly
       * release the ByteBuf in the response immediately to avoid any memory leak.
       *
       * When not using compression, the backing byte array in the response will be reused to construct the response to
       * client, and the ByteBuf will be released in the netty pipeline.
       */
      response.content().release();
    }

    FullHttpResponse fullHttpResponse = response.replace(decompressedData);
    fullHttpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, decompressedData.readableBytes());
    fullHttpResponse.headers().set(HttpConstants.VENICE_COMPRESSION_STRATEGY, CompressionStrategy.NO_OP.getValue());
    return fullHttpResponse;
  }

  /**
   * Decompressing multi-get response in router side is a bit of overhead.
   * Since records could be concatenated within one response, we need to
   * deserialize the records; decompress the records and then serialize
   * them back.
   *
   * This could be mitigated if client-side decompression is supported later.
   */
  private FullHttpResponse buildMultiKeyResponse(String storeName, int version, List<FullHttpResponse> responses,
      AggRouterHttpRequestStats stats, Map<CharSequence, String> headerMap, RequestType requestType) {
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
      headerMap.forEach((headerName, headerValue) -> {
        String currentValue = response.headers().get(headerName);
        if (null == currentValue) {
          throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.of(storeName), Optional.of(requestType),
              BAD_GATEWAY, "Header: " + headerName + " is expected in multi-get sub-response");
        }
        if (!headerValue.equals(currentValue)) {
          throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.of(storeName), Optional.of(requestType),
              BAD_GATEWAY, "Incompatible header received for " + headerName + ", values: " + headerValue + ", " +  currentValue);
        }
      });

      CompressionStrategy compressionStrategy = response.headers().contains(VENICE_COMPRESSION_STRATEGY) ?
          CompressionStrategy.valueOf(Integer.valueOf(response.headers().get(VENICE_COMPRESSION_STRATEGY))) : CompressionStrategy.NO_OP;

      if (response.content() instanceof CompositeByteBuf) {
        CompositeByteBuf compositeByteBuf = (CompositeByteBuf)response.content();
        for (int i = 0; i < compositeByteBuf.numComponents(); i++) {
          byte[] content = compositeByteBuf.internalComponent(i).array();
          resultLen += addResponseToList(content, contentList, storeName, version, requestType, compressionStrategy, stats);
        }
      } else {
        byte[] content = response.content().array();
        resultLen += addResponseToList(content, contentList, storeName, version, requestType, compressionStrategy, stats);
      }

    }

    // Concat all the responses
    CompositeByteBuf result = Unpooled.compositeBuffer(contentList.size());
    for (int i = 0; i < contentList.size(); i++) {
      byte[] content = contentList.get(i);
      result.addComponent(true, i, Unpooled.wrappedBuffer(content));
    }

    FullHttpResponse multiKeyResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK, result);
    headerMap.forEach((headerName, headerValue) -> {
      multiKeyResponse.headers().add(headerName, headerValue);
    });
    multiKeyResponse.headers().add(HttpHeaderNames.CONTENT_LENGTH, result.readableBytes());
    multiKeyResponse.headers().add(VENICE_COMPRESSION_STRATEGY, CompressionStrategy.NO_OP.getValue());

    return multiKeyResponse;
  }

  private ByteBuffer decompressRecord(String storeName, int version, CompressionStrategy compressionStrategy, ByteBuffer compressedData, AggRouterHttpRequestStats stats) {
    try {
      // record the response size first before copying data from ByteBuffer in case the offset in ByteBuffer is changed
      stats.recordCompressedResponseSize(storeName, compressedData.remaining());
      long decompressionStartTimeInNs = System.nanoTime();
      ByteBuffer decompressed = CompressorFactory.getCompressor(compressionStrategy).decompress(compressedData);
      stats.recordDecompressionTime(storeName, LatencyUtils.getLatencyInMS(decompressionStartTimeInNs));
      return decompressed;
    } catch (IOException e) {
      throw new VeniceException(String.format("failed to decompress data. Store: %s; Version: %d", storeName, version), e);
    }
  }

  private int addResponseToList(byte[] content, List<byte[]> contentList, String storeName, int version,
          RequestType requestType, CompressionStrategy compressionStrategy, AggRouterHttpRequestStats stats) {
    switch (requestType) {
      case COMPUTE:
        // for compute request, decompression has been done on server already.
        contentList.add(content);
        return content.length;
      case MULTI_GET:
        if (compressionStrategy != CompressionStrategy.NO_OP) {
          /**
           * Reuse the original byte array by {@link org.apache.avro.io.OptimizedBinaryDecoder}.
           */
          Iterable<MultiGetResponseRecordV1> records = recordDeserializer.deserializeObjects(
              OptimizedBinaryDecoderFactory.defaultFactory().createOptimizedBinaryDecoder(content, 0, content.length));

          for (MultiGetResponseRecordV1 record : records) {
            record.value = decompressRecord(storeName, version, compressionStrategy, record.value, stats);
          }

          byte[] decompressedRecords = recordSerializer.serializeObjects(records);
          contentList.add(decompressedRecords);
          return decompressedRecords.length;
        } else {
          contentList.add(content);
          return content.length;
        }
      case SINGLE_GET:
        throw new VeniceException("Error! Processing single-get requests while building multi keys response. Store: " + storeName);
      default:
        throw new VeniceException("Unknown request type: " + requestType);
    }
  }
}
