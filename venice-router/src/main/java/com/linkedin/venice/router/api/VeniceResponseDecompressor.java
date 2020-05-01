package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.netty4.misc.BasicFullHttpRequest;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Pair;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;

import static com.linkedin.venice.HttpConstants.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;


/**
 * This class is used to handle all the decompression related logic in Router, and it will be used in
 * {@link VeniceResponseAggregator} for regular requests and {@link VeniceDispatcher} for streaming requests.
 */
public class VeniceResponseDecompressor {
  private static final RecordSerializer<MultiGetResponseRecordV1> recordSerializer =
      SerializerDeserializerFactory.getAvroGenericSerializer(MultiGetResponseRecordV1.SCHEMA$);;
  private static final RecordDeserializer<MultiGetResponseRecordV1> recordDeserializer =
      SerializerDeserializerFactory.getAvroSpecificDeserializer(MultiGetResponseRecordV1.class);

  // Headers expected in each storage node multi-get response
  public static final Map<CharSequence, String> MULTI_GET_VALID_HEADER_MAP = new HashMap<>();
  public static final Map<CharSequence, String> COMPUTE_VALID_HEADER_MAP = new HashMap<>();
  static {
    MULTI_GET_VALID_HEADER_MAP.put(HttpHeaderNames.CONTENT_TYPE, HttpConstants.AVRO_BINARY);
    /**
     * TODO: need to revisit this logic if there are multiple response versions for batch-get are available.
     */
    MULTI_GET_VALID_HEADER_MAP.put(HttpConstants.VENICE_SCHEMA_ID,
        Integer.toString(ReadAvroProtocolDefinition.MULTI_GET_RESPONSE_V1.getProtocolVersion()));

    COMPUTE_VALID_HEADER_MAP.put(HttpHeaderNames.CONTENT_TYPE, HttpConstants.AVRO_BINARY);
    /**
     * TODO: need to revisit this logic if there are multiple response versions for read compute are available.
     */
    COMPUTE_VALID_HEADER_MAP.put(HttpConstants.VENICE_SCHEMA_ID,
        Integer.toString(ReadAvroProtocolDefinition.COMPUTE_RESPONSE_V1.getProtocolVersion()));
  }

  private final CompressionStrategy clientCompression;
  private final RouterStats<AggRouterHttpRequestStats> routerStats;
  private final String storeName;
  private final int version;
  private final String kafkaTopic;

  public VeniceResponseDecompressor(boolean decompressOnClient, RouterStats<AggRouterHttpRequestStats> routerStats, BasicFullHttpRequest request, String storeName, int version) {
    this.routerStats = routerStats;
    this.clientCompression = decompressOnClient ? getClientSupportedCompression(request) : CompressionStrategy.NO_OP;
    this.storeName = storeName;
    this.version = version;
    this.kafkaTopic = Version.composeKafkaTopic(storeName, version);
  }

  private static CompressionStrategy getClientSupportedCompression(HttpRequest request) {
    return getCompressionStrategy(request.headers().get(VENICE_SUPPORTED_COMPRESSION_STRATEGY));
  }

  private static CompressionStrategy getResponseCompressionStrategy(HttpResponse response) {
    return getCompressionStrategy(response.headers().get(VENICE_COMPRESSION_STRATEGY));
  }

  public static CompressionStrategy getCompressionStrategy(String compressionHeader) {
    if (null == compressionHeader) {
      return CompressionStrategy.NO_OP;
    }
    return CompressionStrategy.valueOf(Integer.valueOf(compressionHeader));
  }

  private boolean canPassThroughResponse(CompressionStrategy responseCompression) {
    return responseCompression == clientCompression || responseCompression == CompressionStrategy.NO_OP;
  }

  public FullHttpResponse processSingleGetResponse(FullHttpResponse response) {
    if (response.status() != OK) {
      return response;
    }

    CompressionStrategy responseCompression = getResponseCompressionStrategy(response);
    if (canPassThroughResponse(responseCompression)) {
      // Decompress record on the client side if needed
      return response;
    }

    AggRouterHttpRequestStats stats = routerStats.getStatsByType(RequestType.SINGLE_GET);
    stats.recordCompressedResponseSize(storeName, response.content().readableBytes());
    long startTimeInNs = System.nanoTime();
    ByteBuf decompressedData = Unpooled.wrappedBuffer(decompressRecord(responseCompression,
        response.content().nioBuffer(), RequestType.SINGLE_GET));
    stats.recordDecompressionTime(storeName, LatencyUtils.getLatencyInMS(startTimeInNs));

    /**
     * When using compression, the data in response is already copied to `decompressedData`, so we can explicitly
     * release the ByteBuf in the response immediately to avoid any memory leak.
     *
     * When not using compression, the backing byte array in the response will be reused to construct the response to
     * client, and the ByteBuf will be released in the netty pipeline.
     */
    response.content().release();

    FullHttpResponse fullHttpResponse  = response.replace(decompressedData);
    fullHttpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, fullHttpResponse.content().readableBytes());
    fullHttpResponse.headers().set(HttpConstants.VENICE_COMPRESSION_STRATEGY, CompressionStrategy.NO_OP.getValue());
    return fullHttpResponse;
  }

  public CompressionStrategy validateAndExtractCompressionStrategy(List<FullHttpResponse> responses) {
    CompressionStrategy compressionStrategy = null;
    for (FullHttpResponse response : responses) {
      CompressionStrategy responseCompression = getResponseCompressionStrategy(response);
      if (compressionStrategy == null) {
        compressionStrategy = responseCompression;
      }
      if (responseCompression != compressionStrategy) {
        // Compression strategy should be consistent accross all records for a specific store version
        String errorMsg = String.format("Inconsistent compression strategy retruned. Store: %s; Version: %d, ExpectedCompression: %d, ResponseCompression: %d",
            storeName, version, compressionStrategy.getValue(), responseCompression.getValue());
        throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.of(storeName), Optional.of(RequestType.MULTI_GET),
            BAD_GATEWAY, errorMsg);
      }
    }
    return compressionStrategy;
  }

  public FullHttpResponse processComputeResponses(List<FullHttpResponse> responses) {
    /**
     * Here we will check the consistency of the following headers among all the responses:
     * 1. {@link HttpHeaderNames.CONTENT_TYPE}
     * 2. {@link HttpConstants.VENICE_SCHEMA_ID}
     */
    CompositeByteBuf content = Unpooled.compositeBuffer();
    for (FullHttpResponse response : responses) {
      if (response.status() != OK) {
        // Return error response directly.
        return response;
      }
      COMPUTE_VALID_HEADER_MAP.forEach((headerName, headerValue) -> {
        String currentValue = response.headers().get(headerName);
        if (null == currentValue) {
          throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.of(storeName), Optional.of(RequestType.COMPUTE),
              BAD_GATEWAY, "Header: " + headerName + " is expected in compute sub-response");
        }
        if (!headerValue.equals(currentValue)) {
          throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.of(storeName), Optional.of(RequestType.COMPUTE),
              BAD_GATEWAY,
              "Incompatible header received for " + headerName + ", values: " + headerValue + ", " + currentValue);
        }
      });

      content.addComponent(true, response.content());
    }

    FullHttpResponse computeResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK, content);
      COMPUTE_VALID_HEADER_MAP.forEach((headerName, headerValue) -> {
      computeResponse.headers().add(headerName, headerValue);
    });
    computeResponse.headers().add(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
    computeResponse.headers().add(VENICE_COMPRESSION_STRATEGY, CompressionStrategy.NO_OP.getValue());
    return computeResponse;
  }

  /**
   * Decompressing multi-get response in router side is a bit of overhead.
   * Since records could be concatenated within one response, we need to
   * deserialize the records; decompress the records and then serialize
   * them back.
   */
  public FullHttpResponse processMultiGetResponses(List<FullHttpResponse> responses) {
    long decompressedSize = 0;
    long decompressionTimeInNs = 0;
    CompositeByteBuf content = Unpooled.compositeBuffer();
    // Venice only supports either compression of the whole database or no compression at all.
    CompressionStrategy responseCompression = validateAndExtractCompressionStrategy(responses);

    for (FullHttpResponse response : responses) {
      if (response.status() != OK) {
        // Return error response directly for now.
        return response;
      }

      /**
       * Here we will check the consistency of the following headers among all the responses:
       * 1. {@link HttpHeaderNames.CONTENT_TYPE}
       * 2. {@link HttpConstants.VENICE_SCHEMA_ID}
       */
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

      if (canPassThroughResponse(responseCompression)) {
        content.addComponent(true, response.content());
      } else {
        long startTimeInNs = System.nanoTime();
        if (response.content() instanceof CompositeByteBuf) {
          for (ByteBuf buffer : (CompositeByteBuf)response.content()) {
            content.addComponent(true, decompressMultiGetRecords(responseCompression, buffer));
          }
        } else {
          content.addComponent(true, decompressMultiGetRecords(responseCompression, response.content()));
        }
        decompressedSize +=  response.content().readableBytes();
        decompressionTimeInNs += System.nanoTime() - startTimeInNs;
        /**
         * When using compression, the data in response is already copied during decompression, so we can explicitly
         * release the ByteBuf in the response immediately to avoid any memory leak.
         *
         * When not using compression, the backing byte array in the response will be reused to construct the response to
         * client, and the ByteBuf will be released in the netty pipeline.
         */
        response.content().release();
      }
    }

    if (decompressedSize > 0) {
      AggRouterHttpRequestStats stats = routerStats.getStatsByType(RequestType.MULTI_GET);
      stats.recordCompressedResponseSize(storeName, decompressedSize);
      /**
       * The following metric is actually measuring the deserialization/decompression/re-serialization.
       * Since all the overhead is introduced by the value compression, it might be fine to track them altogether.
       */
      stats.recordDecompressionTime(storeName, LatencyUtils.convertLatencyFromNSToMS(decompressionTimeInNs));
      // Content is already decompressed by service router above
      responseCompression = CompressionStrategy.NO_OP;
    }

    FullHttpResponse multiGetResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK, content);
    MULTI_GET_VALID_HEADER_MAP.forEach((headerName, headerValue) -> {
      multiGetResponse.headers().add(headerName, headerValue);
    });
    multiGetResponse.headers().add(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
    multiGetResponse.headers().add(VENICE_COMPRESSION_STRATEGY, responseCompression.getValue());
    return multiGetResponse;
  }

  public Pair<ByteBuf, CompressionStrategy> processMultiGetResponseForStreaming(CompressionStrategy responseCompression, ByteBuf content) {
    if (canPassThroughResponse(responseCompression)) {
      // Decompress record on the client side if needed
      return new Pair<>(content, responseCompression);
    }

    AggRouterHttpRequestStats stats = routerStats.getStatsByType(RequestType.MULTI_GET_STREAMING);
    stats.recordCompressedResponseSize(storeName, content.readableBytes());
    long startTimeInNs = System.nanoTime();
    ByteBuf decompressedContent = decompressMultiGetRecords(responseCompression, content);
    stats.recordDecompressionTime(storeName, LatencyUtils.getLatencyInMS(startTimeInNs));

    content.release();
    return new Pair<>(decompressedContent, CompressionStrategy.NO_OP);
  }

  private ByteBuffer decompressRecord(CompressionStrategy compressionStrategy, ByteBuffer compressedData, RequestType requestType) {
    try {
      VeniceCompressor compressor;
      if (compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
        compressor = CompressorFactory.getVersionSpecificCompressor(kafkaTopic);
        if (compressor == null) {
          throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.of(storeName), Optional.of(requestType),
              SERVICE_UNAVAILABLE, "Compressor not available for resource " + kafkaTopic + ". Dictionary not downloaded.");
        }
      } else {
        compressor = CompressorFactory.getCompressor(compressionStrategy);
      }
      ByteBuffer decompressed = compressor.decompress(compressedData);
      return decompressed;
    } catch (IOException e) {
      String errorMsg = String.format("Failed to decompress data. Store: %s; Version: %d, error: %s", storeName, version, e.getMessage());
      throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.of(storeName), Optional.of(requestType),
          BAD_GATEWAY, errorMsg);
    }
  }

  private ByteBuf decompressMultiGetRecords(CompressionStrategy compressionStrategy, ByteBuf data) {
    Iterable<MultiGetResponseRecordV1> records = recordDeserializer.deserializeObjects(
        OptimizedBinaryDecoderFactory.defaultFactory().createOptimizedBinaryDecoder(data.array(), 0, data.readableBytes()));
    for (MultiGetResponseRecordV1 record : records) {
      record.value = decompressRecord(compressionStrategy, record.value, RequestType.MULTI_GET);
    }
    return Unpooled.wrappedBuffer(recordSerializer.serializeObjects(records));
  }
}
