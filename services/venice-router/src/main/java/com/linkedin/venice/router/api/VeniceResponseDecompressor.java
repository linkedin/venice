package com.linkedin.venice.router.api;

import static com.linkedin.venice.HttpConstants.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;

import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Pair;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpRequest;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;


/**
 * This class is used to handle all the decompression related logic in Router, and it will be used in
 * {@link VeniceResponseAggregator} for regular requests and {@link VeniceDispatcher} for streaming requests.
 */
public class VeniceResponseDecompressor {
  private static final RecordSerializer<MultiGetResponseRecordV1> recordSerializer =
      FastSerializerDeserializerFactory.getFastAvroGenericSerializer(MultiGetResponseRecordV1.SCHEMA$);
  private static final RecordDeserializer<MultiGetResponseRecordV1> recordDeserializer =
      FastSerializerDeserializerFactory
          .getFastAvroSpecificDeserializer(MultiGetResponseRecordV1.SCHEMA$, MultiGetResponseRecordV1.class);

  private final CompressionStrategy clientCompression;
  private final RouterStats<AggRouterHttpRequestStats> routerStats;
  private final String storeName;
  private final int version;
  private final String kafkaTopic;
  private final CompressorFactory compressorFactory;
  private final ExecutorService decompressionExecutor;
  private final int multiGetDecompressionBatchSize;

  public VeniceResponseDecompressor(
      boolean decompressOnClient,
      RouterStats<AggRouterHttpRequestStats> routerStats,
      BasicFullHttpRequest request,
      String storeName,
      int version,
      CompressorFactory compressorFactory,
      ExecutorService decompressionExecutor,
      int multiGetDecompressionBatchSize) {
    this.routerStats = routerStats;
    this.clientCompression = decompressOnClient ? getClientSupportedCompression(request) : CompressionStrategy.NO_OP;
    this.storeName = storeName;
    this.version = version;
    this.kafkaTopic = Version.composeKafkaTopic(storeName, version);
    this.compressorFactory = compressorFactory;
    this.decompressionExecutor = decompressionExecutor;
    this.multiGetDecompressionBatchSize = multiGetDecompressionBatchSize;
  }

  private static CompressionStrategy getClientSupportedCompression(HttpRequest request) {
    return getCompressionStrategy(request.headers().get(VENICE_SUPPORTED_COMPRESSION_STRATEGY));
  }

  public static CompressionStrategy getCompressionStrategy(String compressionHeader) {
    if (compressionHeader == null) {
      return CompressionStrategy.NO_OP;
    }
    return CompressionStrategy.valueOf(Integer.parseInt(compressionHeader));
  }

  public boolean canPassThroughResponse(CompressionStrategy responseCompression) {
    return responseCompression == clientCompression || responseCompression == CompressionStrategy.NO_OP;
  }

  public ContentDecompressResult decompressSingleGetContent(CompressionStrategy compressionStrategy, ByteBuf content) {
    if (canPassThroughResponse(compressionStrategy)) {
      // Decompress record on the client side if needed
      return new ContentDecompressResult(content, compressionStrategy, 0);
    }

    AggRouterHttpRequestStats stats = routerStats.getStatsByType(RequestType.SINGLE_GET);
    stats.recordCompressedResponseSize(storeName, content.readableBytes());
    long startTimeInNs = System.nanoTime();
    ByteBuf copy = content.isReadOnly() ? content.copy() : content;
    ByteBuf decompressedData =
        Unpooled.wrappedBuffer(decompressRecord(compressionStrategy, copy.nioBuffer(), RequestType.SINGLE_GET));
    final long decompressionTimeInNs = System.nanoTime() - startTimeInNs;
    stats.recordDecompressionTime(storeName, LatencyUtils.getLatencyInMS(startTimeInNs));

    /**
     * When using compression, the data in response is already copied to `decompressedData`, so we can explicitly
     * release the ByteBuf in the response immediately to avoid any memory leak.
     *
     * When not using compression, the backing byte array in the response will be reused to construct the response to
     * client, and the ByteBuf will be released in the netty pipeline.
     */
    content.release();

    return new ContentDecompressResult(decompressedData, CompressionStrategy.NO_OP, decompressionTimeInNs);
  }

  /**
   * Decompressing multi-get response in router side is a bit of overhead.
   * Since records could be concatenated within one response, we need to
   * deserialize the records; decompress the records and then serialize
   * them back.
   */
  public ContentDecompressResult decompressMultiGetContent(CompressionStrategy compressionStrategy, ByteBuf content) {
    if (canPassThroughResponse(compressionStrategy)) {
      return new ContentDecompressResult(content, compressionStrategy, 0L);
    } else {
      CompositeByteBuf decompressedData = Unpooled.compositeBuffer();
      long startTimeInNs = System.nanoTime();
      if (content instanceof CompositeByteBuf) {
        for (ByteBuf buffer: (CompositeByteBuf) content) {
          decompressedData.addComponent(true, decompressMultiGetRecords(compressionStrategy, buffer));
        }
      } else {
        decompressedData.addComponent(true, decompressMultiGetRecords(compressionStrategy, content));
      }
      final long decompressionTimeInNs = System.nanoTime() - startTimeInNs;
      /**
       * When using compression, the data in response is already copied during decompression, so we can explicitly
       * release the ByteBuf in the response immediately to avoid any memory leak.
       *
       * When not using compression, the backing byte array in the response will be reused to construct the response to
       * client, and the ByteBuf will be released in the netty pipeline.
       */
      content.release();

      return new ContentDecompressResult(decompressedData, CompressionStrategy.NO_OP, decompressionTimeInNs);
    }
  }

  public Pair<ByteBuf, CompressionStrategy> processMultiGetResponseForStreaming(
      CompressionStrategy responseCompression,
      ByteBuf content) {
    if (canPassThroughResponse(responseCompression)) {
      // Decompress record on the client side if needed
      return new Pair<>(content, responseCompression);
    }

    AggRouterHttpRequestStats stats = routerStats.getStatsByType(RequestType.MULTI_GET_STREAMING);
    stats.recordCompressedResponseSize(storeName, content.readableBytes());
    long startTimeInNs = System.nanoTime();
    ByteBuf copy = content.isReadOnly() ? content.copy() : content;
    ByteBuf decompressedContent = decompressMultiGetRecords(responseCompression, copy);
    stats.recordDecompressionTime(storeName, LatencyUtils.getLatencyInMS(startTimeInNs));
    content.release();
    return new Pair<>(decompressedContent, CompressionStrategy.NO_OP);
  }

  private ByteBuffer decompressRecord(
      CompressionStrategy compressionStrategy,
      ByteBuffer compressedData,
      RequestType requestType) {
    try {
      VeniceCompressor compressor;
      if (compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
        compressor = compressorFactory.getVersionSpecificCompressor(kafkaTopic);
        if (compressor == null) {
          throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(
              Optional.of(storeName),
              Optional.of(requestType),
              SERVICE_UNAVAILABLE,
              "Compressor not available for resource " + kafkaTopic + ". Dictionary not downloaded.");
        }
      } else {
        compressor = compressorFactory.getCompressor(compressionStrategy);
      }
      ByteBuffer decompressed = compressor.decompress(compressedData);
      return decompressed;
    } catch (IOException e) {
      String errorMsg = String
          .format("Failed to decompress data. Store: %s; Version: %d, error: %s", storeName, version, e.getMessage());
      throw RouterExceptionAndTrackingUtils
          .newVeniceExceptionAndTracking(Optional.of(storeName), Optional.of(requestType), BAD_GATEWAY, errorMsg);
    }
  }

  private ByteBuf decompressMultiGetRecords(CompressionStrategy compressionStrategy, ByteBuf data) {
    ByteBuf copy = data.isReadOnly() ? data.copy() : data;
    Iterable<MultiGetResponseRecordV1> records = recordDeserializer.deserializeObjects(
        OptimizedBinaryDecoderFactory.defaultFactory()
            .createOptimizedBinaryDecoder(copy.array(), 0, copy.readableBytes()));

    List<CompletableFuture<Void>> decompressRecordsFutures = new ArrayList<>();
    List<MultiGetResponseRecordV1> decompressionBatch = null;
    for (MultiGetResponseRecordV1 record: records) {
      if (decompressionBatch == null) {
        decompressionBatch = new ArrayList<>(multiGetDecompressionBatchSize);
      }
      decompressionBatch.add(record);
      if (decompressionBatch.size() == multiGetDecompressionBatchSize) {
        decompressRecordsFutures.add(decompressRecords(decompressionBatch, compressionStrategy, decompressionExecutor));
        decompressionBatch = null;
      }
    }

    if (decompressionBatch != null) {
      decompressRecordsFutures.add(decompressRecords(decompressionBatch, compressionStrategy, decompressionExecutor));
    }

    try {
      // Wait for all decompression futures to complete
      CompletableFuture.allOf(decompressRecordsFutures.toArray(new CompletableFuture[decompressRecordsFutures.size()]))
          .handle((none, e) -> {
            if (e != null) {
              throw new VeniceException(e);
            }

            return null;
          })
          .get(1L, TimeUnit.SECONDS);
    } catch (Exception e) {
      String errorMsg = String
          .format("Failed to decompress data. Store: %s; Version: %d, error: %s", storeName, version, e.getMessage());
      throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(
          Optional.of(storeName),
          Optional.of(RequestType.MULTI_GET),
          BAD_GATEWAY,
          errorMsg);
    }

    return Unpooled.wrappedBuffer(recordSerializer.serializeObjects(records, AvroSerializer.REUSE.get()));
  }

  private CompletableFuture<Void> decompressRecords(
      List<MultiGetResponseRecordV1> records,
      CompressionStrategy compressionStrategy,
      ExecutorService executor) {
    return CompletableFuture.supplyAsync(() -> {
      for (MultiGetResponseRecordV1 record: records) {
        record.value = decompressRecord(compressionStrategy, record.value, RequestType.MULTI_GET);
      }
      return null;
    }, executor);
  }
}
