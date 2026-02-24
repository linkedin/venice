package com.linkedin.venice.spark.datawriter.compression;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.spark.datawriter.task.DataWriterAccumulators;
import com.linkedin.venice.spark.datawriter.task.SparkDataWriterTaskTracker;
import com.linkedin.venice.utils.ByteUtils;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;


/**
 * Spark compression re-encoder for repush jobs.
 */
public class SparkCompressionReEncoder implements Serializable {
  private static final long serialVersionUID = 1L;

  private final CompressionStrategy sourceStrategy;
  private final CompressionStrategy destStrategy;
  private final byte[] sourceDict;
  private final byte[] destDict;
  private final StructType schema;
  private final int valueColumnIndex;
  private final int keyColumnIndex;
  private final boolean compressionMetricCollectionEnabled;
  private final boolean isSameCompression;
  private final DataWriterAccumulators accumulators;

  private transient CompressorFactory compressorFactory;
  private transient VeniceCompressor sourceCompressor;
  private transient VeniceCompressor destCompressor;
  private transient VeniceCompressor gzipCompressor;
  private transient VeniceCompressor zstdMetricCompressor;
  private transient SparkDataWriterTaskTracker taskTracker;

  public SparkCompressionReEncoder(
      CompressionStrategy sourceStrategy,
      CompressionStrategy destStrategy,
      byte[] sourceDict,
      byte[] destDict,
      StructType schema,
      int valueColumnIndex,
      int keyColumnIndex,
      boolean compressionMetricCollectionEnabled,
      DataWriterAccumulators accumulators) {
    this.sourceStrategy = sourceStrategy;
    this.destStrategy = destStrategy;
    this.sourceDict = sourceDict;
    this.destDict = destDict;
    this.schema = schema;
    this.valueColumnIndex = valueColumnIndex;
    this.keyColumnIndex = keyColumnIndex;
    this.compressionMetricCollectionEnabled = compressionMetricCollectionEnabled;
    this.isSameCompression = sourceStrategy == destStrategy && Arrays.equals(sourceDict, destDict);
    this.accumulators = accumulators;
  }

  public Row reEncode(Row row) throws IOException {
    byte[] sourceValueBytes = row.getAs(valueColumnIndex);
    if (sourceValueBytes == null || sourceValueBytes.length == 0) {
      // DELETE records (null value) and any empty values don't need re-encoding
      return row;
    }

    SparkDataWriterTaskTracker tracker = getTaskTracker();

    // If source and dest use the same strategy and dictionary, the source bytes
    // are already in the correct target format — skip decompression and recompression entirely.
    if (isSameCompression) {
      if (tracker != null) {
        byte[] keyBytes = row.getAs(keyColumnIndex);
        if (keyBytes != null) {
          tracker.trackKeySize(keyBytes.length);
          tracker.trackCompressedValueSize(sourceValueBytes.length);
        }
      }
      return row;
    }

    // Different strategies — decompress from source and re-compress to destination
    ByteBuffer decompressed = getSourceCompressor().decompress(sourceValueBytes, 0, sourceValueBytes.length);
    byte[] uncompressedBytes = ByteUtils.extractByteArray(decompressed);
    int uncompressedSize = uncompressedBytes.length;

    ByteBuffer recompressed = getDestCompressor().compress(ByteBuffer.wrap(uncompressedBytes), 0);
    byte[] recompressedBytes = ByteUtils.extractByteArray(recompressed);

    // Track metrics only for valid records (non-null key)
    if (tracker != null) {
      byte[] keyBytes = row.getAs(keyColumnIndex);
      if (keyBytes != null) {
        tracker.trackKeySize(keyBytes.length);
        tracker.trackUncompressedValueSize(uncompressedSize);
        tracker.trackLargestUncompressedValueSize(uncompressedSize);
        tracker.trackCompressedValueSize(recompressedBytes.length);
        if (compressionMetricCollectionEnabled) {
          trackAlternativeCompressionMetrics(tracker, uncompressedBytes, recompressedBytes);
        }
      }
    }

    // Create a new row with the re-compressed value
    Object[] values = new Object[row.length()];
    for (int i = 0; i < row.length(); i++) {
      if (i == valueColumnIndex) {
        values[i] = recompressedBytes;
      } else {
        values[i] = row.get(i);
      }
    }
    return new GenericRowWithSchema(values, schema);
  }

  private void trackAlternativeCompressionMetrics(
      SparkDataWriterTaskTracker tracker,
      byte[] uncompressedBytes,
      byte[] compressedBytes) throws IOException {
    // Track GZIP compressed size
    if (destStrategy == CompressionStrategy.GZIP) {
      // Already compressed with GZIP, reuse
      tracker.trackGzipCompressedValueSize(compressedBytes.length);
    } else {
      byte[] gzipBytes =
          ByteUtils.extractByteArray(getGzipCompressor().compress(ByteBuffer.wrap(uncompressedBytes), 0));
      tracker.trackGzipCompressedValueSize(gzipBytes.length);
    }

    // Track ZSTD compressed size (only if dictionary available)
    if (destDict != null && destDict.length > 0) {
      if (destStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
        // Already compressed with ZSTD_WITH_DICT, reuse
        tracker.trackZstdCompressedValueSize(compressedBytes.length);
      } else {
        byte[] zstdBytes =
            ByteUtils.extractByteArray(getZstdMetricCompressor().compress(ByteBuffer.wrap(uncompressedBytes), 0));
        tracker.trackZstdCompressedValueSize(zstdBytes.length);
      }
    }
  }

  public void close() {
    if (compressorFactory != null) {
      compressorFactory.close();
    }
  }

  private CompressorFactory getCompressorFactory() {
    if (compressorFactory == null) {
      compressorFactory = new CompressorFactory();
    }
    return compressorFactory;
  }

  private VeniceCompressor getSourceCompressor() {
    if (sourceCompressor == null) {
      sourceCompressor = getCompressor(sourceStrategy, sourceDict, "repush_reencoder_source");
    }
    return sourceCompressor;
  }

  private VeniceCompressor getDestCompressor() {
    if (destCompressor == null) {
      destCompressor = getCompressor(destStrategy, destDict, "repush_reencoder_dest");
    }
    return destCompressor;
  }

  private VeniceCompressor getGzipCompressor() {
    if (gzipCompressor == null) {
      gzipCompressor = getCompressorFactory().getCompressor(CompressionStrategy.GZIP);
    }
    return gzipCompressor;
  }

  private VeniceCompressor getZstdMetricCompressor() {
    if (zstdMetricCompressor == null) {
      zstdMetricCompressor = getCompressorFactory().createVersionSpecificCompressorIfNotExist(
          CompressionStrategy.ZSTD_WITH_DICT,
          "repush_metric_zstd",
          destDict);
    }
    return zstdMetricCompressor;
  }

  private SparkDataWriterTaskTracker getTaskTracker() {
    if (taskTracker == null && accumulators != null) {
      taskTracker = new SparkDataWriterTaskTracker(accumulators);
    }
    return taskTracker;
  }

  private VeniceCompressor getCompressor(CompressionStrategy strategy, byte[] dict, String cacheKey) {
    if (strategy == CompressionStrategy.ZSTD_WITH_DICT && dict != null && dict.length > 0) {
      // cacheKey must be unique per compressor instance to avoid returning a cached compressor
      // with a different dictionary (e.g., source dict vs dest dict).
      return getCompressorFactory().createVersionSpecificCompressorIfNotExist(strategy, cacheKey, dict);
    }
    return getCompressorFactory().getCompressor(strategy);
  }
}
