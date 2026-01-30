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
  private final boolean compressionMetricCollectionEnabled;
  private final DataWriterAccumulators accumulators;

  private transient CompressorFactory compressorFactory;
  private transient VeniceCompressor sourceCompressor;
  private transient VeniceCompressor destCompressor;
  private transient SparkDataWriterTaskTracker taskTracker;

  public SparkCompressionReEncoder(
      CompressionStrategy sourceStrategy,
      CompressionStrategy destStrategy,
      byte[] sourceDict,
      byte[] destDict,
      StructType schema,
      int valueColumnIndex,
      boolean compressionMetricCollectionEnabled,
      DataWriterAccumulators accumulators) {
    this.sourceStrategy = sourceStrategy;
    this.destStrategy = destStrategy;
    this.sourceDict = sourceDict;
    this.destDict = destDict;
    this.schema = schema;
    this.valueColumnIndex = valueColumnIndex;
    this.compressionMetricCollectionEnabled = compressionMetricCollectionEnabled;
    this.accumulators = accumulators;
  }

  public Row reEncode(Row row) throws IOException {
    byte[] sourceValueBytes = row.getAs(valueColumnIndex);
    if (sourceValueBytes == null) {
      return row;
    }

    ByteBuffer decompressed = getSourceCompressor().decompress(sourceValueBytes, 0, sourceValueBytes.length);

    if (compressionMetricCollectionEnabled) {
      SparkDataWriterTaskTracker tracker = getTaskTracker();
      if (tracker != null) {
        tracker.trackUncompressedValueSize(decompressed.remaining());
      }
    }

    // If strategies are same and dictionaries are same, no need to re-encode
    if (sourceStrategy == destStrategy && Arrays.equals(sourceDict, destDict)) {
      return row;
    }

    ByteBuffer recompressed = getDestCompressor().compress(decompressed, 0);

    // Create a new row with the re-compressed value
    Object[] values = new Object[row.length()];
    for (int i = 0; i < row.length(); i++) {
      if (i == valueColumnIndex) {
        values[i] = ByteUtils.extractByteArray(recompressed);
      } else {
        values[i] = row.get(i);
      }
    }
    return new GenericRowWithSchema(values, schema);
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
      sourceCompressor = getCompressor(sourceStrategy, sourceDict);
    }
    return sourceCompressor;
  }

  private VeniceCompressor getDestCompressor() {
    if (destCompressor == null) {
      destCompressor = getCompressor(destStrategy, destDict);
    }
    return destCompressor;
  }

  private SparkDataWriterTaskTracker getTaskTracker() {
    if (taskTracker == null && accumulators != null) {
      taskTracker = new SparkDataWriterTaskTracker(accumulators);
    }
    return taskTracker;
  }

  private VeniceCompressor getCompressor(CompressionStrategy strategy, byte[] dict) {
    if (strategy == CompressionStrategy.ZSTD_WITH_DICT && dict != null && dict.length > 0) {
      // Topic name is only used as a cache key in CompressorFactory for ZSTD_WITH_DICT
      return getCompressorFactory()
          .createVersionSpecificCompressorIfNotExist(strategy, "repush_reencoder_internal", dict);
    }
    return getCompressorFactory().getCompressor(strategy);
  }

}
