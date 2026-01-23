package com.linkedin.venice.spark.datawriter.compression;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.utils.ByteUtils;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;


/**
 * Spark-compatible compression re-encoder for repush workloads.
 * If source and destination compression strategies are different, it decompresses the value
 * using the source strategy and re-compresses it using the destination strategy.
 */
public class SparkCompressionReencoder implements Serializable {
  private static final long serialVersionUID = 1L;

  private final CompressionStrategy sourceStrategy;
  private final CompressionStrategy destStrategy;
  private final byte[] sourceDict;
  private final byte[] destDict;
  private final StructType schema;
  private final int valueColumnIndex;

  private transient CompressorFactory compressorFactory;
  private transient VeniceCompressor sourceCompressor;
  private transient VeniceCompressor destCompressor;

  public SparkCompressionReencoder(
      CompressionStrategy sourceStrategy,
      CompressionStrategy destStrategy,
      byte[] sourceDict,
      byte[] destDict,
      StructType schema,
      int valueColumnIndex) {
    this.sourceStrategy = sourceStrategy;
    this.destStrategy = destStrategy;
    this.sourceDict = sourceDict;
    this.destDict = destDict;
    this.schema = schema;
    this.valueColumnIndex = valueColumnIndex;
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

  private VeniceCompressor getCompressor(CompressionStrategy strategy, byte[] dict) {
    if (strategy == CompressionStrategy.ZSTD_WITH_DICT && dict != null && dict.length > 0) {
      // Topic name is only used as a cache key in CompressorFactory for ZSTD_WITH_DICT
      return getCompressorFactory()
          .createVersionSpecificCompressorIfNotExist(strategy, "repush_reencoder_internal", dict);
    }
    return getCompressorFactory().getCompressor(strategy);
  }

  public Row reencode(Row row) throws IOException {
    // If strategies are same and dictionaries are same, no need to re-encode
    if (sourceStrategy == destStrategy && Arrays.equals(sourceDict, destDict)) {
      return row;
    }

    byte[] sourceValueBytes = row.getAs(valueColumnIndex);
    if (sourceValueBytes == null) {
      return row;
    }

    // Decompress and re-compress
    ByteBuffer decompressed = getSourceCompressor().decompress(sourceValueBytes, 0, sourceValueBytes.length);
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
}
