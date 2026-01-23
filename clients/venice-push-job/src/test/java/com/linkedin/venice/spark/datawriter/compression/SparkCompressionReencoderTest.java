package com.linkedin.venice.spark.datawriter.compression;

import static com.linkedin.venice.spark.SparkConstants.DEFAULT_SCHEMA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.utils.ByteUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.testng.annotations.Test;


public class SparkCompressionReencoderTest {
  private final CompressorFactory compressorFactory = new CompressorFactory();

  @Test
  public void testReencodeSameStrategy() throws IOException {
    String originalValue = "test-value-content-for-compression";
    byte[] valueBytes = originalValue.getBytes();

    VeniceCompressor gzipCompressor = compressorFactory.getCompressor(CompressionStrategy.GZIP);
    byte[] compressedBytes = ByteUtils.extractByteArray(gzipCompressor.compress(ByteBuffer.wrap(valueBytes), 0));

    Row row =
        new GenericRowWithSchema(new Object[] { "key".getBytes(), compressedBytes, "rmd".getBytes() }, DEFAULT_SCHEMA);

    // Same strategy, same dict (null)
    SparkCompressionReencoder reencoder = new SparkCompressionReencoder(
        CompressionStrategy.GZIP,
        CompressionStrategy.GZIP,
        null,
        null,
        DEFAULT_SCHEMA,
        1);

    Row result = reencoder.reencode(row);

    // Should return identical row (optimization)
    assertEquals(result, row);
    assertTrue(Arrays.equals((byte[]) result.get(1), compressedBytes));
  }

  @Test
  public void testReencodeDifferentStrategy() throws IOException {
    String originalValue = "test-value-content-for-compression-transformation";
    byte[] uncompressedBytes = originalValue.getBytes();

    // Source: GZIP
    VeniceCompressor gzipCompressor = compressorFactory.getCompressor(CompressionStrategy.GZIP);
    byte[] sourceCompressedBytes =
        ByteUtils.extractByteArray(gzipCompressor.compress(ByteBuffer.wrap(uncompressedBytes), 0));

    Row row = new GenericRowWithSchema(
        new Object[] { "key".getBytes(), sourceCompressedBytes, "rmd".getBytes() },
        DEFAULT_SCHEMA);

    // Target: ZSTD_WITH_DICT
    byte[] dict = "test-dict".getBytes();
    SparkCompressionReencoder reencoder = new SparkCompressionReencoder(
        CompressionStrategy.GZIP,
        CompressionStrategy.ZSTD_WITH_DICT,
        null,
        dict,
        DEFAULT_SCHEMA,
        1);

    Row result = reencoder.reencode(row);

    assertNotEquals(result, row);
    byte[] resultCompressedBytes = (byte[]) result.get(1);
    assertNotEquals(resultCompressedBytes, sourceCompressedBytes);

    // Verify it can be decompressed by ZSTD_WITH_DICT
    VeniceCompressor zstdCompressor = compressorFactory
        .createVersionSpecificCompressorIfNotExist(CompressionStrategy.ZSTD_WITH_DICT, "test-topic", dict);
    ByteBuffer decompressed = zstdCompressor.decompress(resultCompressedBytes, 0, resultCompressedBytes.length);
    assertEquals(new String(ByteUtils.extractByteArray(decompressed)), originalValue);
  }

  @Test
  public void testReencodeToNoOp() throws IOException {
    String originalValue = "test-value-for-noop-target";
    byte[] uncompressedBytes = originalValue.getBytes();

    // Source: GZIP
    VeniceCompressor gzipCompressor = compressorFactory.getCompressor(CompressionStrategy.GZIP);
    byte[] sourceCompressedBytes =
        ByteUtils.extractByteArray(gzipCompressor.compress(ByteBuffer.wrap(uncompressedBytes), 0));

    Row row = new GenericRowWithSchema(
        new Object[] { "key".getBytes(), sourceCompressedBytes, "rmd".getBytes() },
        DEFAULT_SCHEMA);

    // Target: NO_OP
    SparkCompressionReencoder reencoder = new SparkCompressionReencoder(
        CompressionStrategy.GZIP,
        CompressionStrategy.NO_OP,
        null,
        null,
        DEFAULT_SCHEMA,
        1);

    Row result = reencoder.reencode(row);

    byte[] resultBytes = (byte[]) result.get(1);
    assertEquals(new String(resultBytes), originalValue);
  }

  @Test
  public void testReencodeFromNoOp() throws IOException {
    String originalValue = "test-value-from-noop-source";
    byte[] uncompressedBytes = originalValue.getBytes();

    Row row = new GenericRowWithSchema(
        new Object[] { "key".getBytes(), uncompressedBytes, "rmd".getBytes() },
        DEFAULT_SCHEMA);

    // Source: NO_OP, Target: GZIP
    SparkCompressionReencoder reencoder = new SparkCompressionReencoder(
        CompressionStrategy.NO_OP,
        CompressionStrategy.GZIP,
        null,
        null,
        DEFAULT_SCHEMA,
        1);

    Row result = reencoder.reencode(row);

    byte[] resultBytes = (byte[]) result.get(1);

    // Verify it's compressed with GZIP
    VeniceCompressor gzipCompressor = compressorFactory.getCompressor(CompressionStrategy.GZIP);
    ByteBuffer decompressed = gzipCompressor.decompress(resultBytes, 0, resultBytes.length);
    assertEquals(new String(ByteUtils.extractByteArray(decompressed)), originalValue);
  }
}
