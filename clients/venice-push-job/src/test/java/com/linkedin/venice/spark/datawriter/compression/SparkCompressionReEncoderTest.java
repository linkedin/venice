package com.linkedin.venice.spark.datawriter.compression;

import static com.linkedin.venice.spark.SparkConstants.DEFAULT_SCHEMA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.spark.datawriter.task.DataWriterAccumulators;
import com.linkedin.venice.utils.ByteUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class SparkCompressionReEncoderTest {
  private final CompressorFactory compressorFactory = new CompressorFactory();
  private SparkSession sparkSession;

  // DEFAULT_SCHEMA: key(0), value(1), rmd(2)
  private static final int KEY_IDX = 0;
  private static final int VALUE_IDX = 1;

  @BeforeClass
  public void setUp() {
    SparkConf conf = new SparkConf().setAppName("SparkCompressionReEncoderTest").setMaster("local[1]");
    sparkSession = SparkSession.builder().config(conf).getOrCreate();
  }

  @AfterClass
  public void tearDown() {
    if (sparkSession != null) {
      sparkSession.stop();
    }
  }

  @Test
  public void testReEncodeSameStrategy() throws IOException {
    String originalValue = "test-value-content-for-compression";
    byte[] valueBytes = originalValue.getBytes();

    VeniceCompressor gzipCompressor = compressorFactory.getCompressor(CompressionStrategy.GZIP);
    byte[] compressedBytes = ByteUtils.extractByteArray(gzipCompressor.compress(ByteBuffer.wrap(valueBytes), 0));

    Row row =
        new GenericRowWithSchema(new Object[] { "key".getBytes(), compressedBytes, "rmd".getBytes() }, DEFAULT_SCHEMA);

    SparkCompressionReEncoder reEncoder = new SparkCompressionReEncoder(
        CompressionStrategy.GZIP,
        CompressionStrategy.GZIP,
        null,
        null,
        DEFAULT_SCHEMA,
        VALUE_IDX,
        KEY_IDX,
        false,
        null);

    Row result = reEncoder.reEncode(row);

    assertEquals(result, row);
    assertTrue(Arrays.equals((byte[]) result.get(1), compressedBytes));
  }

  @Test
  public void testReEncodeDifferentStrategy() throws IOException {
    String originalValue = "test-value-content-for-compression-transformation";
    byte[] uncompressedBytes = originalValue.getBytes();

    VeniceCompressor gzipCompressor = compressorFactory.getCompressor(CompressionStrategy.GZIP);
    byte[] sourceCompressedBytes =
        ByteUtils.extractByteArray(gzipCompressor.compress(ByteBuffer.wrap(uncompressedBytes), 0));

    Row row = new GenericRowWithSchema(
        new Object[] { "key".getBytes(), sourceCompressedBytes, "rmd".getBytes() },
        DEFAULT_SCHEMA);

    byte[] dict = "test-dict".getBytes();
    SparkCompressionReEncoder reEncoder = new SparkCompressionReEncoder(
        CompressionStrategy.GZIP,
        CompressionStrategy.ZSTD_WITH_DICT,
        null,
        dict,
        DEFAULT_SCHEMA,
        VALUE_IDX,
        KEY_IDX,
        false,
        null);

    Row result = reEncoder.reEncode(row);

    assertNotEquals(result, row);
    byte[] resultCompressedBytes = (byte[]) result.get(1);
    assertNotEquals(resultCompressedBytes, sourceCompressedBytes);

    VeniceCompressor zstdCompressor = compressorFactory
        .createVersionSpecificCompressorIfNotExist(CompressionStrategy.ZSTD_WITH_DICT, "test-topic", dict);
    ByteBuffer decompressed = zstdCompressor.decompress(resultCompressedBytes, 0, resultCompressedBytes.length);
    assertEquals(new String(ByteUtils.extractByteArray(decompressed)), originalValue);
  }

  @Test
  public void testReEncodeToNoOp() throws IOException {
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
    SparkCompressionReEncoder reEncoder = new SparkCompressionReEncoder(
        CompressionStrategy.GZIP,
        CompressionStrategy.NO_OP,
        null,
        null,
        DEFAULT_SCHEMA,
        VALUE_IDX,
        KEY_IDX,
        false,
        null);

    Row result = reEncoder.reEncode(row);

    byte[] resultBytes = (byte[]) result.get(1);
    assertEquals(new String(resultBytes), originalValue);
  }

  @Test
  public void testReEncodeFromNoOp() throws IOException {
    String originalValue = "test-value-from-noop-source";
    byte[] uncompressedBytes = originalValue.getBytes();

    Row row = new GenericRowWithSchema(
        new Object[] { "key".getBytes(), uncompressedBytes, "rmd".getBytes() },
        DEFAULT_SCHEMA);

    // Source: NO_OP, Target: GZIP
    SparkCompressionReEncoder reEncoder = new SparkCompressionReEncoder(
        CompressionStrategy.NO_OP,
        CompressionStrategy.GZIP,
        null,
        null,
        DEFAULT_SCHEMA,
        VALUE_IDX,
        KEY_IDX,
        false,
        null);

    Row result = reEncoder.reEncode(row);

    byte[] resultBytes = (byte[]) result.get(1);

    // Verify it's compressed with GZIP
    VeniceCompressor gzipCompressor = compressorFactory.getCompressor(CompressionStrategy.GZIP);
    ByteBuffer decompressed = gzipCompressor.decompress(resultBytes, 0, resultBytes.length);
    assertEquals(new String(ByteUtils.extractByteArray(decompressed)), originalValue);
  }

  @Test
  public void testMetricCollectionTracksAllMetrics() throws IOException {
    byte[] keyBytes = "test-key".getBytes();
    byte[] uncompressedBytes = "content-for-all-metrics".getBytes();

    // Source: GZIP compressed
    VeniceCompressor gzipCompressor = compressorFactory.getCompressor(CompressionStrategy.GZIP);
    byte[] sourceCompressedBytes =
        ByteUtils.extractByteArray(gzipCompressor.compress(ByteBuffer.wrap(uncompressedBytes), 0));

    Row row =
        new GenericRowWithSchema(new Object[] { keyBytes, sourceCompressedBytes, "rmd".getBytes() }, DEFAULT_SCHEMA);

    DataWriterAccumulators accumulators = new DataWriterAccumulators(sparkSession);

    // GZIP -> NO_OP (re-encoding path)
    SparkCompressionReEncoder reEncoder = new SparkCompressionReEncoder(
        CompressionStrategy.GZIP,
        CompressionStrategy.NO_OP,
        null,
        null,
        DEFAULT_SCHEMA,
        VALUE_IDX,
        KEY_IDX,
        false,
        accumulators);

    reEncoder.reEncode(row);

    // Key size tracked
    assertEquals(accumulators.totalKeySizeCounter.value().longValue(), (long) keyBytes.length);
    // Uncompressed value size tracked
    assertEquals(accumulators.uncompressedValueSizeCounter.value().longValue(), (long) uncompressedBytes.length);
    // Compressed value size tracked (NO_OP dest means compressed = uncompressed)
    assertEquals(accumulators.compressedValueSizeCounter.value().longValue(), (long) uncompressedBytes.length);
  }

  @Test
  public void testMetricCollectionWithSameStrategy() throws IOException {
    byte[] keyBytes = "same-strategy-key".getBytes();
    byte[] uncompressedBytes = "content-for-same-strategy-metrics".getBytes();

    Row row = new GenericRowWithSchema(new Object[] { keyBytes, uncompressedBytes, "rmd".getBytes() }, DEFAULT_SCHEMA);

    DataWriterAccumulators accumulators = new DataWriterAccumulators(sparkSession);

    // NO_OP -> NO_OP (same strategy pass-through path — skips decompression entirely)
    SparkCompressionReEncoder reEncoder = new SparkCompressionReEncoder(
        CompressionStrategy.NO_OP,
        CompressionStrategy.NO_OP,
        null,
        null,
        DEFAULT_SCHEMA,
        VALUE_IDX,
        KEY_IDX,
        false,
        accumulators);

    reEncoder.reEncode(row);

    // Pass-through tracks key size and compressed value size only (no decompression)
    assertEquals(accumulators.totalKeySizeCounter.value().longValue(), (long) keyBytes.length);
    assertEquals(accumulators.compressedValueSizeCounter.value().longValue(), (long) uncompressedBytes.length);
    // Uncompressed size NOT tracked on pass-through (decompression is skipped)
    assertEquals(accumulators.uncompressedValueSizeCounter.value().longValue(), 0);
  }

  @Test
  public void testAlternativeCompressionMetrics() throws IOException {
    byte[] uncompressedBytes = "content-for-alternative-compression-metrics-test".getBytes();

    // Source: GZIP compressed
    VeniceCompressor gzipCompressor = compressorFactory.getCompressor(CompressionStrategy.GZIP);
    byte[] sourceCompressedBytes =
        ByteUtils.extractByteArray(gzipCompressor.compress(ByteBuffer.wrap(uncompressedBytes), 0));

    Row row = new GenericRowWithSchema(
        new Object[] { "key".getBytes(), sourceCompressedBytes, "rmd".getBytes() },
        DEFAULT_SCHEMA);

    DataWriterAccumulators accumulators = new DataWriterAccumulators(sparkSession);

    // GZIP -> NO_OP with compressionMetricCollectionEnabled=true (different strategies, exercises decompress path)
    SparkCompressionReEncoder reEncoder = new SparkCompressionReEncoder(
        CompressionStrategy.GZIP,
        CompressionStrategy.NO_OP,
        null,
        null,
        DEFAULT_SCHEMA,
        VALUE_IDX,
        KEY_IDX,
        true,
        accumulators);

    reEncoder.reEncode(row);

    // GZIP alternative metric should be tracked (compressed with gzip for comparison)
    assertTrue(accumulators.gzipCompressedValueSizeCounter.value() > 0);
    // ZSTD not tracked because no dictionary provided
    assertEquals(accumulators.zstdCompressedValueSizeCounter.value().longValue(), 0);
  }

  @Test
  public void testMetricsDisabledNoAlternativeTracking() throws IOException {
    byte[] keyBytes = "key-for-disabled-metrics".getBytes();
    byte[] uncompressedBytes = "content-for-disabled-alternative-metrics".getBytes();

    // Source: GZIP compressed
    VeniceCompressor gzipCompressor = compressorFactory.getCompressor(CompressionStrategy.GZIP);
    byte[] sourceCompressedBytes =
        ByteUtils.extractByteArray(gzipCompressor.compress(ByteBuffer.wrap(uncompressedBytes), 0));

    Row row =
        new GenericRowWithSchema(new Object[] { keyBytes, sourceCompressedBytes, "rmd".getBytes() }, DEFAULT_SCHEMA);

    DataWriterAccumulators accumulators = new DataWriterAccumulators(sparkSession);

    // GZIP -> NO_OP with compressionMetricCollectionEnabled=false (different strategies, exercises decompress path)
    SparkCompressionReEncoder reEncoder = new SparkCompressionReEncoder(
        CompressionStrategy.GZIP,
        CompressionStrategy.NO_OP,
        null,
        null,
        DEFAULT_SCHEMA,
        VALUE_IDX,
        KEY_IDX,
        false,
        accumulators);

    reEncoder.reEncode(row);

    // Basic metrics still tracked (decompression happens for different strategies)
    assertEquals(accumulators.totalKeySizeCounter.value().longValue(), (long) keyBytes.length);
    assertEquals(accumulators.uncompressedValueSizeCounter.value().longValue(), (long) uncompressedBytes.length);
    // Compressed value = uncompressed bytes since dest is NO_OP
    assertEquals(accumulators.compressedValueSizeCounter.value().longValue(), (long) uncompressedBytes.length);

    // Alternative metrics NOT tracked (compressionMetricCollectionEnabled=false)
    assertEquals(accumulators.gzipCompressedValueSizeCounter.value().longValue(), 0);
    assertEquals(accumulators.zstdCompressedValueSizeCounter.value().longValue(), 0);
  }

  @Test
  public void testAlternativeCompressionMetricsWithZstdDict() throws IOException {
    // When destDict is available and dest is NOT ZSTD_WITH_DICT, ZSTD metric is computed
    byte[] uncompressedBytes = "content-for-zstd-alternative-metrics-test".getBytes();
    byte[] destDict = "test-dict-for-zstd-metrics".getBytes();

    // Source: NO_OP, Dest: NO_OP, but destDict provided → ZSTD metric path exercises else branch
    Row row = new GenericRowWithSchema(
        new Object[] { "key".getBytes(), uncompressedBytes, "rmd".getBytes() },
        DEFAULT_SCHEMA);

    DataWriterAccumulators accumulators = new DataWriterAccumulators(sparkSession);

    SparkCompressionReEncoder reEncoder = new SparkCompressionReEncoder(
        CompressionStrategy.NO_OP,
        CompressionStrategy.NO_OP,
        null,
        destDict,
        DEFAULT_SCHEMA,
        VALUE_IDX,
        KEY_IDX,
        true,
        accumulators);

    reEncoder.reEncode(row);

    // GZIP alternative metric tracked
    assertTrue(accumulators.gzipCompressedValueSizeCounter.value() > 0);
    // ZSTD alternative metric tracked because destDict is provided
    assertTrue(accumulators.zstdCompressedValueSizeCounter.value() > 0);

    reEncoder.close();
  }

  @Test
  public void testAlternativeCompressionMetricsGzipDest() throws IOException {
    // When dest IS GZIP, the already-compressed bytes are reused for the GZIP metric
    byte[] uncompressedBytes = "content-for-gzip-dest-metrics".getBytes();

    Row row = new GenericRowWithSchema(
        new Object[] { "key".getBytes(), uncompressedBytes, "rmd".getBytes() },
        DEFAULT_SCHEMA);

    DataWriterAccumulators accumulators = new DataWriterAccumulators(sparkSession);

    // Source: NO_OP, Dest: GZIP, metrics enabled → GZIP reuse path
    SparkCompressionReEncoder reEncoder = new SparkCompressionReEncoder(
        CompressionStrategy.NO_OP,
        CompressionStrategy.GZIP,
        null,
        null,
        DEFAULT_SCHEMA,
        VALUE_IDX,
        KEY_IDX,
        true,
        accumulators);

    reEncoder.reEncode(row);

    // GZIP metric tracked (reuses dest-compressed bytes since dest is GZIP)
    assertTrue(accumulators.gzipCompressedValueSizeCounter.value() > 0);
    // ZSTD not tracked (no dict)
    assertEquals(accumulators.zstdCompressedValueSizeCounter.value().longValue(), 0);

    reEncoder.close();
  }

  @Test
  public void testAlternativeCompressionMetricsZstdDest() throws IOException {
    // When dest IS ZSTD_WITH_DICT, the already-compressed bytes are reused for ZSTD metric
    byte[] uncompressedBytes = "content-for-zstd-dest-metrics".getBytes();
    byte[] destDict = "test-dict-for-zstd-dest".getBytes();

    Row row = new GenericRowWithSchema(
        new Object[] { "key".getBytes(), uncompressedBytes, "rmd".getBytes() },
        DEFAULT_SCHEMA);

    DataWriterAccumulators accumulators = new DataWriterAccumulators(sparkSession);

    // Source: NO_OP, Dest: ZSTD_WITH_DICT, metrics enabled → ZSTD reuse path
    SparkCompressionReEncoder reEncoder = new SparkCompressionReEncoder(
        CompressionStrategy.NO_OP,
        CompressionStrategy.ZSTD_WITH_DICT,
        null,
        destDict,
        DEFAULT_SCHEMA,
        VALUE_IDX,
        KEY_IDX,
        true,
        accumulators);

    reEncoder.reEncode(row);

    // GZIP metric tracked (dest is not GZIP, so a fresh GZIP compression is done)
    assertTrue(accumulators.gzipCompressedValueSizeCounter.value() > 0);
    // ZSTD metric tracked (reuses dest-compressed bytes since dest is ZSTD_WITH_DICT)
    assertTrue(accumulators.zstdCompressedValueSizeCounter.value() > 0);

    reEncoder.close();
  }

  @Test
  public void testReEncodeZstdWithDifferentDictionaries() throws IOException {
    // This test validates that source and dest ZSTD compressors use different cache keys,
    // ensuring the dest compressor is created with the dest dictionary (not the source dictionary).
    String originalValue = "test-value-for-zstd-different-dicts-reencoding";
    byte[] uncompressedBytes = originalValue.getBytes();

    byte[] sourceDict = "source-dictionary-bytes-for-zstd".getBytes();
    byte[] destDict = "dest-dictionary-bytes-for-zstd-different".getBytes();

    // Compress with source dictionary
    VeniceCompressor sourceZstd = compressorFactory
        .createVersionSpecificCompressorIfNotExist(CompressionStrategy.ZSTD_WITH_DICT, "test-source-topic", sourceDict);
    byte[] sourceCompressedBytes =
        ByteUtils.extractByteArray(sourceZstd.compress(ByteBuffer.wrap(uncompressedBytes), 0));

    Row row = new GenericRowWithSchema(
        new Object[] { "key".getBytes(), sourceCompressedBytes, "rmd".getBytes() },
        DEFAULT_SCHEMA);

    // ZSTD_WITH_DICT (source dict) -> ZSTD_WITH_DICT (dest dict)
    SparkCompressionReEncoder reEncoder = new SparkCompressionReEncoder(
        CompressionStrategy.ZSTD_WITH_DICT,
        CompressionStrategy.ZSTD_WITH_DICT,
        sourceDict,
        destDict,
        DEFAULT_SCHEMA,
        VALUE_IDX,
        KEY_IDX,
        false,
        null);

    Row result = reEncoder.reEncode(row);

    // Result should be re-compressed with the dest dictionary
    byte[] resultBytes = (byte[]) result.get(1);
    assertNotEquals(resultBytes, sourceCompressedBytes);

    // Verify the result can be decompressed with the dest dictionary
    VeniceCompressor destZstd = compressorFactory
        .createVersionSpecificCompressorIfNotExist(CompressionStrategy.ZSTD_WITH_DICT, "test-dest-topic", destDict);
    ByteBuffer decompressed = destZstd.decompress(resultBytes, 0, resultBytes.length);
    assertEquals(new String(ByteUtils.extractByteArray(decompressed)), originalValue);

    reEncoder.close();
  }

  @Test
  public void testReEncodeSkipsNullValue() throws IOException {
    // DELETE records have null values, re-encoder should skip them
    Row row = new GenericRowWithSchema(new Object[] { "key".getBytes(), null, "rmd".getBytes() }, DEFAULT_SCHEMA);

    SparkCompressionReEncoder reEncoder = new SparkCompressionReEncoder(
        CompressionStrategy.GZIP,
        CompressionStrategy.NO_OP,
        null,
        null,
        DEFAULT_SCHEMA,
        VALUE_IDX,
        KEY_IDX,
        false,
        null);

    Row result = reEncoder.reEncode(row);

    // Row returned unchanged — no decompression attempted
    assertEquals(result, row);
    assertNull(result.get(1));
  }

  @Test
  public void testReEncodeSkipsEmptyValue() throws IOException {
    // empty byte arrays should be skipped
    byte[] emptyValue = new byte[0];

    Row row = new GenericRowWithSchema(new Object[] { "key".getBytes(), emptyValue, "rmd".getBytes() }, DEFAULT_SCHEMA);

    SparkCompressionReEncoder reEncoder = new SparkCompressionReEncoder(
        CompressionStrategy.GZIP,
        CompressionStrategy.NO_OP,
        null,
        null,
        DEFAULT_SCHEMA,
        VALUE_IDX,
        KEY_IDX,
        false,
        null);

    Row result = reEncoder.reEncode(row);

    // Row returned unchanged — no decompression attempted
    assertEquals(result, row);
    assertTrue(Arrays.equals((byte[]) result.get(1), emptyValue));
  }
}
