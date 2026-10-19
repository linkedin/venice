package com.linkedin.venice.spark.datawriter.jobs;

import static com.linkedin.venice.spark.SparkConstants.*;
import static org.testng.Assert.*;

import com.linkedin.venice.spark.datawriter.task.DataWriterAccumulators;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class SparkCompactionTest {
  private SparkSession sparkSession;

  @BeforeClass
  public void setUp() {
    sparkSession = SparkSession.builder().appName("SparkCompactionTest").master("local[2]").getOrCreate();
  }

  @AfterClass
  public void tearDown() {
    if (sparkSession != null) {
      sparkSession.stop();
    }
  }

  /**
   * Test compaction with duplicate keys having identical values.
   * Should keep only the record with the highest offset.
   */
  @Test
  public void testCompactionWithIdenticalValues() {
    // Create test data with duplicate keys (same value)
    List<Row> testData = new ArrayList<>();
    byte[] key1 = "key1".getBytes();
    byte[] value1 = "value1".getBytes();
    byte[] rmd1 = "rmd1".getBytes();

    // Same key, same value, different offsets
    testData.add(createRow("region1", 0, 100L, 0, 1, key1, value1, 1, rmd1)); // offset 100
    testData.add(createRow("region1", 0, 200L, 0, 1, key1, value1, 1, rmd1)); // offset 200 (latest)
    testData.add(createRow("region1", 0, 150L, 0, 1, key1, value1, 1, rmd1)); // offset 150

    Dataset<Row> inputDF = sparkSession.createDataFrame(testData, RAW_PUBSUB_INPUT_TABLE_SCHEMA);

    // Apply compaction
    DataWriterAccumulators accumulators = new DataWriterAccumulators(sparkSession);
    Dataset<Row> compactedDF = applyCompactionHelper(inputDF, accumulators);

    // Verify: should have only 1 record (the one with offset 200)
    List<Row> results = compactedDF.collectAsList();
    assertEquals(results.size(), 1, "Should have exactly 1 record after compaction");

    Row result = results.get(0);
    assertEquals((long) result.getAs(OFFSET_COLUMN_NAME), 200L, "Should keep the record with highest offset");
    assertArrayEquals((byte[]) result.getAs(KEY_COLUMN_NAME), key1, "Key should match");
    assertArrayEquals((byte[]) result.getAs(VALUE_COLUMN_NAME), value1, "Value should match");

    // Verify metrics
    assertEquals(accumulators.totalDuplicateKeyCounter.value().longValue(), 1L, "Should track 1 duplicate key");
    assertEquals(
        accumulators.duplicateKeyWithIdenticalValueCounter.value().longValue(),
        1L,
        "Should track 1 duplicate with identical value");
    assertEquals(
        accumulators.duplicateKeyWithDistinctValueCounter.value().longValue(),
        0L,
        "Should have 0 duplicates with distinct values");
  }

  /**
   * Test compaction with duplicate keys having distinct values.
   * Should keep only the record with the highest offset and track distinct value metric.
   */
  @Test
  public void testCompactionWithDistinctValues() {
    // Create test data with duplicate keys (different values)
    List<Row> testData = new ArrayList<>();
    byte[] key1 = "key1".getBytes();
    byte[] value1 = "value1".getBytes();
    byte[] value2 = "value2".getBytes();
    byte[] value3 = "value3".getBytes();
    byte[] rmd1 = "rmd1".getBytes();

    // Same key, different values, different offsets
    testData.add(createRow("region1", 0, 100L, 0, 1, key1, value1, 1, rmd1)); // offset 100
    testData.add(createRow("region1", 0, 300L, 0, 1, key1, value3, 1, rmd1)); // offset 300 (latest)
    testData.add(createRow("region1", 0, 200L, 0, 1, key1, value2, 1, rmd1)); // offset 200

    Dataset<Row> inputDF = sparkSession.createDataFrame(testData, RAW_PUBSUB_INPUT_TABLE_SCHEMA);

    // Apply compaction
    DataWriterAccumulators accumulators = new DataWriterAccumulators(sparkSession);
    Dataset<Row> compactedDF = applyCompactionHelper(inputDF, accumulators);

    // Verify: should have only 1 record (the one with offset 300)
    List<Row> results = compactedDF.collectAsList();
    assertEquals(results.size(), 1, "Should have exactly 1 record after compaction");

    Row result = results.get(0);
    assertEquals((long) result.getAs(OFFSET_COLUMN_NAME), 300L, "Should keep the record with highest offset");
    assertArrayEquals((byte[]) result.getAs(KEY_COLUMN_NAME), key1, "Key should match");
    assertArrayEquals((byte[]) result.getAs(VALUE_COLUMN_NAME), value3, "Should keep the latest value");

    // Verify metrics
    assertEquals(accumulators.totalDuplicateKeyCounter.value().longValue(), 1L, "Should track 1 duplicate key");
    assertEquals(
        accumulators.duplicateKeyWithDistinctValueCounter.value().longValue(),
        1L,
        "Should track 1 duplicate with distinct values");
    assertEquals(
        accumulators.duplicateKeyWithIdenticalValueCounter.value().longValue(),
        0L,
        "Should have 0 duplicates with identical values");
  }

  /**
   * Test compaction with multiple keys, some with duplicates and some without.
   */
  @Test
  public void testCompactionWithMixedKeys() {
    List<Row> testData = new ArrayList<>();
    byte[] key1 = "key1".getBytes();
    byte[] key2 = "key2".getBytes();
    byte[] key3 = "key3".getBytes();
    byte[] value1 = "value1".getBytes();
    byte[] value2 = "value2".getBytes();
    byte[] value3 = "value3".getBytes();
    byte[] rmd = "rmd".getBytes();

    // key1: 2 duplicates (identical values)
    testData.add(createRow("region1", 0, 100L, 0, 1, key1, value1, 1, rmd));
    testData.add(createRow("region1", 0, 200L, 0, 1, key1, value1, 1, rmd)); // latest

    // key2: 3 duplicates (distinct values)
    testData.add(createRow("region1", 0, 150L, 0, 1, key2, value1, 1, rmd));
    testData.add(createRow("region1", 0, 250L, 0, 1, key2, value2, 1, rmd)); // latest
    testData.add(createRow("region1", 0, 180L, 0, 1, key2, value3, 1, rmd));

    // key3: no duplicates
    testData.add(createRow("region1", 0, 300L, 0, 1, key3, value3, 1, rmd));

    Dataset<Row> inputDF = sparkSession.createDataFrame(testData, RAW_PUBSUB_INPUT_TABLE_SCHEMA);

    // Apply compaction
    DataWriterAccumulators accumulators = new DataWriterAccumulators(sparkSession);
    Dataset<Row> compactedDF = applyCompactionHelper(inputDF, accumulators);

    // Verify: should have 3 records (one per unique key)
    List<Row> results = compactedDF.collectAsList();
    assertEquals(results.size(), 3, "Should have 3 records after compaction (one per unique key)");

    // Verify each key kept the latest record
    boolean foundKey1 = false, foundKey2 = false, foundKey3 = false;
    for (Row result: results) {
      byte[] key = result.getAs(KEY_COLUMN_NAME);
      if (Arrays.equals(key, key1)) {
        foundKey1 = true;
        assertEquals((long) result.getAs(OFFSET_COLUMN_NAME), 200L, "key1 should have offset 200");
      } else if (Arrays.equals(key, key2)) {
        foundKey2 = true;
        assertEquals((long) result.getAs(OFFSET_COLUMN_NAME), 250L, "key2 should have offset 250");
        assertArrayEquals((byte[]) result.getAs(VALUE_COLUMN_NAME), value2, "key2 should have value2");
      } else if (Arrays.equals(key, key3)) {
        foundKey3 = true;
        assertEquals((long) result.getAs(OFFSET_COLUMN_NAME), 300L, "key3 should have offset 300");
      }
    }

    assertTrue(foundKey1 && foundKey2 && foundKey3, "Should find all 3 keys");

    // Verify metrics
    assertEquals(
        (long) accumulators.totalDuplicateKeyCounter.value(),
        2L,
        "Should track 2 duplicate keys (key1 and key2)");
    assertEquals(
        (long) accumulators.duplicateKeyWithIdenticalValueCounter.value(),
        1L,
        "Should track 1 duplicate with identical values (key1)");
    assertEquals(
        (long) accumulators.duplicateKeyWithDistinctValueCounter.value(),
        1L,
        "Should track 1 duplicate with distinct values (key2)");
  }

  /**
   * Test compaction with no duplicates - all records should pass through.
   */
  @Test
  public void testCompactionWithNoDuplicates() {
    List<Row> testData = new ArrayList<>();
    byte[] key1 = "key1".getBytes();
    byte[] key2 = "key2".getBytes();
    byte[] key3 = "key3".getBytes();
    byte[] value1 = "value1".getBytes();
    byte[] value2 = "value2".getBytes();
    byte[] value3 = "value3".getBytes();
    byte[] rmd = "rmd".getBytes();

    // All unique keys
    testData.add(createRow("region1", 0, 100L, 0, 1, key1, value1, 1, rmd));
    testData.add(createRow("region1", 0, 200L, 0, 1, key2, value2, 1, rmd));
    testData.add(createRow("region1", 0, 300L, 0, 1, key3, value3, 1, rmd));

    Dataset<Row> inputDF = sparkSession.createDataFrame(testData, RAW_PUBSUB_INPUT_TABLE_SCHEMA);

    // Apply compaction
    DataWriterAccumulators accumulators = new DataWriterAccumulators(sparkSession);
    Dataset<Row> compactedDF = applyCompactionHelper(inputDF, accumulators);

    // Verify: should have all 3 records
    List<Row> results = compactedDF.collectAsList();
    assertEquals(results.size(), 3, "Should have all 3 records (no duplicates)");

    // Verify metrics
    assertEquals((long) accumulators.totalDuplicateKeyCounter.value(), 0L, "Should have 0 duplicate keys");
    assertEquals(
        (long) accumulators.duplicateKeyWithIdenticalValueCounter.value(),
        0L,
        "Should have 0 duplicates with identical values");
    assertEquals(
        (long) accumulators.duplicateKeyWithDistinctValueCounter.value(),
        0L,
        "Should have 0 duplicates with distinct values");
  }

  /**
   * Test compaction with DELETE records.
   * DELETE records should also be compacted by offset.
   */
  @Test
  public void testCompactionWithDeleteRecords() {
    List<Row> testData = new ArrayList<>();
    byte[] key1 = "key1".getBytes();
    byte[] emptyValue = new byte[0];
    byte[] rmd = "rmd".getBytes();

    // Multiple DELETE records for same key
    testData.add(createRow("region1", 0, 100L, 1, 1, key1, emptyValue, 1, rmd)); // DELETE, offset 100
    testData.add(createRow("region1", 0, 200L, 1, 1, key1, emptyValue, 1, rmd)); // DELETE, offset 200 (latest)
    testData.add(createRow("region1", 0, 150L, 1, 1, key1, emptyValue, 1, rmd)); // DELETE, offset 150

    Dataset<Row> inputDF = sparkSession.createDataFrame(testData, RAW_PUBSUB_INPUT_TABLE_SCHEMA);

    // Apply compaction
    DataWriterAccumulators accumulators = new DataWriterAccumulators(sparkSession);
    Dataset<Row> compactedDF = applyCompactionHelper(inputDF, accumulators);

    // Verify: should have only 1 DELETE record (the one with offset 200)
    List<Row> results = compactedDF.collectAsList();
    assertEquals(results.size(), 1, "Should have exactly 1 DELETE record after compaction");

    Row result = results.get(0);
    assertEquals((long) result.getAs(OFFSET_COLUMN_NAME), 200L, "Should keep the DELETE with highest offset");
    assertEquals((int) result.getAs(MESSAGE_TYPE), 1, "Should be a DELETE message");
    assertArrayEquals((byte[]) result.getAs(KEY_COLUMN_NAME), key1, "Key should match");
  }

  /**
   * Test compaction preserves all columns from RAW_PUBSUB_INPUT_TABLE_SCHEMA.
   */
  @Test
  public void testCompactionPreservesAllColumns() {
    List<Row> testData = new ArrayList<>();
    byte[] key1 = "key1".getBytes();
    byte[] value1 = "value1".getBytes();
    byte[] rmd1 = "rmd1".getBytes();
    byte[] chunkedSuffix = "suffix".getBytes();

    // Create row with all fields populated
    testData.add(createRowWithChunkedSuffix("region1", 5, 100L, 0, 10, key1, value1, 2, rmd1, chunkedSuffix)); // offset
                                                                                                               // 100
    testData.add(createRowWithChunkedSuffix("region2", 5, 200L, 0, 10, key1, value1, 2, rmd1, chunkedSuffix)); // offset
                                                                                                               // 200
                                                                                                               // (latest)

    Dataset<Row> inputDF = sparkSession.createDataFrame(testData, RAW_PUBSUB_INPUT_TABLE_SCHEMA);

    // Apply compaction
    DataWriterAccumulators accumulators = new DataWriterAccumulators(sparkSession);
    Dataset<Row> compactedDF = applyCompactionHelper(inputDF, accumulators);

    // Verify: should have only 1 record with all columns preserved
    List<Row> results = compactedDF.collectAsList();
    assertEquals(results.size(), 1, "Should have exactly 1 record after compaction");

    Row result = results.get(0);
    assertEquals((String) result.getAs("__region__"), "region2", "Region should be preserved");
    assertEquals((int) result.getAs("__partition__"), 5, "Partition should be preserved");
    assertEquals((long) result.getAs(OFFSET_COLUMN_NAME), 200L, "Offset should match");
    assertEquals((int) result.getAs(MESSAGE_TYPE), 0, "Message type should be preserved");
    assertEquals((int) result.getAs(SCHEMA_ID_COLUMN_NAME), 10, "Schema ID should be preserved");
    assertArrayEquals((byte[]) result.getAs(KEY_COLUMN_NAME), key1, "Key should be preserved");
    assertArrayEquals((byte[]) result.getAs(VALUE_COLUMN_NAME), value1, "Value should be preserved");
    assertEquals((int) result.getAs(RMD_VERSION_ID_COLUMN_NAME), 2, "RMD version ID should be preserved");
    assertArrayEquals((byte[]) result.getAs(REPLICATION_METADATA_PAYLOAD), rmd1, "RMD should be preserved");
    assertArrayEquals(
        (byte[]) result.getAs(CHUNKED_KEY_SUFFIX_COLUMN_NAME),
        chunkedSuffix,
        "Chunked key suffix should be preserved");
  }

  // Helper methods

  /**
   * Helper to apply compaction (simulates the logic in AbstractDataWriterSparkJob).
   */
  private Dataset<Row> applyCompactionHelper(Dataset<Row> dataFrame, DataWriterAccumulators accumulators) {
    return dataFrame
        .groupByKey(
            (org.apache.spark.api.java.function.MapFunction<Row, byte[]>) row -> row.getAs(KEY_COLUMN_NAME),
            org.apache.spark.sql.Encoders.BINARY())
        .flatMapGroups(
            (org.apache.spark.api.java.function.FlatMapGroupsFunction<byte[], Row, Row>) (keyBytes, rowsIterator) -> {
              List<Row> rowsList = new ArrayList<>();
              rowsIterator.forEachRemaining(rowsList::add);

              if (rowsList.isEmpty()) {
                return java.util.Collections.emptyIterator();
              }

              // Track duplicate keys
              if (rowsList.size() > 1) {
                accumulators.totalDuplicateKeyCounter.add(1);

                // Check if values are identical or distinct
                boolean hasDistinctValues = false;
                byte[] firstValue = rowsList.get(0).getAs(VALUE_COLUMN_NAME);
                for (int i = 1; i < rowsList.size(); i++) {
                  byte[] currentValue = rowsList.get(i).getAs(VALUE_COLUMN_NAME);
                  if (!Arrays.equals(firstValue, currentValue)) {
                    hasDistinctValues = true;
                    break;
                  }
                }

                if (hasDistinctValues) {
                  accumulators.duplicateKeyWithDistinctValueCounter.add(1);
                } else {
                  accumulators.duplicateKeyWithIdenticalValueCounter.add(1);
                }
              }

              // Sort by offset DESC and keep the first (latest) record
              Row latestRecord = rowsList.stream()
                  .max(java.util.Comparator.comparingLong(r -> (long) r.getAs(OFFSET_COLUMN_NAME)))
                  .orElse(null);

              if (latestRecord == null) {
                return java.util.Collections.emptyIterator();
              }

              return java.util.Collections.singletonList(latestRecord).iterator();
            },
            org.apache.spark.sql.catalyst.encoders.RowEncoder.apply(RAW_PUBSUB_INPUT_TABLE_SCHEMA));
  }

  /**
   * Helper to create a Row with RAW_PUBSUB_INPUT_TABLE_SCHEMA.
   */
  private Row createRow(
      String region,
      int partition,
      long offset,
      int messageType,
      int schemaId,
      byte[] key,
      byte[] value,
      int rmdVersionId,
      byte[] rmd) {
    return new GenericRowWithSchema(
        new Object[] { region, partition, offset, messageType, schemaId, key, value, rmdVersionId, rmd, null },
        RAW_PUBSUB_INPUT_TABLE_SCHEMA);
  }

  /**
   * Helper to create a Row with all fields including chunked_key_suffix.
   */
  private Row createRowWithChunkedSuffix(
      String region,
      int partition,
      long offset,
      int messageType,
      int schemaId,
      byte[] key,
      byte[] value,
      int rmdVersionId,
      byte[] rmd,
      byte[] chunkedKeySuffix) {
    return new GenericRowWithSchema(
        new Object[] { region, partition, offset, messageType, schemaId, key, value, rmdVersionId, rmd,
            chunkedKeySuffix },
        RAW_PUBSUB_INPUT_TABLE_SCHEMA);
  }

  private void assertArrayEquals(byte[] expected, byte[] actual, String message) {
    assertTrue(Arrays.equals(expected, actual), message);
  }
}
