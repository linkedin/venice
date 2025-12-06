package com.linkedin.venice.spark.datawriter.writer;

import static java.util.Objects.*;

import com.linkedin.venice.hadoop.task.datawriter.AbstractPartitionWriter;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import com.linkedin.venice.spark.datawriter.task.DataWriterAccumulators;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Test implementation of SparkPartitionWriter that captures records instead of writing to Kafka.
 * Used by end-to-end tests to verify the complete data processing pipeline.
 */
public class TestSparkPartitionWriter extends SparkPartitionWriter {
  private final String testName;

  // Shared storage for captured output from partition writers across all tests
  private static final ConcurrentHashMap<String, List<TestRecord>> CAPTURED_RECORDS = new ConcurrentHashMap<>();

  /**
   * Represents a captured record from the test
   */
  public static class TestRecord {
    public final byte[] key;
    public final byte[] value;
    public final byte[] rmd;

    public TestRecord(byte[] key, byte[] value, byte[] rmd) {
      this.key = key;
      this.value = value;
      this.rmd = rmd;
    }
  }

  public TestSparkPartitionWriter(String testName, Properties jobProperties, DataWriterAccumulators accumulators) {
    super(jobProperties, accumulators);
    this.testName = testName;
  }

  @Override
  public void processRows(Iterator<org.apache.spark.sql.Row> rows) {
    List<VeniceRecordWithMetadata> valueRecordsForKey = new ArrayList<>();
    byte[] key = null;

    while (rows.hasNext()) {
      org.apache.spark.sql.Row row = rows.next();
      byte[] incomingKey = requireNonNull(row.getAs("key"), "Key cannot be null");

      byte[] rmd = null;
      try {
        rmd = row.getAs("rmd");
      } catch (IllegalArgumentException e) {
        // Ignore if rmd is not present
      }

      if (!java.util.Arrays.equals(incomingKey, key)) {
        if (key != null) {
          // Key is different from the prev one and is not null. Capture it.
          processValuesForKey(key, valueRecordsForKey.iterator(), null);
        }
        key = incomingKey;
        valueRecordsForKey = new ArrayList<>();
      }

      byte[] incomingValue = row.getAs("value");
      valueRecordsForKey.add(new AbstractPartitionWriter.VeniceRecordWithMetadata(incomingValue, rmd));
    }

    if (key != null) {
      processValuesForKey(key, valueRecordsForKey.iterator(), null);
    }
  }

  @Override
  public void processValuesForKey(
      byte[] key,
      Iterator<AbstractPartitionWriter.VeniceRecordWithMetadata> values,
      DataWriterTaskTracker dataWriterTaskTracker) {
    // Capture the records instead of writing to Kafka
    while (values.hasNext()) {
      AbstractPartitionWriter.VeniceRecordWithMetadata record = values.next();
      captureRecord(testName, key, record.getValue(), record.getRmd());
    }
  }

  private static void captureRecord(String testName, byte[] key, byte[] value, byte[] rmd) {
    CAPTURED_RECORDS.computeIfAbsent(testName, k -> Collections.synchronizedList(new ArrayList<>()))
        .add(new TestRecord(key, value, rmd));
  }

  public static List<TestRecord> getCapturedRecords(String testName) {
    return CAPTURED_RECORDS.getOrDefault(testName, Collections.emptyList());
  }

  public static void clearCapturedRecords(String testName) {
    CAPTURED_RECORDS.remove(testName);
  }
}
