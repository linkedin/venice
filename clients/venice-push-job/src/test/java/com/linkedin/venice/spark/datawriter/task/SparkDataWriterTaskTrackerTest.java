package com.linkedin.venice.spark.datawriter.task;

import com.linkedin.venice.spark.SparkConstants;
import org.apache.spark.sql.SparkSession;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class SparkDataWriterTaskTrackerTest {
  private SparkSession spark;

  @BeforeClass
  public void setUp() {
    spark = SparkSession.builder().appName("TestApp").master(SparkConstants.DEFAULT_SPARK_CLUSTER).getOrCreate();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    spark.stop();
  }

  @Test
  public void testSprayAllPartitions() {
    DataWriterAccumulators accumulators = new DataWriterAccumulators(spark);
    SparkDataWriterTaskTracker tracker = new SparkDataWriterTaskTracker(accumulators);

    tracker.trackSprayAllPartitions();
    Assert.assertEquals(tracker.getSprayAllPartitionsCount(), 1);

    DataWriterAccumulators expectedAccumulators = new DataWriterAccumulators(spark);
    expectedAccumulators.sprayAllPartitionsTriggeredCount.add(1);

    verifyAllAccumulators(accumulators, expectedAccumulators);
  }

  @Test
  public void testEmptyRecord() {
    DataWriterAccumulators accumulators = new DataWriterAccumulators(spark);
    SparkDataWriterTaskTracker tracker = new SparkDataWriterTaskTracker(accumulators);

    tracker.trackEmptyRecord();

    DataWriterAccumulators expectedAccumulators = new DataWriterAccumulators(spark);
    expectedAccumulators.emptyRecordCounter.add(1);

    verifyAllAccumulators(accumulators, expectedAccumulators);
  }

  @Test
  public void testKeySize() {
    DataWriterAccumulators accumulators = new DataWriterAccumulators(spark);
    SparkDataWriterTaskTracker tracker = new SparkDataWriterTaskTracker(accumulators);

    tracker.trackKeySize(100);
    Assert.assertEquals(tracker.getTotalKeySize(), 100);

    tracker.trackKeySize(100);
    Assert.assertEquals(tracker.getTotalKeySize(), 200);

    DataWriterAccumulators expectedAccumulators = new DataWriterAccumulators(spark);
    expectedAccumulators.totalKeySizeCounter.add(200);

    verifyAllAccumulators(accumulators, expectedAccumulators);
  }

  @Test
  public void testUncompressedValueSize() {
    DataWriterAccumulators accumulators = new DataWriterAccumulators(spark);
    SparkDataWriterTaskTracker tracker = new SparkDataWriterTaskTracker(accumulators);

    tracker.trackUncompressedValueSize(100);
    Assert.assertEquals(tracker.getTotalUncompressedValueSize(), 100);

    tracker.trackUncompressedValueSize(100);
    Assert.assertEquals(tracker.getTotalUncompressedValueSize(), 200);

    DataWriterAccumulators expectedAccumulators = new DataWriterAccumulators(spark);
    expectedAccumulators.uncompressedValueSizeCounter.add(200);

    verifyAllAccumulators(accumulators, expectedAccumulators);
  }

  @Test
  public void testCompressedValueSize() {
    DataWriterAccumulators accumulators = new DataWriterAccumulators(spark);
    SparkDataWriterTaskTracker tracker = new SparkDataWriterTaskTracker(accumulators);

    tracker.trackCompressedValueSize(100);
    Assert.assertEquals(tracker.getTotalValueSize(), 100);

    tracker.trackCompressedValueSize(100);
    Assert.assertEquals(tracker.getTotalValueSize(), 200);

    DataWriterAccumulators expectedAccumulators = new DataWriterAccumulators(spark);
    expectedAccumulators.compressedValueSizeCounter.add(200);

    verifyAllAccumulators(accumulators, expectedAccumulators);
  }

  @Test
  public void testGzipCompressedValueSize() {
    DataWriterAccumulators accumulators = new DataWriterAccumulators(spark);
    SparkDataWriterTaskTracker tracker = new SparkDataWriterTaskTracker(accumulators);

    tracker.trackGzipCompressedValueSize(100);
    Assert.assertEquals(tracker.getTotalGzipCompressedValueSize(), 100);

    tracker.trackGzipCompressedValueSize(100);
    Assert.assertEquals(tracker.getTotalGzipCompressedValueSize(), 200);

    DataWriterAccumulators expectedAccumulators = new DataWriterAccumulators(spark);
    expectedAccumulators.gzipCompressedValueSizeCounter.add(200);

    verifyAllAccumulators(accumulators, expectedAccumulators);
  }

  @Test
  public void testZstdCompressedValueSize() {
    DataWriterAccumulators accumulators = new DataWriterAccumulators(spark);
    SparkDataWriterTaskTracker tracker = new SparkDataWriterTaskTracker(accumulators);

    tracker.trackZstdCompressedValueSize(100);
    Assert.assertEquals(tracker.getTotalZstdCompressedValueSize(), 100);

    tracker.trackZstdCompressedValueSize(100);
    Assert.assertEquals(tracker.getTotalZstdCompressedValueSize(), 200);

    DataWriterAccumulators expectedAccumulators = new DataWriterAccumulators(spark);
    expectedAccumulators.zstdCompressedValueSizeCounter.add(200);

    verifyAllAccumulators(accumulators, expectedAccumulators);
  }

  @Test
  public void testWriteAclAuthorizationFailure() {
    DataWriterAccumulators accumulators = new DataWriterAccumulators(spark);
    SparkDataWriterTaskTracker tracker = new SparkDataWriterTaskTracker(accumulators);

    tracker.trackWriteAclAuthorizationFailure();
    tracker.trackWriteAclAuthorizationFailure();
    Assert.assertEquals(tracker.getWriteAclAuthorizationFailureCount(), 2);

    DataWriterAccumulators expectedAccumulators = new DataWriterAccumulators(spark);
    expectedAccumulators.writeAclAuthorizationFailureCounter.add(2);

    verifyAllAccumulators(accumulators, expectedAccumulators);
  }

  @Test
  public void testRecordTooLargeFailure() {
    DataWriterAccumulators accumulators = new DataWriterAccumulators(spark);
    SparkDataWriterTaskTracker tracker = new SparkDataWriterTaskTracker(accumulators);

    tracker.trackRecordTooLargeFailure();
    tracker.trackRecordTooLargeFailure();
    Assert.assertEquals(tracker.getRecordTooLargeFailureCount(), 2);

    DataWriterAccumulators expectedAccumulators = new DataWriterAccumulators(spark);
    expectedAccumulators.recordTooLargeFailureCounter.add(2);

    verifyAllAccumulators(accumulators, expectedAccumulators);
  }

  @Test
  public void testRecordSentToPubSub() {
    DataWriterAccumulators accumulators = new DataWriterAccumulators(spark);
    SparkDataWriterTaskTracker tracker = new SparkDataWriterTaskTracker(accumulators);

    tracker.trackRecordSentToPubSub();
    Assert.assertEquals(tracker.getOutputRecordsCount(), 1);

    DataWriterAccumulators expectedAccumulators = new DataWriterAccumulators(spark);
    expectedAccumulators.outputRecordCounter.add(1);

    verifyAllAccumulators(accumulators, expectedAccumulators);
  }

  @Test
  public void testDuplicateKeyWithDistinctValue() {
    DataWriterAccumulators accumulators = new DataWriterAccumulators(spark);
    SparkDataWriterTaskTracker tracker = new SparkDataWriterTaskTracker(accumulators);

    tracker.trackDuplicateKeyWithDistinctValue(10);
    Assert.assertEquals(tracker.getDuplicateKeyWithDistinctValueCount(), 10);
    tracker.trackDuplicateKeyWithDistinctValue(20);
    Assert.assertEquals(tracker.getDuplicateKeyWithDistinctValueCount(), 30);

    DataWriterAccumulators expectedAccumulators = new DataWriterAccumulators(spark);
    expectedAccumulators.duplicateKeyWithDistinctValueCounter.add(30);

    verifyAllAccumulators(accumulators, expectedAccumulators);
  }

  @Test
  public void testDuplicateKeyWithIdenticalValue() {
    DataWriterAccumulators accumulators = new DataWriterAccumulators(spark);
    SparkDataWriterTaskTracker tracker = new SparkDataWriterTaskTracker(accumulators);

    tracker.trackDuplicateKeyWithIdenticalValue(10);
    tracker.trackDuplicateKeyWithIdenticalValue(20);

    DataWriterAccumulators expectedAccumulators = new DataWriterAccumulators(spark);
    expectedAccumulators.duplicateKeyWithIdenticalValueCounter.add(30);

    verifyAllAccumulators(accumulators, expectedAccumulators);
  }

  @Test
  public void testRepushTtlFilteredRecord() {
    DataWriterAccumulators accumulators = new DataWriterAccumulators(spark);
    SparkDataWriterTaskTracker tracker = new SparkDataWriterTaskTracker(accumulators);

    tracker.trackRepushTtlFilteredRecord();
    tracker.trackRepushTtlFilteredRecord();

    Assert.assertEquals(tracker.getRepushTtlFilterCount(), 2);

    DataWriterAccumulators expectedAccumulators = new DataWriterAccumulators(spark);
    expectedAccumulators.repushTtlFilteredRecordCounter.add(2);

    verifyAllAccumulators(accumulators, expectedAccumulators);
  }

  @Test
  public void testPartitionWriterClose() {
    DataWriterAccumulators accumulators = new DataWriterAccumulators(spark);
    SparkDataWriterTaskTracker tracker = new SparkDataWriterTaskTracker(accumulators);

    tracker.trackPartitionWriterClose();
    tracker.trackPartitionWriterClose();

    Assert.assertEquals(tracker.getPartitionWriterCloseCount(), 2);

    DataWriterAccumulators expectedAccumulators = new DataWriterAccumulators(spark);
    expectedAccumulators.partitionWriterCloseCounter.add(2);

    verifyAllAccumulators(accumulators, expectedAccumulators);
  }

  // Verify values of all accumulators to ensure that they don't get updated through side effects
  private void verifyAllAccumulators(DataWriterAccumulators actual, DataWriterAccumulators expected) {
    Assert.assertEquals(
        actual.sprayAllPartitionsTriggeredCount.value(),
        expected.sprayAllPartitionsTriggeredCount.value());
    Assert.assertEquals(actual.emptyRecordCounter.value(), expected.emptyRecordCounter.value());
    Assert.assertEquals(actual.totalKeySizeCounter.value(), expected.totalKeySizeCounter.value());
    Assert.assertEquals(actual.uncompressedValueSizeCounter.value(), expected.uncompressedValueSizeCounter.value());
    Assert.assertEquals(actual.compressedValueSizeCounter.value(), expected.compressedValueSizeCounter.value());
    Assert.assertEquals(actual.gzipCompressedValueSizeCounter.value(), expected.gzipCompressedValueSizeCounter.value());
    Assert.assertEquals(actual.zstdCompressedValueSizeCounter.value(), expected.zstdCompressedValueSizeCounter.value());
    Assert.assertEquals(actual.outputRecordCounter.value(), expected.outputRecordCounter.value());
    Assert.assertEquals(
        actual.duplicateKeyWithIdenticalValueCounter.value(),
        expected.duplicateKeyWithIdenticalValueCounter.value());
    Assert.assertEquals(
        actual.writeAclAuthorizationFailureCounter.value(),
        expected.writeAclAuthorizationFailureCounter.value());
    Assert.assertEquals(actual.recordTooLargeFailureCounter.value(), expected.recordTooLargeFailureCounter.value());
    Assert.assertEquals(
        actual.duplicateKeyWithDistinctValueCounter.value(),
        expected.duplicateKeyWithDistinctValueCounter.value());
    Assert.assertEquals(actual.partitionWriterCloseCounter.value(), expected.partitionWriterCloseCounter.value());
    Assert.assertEquals(actual.repushTtlFilteredRecordCounter.value(), expected.repushTtlFilteredRecordCounter.value());
  }
}
