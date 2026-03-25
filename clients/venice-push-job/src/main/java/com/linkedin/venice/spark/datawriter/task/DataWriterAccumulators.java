package com.linkedin.venice.spark.datawriter.task;

import java.io.Serializable;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;


/**
 * All the {@link org.apache.spark.util.AccumulatorV2} objects that are used in the Spark DataWriter jobs.
 */
public class DataWriterAccumulators implements Serializable {
  private static final long serialVersionUID = 1L;

  public final LongAccumulator sprayAllPartitionsTriggeredCount;
  public final LongAccumulator emptyRecordCounter;
  public final LongAccumulator totalKeySizeCounter;
  public final LongAccumulator uncompressedValueSizeCounter;
  public final MaxAccumulator<Integer> largestUncompressedValueSize;
  public final LongAccumulator compressedValueSizeCounter;
  public final LongAccumulator gzipCompressedValueSizeCounter;
  public final LongAccumulator zstdCompressedValueSizeCounter;
  public final LongAccumulator outputRecordCounter;
  public final LongAccumulator duplicateKeyWithIdenticalValueCounter;
  public final LongAccumulator writeAclAuthorizationFailureCounter;
  public final LongAccumulator recordTooLargeFailureCounter;
  public final LongAccumulator uncompressedRecordTooLargeFailureCounter;
  public final LongAccumulator duplicateKeyWithDistinctValueCounter;
  public final LongAccumulator partitionWriterCloseCounter;
  public final LongAccumulator repushTtlFilteredRecordCounter;
  public final LongAccumulator incrementalPushThrottleTimeCounter;
  public final LongAccumulator totalDuplicateKeyCounter;
  public final MapLongAccumulator perPartitionRecordCounts;
  /** Null when {@code repushHllVerificationEnabled} is false. */
  public final HyperLogLogAccumulator readSideHllAccumulator;
  /** Null when {@code repushHllVerificationEnabled} is false. */
  public final MapHyperLogLogAccumulator perPartitionReadSideHllAccumulator;

  public DataWriterAccumulators(SparkSession session) {
    this(session, false);
  }

  public DataWriterAccumulators(SparkSession session, boolean enableHllAccumulators) {
    SparkContext sparkContext = session.sparkContext();
    sprayAllPartitionsTriggeredCount = sparkContext.longAccumulator("Spray All Partitions Triggered");
    emptyRecordCounter = sparkContext.longAccumulator("Empty Records");
    totalKeySizeCounter = sparkContext.longAccumulator("Total Key Size");
    uncompressedValueSizeCounter = sparkContext.longAccumulator("Total Uncompressed Value Size");
    largestUncompressedValueSize =
        new MaxAccumulator<>(Integer.MIN_VALUE, sparkContext, "Largest uncompressed value size");
    compressedValueSizeCounter = sparkContext.longAccumulator("Total Compressed Value Size");
    gzipCompressedValueSizeCounter = sparkContext.longAccumulator("Total Gzip Compressed Value Size");
    zstdCompressedValueSizeCounter = sparkContext.longAccumulator("Total Zstd Compressed Value Size");
    outputRecordCounter = sparkContext.longAccumulator("Total Output Records");
    partitionWriterCloseCounter = sparkContext.longAccumulator("Partition Writers Closed");
    repushTtlFilteredRecordCounter = sparkContext.longAccumulator("Repush TTL Filtered Records");
    incrementalPushThrottleTimeCounter = sparkContext.longAccumulator("Incremental Push Throttle Time (ms)");
    writeAclAuthorizationFailureCounter = sparkContext.longAccumulator("ACL Authorization Failures");
    recordTooLargeFailureCounter = sparkContext.longAccumulator("Record Too Large Failures");
    uncompressedRecordTooLargeFailureCounter = sparkContext.longAccumulator("Uncompressed Record Too Large Failures");
    duplicateKeyWithIdenticalValueCounter = sparkContext.longAccumulator("Duplicate Key With Identical Value");
    duplicateKeyWithDistinctValueCounter = sparkContext.longAccumulator("Duplicate Key With Distinct Value");
    totalDuplicateKeyCounter = sparkContext.longAccumulator("Total Duplicate Keys (Compaction)");
    this.perPartitionRecordCounts = new MapLongAccumulator();
    sparkContext.register(perPartitionRecordCounts, "perPartitionRecordCounts");
    if (enableHllAccumulators) {
      this.readSideHllAccumulator = new HyperLogLogAccumulator();
      sparkContext.register(readSideHllAccumulator, "Repush Read-Side HLL Unique Key Count");
      this.perPartitionReadSideHllAccumulator = new MapHyperLogLogAccumulator();
      sparkContext.register(perPartitionReadSideHllAccumulator, "Repush Per-Partition Read-Side HLL");
    } else {
      this.readSideHllAccumulator = null;
      this.perPartitionReadSideHllAccumulator = null;
    }
  }
}
