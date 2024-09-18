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
  public final LongAccumulator compressedValueSizeCounter;
  public final LongAccumulator gzipCompressedValueSizeCounter;
  public final LongAccumulator zstdCompressedValueSizeCounter;
  public final LongAccumulator outputRecordCounter;
  public final LongAccumulator duplicateKeyWithIdenticalValueCounter;
  public final LongAccumulator writeAclAuthorizationFailureCounter;
  public final LongAccumulator recordTooLargeFailureCounter;
  public final LongAccumulator duplicateKeyWithDistinctValueCounter;
  public final LongAccumulator partitionWriterCloseCounter;
  public final LongAccumulator repushTtlFilteredRecordCounter;

  public DataWriterAccumulators(SparkSession session) {
    SparkContext sparkContext = session.sparkContext();
    sprayAllPartitionsTriggeredCount = sparkContext.longAccumulator("Spray All Partitions Triggered");
    emptyRecordCounter = sparkContext.longAccumulator("Empty Records");
    totalKeySizeCounter = sparkContext.longAccumulator("Total Key Size");
    uncompressedValueSizeCounter = sparkContext.longAccumulator("Total Uncompressed Value Size");
    compressedValueSizeCounter = sparkContext.longAccumulator("Total Compressed Value Size");
    gzipCompressedValueSizeCounter = sparkContext.longAccumulator("Total Gzip Compressed Value Size");
    zstdCompressedValueSizeCounter = sparkContext.longAccumulator("Total Zstd Compressed Value Size");
    outputRecordCounter = sparkContext.longAccumulator("Total Output Records");
    partitionWriterCloseCounter = sparkContext.longAccumulator("Partition Writers Closed");
    repushTtlFilteredRecordCounter = sparkContext.longAccumulator("Repush TTL Filtered Records");
    writeAclAuthorizationFailureCounter = sparkContext.longAccumulator("ACL Authorization Failures");
    recordTooLargeFailureCounter = sparkContext.longAccumulator("Record Too Large Failures");
    duplicateKeyWithIdenticalValueCounter = sparkContext.longAccumulator("Duplicate Key With Identical Value");
    duplicateKeyWithDistinctValueCounter = sparkContext.longAccumulator("Duplicate Key With Distinct Value");
  }
}
