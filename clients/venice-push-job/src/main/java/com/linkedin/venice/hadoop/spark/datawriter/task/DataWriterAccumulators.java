package com.linkedin.venice.hadoop.spark.datawriter.task;

import java.io.Serializable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;


public class DataWriterAccumulators implements Serializable {
  private static final long serialVersionUID = 1L;

  final LongAccumulator sprayAllPartitionsTriggeredCount;
  final LongAccumulator emptyRecordCounter;
  final LongAccumulator totalKeySizeCounter;
  final LongAccumulator uncompressedValueSizeCounter;
  final LongAccumulator compressedValueSizeCounter;
  final LongAccumulator gzipCompressedValueSizeCounter;
  final LongAccumulator zstdCompressedValueSizeCounter;
  final LongAccumulator outputRecordCounter;
  final LongAccumulator duplicateKeyWithIdenticalValueCounter;
  final LongAccumulator writeAclAuthorizationFailureCounter;
  final LongAccumulator recordTooLargeFailureCounter;
  final LongAccumulator duplicateKeyWithDistinctValueCounter;
  final LongAccumulator partitionWriterCloseCounter;
  final LongAccumulator repushTtlFilteredRecordCounter;

  public DataWriterAccumulators(SparkSession session) {
    sprayAllPartitionsTriggeredCount = session.sparkContext().longAccumulator("Spray All Partitions Triggered");
    emptyRecordCounter = session.sparkContext().longAccumulator("Empty Records");
    totalKeySizeCounter = session.sparkContext().longAccumulator("Total Key Size");
    uncompressedValueSizeCounter = session.sparkContext().longAccumulator("Total Uncompressed Value Size");
    compressedValueSizeCounter = session.sparkContext().longAccumulator("Total Compressed Value Size");
    gzipCompressedValueSizeCounter = session.sparkContext().longAccumulator("Total Gzip Compressed Value Size");
    zstdCompressedValueSizeCounter = session.sparkContext().longAccumulator("Total Zstd Compressed Value Size");
    outputRecordCounter = session.sparkContext().longAccumulator("Total Output Records");
    partitionWriterCloseCounter = session.sparkContext().longAccumulator("Partition Writers Closed");
    repushTtlFilteredRecordCounter = session.sparkContext().longAccumulator("Repush TTL Filtered Records");
    writeAclAuthorizationFailureCounter = session.sparkContext().longAccumulator("ACL Authorization Failures");
    recordTooLargeFailureCounter = session.sparkContext().longAccumulator("Record Too Large Failures");
    duplicateKeyWithIdenticalValueCounter =
        session.sparkContext().longAccumulator("Duplicate Key With Identical Value");
    duplicateKeyWithDistinctValueCounter = session.sparkContext().longAccumulator("Duplicate Key With Distinct Value");
  }
}
