package com.linkedin.venice.endToEnd;

/**
 * Integration test to verify active/active replication when applying read-write leader compaction optimization
 * and parallel processing of AA/WC workload.
 */
public class TestActiveActiveIngestionWithReadWriteLeaderCompactionTuningAndParallelProcessing
    extends TestActiveActiveIngestion {
  @Override
  protected boolean isLevel0CompactionTuningForReadWriteLeaderEnabled() {
    return true;
  }

  @Override
  protected boolean isAAWCParallelProcessingEnabled() {
    return true;
  }
}
