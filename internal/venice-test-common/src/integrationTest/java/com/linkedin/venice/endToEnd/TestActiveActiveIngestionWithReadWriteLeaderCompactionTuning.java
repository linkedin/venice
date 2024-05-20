package com.linkedin.venice.endToEnd;

/**
 * Integration test to verify active/active replication when applying read-write leader compaction optimization.
 */
public class TestActiveActiveIngestionWithReadWriteLeaderCompactionTuning extends TestActiveActiveIngestion {
  @Override
  protected boolean isLevel0CompactionTuningForReadWriteLeaderEnabled() {
    return true;
  }
}
