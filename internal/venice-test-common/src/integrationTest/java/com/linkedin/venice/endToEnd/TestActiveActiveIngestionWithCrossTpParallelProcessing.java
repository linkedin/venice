package com.linkedin.venice.endToEnd;

/**
 * Integration test to verify active/active replication when cross-TP parallel processing is enabled.
 * This feature parallelizes the processing of topic-partitions within ConsumptionTask to prevent
 * slow TPs from blocking others in the same poll batch.
 */
public class TestActiveActiveIngestionWithCrossTpParallelProcessing extends TestActiveActiveIngestion {
  @Override
  protected boolean isCrossTpParallelProcessingEnabled() {
    return true;
  }
}
