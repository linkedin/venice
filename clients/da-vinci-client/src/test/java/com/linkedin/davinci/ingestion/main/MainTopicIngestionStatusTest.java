package com.linkedin.davinci.ingestion.main;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;


public class MainTopicIngestionStatusTest {
  @Test
  public void testHasPartitionIngestingInIsolatedProcess() {
    String topicName = "test_store_v1";
    MainTopicIngestionStatus ingestionStatus = new MainTopicIngestionStatus(topicName);
    ingestionStatus.setPartitionIngestionStatusToLocalIngestion(1);
    ingestionStatus.setPartitionIngestionStatusToLocalIngestion(2);
    assertFalse(ingestionStatus.hasPartitionIngestingInIsolatedProcess());

    ingestionStatus.setPartitionIngestionStatusToIsolatedIngestion(3);
    assertTrue(ingestionStatus.hasPartitionIngestingInIsolatedProcess());

    ingestionStatus.removePartitionIngestionStatus(3);
    assertFalse(ingestionStatus.hasPartitionIngestingInIsolatedProcess());
  }
}
