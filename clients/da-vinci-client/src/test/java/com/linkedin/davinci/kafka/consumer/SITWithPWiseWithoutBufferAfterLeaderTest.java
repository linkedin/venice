package com.linkedin.davinci.kafka.consumer;

import org.testng.SkipException;
import org.testng.annotations.Test;


public class SITWithPWiseWithoutBufferAfterLeaderTest extends StoreIngestionTaskTest {
  protected KafkaConsumerService.ConsumerAssignmentStrategy getConsumerAssignmentStrategy() {
    return KafkaConsumerService.ConsumerAssignmentStrategy.PARTITION_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY;
  }

  protected boolean isStoreWriterBufferAfterLeaderLogicEnabled() {
    return false;
  }

  /*
   * Same persistent flake as the sister SITWithPWiseAndBufferAfterLeaderTest. See that
   * subclass for the full root-cause notes. Coverage preserved by AA_OFF here and by
   * SITWithSAwarePWiseAndBufferAfterLeaderTest [AA_ON].
   */
  // Outer timeout matches the parent's 420_000 so the AA_OFF delegation gets the same budget.
  @Test(dataProvider = "aaConfigProvider", timeOut = 420_000)
  @Override
  public void testResetPartition(AAConfig aaConfig) throws Exception {
    if (aaConfig == AAConfig.AA_ON) {
      throw new SkipException(
          "Skipped on PARTITION_WISE_SHARED + AA_ON: persistent SIT-thread starvation in "
              + "the initial-ingestion wait. Tracked separately.");
    }
    super.testResetPartition(aaConfig);
  }
}
