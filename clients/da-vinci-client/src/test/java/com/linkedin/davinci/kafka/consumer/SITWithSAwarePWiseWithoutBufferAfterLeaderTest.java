package com.linkedin.davinci.kafka.consumer;

import org.testng.SkipException;
import org.testng.annotations.Test;


public class SITWithSAwarePWiseWithoutBufferAfterLeaderTest extends StoreIngestionTaskTest {
  protected KafkaConsumerService.ConsumerAssignmentStrategy getConsumerAssignmentStrategy() {
    return KafkaConsumerService.ConsumerAssignmentStrategy.STORE_AWARE_PARTITION_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY;
  }

  protected boolean isStoreWriterBufferAfterLeaderLogicEnabled() {
    return false;
  }

  /*
   * Newly flaking on AA_ON in CI run 25763732120 (shard 8): 180.229 s with only
   * `getPartitionOrThrow(1)` recorded on the mock storage engine. Same family as the
   * PWise variants (see SITWithPWiseAndBufferAfterLeaderTest for full root-cause notes);
   * surfaces here when the SAware strategy is combined with the WithoutBuffer config.
   * Coverage preserved by AA_OFF here and by SITWithSAwarePWiseAndBufferAfterLeaderTest
   * [AA_ON], which still passes deterministically in the same run.
   */
  // Outer timeout matches the parent's 420_000 so the AA_OFF delegation gets the same budget.
  @Test(dataProvider = "aaConfigProvider", timeOut = 420_000)
  @Override
  public void testResetPartition(AAConfig aaConfig) throws Exception {
    if (aaConfig == AAConfig.AA_ON) {
      throw new SkipException(
          "Skipped on STORE_AWARE_PARTITION_WISE_SHARED + WithoutBuffer + AA_ON: persistent "
              + "SIT-thread starvation in the initial-ingestion wait. Tracked separately.");
    }
    super.testResetPartition(aaConfig);
  }
}
