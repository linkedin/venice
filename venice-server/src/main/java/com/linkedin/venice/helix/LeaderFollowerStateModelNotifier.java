package com.linkedin.venice.helix;


public class LeaderFollowerStateModelNotifier extends StateModelNotifier {
  @Override
  public void catchUpBaseTopicOffsetLag(String kafkaTopic, int partitionId) {
    if (getLatch(kafkaTopic, partitionId) != null) {
      getLatch(kafkaTopic, partitionId).countDown();
    }
  }
}
