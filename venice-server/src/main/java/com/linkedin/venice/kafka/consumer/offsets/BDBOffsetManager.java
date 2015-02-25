package com.linkedin.venice.kafka.consumer.offsets;

public class BDBOffsetManager implements OffsetManager {

  //TODO later add implementations and constructors.

  @Override
  public void recordOffset(String topicName, int partitionId, long offset) {
    //TODO later add code here
  }

  @Override
  public long getLastOffset(String topicName, int partitionId) {
    // TODO later add code here
    return 0;
  }
}
