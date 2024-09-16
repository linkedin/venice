package com.linkedin.venice.consumer;

public class TestChangelogConsumerWithParallelProcessing extends TestChangelogConsumer {
  @Override
  protected boolean isAAWCParallelProcessingEnabled() {
    return true;
  }
}
