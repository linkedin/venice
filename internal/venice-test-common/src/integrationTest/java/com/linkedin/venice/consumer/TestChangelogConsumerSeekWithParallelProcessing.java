package com.linkedin.venice.consumer;

public class TestChangelogConsumerSeekWithParallelProcessing extends TestChangelogConsumerSeek {
  @Override
  protected boolean isAAWCParallelProcessingEnabled() {
    return true;
  }
}
