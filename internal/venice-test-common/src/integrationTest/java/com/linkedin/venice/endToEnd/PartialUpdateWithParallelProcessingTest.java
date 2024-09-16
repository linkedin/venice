package com.linkedin.venice.endToEnd;

public class PartialUpdateWithParallelProcessingTest extends PartialUpdateTest {
  @Override
  protected boolean isAAWCParallelProcessingEnabled() {
    return true;
  }
}
