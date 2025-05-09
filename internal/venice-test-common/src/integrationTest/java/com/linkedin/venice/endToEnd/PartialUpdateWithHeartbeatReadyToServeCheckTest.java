package com.linkedin.venice.endToEnd;

public class PartialUpdateWithHeartbeatReadyToServeCheckTest extends PartialUpdateTest {
  @Override
  protected boolean isHeartbeatReadyToServeCheckEnabled() {
    return true;
  }
}
