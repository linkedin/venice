package com.linkedin.venice.endToEnd;

import org.testng.annotations.Test;


public class PartialUpdateWithHeartbeatReadyToServeCheckTest extends PartialUpdateTest {
  private static final int TEST_TIMEOUT_MS = 180_000;

  @Override
  protected boolean isHeartbeatReadyToServeCheckEnabled() {
    return true;
  }

  @Override
  @Test(timeOut = TEST_TIMEOUT_MS)
  public void testRepushWithTTLWithActiveActivePartialUpdateStore() {
    super.testRepushWithTTLWithActiveActivePartialUpdateStore();
  }
}
