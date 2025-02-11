package com.linkedin.venice.D2;

import org.testng.annotations.Test;


public class D2ClientUtilsTest {
  @Test
  public void testClose() {
    // Should not throw
    D2ClientUtils.shutdownClient(null);
  }
}
