package com.linkedin.venice.stats.opentelemetrydimensions;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;


public class VeniceRequestRetryTypeTest {
  @Test
  public void testVeniceRequestRetryType() {
    for (VeniceRequestRetryType retryType: VeniceRequestRetryType.values()) {
      switch (retryType) {
        case ERROR_RETRY:
          assertEquals(retryType.getRetryType(), "error_retry");
          break;
        case LONG_TAIL_RETRY:
          assertEquals(retryType.getRetryType(), "long_tail_retry");
          break;
        default:
          throw new IllegalArgumentException("Unknown retry type: " + retryType);
      }
    }
  }
}
