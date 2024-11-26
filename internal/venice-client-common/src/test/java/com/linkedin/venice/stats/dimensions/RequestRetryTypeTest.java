package com.linkedin.venice.stats.dimensions;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;


public class RequestRetryTypeTest {
  @Test
  public void testVeniceRequestRetryType() {
    for (RequestRetryType retryType: RequestRetryType.values()) {
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
