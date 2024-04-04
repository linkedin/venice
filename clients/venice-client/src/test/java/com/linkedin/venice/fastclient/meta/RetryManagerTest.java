package com.linkedin.venice.fastclient.meta;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.utils.TestUtils;
import java.io.IOException;
import java.time.Clock;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RetryManagerTest {
  private static final long TEST_TIMEOUT_IN_MS = 10000;

  @Test(timeOut = TEST_TIMEOUT_IN_MS)
  public void testRetryManagerDisabled() throws IOException {
    Clock mockClock = mock(Clock.class);
    long start = System.currentTimeMillis();
    doReturn(start).when(mockClock).millis();
    RetryManager retryManager = new RetryManager(0, 0.1d, mockClock);
    retryManager.recordRequest();
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(retryManager.isRetryAllowed());
    }
    Assert.assertNull(retryManager.getRetryTokenBucket());
    retryManager.close();
  }

  @Test(timeOut = TEST_TIMEOUT_IN_MS)
  public void testRetryManager() throws IOException {
    Clock mockClock = mock(Clock.class);
    long start = System.currentTimeMillis();
    doReturn(start).when(mockClock).millis();
    try (RetryManager retryManager = new RetryManager(1000, 0.1d, mockClock)) {
      doReturn(start + 1000).when(mockClock).millis();
      for (int i = 0; i < 50; i++) {
        retryManager.recordRequest();
      }
      TestUtils.waitForNonDeterministicAssertion(
          5,
          TimeUnit.SECONDS,
          () -> Assert.assertNotNull(retryManager.getRetryTokenBucket()));
      // The retry budget should be set to 50 * 0.1 = 5
      // With refill interval of 1s and capacity multiple of 5 that makes the token bucket capacity of 25
      for (int i = 0; i < 25; i++) {
        Assert.assertTrue(retryManager.isRetryAllowed());
      }
      Assert.assertFalse(retryManager.isRetryAllowed());
      doReturn(start + 2001).when(mockClock).millis();
      // We should eventually be able to perform retries again
      TestUtils.waitForNonDeterministicAssertion(
          5,
          TimeUnit.SECONDS,
          () -> Assert.assertTrue(retryManager.isRetryAllowed()));
    }
  }
}
