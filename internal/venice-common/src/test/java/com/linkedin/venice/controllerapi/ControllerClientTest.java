package com.linkedin.venice.controllerapi;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.concurrent.atomic.AtomicInteger;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ControllerClientTest {
  /**
   * A transient failure on an earlier attempt must not mask a later successful attempt: the retry state is cleared
   * at the start of every attempt, so the first success is returned rather than the run throwing on a stale failure.
   */
  @Test
  public void testRetryableRequestReturnsSuccessAfterTransientFailure() {
    ControllerClient client = Mockito.mock(ControllerClient.class);
    ControllerResponse success = new ControllerResponse();
    AtomicInteger attempts = new AtomicInteger(0);

    // Zero backoff keeps the retry loop instant under test.
    ControllerResponse result = ControllerClient.retryableRequest(client, 3, 0, c -> {
      if (attempts.getAndIncrement() == 0) {
        throw new VeniceException("transient failure on first attempt");
      }
      return success;
    }, r -> false);

    Assert.assertSame(result, success);
    Assert.assertFalse(result.isError());
    Assert.assertEquals(attempts.get(), 2, "Should stop retrying as soon as an attempt succeeds");
  }

  @Test
  public void testRetryableRequestThrowsAfterAllAttemptsFail() {
    ControllerClient client = Mockito.mock(ControllerClient.class);
    AtomicInteger attempts = new AtomicInteger(0);

    Assert.assertThrows(VeniceException.class, () -> ControllerClient.retryableRequest(client, 3, 0, c -> {
      attempts.incrementAndGet();
      throw new VeniceException("persistent failure");
    }, r -> false));

    Assert.assertEquals(attempts.get(), 3, "Should exhaust all attempts before throwing");
  }
}
