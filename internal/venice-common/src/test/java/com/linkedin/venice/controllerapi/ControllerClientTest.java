package com.linkedin.venice.controllerapi;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.StorageMode;
import com.linkedin.venice.meta.VersionStorageModeUpdateReason;
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

  /**
   * Callers written against the pre-existing overloads must keep compiling and must land on the same request the
   * controller already understands, with the reason defaulted rather than invented.
   */
  @Test
  public void testUpdateStoreVersionStorageModeWithoutReasonDefaultsToUnspecified() {
    ControllerClient client = Mockito.mock(ControllerClient.class);
    Mockito.doCallRealMethod()
        .when(client)
        .updateStoreVersionStorageMode(Mockito.anyString(), Mockito.anyInt(), Mockito.any());
    Mockito.doCallRealMethod()
        .when(client)
        .updateStoreVersionStorageMode(Mockito.anyString(), Mockito.anyInt(), Mockito.any(), Mockito.any());

    client.updateStoreVersionStorageMode("store", 1, StorageMode.INTERNAL);
    client.updateStoreVersionStorageMode("store", 1, StorageMode.INTERNAL, "dc-1");

    Mockito.verify(client)
        .updateStoreVersionStorageMode(
            "store",
            1,
            StorageMode.INTERNAL,
            null,
            VersionStorageModeUpdateReason.UNSPECIFIED);
    Mockito.verify(client)
        .updateStoreVersionStorageMode(
            "store",
            1,
            StorageMode.INTERNAL,
            "dc-1",
            VersionStorageModeUpdateReason.UNSPECIFIED);
  }

  @Test
  public void testVersionStorageModeUpdateReasonParsing() {
    Assert.assertEquals(
        VersionStorageModeUpdateReason.parseOrDefault("EXTERNAL_WRITE_FAILURE"),
        VersionStorageModeUpdateReason.EXTERNAL_WRITE_FAILURE);
    Assert.assertEquals(
        VersionStorageModeUpdateReason.parseOrDefault("external_write_failure"),
        VersionStorageModeUpdateReason.EXTERNAL_WRITE_FAILURE);
    // Absent, blank or unknown values must never fail the request; they simply are not alertable.
    Assert
        .assertEquals(VersionStorageModeUpdateReason.parseOrDefault(null), VersionStorageModeUpdateReason.UNSPECIFIED);
    Assert
        .assertEquals(VersionStorageModeUpdateReason.parseOrDefault("  "), VersionStorageModeUpdateReason.UNSPECIFIED);
    Assert.assertEquals(
        VersionStorageModeUpdateReason.parseOrDefault("SOMETHING_FROM_A_NEWER_CLIENT"),
        VersionStorageModeUpdateReason.UNSPECIFIED);
  }
}
