package com.linkedin.davinci.kafka.consumer;

import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.davinci.external.LegacyStoreBufferService;
import com.linkedin.venice.utils.TestUtils;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.Test;


public class AbstractStoreBufferServiceCompatibilityTest {
  @Test
  public void testExternalLegacyImplementationRetainsOldContract() throws Exception {
    AbstractStoreBufferService buffer = new LegacyStoreBufferService();
    assertSame(
        TestUtils.getStoreIngestionTaskBuilder("test_store").setStoreBufferService(buffer).getStoreBufferService(),
        buffer);
    assertTrue(
        buffer.execSyncOffsetFromSnapshotAsync(null, null, CompletableFuture.completedFuture(null), null).isDone());
    UnsupportedOperationException failure = expectThrows(
        UnsupportedOperationException.class,
        () -> buffer.execSyncOffsetFromSnapshotAsync(null, null, CompletableFuture.completedFuture(null), null, null));
    assertTrue(failure.getMessage().contains("does not support ordered time-based checkpoint snapshots"));
  }
}
