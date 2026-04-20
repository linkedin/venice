package com.linkedin.venice.controller;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link VeniceHelixAdmin#checkBackupVersionCleanupCapacityForNewPush}.
 */
public class TestBackupVersionCleanupCapacityCheck {
  private static final String CLUSTER_NAME = "test-cluster";
  private static final String STORE_NAME = "test-store";
  private static final int MIN_VERSIONS_TO_PRESERVE = 2;
  private static final long MIN_CLEANUP_DELAY_MS = TimeUnit.HOURS.toMillis(1);

  @Test
  public void testDeleteOnNewPushStart_UsesNMinusOnePreserveCountAndPassesWhenNoVersionsPending() {
    // DELETE_ON_NEW_PUSH_START: after SOP cleanup we expect only current + new push.
    // Preserve count passed to retrieveVersionsToDelete should be N-1 (= 1).
    Store store = mock(Store.class);
    doReturn(Collections.emptyList()).when(store).retrieveVersionsToDelete(MIN_VERSIONS_TO_PRESERVE - 1);

    VeniceHelixAdmin.checkBackupVersionCleanupCapacityForNewPush(
        CLUSTER_NAME,
        STORE_NAME,
        store,
        BackupStrategy.DELETE_ON_NEW_PUSH_START,
        MIN_VERSIONS_TO_PRESERVE,
        MIN_CLEANUP_DELAY_MS,
        System.currentTimeMillis());

    verify(store).retrieveVersionsToDelete(MIN_VERSIONS_TO_PRESERVE - 1);
    // When nothing is pending, we short-circuit and never check the promotion timestamp.
    verify(store, never()).getLatestVersionPromoteToCurrentTimestamp();
  }

  @Test
  public void testKeepMinVersions_UsesNPreserveCountAndPassesWhenNoVersionsPending() {
    // KEEP_MIN_VERSIONS: preserve N at steady state, N+1 during push.
    // Preserve count passed to retrieveVersionsToDelete should be N (= 2), not N-1.
    Store store = mock(Store.class);
    doReturn(Collections.emptyList()).when(store).retrieveVersionsToDelete(MIN_VERSIONS_TO_PRESERVE);

    VeniceHelixAdmin.checkBackupVersionCleanupCapacityForNewPush(
        CLUSTER_NAME,
        STORE_NAME,
        store,
        BackupStrategy.KEEP_MIN_VERSIONS,
        MIN_VERSIONS_TO_PRESERVE,
        MIN_CLEANUP_DELAY_MS,
        System.currentTimeMillis());

    verify(store).retrieveVersionsToDelete(MIN_VERSIONS_TO_PRESERVE);
    verify(store, never()).retrieveVersionsToDelete(MIN_VERSIONS_TO_PRESERVE - 1);
    verify(store, never()).getLatestVersionPromoteToCurrentTimestamp();
  }

  @Test
  public void testPassesWhenPastMinCleanupDelayEvenWithPendingVersions() {
    // Pending versions exist but promotion was > min delay ago → push allowed.
    Store store = mock(Store.class);
    Version pendingVersion = mock(Version.class);
    doReturn(Collections.singletonList(pendingVersion)).when(store)
        .retrieveVersionsToDelete(MIN_VERSIONS_TO_PRESERVE - 1);
    long now = System.currentTimeMillis();
    long promotionTime = now - MIN_CLEANUP_DELAY_MS - 1;
    doReturn(promotionTime).when(store).getLatestVersionPromoteToCurrentTimestamp();

    VeniceHelixAdmin.checkBackupVersionCleanupCapacityForNewPush(
        CLUSTER_NAME,
        STORE_NAME,
        store,
        BackupStrategy.DELETE_ON_NEW_PUSH_START,
        MIN_VERSIONS_TO_PRESERVE,
        MIN_CLEANUP_DELAY_MS,
        now);

    // Both reads should have happened — we checked versions pending, then checked the timestamp.
    verify(store).retrieveVersionsToDelete(MIN_VERSIONS_TO_PRESERVE - 1);
    verify(store).getLatestVersionPromoteToCurrentTimestamp();
  }

  @Test
  public void testDeleteOnNewPushStart_BlocksPushWhenWithinMinCleanupDelay() {
    // Rollback scenario: versions pending AND promotion was 30min ago (< 1h min delay).
    Store store = mock(Store.class);
    Version pendingVersion = mock(Version.class);
    doReturn(Collections.singletonList(pendingVersion)).when(store)
        .retrieveVersionsToDelete(MIN_VERSIONS_TO_PRESERVE - 1);
    long now = System.currentTimeMillis();
    doReturn(now - TimeUnit.MINUTES.toMillis(30)).when(store).getLatestVersionPromoteToCurrentTimestamp();

    VeniceException e = expectThrows(
        VeniceException.class,
        () -> VeniceHelixAdmin.checkBackupVersionCleanupCapacityForNewPush(
            CLUSTER_NAME,
            STORE_NAME,
            store,
            BackupStrategy.DELETE_ON_NEW_PUSH_START,
            MIN_VERSIONS_TO_PRESERVE,
            MIN_CLEANUP_DELAY_MS,
            now));
    assertTrue(e.getMessage().contains(STORE_NAME), "Error should include store name: " + e.getMessage());
    assertTrue(e.getMessage().contains(CLUSTER_NAME), "Error should include cluster name: " + e.getMessage());
    // Anchor on "1 backup version(s)" to avoid matching the digit 1 that could appear elsewhere in the message.
    assertTrue(
        e.getMessage().contains("1 backup version(s)"),
        "Error should include pending version count: " + e.getMessage());
    assertTrue(
        e.getMessage().contains(String.valueOf(MIN_CLEANUP_DELAY_MS)),
        "Error should include the min cleanup delay value: " + e.getMessage());
  }

  @Test
  public void testKeepMinVersions_BlocksPushWhenWithinMinCleanupDelay() {
    // Same rollback scenario but with KEEP_MIN_VERSIONS strategy and multiple pending versions.
    Store store = mock(Store.class);
    Version v1 = mock(Version.class);
    Version v2 = mock(Version.class);
    doReturn(java.util.Arrays.asList(v1, v2)).when(store).retrieveVersionsToDelete(MIN_VERSIONS_TO_PRESERVE);
    long now = System.currentTimeMillis();
    doReturn(now - TimeUnit.MINUTES.toMillis(30)).when(store).getLatestVersionPromoteToCurrentTimestamp();

    VeniceException e = expectThrows(
        VeniceException.class,
        () -> VeniceHelixAdmin.checkBackupVersionCleanupCapacityForNewPush(
            CLUSTER_NAME,
            STORE_NAME,
            store,
            BackupStrategy.KEEP_MIN_VERSIONS,
            MIN_VERSIONS_TO_PRESERVE,
            MIN_CLEANUP_DELAY_MS,
            now));
    assertTrue(e.getMessage().contains(STORE_NAME));
    // Anchor on "2 backup version(s)" to avoid matching the digit 2 that could appear elsewhere in the message.
    assertTrue(
        e.getMessage().contains("2 backup version(s)"),
        "Error should report the 2 pending versions: " + e.getMessage());
  }

  @Test
  public void testBlocksExactlyAtMinCleanupDelayBoundary() {
    // At the boundary (currentTime == promotionTime + minDelay), the check is <= so it blocks.
    Store store = mock(Store.class);
    Version pendingVersion = mock(Version.class);
    doReturn(Collections.singletonList(pendingVersion)).when(store)
        .retrieveVersionsToDelete(MIN_VERSIONS_TO_PRESERVE - 1);
    long now = System.currentTimeMillis();
    doReturn(now - MIN_CLEANUP_DELAY_MS).when(store).getLatestVersionPromoteToCurrentTimestamp();

    assertThrows(
        VeniceException.class,
        () -> VeniceHelixAdmin.checkBackupVersionCleanupCapacityForNewPush(
            CLUSTER_NAME,
            STORE_NAME,
            store,
            BackupStrategy.DELETE_ON_NEW_PUSH_START,
            MIN_VERSIONS_TO_PRESERVE,
            MIN_CLEANUP_DELAY_MS,
            now));
  }

  @Test
  public void testJustPastBoundaryAllowsPush() {
    // One ms past the boundary should allow the push.
    Store store = mock(Store.class);
    Version pendingVersion = mock(Version.class);
    doReturn(Collections.singletonList(pendingVersion)).when(store)
        .retrieveVersionsToDelete(MIN_VERSIONS_TO_PRESERVE - 1);
    long now = System.currentTimeMillis();
    doReturn(now - MIN_CLEANUP_DELAY_MS - 1).when(store).getLatestVersionPromoteToCurrentTimestamp();

    // Should not throw
    VeniceHelixAdmin.checkBackupVersionCleanupCapacityForNewPush(
        CLUSTER_NAME,
        STORE_NAME,
        store,
        BackupStrategy.DELETE_ON_NEW_PUSH_START,
        MIN_VERSIONS_TO_PRESERVE,
        MIN_CLEANUP_DELAY_MS,
        now);
    verify(store).retrieveVersionsToDelete(MIN_VERSIONS_TO_PRESERVE - 1);
    verify(store).getLatestVersionPromoteToCurrentTimestamp();
  }

  @Test
  public void testDeleteOnNewPushStart_ClampsPreserveCountToAtLeastOneWhenMinIsOne() {
    // If cluster config sets minNumberOfStoreVersionsToPreserve == 1, the DELETE_ON_NEW_PUSH_START branch
    // would compute N-1 = 0, which Store.retrieveVersionsToDelete rejects with IllegalArgumentException.
    // Verify the check clamps to 1 so the push path doesn't break.
    Store store = mock(Store.class);
    doReturn(Collections.emptyList()).when(store).retrieveVersionsToDelete(1);

    VeniceHelixAdmin.checkBackupVersionCleanupCapacityForNewPush(
        CLUSTER_NAME,
        STORE_NAME,
        store,
        BackupStrategy.DELETE_ON_NEW_PUSH_START,
        1, // cluster config edge case
        MIN_CLEANUP_DELAY_MS,
        System.currentTimeMillis());

    verify(store).retrieveVersionsToDelete(1);
    verify(store, never()).retrieveVersionsToDelete(0);
  }
}
