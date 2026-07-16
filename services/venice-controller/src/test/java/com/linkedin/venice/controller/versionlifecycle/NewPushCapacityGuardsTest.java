package com.linkedin.venice.controller.versionlifecycle;

import static com.linkedin.venice.controller.versionlifecycle.VersionLifecycleTestSupport.CLUSTER_NAME;
import static com.linkedin.venice.controller.versionlifecycle.VersionLifecycleTestSupport.MIN_CLEANUP_DELAY_MS;
import static com.linkedin.venice.controller.versionlifecycle.VersionLifecycleTestSupport.MIN_VERSIONS_TO_PRESERVE;
import static com.linkedin.venice.controller.versionlifecycle.VersionLifecycleTestSupport.ROLLED_BACK_RETENTION_MS;
import static com.linkedin.venice.controller.versionlifecycle.VersionLifecycleTestSupport.STORE_NAME;
import static com.linkedin.venice.controller.versionlifecycle.VersionLifecycleTestSupport.mockStoreWithPromoteTimestamp;
import static com.linkedin.venice.controller.versionlifecycle.VersionLifecycleTestSupport.mockStoreWithVersions;
import static com.linkedin.venice.controller.versionlifecycle.VersionLifecycleTestSupport.mockVersion;
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
import com.linkedin.venice.meta.VersionStatus;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


/**
 * Tests for the two new-push capacity guards on {@link VersionLifecyclePolicy}:
 * {@code checkBackupVersionCleanupCapacityForNewPush} (backup-cleanup min-delay) and
 * {@code checkRollbackOriginVersionCapacityForNewPush} (rollback-origin retention window).
 */
public class NewPushCapacityGuardsTest {
  // ---------- checkBackupVersionCleanupCapacityForNewPush ----------
  @Test
  public void deleteOnNewPushStartUsesNMinusOnePreserveCountAndPassesWhenNoVersionsPending() {
    // DELETE_ON_NEW_PUSH_START: after SOP cleanup we expect only current + new push.
    // Preserve count passed to retrieveVersionsToDelete should be N-1 (= 1).
    Store store = mock(Store.class);
    doReturn(Collections.emptyList()).when(store).retrieveVersionsToDelete(MIN_VERSIONS_TO_PRESERVE - 1);

    VersionLifecyclePolicy.checkBackupVersionCleanupCapacityForNewPush(
        CLUSTER_NAME,
        STORE_NAME,
        store,
        BackupStrategy.DELETE_ON_NEW_PUSH_START,
        MIN_VERSIONS_TO_PRESERVE,
        MIN_CLEANUP_DELAY_MS,
        System.currentTimeMillis());

    verify(store).retrieveVersionsToDelete(MIN_VERSIONS_TO_PRESERVE - 1);
    verify(store, never()).getLatestVersionPromoteToCurrentTimestamp();
  }

  @Test
  public void keepMinVersionsUsesNPreserveCountAndPassesWhenNoVersionsPending() {
    // KEEP_MIN_VERSIONS: preserve N at steady state, N+1 during push.
    // Preserve count passed to retrieveVersionsToDelete should be N (= 2), not N-1.
    Store store = mock(Store.class);
    doReturn(Collections.emptyList()).when(store).retrieveVersionsToDelete(MIN_VERSIONS_TO_PRESERVE);

    VersionLifecyclePolicy.checkBackupVersionCleanupCapacityForNewPush(
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
  public void backupCleanupPassesWhenPastMinCleanupDelayEvenWithPendingVersions() {
    Store store = mock(Store.class);
    Version pendingVersion = mock(Version.class);
    doReturn(Collections.singletonList(pendingVersion)).when(store)
        .retrieveVersionsToDelete(MIN_VERSIONS_TO_PRESERVE - 1);
    long now = System.currentTimeMillis();
    long promotionTime = now - MIN_CLEANUP_DELAY_MS - 1;
    doReturn(promotionTime).when(store).getLatestVersionPromoteToCurrentTimestamp();

    VersionLifecyclePolicy.checkBackupVersionCleanupCapacityForNewPush(
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
  public void deleteOnNewPushStartBlocksPushWhenWithinMinCleanupDelay() {
    // Rollback scenario: versions pending AND promotion was 30min ago (< 1h min delay).
    Store store = mock(Store.class);
    Version pendingVersion = mock(Version.class);
    doReturn(Collections.singletonList(pendingVersion)).when(store)
        .retrieveVersionsToDelete(MIN_VERSIONS_TO_PRESERVE - 1);
    long now = System.currentTimeMillis();
    doReturn(now - TimeUnit.MINUTES.toMillis(30)).when(store).getLatestVersionPromoteToCurrentTimestamp();

    VeniceException e = expectThrows(
        VeniceException.class,
        () -> VersionLifecyclePolicy.checkBackupVersionCleanupCapacityForNewPush(
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
  public void keepMinVersionsBlocksPushWhenWithinMinCleanupDelay() {
    Store store = mock(Store.class);
    Version v1 = mock(Version.class);
    Version v2 = mock(Version.class);
    doReturn(Arrays.asList(v1, v2)).when(store).retrieveVersionsToDelete(MIN_VERSIONS_TO_PRESERVE);
    long now = System.currentTimeMillis();
    doReturn(now - TimeUnit.MINUTES.toMillis(30)).when(store).getLatestVersionPromoteToCurrentTimestamp();

    VeniceException e = expectThrows(
        VeniceException.class,
        () -> VersionLifecyclePolicy.checkBackupVersionCleanupCapacityForNewPush(
            CLUSTER_NAME,
            STORE_NAME,
            store,
            BackupStrategy.KEEP_MIN_VERSIONS,
            MIN_VERSIONS_TO_PRESERVE,
            MIN_CLEANUP_DELAY_MS,
            now));
    assertTrue(e.getMessage().contains(STORE_NAME));
    assertTrue(
        e.getMessage().contains("2 backup version(s)"),
        "Error should report the 2 pending versions: " + e.getMessage());
  }

  @Test
  public void backupCleanupBlocksExactlyAtMinCleanupDelayBoundary() {
    // At the boundary (currentTime == promotionTime + minDelay), the check is <= so it blocks.
    Store store = mock(Store.class);
    Version pendingVersion = mock(Version.class);
    doReturn(Collections.singletonList(pendingVersion)).when(store)
        .retrieveVersionsToDelete(MIN_VERSIONS_TO_PRESERVE - 1);
    long now = System.currentTimeMillis();
    doReturn(now - MIN_CLEANUP_DELAY_MS).when(store).getLatestVersionPromoteToCurrentTimestamp();

    assertThrows(
        VeniceException.class,
        () -> VersionLifecyclePolicy.checkBackupVersionCleanupCapacityForNewPush(
            CLUSTER_NAME,
            STORE_NAME,
            store,
            BackupStrategy.DELETE_ON_NEW_PUSH_START,
            MIN_VERSIONS_TO_PRESERVE,
            MIN_CLEANUP_DELAY_MS,
            now));
  }

  @Test
  public void backupCleanupJustPastBoundaryAllowsPush() {
    Store store = mock(Store.class);
    Version pendingVersion = mock(Version.class);
    doReturn(Collections.singletonList(pendingVersion)).when(store)
        .retrieveVersionsToDelete(MIN_VERSIONS_TO_PRESERVE - 1);
    long now = System.currentTimeMillis();
    doReturn(now - MIN_CLEANUP_DELAY_MS - 1).when(store).getLatestVersionPromoteToCurrentTimestamp();

    VersionLifecyclePolicy.checkBackupVersionCleanupCapacityForNewPush(
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
  public void deleteOnNewPushStartClampsPreserveCountToAtLeastOneWhenMinIsOne() {
    // If cluster config sets minNumberOfStoreVersionsToPreserve == 1, the DELETE_ON_NEW_PUSH_START branch
    // would compute N-1 = 0, which Store.retrieveVersionsToDelete rejects with IllegalArgumentException.
    // Verify the check clamps to 1 so the push path doesn't break.
    Store store = mock(Store.class);
    doReturn(Collections.emptyList()).when(store).retrieveVersionsToDelete(1);

    VersionLifecyclePolicy.checkBackupVersionCleanupCapacityForNewPush(
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

  // ---------- checkRollbackOriginVersionCapacityForNewPush ----------

  // Repoints the Store-based scenarios at the field overload (the only surviving signature),
  // unpacking the Store snapshot exactly as the parent does for a child StoreInfo.
  private static void checkRollbackOrigin(Store store, long rolledBackVersionRetentionMs, long currentTimeMs) {
    VersionLifecyclePolicy.checkRollbackOriginVersionCapacityForNewPush(
        CLUSTER_NAME,
        STORE_NAME,
        null,
        store.getVersions(),
        store.getCurrentVersion(),
        store.getLatestVersionPromoteToCurrentTimestamp(),
        rolledBackVersionRetentionMs,
        currentTimeMs);
  }

  @Test
  public void rollbackOriginPassesWhenNoRollbackOriginVersions() {
    Store store = mockStoreWithVersions(
        /* currentVersion */ 2,
        /* promoteTimestampMsAgo */ 0,
        mockVersion(1, VersionStatus.ONLINE),
        mockVersion(2, VersionStatus.ONLINE));

    checkRollbackOrigin(store, ROLLED_BACK_RETENTION_MS, System.currentTimeMillis());
  }

  @Test
  public void rollbackOriginBlocksPushWhenRolledBackVersionExistsWithinRetention() {
    long now = System.currentTimeMillis();
    Store store = mockStoreWithVersions(
        4,
        TimeUnit.HOURS.toMillis(1),
        mockVersion(4, VersionStatus.ONLINE),
        mockVersion(5, VersionStatus.ROLLED_BACK));

    VeniceException e =
        expectThrows(VeniceException.class, () -> checkRollbackOrigin(store, ROLLED_BACK_RETENTION_MS, now));
    assertTrue(e.getMessage().contains(STORE_NAME), "message should include store name: " + e.getMessage());
    assertTrue(e.getMessage().contains("ROLLED_BACK"), "message should include status: " + e.getMessage());
    assertTrue(e.getMessage().contains("version 5"), "message should include version number: " + e.getMessage());
  }

  @Test
  public void rollbackOriginPassesWhenRolledBackVersionPastRetention() {
    long now = System.currentTimeMillis();
    Store store = mockStoreWithVersions(
        4,
        TimeUnit.HOURS.toMillis(25),
        mockVersion(4, VersionStatus.ONLINE),
        mockVersion(5, VersionStatus.ROLLED_BACK));

    checkRollbackOrigin(store, ROLLED_BACK_RETENTION_MS, now);
  }

  @Test
  public void rollbackOriginPassesWhenPartiallyOnlineVersionAboveCurrentExists() {
    // A child region's rollback is binary (ROLLED_BACK), so a rollback never leaves a child
    // PARTIALLY_ONLINE. A child PARTIALLY_ONLINE above currentVersion comes only from a degraded-mode
    // forward push, which is NOT a rollback-origin and must not block a new push.
    long now = System.currentTimeMillis();
    Store store = mockStoreWithVersions(
        4,
        TimeUnit.HOURS.toMillis(1),
        mockVersion(4, VersionStatus.ONLINE),
        mockVersion(5, VersionStatus.PARTIALLY_ONLINE));

    checkRollbackOrigin(store, ROLLED_BACK_RETENTION_MS, now);
  }

  @Test
  public void rollbackOriginPassesWhenRolledBackVersionBelowCurrentVersion() {
    // Stale ROLLED_BACK entry on parent: a rollback happened, then a subsequent push promoted higher.
    // Parent retains more versions than children, so the ROLLED_BACK entry can linger after parent's
    // currentVersion has moved past it. The filter must skip such entries — otherwise every fresh
    // promotion's retention window would re-trigger the guard against the stale entry forever.
    long now = System.currentTimeMillis();
    Store store = mockStoreWithVersions(
        3,
        TimeUnit.HOURS.toMillis(1),
        mockVersion(2, VersionStatus.ROLLED_BACK),
        mockVersion(3, VersionStatus.ONLINE));

    checkRollbackOrigin(store, ROLLED_BACK_RETENTION_MS, now);
  }

  @Test
  public void rollbackOriginBlocksOnlyRolledBackEntryAboveCurrentVersion() {
    // Multi-rollback on parent: stale ROLLED_BACK v2 lingers below currentVersion=4 (aged out by a
    // subsequent push), while v5 is the active rollback-origin above current. The filter must skip
    // v2 and block on v5 — pre-PR filter (status==ROLLED_BACK alone) would have matched v2 first
    // and misattributed the block.
    long now = System.currentTimeMillis();
    Store store = mockStoreWithVersions(
        4,
        TimeUnit.HOURS.toMillis(1),
        mockVersion(2, VersionStatus.ROLLED_BACK),
        mockVersion(4, VersionStatus.ONLINE),
        mockVersion(5, VersionStatus.ROLLED_BACK));

    VeniceException e =
        expectThrows(VeniceException.class, () -> checkRollbackOrigin(store, ROLLED_BACK_RETENTION_MS, now));
    assertTrue(
        e.getMessage().contains("version 5"),
        "should block on v5 (above current), not stale v2: " + e.getMessage());
  }

  @Test
  public void rollbackOriginPassesWhenPartiallyOnlineVersionEqualsCurrentVersion() {
    // Push-origin PARTIALLY_ONLINE: v4 is PARTIALLY_ONLINE and IS the current version → not rollback-origin
    long now = System.currentTimeMillis();
    Store store = mockStoreWithVersions(
        4,
        TimeUnit.HOURS.toMillis(1),
        mockVersion(3, VersionStatus.ONLINE),
        mockVersion(4, VersionStatus.PARTIALLY_ONLINE));

    checkRollbackOrigin(store, ROLLED_BACK_RETENTION_MS, now);
  }

  @Test
  public void rollbackOriginPassesWhenPartiallyOnlineBelowCurrentVersion() {
    // Push-origin PARTIALLY_ONLINE on an older version (current was promoted past it) → not rollback-origin
    long now = System.currentTimeMillis();
    Store store = mockStoreWithVersions(
        5,
        TimeUnit.HOURS.toMillis(1),
        mockVersion(4, VersionStatus.PARTIALLY_ONLINE),
        mockVersion(5, VersionStatus.ONLINE));

    checkRollbackOrigin(store, ROLLED_BACK_RETENTION_MS, now);
  }

  @Test
  public void rollbackOriginPassesJustPastRetention() {
    // Past retention → check short-circuits, no block.
    // Mock promoteTimestamp explicitly so wall-clock jitter doesn't disturb the boundary.
    long now = 1_000_000L;
    long retention = ROLLED_BACK_RETENTION_MS;
    Store store = mockStoreWithPromoteTimestamp(
        4,
        now - retention - 1, // 1ms past expiry in mocked time
        mockVersion(4, VersionStatus.ONLINE),
        mockVersion(5, VersionStatus.ROLLED_BACK));

    checkRollbackOrigin(store, retention, now);
  }

  @Test
  public void rollbackOriginBlocksJustInsideRetentionBoundary() {
    long now = 1_000_000L;
    long retention = ROLLED_BACK_RETENTION_MS;
    Store store = mockStoreWithPromoteTimestamp(
        4,
        now - retention + 1, // 1ms before expiry in mocked time
        mockVersion(4, VersionStatus.ONLINE),
        mockVersion(5, VersionStatus.ROLLED_BACK));

    expectThrows(VeniceException.class, () -> checkRollbackOrigin(store, retention, now));
  }

  @Test
  public void rollbackOriginBlocksAtExactRetentionBoundary() {
    // At the exact expiry ms (currentTime == retentionExpiresAt), the `>` check is not yet satisfied → block.
    long now = 1_000_000L;
    long retention = ROLLED_BACK_RETENTION_MS;
    Store store = mockStoreWithPromoteTimestamp(
        4,
        now - retention,
        mockVersion(4, VersionStatus.ONLINE),
        mockVersion(5, VersionStatus.ROLLED_BACK));

    expectThrows(VeniceException.class, () -> checkRollbackOrigin(store, retention, now));
  }

  @Test
  public void rollbackOriginFieldOverloadBlocksAndIncludesRegionNameInMessage() {
    // The child-snapshot overload evaluates raw fields (as fed from a child StoreInfo) and enriches
    // the rejection message with the region name so operators can see which child is blocking.
    long now = System.currentTimeMillis();
    VeniceException e = expectThrows(
        VeniceException.class,
        () -> VersionLifecyclePolicy.checkRollbackOriginVersionCapacityForNewPush(
            CLUSTER_NAME,
            STORE_NAME,
            "dc-0",
            Arrays.asList(mockVersion(4, VersionStatus.ONLINE), mockVersion(5, VersionStatus.ROLLED_BACK)),
            /* currentVersion */ 4,
            /* latestVersionPromoteToCurrentTimestamp */ now - TimeUnit.HOURS.toMillis(1),
            ROLLED_BACK_RETENTION_MS,
            now));
    assertTrue(e.getMessage().contains("in region dc-0"), "message should include region: " + e.getMessage());
    assertTrue(e.getMessage().contains("version 5"), "message should include version number: " + e.getMessage());
    assertTrue(e.getMessage().contains("ROLLED_BACK"), "message should include status: " + e.getMessage());
  }

  @Test
  public void rollbackOriginFieldOverloadPassesWhenNoRollbackOriginVersion() {
    // Live child snapshot shows no rollback-origin version -> no block, even within the retention
    // window. This is the parent-side allow path when parent metadata is stale but children are clean.
    long now = System.currentTimeMillis();
    VersionLifecyclePolicy.checkRollbackOriginVersionCapacityForNewPush(
        CLUSTER_NAME,
        STORE_NAME,
        "dc-0",
        Arrays.asList(mockVersion(4, VersionStatus.ONLINE), mockVersion(5, VersionStatus.ONLINE)),
        /* currentVersion */ 5,
        /* latestVersionPromoteToCurrentTimestamp */ now,
        ROLLED_BACK_RETENTION_MS,
        now);
  }
}
