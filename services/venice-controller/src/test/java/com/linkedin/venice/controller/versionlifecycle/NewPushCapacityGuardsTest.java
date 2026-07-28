package com.linkedin.venice.controller.versionlifecycle;

import static com.linkedin.venice.controller.versionlifecycle.VersionLifecycleTestSupport.CLUSTER_NAME;
import static com.linkedin.venice.controller.versionlifecycle.VersionLifecycleTestSupport.MIN_CLEANUP_DELAY_MS;
import static com.linkedin.venice.controller.versionlifecycle.VersionLifecycleTestSupport.MIN_VERSIONS_TO_PRESERVE;
import static com.linkedin.venice.controller.versionlifecycle.VersionLifecycleTestSupport.ROLLED_BACK_RETENTION_MS;
import static com.linkedin.venice.controller.versionlifecycle.VersionLifecycleTestSupport.STORE_NAME;
import static com.linkedin.venice.controller.versionlifecycle.VersionLifecycleTestSupport.mockStoreWithPromoteTimestamp;
import static com.linkedin.venice.controller.versionlifecycle.VersionLifecycleTestSupport.mockStoreWithVersions;
import static com.linkedin.venice.controller.versionlifecycle.VersionLifecycleTestSupport.mockVersion;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.VersionStatus;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


/**
 * Tests for the two new-push capacity guards on {@link VersionLifecyclePolicy}:
 * {@code checkBackupVersionCleanupCapacityForNewPush} (backup-cleanup min-delay) and
 * {@code checkRollbackOriginVersionCapacityForNewPush} (rollback-origin retention window).
 */
public class NewPushCapacityGuardsTest {
  // ---------- checkBackupVersionCleanupCapacityForNewPush ----------

  // Unpacks a Store snapshot into the field overload, as the parent does for a child StoreInfo.
  private static void checkBackupCleanup(
      Store store,
      BackupStrategy backupStrategy,
      int minVersionsToPreserve,
      long minCleanupDelayMs,
      long currentTimeMs) {
    VersionLifecyclePolicy.checkBackupVersionCleanupCapacityForNewPush(
        CLUSTER_NAME,
        STORE_NAME,
        null,
        store.getVersions(),
        store.getCurrentVersion(),
        store.getNumVersionsToPreserve(),
        store.isMigrating(),
        backupStrategy,
        minVersionsToPreserve,
        store.getLatestVersionPromoteToCurrentTimestamp(),
        minCleanupDelayMs,
        currentTimeMs);
  }

  @Test
  public void backupCleanupPassesWhenNoDeletableVersions() {
    // Only the current version exists, so nothing is pending deletion regardless of the delay.
    Store store = mockStoreWithVersions(2, /* promotedMsAgo */ 0, mockVersion(2, VersionStatus.ONLINE));
    checkBackupCleanup(
        store,
        BackupStrategy.DELETE_ON_NEW_PUSH_START,
        MIN_VERSIONS_TO_PRESERVE,
        MIN_CLEANUP_DELAY_MS,
        System.currentTimeMillis());
  }

  @Test
  public void deleteOnNewPushStartBlocksWhenKilledBackupWithinMinCleanupDelay() {
    // v1 KILLED is pending deletion (canDelete) and v2 was just promoted, so the push is blocked.
    long now = System.currentTimeMillis();
    Store store = mockStoreWithVersions(
        2,
        /* promotedMsAgo */ TimeUnit.MINUTES.toMillis(30),
        mockVersion(1, VersionStatus.KILLED),
        mockVersion(2, VersionStatus.ONLINE));

    VeniceException e = expectThrows(
        VeniceException.class,
        () -> checkBackupCleanup(
            store,
            BackupStrategy.DELETE_ON_NEW_PUSH_START,
            MIN_VERSIONS_TO_PRESERVE,
            MIN_CLEANUP_DELAY_MS,
            now));
    assertTrue(e.getMessage().contains(STORE_NAME), "Error should include store name: " + e.getMessage());
    assertTrue(e.getMessage().contains(CLUSTER_NAME), "Error should include cluster name: " + e.getMessage());
    assertTrue(e.getMessage().contains("1 backup version(s)"), "Error should include pending count: " + e.getMessage());
    assertTrue(e.getMessage().contains("pending deletion"), "Error should mention pending deletion: " + e.getMessage());
    assertTrue(
        e.getMessage().contains("min cleanup delay"),
        "Error should mention min cleanup delay: " + e.getMessage());
    assertTrue(
        e.getMessage().contains(String.valueOf(MIN_CLEANUP_DELAY_MS)),
        "Error should include the min cleanup delay value: " + e.getMessage());
  }

  @Test
  public void backupCleanupPassesWhenPastMinCleanupDelayEvenWithPendingVersions() {
    long now = System.currentTimeMillis();
    Store store = mockStoreWithPromoteTimestamp(
        2,
        now - MIN_CLEANUP_DELAY_MS - 1,
        mockVersion(1, VersionStatus.KILLED),
        mockVersion(2, VersionStatus.ONLINE));
    checkBackupCleanup(
        store,
        BackupStrategy.DELETE_ON_NEW_PUSH_START,
        MIN_VERSIONS_TO_PRESERVE,
        MIN_CLEANUP_DELAY_MS,
        now);
  }

  @Test
  public void backupCleanupBlocksExactlyAtMinCleanupDelayBoundary() {
    // At the boundary (currentTime == promotionTime + minDelay) the check is <=, so it blocks.
    long now = System.currentTimeMillis();
    Store store = mockStoreWithPromoteTimestamp(
        2,
        now - MIN_CLEANUP_DELAY_MS,
        mockVersion(1, VersionStatus.KILLED),
        mockVersion(2, VersionStatus.ONLINE));
    assertThrows(
        VeniceException.class,
        () -> checkBackupCleanup(
            store,
            BackupStrategy.DELETE_ON_NEW_PUSH_START,
            MIN_VERSIONS_TO_PRESERVE,
            MIN_CLEANUP_DELAY_MS,
            now));
  }

  @Test
  public void preserveCountDiffersBetweenBackupStrategies() {
    // An ONLINE backup (v1) below current (v2), just promoted. DELETE_ON_NEW_PUSH_START preserves
    // N-1 (= 1: just the current), so v1 is deletable -> blocked. KEEP_MIN_VERSIONS preserves N
    // (= 2), so v1 is kept -> allowed.
    long now = System.currentTimeMillis();
    Store store = mockStoreWithVersions(
        2,
        /* promotedMsAgo */ TimeUnit.MINUTES.toMillis(30),
        mockVersion(1, VersionStatus.ONLINE),
        mockVersion(2, VersionStatus.ONLINE));

    assertThrows(
        VeniceException.class,
        () -> checkBackupCleanup(
            store,
            BackupStrategy.DELETE_ON_NEW_PUSH_START,
            MIN_VERSIONS_TO_PRESERVE,
            MIN_CLEANUP_DELAY_MS,
            now));
    checkBackupCleanup(store, BackupStrategy.KEEP_MIN_VERSIONS, MIN_VERSIONS_TO_PRESERVE, MIN_CLEANUP_DELAY_MS, now);
  }

  @Test
  public void deleteOnNewPushStartClampsPreserveCountToAtLeastOneWhenMinIsOne() {
    // min == 1 would compute N-1 = 0, which computeVersionsToDelete rejects with
    // IllegalArgumentException. The clamp to 1 keeps it a normal VeniceException capacity block.
    long now = System.currentTimeMillis();
    Store store = mockStoreWithVersions(
        2,
        /* promotedMsAgo */ TimeUnit.MINUTES.toMillis(30),
        mockVersion(1, VersionStatus.ONLINE),
        mockVersion(2, VersionStatus.ONLINE));
    assertThrows(
        VeniceException.class,
        () -> checkBackupCleanup(
            store,
            BackupStrategy.DELETE_ON_NEW_PUSH_START,
            /* min */ 1,
            MIN_CLEANUP_DELAY_MS,
            now));
  }

  @Test
  public void backupCleanupIncludesRegionNameInMessage() {
    long now = System.currentTimeMillis();
    Store store = mockStoreWithVersions(
        2,
        /* promotedMsAgo */ TimeUnit.MINUTES.toMillis(30),
        mockVersion(1, VersionStatus.KILLED),
        mockVersion(2, VersionStatus.ONLINE));
    VeniceException e = expectThrows(
        VeniceException.class,
        () -> VersionLifecyclePolicy.checkBackupVersionCleanupCapacityForNewPush(
            CLUSTER_NAME,
            STORE_NAME,
            "dc-1",
            store.getVersions(),
            store.getCurrentVersion(),
            store.getNumVersionsToPreserve(),
            store.isMigrating(),
            BackupStrategy.DELETE_ON_NEW_PUSH_START,
            MIN_VERSIONS_TO_PRESERVE,
            store.getLatestVersionPromoteToCurrentTimestamp(),
            MIN_CLEANUP_DELAY_MS,
            now));
    assertTrue(e.getMessage().contains("region dc-1"), "Error should include region name: " + e.getMessage());
  }

  // ---------- checkRollbackOriginVersionCapacityForNewPush ----------

  // Unpacks a Store snapshot into the field overload, as the parent does for a child StoreInfo.
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
    // Stale ROLLED_BACK entry below currentVersion (a later push promoted higher) must be skipped —
    // otherwise every fresh promotion's retention window would re-trigger the guard forever.
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
    // Stale ROLLED_BACK v2 below current is skipped; the active v5 above current blocks.
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
    // PARTIALLY_ONLINE at currentVersion is a push, not a rollback-origin.
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
    // PARTIALLY_ONLINE below currentVersion is a superseded push, not a rollback-origin.
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
    // Past retention → short-circuits, no block. Mock promoteTimestamp so wall-clock jitter can't
    // disturb the boundary.
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
    // The region-named overload enriches the rejection message so operators see which child blocks.
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
    // Clean child snapshot within the retention window → no block.
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
