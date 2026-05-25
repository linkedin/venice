package com.linkedin.venice.controller.versionlifecycle;

import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.pushmonitor.PushMonitor;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class VersionLifecyclePolicyTest {
  private static final String CLUSTER_NAME = "test-cluster";
  private static final String STORE_NAME = "test-store";
  private static final int MIN_VERSIONS_TO_PRESERVE = 2;
  private static final long MIN_CLEANUP_DELAY_MS = TimeUnit.HOURS.toMillis(1);
  private static final long ROLLED_BACK_RETENTION_MS = TimeUnit.HOURS.toMillis(24);

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

  @Test
  public void rollbackOriginPassesWhenNoRollbackOriginVersions() {
    Store store = mockStoreWithVersions(
        /* currentVersion */ 2,
        /* promoteTimestampMsAgo */ 0,
        mockVersion(1, VersionStatus.ONLINE),
        mockVersion(2, VersionStatus.ONLINE));

    VersionLifecyclePolicy.checkRollbackOriginVersionCapacityForNewPush(
        CLUSTER_NAME,
        STORE_NAME,
        store,
        ROLLED_BACK_RETENTION_MS,
        System.currentTimeMillis());
  }

  @Test
  public void rollbackOriginBlocksPushWhenRolledBackVersionExistsWithinRetention() {
    long now = System.currentTimeMillis();
    Store store = mockStoreWithVersions(
        4,
        TimeUnit.HOURS.toMillis(1),
        mockVersion(4, VersionStatus.ONLINE),
        mockVersion(5, VersionStatus.ROLLED_BACK));

    VeniceException e = expectThrows(
        VeniceException.class,
        () -> VersionLifecyclePolicy.checkRollbackOriginVersionCapacityForNewPush(
            CLUSTER_NAME,
            STORE_NAME,
            store,
            ROLLED_BACK_RETENTION_MS,
            now));
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

    VersionLifecyclePolicy
        .checkRollbackOriginVersionCapacityForNewPush(CLUSTER_NAME, STORE_NAME, store, ROLLED_BACK_RETENTION_MS, now);
  }

  @Test
  public void rollbackOriginBlocksPushWhenPartiallyOnlineVersionAboveCurrentExistsWithinRetention() {
    // Parent-side partial rollback: v5 is PARTIALLY_ONLINE with number > currentVersion (v4)
    long now = System.currentTimeMillis();
    Store store = mockStoreWithVersions(
        4,
        TimeUnit.HOURS.toMillis(1),
        mockVersion(4, VersionStatus.ONLINE),
        mockVersion(5, VersionStatus.PARTIALLY_ONLINE));

    VeniceException e = expectThrows(
        VeniceException.class,
        () -> VersionLifecyclePolicy.checkRollbackOriginVersionCapacityForNewPush(
            CLUSTER_NAME,
            STORE_NAME,
            store,
            ROLLED_BACK_RETENTION_MS,
            now));
    assertTrue(e.getMessage().contains("PARTIALLY_ONLINE"), "message should include status: " + e.getMessage());
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

    VersionLifecyclePolicy
        .checkRollbackOriginVersionCapacityForNewPush(CLUSTER_NAME, STORE_NAME, store, ROLLED_BACK_RETENTION_MS, now);
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

    VeniceException e = expectThrows(
        VeniceException.class,
        () -> VersionLifecyclePolicy.checkRollbackOriginVersionCapacityForNewPush(
            CLUSTER_NAME,
            STORE_NAME,
            store,
            ROLLED_BACK_RETENTION_MS,
            now));
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

    VersionLifecyclePolicy
        .checkRollbackOriginVersionCapacityForNewPush(CLUSTER_NAME, STORE_NAME, store, ROLLED_BACK_RETENTION_MS, now);
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

    VersionLifecyclePolicy
        .checkRollbackOriginVersionCapacityForNewPush(CLUSTER_NAME, STORE_NAME, store, ROLLED_BACK_RETENTION_MS, now);
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

    VersionLifecyclePolicy
        .checkRollbackOriginVersionCapacityForNewPush(CLUSTER_NAME, STORE_NAME, store, retention, now);
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

    expectThrows(
        VeniceException.class,
        () -> VersionLifecyclePolicy
            .checkRollbackOriginVersionCapacityForNewPush(CLUSTER_NAME, STORE_NAME, store, retention, now));
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

    expectThrows(
        VeniceException.class,
        () -> VersionLifecyclePolicy
            .checkRollbackOriginVersionCapacityForNewPush(CLUSTER_NAME, STORE_NAME, store, retention, now));
  }

  // ---------- getStartedVersion ----------

  @Test
  public void getStartedVersionReturnsEmptyWhenNoVersionsAboveCurrent() {
    Store store = mock(Store.class);
    doReturn(2).when(store).getCurrentVersion();
    doReturn(Arrays.asList(mockVersion(1, VersionStatus.ONLINE), mockVersion(2, VersionStatus.ONLINE))).when(store)
        .getVersions();

    assertEquals(VersionLifecyclePolicy.getStartedVersion(store), Optional.empty());
  }

  @Test
  public void getStartedVersionReturnsTheSingleStartedVersionAboveCurrent() {
    Store store = mock(Store.class);
    doReturn(2).when(store).getCurrentVersion();
    Version started = mockVersion(3, VersionStatus.STARTED);
    doReturn(Arrays.asList(mockVersion(2, VersionStatus.ONLINE), started)).when(store).getVersions();

    Optional<Version> result = VersionLifecyclePolicy.getStartedVersion(store);
    assertTrue(result.isPresent());
    assertEquals(result.get().getNumber(), 3);
  }

  @Test
  public void getStartedVersionIgnoresOnlineAndPushedAboveCurrent() {
    Store store = mock(Store.class);
    doReturn("test").when(store).getName();
    doReturn(2).when(store).getCurrentVersion();
    doReturn(Arrays.asList(mockVersion(3, VersionStatus.ONLINE), mockVersion(4, VersionStatus.PUSHED))).when(store)
        .getVersions();

    assertEquals(VersionLifecyclePolicy.getStartedVersion(store), Optional.empty());
  }

  @Test
  public void getStartedVersionThrowsOnErrorVersionAboveCurrent() {
    Store store = mock(Store.class);
    doReturn("test").when(store).getName();
    doReturn(2).when(store).getCurrentVersion();
    doReturn(Collections.singletonList(mockVersion(3, VersionStatus.ERROR))).when(store).getVersions();

    VeniceException e = expectThrows(VeniceException.class, () -> VersionLifecyclePolicy.getStartedVersion(store));
    assertTrue(e.getMessage().contains("ERROR"), e.getMessage());
  }

  @Test
  public void getStartedVersionThrowsOnMultipleStartedVersions() {
    Store store = mock(Store.class);
    doReturn("test").when(store).getName();
    doReturn(2).when(store).getCurrentVersion();
    doReturn(Arrays.asList(mockVersion(3, VersionStatus.STARTED), mockVersion(4, VersionStatus.STARTED))).when(store)
        .getVersions();

    VeniceException e = expectThrows(VeniceException.class, () -> VersionLifecyclePolicy.getStartedVersion(store));
    assertTrue(e.getMessage().contains("3,4"), e.getMessage());
  }

  // ---------- getBackupVersionNumber ----------

  @Test
  public void getBackupVersionNumberReturnsLargestOnlineBelowCurrent() {
    List<Version> versions = Arrays.asList(
        mockVersion(1, VersionStatus.ONLINE),
        mockVersion(2, VersionStatus.ONLINE),
        mockVersion(3, VersionStatus.ONLINE)); // current

    assertEquals(VersionLifecyclePolicy.getBackupVersionNumber(versions, 3), 2);
  }

  @Test
  public void getBackupVersionNumberSkipsNonOnlineVersions() {
    // v3 is current; v2 is ERROR — should be skipped — v1 ONLINE wins
    List<Version> versions = Arrays.asList(
        mockVersion(1, VersionStatus.ONLINE),
        mockVersion(2, VersionStatus.ERROR),
        mockVersion(3, VersionStatus.ONLINE));

    assertEquals(VersionLifecyclePolicy.getBackupVersionNumber(versions, 3), 1);
  }

  @Test
  public void getBackupVersionNumberReturnsNonExistingWhenNoOnlineBelowCurrent() {
    List<Version> versions = Arrays.asList(mockVersion(1, VersionStatus.ERROR), mockVersion(2, VersionStatus.ONLINE));

    assertEquals(VersionLifecyclePolicy.getBackupVersionNumber(versions, 2), NON_EXISTING_VERSION);
  }

  @Test
  public void getBackupVersionNumberWalksFromHighestToLowest() {
    // Sort happens in-place desc; the first ONLINE below current wins.
    List<Version> versions = Arrays.asList(
        mockVersion(5, VersionStatus.ONLINE), // current
        mockVersion(4, VersionStatus.ONLINE), // should win
        mockVersion(3, VersionStatus.ONLINE));

    assertEquals(VersionLifecyclePolicy.getBackupVersionNumber(versions, 5), 4);
  }

  // ---------- hasFatalDataValidationError ----------

  @Test
  public void hasFatalDataValidationErrorReturnsTrueWhenPushReportsError() {
    PushMonitor pushMonitor = mock(PushMonitor.class);
    OfflinePushStatus offlinePushStatus = mock(OfflinePushStatus.class);
    doReturn(true).when(offlinePushStatus).hasFatalDataValidationError();
    doReturn(offlinePushStatus).when(pushMonitor).getOfflinePushOrThrow("topic_v1");

    assertTrue(VersionLifecyclePolicy.hasFatalDataValidationError(pushMonitor, "topic_v1"));
  }

  @Test
  public void hasFatalDataValidationErrorReturnsFalseWhenPushReportsClean() {
    PushMonitor pushMonitor = mock(PushMonitor.class);
    OfflinePushStatus offlinePushStatus = mock(OfflinePushStatus.class);
    doReturn(false).when(offlinePushStatus).hasFatalDataValidationError();
    doReturn(offlinePushStatus).when(pushMonitor).getOfflinePushOrThrow("topic_v1");

    assertFalse(VersionLifecyclePolicy.hasFatalDataValidationError(pushMonitor, "topic_v1"));
  }

  @Test
  public void hasFatalDataValidationErrorReturnsFalseWhenPushEntryMissing() {
    PushMonitor pushMonitor = mock(PushMonitor.class);
    doThrow(new VeniceException("not found")).when(pushMonitor).getOfflinePushOrThrow("topic_v1");

    assertFalse(VersionLifecyclePolicy.hasFatalDataValidationError(pushMonitor, "topic_v1"));
  }

  // ---------- updateStoreTTLRepushFlag ----------

  @Test
  public void ttlRePushSetsFlagToTrueWhenCurrentlyFalse() {
    Store store = mock(Store.class);
    ReadWriteStoreRepository repository = mock(ReadWriteStoreRepository.class);
    String ttlRepushId = Version.generateTTLRePushId("test-push");
    when(store.isTTLRepushEnabled()).thenReturn(false);

    VersionLifecyclePolicy.updateStoreTTLRepushFlag(ttlRepushId, store, repository);

    verify(store).setTTLRepushEnabled(true);
    verify(repository).updateStore(store);
  }

  @Test
  public void ttlRePushIsNoOpWhenFlagAlreadyTrue() {
    Store store = mock(Store.class);
    ReadWriteStoreRepository repository = mock(ReadWriteStoreRepository.class);
    String ttlRepushId = Version.generateTTLRePushId("test-push");
    when(store.isTTLRepushEnabled()).thenReturn(true);

    VersionLifecyclePolicy.updateStoreTTLRepushFlag(ttlRepushId, store, repository);

    verify(store, never()).setTTLRepushEnabled(anyBoolean());
    verify(repository, never()).updateStore(any());
  }

  @Test
  public void regularPushWithTTLClearsFlagWhenCurrentlyTrue() {
    Store store = mock(Store.class);
    ReadWriteStoreRepository repository = mock(ReadWriteStoreRepository.class);
    String regularPushWithTtlId = Version.generateRegularPushWithTTLRePushId("test-push");
    when(store.isTTLRepushEnabled()).thenReturn(true);

    VersionLifecyclePolicy.updateStoreTTLRepushFlag(regularPushWithTtlId, store, repository);

    verify(store).setTTLRepushEnabled(false);
    verify(repository).updateStore(store);
  }

  @Test
  public void regularPushWithTTLIsNoOpWhenFlagAlreadyFalse() {
    Store store = mock(Store.class);
    ReadWriteStoreRepository repository = mock(ReadWriteStoreRepository.class);
    String regularPushWithTtlId = Version.generateRegularPushWithTTLRePushId("test-push");
    when(store.isTTLRepushEnabled()).thenReturn(false);

    VersionLifecyclePolicy.updateStoreTTLRepushFlag(regularPushWithTtlId, store, repository);

    verify(store, never()).setTTLRepushEnabled(anyBoolean());
    verify(repository, never()).updateStore(any());
  }

  @Test
  public void compliancePushDoesNotAffectTTLFlag() {
    Store store = mock(Store.class);
    ReadWriteStoreRepository repository = mock(ReadWriteStoreRepository.class);
    String compliancePushId = Version.generateCompliancePushId("test-push");
    when(store.isTTLRepushEnabled()).thenReturn(true);

    VersionLifecyclePolicy.updateStoreTTLRepushFlag(compliancePushId, store, repository);

    verify(store, never()).setTTLRepushEnabled(anyBoolean());
    verify(repository, never()).updateStore(any());
  }

  @Test
  public void unrelatedPushIdDoesNotAffectTTLFlag() {
    Store store = mock(Store.class);
    ReadWriteStoreRepository repository = mock(ReadWriteStoreRepository.class);
    String userPushId = System.currentTimeMillis() + "_https://example.com/user-push";
    when(store.isTTLRepushEnabled()).thenReturn(true);

    VersionLifecyclePolicy.updateStoreTTLRepushFlag(userPushId, store, repository);

    verify(store, never()).setTTLRepushEnabled(anyBoolean());
    verify(repository, never()).updateStore(any());
    reset(store, repository); // keep parity with the original test file
  }

  // ---------- getOverallPushStatus ----------

  @Test
  public void overallPushStatusReturnsCompletedWhenBothCompleted() {
    assertEquals(
        VersionLifecyclePolicy.getOverallPushStatus(ExecutionStatus.COMPLETED, ExecutionStatus.COMPLETED),
        ExecutionStatus.COMPLETED);
  }

  @Test
  public void overallPushStatusReturnsErrorWhenEitherIsError() {
    assertEquals(
        VersionLifecyclePolicy.getOverallPushStatus(ExecutionStatus.ERROR, ExecutionStatus.COMPLETED),
        ExecutionStatus.ERROR);
    assertEquals(
        VersionLifecyclePolicy.getOverallPushStatus(ExecutionStatus.ERROR, ExecutionStatus.ERROR),
        ExecutionStatus.ERROR);
  }

  @Test
  public void overallPushStatusPrefersDvcIngestionErrorOverError() {
    // DVC ingestion errors sit higher in STATUS_PRIORITIES than ERROR, so they outrank a Venice ERROR.
    assertEquals(
        VersionLifecyclePolicy
            .getOverallPushStatus(ExecutionStatus.COMPLETED, ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL),
        ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL);
    assertEquals(
        VersionLifecyclePolicy
            .getOverallPushStatus(ExecutionStatus.ERROR, ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL),
        ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL);
  }

  // ---------- getFinalReturnStatus ----------

  @Test
  public void finalReturnStatusAllCompletedReturnsCompleted() {
    Set<String> regions = setOf("region1", "region2", "region3");
    Map<String, ExecutionStatus> statuses = new HashMap<>();
    statuses.put("region1", ExecutionStatus.COMPLETED);
    statuses.put("region2", ExecutionStatus.COMPLETED);
    statuses.put("region3", ExecutionStatus.COMPLETED);

    assertEquals(
        VersionLifecyclePolicy.getFinalReturnStatus(statuses, regions, 0, new StringBuilder()),
        ExecutionStatus.COMPLETED);
  }

  @Test
  public void finalReturnStatusProgressOutranksCompleted() {
    Set<String> regions = setOf("region1", "region2", "region3");
    Map<String, ExecutionStatus> statuses = new HashMap<>();
    statuses.put("region1", ExecutionStatus.COMPLETED);
    statuses.put("region2", ExecutionStatus.PROGRESS);
    statuses.put("region3", ExecutionStatus.COMPLETED);

    assertEquals(
        VersionLifecyclePolicy.getFinalReturnStatus(statuses, regions, 0, new StringBuilder()),
        ExecutionStatus.PROGRESS);
  }

  @Test
  public void finalReturnStatusErrorBeatsCompleted() {
    Set<String> regions = setOf("region1", "region2", "region3");
    Map<String, ExecutionStatus> statuses = new HashMap<>();
    statuses.put("region1", ExecutionStatus.COMPLETED);
    statuses.put("region2", ExecutionStatus.ERROR);
    statuses.put("region3", ExecutionStatus.COMPLETED);

    assertEquals(
        VersionLifecyclePolicy.getFinalReturnStatus(statuses, regions, 0, new StringBuilder()),
        ExecutionStatus.ERROR);
  }

  @Test
  public void finalReturnStatusUnknownBeatsErrorWithOneRegionFailed() {
    Set<String> regions = setOf("region1", "region2", "region3");
    Map<String, ExecutionStatus> statuses = new HashMap<>();
    statuses.put("region1", ExecutionStatus.COMPLETED);
    statuses.put("region2", ExecutionStatus.ERROR);
    statuses.put("region3", ExecutionStatus.UNKNOWN);

    assertEquals(
        VersionLifecyclePolicy.getFinalReturnStatus(statuses, regions, 1, new StringBuilder()),
        ExecutionStatus.UNKNOWN);
  }

  @Test
  public void finalReturnStatusFailedMajorityDowngradesToProgress() {
    // 2 of 3 regions unreachable → strict majority of 2 not met → downgrade to PROGRESS.
    Set<String> regions = setOf("region1", "region2", "region3");
    Map<String, ExecutionStatus> statuses = new HashMap<>();
    statuses.put("region1", ExecutionStatus.UNKNOWN);
    statuses.put("region2", ExecutionStatus.ERROR);
    statuses.put("region3", ExecutionStatus.UNKNOWN);

    assertEquals(
        VersionLifecyclePolicy.getFinalReturnStatus(statuses, regions, 2, new StringBuilder()),
        ExecutionStatus.PROGRESS);
  }

  @Test
  public void finalReturnStatusDvcDiskFullOutranksDvcOther() {
    Set<String> regions = setOf("region1", "region2", "region3");
    Map<String, ExecutionStatus> statuses = new HashMap<>();
    statuses.put("region1", ExecutionStatus.COMPLETED);
    statuses.put("region2", ExecutionStatus.COMPLETED);
    statuses.put("region3", ExecutionStatus.DVC_INGESTION_ERROR_OTHER);

    assertEquals(
        VersionLifecyclePolicy.getFinalReturnStatus(statuses, regions, 0, new StringBuilder()),
        ExecutionStatus.DVC_INGESTION_ERROR_OTHER);

    statuses.put("region3", ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL);
    assertEquals(
        VersionLifecyclePolicy.getFinalReturnStatus(statuses, regions, 0, new StringBuilder()),
        ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL);
  }

  @Test
  public void finalReturnStatusDvcDiskFullOutranksMemoryLimit() {
    Set<String> regions = setOf("region1", "region2", "region3");
    Map<String, ExecutionStatus> statuses = new HashMap<>();
    statuses.put("region1", ExecutionStatus.COMPLETED);
    statuses.put("region2", ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED);
    statuses.put("region3", ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL);

    assertEquals(
        VersionLifecyclePolicy.getFinalReturnStatus(statuses, regions, 0, new StringBuilder()),
        ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL);
  }

  @Test
  public void finalReturnStatusDvcOtherOutranksMemoryLimit() {
    Set<String> regions = setOf("region1", "region2", "region3");
    Map<String, ExecutionStatus> statuses = new HashMap<>();
    statuses.put("region1", ExecutionStatus.COMPLETED);
    statuses.put("region2", ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED);
    statuses.put("region3", ExecutionStatus.DVC_INGESTION_ERROR_OTHER);

    assertEquals(
        VersionLifecyclePolicy.getFinalReturnStatus(statuses, regions, 0, new StringBuilder()),
        ExecutionStatus.DVC_INGESTION_ERROR_OTHER);
  }

  // ---------- helpers ----------

  private static Version mockVersion(int number, VersionStatus status) {
    Version v = mock(Version.class);
    doReturn(number).when(v).getNumber();
    doReturn(status).when(v).getStatus();
    return v;
  }

  private static Store mockStoreWithVersions(int currentVersion, long promotedMsAgo, Version... versions) {
    Store store = mock(Store.class);
    doReturn(currentVersion).when(store).getCurrentVersion();
    doReturn(System.currentTimeMillis() - promotedMsAgo).when(store).getLatestVersionPromoteToCurrentTimestamp();
    doReturn(versions.length == 0 ? Collections.emptyList() : Arrays.asList(versions)).when(store).getVersions();
    return store;
  }

  private static Store mockStoreWithPromoteTimestamp(int currentVersion, long promoteTimestampMs, Version... versions) {
    Store store = mock(Store.class);
    doReturn(currentVersion).when(store).getCurrentVersion();
    doReturn(promoteTimestampMs).when(store).getLatestVersionPromoteToCurrentTimestamp();
    doReturn(versions.length == 0 ? Collections.emptyList() : Arrays.asList(versions)).when(store).getVersions();
    return store;
  }

  private static Set<String> setOf(String... values) {
    Set<String> out = new HashSet<>();
    Collections.addAll(out, values);
    return out;
  }
}
