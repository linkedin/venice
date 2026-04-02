package com.linkedin.venice.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.TestUtils;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.testng.annotations.Test;


/**
 * Tests that demonstrate backup version retention violations using real Store and Version objects.
 *
 * Goal: retain at least 1 backup version for 7 days across all deletion paths.
 *
 * These tests exercise {@link com.linkedin.venice.meta.AbstractStore#retrieveVersionsToDelete(int)} and
 * {@link StoreBackupVersionCleanupService#whetherStoreReadyToBeCleanup(Store, long, com.linkedin.venice.utils.Time, int, long)}
 * which contain the core deletion decision logic. No mocking is used — all objects are real.
 *
 * Note: the actual deletion execution (deleteOneStoreVersion, deleteOldVersionInStore) requires a running
 * controller cluster and is covered by integration tests in TestVeniceHelixAdminWithSharedEnvironment.
 */
public class TestBackupVersionRetentionViolations {
  private static final long SEVEN_DAYS_MS = TimeUnit.DAYS.toMillis(7);
  private static final long ONE_HOUR_MS = TimeUnit.HOURS.toMillis(1);

  /**
   * Helper: create a store with versions, set current version, and set version statuses.
   */
  private Store createStoreWithVersions(String storeName, int currentVersion, int[][] versionDefs) {
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    for (int[] def: versionDefs) {
      int versionNumber = def[0];
      int statusOrdinal = def[1];
      Version version = new VersionImpl(storeName, versionNumber);
      version.setStatus(VersionStatus.getVersionStatusFromInt(statusOrdinal));
      store.addVersion(version);
    }
    store.setCurrentVersion(currentVersion);
    return store;
  }

  private List<Integer> getDeletedVersionNumbers(Store store, int numVersionsToPreserve) {
    return store.retrieveVersionsToDelete(numVersionsToPreserve)
        .stream()
        .map(Version::getNumber)
        .collect(Collectors.toList());
  }

  // ==================================================================================
  // VIOLATION 1: ROLLED_BACK version deleted by count-based cleanup immediately
  // (no time retention applied by retrieveVersionsToDelete)
  // ==================================================================================

  /**
   * When a push completes after a rollback, count-based cleanup deletes the ROLLED_BACK version
   * immediately because canDelete(ROLLED_BACK) = true. No retention is enforced.
   *
   * State: v2 (ONLINE, current after rollback), v3 (ROLLED_BACK), v4 (ONLINE, new current)
   * Expected: v3 should survive 7 days for rollforward capability
   * Actual: v3 is deleted immediately
   */
  @Test
  public void testViolation_RolledBackDeletedByCountBasedCleanupImmediately() {
    Store store = createStoreWithVersions(
        "test_rollback_count",
        4,
        new int[][] { { 2, VersionStatus.ONLINE.getValue() }, { 3, VersionStatus.ROLLED_BACK.getValue() },
            { 4, VersionStatus.ONLINE.getValue() } });

    List<Integer> deleted = getDeletedVersionNumbers(store, 2);

    // VIOLATION: v3 (ROLLED_BACK) is in the delete list with no retention check
    assertTrue(deleted.contains(3), "VIOLATION: ROLLED_BACK version deleted immediately by count-based cleanup");
  }

  /**
   * Same as above, but after a rollback with no new push. Count-based is triggered with
   * deleteBackupOnStartPush=true (early delete) when a new push starts.
   *
   * State: v2 (ONLINE, current after rollback), v3 (ROLLED_BACK)
   * retrieveVersionsToDelete(1) — adjusted for push start
   */
  @Test
  public void testViolation_RolledBackDeletedByEarlyDeleteOnPushStart() {
    Store store = createStoreWithVersions(
        "test_rollback_early",
        2,
        new int[][] { { 2, VersionStatus.ONLINE.getValue() }, { 3, VersionStatus.ROLLED_BACK.getValue() } });

    // Early delete: retrieveVersionsToDelete(1) — preserve count adjusted by -1 for new push
    List<Integer> deleted = getDeletedVersionNumbers(store, 1);

    // VIOLATION: v3 (ROLLED_BACK) deleted immediately on push start
    assertTrue(deleted.contains(3), "VIOLATION: ROLLED_BACK version deleted on push start with zero retention");
  }

  // ==================================================================================
  // VIOLATION 2: ROLLED_BACK version eligible for time-based cleanup after only 1 hour
  // (whetherStoreReadyToBeCleanup passes, canDelete fast path applies)
  // ==================================================================================

  /**
   * The time-based cleanup's whetherStoreReadyToBeCleanup check passes after 1 hour (or after
   * retention), and then canDelete(ROLLED_BACK) = true causes immediate deletion via the fast path.
   *
   * State: v2 (ONLINE, current), v3 (ROLLED_BACK)
   * Promoted 2 hours ago (past 1-hour minimum, within 7-day retention)
   */
  @Test
  public void testViolation_RolledBackEligibleForTimeBasedCleanupAfterOneHour() {
    Store store = createStoreWithVersions(
        "test_rollback_timebased",
        2,
        new int[][] { { 2, VersionStatus.ONLINE.getValue() }, { 3, VersionStatus.ROLLED_BACK.getValue() } });

    // Promote timestamp 2 hours ago — past 1-hour minimum but within 7-day retention
    long twoHoursAgo = System.currentTimeMillis() - 2 * ONE_HOUR_MS;
    store.setLatestVersionPromoteToCurrentTimestamp(twoHoursAgo);

    // whetherStoreReadyToBeCleanup should return false (within 7-day retention, only 1 version below current)
    // But actually: v3 is ROLLED_BACK with number > current (3 > 2), so it's NOT counted as "below current"
    // The check at line 165 counts versions with number < currentVersion
    // So count of versions below current = 0 (only v3 exists, and 3 > 2)
    // This means retention check applies: twoHoursAgo + 7days > now → NOT ready
    boolean ready = StoreBackupVersionCleanupService
        .whetherStoreReadyToBeCleanup(store, SEVEN_DAYS_MS, new SystemTime(), 2, ONE_HOUR_MS);

    // Store is not ready (retention hasn't passed) — but if it WERE ready,
    // canDelete(ROLLED_BACK) would cause immediate deletion via fast path
    assertFalse(ready, "Store should not be ready within 7-day retention with only 1 backup");

    // Now set promote timestamp to 8 days ago — past retention
    long eightDaysAgo = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(8);
    store.setLatestVersionPromoteToCurrentTimestamp(eightDaysAgo);

    ready = StoreBackupVersionCleanupService
        .whetherStoreReadyToBeCleanup(store, SEVEN_DAYS_MS, new SystemTime(), 2, ONE_HOUR_MS);

    // Now ready — and cleanupBackupVersion would delete v3 via canDelete fast path
    assertTrue(ready, "Store should be ready after 7-day retention");

    // The actual deletion in cleanupBackupVersion would use canDelete(ROLLED_BACK) = true
    // to delete v3 without going through the retention-based version selection logic.
    // This is correct after 7 days, but the problem is that canDelete fast path
    // doesn't distinguish "retention passed" from "just past 1-hour minimum"
  }

  // ==================================================================================
  // VIOLATION 3: Early delete on push start removes ALL ONLINE backups
  // (zero backups remain, zero retention)
  // ==================================================================================

  /**
   * With default minNumberOfStoreVersionsToPreserve=2, early delete passes
   * retrieveVersionsToDelete(2-1=1). The current version takes the entire budget (1-1=0),
   * leaving zero slots for ONLINE backups. ALL ONLINE backups are deleted.
   */
  @Test
  public void testViolation_EarlyDeleteRemovesAllOnlineBackups() {
    Store store = createStoreWithVersions(
        "test_early_delete",
        3,
        new int[][] { { 1, VersionStatus.ONLINE.getValue() }, { 2, VersionStatus.ONLINE.getValue() },
            { 3, VersionStatus.ONLINE.getValue() } });

    // Simulate early delete: retrieveVersionsToDelete(1) — adjusted for push start (2-1=1)
    List<Integer> deleted = getDeletedVersionNumbers(store, 1);

    // VIOLATION: both v1 and v2 are deleted — all backups gone before new push even starts
    assertTrue(deleted.contains(1), "v1 should be deleted");
    assertTrue(deleted.contains(2), "v2 should be deleted");
    assertFalse(deleted.contains(3), "Current version should never be deleted");
    assertEquals(deleted.size(), 2, "VIOLATION: ALL backup versions deleted on push start");
  }

  /**
   * Even with only 1 backup, early delete removes it.
   */
  @Test
  public void testViolation_EarlyDeleteRemovesSingleBackup() {
    Store store = createStoreWithVersions(
        "test_early_delete_single",
        3,
        new int[][] { { 2, VersionStatus.ONLINE.getValue() }, { 3, VersionStatus.ONLINE.getValue() } });

    List<Integer> deleted = getDeletedVersionNumbers(store, 1);

    // VIOLATION: the only backup (v2) is deleted on push start
    assertTrue(deleted.contains(2), "VIOLATION: Only backup version deleted on push start");
    assertEquals(deleted.size(), 1);
  }

  // ==================================================================================
  // VIOLATION 4: Rapid pushes replace backups without any time retention
  // (count-based preserves 1, but rotates it immediately)
  // ==================================================================================

  /**
   * With rapid successive pushes, backups are rotated immediately by count-based cleanup.
   * A backup that was created minutes ago is deleted when the next push completes.
   * No time-based retention is enforced.
   */
  @Test
  public void testViolation_RapidPushesRotateBackupsWithoutRetention() {
    // After v3 completes: v1 deleted, v2 kept as backup
    Store store = createStoreWithVersions(
        "test_rapid_pushes",
        3,
        new int[][] { { 1, VersionStatus.ONLINE.getValue() }, { 2, VersionStatus.ONLINE.getValue() },
            { 3, VersionStatus.ONLINE.getValue() } });

    List<Integer> deleted = getDeletedVersionNumbers(store, 2);
    assertTrue(deleted.contains(1), "v1 deleted by count");
    assertFalse(deleted.contains(2), "v2 preserved as backup");

    // Simulate v4 completing immediately after v3
    store.deleteVersion(1);
    Version v4 = new VersionImpl("test_rapid_pushes", 4);
    v4.setStatus(VersionStatus.ONLINE);
    store.addVersion(v4);
    store.setCurrentVersion(4);

    deleted = getDeletedVersionNumbers(store, 2);

    // VIOLATION: v2 is now deleted — it was a backup for potentially only seconds
    assertTrue(deleted.contains(2), "VIOLATION: Previous backup deleted immediately by next push with no retention");
    assertFalse(deleted.contains(3), "v3 should be preserved as new backup");
  }

  // ==================================================================================
  // VIOLATION 5: Double rollback leaves zero backup versions
  // ==================================================================================

  /**
   * After two consecutive rollbacks, both rolled-back versions are deleted via canDelete,
   * leaving only the current version with zero backups.
   */
  @Test
  public void testViolation_DoubleRollbackLeavesZeroBackups() {
    // After first rollback: v2 current, v3 ROLLED_BACK
    // Count-based cleanup (e.g., triggered by a new push completing):
    Store store = createStoreWithVersions(
        "test_double_rollback",
        2,
        new int[][] { { 1, VersionStatus.ONLINE.getValue() }, { 2, VersionStatus.ONLINE.getValue() },
            { 3, VersionStatus.ROLLED_BACK.getValue() } });

    List<Integer> deleted = getDeletedVersionNumbers(store, 2);
    assertTrue(deleted.contains(3), "v3 (ROLLED_BACK) deleted");
    assertFalse(deleted.contains(1), "v1 preserved as backup");

    // After second rollback: v1 current, v2 ROLLED_BACK
    store.deleteVersion(3);
    store.updateVersionStatus(2, VersionStatus.ROLLED_BACK);
    store.setCurrentVersion(1);

    deleted = getDeletedVersionNumbers(store, 2);

    // VIOLATION: v2 (ROLLED_BACK) deleted, leaving v1 with zero backups
    assertTrue(deleted.contains(2), "VIOLATION: Second ROLLED_BACK deleted, zero backups remain");
    assertEquals(store.getVersions().size() - deleted.size(), 1, "Only current version survives");
  }

  // ==================================================================================
  // VIOLATION 6: KILLED version treated same as ERROR for count-based cleanup
  // (but not immediately deleted like ERROR — inconsistent handling)
  // ==================================================================================

  /**
   * KILLED versions are always deleted by count-based cleanup (canDelete=true) but unlike ERROR
   * versions, they are NOT immediately deleted by the push monitor. They wait for the next
   * cleanup trigger. This is inconsistent but not a retention violation per se — included
   * for completeness.
   */
  @Test
  public void testKilledVersionAlwaysDeletedByCountBased() {
    Store store = createStoreWithVersions(
        "test_killed",
        3,
        new int[][] { { 2, VersionStatus.ONLINE.getValue() }, { 3, VersionStatus.ONLINE.getValue() },
            { 4, VersionStatus.KILLED.getValue() } });
    store.setCurrentVersion(3);

    List<Integer> deleted = getDeletedVersionNumbers(store, 2);

    // KILLED is always deleted regardless of preserve count — same as ERROR
    assertTrue(deleted.contains(4), "KILLED version always deleted by count-based");
    assertFalse(deleted.contains(2), "ONLINE backup preserved");
  }

  // ==================================================================================
  // VIOLATION 7: Mixed statuses — canDelete versions bypass preserve count entirely
  // ==================================================================================

  /**
   * canDelete() versions (ERROR, KILLED, ROLLED_BACK) do NOT count against the preserve limit.
   * They are always deleted regardless of how many versions would remain. Combined with
   * the preserve count consuming slots for the current version, this can leave zero backups.
   */
  @Test
  public void testViolation_CanDeleteVersionsBypassPreserveCount() {
    // v2 (ONLINE), v3 (ROLLED_BACK), v4 (KILLED), v5 (ONLINE, current)
    Store store = createStoreWithVersions(
        "test_mixed_candelete",
        5,
        new int[][] { { 2, VersionStatus.ONLINE.getValue() }, { 3, VersionStatus.ROLLED_BACK.getValue() },
            { 4, VersionStatus.KILLED.getValue() }, { 5, VersionStatus.ONLINE.getValue() } });

    // Normal count-based cleanup (preserve=2)
    List<Integer> deleted = getDeletedVersionNumbers(store, 2);

    // v3 and v4 are always deleted (canDelete=true), regardless of count
    assertTrue(deleted.contains(3), "ROLLED_BACK always deleted");
    assertTrue(deleted.contains(4), "KILLED always deleted");
    // v2 preserved as the 1 allowed ONLINE backup
    assertFalse(deleted.contains(2), "v2 should be preserved as ONLINE backup");

    // Now with early delete (preserve=1): v2 would also be deleted
    deleted = getDeletedVersionNumbers(store, 1);
    assertTrue(deleted.contains(2), "VIOLATION: v2 also deleted with early delete, zero ONLINE backups");
    assertTrue(deleted.contains(3), "ROLLED_BACK deleted");
    assertTrue(deleted.contains(4), "KILLED deleted");
    assertEquals(deleted.size(), 3, "All non-current versions deleted");
  }

  // ==================================================================================
  // REPUSH VIOLATIONS
  // ==================================================================================

  // ==================================================================================
  // VIOLATION 8: Repush source version gets only 1-hour retention instead of 7 days
  // (whetherStoreReadyToBeCleanup uses waitTimeDeleteRepushSourceVersion)
  // ==================================================================================

  /**
   * When the current version is a repush, whetherStoreReadyToBeCleanup uses
   * waitTimeDeleteRepushSourceVersion (1 hour) instead of the configured retention (7 days).
   * This means backup versions for repush stores become eligible for cleanup after only 1 hour.
   *
   * State: v1 (ONLINE, backup), v2 (ONLINE, current, repush from v1)
   * Expected: v1 should be retained for 7 days
   * Actual: cleanup becomes eligible after 1 hour
   */
  @Test
  public void testViolation_RepushSourceVersionGetsOneHourRetentionInsteadOfSevenDays() {
    String storeName = "test_repush_retention";
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());

    Version v1 = new VersionImpl(storeName, 1);
    v1.setStatus(VersionStatus.ONLINE);
    store.addVersion(v1);

    Version v2 = new VersionImpl(storeName, 2);
    v2.setStatus(VersionStatus.ONLINE);
    v2.setRepushSourceVersion(1); // v2 is a repush from v1
    store.addVersion(v2);
    store.setCurrentVersion(2);

    // Set promote timestamp to 2 hours ago — past 1-hour repush retention, within 7-day retention
    long twoHoursAgo = System.currentTimeMillis() - 2 * ONE_HOUR_MS;
    store.setLatestVersionPromoteToCurrentTimestamp(twoHoursAgo);

    // VIOLATION: store is ready for cleanup after only 2 hours (because repush uses 1-hour retention)
    boolean ready = StoreBackupVersionCleanupService
        .whetherStoreReadyToBeCleanup(store, SEVEN_DAYS_MS, new SystemTime(), 2, ONE_HOUR_MS);
    assertTrue(ready, "VIOLATION: Repush source version eligible for cleanup after 1 hour instead of 7 days");

    // Verify: with a non-repush current version, the store would NOT be ready
    v2.setRepushSourceVersion(0); // make it a regular push
    store.setLatestVersionPromoteToCurrentTimestamp(twoHoursAgo);

    ready = StoreBackupVersionCleanupService
        .whetherStoreReadyToBeCleanup(store, SEVEN_DAYS_MS, new SystemTime(), 2, ONE_HOUR_MS);
    assertFalse(ready, "Non-repush version correctly waits for 7-day retention");
  }

  // ==================================================================================
  // VIOLATION 9: Repush versions accumulate because count-based cleanup is skipped
  // (only time-based handles repush cleanup, which may be disabled)
  // ==================================================================================

  /**
   * When repushes complete, handleCompletedPush skips retireOldStoreVersions (line 1115).
   * This means count-based cleanup never runs for repush completions.
   * If time-based cleanup is disabled (the default!), repush versions accumulate indefinitely.
   *
   * This test demonstrates that retrieveVersionsToDelete WOULD delete old versions if called,
   * but the repush completion path never calls it.
   */
  @Test
  public void testViolation_RepushVersionsAccumulateWhenTimeBasedCleanupDisabled() {
    // Simulate: 5 repushes completed, no cleanup ran (time-based disabled)
    String storeName = "test_repush_accumulation";
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());

    for (int i = 1; i <= 5; i++) {
      Version v = new VersionImpl(storeName, i);
      v.setStatus(VersionStatus.ONLINE);
      if (i > 1) {
        v.setRepushSourceVersion(i - 1);
      }
      store.addVersion(v);
    }
    store.setCurrentVersion(5);

    // retrieveVersionsToDelete WOULD delete old versions...
    List<Integer> deleted = getDeletedVersionNumbers(store, 2);
    assertTrue(deleted.size() > 0, "retrieveVersionsToDelete would clean up old versions");

    // ...but it's never called for repush completions (AbstractPushMonitor line 1115).
    // If CONTROLLER_BACKUP_VERSION_RETENTION_BASED_CLEANUP_ENABLED is false (the default),
    // these versions accumulate indefinitely.
    //
    // The only way they get cleaned up is:
    // 1. Time-based cleanup (if enabled) — handles repush chains
    // 2. A non-repush push completes — triggers count-based cleanup
    // 3. A non-repush push starts with DELETE_ON_NEW_PUSH_START — triggers early delete
    assertEquals(store.getVersions().size(), 5, "All 5 versions still exist without cleanup");
  }

  // ==================================================================================
  // VIOLATION 10: Count-based cleanup doesn't distinguish repush from user push
  // (when it IS triggered, repush versions are treated identically)
  // ==================================================================================

  /**
   * When count-based cleanup IS triggered (by a non-repush push completing),
   * it doesn't distinguish between user-pushed versions and repush versions.
   * It preserves the newest ONLINE versions regardless of origin.
   * This means a user's data version can be deleted to make room for repush versions.
   *
   * State: v1 (ONLINE, user push), v2 (ONLINE, repush), v3 (ONLINE, repush), v4 (ONLINE, current, user push)
   * Expected: preserve v1 (user data) over v2/v3 (repushes)
   * Actual: v1 deleted (oldest), v3 preserved (newest backup)
   */
  @Test
  public void testViolation_CountBasedDeletesUserVersionInsteadOfRepush() {
    String storeName = "test_repush_priority";
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());

    Version v1 = new VersionImpl(storeName, 1);
    v1.setStatus(VersionStatus.ONLINE);
    // v1 is a user push (repushSourceVersion = 0)
    store.addVersion(v1);

    Version v2 = new VersionImpl(storeName, 2);
    v2.setStatus(VersionStatus.ONLINE);
    v2.setRepushSourceVersion(1); // repush
    store.addVersion(v2);

    Version v3 = new VersionImpl(storeName, 3);
    v3.setStatus(VersionStatus.ONLINE);
    v3.setRepushSourceVersion(2); // repush
    store.addVersion(v3);

    Version v4 = new VersionImpl(storeName, 4);
    v4.setStatus(VersionStatus.ONLINE);
    // v4 is a user push (repushSourceVersion = 0)
    store.addVersion(v4);
    store.setCurrentVersion(4);

    // Count-based cleanup triggered by v4 (non-repush) completing
    List<Integer> deleted = getDeletedVersionNumbers(store, 2);

    // VIOLATION: v1 (user data) and v2 (repush) are deleted.
    // v3 (repush) is preserved as the backup — even though v1 is the user's data.
    // There is no priority system to preserve user pushes over repushes.
    assertTrue(deleted.contains(1), "v1 (user push) deleted — no push type priority");
    assertTrue(deleted.contains(2), "v2 (repush) deleted");
    assertFalse(deleted.contains(3), "v3 (repush) preserved as newest backup");
    assertFalse(deleted.contains(4), "v4 (current) preserved");
  }

  // ==================================================================================
  // VIOLATION 11: Frequent repushes reset retention clock for unrelated backup versions
  // ==================================================================================

  /**
   * Every call to setCurrentVersion resets latestVersionPromoteToCurrentTimestamp.
   * Frequent NON-repush pushes keep resetting this clock, preventing backup versions
   * from reaching their 7-day retention threshold.
   *
   * The retention reset is most impactful for non-repush pushes (7-day retention).
   * For repushes the retention is only 1 hour, so the reset matters less. But for
   * stores that mix regular pushes with other activity, any version promotion resets
   * the clock for ALL backups.
   *
   * This test uses a scenario with exactly 1 version below current (so the >1 bypass
   * doesn't trigger) to demonstrate the reset.
   */
  @Test
  public void testViolation_VersionPromotionResetsRetentionClockForAllBackups() {
    String storeName = "test_clock_reset";
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());

    Version v1 = new VersionImpl(storeName, 1);
    v1.setStatus(VersionStatus.ONLINE);
    store.addVersion(v1);

    Version v2 = new VersionImpl(storeName, 2);
    v2.setStatus(VersionStatus.ONLINE);
    store.addVersion(v2);
    store.setCurrentVersion(2);

    // v2 was promoted 8 days ago — v1 should be eligible for cleanup (past 7-day retention)
    long eightDaysAgo = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(8);
    store.setLatestVersionPromoteToCurrentTimestamp(eightDaysAgo);

    boolean readyBeforePush = StoreBackupVersionCleanupService
        .whetherStoreReadyToBeCleanup(store, SEVEN_DAYS_MS, new SystemTime(), 2, ONE_HOUR_MS);
    assertTrue(readyBeforePush, "Should be ready — 8 days past retention");

    // Now a new push completes: v3 becomes current
    // First delete v1 (simulating count-based cleanup from v3 completion removed v1)
    // Then we're left with v2 (backup) and v3 (current) — only 1 below current
    store.deleteVersion(1);
    Version v3 = new VersionImpl(storeName, 3);
    v3.setStatus(VersionStatus.ONLINE);
    store.addVersion(v3);
    store.setCurrentVersion(3);
    // setCurrentVersion resets latestVersionPromoteToCurrentTimestamp to now

    // v2 has been a backup since v2 was promoted 8 days ago.
    // But the retention clock was just reset to now.
    boolean readyAfterPush = StoreBackupVersionCleanupService
        .whetherStoreReadyToBeCleanup(store, SEVEN_DAYS_MS, new SystemTime(), 3, ONE_HOUR_MS);

    // VIOLATION: v2 is no longer eligible for cleanup even though it's been a backup
    // for 8+ days. The new push reset latestVersionPromoteToCurrentTimestamp to now.
    // v2 won't be eligible for another 7 days.
    assertFalse(readyAfterPush, "VIOLATION: New push resets retention clock — v2 (old backup) is no longer eligible");
  }

  // ==================================================================================
  // VIOLATION 12: Repush after rollback — repush deletes rolled-back version with
  // only 1-hour retention
  // ==================================================================================

  /**
   * After a rollback, if a repush is triggered, the repush source version retention (1 hour)
   * applies. Combined with canDelete(ROLLED_BACK), the rolled-back version is deleted quickly.
   */
  @Test
  public void testViolation_RepushAfterRollbackDeletesRolledBackWithOneHourRetention() {
    String storeName = "test_repush_after_rollback";
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());

    Version v2 = new VersionImpl(storeName, 2);
    v2.setStatus(VersionStatus.ONLINE);
    store.addVersion(v2);

    // v3 was rolled back
    Version v3 = new VersionImpl(storeName, 3);
    v3.setStatus(VersionStatus.ROLLED_BACK);
    store.addVersion(v3);

    // v4 is a repush from v2 (the current version after rollback), now current
    Version v4 = new VersionImpl(storeName, 4);
    v4.setStatus(VersionStatus.ONLINE);
    v4.setRepushSourceVersion(2);
    store.addVersion(v4);
    store.setCurrentVersion(4);

    // Promoted 2 hours ago — past 1-hour repush retention
    long twoHoursAgo = System.currentTimeMillis() - 2 * ONE_HOUR_MS;
    store.setLatestVersionPromoteToCurrentTimestamp(twoHoursAgo);

    // whetherStoreReadyToBeCleanup: >1 versions below current (v2 and v3 — but wait,
    // v3 has number 3 < 4, so it IS below current)
    boolean ready = StoreBackupVersionCleanupService
        .whetherStoreReadyToBeCleanup(store, SEVEN_DAYS_MS, new SystemTime(), 4, ONE_HOUR_MS);
    assertTrue(ready, "Store ready — >1 versions below current OR past 1-hour repush retention");

    // If cleanupBackupVersion ran, canDelete(ROLLED_BACK) fast path would catch v3
    // v3 is deleted with no 7-day retention — just 1 hour because it's a repush store

    // Count-based would also delete v3 immediately
    List<Integer> deleted = getDeletedVersionNumbers(store, 2);
    assertTrue(deleted.contains(3), "VIOLATION: ROLLED_BACK v3 deleted — no 7-day retention in repush context");
    // v2 is preserved as backup
    assertFalse(deleted.contains(2), "v2 preserved as ONLINE backup");
  }
}
