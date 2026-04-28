package com.linkedin.venice.controller;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionStatus;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link VeniceHelixAdmin#checkRollbackOriginVersionCapacityForNewPush}.
 *
 * <p>The check blocks a new push while any rollback-origin version is still within its retention
 * window. Past retention, the SOP deletion path reclaims the version.
 */
public class TestRollbackOriginVersionCapacityCheck {
  private static final String CLUSTER_NAME = "test-cluster";
  private static final String STORE_NAME = "test-store";
  private static final long ROLLED_BACK_RETENTION_MS = TimeUnit.HOURS.toMillis(24);

  @Test
  public void testPassesWhenNoRollbackOriginVersions() {
    Store store = mockStoreWithVersions(
        /* currentVersion */ 2,
        /* promoteTimestampMsAgo */ 0,
        mockVersion(1, VersionStatus.ONLINE),
        mockVersion(2, VersionStatus.ONLINE));

    VeniceHelixAdmin.checkRollbackOriginVersionCapacityForNewPush(
        CLUSTER_NAME,
        STORE_NAME,
        store,
        ROLLED_BACK_RETENTION_MS,
        System.currentTimeMillis());
  }

  @Test
  public void testBlocksPushWhenRolledBackVersionExistsWithinRetention() {
    // v5 is ROLLED_BACK, current is v4, promoted 1h ago → within 24h retention → block
    long now = System.currentTimeMillis();
    Store store = mockStoreWithVersions(
        4,
        TimeUnit.HOURS.toMillis(1),
        mockVersion(4, VersionStatus.ONLINE),
        mockVersion(5, VersionStatus.ROLLED_BACK));

    VeniceException e = expectThrows(
        VeniceException.class,
        () -> VeniceHelixAdmin.checkRollbackOriginVersionCapacityForNewPush(
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
  public void testPassesWhenRolledBackVersionPastRetention() {
    // v5 is ROLLED_BACK, promoted 25h ago → past 24h retention → no block
    long now = System.currentTimeMillis();
    Store store = mockStoreWithVersions(
        4,
        TimeUnit.HOURS.toMillis(25),
        mockVersion(4, VersionStatus.ONLINE),
        mockVersion(5, VersionStatus.ROLLED_BACK));

    VeniceHelixAdmin
        .checkRollbackOriginVersionCapacityForNewPush(CLUSTER_NAME, STORE_NAME, store, ROLLED_BACK_RETENTION_MS, now);
  }

  @Test
  public void testBlocksPushWhenPartiallyOnlineVersionAboveCurrentExistsWithinRetention() {
    // Parent-side partial rollback: v5 is PARTIALLY_ONLINE with number > currentVersion (v4)
    long now = System.currentTimeMillis();
    Store store = mockStoreWithVersions(
        4,
        TimeUnit.HOURS.toMillis(1),
        mockVersion(4, VersionStatus.ONLINE),
        mockVersion(5, VersionStatus.PARTIALLY_ONLINE));

    VeniceException e = expectThrows(
        VeniceException.class,
        () -> VeniceHelixAdmin.checkRollbackOriginVersionCapacityForNewPush(
            CLUSTER_NAME,
            STORE_NAME,
            store,
            ROLLED_BACK_RETENTION_MS,
            now));
    assertTrue(e.getMessage().contains("PARTIALLY_ONLINE"), "message should include status: " + e.getMessage());
  }

  @Test
  public void testPassesWhenPartiallyOnlineVersionEqualsCurrentVersion() {
    // Push-origin PARTIALLY_ONLINE: v4 is PARTIALLY_ONLINE and IS the current version → not rollback-origin
    long now = System.currentTimeMillis();
    Store store = mockStoreWithVersions(
        4,
        TimeUnit.HOURS.toMillis(1),
        mockVersion(3, VersionStatus.ONLINE),
        mockVersion(4, VersionStatus.PARTIALLY_ONLINE));

    VeniceHelixAdmin
        .checkRollbackOriginVersionCapacityForNewPush(CLUSTER_NAME, STORE_NAME, store, ROLLED_BACK_RETENTION_MS, now);
  }

  @Test
  public void testPassesWhenPartiallyOnlineBelowCurrentVersion() {
    // Push-origin PARTIALLY_ONLINE on an older version (current was promoted past it) → not rollback-origin
    long now = System.currentTimeMillis();
    Store store = mockStoreWithVersions(
        5,
        TimeUnit.HOURS.toMillis(1),
        mockVersion(4, VersionStatus.PARTIALLY_ONLINE),
        mockVersion(5, VersionStatus.ONLINE));

    VeniceHelixAdmin
        .checkRollbackOriginVersionCapacityForNewPush(CLUSTER_NAME, STORE_NAME, store, ROLLED_BACK_RETENTION_MS, now);
  }

  @Test
  public void testPassesJustPastRetention() {
    // Past retention → check short-circuits, no block.
    // Mock promoteTimestamp explicitly so wall-clock jitter doesn't disturb the boundary.
    long now = 1_000_000L;
    long retention = ROLLED_BACK_RETENTION_MS;
    Store store = mockStoreWithPromoteTimestamp(
        4,
        now - retention - 1, // 1ms past expiry in mocked time
        mockVersion(4, VersionStatus.ONLINE),
        mockVersion(5, VersionStatus.ROLLED_BACK));

    VeniceHelixAdmin.checkRollbackOriginVersionCapacityForNewPush(CLUSTER_NAME, STORE_NAME, store, retention, now);
  }

  @Test
  public void testBlocksJustInsideRetentionBoundary() {
    // 1ms before expiry → still within retention → block
    long now = 1_000_000L;
    long retention = ROLLED_BACK_RETENTION_MS;
    Store store = mockStoreWithPromoteTimestamp(
        4,
        now - retention + 1, // 1ms before expiry in mocked time
        mockVersion(4, VersionStatus.ONLINE),
        mockVersion(5, VersionStatus.ROLLED_BACK));

    expectThrows(
        VeniceException.class,
        () -> VeniceHelixAdmin
            .checkRollbackOriginVersionCapacityForNewPush(CLUSTER_NAME, STORE_NAME, store, retention, now));
  }

  @Test
  public void testBlocksAtExactRetentionBoundary() {
    // At the exact expiry ms (currentTime == retentionExpiresAt), the `>` check is not yet satisfied → block.
    long now = 1_000_000L;
    long retention = ROLLED_BACK_RETENTION_MS;
    Store store = mockStoreWithPromoteTimestamp(
        4,
        now - retention, // exactly at expiry
        mockVersion(4, VersionStatus.ONLINE),
        mockVersion(5, VersionStatus.ROLLED_BACK));

    expectThrows(
        VeniceException.class,
        () -> VeniceHelixAdmin
            .checkRollbackOriginVersionCapacityForNewPush(CLUSTER_NAME, STORE_NAME, store, retention, now));
  }

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
}
