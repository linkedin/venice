package com.linkedin.venice.controller;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.stats.VeniceAdminStats;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.Version.PushType;
import com.linkedin.venice.meta.VersionStatus;
import java.util.Arrays;
import java.util.Collections;
import org.apache.http.HttpStatus;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class FailedPushRetryCooldownPolicyTest {
  private static final String STORE_NAME = "test-store";
  private static final long CURRENT_TIME_MS = 1_000_000;
  private static final long COOLDOWN_MS = 600_000;

  @DataProvider(name = "failed-push-types-and-statuses")
  public static Object[][] failedPushTypesAndStatuses() {
    return new Object[][] { { PushType.BATCH, VersionStatus.ERROR }, { PushType.BATCH, VersionStatus.KILLED },
        { PushType.STREAM_REPROCESSING, VersionStatus.ERROR } };
  }

  @Test(dataProvider = "failed-push-types-and-statuses")
  public void testRecentFailedPushIsRejected(PushType pushType, VersionStatus failedStatus) {
    Version failedVersion = mockVersion(2, CURRENT_TIME_MS - 60_000, failedStatus);
    Store store = mockStore(STORE_NAME, failedVersion);
    VeniceAdminStats stats = mock(VeniceAdminStats.class);

    VeniceHttpException exception = expectThrows(
        VeniceHttpException.class,
        () -> FailedPushRetryCooldownPolicy
            .enforce(store, pushType, "new-push-id", COOLDOWN_MS, CURRENT_TIME_MS, stats));

    assertEquals(exception.getHttpStatusCode(), HttpStatus.SC_TOO_MANY_REQUESTS);
    assertTrue(exception.getMessage().contains("version 2"));
    assertTrue(exception.getMessage().contains(failedStatus.toString()));
    assertTrue(exception.getMessage().contains("Retry in 540000 ms"));
    verify(stats).recordFailedPushRetryCooldownRejection(pushType);
  }

  @Test
  public void testFailedPushOutsideCooldownIsAllowed() {
    Version failedVersion = mockVersion(1, CURRENT_TIME_MS - COOLDOWN_MS, VersionStatus.ERROR);
    VeniceAdminStats stats = mock(VeniceAdminStats.class);

    FailedPushRetryCooldownPolicy.enforce(
        mockStore(STORE_NAME, failedVersion),
        PushType.BATCH,
        "new-push-id",
        COOLDOWN_MS,
        CURRENT_TIME_MS,
        stats);

    verify(stats, never()).recordFailedPushRetryCooldownRejection(PushType.BATCH);
  }

  @DataProvider(name = "non-failed-terminal-statuses")
  public static Object[][] nonFailedTerminalStatuses() {
    return new Object[][] { { VersionStatus.PUSHED }, { VersionStatus.ONLINE }, { VersionStatus.PARTIALLY_ONLINE },
        { VersionStatus.ROLLED_BACK } };
  }

  @Test(dataProvider = "non-failed-terminal-statuses")
  public void testMoreRecentNonFailedTerminalPushAllowsRetry(VersionStatus latestStatus) {
    Version recentFailure = mockVersion(1, CURRENT_TIME_MS - 2_000, VersionStatus.ERROR);
    Version moreRecentTerminalVersion = mockVersion(2, CURRENT_TIME_MS - 1_000, latestStatus);
    VeniceAdminStats stats = mock(VeniceAdminStats.class);

    FailedPushRetryCooldownPolicy.enforce(
        mockStore(STORE_NAME, recentFailure, moreRecentTerminalVersion),
        PushType.BATCH,
        "new-push-id",
        COOLDOWN_MS,
        CURRENT_TIME_MS,
        stats);

    verify(stats, never()).recordFailedPushRetryCooldownRejection(PushType.BATCH);
  }

  @Test
  public void testZeroCooldownDisablesEnforcement() {
    Version failedVersion = mockVersion(1, CURRENT_TIME_MS - 1, VersionStatus.ERROR);
    VeniceAdminStats stats = mock(VeniceAdminStats.class);

    FailedPushRetryCooldownPolicy
        .enforce(mockStore(STORE_NAME, failedVersion), PushType.BATCH, "new-push-id", 0, CURRENT_TIME_MS, stats);

    verify(stats, never()).recordFailedPushRetryCooldownRejection(PushType.BATCH);
  }

  @DataProvider(name = "excluded-pushes")
  public static Object[][] excludedPushes() {
    return new Object[][] { { STORE_NAME, PushType.INCREMENTAL }, { STORE_NAME, PushType.STREAM },
        { VeniceSystemStoreType.META_STORE.getSystemStoreName(STORE_NAME), PushType.BATCH } };
  }

  @Test(dataProvider = "excluded-pushes")
  public void testExcludedPushesAreAllowed(String storeName, PushType pushType) {
    Version failedVersion = mockVersion(1, CURRENT_TIME_MS - 1, VersionStatus.ERROR);
    VeniceAdminStats stats = mock(VeniceAdminStats.class);

    FailedPushRetryCooldownPolicy
        .enforce(mockStore(storeName, failedVersion), pushType, "new-push-id", COOLDOWN_MS, CURRENT_TIME_MS, stats);

    verify(stats, never()).recordFailedPushRetryCooldownRejection(pushType);
  }

  private static Store mockStore(String storeName, Version... versions) {
    Store store = mock(Store.class);
    when(store.getName()).thenReturn(storeName);
    when(store.getVersions()).thenReturn(versions.length == 0 ? Collections.emptyList() : Arrays.asList(versions));
    return store;
  }

  private static Version mockVersion(int number, long createdTime, VersionStatus status) {
    Version version = mock(Version.class);
    when(version.getNumber()).thenReturn(number);
    when(version.getCreatedTime()).thenReturn(createdTime);
    when(version.getStatus()).thenReturn(status);
    return version;
  }
}
