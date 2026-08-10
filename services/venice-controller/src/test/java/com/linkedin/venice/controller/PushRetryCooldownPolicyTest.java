package com.linkedin.venice.controller;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.stats.VeniceAdminStats;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.Version.PushType;
import com.linkedin.venice.meta.VersionStatus;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;
import org.apache.http.HttpStatus;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class PushRetryCooldownPolicyTest {
  private static final String STORE_NAME = "test-store";
  private static final long CURRENT_TIME_MS = 1_000_000;
  private static final long COOLDOWN_MS = 600_000;

  @DataProvider(name = "version-creating-push-types")
  public static Object[][] versionCreatingPushTypes() {
    return new Object[][] { { PushType.BATCH }, { PushType.STREAM_REPROCESSING } };
  }

  @Test(dataProvider = "version-creating-push-types")
  public void testRecentVersionIsRejectedForVersionCreatingPushTypes(PushType pushType) {
    Version recentVersion = mockVersion(2, CURRENT_TIME_MS - 60_000, VersionStatus.ONLINE);
    Store store = mockStore(STORE_NAME, recentVersion);
    VeniceAdminStats stats = mock(VeniceAdminStats.class);

    VeniceHttpException exception = expectThrows(
        VeniceHttpException.class,
        () -> PushRetryCooldownPolicy.enforce(store, pushType, "new-push-id", COOLDOWN_MS, CURRENT_TIME_MS, stats));

    assertEquals(exception.getHttpStatusCode(), HttpStatus.SC_TOO_MANY_REQUESTS);
    assertTrue(exception.getMessage().contains("version 2"));
    assertTrue(exception.getMessage().contains("version-creating pushes must be spaced at least 600000 ms apart"));
    assertTrue(exception.getMessage().contains("Retry in 540000 ms"));
    verify(stats).recordPushRetryCooldownRejection(pushType);
  }

  @DataProvider(name = "persisted-version-statuses")
  public static Object[][] persistedVersionStatuses() {
    return Stream
        .of(
            VersionStatus.STARTED,
            VersionStatus.PUSHED,
            VersionStatus.ONLINE,
            VersionStatus.ERROR,
            VersionStatus.CREATED,
            VersionStatus.PARTIALLY_ONLINE,
            VersionStatus.KILLED,
            VersionStatus.ROLLED_BACK)
        .map(status -> new Object[] { status })
        .toArray(Object[][]::new);
  }

  @Test(dataProvider = "persisted-version-statuses")
  public void testRecentVersionIsRejectedRegardlessOfStatus(VersionStatus status) {
    Version recentVersion = mockVersion(1, CURRENT_TIME_MS - 1, status);
    VeniceAdminStats stats = mock(VeniceAdminStats.class);

    expectThrows(
        VeniceHttpException.class,
        () -> PushRetryCooldownPolicy.enforce(
            mockStore(STORE_NAME, recentVersion),
            PushType.BATCH,
            "new-push-id",
            COOLDOWN_MS,
            CURRENT_TIME_MS,
            stats));

    verify(stats).recordPushRetryCooldownRejection(PushType.BATCH);
  }

  @Test
  public void testMostRecentVersionIsSelectedByCreationTime() {
    Version olderHigherNumberVersion = mockVersion(2, CURRENT_TIME_MS - 120_000, VersionStatus.ERROR);
    Version newerLowerNumberVersion = mockVersion(1, CURRENT_TIME_MS - 60_000, VersionStatus.STARTED);
    VeniceAdminStats stats = mock(VeniceAdminStats.class);

    VeniceHttpException exception = expectThrows(
        VeniceHttpException.class,
        () -> PushRetryCooldownPolicy.enforce(
            mockStore(STORE_NAME, olderHigherNumberVersion, newerLowerNumberVersion),
            PushType.BATCH,
            "new-push-id",
            COOLDOWN_MS,
            CURRENT_TIME_MS,
            stats));

    assertTrue(exception.getMessage().contains("version 1"));
    assertTrue(exception.getMessage().contains("Retry in 540000 ms"));
  }

  @Test
  public void testVersionAtCooldownBoundaryIsAllowed() {
    Version version = mockVersion(1, CURRENT_TIME_MS - COOLDOWN_MS, VersionStatus.ERROR);
    VeniceAdminStats stats = mock(VeniceAdminStats.class);

    PushRetryCooldownPolicy
        .enforce(mockStore(STORE_NAME, version), PushType.BATCH, "new-push-id", COOLDOWN_MS, CURRENT_TIME_MS, stats);

    verify(stats, never()).recordPushRetryCooldownRejection(PushType.BATCH);
  }

  @Test
  public void testZeroCooldownDisablesEnforcement() {
    Version recentVersion = mockVersion(1, CURRENT_TIME_MS - 1, VersionStatus.ERROR);
    VeniceAdminStats stats = mock(VeniceAdminStats.class);

    PushRetryCooldownPolicy
        .enforce(mockStore(STORE_NAME, recentVersion), PushType.BATCH, "new-push-id", 0, CURRENT_TIME_MS, stats);

    verify(stats, never()).recordPushRetryCooldownRejection(PushType.BATCH);
  }

  @DataProvider(name = "non-version-creating-push-types")
  public static Object[][] nonVersionCreatingPushTypes() {
    return new Object[][] { { PushType.INCREMENTAL }, { PushType.STREAM } };
  }

  @Test(dataProvider = "non-version-creating-push-types")
  public void testNonVersionCreatingPushesAreAllowed(PushType pushType) {
    Version recentVersion = mockVersion(1, CURRENT_TIME_MS - 1, VersionStatus.ERROR);
    VeniceAdminStats stats = mock(VeniceAdminStats.class);

    PushRetryCooldownPolicy
        .enforce(mockStore(STORE_NAME, recentVersion), pushType, "new-push-id", COOLDOWN_MS, CURRENT_TIME_MS, stats);

    verify(stats, never()).recordPushRetryCooldownRejection(pushType);
  }

  @DataProvider(name = "system-store-names")
  public static Object[][] systemStoreNames() {
    return Stream
        .concat(
            Arrays.stream(VeniceSystemStoreType.values())
                .map(systemStoreType -> systemStoreType.getSystemStoreName(STORE_NAME)),
            Stream.of(
                VeniceSystemStoreUtils.getParticipantStoreNameForCluster("test-cluster"),
                VeniceSystemStoreUtils.getPushJobDetailsStoreName(),
                VeniceSystemStoreUtils.getParentControllerMetadataStoreNameForCluster("test-cluster")))
        .map(systemStoreName -> new Object[] { systemStoreName })
        .toArray(Object[][]::new);
  }

  @Test(dataProvider = "system-store-names")
  public void testSystemStoresAreAllowed(String systemStoreName) {
    Version recentVersion = mockVersion(1, CURRENT_TIME_MS - 1, VersionStatus.ERROR);
    VeniceAdminStats stats = mock(VeniceAdminStats.class);

    PushRetryCooldownPolicy.enforce(
        mockStore(systemStoreName, recentVersion),
        PushType.BATCH,
        "new-push-id",
        COOLDOWN_MS,
        CURRENT_TIME_MS,
        stats);

    verify(stats, never()).recordPushRetryCooldownRejection(PushType.BATCH);
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
