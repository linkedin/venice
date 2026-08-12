package com.linkedin.venice.controller;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.stats.VeniceAdminStats;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.ReadStrategy;
import com.linkedin.venice.meta.RoutingStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.Version.PushType;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.http.HttpStatus;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class VersionCreationAttemptCooldownPolicyTest {
  private static final String STORE_NAME = "test-store";
  private static final long CURRENT_TIME_MS = 1_000_000;
  private static final long COOLDOWN_MS = 600_000;

  @DataProvider(name = "version-creating-push-types")
  public static Object[][] versionCreatingPushTypes() {
    return new Object[][] { { PushType.BATCH }, { PushType.STREAM_REPROCESSING } };
  }

  @Test(dataProvider = "version-creating-push-types")
  public void testNoPriorStateAllowsAndReserves(PushType pushType) {
    Store store = createStore(STORE_NAME);

    assertTrue(
        VersionCreationAttemptCooldownPolicy
            .checkAndReserve(store, pushType, "new-push-id", COOLDOWN_MS, CURRENT_TIME_MS, null));
    assertEquals(store.getLastVersionCreationAttemptTimestampMs(), CURRENT_TIME_MS);
    assertEquals(store.getLastVersionCreationAttemptPushJobId(), "new-push-id");
  }

  @Test(dataProvider = "version-creating-push-types")
  public void testRecentMarkerWithDifferentPushIdIsRejected(PushType pushType) {
    Store store = createStore(STORE_NAME);
    store.setLastVersionCreationAttemptTimestampMs(CURRENT_TIME_MS - 60_000);
    store.setLastVersionCreationAttemptPushJobId("previous-push-id");
    VeniceAdminStats stats = mock(VeniceAdminStats.class);

    VeniceHttpException exception = expectThrows(
        VeniceHttpException.class,
        () -> VersionCreationAttemptCooldownPolicy
            .checkAndReserve(store, pushType, "new-push-id", COOLDOWN_MS, CURRENT_TIME_MS, stats));

    assertEquals(exception.getHttpStatusCode(), HttpStatus.SC_TOO_MANY_REQUESTS);
    assertTrue(exception.getMessage().contains("version-creation attempts must be spaced at least 600000 ms apart"));
    assertTrue(exception.getMessage().contains("Retry in 540000 ms"));
    verify(stats).recordVersionCreationAttemptCooldownRejection(pushType);
  }

  @Test
  public void testSameMarkerPushIdIsAllowedWithoutSlidingTimestamp() {
    Store store = createStore(STORE_NAME);
    long attemptTimeMs = CURRENT_TIME_MS - 60_000;
    store.setLastVersionCreationAttemptTimestampMs(attemptTimeMs);
    store.setLastVersionCreationAttemptPushJobId("same-push-id");
    VeniceAdminStats stats = mock(VeniceAdminStats.class);

    assertFalse(
        VersionCreationAttemptCooldownPolicy
            .checkAndReserve(store, PushType.BATCH, "same-push-id", COOLDOWN_MS, CURRENT_TIME_MS, stats));

    assertEquals(store.getLastVersionCreationAttemptTimestampMs(), attemptTimeMs);
    assertEquals(store.getLastVersionCreationAttemptPushJobId(), "same-push-id");
    verify(stats, never()).recordVersionCreationAttemptCooldownRejection(PushType.BATCH);
  }

  @Test
  public void testRejectedAttemptDoesNotSlideWindow() {
    Store store = createStore(STORE_NAME);
    long attemptTimeMs = CURRENT_TIME_MS - 60_000;
    store.setLastVersionCreationAttemptTimestampMs(attemptTimeMs);
    store.setLastVersionCreationAttemptPushJobId("previous-push-id");

    expectThrows(
        VeniceHttpException.class,
        () -> VersionCreationAttemptCooldownPolicy
            .checkAndReserve(store, PushType.BATCH, "new-push-id", COOLDOWN_MS, CURRENT_TIME_MS, null));
    expectThrows(
        VeniceHttpException.class,
        () -> VersionCreationAttemptCooldownPolicy
            .checkAndReserve(store, PushType.BATCH, "another-push-id", COOLDOWN_MS, CURRENT_TIME_MS + 1, null));

    assertEquals(store.getLastVersionCreationAttemptTimestampMs(), attemptTimeMs);
    assertEquals(store.getLastVersionCreationAttemptPushJobId(), "previous-push-id");
  }

  @Test
  public void testExpiryBoundaryAllowsAndReserves() {
    Store store = createStore(STORE_NAME);
    store.setLastVersionCreationAttemptTimestampMs(CURRENT_TIME_MS - COOLDOWN_MS);
    store.setLastVersionCreationAttemptPushJobId("previous-push-id");

    assertTrue(
        VersionCreationAttemptCooldownPolicy
            .checkAndReserve(store, PushType.BATCH, "new-push-id", COOLDOWN_MS, CURRENT_TIME_MS, null));
    assertEquals(store.getLastVersionCreationAttemptTimestampMs(), CURRENT_TIME_MS);
    assertEquals(store.getLastVersionCreationAttemptPushJobId(), "new-push-id");
  }

  @Test
  public void testLatestVersionTimestampProvidesUpgradeFallback() {
    Store store = createStore(STORE_NAME);
    store.addVersion(createVersion(1, CURRENT_TIME_MS - 60_000, VersionStatus.ONLINE));

    VeniceHttpException exception = expectThrows(
        VeniceHttpException.class,
        () -> VersionCreationAttemptCooldownPolicy
            .checkAndReserve(store, PushType.BATCH, "new-push-id", COOLDOWN_MS, CURRENT_TIME_MS, null));

    assertTrue(exception.getMessage().contains("Retry in 540000 ms"));
    assertEquals(store.getLastVersionCreationAttemptTimestampMs(), 0);
    assertEquals(store.getLastVersionCreationAttemptPushJobId(), "");
  }

  @Test
  public void testEffectivePriorTimeUsesNewerOfMarkerAndVersion() {
    Store markerNewer = createStore(STORE_NAME);
    markerNewer.setLastVersionCreationAttemptTimestampMs(CURRENT_TIME_MS - 60_000);
    markerNewer.setLastVersionCreationAttemptPushJobId("previous-push-id");
    markerNewer.addVersion(createVersion(1, CURRENT_TIME_MS - 120_000, VersionStatus.ERROR));
    VeniceHttpException markerException = expectThrows(
        VeniceHttpException.class,
        () -> VersionCreationAttemptCooldownPolicy
            .checkAndReserve(markerNewer, PushType.BATCH, "new-push-id", COOLDOWN_MS, CURRENT_TIME_MS, null));
    assertTrue(markerException.getMessage().contains("Retry in 540000 ms"));

    Store versionNewer = createStore(STORE_NAME);
    versionNewer.setLastVersionCreationAttemptTimestampMs(CURRENT_TIME_MS - 120_000);
    versionNewer.setLastVersionCreationAttemptPushJobId("previous-push-id");
    versionNewer.addVersion(createVersion(1, CURRENT_TIME_MS - 60_000, VersionStatus.ERROR));
    VeniceHttpException versionException = expectThrows(
        VeniceHttpException.class,
        () -> VersionCreationAttemptCooldownPolicy
            .checkAndReserve(versionNewer, PushType.BATCH, "new-push-id", COOLDOWN_MS, CURRENT_TIME_MS, null));
    assertTrue(versionException.getMessage().contains("Retry in 540000 ms"));
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
  public void testLatestVersionFallbackIsStatusIndependent(VersionStatus status) {
    Store store = createStore(STORE_NAME);
    store.addVersion(createVersion(1, CURRENT_TIME_MS - 1, status));

    expectThrows(
        VeniceHttpException.class,
        () -> VersionCreationAttemptCooldownPolicy
            .checkAndReserve(store, PushType.BATCH, "new-push-id", COOLDOWN_MS, CURRENT_TIME_MS, null));
  }

  @DataProvider(name = "excluded-push-types")
  public static Object[][] excludedPushTypes() {
    return new Object[][] { { PushType.INCREMENTAL }, { PushType.STREAM } };
  }

  @Test(dataProvider = "excluded-push-types")
  public void testExcludedPushTypesDoNotReserve(PushType pushType) {
    Store store = createStore(STORE_NAME);
    store.setLastVersionCreationAttemptTimestampMs(CURRENT_TIME_MS - 1);
    store.setLastVersionCreationAttemptPushJobId("previous-push-id");

    assertFalse(
        VersionCreationAttemptCooldownPolicy
            .checkAndReserve(store, pushType, "new-push-id", COOLDOWN_MS, CURRENT_TIME_MS, null));
    assertEquals(store.getLastVersionCreationAttemptTimestampMs(), CURRENT_TIME_MS - 1);
    assertEquals(store.getLastVersionCreationAttemptPushJobId(), "previous-push-id");
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
  public void testSystemStoresDoNotReserve(String systemStoreName) {
    Store store = createStore(systemStoreName);

    assertFalse(
        VersionCreationAttemptCooldownPolicy
            .checkAndReserve(store, PushType.BATCH, "new-push-id", COOLDOWN_MS, CURRENT_TIME_MS, null));
    assertEquals(store.getLastVersionCreationAttemptTimestampMs(), 0);
    assertEquals(store.getLastVersionCreationAttemptPushJobId(), "");
  }

  @Test
  public void testZeroCooldownDoesNotReserve() {
    Store store = createStore(STORE_NAME);

    assertFalse(
        VersionCreationAttemptCooldownPolicy
            .checkAndReserve(store, PushType.BATCH, "new-push-id", 0, CURRENT_TIME_MS, null));
    assertEquals(store.getLastVersionCreationAttemptTimestampMs(), 0);
    assertEquals(store.getLastVersionCreationAttemptPushJobId(), "");
  }

  private static Store createStore(String storeName) {
    return new com.linkedin.venice.meta.ZKStore(
        storeName,
        "owner",
        1,
        PersistenceType.ROCKS_DB,
        RoutingStrategy.CONSISTENT_HASH,
        ReadStrategy.ANY_OF_ONLINE,
        OfflinePushStrategy.WAIT_ALL_REPLICAS,
        1);
  }

  private static Version createVersion(int number, long createdTime, VersionStatus status) {
    Version version =
        new VersionImpl(STORE_NAME, number, createdTime, "previous-push-id", 1, new PartitionerConfigImpl(), null);
    version.setStatus(status);
    return version;
  }
}
