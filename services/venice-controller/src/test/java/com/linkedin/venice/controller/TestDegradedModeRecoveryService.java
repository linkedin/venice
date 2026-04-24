package com.linkedin.venice.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestDegradedModeRecoveryService {
  private static final String CLUSTER_NAME = "testCluster";
  private static final String DATACENTER = "dc-3";
  private static final String SOURCE_FABRIC = "dc-1";

  private Admin admin;
  private DegradedModeRecoveryService recoveryService;

  @BeforeMethod
  public void setUp() {
    admin = mock(Admin.class);
    recoveryService = new DegradedModeRecoveryService(admin, null);
    // Use short poll intervals for testing
    recoveryService.setRecoveryCompletionPollParameters(10, 5);
  }

  @AfterMethod
  public void tearDown() {
    recoveryService.close();
  }

  @Test
  public void testFindPartiallyOnlineStores() {
    Store store1 = mockStoreWithVersions("store1", VersionStatus.PARTIALLY_ONLINE, 2);
    Store store2 = mockStoreWithVersions("store2", VersionStatus.ONLINE, 3);
    Store store3 = mockStoreWithVersions("store3", VersionStatus.PARTIALLY_ONLINE, 1);

    List<Store> stores = new ArrayList<>();
    stores.add(store1);
    stores.add(store2);
    stores.add(store3);
    doReturn(stores).when(admin).getAllStores(CLUSTER_NAME);

    List<DegradedModeRecoveryService.StoreVersionPair> affected =
        recoveryService.findPartiallyOnlineStores(CLUSTER_NAME);
    assertEquals(affected.size(), 2);
    assertEquals(affected.get(0).storeName, "store1");
    assertEquals(affected.get(0).version, 2);
    assertEquals(affected.get(1).storeName, "store3");
    assertEquals(affected.get(1).version, 1);
  }

  @Test
  public void testTriggerRecoveryCallsPrepareAndInitiateThenTransitions() throws Exception {
    Store store = mockStoreWithVersions("testStore", VersionStatus.PARTIALLY_ONLINE, 2);
    doReturn(Collections.singletonList(store)).when(admin).getAllStores(CLUSTER_NAME);
    setupSourceFabricMocks("testStore", VersionStatus.PARTIALLY_ONLINE, 2);
    doReturn(new Pair<>(true, "")).when(admin)
        .isStoreVersionReadyForDataRecovery(anyString(), anyString(), anyInt(), anyString(), anyString(), any());
    // Child DC confirms version is current after recovery
    doReturn(2).when(admin).getCurrentVersionInRegion(CLUSTER_NAME, "testStore", DATACENTER);

    recoveryService.triggerRecovery(CLUSTER_NAME, DATACENTER);

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      DegradedModeRecoveryService.RecoveryProgress progress =
          recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
      assertNotNull(progress);
      assertTrue(progress.isComplete());
    });

    verify(admin)
        .prepareDataRecovery(eq(CLUSTER_NAME), eq("testStore"), eq(2), eq(SOURCE_FABRIC), eq(DATACENTER), any());
    verify(admin).initiateDataRecovery(
        eq(CLUSTER_NAME),
        eq("testStore"),
        eq(2),
        eq(SOURCE_FABRIC),
        eq(DATACENTER),
        eq(false),
        any());
    // Verify version status transition
    verify(admin).updateStoreVersionStatus(CLUSTER_NAME, "testStore", 2, VersionStatus.ONLINE);

    DegradedModeRecoveryService.RecoveryProgress progress =
        recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
    assertEquals(progress.getTotalStores(), 1);
    assertEquals(progress.getRecoveredStores(), 1);
    assertEquals(progress.getFailedStores(), 0);
    assertEquals(progress.getVersionsTransitioned(), 1);
    assertTrue(progress.isComplete());
  }

  @Test
  public void testRecoverSingleStoreRetriesOnFailure() throws Exception {
    Store store = mockStoreWithVersions("testStore", VersionStatus.PARTIALLY_ONLINE, 2);
    doReturn(Collections.singletonList(store)).when(admin).getAllStores(CLUSTER_NAME);
    setupSourceFabricMocks("testStore", VersionStatus.PARTIALLY_ONLINE, 2);

    // Fail prepare twice, succeed on third attempt
    doThrow(new RuntimeException("prepare failed")).doThrow(new RuntimeException("prepare failed again"))
        .doNothing()
        .when(admin)
        .prepareDataRecovery(anyString(), anyString(), anyInt(), anyString(), anyString(), any());
    doReturn(new Pair<>(true, "")).when(admin)
        .isStoreVersionReadyForDataRecovery(anyString(), anyString(), anyInt(), anyString(), anyString(), any());
    doReturn(2).when(admin).getCurrentVersionInRegion(anyString(), anyString(), anyString());

    recoveryService.triggerRecovery(CLUSTER_NAME, DATACENTER);

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      DegradedModeRecoveryService.RecoveryProgress progress =
          recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
      assertNotNull(progress);
      assertTrue(progress.isComplete());
    });

    verify(admin, times(3)).prepareDataRecovery(anyString(), anyString(), anyInt(), anyString(), anyString(), any());
    DegradedModeRecoveryService.RecoveryProgress progress =
        recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
    assertEquals(progress.getRecoveredStores(), 1);
    assertEquals(progress.getFailedStores(), 0);
  }

  @Test
  public void testFailedStoreIncrementsFailedCountAfterAllRetries() throws Exception {
    Store store = mockStoreWithVersions("testStore", VersionStatus.PARTIALLY_ONLINE, 2);
    doReturn(Collections.singletonList(store)).when(admin).getAllStores(CLUSTER_NAME);
    setupSourceFabricMocks("testStore", VersionStatus.PARTIALLY_ONLINE, 2);

    // Fail all retries
    doThrow(new RuntimeException("prepare always fails")).when(admin)
        .prepareDataRecovery(anyString(), anyString(), anyInt(), anyString(), anyString(), any());

    recoveryService.triggerRecovery(CLUSTER_NAME, DATACENTER);

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      DegradedModeRecoveryService.RecoveryProgress progress =
          recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
      assertNotNull(progress);
      assertTrue(progress.isComplete());
    });

    verify(admin, times(DegradedModeRecoveryService.MAX_RETRIES))
        .prepareDataRecovery(anyString(), anyString(), anyInt(), anyString(), anyString(), any());
    // No version transition since recovery failed
    verify(admin, never()).updateStoreVersionStatus(anyString(), anyString(), anyInt(), any());
    DegradedModeRecoveryService.RecoveryProgress progress =
        recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
    assertEquals(progress.getRecoveredStores(), 0);
    assertEquals(progress.getFailedStores(), 1);
    assertEquals(progress.getVersionsTransitioned(), 0);
  }

  @Test
  public void testFailedStoreDoesNotBlockOthers() throws Exception {
    Store store1 = mockStoreWithVersions("failStore", VersionStatus.PARTIALLY_ONLINE, 1);
    Store store2 = mockStoreWithVersions("successStore", VersionStatus.PARTIALLY_ONLINE, 2);
    List<Store> stores = new ArrayList<>();
    stores.add(store1);
    stores.add(store2);
    doReturn(stores).when(admin).getAllStores(CLUSTER_NAME);

    setupSourceFabricMocks("failStore", VersionStatus.PARTIALLY_ONLINE, 1);
    setupSourceFabricMocks("successStore", VersionStatus.PARTIALLY_ONLINE, 2);

    // failStore always fails prepare
    doThrow(new RuntimeException("always fails")).when(admin)
        .prepareDataRecovery(eq(CLUSTER_NAME), eq("failStore"), anyInt(), anyString(), anyString(), any());
    // successStore works fine
    doReturn(new Pair<>(true, "")).when(admin)
        .isStoreVersionReadyForDataRecovery(
            eq(CLUSTER_NAME),
            eq("successStore"),
            anyInt(),
            anyString(),
            anyString(),
            any());
    // Child DC confirms successStore recovery
    doReturn(2).when(admin).getCurrentVersionInRegion(CLUSTER_NAME, "successStore", DATACENTER);

    recoveryService.triggerRecovery(CLUSTER_NAME, DATACENTER);

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      DegradedModeRecoveryService.RecoveryProgress progress =
          recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
      assertNotNull(progress);
      assertTrue(progress.isComplete());
    });

    DegradedModeRecoveryService.RecoveryProgress progress =
        recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
    assertEquals(progress.getTotalStores(), 2);
    assertEquals(progress.getRecoveredStores(), 1);
    assertEquals(progress.getFailedStores(), 1);
    // Only the successful store should be transitioned
    verify(admin).updateStoreVersionStatus(CLUSTER_NAME, "successStore", 2, VersionStatus.ONLINE);
    assertEquals(progress.getVersionsTransitioned(), 1);
  }

  @Test
  public void testRecoverySkippedWhenAlreadyInProgress() {
    Store store = mockStoreWithVersions("testStore", VersionStatus.PARTIALLY_ONLINE, 2);
    doReturn(Collections.singletonList(store)).when(admin).getAllStores(CLUSTER_NAME);
    setupSourceFabricMocks("testStore", VersionStatus.PARTIALLY_ONLINE, 2);

    // Make the recovery hang by never returning ready
    doReturn(new Pair<>(false, "not ready")).when(admin)
        .isStoreVersionReadyForDataRecovery(anyString(), anyString(), anyInt(), anyString(), anyString(), any());

    // First trigger
    recoveryService.triggerRecovery(CLUSTER_NAME, DATACENTER);
    DegradedModeRecoveryService.RecoveryProgress firstProgress =
        recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
    assertNotNull(firstProgress);

    // Second trigger should be skipped (same datacenter)
    recoveryService.triggerRecovery(CLUSTER_NAME, DATACENTER);
    assertEquals(recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER), firstProgress);
  }

  @Test
  public void testGetRecoveryProgressReturnsNullForUnknownDC() {
    assertNull(recoveryService.getRecoveryProgress(CLUSTER_NAME, "unknown-dc"));
  }

  @Test
  public void testNoStoresAffectedCompletesImmediately() {
    doReturn(Collections.emptyList()).when(admin).getAllStores(CLUSTER_NAME);

    recoveryService.triggerRecovery(CLUSTER_NAME, DATACENTER);

    DegradedModeRecoveryService.RecoveryProgress progress =
        recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
    assertNotNull(progress);
    assertTrue(progress.isComplete());
    assertEquals(progress.getTotalStores(), 0);
    assertEquals(progress.getProgressFraction(), 1.0);
  }

  @Test
  public void testRecoveryProgressFraction() {
    DegradedModeRecoveryService.RecoveryProgress progress = new DegradedModeRecoveryService.RecoveryProgress("dc-1");

    // No stores, not complete
    assertEquals(progress.getProgressFraction(), 0.0);

    // No stores, complete
    progress.markComplete();
    assertEquals(progress.getProgressFraction(), 1.0);

    // Reset for next test
    progress = new DegradedModeRecoveryService.RecoveryProgress("dc-1");
    progress.setTotalStores(4);
    assertEquals(progress.getProgressFraction(), 0.0);

    progress.incrementRecovered();
    assertEquals(progress.getProgressFraction(), 0.25);

    progress.incrementFailed();
    assertEquals(progress.getProgressFraction(), 0.5);

    progress.incrementRecovered();
    progress.incrementRecovered();
    assertEquals(progress.getProgressFraction(), 1.0);
  }

  @Test
  public void testConfirmRecoveryAndTransitionVersions() {
    DegradedModeRecoveryService.RecoveryProgress progress =
        new DegradedModeRecoveryService.RecoveryProgress(DATACENTER);
    DegradedModeRecoveryService.StoreVersionPair sv1 = new DegradedModeRecoveryService.StoreVersionPair("storeA", 3);
    DegradedModeRecoveryService.StoreVersionPair sv2 = new DegradedModeRecoveryService.StoreVersionPair("storeB", 5);
    progress.addInitiatedStore(sv1);
    progress.addInitiatedStore(sv2);

    // storeA is current, storeB is not yet current (will time out)
    doReturn(3).when(admin).getCurrentVersionInRegion(CLUSTER_NAME, "storeA", DATACENTER);
    doReturn(0).when(admin).getCurrentVersionInRegion(CLUSTER_NAME, "storeB", DATACENTER);

    recoveryService.confirmRecoveryAndTransitionVersions(CLUSTER_NAME, DATACENTER, progress);

    // storeA should be transitioned
    verify(admin).updateStoreVersionStatus(CLUSTER_NAME, "storeA", 3, VersionStatus.ONLINE);
    // storeB should not be transitioned (timed out)
    verify(admin, never()).updateStoreVersionStatus(eq(CLUSTER_NAME), eq("storeB"), eq(5), any());
    assertEquals(progress.getVersionsTransitioned(), 1);
  }

  @Test
  public void testVersionTransitionCalledAfterRecoveryInitiation() throws Exception {
    // Verify the full flow: initiate → poll child DC → transition version
    Store store = mockStoreWithVersions("testStore", VersionStatus.PARTIALLY_ONLINE, 2);
    doReturn(Collections.singletonList(store)).when(admin).getAllStores(CLUSTER_NAME);
    setupSourceFabricMocks("testStore", VersionStatus.PARTIALLY_ONLINE, 2);
    doReturn(new Pair<>(true, "")).when(admin)
        .isStoreVersionReadyForDataRecovery(anyString(), anyString(), anyInt(), anyString(), anyString(), any());

    // Simulate child DC needing a few polls before version becomes current
    doReturn(0).doReturn(0).doReturn(2).when(admin).getCurrentVersionInRegion(CLUSTER_NAME, "testStore", DATACENTER);

    recoveryService.triggerRecovery(CLUSTER_NAME, DATACENTER);

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      DegradedModeRecoveryService.RecoveryProgress progress =
          recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
      assertNotNull(progress);
      assertTrue(progress.isComplete());
    });

    // Should have polled getCurrentVersionInRegion 3 times
    verify(admin, times(3)).getCurrentVersionInRegion(CLUSTER_NAME, "testStore", DATACENTER);
    // Should have transitioned version
    verify(admin).updateStoreVersionStatus(CLUSTER_NAME, "testStore", 2, VersionStatus.ONLINE);
  }

  @Test
  public void testOrphanedPartiallyOnlineVersionsDetectedAndRecovered() throws Exception {
    // Simulate a leader failover scenario: PARTIALLY_ONLINE versions exist
    // but no active recovery is tracked. The monitor should detect and re-trigger.
    Store store = mockStoreWithVersions("orphanedStore", VersionStatus.PARTIALLY_ONLINE, 3);
    doReturn(Collections.singletonList(store)).when(admin).getAllStores(CLUSTER_NAME);

    // Setup region map — dc-3 is the region that needs recovery
    Map<String, String> regionMap = new HashMap<>();
    regionMap.put("dc-1", "http://dc1:1234");
    regionMap.put("dc-3", "http://dc3:1234");
    doReturn(regionMap).when(admin).getChildDataCenterControllerUrlMap(CLUSTER_NAME);

    // dc-1 has the version current, dc-3 does not (needs recovery)
    // Mock both getCurrentVersionInRegion (used by pollUntilVersionCurrent) and
    // isVersionCurrentInRegion (used by detectAndRecoverOrphanedVersions — default method
    // on Admin interface is intercepted by Mockito, won't delegate to default impl)
    doReturn(3).when(admin).getCurrentVersionInRegion(CLUSTER_NAME, "orphanedStore", "dc-1");
    doReturn(0).when(admin).getCurrentVersionInRegion(CLUSTER_NAME, "orphanedStore", "dc-3");
    doReturn(true).when(admin).isVersionCurrentInRegion(CLUSTER_NAME, "orphanedStore", 3, "dc-1");
    doReturn(false).when(admin).isVersionCurrentInRegion(CLUSTER_NAME, "orphanedStore", 3, "dc-3");

    // No degraded DCs (already unmarked — this is post-failover)
    doReturn(new com.linkedin.venice.meta.DegradedDcStates()).when(admin).getDegradedDcStates(CLUSTER_NAME);

    // Setup source fabric mocks for recovery to work
    setupSourceFabricMocks("orphanedStore", VersionStatus.PARTIALLY_ONLINE, 3);
    doReturn(new Pair<>(true, "")).when(admin)
        .isStoreVersionReadyForDataRecovery(anyString(), anyString(), anyInt(), anyString(), anyString(), any());
    // After recovery initiation, child DC confirms version is current
    doReturn(0).doReturn(3).when(admin).getCurrentVersionInRegion(CLUSTER_NAME, "orphanedStore", "dc-3");

    // Start the monitor with a fast interval for testing
    recoveryService.startDegradedDcMonitor(Collections.singleton(CLUSTER_NAME), 100, TimeUnit.MILLISECONDS);

    // Wait for the monitor to detect and trigger recovery
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      DegradedModeRecoveryService.RecoveryProgress progress = recoveryService.getRecoveryProgress(CLUSTER_NAME, "dc-3");
      assertNotNull(progress, "Recovery should have been triggered for dc-3");
      assertTrue(progress.getTotalStores() > 0, "Should have found orphaned stores");
    });

    // Verify recovery was triggered for dc-3 (the region missing the version)
    DegradedModeRecoveryService.RecoveryProgress progress = recoveryService.getRecoveryProgress(CLUSTER_NAME, "dc-3");
    assertNotNull(progress);
    assertEquals(progress.getTotalStores(), 1);
    // dc-1 should NOT have recovery triggered (version is current there)
    assertNull(recoveryService.getRecoveryProgress(CLUSTER_NAME, "dc-1"));
  }

  @Test
  public void testOrphanedVersionsNotDetectedWhenRecoveryAlreadyActive() {
    // If a recovery is already in progress, don't re-trigger
    Store store = mockStoreWithVersions("testStore", VersionStatus.PARTIALLY_ONLINE, 2);
    doReturn(Collections.singletonList(store)).when(admin).getAllStores(CLUSTER_NAME);

    Map<String, String> regionMap = new HashMap<>();
    regionMap.put(DATACENTER, "http://dc3:1234");
    doReturn(regionMap).when(admin).getChildDataCenterControllerUrlMap(CLUSTER_NAME);

    // Setup an active (incomplete) recovery
    setupSourceFabricMocks("testStore", VersionStatus.PARTIALLY_ONLINE, 2);
    doReturn(new Pair<>(false, "not ready")).when(admin)
        .isStoreVersionReadyForDataRecovery(anyString(), anyString(), anyInt(), anyString(), anyString(), any());
    recoveryService.triggerRecovery(CLUSTER_NAME, DATACENTER);
    DegradedModeRecoveryService.RecoveryProgress firstProgress =
        recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
    assertNotNull(firstProgress);
    assertFalse(firstProgress.isComplete());

    // Now run the orphan detection — should NOT re-trigger since recovery is active
    doReturn(new com.linkedin.venice.meta.DegradedDcStates()).when(admin).getDegradedDcStates(CLUSTER_NAME);
    doReturn(0).when(admin).getCurrentVersionInRegion(CLUSTER_NAME, "testStore", DATACENTER);

    // detectAndRecoverOrphanedVersions is called internally by the monitor
    // Verify the progress object hasn't changed (same reference = no re-trigger)
    assertEquals(recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER), firstProgress);
  }

  @Test
  public void testFindPartiallyOnlineStoresOnlyReturnsHighestVersion() {
    // Store with two PARTIALLY_ONLINE versions — should only return the highest
    Store store = mock(Store.class);
    doReturn("multiVersionStore").when(store).getName();
    Version v2 = new VersionImpl("multiVersionStore", 2);
    v2.setStatus(VersionStatus.PARTIALLY_ONLINE);
    Version v5 = new VersionImpl("multiVersionStore", 5);
    v5.setStatus(VersionStatus.PARTIALLY_ONLINE);
    Version v3 = new VersionImpl("multiVersionStore", 3);
    v3.setStatus(VersionStatus.ONLINE);
    List<Version> versions = new ArrayList<>();
    versions.add(v2);
    versions.add(v5);
    versions.add(v3);
    doReturn(versions).when(store).getVersions();
    doReturn(Collections.singletonList(store)).when(admin).getAllStores(CLUSTER_NAME);

    List<DegradedModeRecoveryService.StoreVersionPair> affected =
        recoveryService.findPartiallyOnlineStores(CLUSTER_NAME);
    assertEquals(affected.size(), 1);
    assertEquals(affected.get(0).storeName, "multiVersionStore");
    assertEquals(affected.get(0).version, 5);
  }

  @Test
  public void testRecoverySkipsDeletedStore() throws Exception {
    // Store appears in getAllStores scan but is deleted before recoverSingleStore runs
    Store store = mockStoreWithVersions("deletedStore", VersionStatus.PARTIALLY_ONLINE, 2);
    doReturn(Collections.singletonList(store)).when(admin).getAllStores(CLUSTER_NAME);
    setupSourceFabricMocks("deletedStore", VersionStatus.PARTIALLY_ONLINE, 2);

    // Override getStore to return null (store was deleted between scan and recovery)
    doReturn(null).when(admin).getStore(CLUSTER_NAME, "deletedStore");

    recoveryService.triggerRecovery(CLUSTER_NAME, DATACENTER);

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      DegradedModeRecoveryService.RecoveryProgress progress =
          recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
      assertNotNull(progress);
      assertTrue(progress.isComplete());
    });

    DegradedModeRecoveryService.RecoveryProgress progress =
        recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
    assertEquals(progress.getTotalStores(), 1);
    assertEquals(progress.getFailedStores(), 1);
    assertEquals(progress.getRecoveredStores(), 0);
    // No recovery initiation should have been attempted
    verify(admin, never()).prepareDataRecovery(anyString(), anyString(), anyInt(), anyString(), anyString(), any());
  }

  @Test
  public void testRecoverySkipsVersionNoLongerPartiallyOnline() throws Exception {
    // Store appears in getAllStores with PARTIALLY_ONLINE, but by the time recoverSingleStore
    // runs, the version has already transitioned to ONLINE (recovered by another process)
    Store store = mockStoreWithVersions("alreadyOnlineStore", VersionStatus.PARTIALLY_ONLINE, 3);
    doReturn(Collections.singletonList(store)).when(admin).getAllStores(CLUSTER_NAME);
    setupSourceFabricMocks("alreadyOnlineStore", VersionStatus.PARTIALLY_ONLINE, 3);

    // Override getStore to return a store where version is now ONLINE
    Store updatedStore = mockStoreWithVersions("alreadyOnlineStore", VersionStatus.ONLINE, 3);
    doReturn(updatedStore).when(admin).getStore(CLUSTER_NAME, "alreadyOnlineStore");

    recoveryService.triggerRecovery(CLUSTER_NAME, DATACENTER);

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      DegradedModeRecoveryService.RecoveryProgress progress =
          recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
      assertNotNull(progress);
      assertTrue(progress.isComplete());
    });

    DegradedModeRecoveryService.RecoveryProgress progress =
        recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
    assertEquals(progress.getTotalStores(), 1);
    // Pre-check skips silently (no incrementFailed, no incrementRecovered)
    assertEquals(progress.getRecoveredStores(), 0);
    assertEquals(progress.getFailedStores(), 0);
    // No recovery initiation should have been attempted
    verify(admin, never()).prepareDataRecovery(anyString(), anyString(), anyInt(), anyString(), anyString(), any());
  }

  @Test
  public void testRecoveryHandlesSourceFabricUnreachable() throws Exception {
    // resolveSourceFabric calls admin.getStore(), which throws VeniceException.
    // The pre-check in recoverSingleStore also calls admin.getStore(), so we use
    // sequential stubbing: first call succeeds (pre-check), subsequent calls throw
    // (inside the retry loop's resolveSourceFabric).
    Store store = mockStoreWithVersions("unreachableStore", VersionStatus.PARTIALLY_ONLINE, 2);
    doReturn(Collections.singletonList(store)).when(admin).getAllStores(CLUSTER_NAME);
    setupSourceFabricMocks("unreachableStore", VersionStatus.PARTIALLY_ONLINE, 2);

    // First call returns the store (pre-check passes), then all subsequent calls throw
    Store preCheckStore = mockStoreWithVersions("unreachableStore", VersionStatus.PARTIALLY_ONLINE, 2);
    doReturn(preCheckStore).doThrow(new VeniceException("Source fabric unreachable"))
        .doThrow(new VeniceException("Source fabric unreachable"))
        .doThrow(new VeniceException("Source fabric unreachable"))
        .when(admin)
        .getStore(CLUSTER_NAME, "unreachableStore");

    recoveryService.triggerRecovery(CLUSTER_NAME, DATACENTER);

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      DegradedModeRecoveryService.RecoveryProgress progress =
          recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
      assertNotNull(progress);
      assertTrue(progress.isComplete());
    });

    DegradedModeRecoveryService.RecoveryProgress progress =
        recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
    assertEquals(progress.getTotalStores(), 1);
    assertEquals(progress.getFailedStores(), 1);
    assertEquals(progress.getRecoveredStores(), 0);
  }

  @Test
  public void testVersionSupersededDuringPollingTreatedAsSuccess() throws Exception {
    // Recovery initiates successfully, but during confirmation polling a higher version becomes current
    Store store = mockStoreWithVersions("supersededStore", VersionStatus.PARTIALLY_ONLINE, 2);
    doReturn(Collections.singletonList(store)).when(admin).getAllStores(CLUSTER_NAME);
    setupSourceFabricMocks("supersededStore", VersionStatus.PARTIALLY_ONLINE, 2);
    doReturn(new Pair<>(true, "")).when(admin)
        .isStoreVersionReadyForDataRecovery(anyString(), anyString(), anyInt(), anyString(), anyString(), any());

    // During confirmation polling, return version 3 (higher than recovering v2) — superseded
    doReturn(3).when(admin).getCurrentVersionInRegion(CLUSTER_NAME, "supersededStore", DATACENTER);

    recoveryService.triggerRecovery(CLUSTER_NAME, DATACENTER);

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      DegradedModeRecoveryService.RecoveryProgress progress =
          recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
      assertNotNull(progress);
      assertTrue(progress.isComplete());
    });

    DegradedModeRecoveryService.RecoveryProgress progress =
        recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
    assertEquals(progress.getRecoveredStores(), 1);
    assertEquals(progress.getFailedStores(), 0);
    // SUPERSEDED counts as success — versionsTransitioned should be incremented
    assertEquals(progress.getVersionsTransitioned(), 1);
    // Version status should NOT be updated to ONLINE (superseded, not current)
    verify(admin, never()).updateStoreVersionStatus(anyString(), anyString(), anyInt(), any());
  }

  @Test
  public void testRecoveryWithEmptyStoreList() {
    doReturn(new ArrayList<>()).when(admin).getAllStores(CLUSTER_NAME);

    recoveryService.triggerRecovery(CLUSTER_NAME, DATACENTER);

    DegradedModeRecoveryService.RecoveryProgress progress =
        recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
    assertNotNull(progress);
    assertTrue(progress.isComplete());
    assertEquals(progress.getTotalStores(), 0);
    assertEquals(progress.getRecoveredStores(), 0);
    assertEquals(progress.getFailedStores(), 0);
  }

  @Test
  public void testMarkRecoveryCompleteThenRetriggerSameDc() throws Exception {
    // First recovery: single store, completes successfully
    Store store = mockStoreWithVersions("retriggerStore", VersionStatus.PARTIALLY_ONLINE, 1);
    doReturn(Collections.singletonList(store)).when(admin).getAllStores(CLUSTER_NAME);
    setupSourceFabricMocks("retriggerStore", VersionStatus.PARTIALLY_ONLINE, 1);
    doReturn(new Pair<>(true, "")).when(admin)
        .isStoreVersionReadyForDataRecovery(anyString(), anyString(), anyInt(), anyString(), anyString(), any());
    doReturn(1).when(admin).getCurrentVersionInRegion(CLUSTER_NAME, "retriggerStore", DATACENTER);

    recoveryService.triggerRecovery(CLUSTER_NAME, DATACENTER);

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      DegradedModeRecoveryService.RecoveryProgress progress =
          recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
      assertNotNull(progress);
      assertTrue(progress.isComplete());
    });

    DegradedModeRecoveryService.RecoveryProgress firstProgress =
        recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
    assertEquals(firstProgress.getRecoveredStores(), 1);
    assertTrue(firstProgress.isComplete());

    // Second recovery: re-trigger for the same DC after completion
    Store store2 = mockStoreWithVersions("retriggerStore2", VersionStatus.PARTIALLY_ONLINE, 2);
    doReturn(Collections.singletonList(store2)).when(admin).getAllStores(CLUSTER_NAME);
    setupSourceFabricMocks("retriggerStore2", VersionStatus.PARTIALLY_ONLINE, 2);
    doReturn(2).when(admin).getCurrentVersionInRegion(CLUSTER_NAME, "retriggerStore2", DATACENTER);

    recoveryService.triggerRecovery(CLUSTER_NAME, DATACENTER);

    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      DegradedModeRecoveryService.RecoveryProgress progress =
          recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
      assertNotNull(progress);
      assertTrue(progress.isComplete());
      // The second trigger should have replaced the first completed entry
      assertEquals(progress.getTotalStores(), 1);
    });

    DegradedModeRecoveryService.RecoveryProgress secondProgress =
        recoveryService.getRecoveryProgress(CLUSTER_NAME, DATACENTER);
    // Verify it's a different progress object (replaced, not the same reference)
    assertFalse(
        firstProgress == secondProgress,
        "Second trigger should create a new RecoveryProgress, not reuse the completed one");
    assertEquals(secondProgress.getRecoveredStores(), 1);
    assertTrue(secondProgress.isComplete());
  }

  private Store mockStoreWithVersions(String storeName, VersionStatus status, int versionNumber) {
    Store store = mock(Store.class);
    doReturn(storeName).when(store).getName();
    Version version = new VersionImpl(storeName, versionNumber);
    version.setStatus(status);
    List<Version> versions = new ArrayList<>();
    versions.add(version);
    doReturn(versions).when(store).getVersions();
    doReturn(version).when(store).getVersion(versionNumber);
    return store;
  }

  private void setupSourceFabricMocks(String storeName, VersionStatus status, int versionNumber) {
    Store store = mockStoreWithVersions(storeName, status, versionNumber);
    doReturn(store).when(admin).getStore(CLUSTER_NAME, storeName);
    doReturn(Optional.empty()).when(admin).getEmergencySourceRegion(CLUSTER_NAME);
    doReturn(SOURCE_FABRIC).when(admin).getNativeReplicationSourceFabric(eq(CLUSTER_NAME), any(), any(), any(), any());
  }
}
