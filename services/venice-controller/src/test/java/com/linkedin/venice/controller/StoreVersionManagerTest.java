package com.linkedin.venice.controller;

import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreGraveyard;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.pushmonitor.PushMonitor;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link StoreVersionManager} in isolation. Because the manager owns its collaborators via explicit
 * constructor injection (rather than reaching back into {@link VeniceHelixAdmin}), it can be exercised with a real
 * {@link ClusterLockManager} and mocked repository / push monitor / graveyard -- no Helix, ZooKeeper, or partial
 * mocking of the admin required.
 */
public class StoreVersionManagerTest {
  private static final String CLUSTER_NAME = "test-cluster";
  private static final String STORE_NAME = "test-store";

  private ReadWriteStoreRepository storeRepository;
  private PushMonitor pushMonitor;
  private StoreGraveyard storeGraveyard;
  private StoreVersionManager manager;

  @BeforeMethod
  public void setUp() {
    storeRepository = mock(ReadWriteStoreRepository.class);
    pushMonitor = mock(PushMonitor.class);
    storeGraveyard = mock(StoreGraveyard.class);
    manager =
        new StoreVersionManager(new ClusterLockManager(CLUSTER_NAME), storeRepository, pushMonitor, storeGraveyard);
  }

  private Store newStore() {
    return TestUtils.createTestStore(STORE_NAME, "owner", System.currentTimeMillis());
  }

  @Test
  public void testGetCurrentVersionReturnsCurrentWhenReadsEnabled() {
    Store store = newStore();
    store.setEnableReads(true);
    store.setCurrentVersionWithoutCheck(7);
    doReturn(store).when(storeRepository).getStore(STORE_NAME);

    Assert.assertEquals(manager.getCurrentVersion(STORE_NAME), 7);
  }

  @Test
  public void testGetCurrentVersionReturnsNonExistingWhenReadsDisabled() {
    Store store = newStore();
    store.setEnableReads(false);
    store.setCurrentVersionWithoutCheck(7);
    doReturn(store).when(storeRepository).getStore(STORE_NAME);

    Assert.assertEquals(manager.getCurrentVersion(STORE_NAME), NON_EXISTING_VERSION);
  }

  @Test
  public void testGetCurrentVersionThrowsWhenStoreMissing() {
    doReturn(null).when(storeRepository).getStore(STORE_NAME);
    Assert.expectThrows(VeniceNoStoreException.class, () -> manager.getCurrentVersion(STORE_NAME));
  }

  @Test
  public void testVersionsForStoreReturnsAllVersions() {
    Store store = newStore();
    store.addVersion(new VersionImpl(STORE_NAME, 1, "push-1"));
    store.addVersion(new VersionImpl(STORE_NAME, 2, "push-2"));
    doReturn(store).when(storeRepository).getStore(STORE_NAME);

    Assert.assertEquals(manager.versionsForStore(STORE_NAME).size(), 2);
  }

  @Test
  public void testVersionsForStoreThrowsWhenStoreMissing() {
    doReturn(null).when(storeRepository).getStore(STORE_NAME);
    Assert.expectThrows(VeniceNoStoreException.class, () -> manager.versionsForStore(STORE_NAME));
  }

  @Test
  public void testGetLargestUsedVersionFallsBackToGraveyardWhenStoreMissing() {
    doReturn(null).when(storeRepository).getStore(STORE_NAME);
    doReturn(11).when(storeGraveyard).getLargestUsedVersionNumber(STORE_NAME);

    Assert.assertEquals(manager.getLargestUsedVersion(STORE_NAME), 11);
  }

  @Test
  public void testGetLargestUsedVersionReturnsMaxOfStoreAndGraveyard() {
    Store store = newStore();
    store.setLargestUsedVersionNumber(9);
    doReturn(store).when(storeRepository).getStore(STORE_NAME);
    doReturn(4).when(storeGraveyard).getLargestUsedVersionNumber(STORE_NAME);

    Assert.assertEquals(manager.getLargestUsedVersion(STORE_NAME), 9);

    // Graveyard ahead of the live store wins.
    doReturn(15).when(storeGraveyard).getLargestUsedVersionNumber(STORE_NAME);
    Assert.assertEquals(manager.getLargestUsedVersion(STORE_NAME), 15);
  }

  @Test
  public void testGetBackupVersionReturnsLargestOnlineBelowCurrent() {
    Store store = newStore();
    store.addVersion(new VersionImpl(STORE_NAME, 1, "push-1"));
    store.addVersion(new VersionImpl(STORE_NAME, 2, "push-2"));
    store.updateVersionStatus(1, VersionStatus.ONLINE);
    store.updateVersionStatus(2, VersionStatus.ONLINE);
    store.setCurrentVersionWithoutCheck(2);
    doReturn(store).when(storeRepository).getStore(STORE_NAME);

    Assert.assertEquals(manager.getBackupVersion(STORE_NAME), 1);
  }

  @Test
  public void testGetFutureVersionWithStatusReturnsMaxNonCurrentMatchingStatus() {
    Store store = newStore();
    store.addVersion(new VersionImpl(STORE_NAME, 1, "push-1"));
    store.addVersion(new VersionImpl(STORE_NAME, 2, "push-2"));
    store.updateVersionStatus(1, VersionStatus.ONLINE);
    store.updateVersionStatus(2, VersionStatus.PUSHED);
    store.setCurrentVersionWithoutCheck(1);
    doReturn(store).when(storeRepository).getStore(STORE_NAME);

    // Max version (2) is not current and is PUSHED -> returned for PUSHED, but not for ONLINE.
    Assert.assertEquals(manager.getFutureVersionWithStatus(STORE_NAME, VersionStatus.PUSHED), 2);
    Assert.assertEquals(manager.getFutureVersionWithStatus(STORE_NAME, VersionStatus.ONLINE), NON_EXISTING_VERSION);
  }

  @Test
  public void testGetFutureVersionWithStatusReturnsNonExistingWhenMaxIsCurrent() {
    Store store = newStore();
    store.addVersion(new VersionImpl(STORE_NAME, 1, "push-1"));
    store.updateVersionStatus(1, VersionStatus.ONLINE);
    store.setCurrentVersionWithoutCheck(1);
    doReturn(store).when(storeRepository).getStore(STORE_NAME);

    Assert.assertEquals(manager.getFutureVersionWithStatus(STORE_NAME, VersionStatus.ONLINE), NON_EXISTING_VERSION);
  }

  @Test
  public void testGetFutureVersionReturnsNonExistingWhenNoOngoingPush() {
    doReturn(Collections.emptyList()).when(pushMonitor).getOfflinePushStatusForStore(STORE_NAME);
    Assert.assertEquals(manager.getFutureVersion(STORE_NAME), NON_EXISTING_VERSION);
  }

  @Test
  public void testGetFutureVersionReturnsInFlightPushVersion() {
    OfflinePushStatus inFlight = mock(OfflinePushStatus.class);
    doReturn(ExecutionStatus.STARTED).when(inFlight).getCurrentStatus();
    doReturn(Version.composeKafkaTopic(STORE_NAME, 3)).when(inFlight).getKafkaTopic();
    doReturn(Collections.singletonList(inFlight)).when(pushMonitor).getOfflinePushStatusForStore(STORE_NAME);

    Assert.assertEquals(manager.getFutureVersion(STORE_NAME), 3);
  }

  @Test
  public void testGetFutureVersionFallsBackToCompletedPushedVersion() {
    // The only push is terminal, so getFutureVersion falls back to the max non-current version with PUSHED status.
    OfflinePushStatus terminal = mock(OfflinePushStatus.class);
    doReturn(ExecutionStatus.COMPLETED).when(terminal).getCurrentStatus();
    doReturn(Collections.singletonList(terminal)).when(pushMonitor).getOfflinePushStatusForStore(STORE_NAME);

    Store store = newStore();
    store.addVersion(new VersionImpl(STORE_NAME, 1, "push-1"));
    store.addVersion(new VersionImpl(STORE_NAME, 2, "push-2"));
    store.updateVersionStatus(1, VersionStatus.ONLINE);
    store.updateVersionStatus(2, VersionStatus.PUSHED);
    store.setCurrentVersionWithoutCheck(1);
    doReturn(store).when(storeRepository).getStore(STORE_NAME);

    Assert.assertEquals(manager.getFutureVersion(STORE_NAME), 2);
  }
}
