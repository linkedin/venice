package com.linkedin.venice.helix;

import static com.linkedin.venice.zk.VeniceZkPaths.STORES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Covers flag toggles between the legacy embedded-version layout and the per-version-znode layout.
 */
public class TestHelixReadWriteStoreRepositoryVersionSplitFlagToggle {
  private static final String CLUSTER = "test-version-split-flag-toggle-cluster";
  private static final String CLUSTER_PATH = "/" + CLUSTER;
  private static final String STORES_PATH = CLUSTER_PATH + "/" + STORES;

  private ZkServerWrapper zkServerWrapper;
  private ZkClient zkClient;
  private HelixAdapterSerializer adapter;

  @BeforeMethod
  public void setUp() {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkClient = ZkClientFactory.newZkClient(zkServerWrapper.getAddress());
    adapter = new HelixAdapterSerializer();
    zkClient.setZkSerializer(adapter);
    zkClient.create(CLUSTER_PATH, null, CreateMode.PERSISTENT);
    zkClient.create(STORES_PATH, null, CreateMode.PERSISTENT);
  }

  @AfterMethod
  public void tearDown() {
    zkClient.deleteRecursively(CLUSTER_PATH);
    zkClient.close();
    zkServerWrapper.close();
  }

  @Test
  public void flagOffUpdateAfterSplitWriteRemovesDeletedVersionsFromEmbeddedJsonAndZnodes() {
    String storeName = Utils.getUniqueString("toggle_delete_store");
    ZkBaseDataAccessor<Object> raw = new ZkBaseDataAccessor<>(zkClient);

    Store legacyStore = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    addVersions(storeName, legacyStore, 100, 101, 111);
    seedLegacyStoreDirectly(storeName, legacyStore);

    HelixReadWriteStoreRepository splitWriter = newRepo(true);
    splitWriter.refresh();
    Store splitStore = splitWriter.getStore(storeName);
    addVersions(storeName, splitStore, 112, 122);
    splitWriter.updateStore(splitStore);

    Store splitStoreOnZk = (Store) raw.get(STORES_PATH + "/" + storeName, null, AccessOption.PERSISTENT);
    assertNotNull(splitStoreOnZk);
    assertVersions(splitStoreOnZk, 100, 101, 111);
    assertVersionZnodes(storeName, 112, 122);

    HelixReadWriteStoreRepository legacyWriter = newRepo(false);
    legacyWriter.refresh();
    Store legacyUpdate = legacyWriter.getStore(storeName);
    legacyUpdate.deleteVersion(101);
    legacyUpdate.deleteVersion(112);
    legacyUpdate.addVersion(new VersionImpl(storeName, 123, "push-123"));
    legacyWriter.updateStore(legacyUpdate);

    Store legacyStoreOnZk = (Store) raw.get(STORES_PATH + "/" + storeName, null, AccessOption.PERSISTENT);
    assertNotNull(legacyStoreOnZk);
    assertVersions(legacyStoreOnZk, 100, 111, 122, 123);
    assertFalse(legacyStoreOnZk.containsVersion(101), "deleted legacy version must be removed from embedded JSON");
    assertFalse(legacyStoreOnZk.containsVersion(112), "deleted znode version must not be written into embedded JSON");
    assertFalse(
        raw.exists(STORES_PATH + "/" + storeName + "/versions", AccessOption.PERSISTENT),
        "flag-off update must remove stale per-version znode tree");

    HelixReadWriteStoreRepository freshSplitReader = newRepo(true);
    freshSplitReader.refresh();
    Store rehydrated = freshSplitReader.getStore(storeName);
    assertNotNull(rehydrated);
    assertVersions(rehydrated, 100, 111, 122, 123);
    assertFalse(rehydrated.containsVersion(101));
    assertFalse(rehydrated.containsVersion(112));
  }

  private void addVersions(String storeName, Store store, int... versionNumbers) {
    for (int versionNumber: versionNumbers) {
      store.addVersion(new VersionImpl(storeName, versionNumber, "push-" + versionNumber));
    }
  }

  private void assertVersions(Store store, Integer... expectedVersionNumbers) {
    Set<Integer> actualVersionNumbers = new TreeSet<>();
    for (Version version: store.getVersions()) {
      actualVersionNumbers.add(version.getNumber());
    }
    assertEquals(actualVersionNumbers, new TreeSet<>(Arrays.asList(expectedVersionNumbers)));
  }

  private void assertVersionZnodes(String storeName, int... versionNumbers) {
    ZkBaseDataAccessor<Object> raw = new ZkBaseDataAccessor<>(zkClient);
    assertTrue(raw.exists(STORES_PATH + "/" + storeName + "/versions", AccessOption.PERSISTENT));
    for (int versionNumber: versionNumbers) {
      assertTrue(raw.exists(STORES_PATH + "/" + storeName + "/versions/" + versionNumber, AccessOption.PERSISTENT));
    }
  }

  private void seedLegacyStoreDirectly(String storeName, Store store) {
    adapter.registerSerializer(STORES_PATH + "/*", new StoreJSONSerializer());
    zkClient.setZkSerializer(adapter);
    ZkBaseDataAccessor<Store> storeAccessor = new ZkBaseDataAccessor<>(zkClient);
    storeAccessor.create(STORES_PATH + "/" + storeName, store, AccessOption.PERSISTENT);
  }

  private HelixReadWriteStoreRepository newRepo(boolean perVersionZnodeWriteEnabled) {
    return new HelixReadWriteStoreRepository(
        zkClient,
        adapter,
        CLUSTER,
        Optional.empty(),
        new ClusterLockManager(CLUSTER),
        perVersionZnodeWriteEnabled);
  }
}
