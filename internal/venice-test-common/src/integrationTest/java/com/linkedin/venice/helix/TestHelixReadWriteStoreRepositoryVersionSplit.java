package com.linkedin.venice.helix;

import static com.linkedin.venice.zk.VeniceZkPaths.STORES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.Optional;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Exercises the split persistence layout against a real ZK. The invariants under test:
 * <ul>
 *   <li>Stores created via {@link HelixReadWriteStoreRepository#addStore} land every version in {@code /versions/<n>};
 *       the embedded list is empty.</li>
 *   <li>Stores created before this code (with an embedded versions list) keep their embedded entries intact forever;
 *       newly added versions go to {@code /versions/<n>} and never join the embedded list.</li>
 *   <li>Mutations to an existing znode-version (e.g. status change) propagate on the next {@code updateStore}.</li>
 *   <li>A version number that appears in BOTH layers is treated as a corrupt-state bug and surfaces as an exception
 *       at read time.</li>
 * </ul>
 */
public class TestHelixReadWriteStoreRepositoryVersionSplit {
  private static final String CLUSTER = "test-version-split-cluster";
  private static final String CLUSTER_PATH = "/" + CLUSTER;
  private static final String STORES_PATH = CLUSTER_PATH + "/" + STORES;

  private ZkServerWrapper zkServerWrapper;
  private ZkClient zkClient;
  private HelixAdapterSerializer adapter;
  private HelixReadWriteStoreRepository writeRepo;

  @BeforeMethod
  public void setUp() {
    zkServerWrapper = ServiceFactory.getZkServer();
    zkClient = ZkClientFactory.newZkClient(zkServerWrapper.getAddress());
    adapter = new HelixAdapterSerializer();
    zkClient.setZkSerializer(adapter);
    zkClient.create(CLUSTER_PATH, null, CreateMode.PERSISTENT);
    zkClient.create(STORES_PATH, null, CreateMode.PERSISTENT);

    writeRepo = new HelixReadWriteStoreRepository(
        zkClient,
        adapter,
        CLUSTER,
        Optional.empty(),
        new ClusterLockManager(CLUSTER),
        true);
    writeRepo.refresh();
  }

  @AfterMethod
  public void tearDown() {
    writeRepo.clear();
    zkClient.deleteRecursively(CLUSTER_PATH);
    zkClient.close();
    zkServerWrapper.close();
  }

  @Test
  public void addStoreWritesAllVersionsAsZnodesAndLeavesEmbeddedEmpty() {
    String storeName = Utils.getUniqueString("test_store");
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    store.addVersion(new VersionImpl(storeName, 1, "push-1"));
    store.addVersion(new VersionImpl(storeName, 2, "push-2"));

    writeRepo.addStore(store);

    ZkBaseDataAccessor<Object> raw = new ZkBaseDataAccessor<>(zkClient);
    assertTrue(raw.exists(STORES_PATH + "/" + storeName + "/versions", AccessOption.PERSISTENT));
    assertTrue(raw.exists(STORES_PATH + "/" + storeName + "/versions/1", AccessOption.PERSISTENT));
    assertTrue(raw.exists(STORES_PATH + "/" + storeName + "/versions/2", AccessOption.PERSISTENT));

    Store onZk = (Store) raw.get(STORES_PATH + "/" + storeName, null, AccessOption.PERSISTENT);
    assertNotNull(onZk);
    assertTrue(onZk.getVersions().isEmpty(), "addStore must persist an empty embedded versions list");
  }

  @Test
  public void legacyEmbeddedVersionsAreHydratedOnRead() {
    String storeName = Utils.getUniqueString("legacy_store");
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    Version v1 = new VersionImpl(storeName, 1, "legacy-push-1");
    v1.setStatus(VersionStatus.ONLINE);
    store.addVersion(v1);
    store.addVersion(new VersionImpl(storeName, 2, "legacy-push-2"));

    seedLegacyStoreDirectly(storeName, store);

    HelixReadWriteStoreRepository freshRepo = newRepo();
    freshRepo.refresh();

    Store hydrated = freshRepo.getStore(storeName);
    assertNotNull(hydrated);
    assertEquals(hydrated.getVersions().size(), 2);
    assertEquals(hydrated.getVersion(1).getStatus(), VersionStatus.ONLINE);
  }

  /**
   * A legacy store's embedded list is frozen: adding a new version places it in {@code /versions/<n>} only, and the
   * pre-existing embedded entries stay where they are. No migration of legacy versions ever happens.
   */
  @Test
  public void addingVersionToLegacyStoreOnlyWritesNewZnode() {
    String storeName = Utils.getUniqueString("frozen_legacy_store");
    Store legacyStore = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    legacyStore.addVersion(new VersionImpl(storeName, 1, "legacy-push-1"));
    legacyStore.addVersion(new VersionImpl(storeName, 2, "legacy-push-2"));

    seedLegacyStoreDirectly(storeName, legacyStore);
    writeRepo.refresh();

    Store updated = writeRepo.getStore(storeName);
    updated.addVersion(new VersionImpl(storeName, 3, "new-push-3"));
    writeRepo.updateStore(updated);

    ZkBaseDataAccessor<Object> raw = new ZkBaseDataAccessor<>(zkClient);
    assertFalse(
        raw.exists(STORES_PATH + "/" + storeName + "/versions/1", AccessOption.PERSISTENT),
        "legacy version 1 must remain embedded, not promoted to a znode");
    assertFalse(
        raw.exists(STORES_PATH + "/" + storeName + "/versions/2", AccessOption.PERSISTENT),
        "legacy version 2 must remain embedded, not promoted to a znode");
    assertTrue(raw.exists(STORES_PATH + "/" + storeName + "/versions/3", AccessOption.PERSISTENT));

    Store onZk = (Store) raw.get(STORES_PATH + "/" + storeName, null, AccessOption.PERSISTENT);
    assertEquals(onZk.getVersions().size(), 2, "embedded list must still hold the two legacy versions");
    assertTrue(onZk.containsVersion(1));
    assertTrue(onZk.containsVersion(2));

    writeRepo.clear();
    writeRepo.refresh();
    Store rehydrated = writeRepo.getStore(storeName);
    assertEquals(rehydrated.getVersions().size(), 3);
  }

  @Test
  public void updateStoreRemovesDeletedZnodeVersion() {
    String storeName = Utils.getUniqueString("delete_version_store");
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    store.addVersion(new VersionImpl(storeName, 1, "push-1"));
    store.addVersion(new VersionImpl(storeName, 2, "push-2"));
    writeRepo.addStore(store);

    Store fetched = writeRepo.getStore(storeName);
    fetched.deleteVersion(1);
    writeRepo.updateStore(fetched);

    ZkBaseDataAccessor<Object> raw = new ZkBaseDataAccessor<>(zkClient);
    assertFalse(raw.exists(STORES_PATH + "/" + storeName + "/versions/1", AccessOption.PERSISTENT));
    assertTrue(raw.exists(STORES_PATH + "/" + storeName + "/versions/2", AccessOption.PERSISTENT));
  }

  /**
   * Deleting a legacy embedded version updates the embedded list in the store znode; no znode is created or removed.
   */
  @Test
  public void updateStoreRemovesDeletedLegacyEmbeddedVersion() {
    String storeName = Utils.getUniqueString("delete_legacy_store");
    Store legacyStore = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    legacyStore.addVersion(new VersionImpl(storeName, 1, "legacy-push-1"));
    legacyStore.addVersion(new VersionImpl(storeName, 2, "legacy-push-2"));
    seedLegacyStoreDirectly(storeName, legacyStore);
    writeRepo.refresh();

    Store fetched = writeRepo.getStore(storeName);
    fetched.deleteVersion(1);
    writeRepo.updateStore(fetched);

    ZkBaseDataAccessor<Object> raw = new ZkBaseDataAccessor<>(zkClient);
    Store onZk = (Store) raw.get(STORES_PATH + "/" + storeName, null, AccessOption.PERSISTENT);
    assertEquals(onZk.getVersions().size(), 1);
    assertTrue(onZk.containsVersion(2));
    assertFalse(onZk.containsVersion(1));
    assertFalse(raw.exists(STORES_PATH + "/" + storeName + "/versions/1", AccessOption.PERSISTENT));
  }

  @Test
  public void deleteStoreTearsDownVersionsContainer() {
    String storeName = Utils.getUniqueString("del_store");
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    store.addVersion(new VersionImpl(storeName, 1, "push-1"));
    store.addVersion(new VersionImpl(storeName, 2, "push-2"));
    writeRepo.addStore(store);

    writeRepo.deleteStore(storeName);

    ZkBaseDataAccessor<Object> raw = new ZkBaseDataAccessor<>(zkClient);
    assertFalse(raw.exists(STORES_PATH + "/" + storeName, AccessOption.PERSISTENT));
    assertFalse(raw.exists(STORES_PATH + "/" + storeName + "/versions", AccessOption.PERSISTENT));
  }

  /**
   * An in-place mutation on a znode-version (the {@code updateVersionStatus} pattern mutates the Avro record by
   * reference) must be persisted by the next {@code updateStore}: the znode is re-written with the mutated state.
   */
  @Test
  public void mutationOfZnodeVersionPersistsViaUpdateStore() {
    String storeName = Utils.getUniqueString("mutating_store");
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    store.addVersion(new VersionImpl(storeName, 1, "push-1"));
    writeRepo.addStore(store);

    Store fetched = writeRepo.getStore(storeName);
    fetched.updateVersionStatus(1, VersionStatus.ONLINE);
    writeRepo.updateStore(fetched);

    HelixVersionAccessor accessor = new HelixVersionAccessor(zkClient, adapter, CLUSTER);
    Version persisted = accessor.getVersion(storeName, 1);
    assertNotNull(persisted);
    assertEquals(persisted.getStatus(), VersionStatus.ONLINE, "status mutation must be flushed to /versions/1");
  }

  @Test
  public void unknownStoreReturnsNullOnGet() {
    Store store = writeRepo.getStore("never_existed");
    assertNull(store);
  }

  /**
   * Mixed-state cluster: one store written through the repo (split layout), one seeded directly in the legacy layout
   * (embedded only). Both must hydrate correctly; the legacy store's embedded list is preserved across a subsequent
   * update that adds a new version.
   */
  @Test
  public void legacyAndSplitStoresCoexistAndUpdatesPreserveEmbedded() {
    String splitStoreName = Utils.getUniqueString("split_store");
    Store splitStore = TestUtils.createTestStore(splitStoreName, "owner", System.currentTimeMillis());
    splitStore.addVersion(new VersionImpl(splitStoreName, 1, "split-push-1"));
    splitStore.addVersion(new VersionImpl(splitStoreName, 2, "split-push-2"));
    writeRepo.addStore(splitStore);

    String legacyStoreName = Utils.getUniqueString("legacy_store");
    Store legacyStore = TestUtils.createTestStore(legacyStoreName, "owner", System.currentTimeMillis());
    Version legacyV1 = new VersionImpl(legacyStoreName, 1, "legacy-push-1");
    legacyV1.setStatus(VersionStatus.ONLINE);
    legacyStore.addVersion(legacyV1);
    legacyStore.addVersion(new VersionImpl(legacyStoreName, 2, "legacy-push-2"));
    seedLegacyStoreDirectly(legacyStoreName, legacyStore);

    HelixReadWriteStoreRepository freshRepo = newRepo();
    freshRepo.refresh();

    Store splitHydrated = freshRepo.getStore(splitStoreName);
    assertNotNull(splitHydrated);
    assertEquals(splitHydrated.getVersions().size(), 2);

    Store legacyHydrated = freshRepo.getStore(legacyStoreName);
    assertNotNull(legacyHydrated);
    assertEquals(legacyHydrated.getVersions().size(), 2);
    assertEquals(legacyHydrated.getVersion(1).getStatus(), VersionStatus.ONLINE);

    legacyHydrated.addVersion(new VersionImpl(legacyStoreName, 3, "legacy-push-3"));
    freshRepo.updateStore(legacyHydrated);

    ZkBaseDataAccessor<Object> raw = new ZkBaseDataAccessor<>(zkClient);
    assertTrue(raw.exists(STORES_PATH + "/" + legacyStoreName + "/versions/3", AccessOption.PERSISTENT));
    assertFalse(
        raw.exists(STORES_PATH + "/" + legacyStoreName + "/versions/1", AccessOption.PERSISTENT),
        "legacy version 1 must remain embedded after the update");
    Store legacyOnZk = (Store) raw.get(STORES_PATH + "/" + legacyStoreName, null, AccessOption.PERSISTENT);
    assertEquals(legacyOnZk.getVersions().size(), 2, "embedded list should still hold versions 1 and 2");
  }

  /**
   * A legacy store with new versions appended as znodes: the two layers are disjoint, hydration returns the union.
   */
  @Test
  public void disjointMixedLayoutReturnsUnion() {
    String storeName = Utils.getUniqueString("mixed_layout_store");

    Store legacyStore = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    legacyStore.addVersion(new VersionImpl(storeName, 1, "legacy-push-1"));
    legacyStore.addVersion(new VersionImpl(storeName, 2, "legacy-push-2"));
    legacyStore.addVersion(new VersionImpl(storeName, 3, "legacy-push-3"));
    seedLegacyStoreDirectly(storeName, legacyStore);

    HelixVersionAccessor seedAccessor = new HelixVersionAccessor(zkClient, adapter, CLUSTER);
    seedAccessor.putVersion(storeName, new VersionImpl(storeName, 4, "push-4"));
    seedAccessor.putVersion(storeName, new VersionImpl(storeName, 5, "push-5"));

    HelixReadWriteStoreRepository freshRepo = newRepo();
    freshRepo.refresh();

    Store hydrated = freshRepo.getStore(storeName);
    assertNotNull(hydrated);
    assertEquals(hydrated.getVersions().size(), 5);
    for (int v = 1; v <= 5; v++) {
      assertTrue(hydrated.containsVersion(v));
    }
    assertEquals(hydrated.getVersion(1).getPushJobId(), "legacy-push-1");
    assertEquals(hydrated.getVersion(4).getPushJobId(), "push-4");
  }

  /**
   * A version number that exists in BOTH the embedded list and as a per-version znode is a corrupt-state bug. The read
   * path detects this and throws so callers can't silently work with ambiguous data.
   */
  @Test
  public void overlapBetweenEmbeddedAndZnodeThrows() {
    String storeName = Utils.getUniqueString("overlap_store");

    Store legacyStore = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    legacyStore.addVersion(new VersionImpl(storeName, 1, "legacy-push-1"));
    seedLegacyStoreDirectly(storeName, legacyStore);

    HelixVersionAccessor seedAccessor = new HelixVersionAccessor(zkClient, adapter, CLUSTER);
    seedAccessor.putVersion(storeName, new VersionImpl(storeName, 1, "znode-push-1"));

    HelixReadWriteStoreRepository freshRepo = newRepo();
    assertThrows(VeniceException.class, freshRepo::refresh);
  }

  /**
   * Flag-off behavior: addStore must persist the entire versions list inside the store znode and create no
   * per-version znodes. This is the layout every cluster used before the split-write work.
   */
  @Test
  public void addStoreWithFlagOffEmbedsVersionsAndCreatesNoZnodes() {
    HelixReadWriteStoreRepository legacyRepo = newRepo(false);
    legacyRepo.refresh();

    String storeName = Utils.getUniqueString("legacy_write_store");
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    store.addVersion(new VersionImpl(storeName, 1, "push-1"));
    store.addVersion(new VersionImpl(storeName, 2, "push-2"));
    legacyRepo.addStore(store);

    ZkBaseDataAccessor<Object> raw = new ZkBaseDataAccessor<>(zkClient);
    assertFalse(
        raw.exists(STORES_PATH + "/" + storeName + "/versions", AccessOption.PERSISTENT),
        "flag-off writes must not create the versions container");
    Store onZk = (Store) raw.get(STORES_PATH + "/" + storeName, null, AccessOption.PERSISTENT);
    assertNotNull(onZk);
    assertEquals(onZk.getVersions().size(), 2, "flag-off writes embed every version in the store znode");
    assertTrue(onZk.containsVersion(1));
    assertTrue(onZk.containsVersion(2));
  }

  /**
   * Flag-off behavior: a newly added version on updateStore lands in the embedded list, not a znode. Confirms that
   * routing the flag through to {@link HelixReadWriteStoreRepository#writeStoreToZk} actually changes behavior end to
   * end on the update path (not just on addStore).
   */
  @Test
  public void updateStoreWithFlagOffAppendsToEmbeddedListInsteadOfCreatingZnode() {
    HelixReadWriteStoreRepository legacyRepo = newRepo(false);
    legacyRepo.refresh();

    String storeName = Utils.getUniqueString("legacy_update_store");
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    store.addVersion(new VersionImpl(storeName, 1, "push-1"));
    legacyRepo.addStore(store);

    Store fetched = legacyRepo.getStore(storeName);
    fetched.addVersion(new VersionImpl(storeName, 2, "push-2"));
    legacyRepo.updateStore(fetched);

    ZkBaseDataAccessor<Object> raw = new ZkBaseDataAccessor<>(zkClient);
    assertFalse(raw.exists(STORES_PATH + "/" + storeName + "/versions/2", AccessOption.PERSISTENT));
    Store onZk = (Store) raw.get(STORES_PATH + "/" + storeName, null, AccessOption.PERSISTENT);
    assertEquals(onZk.getVersions().size(), 2);
    assertTrue(onZk.containsVersion(2));
  }

  /**
   * Forward compatibility: a reader on a build that supports the split layout must correctly hydrate a store that a
   * flag-off controller wrote in the legacy single-znode layout. This is the steady state during a rollout where
   * readers have been upgraded but the writer's flag has not been flipped yet.
   */
  @Test
  public void splitAwareReaderHydratesLegacyWrittenStore() {
    HelixReadWriteStoreRepository legacyWriter = newRepo(false);
    legacyWriter.refresh();

    String storeName = Utils.getUniqueString("rollout_store");
    Store store = TestUtils.createTestStore(storeName, "owner", System.currentTimeMillis());
    store.addVersion(new VersionImpl(storeName, 1, "push-1"));
    store.addVersion(new VersionImpl(storeName, 2, "push-2"));
    legacyWriter.addStore(store);

    HelixReadWriteStoreRepository splitAwareReader = newRepo(true);
    splitAwareReader.refresh();

    Store hydrated = splitAwareReader.getStore(storeName);
    assertNotNull(hydrated);
    assertEquals(hydrated.getVersions().size(), 2);
    assertTrue(hydrated.containsVersion(1));
    assertTrue(hydrated.containsVersion(2));
  }

  private void seedLegacyStoreDirectly(String storeName, Store store) {
    adapter.registerSerializer(STORES_PATH + "/*", new StoreJSONSerializer());
    zkClient.setZkSerializer(adapter);
    ZkBaseDataAccessor<Store> storeAccessor = new ZkBaseDataAccessor<>(zkClient);
    storeAccessor.create(STORES_PATH + "/" + storeName, store, AccessOption.PERSISTENT);
  }

  private HelixReadWriteStoreRepository newRepo() {
    return newRepo(true);
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
