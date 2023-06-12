package com.linkedin.venice.helix;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.SystemStore;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test(singleThreaded = true)
public class TestHelixReadOnlyStoreRepositoryAdapter {
  private ZkClient zkClient;
  private final static String cluster = "test-metadata-cluster";
  private final static String clusterPath = "/test-metadata-cluster";
  private ZkServerWrapper zkServerWrapper;
  private final HelixAdapterSerializer adapter = new HelixAdapterSerializer();

  private HelixReadOnlyStoreRepositoryAdapter repo;
  private HelixReadWriteStoreRepository writeRepo;

  private final VeniceSystemStoreType newRepositorySupportedSystemStoreType = VeniceSystemStoreType.META_STORE;
  private String regularStoreName;
  private String regularStoreNameWithMetaSystemStoreEnabled;

  @BeforeClass
  public void zkSetup() {
    zkServerWrapper = ServiceFactory.getZkServer();
    String zkAddress = zkServerWrapper.getAddress();
    zkClient = ZkClientFactory.newZkClient(zkAddress);
    zkClient.setZkSerializer(adapter);
    zkClient.create(clusterPath, null, CreateMode.PERSISTENT);
    String storesPath = "/stores";
    zkClient.create(clusterPath + storesPath, null, CreateMode.PERSISTENT);

    HelixReadOnlyZKSharedSystemStoreRepository zkSharedSystemStoreRepository =
        new HelixReadOnlyZKSharedSystemStoreRepository(zkClient, adapter, cluster);
    HelixReadOnlyStoreRepository storeRepository =
        new HelixReadOnlyStoreRepository(zkClient, adapter, cluster, 1, 1000);
    zkSharedSystemStoreRepository.refresh();
    storeRepository.refresh();
    repo = new HelixReadOnlyStoreRepositoryAdapter(zkSharedSystemStoreRepository, storeRepository, cluster);
    writeRepo = new HelixReadWriteStoreRepository(
        zkClient,
        adapter,
        cluster,
        Optional.empty(),
        new ClusterLockManager(cluster));
    repo.refresh();
    writeRepo.refresh();
    // Create zk shared store first
    Store zkSharedStore = TestUtils
        .createTestStore(newRepositorySupportedSystemStoreType.getZkSharedStoreName(), "test_system_store_owner", 1);
    zkSharedStore.setBatchGetLimit(1);
    zkSharedStore.setReadComputationEnabled(false);
    writeRepo.addStore(zkSharedStore);
    TestUtils.waitForNonDeterministicAssertion(
        10,
        TimeUnit.SECONDS,
        () -> assertTrue(writeRepo.hasStore(newRepositorySupportedSystemStoreType.getZkSharedStoreName())));
    // Create one regular store
    regularStoreName = Utils.getUniqueString("test_store");
    Store s1 = TestUtils.createTestStore(regularStoreName, "owner", System.currentTimeMillis());
    s1.addVersion(new VersionImpl(s1.getName(), s1.getLargestUsedVersionNumber() + 1, "pushJobId"));
    s1.setReadQuotaInCU(100);
    s1.setReadQuotaInCU(100);
    s1.setBatchGetLimit(100);
    s1.setReadComputationEnabled(true);
    writeRepo.addStore(s1);
    // Create another regular store with meta system store enabled explicitly
    regularStoreNameWithMetaSystemStoreEnabled = Utils.getUniqueString("test_store_with_meta_system_store_enabled");
    Store s2 =
        TestUtils.createTestStore(regularStoreNameWithMetaSystemStoreEnabled, "owner", System.currentTimeMillis());
    s2.addVersion(new VersionImpl(s2.getName(), s2.getLargestUsedVersionNumber() + 1, "pushJobId"));
    s2.setReadQuotaInCU(100);
    s2.setBatchGetLimit(100);
    s2.setReadComputationEnabled(true);
    s2.setStoreMetaSystemStoreEnabled(true);
    writeRepo.addStore(s2);
  }

  @AfterClass
  public void zkCleanup() {
    repo.clear();
    zkClient.deleteRecursively(clusterPath);
    zkClient.close();
    zkServerWrapper.close();
  }

  @Test
  public void testGetStore() {
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      // Regular venice store can be retrieved back
      assertNotNull(repo.getStore(newRepositorySupportedSystemStoreType.getZkSharedStoreName()));
      assertNotNull(repo.getStore(regularStoreName));
      // Unknown store should stay unknown
      assertNull(repo.getStore(Utils.getUniqueString("unknown_store")));
      // metadata system store for the existing regular store can be fetched
      assertNotNull(repo.getStore(newRepositorySupportedSystemStoreType.getSystemStoreName(regularStoreName)));
      // metadata system store for the unknown store should be null
      assertNull(
          repo.getStore(
              newRepositorySupportedSystemStoreType.getSystemStoreName(Utils.getUniqueString("unknown_store"))));

      // check some details of the retrieved metadata system store
      Store metadataSystemStore =
          repo.getStore(newRepositorySupportedSystemStoreType.getSystemStoreName(regularStoreName));
      assertTrue(
          metadataSystemStore instanceof SystemStore,
          "The returned store should be an instance of 'SystemStore'");
    });
  }

  @Test
  public void testHasStore() {
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      // Regular venice store can be retrieved back
      assertTrue(repo.hasStore(newRepositorySupportedSystemStoreType.getZkSharedStoreName()));
      assertTrue(repo.hasStore(regularStoreName));
      // Unknown store should stay unknown
      assertFalse(repo.hasStore(Utils.getUniqueString("unknown_store")));
      // metadata system store for the existing regular store can be fetched
      assertTrue(repo.hasStore(newRepositorySupportedSystemStoreType.getSystemStoreName(regularStoreName)));
      // metadata system store for the unknown store should be null
      assertFalse(
          repo.hasStore(
              newRepositorySupportedSystemStoreType.getSystemStoreName(Utils.getUniqueString("unknown_store"))));
    });
  }

  @Test
  public void testGetAllStores() {
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      List<Store> allStores = repo.getAllStores();
      assertEquals(
          allStores.size(),
          4,
          "'getAllStores' should return regular stores and the corresponding meta system store if enabled");
      Set<String> storeNameSet = allStores.stream().map(Store::getName).collect(Collectors.toSet());
      assertTrue(storeNameSet.contains(newRepositorySupportedSystemStoreType.getZkSharedStoreName()));
      assertTrue(storeNameSet.contains(regularStoreName));
      assertTrue(storeNameSet.contains(regularStoreNameWithMetaSystemStoreEnabled));
      assertTrue(
          storeNameSet.contains(
              VeniceSystemStoreType.META_STORE.getSystemStoreName(regularStoreNameWithMetaSystemStoreEnabled)));
    });
  }

  @Test
  public void testGetBatchGetLimit() {
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      assertEquals(
          repo.getBatchGetLimit(newRepositorySupportedSystemStoreType.getSystemStoreName(regularStoreName)),
          1);
      assertEquals(repo.getBatchGetLimit(regularStoreName), 100);
      assertThrows(() -> repo.getBatchGetLimit(Utils.getUniqueString("unknown_store")));
    });
  }

  @Test
  public void testIsReadComputationEnabled() {
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      assertFalse(
          repo.isReadComputationEnabled(newRepositorySupportedSystemStoreType.getSystemStoreName(regularStoreName)));
      assertTrue(repo.isReadComputationEnabled(regularStoreName));
      assertThrows(() -> repo.isReadComputationEnabled(Utils.getUniqueString("unknown_store")));
    });
  }

  @Test
  public void testListenersForZKSharedStoreChange() {
    StoreDataChangedListener storeDataChangedListener = mock(StoreDataChangedListener.class);
    ArgumentCaptor<Store> storeArgumentCaptor = ArgumentCaptor.forClass(Store.class);

    repo.registerStoreDataChangedListener(storeDataChangedListener);
    try {
      // Change the zk shared store
      String zkSharedStoreName = VeniceSystemStoreType.META_STORE.getZkSharedStoreName();
      Store zkSharedStore = writeRepo.getStoreOrThrow(zkSharedStoreName);
      zkSharedStore.setBatchGetLimit(100);
      writeRepo.updateStore(zkSharedStore);
      String systemStoreName =
          VeniceSystemStoreType.META_STORE.getSystemStoreName(regularStoreNameWithMetaSystemStoreEnabled);
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        // Will receive notification for both zk shared system store and the system store
        verify(storeDataChangedListener, times(2)).handleStoreChanged(storeArgumentCaptor.capture());
        List<Store> notifiedStores = storeArgumentCaptor.getAllValues();
        Set<String> notifiedStoreNames = notifiedStores.stream().map(Store::getName).collect(Collectors.toSet());
        assertTrue(
            notifiedStoreNames.contains(zkSharedStoreName),
            "ZK Shared store: " + zkSharedStoreName + " should be notified");
        assertTrue(
            notifiedStoreNames.contains(systemStoreName),
            "The corresponding system store: " + systemStoreName + " should be notified");
      });
    } finally {
      repo.unregisterStoreDataChangedListener(storeDataChangedListener);
    }
  }

  @Test
  public void testListenersForVeniceStoreChange() {
    StoreDataChangedListener storeDataChangedListener = mock(StoreDataChangedListener.class);
    ArgumentCaptor<Store> storeArgumentCaptor = ArgumentCaptor.forClass(Store.class);

    repo.registerStoreDataChangedListener(storeDataChangedListener);
    try {
      // Change regular store
      Store regularStore = writeRepo.getStore(regularStoreNameWithMetaSystemStoreEnabled);
      regularStore.setBackupStrategy(BackupStrategy.KEEP_MIN_VERSIONS);
      writeRepo.updateStore(regularStore);
      String systemStoreName =
          VeniceSystemStoreType.META_STORE.getSystemStoreName(regularStoreNameWithMetaSystemStoreEnabled);

      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        // Will receive notification for both the regular store and the corresponding system store
        verify(storeDataChangedListener, times(2)).handleStoreChanged(storeArgumentCaptor.capture());
        List<Store> notifiedStores = storeArgumentCaptor.getAllValues();
        Set<String> notifiedStoreNames = notifiedStores.stream().map(Store::getName).collect(Collectors.toSet());
        assertTrue(
            notifiedStoreNames.contains(regularStoreNameWithMetaSystemStoreEnabled),
            "Venice store: " + regularStoreNameWithMetaSystemStoreEnabled + " should be notified");
        assertTrue(
            notifiedStoreNames.contains(systemStoreName),
            "The corresponding system store: " + systemStoreName + " should be notified");
      });
    } finally {
      repo.unregisterStoreDataChangedListener(storeDataChangedListener);
    }
  }

  @Test
  public void testListenersForVeniceStoreCreationDeletion() {
    StoreDataChangedListener storeDataChangedListener = mock(StoreDataChangedListener.class);
    ArgumentCaptor<Store> storeArgumentCaptorForCreation = ArgumentCaptor.forClass(Store.class);

    repo.registerStoreDataChangedListener(storeDataChangedListener);
    try {
      // Create another store with meta system store enabled
      String newStoreName = Utils.getUniqueString("another_store_with_meta_system_store_enabled");
      Store newStore = TestUtils.createTestStore(newStoreName, "test_owner", System.currentTimeMillis());
      newStore.setStoreMetaSystemStoreEnabled(true);
      writeRepo.addStore(newStore);
      String systemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(newStoreName);

      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        // Will receive notification for both the regular store and the corresponding system store
        verify(storeDataChangedListener, times(2)).handleStoreCreated(storeArgumentCaptorForCreation.capture());
        List<Store> notifiedStores = storeArgumentCaptorForCreation.getAllValues();
        Set<String> notifiedStoreNames = notifiedStores.stream().map(Store::getName).collect(Collectors.toSet());
        assertTrue(
            notifiedStoreNames.contains(newStoreName),
            "Venice store: " + regularStoreNameWithMetaSystemStoreEnabled + " should be notified");
        assertTrue(
            notifiedStoreNames.contains(systemStoreName),
            "The corresponding system store: " + systemStoreName + " should be notified");
      });

      // Test store deletion
      writeRepo.deleteStore(newStoreName);
      ArgumentCaptor<Store> storeArgumentCaptorForDeletion = ArgumentCaptor.forClass(Store.class);

      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        // Will receive notification for both the regular store and the corresponding system store
        verify(storeDataChangedListener, times(2)).handleStoreDeleted(storeArgumentCaptorForDeletion.capture());
        List<String> notifiedStoreNames =
            storeArgumentCaptorForDeletion.getAllValues().stream().map(Store::getName).collect(Collectors.toList());
        assertTrue(
            notifiedStoreNames.contains(newStoreName),
            "Venice store: " + regularStoreNameWithMetaSystemStoreEnabled + " should be notified");
        assertTrue(
            notifiedStoreNames.contains(systemStoreName),
            "The corresponding system store: " + systemStoreName + " should be notified");
      });
    } finally {
      repo.unregisterStoreDataChangedListener(storeDataChangedListener);
    }
  }
}
