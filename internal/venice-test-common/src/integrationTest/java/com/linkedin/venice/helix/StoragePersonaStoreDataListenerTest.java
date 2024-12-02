package com.linkedin.venice.helix;

import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StoragePersonaStoreDataListenerTest {
  public HelixReadWriteStoreRepository storeRepository;
  private StoragePersonaRepository personaRepository;
  private ZkClient zkClient;
  private ZkServerWrapper zk;
  private final HelixAdapterSerializer adapter = new HelixAdapterSerializer();
  private final static String testClusterName = "testClusterName";

  @BeforeMethod
  public void setUp() {
    zk = ServiceFactory.getZkServer();
    zkClient = ZkClientFactory.newZkClient(zk.getAddress());
    storeRepository = new HelixReadWriteStoreRepository(
        zkClient,
        adapter,
        testClusterName,
        Optional.empty(),
        new ClusterLockManager(testClusterName));
    personaRepository = new StoragePersonaRepository(testClusterName, storeRepository, adapter, zkClient);
  }

  @AfterMethod
  public void cleanUp() {
    zkClient.close();
    zk.close();
  }

  @Test
  public void testDeleteStore() {
    Store store = TestUtils.createTestStore("testStore", "testOwner", 1000);
    store.setStorageQuotaInByte(100);
    storeRepository.putStore(store);
    Set<String> stores = new HashSet<>();
    stores.add(store.getName());
    Set<String> owners = new HashSet<>();
    owners.add("testOwner");
    personaRepository.addPersona("testPersona", 100, stores, owners);
    storeRepository.removeStore(store.getName());
    Assert.assertEquals(personaRepository.getPersona("testPersona").getStoresToEnforce().size(), 0);
    Assert.assertNull(personaRepository.getPersonaContainingStore(store.getName()));
  }

}
