package com.linkedin.venice.storage;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.server.PartitionAssignmentRepository;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.bdb.BdbStorageEngineFactory;
import com.linkedin.venice.utils.VeniceProperties;
import org.testng.annotations.Test;


public class BdbStoreTest extends AbstractStoreTest {

  public BdbStoreTest()
    throws Exception {
    createStoreForTest();
  }

  @Override
  public void createStoreForTest()
    throws Exception {
    String storeName = "store-test-bdb";
    VeniceProperties storeProps = AbstractStorageEngineTest.getServerProperties(PersistenceType.BDB);
    VeniceStoreConfig storeConfig = new VeniceStoreConfig(storeName, storeProps);

    // populate partitionNodeAssignment
    PartitionAssignmentRepository partitionAssignmentRepository = new PartitionAssignmentRepository();
    int partitionId = 0;
    partitionAssignmentRepository.addPartition(storeName, partitionId);

    VeniceServerConfig serverConfig = new VeniceServerConfig(storeProps);
    BdbStorageEngineFactory factory = new BdbStorageEngineFactory(serverConfig);
    AbstractStorageEngine engine = factory.getStore(storeConfig);
    engine.addStoragePartition(partitionId);

    testStore = engine;

  }

  @Test
  public void testGetAndPut() {
    super.testGetAndPut();
  }

  @Test
  public void testDelete() {
    super.testDelete();
  }

  @Test
  public void testUpdate() {
    super.testUpdate();
  }

  @Test
  public void testGetInvalidKeys() {
    super.testGetInvalidKeys();
  }

  @Test
  public void testPutNullKey() {
    super.testPutNullKey();
  }


}
