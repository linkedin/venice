package com.linkedin.venice.storage;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.server.PartitionAssignmentRepository;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.store.bdb.BdbStorageEngineFactory;
import org.testng.annotations.Test;

import java.io.File;


public class BdbStoreTest extends AbstractStoreTest {

  PartitionAssignmentRepository partitionAssignmentRepository;
  VeniceConfigLoader veniceConfigLoader;

  public BdbStoreTest()
    throws Exception {
    createStoreForTest();
  }

  @Override
  public void createStoreForTest()
    throws Exception {
    /* TODO: this does not run from IDE because IDE expects relative path starting from venice-server */
    File configFile = new File("src/test/resources/config");
    veniceConfigLoader = VeniceConfigLoader.loadFromConfigDirectory(configFile.getAbsolutePath());
    VeniceServerConfig serverConfig = veniceConfigLoader.getVeniceServerConfig();
    String storeName = "testng-bdb";
    VeniceStoreConfig storeConfig = veniceConfigLoader.getStoreConfig(storeName);



    // populate partitionNodeAssignment
    partitionAssignmentRepository = new PartitionAssignmentRepository();
    int nodeId = 0;
    // only adding 1 partition, config indicates 5 partitions
    partitionAssignmentRepository.addPartition(storeName, 0);

    BdbStorageEngineFactory factory = new BdbStorageEngineFactory(serverConfig, partitionAssignmentRepository);
    testStore = factory.getStore(storeConfig);
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
