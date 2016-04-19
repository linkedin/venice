package com.linkedin.venice.storage;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.server.PartitionAssignmentRepository;
import com.linkedin.venice.server.VeniceConfigLoader;
import com.linkedin.venice.store.bdb.BdbStorageEngineFactory;
import org.testng.annotations.Test;

import java.io.File;


public class BdbStorageEngineTest extends AbstractStorageEngineTest {

  PartitionAssignmentRepository partitionAssignmentRepository;
  VeniceConfigLoader veniceConfigLoader;

  public BdbStorageEngineTest()
    throws Exception {
    createStorageEngineForTest();
  }

  @Override
  public void createStorageEngineForTest()
    throws Exception {
    File configFile = new File("src/test/resources/config");  //TODO this does not run from IDE because IDE expects
    // relative path starting from venice-server
    veniceConfigLoader = VeniceConfigLoader.loadFromConfigDirectory(configFile.getAbsolutePath());
    String storeName = "testng-bdb";
    VeniceStoreConfig storeConfig = veniceConfigLoader.getStoreConfig(storeName);

    //populate partitionNodeAssignment
    partitionAssignmentRepository = new PartitionAssignmentRepository();
    int nodeId = 0;
    // only adding 1 partition, config indicates 5 partitions
    partitionAssignmentRepository.addPartition(storeName, 0);

    VeniceServerConfig serverConfig = veniceConfigLoader.getVeniceServerConfig();

    BdbStorageEngineFactory factory = new BdbStorageEngineFactory(serverConfig, partitionAssignmentRepository);
    testStoreEngine = factory.getStore(storeConfig);

    createStoreForTest();
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

  @Test
  public void testPartitioning()
    throws Exception {
    super.testPartitioning();
  }

  @Test
  public void testAddingAPartitionTwice()
    throws Exception {
    super.testAddingAPartitionTwice();
  }

  @Test
  public void testRemovingPartitionTwice()
    throws Exception {
    super.testRemovingPartitionTwice();
  }

  @Test
  public void testOperationsOnNonExistingPartition()
    throws Exception {
    super.testOperationsOnNonExistingPartition();
  }
}
