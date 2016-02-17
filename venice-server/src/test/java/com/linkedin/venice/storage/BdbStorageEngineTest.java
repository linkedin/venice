package com.linkedin.venice.storage;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.partition.AbstractPartitionNodeAssignmentScheme;
import com.linkedin.venice.partition.ModuloPartitionNodeAssignmentScheme;
import com.linkedin.venice.server.PartitionNodeAssignmentRepository;
import com.linkedin.venice.server.VeniceConfigService;
import com.linkedin.venice.store.bdb.BdbStorageEngineFactory;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;


public class BdbStorageEngineTest extends AbstractStorageEngineTest {

  PartitionNodeAssignmentRepository partitionNodeAssignmentRepository;
  AbstractPartitionNodeAssignmentScheme partitionNodeAssignmentScheme;
  VeniceConfigService veniceConfigService;

  public BdbStorageEngineTest()
    throws Exception {
    createStorageEngineForTest();
  }

  @Override
  public void createStorageEngineForTest()
    throws Exception {
    File configFile = new File("src/test/resources/config");  //TODO this does not run from IDE because IDE expects
    // relative path starting from venice-server
    veniceConfigService = new VeniceConfigService(configFile.getAbsolutePath());
    Map<String, VeniceStoreConfig> storeConfigs = veniceConfigService.getAllStoreConfigs();

    if (storeConfigs.size() < 1) {
      throw new Exception("No stores defined for executing tests");
    }

    String storeName = "testng-bdb";
    VeniceStoreConfig storeConfig = storeConfigs.get(storeName);

    partitionNodeAssignmentScheme = new ModuloPartitionNodeAssignmentScheme();
    //populate partitionNodeAssignment
    partitionNodeAssignmentRepository = new PartitionNodeAssignmentRepository();
    partitionNodeAssignmentRepository.setAssignment(storeName,
      partitionNodeAssignmentScheme.getNodeToLogicalPartitionsMap(storeConfig));

    VeniceServerConfig serverConfig = veniceConfigService.getVeniceServerConfig();

    BdbStorageEngineFactory factory = new BdbStorageEngineFactory(serverConfig, partitionNodeAssignmentRepository);
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
