package com.linkedin.venice.storage;

import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.partition.AbstractPartitionNodeAssignmentScheme;
import com.linkedin.venice.partition.ModuloPartitionNodeAssignmentScheme;
import com.linkedin.venice.server.PartitionNodeAssignmentRepository;
import com.linkedin.venice.server.VeniceConfigService;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.memory.InMemoryStorageEngine;
import java.io.File;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class InMemoryStorageEngineTest extends AbstractStorageEngineTest {

  PartitionNodeAssignmentRepository partitionNodeAssignmentRepository;
  AbstractPartitionNodeAssignmentScheme partitionNodeAssignmentScheme;
  VeniceConfigService veniceConfigService;


  public InMemoryStorageEngineTest()
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

    if(storeConfigs.size() < 1){
      throw new Exception("No stores defined for executing tests");
    }

    String storeName = null;
    VeniceStoreConfig storeConfig = null;
    for(String store: storeConfigs.keySet()){
      storeName = store;
      storeConfig = storeConfigs.get(storeName);
      break;
    }

    partitionNodeAssignmentScheme = new ModuloPartitionNodeAssignmentScheme();
    numOfPartitions = storeConfig.getNumKafkaPartitions();
    //populate partitionNodeAssignment
    partitionNodeAssignmentRepository = new PartitionNodeAssignmentRepository();
    partitionNodeAssignmentRepository.setAssignment(storeName,
        partitionNodeAssignmentScheme.getNodeToLogicalPartitionsMap(storeConfig));

    AbstractStorageEngine inMemoryStorageEngine =
        new InMemoryStorageEngine(storeConfig, partitionNodeAssignmentRepository);
    this.testStoreEngine = inMemoryStorageEngine;

    createStoreForTest();
  }

  @Test
  public void testGetAndPut(){
    super.testGetAndPut();
  }

  @Test
  public void testDelete(){
    super.testDelete();
  }

  @Test
  public void testUpdate() {
    super.testUpdate();
  }

  @Test
  public void testGetInvalidKeys()
  {
    super.testGetInvalidKeys();
  }

  @Test
  public void testPutNullKey(){
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
      throws Exception{
    super.testRemovingPartitionTwice();
  }

  @Test
  public void testOperationsOnNonExistingPartition()
      throws Exception {
    super.testOperationsOnNonExistingPartition();
  }
}
