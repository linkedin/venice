package com.linkedin.venice.storage;

import com.linkedin.venice.partition.AbstractPartitionNodeAssignmentScheme;
import com.linkedin.venice.partition.ModuloPartitionNodeAssignmentScheme;
import com.linkedin.venice.server.PartitionNodeAssignmentRepository;
import com.linkedin.venice.server.VeniceConfig;
import com.linkedin.venice.store.Store;
import com.linkedin.venice.store.memory.InMemoryStorageEngine;
import java.util.Properties;


public class InMemoryStoreTest extends AbstractStoreTest{

  PartitionNodeAssignmentRepository partitionNodeAssignmentRepository;
  AbstractPartitionNodeAssignmentScheme partitionNodeAssignmentScheme;
  int numStorageNodes = 1;
  String storeName = "test";
  int replicationFactor = 1;
  int nodeId = 0;
  VeniceConfig veniceConfig;

  public InMemoryStoreTest()
      throws Exception {
    this.numOfPartitions = 5;
    createStoreForTest();
  }

  @Override
  public void createStoreForTest()
      throws Exception {
    //Store Properties
    Properties storeConfigs = new Properties();
    storeConfigs.put("name",storeName);
    storeConfigs.put("kafka.number.partitions", String.valueOf(numOfPartitions));
    storeConfigs.put("storage.node.replicas",String.valueOf(replicationFactor));


    partitionNodeAssignmentScheme = new ModuloPartitionNodeAssignmentScheme();

    //populate partitionNodeAssignment
    partitionNodeAssignmentRepository = new PartitionNodeAssignmentRepository();
    partitionNodeAssignmentRepository.setAssignment(storeName, partitionNodeAssignmentScheme.getNodeToLogicalPartitionsMap(storeConfigs, numStorageNodes));

    Properties configProps = new Properties();
    configProps.put("node.id", String.valueOf(nodeId));
    veniceConfig = new VeniceConfig(configProps);

    Store inMemoryStorageEngine = new InMemoryStorageEngine(veniceConfig, storeConfigs, partitionNodeAssignmentRepository);
    this.testStore = inMemoryStorageEngine;
  }
}
