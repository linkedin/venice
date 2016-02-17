package com.linkedin.venice.partition;

import com.linkedin.venice.config.VeniceStoreConfig;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class ModuloPartitionNodeAssignmentScheme extends AbstractPartitionNodeAssignmentScheme {

  @Deprecated // Partition Node Assignment will not happen at the storage-node level.  TODO: factor out the assignment
  public ModuloPartitionNodeAssignmentScheme() {
    super("modulo");
  }

  /**
   * Method that calculates the nodeIds (including the replica) for a given partitionId
   *
   * TODO: this is not truly balanced scheme since if the number of partitions per store vary, few set of nodes may host
   * more partitions than others.. This is a just a first cut partitioning scheme.
   */
  @Override
  public Map<Integer, Set<Integer>> getNodeToLogicalPartitionsMap(VeniceStoreConfig storeConfig) {
    Map<Integer, Set<Integer>> nodeToLogicalPartitionIdsMap = new HashMap<Integer, Set<Integer>>();
//    for (int i = 0; i < storeConfig.getNumKafkaPartitions(); i++) {
//      for (int j = 0; j < storeConfig.getStorageReplicationFactor(); j++) {
    for (int i = 0; i < 1; i++) {
      for (int j = 0; j < 1; j++) {
        int nodeId = (i + j) % storeConfig.getStorageNodeCount();
        if (!nodeToLogicalPartitionIdsMap.containsKey(nodeId)) {
          nodeToLogicalPartitionIdsMap.put(nodeId, new HashSet<Integer>());
        }
        Set<Integer> logicalPartitionIds = nodeToLogicalPartitionIdsMap.get(nodeId);
        logicalPartitionIds.add(i);
        nodeToLogicalPartitionIdsMap.put(nodeId, logicalPartitionIds);
      }
    }

    return nodeToLogicalPartitionIdsMap;
  }
}
