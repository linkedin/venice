package com.linkedin.venice.partition;

import com.linkedin.venice.server.VeniceConfig;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


public class ModuloPartitionNodeAssignmentScheme extends AbstractPartitionNodeAssignmentScheme {

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
  public Map<Integer, Set<Integer>> getNodeToLogicalPartitionsMap(Properties storeConfig, int numStorageNodes) {
    Map<Integer, Set<Integer>> nodeToLogicalPartitionIdsMap = new HashMap<Integer, Set<Integer>>();
    // TODO This assumes that the store configs has properties like: 1) # of Kafka partitions, 2) Replication factor in
    // storage etc. Make sure the storeConfig is isolated and cleaned up in VencieServer before the control reaches here.
    for (int i = 0; i < Integer.parseInt(storeConfig.getProperty("kafka.number.partitions")); i++) {
      for (int j = 0; j < Integer.parseInt(storeConfig.getProperty("storage.node.replicas", "2")); j++) {
        int nodeId = (i + j) % numStorageNodes;
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
