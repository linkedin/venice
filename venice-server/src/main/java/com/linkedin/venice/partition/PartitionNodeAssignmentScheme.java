package com.linkedin.venice.partition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * A PartitionNodeAssignmentScheme maps propagating layer's (For ex. kafka's) partitions to storage nodes based on some scheme.
 *
 * Assumptions:
 * 1. The number of physical partitions a storage node holds can increase with a new Store.
 * 2. The physical storage partitions are named in this format "<storename>_<propagating_Layer_partition_id>. Note that storenames are case sensitive.
 * 3. The physical storage partitions are not multi tenant - meaning they cannot listen to two different kafka topics
 * 4. Each physical node will hold on an average ((#LogicalPartitionsFromPropagatingLauer * ReplicationFactor)/ #StorageNodes). Here the #LogicalPartitionsFromPropagatingLayer does not consider replication within the propagating layer
 *
 * Multi tenancy and performance problems in future:
 * TODO: Later when we reach a situation where the number of physical node partitions increase due to different reasons like:
 * 1. Storage constraints
 * 2. Storage engine does no scale well for database per partition model
 * 3. Other bottlenecks like throughput is high. etc
 *
 * At that point we may have to decide to do the following-
 * 1. Add more storage to the physical nodes or expand the number of nodes.
 * 2. Change the Storage engine model from one database per partition to one database per store. Or one database for 3 storage partitions,etc.
 * 3. Decide if we have to rebalance the cluster, or move a high throughput Venice store to another cluster.
 *
 */
public abstract class PartitionNodeAssignmentScheme {

  private String schemeName;

  public PartitionNodeAssignmentScheme(String schemeName) {
    this.schemeName = schemeName;
  }

  /**
   * Get the name of the PartitionNodeAssignmentScheme
   *
   * @return the name of the Scheme
   */
  public String getStrategyName() {
    return this.schemeName;
  }

  /**
   * Given the store name and the propagation layer's logical partition id derive the venice storage
   * logical partition id
   *
   * @param storeName  The venice storeName
   * @param propagationLayerLogicalPartitionId The partition id of the propagating layer for the given store
   * @return venice storage logical partition id
   */
  public static String getVeniceLogicalPartitionId(String storeName, int propagationLayerLogicalPartitionId) {
    return storeName + "_" + propagationLayerLogicalPartitionId;
  }

  /**
   * When a new Venice Store is added (a new topic is added). The corresponding logical partitions from the propagating
   * layer need to be mapped to storage nodes. This method returns such a mapping and the caller of this method creates
   * new Venice storage partitions based on the mapping and updates the PartitionNodeAssignmentRepository.
   *
   *
   * @param storeName The new store name. (If kafka is the propagating layer, then this is same as Kafka topic)
   * @param numberOfLogicalPartitionsFromPropagatingLayer  Number of logical partitions from the propagating layer (without counting replica) for this new store
   * @param storageReplicationFactor  The desired replication factor in storage side
   * @return a hashmap where key is a node ids and value is the corresponding partitions that the node is responsible for.
   */
  public abstract HashMap<Integer, List<String>> MapPropagatingLayerPartitionsToStorageNode(String storeName,
      int numberOfLogicalPartitionsFromPropagatingLayer, int storageReplicationFactor);
}
