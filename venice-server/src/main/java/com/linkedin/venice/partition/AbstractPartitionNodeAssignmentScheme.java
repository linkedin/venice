package com.linkedin.venice.partition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


/**
 * A AbstractPartitionNodeAssignmentScheme maps propagating layer's (For ex. kafka's) partitions to nodes based on some scheme.
 *
 * Assumptions:
 * 1. The number of logical partitions a node holds can increase with a new Store.
 * 2. The physical storage partitions are named in this format "<storename>_<propagating_Layer_partition_id>. Note that storenames are case sensitive.
 * 3. The physical storage partitions are not multi tenant - meaning they cannot listen to two different kafka topics
 * 4. Each physical node will hold on an average ((#LogicalPartitionsFromPropagatingLauer * ReplicationFactor)/ #Nodes). Here the #LogicalPartitionsFromPropagatingLayer does not consider replication within the propagating layer
 *
 * Multi tenancy and performance problems in future:
 * TODO: Later when we reach a situation where the number of physical node partitions increase due to different reasons like:
 * 1. Storage constraints
 * 2. Storage engine does no scale well for database per partition model
 * 3. Other bottlenecks like throughput is high. etc
 *
 * At that point we may have to decide to do the following-
 * 1. Add more storage to the nodes or expand the number of nodes.
 * 2. Change the Storage engine model from one database per partition to one database per store. Or one database for 3 storage partitions,etc.
 * 3. Decide if we have to rebalance the cluster, or move a high throughput Venice store to another cluster.
 *
 */
public abstract class AbstractPartitionNodeAssignmentScheme {

  private String name;

  public AbstractPartitionNodeAssignmentScheme(String name) {
    this.name = name;
  }

  /**
   * Get the name of the PartitionNodeAssignmentScheme
   *
   * @return the name of the Scheme
   */
  public String getName() {
    return this.name;
  }

  /**
   * When a new Venice Store is added (a new topic is added). The corresponding logical partitions from the propagating
   * layer need to be mapped to nodes. This method returns such a mapping and the caller of this method creates
   * new Venice storage partitions based on the mapping and updates the PartitionNodeAssignmentRepository.
   *
   * @param storeConfig  The configs related to this store.
   * @param numStorageNodes  Total number of storage nodes in the cluster
   * @return A map where key is a node ids and value is the corresponding set of logical partitions that the node is responsible for.
   */
  public abstract Map<Integer, Set<Integer>> getNodeToLogicalPartitionsMap(Properties storeConfig, int numStorageNodes);
}
