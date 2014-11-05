package com.linkedin.venice.partition;

import java.util.List;
import java.util.Map;


/**
 * A PartitionNodeMappingStrategy maps kafka partitions to storage nodes based on some scheme.
 *
 * Assumptions:
 * 1. The number of physical partitions a storage node holds can increase with a new kafka topic
 * 2. The node partitions are named in this format "<storename>_<kafka_partiion_id>. Note that storenames are case sensitive.
 * 3. The node partitions are not multi tenant - meaning they cannot listen to two different kafka topics
 * 4. Each physical node will hold on an average ((#KakfkaPartitions * ReplicationFactor)/ #StorageNodes). Here the #KafkaPartitions do not consider replication within kafka
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
 * 3. Decide if we have to rebalance the cluster, or move a high throuput Venice store to another cluster.
 *
 */
public interface PartitionNodeMappingStrategy {
  /**
   * Get the name of the PartitionNodeMappingStrategy
   *
   * @return the name of the Strategy
   */
  public String getStrategyName();

  /**
   * When a new Venice Store is added (a new Kafka topic is added). the corresponding partitions need to be mapped to storage nodes.
   * In mapping the new kafka partitions, new storage node partitions are created as well based on method arguments
   *
   * @param veniceStoreName The new store name also same as the kafkaTopic name.
   * @param numberOfKafkaPartitions  Number of Kafka partitions (without counting replica) in this kafka topic
   * @param storageReplicationFactor  The desired replciation factor in storage side
   */
  public void assignKafkaPartitionToNode(String veniceStoreName, int numberOfKafkaPartitions,
      int storageReplicationFactor);

  /**
   * Get List of all nodes that listen to a specific kafka partition. Note that Kafka Topic name is same as Venice StoreName.
   * This will be later used by the reader to chosse which storage node to query the key for.
   *
   * @param veniceStoreName  to search for
   * @param kafkaPartitionId Kafkfa PartitionId
   * @return All node ids that subsribe to the @kafkaPartitionId
   */
  public List<Integer> getAllNodeIdsSubscribedToKafkaPartition(String veniceStoreName, int kafkaPartitionId);

  /**
   * Given a node id, get the map of all stores and their corresponding storage partitions
   *
   * @param nodeId the node id to look up
   * @return Map of <storename, List<storage partiion ids>>
   *
   */
  public Map<String, List<String>> getStorePartitionList(int nodeId);

  /**
   * Given the nodeId and Venice StoreName get the list of storage partitions served
   *
   * @param nodeId the node id to search for
   * @param veniceStoreName the storename to look up
   * @return List of storage partitions owned by the node @nodeId and store @veniceStoreName
   */
  public List<String> getPartitionList(int nodeId, String veniceStoreName);

  /**
   * Given a Venice StoreName, get all nodes hosting the store and the corresponding storage partitions
   *
   * @param veniceStoreName to search for
   * @return Map of <nodeid, corresponding storage partition ids >
   */
  public Map<Integer, List<Integer>> getNodePartitionList(String veniceStoreName);
}
