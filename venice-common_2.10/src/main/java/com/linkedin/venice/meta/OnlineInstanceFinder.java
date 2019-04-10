package com.linkedin.venice.meta;

import java.util.List;
import java.util.Map;


/**
 * Look up online instances related to a topic's partition. It's used in Venice router to help route requests to
 * a certain host. Check out VeniceVersionFinder and VeniceHostFinder for more details.
 *
 * Currently, there are 2 approaches.
 * 1. {@link com.linkedin.venice.helix.HelixRoutingDataRepository} finds online hosts according to Helix resource
 * current state. This approach is used if a resource is in Online/Offline state model.
 * 2. {@link com.linkedin.venice.pushmonitor.PartitionStatusOnlineInstanceFinder} finds online hosts according to
 * Partition status. This approach is used if a resource is in Leader/Follower state model.
 */
public interface OnlineInstanceFinder {
  /**
   * Query instances that belong to given kafka topic and partition. All of instances in result are ready to serve.
   */
  List<Instance> getReadyToServeInstances(String kafkaTopic, int partitionId);

  /**
   * Query instances that belong to given kafka topic and partition.
   * @return a map that has {@link com.linkedin.venice.helix.HelixState} as the key and list of instances as the value
   */
  Map<String, List<Instance>> getAllInstances(String kafkaTopic, int partitionId);

  /**
   * Query number of partition in given kafka topic.
   */
  int getNumberOfPartitions(String kafkaTopic);
}
