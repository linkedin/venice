package com.linkedin.venice.meta;

import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.routerapi.ReplicaState;
import java.util.List;
import java.util.Map;


/**
 * Look up online instances related to a topic's partition. It's used in Venice router to help route requests to
 * a certain host. Check out VeniceVersionFinder and VeniceHostFinder for more details.
 *
 * Currently, there are 2 approaches.
 * 1. {@link HelixExternalViewRepository} finds online hosts according to Helix resource
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
   * Look for ready to serve instances on the given partition assignment. This is normally used to predict if
   * a potential partition assignment will cause any replica unavailability issue
   */
  List<Instance> getReadyToServeInstances(PartitionAssignment partitionAssignment, int partitionId);

  /**
   * Query instances that belong to given kafka topic and partition.
   * @return a map that has {@link com.linkedin.venice.helix.HelixState} as the key and list of instances as the value
   */
  Map<String, List<Instance>> getAllInstances(String kafkaTopic, int partitionId);

  /**
   * @return a list of {@link ReplicaState} with replica level details about a given store name, version and partition.
   */
  List<ReplicaState> getReplicaStates(String kafkaTopic, int partitionId);

  /**
   * Query number of partition in given kafka topic.
   */
  int getNumberOfPartitions(String kafkaTopic);

  /**
   * Check whether the underlying {@link RoutingDataRepository} contains certain resource.
   */
  boolean hasResource(String resourceName);
}
