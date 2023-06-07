package com.linkedin.venice.meta;

import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import java.util.List;
import java.util.Map;


/**
 * Look up online instances related to a topic's partition. It's used in Venice router to help route requests to
 * a certain host. Check out VeniceVersionFinder and VeniceHostFinder for more details.
 *
 * Currently, there are 2 implementations based on different sources of metadata:
 * 1. {@link HelixCustomizedViewOfflinePushRepository} used in Router.
 * 2. {@link HelixExternalViewRepository} used in Controller and Server.
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
   * Query instances that are online (in leader or follower state), but not necessarily ready to serve yet.
   */
  List<Instance> getWorkingInstances(String kafkaTopic, int partitionId);

  /**
   * Query instances that belong to given kafka topic and partition.
   * @return a map that has {@link com.linkedin.venice.helix.HelixState} as the key and list of instances as the value
   */
  Map<ExecutionStatus, List<Instance>> getAllInstances(String kafkaTopic, int partitionId);

  /**
   * Query number of partition in given kafka topic.
   */
  int getNumberOfPartitions(String kafkaTopic);
}
