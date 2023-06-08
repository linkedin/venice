package com.linkedin.venice.meta;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.helix.ResourceAssignment;
import com.linkedin.venice.pushmonitor.ReadOnlyPartitionStatus;


/**
 * Repository to access routing data like Partition and replica.
 * <p>
 * In Helix Full-auto model, Helix manage how to assign partitions to nodes. So here repository is read-only. In the
 * further, if Venice need more flexibility to manage cluster, some update/delete methods could be added here.
 */
public interface RoutingDataRepository extends VeniceResource, OnlineInstanceFinder {
  /**
   * Query all partitions allocations that belong to given kafka topic. The instances in returned allocations are ready
   * to serve OR being bootstrap.
   */
  PartitionAssignment getPartitionAssignments(String kafkaTopic);

  /**
   * Whether this repository contains routing data for given kafka topic or not.
   */
  boolean containsKafkaTopic(String kafkaTopic);

  /**
   * Query the leader controller of current cluster.
   */
  Instance getLeaderController();

  /**
   * Timestamp in milliseconds of the last time leader controller changed.
   */
  long getLeaderControllerChangeTimeMs();

  /**
   * Add a listener on kafka topic to get the notification when routing data is changed.
   */
  void subscribeRoutingDataChange(String kafkaTopic, RoutingDataChangedListener listener);

  /**
   * Remove the listener for given kafka topic.
   */
  void unSubscribeRoutingDataChange(String kafkaTopic, RoutingDataChangedListener listener);

  boolean isLiveInstance(String instanceId);

  ResourceAssignment getResourceAssignment();

  /**
   * Whether the resources names exist in ideal state or not.
   */
  boolean doesResourcesExistInIdealState(String resource);

  /**
   * Given resource name and partition number, return the current leader instance.
   * @return the current leader instance. Null if there is no leader/the resource doesn't use
   * L/F model/resource doesn't exist.
   */
  Instance getLeaderInstance(String resourceName, int partition);

  interface RoutingDataChangedListener {
    /**
     * Handle routing data changed event.
     * @param partitionAssignment Newest partitions assignments information including resource name and  all of instances assigned to this resource.
     *                            If the number of partition is 0, it means the kafka topic is deleted.
     */

    void onExternalViewChange(PartitionAssignment partitionAssignment);

    void onCustomizedViewChange(PartitionAssignment partitionAssignment);

    void onPartitionStatusChange(String topic, ReadOnlyPartitionStatus partitionStatus);

    void onRoutingDataDeleted(String kafkaTopic);
  }
}
