package com.linkedin.venice.meta;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.helix.ResourceAssignment;
import java.util.Map;


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
   * Query the master controller of current cluster.
   */
  Instance getMasterController();

  /**
   * Timestamp in milliseconds of the last time master controller changed.
   */
  long getMasterControllerChangeTime();

  /**
   * Add a listener on kafka topic to get the notification when routing data is changed.
   */
  void subscribeRoutingDataChange(String kafkaTopic, RoutingDataChangedListener listener);

  /**
   * Remove the listener for given kafka topic.
   */
  void unSubscribeRoutingDataChange(String kafkaTopic, RoutingDataChangedListener listener);

  /**
   * Get the map contains all live instances which key is instance Id and value is the instance object.
   */
  Map<String, Instance> getLiveInstancesMap();

  ResourceAssignment getResourceAssignment();

  /**
   * Whether the resources names exist in ideal state or not.
   */
  boolean doseResourcesExistInIdealState(String resource);

  interface RoutingDataChangedListener {
    /**
     * Handle routing data changed event.
     * @param partitionAssignment Newest partitions assignments information including resource name and  all of instances assigned to this resource.
     *                            If the number of partition is 0, it means the kafka topic is deleted.
     */
    void onRoutingDataChanged(PartitionAssignment partitionAssignment);

    void onRoutingDataDeleted(String kafkaTopic);
  }
}
