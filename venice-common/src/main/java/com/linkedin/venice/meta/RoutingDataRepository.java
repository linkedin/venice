package com.linkedin.venice.meta;

import com.linkedin.venice.VeniceResource;
import java.util.List;
import java.util.Map;


/**
 * Repository to accesss routing data like Partition and replica.
 * <p>
 * In Helix Full-auto model, Helix manage how to assign partitions to nodes. So here repository is read-only. In the
 * further, if Venice need more flexibility to manage cluster, some update/delete methods could be added here.
 */
public interface RoutingDataRepository extends VeniceResource {
  /**
   * Query instances that belong to given kafka topic and partition.
   *
   * @param kafkaTopic
   * @param partitionId
   *
   * @return
   */
  public List<Instance> getInstances(String kafkaTopic, int partitionId);

  /**
   * Query all partitions that belong to given kafka topic.
   *
   * @param kafkaTopic
   *
   * @return
   */
  public Map<Integer, Partition> getPartitions(String kafkaTopic);

  /**
   * Query number of partition in given kafka topic.
   *
   * @param kafkaTopic
   *
   * @return
   */
  public int getNumberOfPartitions(String kafkaTopic);

  /**
   * Whether this repository contains routing data for given kafka topic or not.
   * @param kafkaTopic
   * @return
   */
  public boolean containsKafkaTopic(String kafkaTopic);

  /**
   * Query the master controller of current cluster.
   * @return
   */
  public Instance getMasterController();

  /**
   * Add a listener on kafka topic to get the notification when routing data is changed.
   *
   * @param kafkaTopic
   * @param listener
   */
  public void subscribeRoutingDataChange(String kafkaTopic, RoutingDataChangedListener listener);

  /**
   * Remove the listener for given kafka topic.
   *
   * @param kafkaTopic
   * @param listener
   */
  public void unSubscribeRoutingDataChange(String kafkaTopic, RoutingDataChangedListener listener);

  interface RoutingDataChangedListener {
    /**
     * Handle routing data changed event.
     *
     * @param kafkaTopic
     * @param partitions Newest partitions information. If it's null, it means the kafka topic is deleted. The key of
     *                   map is partition id and the value of map are the partition information including instances
     *                   assigned to this partition.
     */
    void onRoutingDataChanged(String kafkaTopic, Map<Integer, Partition> partitions);
  }
}
