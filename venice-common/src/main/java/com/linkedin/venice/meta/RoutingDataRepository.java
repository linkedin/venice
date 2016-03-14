package com.linkedin.venice.meta;

import java.util.List;
import java.util.Map;


/**
 * Repository to accesss routing data like Partition and replica.
 * <p>
 * In Helix Full-auto model, Helix manage how to assign partitions to nodes. So here repository is read-only. In the
 * further, if Venice need more flexibility to manage cluster, some update/delete methods could be added here.
 */
public interface RoutingDataRepository {
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
}
