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
     * Query instances that belong to given resource and partition.
     * @param resourceName
     * @param partitionId
     * @return
     */
    public List<Instance> getInstances(String resourceName, int partitionId);

    /**
     * Query all partitions that belong to given resource.
     * @param resourceName
     * @return
     */
    public Map<Integer,Partition> getPartitions(String resourceName);

    /**
     * Query number of partition in given resource.
     * @param resourceName
     * @return
     */
    public int getNumberOfPartitions(String resourceName);
}
