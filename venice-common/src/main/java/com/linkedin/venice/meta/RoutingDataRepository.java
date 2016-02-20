package com.linkedin.venice.meta;

import java.util.List;


/**
 * Repository to accesss routing data like Partition and replica.
 * <p>
 * In Helix Full-auto model, Helix manage how to assign partitions to nodes. So here repository is read-only. In the
 * further, if Venice need more flexibility to manage cluster, some update/delete methods could be added here.
 */
public interface RoutingDataRepository {
    public List<Instance> getInstances(String resourceName, int partitionId);

    public List<Integer> getPartitionIds(String resourceName);

    public List<Partition> getPartitions(String resoursName);

    public int getPartitionNumber(String resourceName);
}
