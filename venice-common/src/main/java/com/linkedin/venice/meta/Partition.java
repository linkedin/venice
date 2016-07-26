package com.linkedin.venice.meta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.validation.constraints.NotNull;


/**
 * Class defines the partition in Venice.
 * <p>
 * Partition is a logic unit to distributed the data in Venice cluster. Each resource(Store+version) will be assigned to
 * a set of partition so that data in this resource will be distributed averagely in ideal. Each partition contains 1 or
 * multiple replica which hold the same data in ideal.
 * <p>
 * In Helix Full-auto model, Helix manage how to assign partitions to nodes. So here partition is read-only. In the
 * further, if Venice need more flexibility to manage cluster, some update/delete methods could be added here.
 */
public class Partition {
    private static final String SEPARATOR = "_";
    /**
     * Id of partition. One of the number between [0 ~ total number of partition)
     */
    private final int id;
    /**
     * Instances who hold the replicas of this partition and still live. Live stands for it's ready to serve or doing
     * the bootstrap.
     */
    private final List<Instance> bootstrapInstances;

    private final List<Instance> readyToServeInstance;

    private final List<Instance> errorInstances;

    public Partition(int id, @NotNull List<Instance> bootstrapInstances,
        @NotNull List<Instance> readyToServeInstance, List<Instance> errorInstances) {
        this.id = id;
        this.bootstrapInstances = Collections.unmodifiableList(bootstrapInstances);
        this.readyToServeInstance = Collections.unmodifiableList(readyToServeInstance);
        this.errorInstances = Collections.unmodifiableList(errorInstances);
    }

    public List<Instance> getReadyToServeInstances() {
        return readyToServeInstance;
    }

    public List<Instance> getBootstrapAndReadyToServeInstances() {
        List<Instance> instances = new ArrayList<>(readyToServeInstance);
        instances.addAll(bootstrapInstances);
        return instances;
    }

    public List<Instance> getErrorInstances() {
        return errorInstances;
    }

    public List<Instance> getBootstrapInstances() {
        return bootstrapInstances;
    }

    public static String getPartitionName(String resourceName, int partitionId) {
        return resourceName + SEPARATOR + partitionId;
    }

    public static int getPartitionIdFromName(String partitionName) {
        try {
            return Integer.parseInt(partitionName.substring(partitionName.lastIndexOf(SEPARATOR) + 1));
        } catch (Throwable e) {
            throw new IllegalArgumentException("Partition name is invalid:" + partitionName);
        }
    }

    public int getId() {
        return id;
    }
}
