package com.linkedin.venice.meta;

import com.sun.istack.internal.NotNull;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.linkedin.venice.meta.MetadataConstants.*;


/**
 * Class defines the store of Venice.
 */
public class Store implements Serializable {
    /**
     * Store name.
     */
    private final String name;
    /**
     * Owner of this store.
     */
    private final String owner;
    /**
     * Default partition number of this store. Each version should use this number in most of cases.  Unless there is a
     * dramatic change on data size between different versions.
     */
    private final int defaultPartitionNumber;
    /**
     * time when this store was created.
     */
    private final long createdTime;
    /**
     * The number of version which is used currently.
     */
    private int currentVersion = 0;
    /**
     * Number of essential replication.
     */
    private int replicationFactor = DEFAULT_REPLICATION_FACTOR;
    /**
     * Type of persistence storage engine.
     */
    private String persistenceType = IN_MEMORY;
    /**
     * How to route the key to partition.
     */
    private String routingStrategy = CONSISTENCY_HASH;
    /**
     * How to read data from multiple replications.
     */
    private String readStrategy = ANY_OF_ONLINE;
    /**
     * When doing off-line push, how to decide the data is ready to serve.
     */
    private String offLinePushStrategy = WAIT_ALL_REPLICAS;
    /**
     * List of non-retiered versions.
     */
    private List<Version> versions;

    public Store(String name, String owner, int defaultPartitionNumber, long createdTime) {
        this.name = name;
        this.owner = owner;
        this.defaultPartitionNumber = defaultPartitionNumber;
        this.createdTime = createdTime;
        versions = new ArrayList<Version>();
    }

    public String getName() {
        return name;
    }

    public String getOwner() {
        return owner;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public int getCurrentVersion() {
        return currentVersion;
    }

    public void setCurrentVersion(int currentVersion) {
        this.currentVersion = currentVersion;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public String getRoutingStrategy() {
        return routingStrategy;
    }

    public void setRoutingStrategy(String routingStrategy) {
        this.routingStrategy = routingStrategy;
    }

    public String getReadStrategy() {
        return readStrategy;
    }

    public void setReadStrategy(@NotNull String readStrategy) {
        this.readStrategy = readStrategy;
    }

    public String getOffLinePushStrategy() {
        return offLinePushStrategy;
    }

    public void setOffLinePushStrategy(@NotNull String offLinePushStrategy) {
        this.offLinePushStrategy = offLinePushStrategy;
    }

    public String getPersistenceType() {
        return persistenceType;
    }

    public void setPersistenceType(@NotNull String persistenceType) {
        this.persistenceType = persistenceType;
    }

    public int getDefaultPartitionNumber() {
        return defaultPartitionNumber;
    }

    public List<Version> getVersions() {
        return Collections.unmodifiableList(this.versions);
    }

    /**
     * Add a version into store.
     *
     * @param version
     */
    public void addVersion(@NotNull Version version) {
        int index = 0;
        for (; index < versions.size(); index++) {
            if (versions.get(index).getNumber() == version.getNumber()) {
                throw new IllegalArgumentException(
                    "Version is repeated.Store:" + name + " Number:" + version.getNumber());
            }
            if (versions.get(index).getNumber() > version.getNumber()) {
                break;
            }
        }
        versions.add(index, version);
    }

    /**
     * Delete a version into store.
     *
     * @param versionNumber
     */
    public void deleteVersion(int versionNumber) {
        for (int i = 0; i < versions.size(); i++) {
            if (versions.get(i).getNumber() == versionNumber) {
                versions.remove(i);
                break;
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Store store = (Store) o;

        if (defaultPartitionNumber != store.defaultPartitionNumber) {
            return false;
        }
        if (createdTime != store.createdTime) {
            return false;
        }
        if (currentVersion != store.currentVersion) {
            return false;
        }
        if (replicationFactor != store.replicationFactor) {
            return false;
        }
        if (!name.equals(store.name)) {
            return false;
        }
        if (!owner.equals(store.owner)) {
            return false;
        }
        if (!persistenceType.equals(store.persistenceType)) {
            return false;
        }
        if (!routingStrategy.equals(store.routingStrategy)) {
            return false;
        }
        if (!readStrategy.equals(store.readStrategy)) {
            return false;
        }
        if (!offLinePushStrategy.equals(store.offLinePushStrategy)) {
            return false;
        }
        return versions.equals(store.versions);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + owner.hashCode();
        result = 31 * result + defaultPartitionNumber;
        result = 31 * result + (int) (createdTime ^ (createdTime >>> 32));
        result = 31 * result + currentVersion;
        result = 31 * result + replicationFactor;
        result = 31 * result + persistenceType.hashCode();
        result = 31 * result + routingStrategy.hashCode();
        result = 31 * result + readStrategy.hashCode();
        result = 31 * result + offLinePushStrategy.hashCode();
        result = 31 * result + versions.hashCode();
        return result;
    }

    /**
     * Cloned a new store based on current data in this store.
     *
     * @return cloned store.
     */
    public Store cloneStore() {
        Store clonedStore = new Store(name, owner, defaultPartitionNumber, createdTime);
        clonedStore.setCurrentVersion(currentVersion);
        clonedStore.setOffLinePushStrategy(offLinePushStrategy);
        clonedStore.setPersistenceType(persistenceType);
        clonedStore.setReadStrategy(readStrategy);
        clonedStore.setReplicationFactor(replicationFactor);
        clonedStore.setRoutingStrategy(routingStrategy);

        for (Version v : this.versions) {
            clonedStore.addVersion(v.cloneVersion());
        }
        return clonedStore;
    }
}
