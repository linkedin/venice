package com.linkedin.venice.meta;

import com.sun.istack.internal.NotNull;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


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
     * time when this store was created.
     */
    private final long createdTime;
    /**
     * The number of version which is used currently.
     */
    private int currentVersion = 0;
    /**
     * Type of persistence storage engine.
     */
    private final PersistenceType persistenceType;
    /**
     * How to route the key to partition.
     */
    private final RoutingStrategy routingStrategy;
    /**
     * How to read data from multiple replications.
     */
    private final ReadStrategy readStrategy;
    /**
     * When doing off-line push, how to decide the data is ready to serve.
     */
    private final OfflinePUshStrategy offLinePushStrategy;
    /**
     * List of non-retired versions.
     */
    private List<Version> versions;

    public Store(String name, String owner, long createdTime) {
        this(name, owner, createdTime, PersistenceType.IN_MEMORY, RoutingStrategy.CONSISTENCY_HASH,
            ReadStrategy.ANY_OF_ONLINE, OfflinePUshStrategy.WAIT_ALL_REPLICAS);
    }

    public Store(String name, String owner, long createdTime, PersistenceType persistenceType,
        RoutingStrategy routingStrategy, ReadStrategy readStrategy, OfflinePUshStrategy offlinePUshStrategy) {
        this.name = name;
        this.owner = owner;
        this.createdTime = createdTime;
        this.persistenceType = persistenceType;
        this.routingStrategy = routingStrategy;
        this.readStrategy = readStrategy;
        this.offLinePushStrategy = offlinePUshStrategy;
        versions = new ArrayList<>();
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

    public PersistenceType getPersistenceType() {
        return persistenceType;
    }

    public RoutingStrategy getRoutingStrategy() {
        return routingStrategy;
    }

    public ReadStrategy getReadStrategy() {
        return readStrategy;
    }

    public OfflinePUshStrategy getOffLinePushStrategy() {
        return offLinePushStrategy;
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
        if (!name.equals(version.getStoreName())) {
            throw new IllegalArgumentException("Version dose not belong to this store.");
        }
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

    /**
     * Increase a new version to this store.
     */
    public void increseVersion() {
        int versionNumber = 0;
        if (versions.size() > 0) {
            versionNumber = versions.get(versions.size() - 1).getNumber() + 1;
        } else {
            //No version in this store. Use verions 1 as the first version.
            versionNumber = 1;
        }
        Version version = new Version(name, versionNumber, System.currentTimeMillis());
        addVersion(version);
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

        if (createdTime != store.createdTime) {
            return false;
        }
        if (currentVersion != store.currentVersion) {
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
        result = 31 * result + (int) (createdTime ^ (createdTime >>> 32));
        result = 31 * result + currentVersion;
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
        Store clonedStore = new Store(name, owner, createdTime);
        clonedStore.setCurrentVersion(currentVersion);

        for (Version v : this.versions) {
            clonedStore.addVersion(v.cloneVersion());
        }
        return clonedStore;
    }
}
