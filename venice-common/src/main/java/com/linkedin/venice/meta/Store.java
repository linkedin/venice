package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.validation.constraints.NotNull;


/**
 * Class defines the store of Venice.
 * <p>
 * This class is NOT thread safe. Concurrency request to Store instance should be controlled in repository level.
 * When adding fields to this method, make sure to update equals, hashcode, clone,
 * and make sure json serialization still works
 */
public class Store {
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
   * Version number that has been claimed by an upstream (H2V) system which will create the corresponding kafka topic.
   */
  private int reservedVersion = 0;
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
  private final OfflinePushStrategy offLinePushStrategy;
  /**
   * List of non-retired versions.
   */
  private List<Version> versions;

  public Store(@NotNull String name, @NotNull String owner, long createdTime, @NotNull PersistenceType persistenceType,
      @NotNull RoutingStrategy routingStrategy, @NotNull ReadStrategy readStrategy,
      @NotNull OfflinePushStrategy offlinePushStrategy) {
    this.name = name;
    this.owner = owner;
    this.createdTime = createdTime;
    this.persistenceType = persistenceType;
    this.routingStrategy = routingStrategy;
    this.readStrategy = readStrategy;
    this.offLinePushStrategy = offlinePushStrategy;
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

  public int getReservedVersion() {
    return this.reservedVersion;
  }

  public void setReservedVersion(int reservedVersion){
    this.reservedVersion = reservedVersion;
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

  public OfflinePushStrategy getOffLinePushStrategy() {
    return offLinePushStrategy;
  }

  public List<Version> getVersions() {
    return Collections.unmodifiableList(this.versions);
  }

  public void setVersions(List<Version> versions) {
    this.versions = versions;
  }



  public void reserveVersionNumber(int version){
    if (version < peekNextVersion().getNumber()){
      throw new VeniceException("Cannot reserve a version number smaller than next available version number");
    }
    reservedVersion = version;
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
        throw new IllegalArgumentException("Version is repeated.Store:" + name + " Number:" + version.getNumber());
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

  public boolean containsVersion(int versionNumber) {
    for (int i = 0; i < versions.size(); i++) {
      if (versions.get(i).getNumber() == versionNumber) {
        return true;
      }
    }
    return false;
  }

  public void updateVersionStatus(int versionNumber, VersionStatus status) {
    for (int i = versions.size() - 1; i >= 0; i--) {
      if (versions.get(i).getNumber() == versionNumber) {
        versions.get(i).setStatus(status);
        return;
      }
    }
    throw new VeniceException("Version:" + versionNumber + "does not exist");
  }

  /**
   * Increase a new version to this store.
   */
  public Version increaseVersion() {
    return increaseVersion(true);
  }

  public Version peekNextVersion(){
    return increaseVersion(false);
  }

  private Version increaseVersion(boolean createNewVersion) {
    int versionNumber = 1;
    if (versions.size() > 0) {
      versionNumber = versions.get(versions.size() - 1).getNumber() + 1;
    }
    if (reservedVersion >= versionNumber){
      versionNumber = reservedVersion + 1; /* must skip past any reserved versions */
    }
    Version version = new Version(name, versionNumber);
    if (createNewVersion) {
      addVersion(version);
      return version.cloneVersion();
    } else {
      return version;
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

    if (createdTime != store.createdTime) {
      return false;
    }
    if (currentVersion != store.currentVersion) {
      return false;
    }
    if (reservedVersion != store.reservedVersion) {
      return  false;
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
    result = 31 * result + reservedVersion;
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
    Store clonedStore =
        new Store(name, owner, createdTime, persistenceType, routingStrategy, readStrategy, offLinePushStrategy);
    clonedStore.setCurrentVersion(currentVersion);
    clonedStore.setReservedVersion(reservedVersion);

    for (Version v : this.versions) {
      clonedStore.addVersion(v.cloneVersion());
    }
    return clonedStore;
  }
}
