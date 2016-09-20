package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.StorePausedException;
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
   * Default partition count for all of versions in this store. Once first version become online, the number will be
   * assigned.
   */
  private int partitionCount = 0;
  /**
   * If a store is paused, new version can not be created for it.
   */
  private boolean paused = false;
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

  /**
   * The largest version number ever used before for this store.
   */
  private int largestUsedVersionNumber = 0;

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

  @SuppressWarnings("unused") // Used by Serializer/De-serializer for storing to Zoo Keeper
  public long getCreatedTime() {
    return createdTime;
  }

  public int getCurrentVersion() {
    return currentVersion;
  }

  public void setCurrentVersion(int currentVersion) {
    checkPausedStore("setCurrentVersion", currentVersion);
    this.currentVersion = currentVersion;
  }

  @SuppressWarnings("unused") // Used by Serializer/De-serializer for storing to Zoo Keeper
  public PersistenceType getPersistenceType() {
    return persistenceType;
  }

  @SuppressWarnings("unused") // Used by Serializer/De-serializer for storing to Zoo Keeper
  public RoutingStrategy getRoutingStrategy() {
    return routingStrategy;
  }

  @SuppressWarnings("unused") // Used by Serializer/De-serializer for storing to Zoo Keeper
  public ReadStrategy getReadStrategy() {
    return readStrategy;
  }

  @SuppressWarnings("unused") // Used by Serializer/De-serializer for storing to Zoo Keeper
  public OfflinePushStrategy getOffLinePushStrategy() {
    return offLinePushStrategy;
  }

  public List<Version> getVersions() {
    return Collections.unmodifiableList(this.versions);
  }

  @SuppressWarnings("unused") // Used by Serializer/De-serializer for storing to Zoo Keeper
  public void setVersions(List<Version> versions) {
    this.versions = versions;
    //Backward capability for the old store in ZK.
    if (largestUsedVersionNumber == 0 && !versions.isEmpty()) {
      largestUsedVersionNumber = versions.get(versions.size() - 1).getNumber();
    }
  }

  @SuppressWarnings("unused") // Used by Serializer/De-serializer for storing to Zoo Keeper
  public int getLargestUsedVersionNumber() {
    return largestUsedVersionNumber;
  }

  @SuppressWarnings("unused") // Used by Serializer/De-serializer for storing to Zoo Keeper
  public void setLargestUsedVersionNumber(int largestUsedVersionNumber) {
    this.largestUsedVersionNumber = largestUsedVersionNumber;
  }

  public int getPartitionCount() {
    return partitionCount;
  }

  public boolean isPaused() {
    return paused;
  }

  public void setPaused(boolean paused) {
    this.paused = paused;
    if (!this.paused) {
      setPushedVersionsOnline();
    }
  }

  /**
   * Add a version into store.
   *
   * @param version
   */
  public void addVersion(Version version){
    addVersion(version, true);
  }
  private void forceAddVersion(Version version){
    addVersion(version, false);
  }

  /**
   * Add a version into store
   * @param version
   * @param checkPaused if checkPaused is true, and the store is paused, then this will throw a StorePausedException.
   *                    Setting to false will ignore the paused status of the store (for example for cloning a store).
   */
  private void addVersion(Version version, boolean checkPaused) {
    if (checkPaused) {
      checkPausedStore("add", version.getNumber());
    }
    if (!name.equals(version.getStoreName())) {
      throw new VeniceException("Version does not belong to this store.");
    }
    int index = 0;
    for (; index < versions.size(); index++) {
      if (versions.get(index).getNumber() == version.getNumber()) {
        throw new VeniceException("Version is repeated. Store: " + name + " Version: " + version.getNumber());
      }
      if (versions.get(index).getNumber() > version.getNumber()) {
        break;
      }
    }
    versions.add(index, version);
    if (version.getNumber() > largestUsedVersionNumber) {
      largestUsedVersionNumber = version.getNumber();
    }
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
    if (status.equals(VersionStatus.ONLINE)) {
      checkPausedStore("become ONLINE", versionNumber);
    }
    for (int i = versions.size() - 1; i >= 0; i--) {
      if (versions.get(i).getNumber() == versionNumber) {
        versions.get(i).setStatus(status);
        return;
      }
    }
    throw new VeniceException("Version:" + versionNumber + " does not exist");
  }

  /**
   * Increase a new version to this store.
   */
  public Version increaseVersion() {
    return increaseVersion(true);
  }

  public Version peekNextVersion() {
    return increaseVersion(false);
  }

  private Version increaseVersion(boolean createNewVersion) {
    int versionNumber = largestUsedVersionNumber + 1;
    checkPausedStore("increase", versionNumber);
    Version version = new Version(name, versionNumber);
    if (createNewVersion) {
      addVersion(version);
      return version.cloneVersion();
    } else {
      return version;
    }
  }

  public List<Version> retrieveVersionsToDelete(int numVersionsToPreserve) {
    // when numVersionsToPreserve is less than 1, it usually means a config issue.
    // Setting it to zero, will cause the store to be deleted as soon as push completes.
    if(numVersionsToPreserve < 1) {
      throw new IllegalArgumentException("At least 1 version should be preserved. Parameter " + numVersionsToPreserve);
    }

    if(versions.size() == 0) {
      return new ArrayList<>();
    }

    List<Version> versionsToDelete = new ArrayList<>();

    // Ignore the last version from considering it for delete.
    // If a version gets deleted the same version number will be generated for the next version
    // in the current code. Due to timing and synchronization issues as well as reasoning
    // this case is difficult to handle.
    // TODO : but this still wastes the space on storage node by preserving the highest version
    // regardless of its state. It can be solved better by always incrementing the versions
    // and never re-using it.

    // The code assumes that Versions are sorted in increasing order by addVersion and increaseVersion
    int lastElementIndex =  versions.size() - 1;
    Version latestVersion = versions.get(lastElementIndex);
    if(VersionStatus.preserveLastFew(latestVersion.getStatus())) {
      // Last version is always preserved and it can be archived, reduce the number of versions to preserveLastFew by 1.
      numVersionsToPreserve --;
    }

    for(int i = lastElementIndex - 1;i >= 0 ; i --){
      Version version = versions.get(i);
      // Error Versions are deleted immediately
      if(VersionStatus.canDelete(version.getStatus())) {
        versionsToDelete.add(version);
      } else if (VersionStatus.preserveLastFew(version.getStatus())) {
        if(numVersionsToPreserve > 0) {
          numVersionsToPreserve --;
        } else {
          versionsToDelete.add(version);
        }
      }
      // TODO here we don't deal with the STARTED and PUSHED version, just keep all of them, need to consider collect them too in the future.
    }

    return versionsToDelete;
  }

  /**
   * Set all of PUSHED version to ONLINE once store is resumed.
   */
  private void setPushedVersionsOnline() {
    // TODO, if the PUSHED version is the latest vesion, after store is resumed, shall we put this version as the current version?
    versions.stream().filter(version -> version.getStatus().equals(VersionStatus.PUSHED)).forEach(version -> {
      updateVersionStatus(version.getNumber(), VersionStatus.ONLINE);
    });
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
    if (partitionCount != store.partitionCount) {
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
    if (paused != store.paused) {
      return false;
    }
    if (largestUsedVersionNumber != store.largestUsedVersionNumber){
      return false;
    }
    return versions.equals(store.versions);
  }

  public void setPartitionCount(int partitionCount) {
    this.partitionCount = partitionCount;
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + owner.hashCode();
    result = 31 * result + (int) (createdTime ^ (createdTime >>> 32));
    result = 31 * result + currentVersion;
    result = 31 * result + partitionCount;
    result = 31 * result + (paused ? 1 : 0);
    result = 31 * result + largestUsedVersionNumber;

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
    clonedStore.setPartitionCount(partitionCount);
    clonedStore.setPaused(paused);
    clonedStore.setLargestUsedVersionNumber(largestUsedVersionNumber);

    for (Version v : this.versions) {
      clonedStore.forceAddVersion(v.cloneVersion());
    }
    return clonedStore;
  }

  private void checkPausedStore(String action, int version) {
    if (paused) {
      throw new StorePausedException(name, action, version);
    }
  }
}
