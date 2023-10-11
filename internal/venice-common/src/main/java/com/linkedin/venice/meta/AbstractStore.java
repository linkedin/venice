package com.linkedin.venice.meta;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.StoreDisabledException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.systemstore.schemas.StoreVersion;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;


/**
 * This is an abstraction of metadata maintained per Store.
 */
public abstract class AbstractStore implements Store {
  public static final int DEFAULT_REPLICATION_FACTOR = 3;
  /**
   * Default storage quota 20GB
   */
  public static final long DEFAULT_STORAGE_QUOTA = (long) 20 * (1 << 30);

  /**
   * Default read quota 1800 QPS per node
   */
  public static final long DEFAULT_READ_QUOTA = 1800;

  protected interface StoreVersionSupplier {
    /**
     * This function will return a reference to the internal versions structure, and any change applying to the returned
     * object will be reflected in the referenced {@link Store}.
     * @return
     */
    List<StoreVersion> getForUpdate();

    /**
     * This function will return a list of Versions, which are read-only, and any modification against them will
     * throw {@link UnsupportedOperationException}.
     * @return
     */
    List<Version> getForRead();
  }

  /**
   * This field is to let current class talk to the inherited classes for version related operations.
   */
  private StoreVersionSupplier storeVersionsSupplier;

  /**
   * This function should be invoked only once.
   */
  protected synchronized void setupVersionSupplier(StoreVersionSupplier versionsSupplier) {
    if (this.storeVersionsSupplier != null) {
      throw new VeniceException("Field: 'storeVersionsSupplier' shouldn't be setup more than once");
    }
    this.storeVersionsSupplier = versionsSupplier;
  }

  private void checkVersionSupplier() {
    if (this.storeVersionsSupplier == null) {
      throw new VeniceException("Field: 'storeVersionsSupplier' hasn't been setup yet");
    }
  }

  @Override
  public List<Version> getVersions() {
    checkVersionSupplier();
    return storeVersionsSupplier.getForRead();
  }

  @Override
  public void setVersions(List<Version> versions) {
    checkVersionSupplier();
    storeVersionsSupplier.getForUpdate().clear();
    versions.forEach(v -> storeVersionsSupplier.getForUpdate().add(v.dataModel()));
  }

  @Override
  public Optional<CompressionStrategy> getVersionCompressionStrategy(int versionNumber) {
    if (versionNumber == NON_EXISTING_VERSION) {
      return Optional.empty();
    }

    return getVersion(versionNumber).map(version -> version.getCompressionStrategy());
  }

  @Override
  public void setBufferReplayForHybridForVersion(int versionNum, boolean enabled) {
    for (int i = storeVersionsSupplier.getForUpdate().size() - 1; i >= 0; i--) {
      Version version = new VersionImpl(storeVersionsSupplier.getForUpdate().get(i));
      if (version.getNumber() == versionNum) {
        version.setBufferReplayEnabledForHybrid(enabled);
        return;
      }
    }
    throw new VeniceException("Unknown version: " + versionNum + " in store: " + getName());
  }

  @Override
  public void addVersion(Version version) {
    addVersion(version, true, false);
  }

  @Override
  public void addVersion(Version version, boolean isClonedVersion) {
    addVersion(version, true, isClonedVersion);
  }

  @Override
  public void forceAddVersion(Version version, boolean isClonedVersion) {
    addVersion(version, false, isClonedVersion);
  }

  @Override
  public void checkDisableStoreWrite(String action, int version) {
    if (!isEnableWrites()) {
      throw new StoreDisabledException(getName(), action, version);
    }
  }

  /**
   * Add a version into store
   * @param version
   * @param checkDisableWrite if checkDisableWrite is true, and the store is disabled to write, then this will throw a StoreDisabledException.
   *                    Setting to false will ignore the enableWrites status of the store (for example for cloning a store).
   * @param isClonedVersion if true, the version being added is cloned from an existing version instance, so don't apply
   *                        any store level config on it; if false, the version being added is new version, so the new version
   *                        config should be the same as store config.
   */
  private void addVersion(Version version, boolean checkDisableWrite, boolean isClonedVersion) {
    checkVersionSupplier();
    if (checkDisableWrite) {
      checkDisableStoreWrite("add", version.getNumber());
    }
    if (!getName().equals(version.getStoreName())) {
      throw new VeniceException("Version does not belong to this store.");
    }
    int index = 0;
    for (; index < storeVersionsSupplier.getForUpdate().size(); index++) {
      if (storeVersionsSupplier.getForUpdate().get(index).number == version.getNumber()) {
        throw new VeniceException("Version is repeated. Store: " + getName() + " Version: " + version.getNumber());
      }
      if (storeVersionsSupplier.getForUpdate().get(index).number > version.getNumber()) {
        break;
      }
    }

    if (!isClonedVersion) {
      /**
       * Important:
       * We need to clone the object from the store config here since the version-level config could be
       * changed after. Without a new copy, the following version-level change will reflect in the store-level
       * config as well since they are referring to the same object.
       */
      // For new version, apply store level config on it.
      // update version compression type
      version.setCompressionStrategy(getCompressionStrategy());

      version.setChunkingEnabled(isChunkingEnabled());
      version.setRmdChunkingEnabled(isRmdChunkingEnabled());

      PartitionerConfig partitionerConfig = getPartitionerConfig();
      if (partitionerConfig != null) {
        version.setPartitionerConfig(partitionerConfig.clone());
      }

      version.setNativeReplicationEnabled(isNativeReplicationEnabled());

      version.setReplicationFactor(getReplicationFactor());

      version.setNativeReplicationSourceFabric(getNativeReplicationSourceFabric());

      version.setIncrementalPushEnabled(isIncrementalPushEnabled());

      version.setUseVersionLevelIncrementalPushEnabled(true);

      HybridStoreConfig hybridStoreConfig = getHybridStoreConfig();
      if (hybridStoreConfig != null) {
        version.setHybridStoreConfig(hybridStoreConfig.clone());
      }

      version.setUseVersionLevelHybridConfig(true);

      version.setActiveActiveReplicationEnabled(isActiveActiveReplicationEnabled());
      version.setViewConfigs(getViewConfigs());
    }

    storeVersionsSupplier.getForUpdate().add(index, version.dataModel());
    if (version.getNumber() > getLargestUsedVersionNumber()) {
      setLargestUsedVersionNumber(version.getNumber());
    }
  }

  @Override
  public Version deleteVersion(int versionNumber) {
    checkVersionSupplier();
    for (int i = 0; i < storeVersionsSupplier.getForUpdate().size(); i++) {
      Version version = new VersionImpl(storeVersionsSupplier.getForUpdate().get(i));
      if (version.getNumber() == versionNumber) {
        storeVersionsSupplier.getForUpdate().remove(i);
        return version;
      }
    }
    return null;
  }

  @Override
  public boolean containsVersion(int versionNumber) {
    checkVersionSupplier();
    for (Version version: storeVersionsSupplier.getForRead()) {
      if (version.getNumber() == versionNumber) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void updateVersionStatus(int versionNumber, VersionStatus status) {
    checkVersionSupplier();
    if (status.equals(VersionStatus.ONLINE)) {
      checkDisableStoreWrite("become ONLINE", versionNumber);
    }
    for (int i = storeVersionsSupplier.getForUpdate().size() - 1; i >= 0; i--) {
      Version version = new VersionImpl(storeVersionsSupplier.getForUpdate().get(i));
      if (version.getNumber() == versionNumber) {
        version.setStatus(status);
        return;
      }
    }
    throw new VeniceException("Version:" + versionNumber + " does not exist");
  }

  @Override
  public Version peekNextVersion() {
    return increaseVersion(Version.guidBasedDummyPushId(), false);
  }

  @Override
  public Optional<Version> getVersion(int versionNumber) {
    checkVersionSupplier();
    for (Version version: storeVersionsSupplier.getForRead()) {
      if (version.getNumber() == versionNumber) {
        return Optional.of(version);
      }
    }

    return Optional.empty();
  }

  @Override
  public VersionStatus getVersionStatus(int versionNumber) {
    Optional<Version> version = getVersion(versionNumber);
    if (!version.isPresent()) {
      return VersionStatus.NOT_CREATED;
    }

    return version.get().getStatus();
  }

  private Version increaseVersion(String pushJobId, boolean createNewVersion) {
    int versionNumber = getLargestUsedVersionNumber() + 1;
    checkDisableStoreWrite("increase", versionNumber);
    Version version = new VersionImpl(getName(), versionNumber, pushJobId);
    if (createNewVersion) {
      addVersion(version);
      return version.cloneVersion();
    } else {
      return version;
    }
  }

  @Override
  public List<Version> retrieveVersionsToDelete(int clusterNumVersionsToPreserve) {
    checkVersionSupplier();
    int curNumVersionsToPreserve = clusterNumVersionsToPreserve;
    if (getNumVersionsToPreserve() != NUM_VERSION_PRESERVE_NOT_SET) {
      curNumVersionsToPreserve = getNumVersionsToPreserve();
    }
    // when numVersionsToPreserve is less than 1, it usually means a config issue.
    // Setting it to zero, will cause the store to be deleted as soon as push completes.
    if (curNumVersionsToPreserve < 1) {
      throw new IllegalArgumentException(
          "At least 1 version should be preserved. Parameter " + curNumVersionsToPreserve);
    }

    List<Version> versions = storeVersionsSupplier.getForRead();
    int versionCnt = versions.size();
    if (versionCnt == 0) {
      return Collections.emptyList();
    }

    // The code assumes that Versions are sorted in increasing order by addVersion and increaseVersion
    int lastElementIndex = versionCnt - 1;
    List<Version> versionsToDelete = new ArrayList<>();

    /**
     * The current version need not be the last largest version (e.g. we rolled back to an earlier version).
     * The versions which can be deleted are:
     *     a) ONLINE versions except the current version given we preserve numVersionsToPreserve versions.
     *     b) ERROR version (ideally should not be there as AbstractPushmonitor#handleErrorPush deletes those)
     *     c) STARTED versions if its not the last one and the store is not migrating.
     *     d) KILLED versions by {@link org.apache.kafka.clients.admin.Admin#killOfflinePush} api.
     */
    for (int i = lastElementIndex; i >= 0; i--) {
      Version version = versions.get(i);

      if (version.getNumber() == getCurrentVersion()) { // currentVersion is always preserved
        curNumVersionsToPreserve--;
      } else if (VersionStatus.canDelete(version.getStatus())) { // ERROR and KILLED versions are always deleted
        versionsToDelete.add(version);
      } else if (VersionStatus.ONLINE.equals(version.getStatus())) {
        if (curNumVersionsToPreserve > 0) { // keep the minimum number of version to preserve
          curNumVersionsToPreserve--;
        } else {
          versionsToDelete.add(version);
        }
      } else if (VersionStatus.STARTED.equals(version.getStatus()) && (i != lastElementIndex) && !isMigrating()) {
        // For the non-last started version, if it's not the current version(STARTED version should not be the current
        // version, just prevent some edge cases here.), we should delete it only if the store is not migrating
        // as during store migration are there are concurrent pushes with STARTED version.
        // So if the store is not migrating, it's stuck in STARTED, it means somehow the controller did not update the
        // version status properly.
        versionsToDelete.add(version);
      }
      // TODO here we don't deal with the PUSHED version, just keep all of them, need to consider collect them too in
      // the future.
    }
    return versionsToDelete;
  }

  @Override
  public boolean isSystemStore() {
    return Store.isSystemStore(getName());
  }

  @Override
  public void fixMissingFields() {
    checkVersionSupplier();
    for (StoreVersion storeVersion: storeVersionsSupplier.getForUpdate()) {
      Version version = new VersionImpl(storeVersion);
      if (version.getPartitionerConfig() == null) {
        version.setPartitionerConfig(getPartitionerConfig());
      }
      if (version.getPartitionCount() == 0) {
        version.setPartitionCount(getPartitionCount());
      }
    }
  }
}
