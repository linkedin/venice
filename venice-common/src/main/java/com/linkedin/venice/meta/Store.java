package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.StoreDisabledException;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.validation.constraints.NotNull;
import org.codehaus.jackson.annotate.JsonProperty;


/**
 * Class defines the store of Venice.
 * <p>
 * This class is NOT thread safe. Concurrency request to Store instance should be controlled in repository level.
 * When adding fields to this method, make sure to update equals, hashcode, clone,
 * and make sure json serialization still works
 */
public class Store {
  /**
   * Special version number indicates none of version is available to read.
   */
  public static final int NON_EXISTING_VERSION = 0;

  public static final int IGNORE_VERSION = -1;
  /**
   * Default storage quota 20GB
   */
  public static long DEFAULT_STORAGE_QUOTA = (long)20 * (1 << 30);

  public static final long UNLIMITED_STORAGE_QUOTA = -1;
  /**
   * Default read quota 1800 QPS per node
   */
  public static long DEFAULT_READ_QUOTA = 1800;
  /**
   * Store name.
   */
  private final String name;
  /**
   * Owner of this store.
   */
  private String owner;
  /**
   * time when this store was created.
   */
  private final long createdTime;
  /**
   * The number of version which is used currently.
   */
  private int currentVersion = NON_EXISTING_VERSION;
  /**
   * Default partition count for all of versions in this store. Once first version become online, the number will be
   * assigned.
   */
  private int partitionCount = 0;
  /**
   * If a store is disabled from writing, new version can not be created for it.
   */
  private boolean enableWrites = true;
  /**
   * If a store is disabled from being read, none of versions under this store could serve read requests.
   */
  private boolean enableReads = true;
  /**
   * Maximum capacity a store version is able to have
   */
  private long storageQuotaInByte;
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

  /**
   * Quota for read request hit this store. Measurement is capacity unit.
   */
  private long readQuotaInCU = 0;

  /**
   * Properties related to Hybrid Store behavior. If absent (null), then the store is not hybrid.
   */
  private HybridStoreConfig hybridStoreConfig;

  /**
   * Store-level ACL switch. When disabled, Venice Router should accept every request.
   */
  private boolean accessControlled = false;

  public Store(@NotNull String name, @NotNull String owner, long createdTime, @NotNull PersistenceType persistenceType,
      @NotNull RoutingStrategy routingStrategy, @NotNull ReadStrategy readStrategy,
      @NotNull OfflinePushStrategy offlinePushStrategy) {
    this(name, owner, createdTime, persistenceType, routingStrategy, readStrategy, offlinePushStrategy,
        NON_EXISTING_VERSION, DEFAULT_STORAGE_QUOTA, DEFAULT_READ_QUOTA, null);
  }

  public Store(@NotNull String name, @NotNull String owner, long createdTime, @NotNull PersistenceType persistenceType,
      @NotNull RoutingStrategy routingStrategy, @NotNull ReadStrategy readStrategy,
      @NotNull OfflinePushStrategy offlinePushStrategy, int currentVersion,
      long storageQuotaInByte, long readQuotaInCU, HybridStoreConfig hybridStoreConfig) {
    if (!isValidStoreName(name)) {
      throw new VeniceException("Invalid store name: " + name);
    }
    this.name = name;
    this.owner = owner;
    this.createdTime = createdTime;
    this.persistenceType = persistenceType;
    this.routingStrategy = routingStrategy;
    this.readStrategy = readStrategy;
    this.offLinePushStrategy = offlinePushStrategy;
    this.versions = new ArrayList<>();
    this.storageQuotaInByte = storageQuotaInByte;
    this.currentVersion = currentVersion;
    this.readQuotaInCU = readQuotaInCU;
    this.hybridStoreConfig = hybridStoreConfig;
  }

  /**
   * Store name rules:
   *  1.  Only letters, numbers, underscore or dash
   *  2. No double dashes
   */

  private static Pattern storeNamePattern = Pattern.compile("^[a-zA-Z0-9_-]+$");
  public static boolean isValidStoreName(String name){
    Matcher matcher = storeNamePattern.matcher(name);
    return matcher.matches() && !name.contains("--");
  }

  public String getName() {
    return name;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  @SuppressWarnings("unused") // Used by Serializer/De-serializer for storing to Zoo Keeper
  public long getCreatedTime() {
    return createdTime;
  }

  public int getCurrentVersion() {
    return currentVersion;
  }

  /**
   * Set current serving version number of this store. If store is disabled to write, thrown {@link
   * StoreDisabledException}.
   */
  public void setCurrentVersion(int currentVersion) {
    checkDisableStoreWrite("setStoreCurrentVersion", currentVersion);
    setCurrentVersionWithoutCheck(currentVersion);
  }

  @SuppressWarnings("unused") // Used by Serializer/De-serializer for storing to Zoo Keeper
  @JsonProperty("currentVersion")
  public void setCurrentVersionWithoutCheck(int currentVersion){
    this.currentVersion =  currentVersion;
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

  @SuppressWarnings("unused") // Used by Serializer/De-serializer for storing to Zoo Keeper
  public long getStorageQuotaInByte() {
    //This is a safeguard in case that some old stores do not have storage quota field
    return (storageQuotaInByte <= 0 && storageQuotaInByte != UNLIMITED_STORAGE_QUOTA)
        ? DEFAULT_STORAGE_QUOTA : storageQuotaInByte;
  }

  @SuppressWarnings("unused") // Used by Serializer/De-serializer for storing to Zoo Keeper
  public void setStorageQuotaInByte(long storageQuotaInByte) {
    this.storageQuotaInByte = storageQuotaInByte;
  }

  public int getPartitionCount() {
    return partitionCount;
  }

  public boolean isEnableWrites() {
    return enableWrites;
  }

  public void setEnableWrites(boolean enableWrites) {
    this.enableWrites = enableWrites;
    if (this.enableWrites) {
      setPushedVersionsOnline();
    }
  }

  public boolean isEnableReads() {
    return enableReads;
  }

  public void setEnableReads(boolean enableReads) {
    this.enableReads = enableReads;
  }

  public long getReadQuotaInCU() {
    // In case the store haven't been assigned a quota, use this value as the default quota instead of using 0.
    // If the store was created before we releasing quota feature, JSON framework wil give 0 as the default value
    // while deserializing the store from ZK.
    return readQuotaInCU <= 0 ? DEFAULT_READ_QUOTA : readQuotaInCU;
  }

  public void setReadQuotaInCU(long readQuotaInCU) {
    this.readQuotaInCU = readQuotaInCU;
  }

  public HybridStoreConfig getHybridStoreConfig() {
    return hybridStoreConfig;
  }

  public void setHybridStoreConfig(HybridStoreConfig hybridStoreConfig) {
    this.hybridStoreConfig = hybridStoreConfig;
  }

  public boolean isHybrid() {
    return null != hybridStoreConfig;
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
   * @param checkDisableWrite if checkDisableWrite is true, and the store is disabled to write, then this will throw a StoreDisabledException.
   *                    Setting to false will ignore the enableWrites status of the store (for example for cloning a store).
   */
  private void addVersion(Version version, boolean checkDisableWrite) {
    if (checkDisableWrite) {
      checkDisableStoreWrite("add", version.getNumber());
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
   * Delete a version from a store.
   *
   * @param versionNumber
   */
  public Version deleteVersion(int versionNumber) {
    for (int i = 0; i < versions.size(); i++) {
      if (versions.get(i).getNumber() == versionNumber) {
        return versions.remove(i);
      }
    }
    return null;
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
      checkDisableStoreWrite("become ONLINE", versionNumber);
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
   * Use the form of this method that accepts a pushJobId
   */
  @Deprecated
  public Version increaseVersion() {
    return increaseVersion(Version.guidBasedDummyPushId(), true);
  }

  public Version increaseVersion(String pushJobId) {
    return increaseVersion(pushJobId, true);
  }

  public Version peekNextVersion() {
    return increaseVersion(Version.guidBasedDummyPushId(), false);
  }

  public VersionStatus getVersionStatus(int versionNumber) {
    for (int i = 0; i < versions.size(); i++) {
      if (versions.get(i).getNumber() == versionNumber) {
        return versions.get(i).getStatus();
      }
    }
    return VersionStatus.NOT_CREATED;
  }

  private Version increaseVersion(String pushJobId, boolean createNewVersion) {
    int versionNumber = largestUsedVersionNumber + 1;
    checkDisableStoreWrite("increase", versionNumber);
    Version version = new Version(name, versionNumber, pushJobId);
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
   * Set all of PUSHED version to ONLINE once store is enabled to write.
   */
  private void setPushedVersionsOnline() {
    // TODO, if the PUSHED version is the latest vesion, after store is enabled to write, shall we put this version as the current version?
    versions.stream().filter(version -> version.getStatus().equals(VersionStatus.PUSHED)).forEach(version -> {
      updateVersionStatus(version.getNumber(), VersionStatus.ONLINE);
    });
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + owner.hashCode();
    result = 31 * result + (int) (createdTime ^ (createdTime >>> 32));
    result = 31 * result + currentVersion;
    result = 31 * result + partitionCount;
    result = 31 * result + (enableWrites ? 1 : 0);
    result = 31 * result + (enableReads ? 1 : 0);
    result = 31 * result + (int) (storageQuotaInByte ^ (storageQuotaInByte >>> 32));
    result = 31 * result + persistenceType.hashCode();
    result = 31 * result + routingStrategy.hashCode();
    result = 31 * result + readStrategy.hashCode();
    result = 31 * result + offLinePushStrategy.hashCode();
    result = 31 * result + versions.hashCode();
    result = 31 * result + largestUsedVersionNumber;
    result = 31 * result + (int) (readQuotaInCU ^ (readQuotaInCU >>> 32));
    result = 31 * result + (hybridStoreConfig != null ? hybridStoreConfig.hashCode() : 0);
    result = 31 * result + (accessControlled ? 1 : 0);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Store store = (Store) o;

    if (createdTime != store.createdTime) return false;
    if (currentVersion != store.currentVersion) return false;
    if (partitionCount != store.partitionCount) return false;
    if (enableWrites != store.enableWrites) return false;
    if (enableReads != store.enableReads) return false;
    if (storageQuotaInByte != store.storageQuotaInByte) return false;
    if (largestUsedVersionNumber != store.largestUsedVersionNumber) return false;
    if (readQuotaInCU != store.readQuotaInCU) return false;
    if (!name.equals(store.name)) return false;
    if (!owner.equals(store.owner)) return false;
    if (persistenceType != store.persistenceType) return false;
    if (routingStrategy != store.routingStrategy) return false;
    if (readStrategy != store.readStrategy) return false;
    if (offLinePushStrategy != store.offLinePushStrategy) return false;
    if (!versions.equals(store.versions)) return false;
    if (accessControlled != store.accessControlled) return false;
    return !(hybridStoreConfig != null ? !hybridStoreConfig.equals(store.hybridStoreConfig) : store.hybridStoreConfig != null);
  }

  public void setPartitionCount(int partitionCount) {
    this.partitionCount = partitionCount;
  }

  /**
   * Cloned a new store based on current data in this store.
   *
   * @return cloned store.
   */
  public Store cloneStore() {
    Store clonedStore =
        new Store(name,
                  owner,
                  createdTime,
                  persistenceType,
                  routingStrategy,
                  readStrategy,
                  offLinePushStrategy,
                  currentVersion,
                  storageQuotaInByte,
                  readQuotaInCU,
                  null == hybridStoreConfig ? null : hybridStoreConfig.clone());
    clonedStore.setEnableReads(enableReads);
    clonedStore.setEnableWrites(enableWrites);
    clonedStore.setPartitionCount(partitionCount);
    clonedStore.setLargestUsedVersionNumber(largestUsedVersionNumber);
    clonedStore.setAccessControlled(accessControlled);

    for (Version v : this.versions) {
      clonedStore.forceAddVersion(v.cloneVersion());
    }
    return clonedStore;
  }

  private void checkDisableStoreWrite(String action, int version) {
    if (!enableWrites) {
      throw new StoreDisabledException(name, action, version);
    }
  }

  public static void setDefaultStorageQuota(long storageQuota) {
    Store.DEFAULT_STORAGE_QUOTA = storageQuota;
  }

  public static void setDefaultReadQuota(long readQuota) {
    Store.DEFAULT_READ_QUOTA = readQuota;
  }

  public boolean isAccessControlled() {
    return accessControlled;
  }

  public void setAccessControlled(boolean accessControlled) {
    this.accessControlled = accessControlled;
  }
}
