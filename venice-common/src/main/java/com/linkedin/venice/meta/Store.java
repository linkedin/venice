package com.linkedin.venice.meta;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.StoreDisabledException;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
 *
 * When you want to add a simple field to Store metadata, you just need to create getter/setter for the new field.
 * When you try to add a method starting with 'get', the default json serialization will do serialization by this
 * method, which could produce some unexpected serialization result, so if it is not for serialization purpose, please
 * specify {@link org.codehaus.jackson.annotate.JsonIgnore} to ignore the method, whose name is starting with 'get'.
 *
 * TODO: we need to refactor this class to separate Store operations from Store POJO, which is being used by JSON
 * TODO: Since metadata keeps increasing, maybe we would like to refactor it to builder pattern.
 * TODO: It's handy to make {@link #versions} as a hash map.
 */
public class Store {

  private static final String SYSTEM_STORE_NAME_PADDING = "venice_system_store_";
  public static final String SYSTEM_STORE_FORMAT = SYSTEM_STORE_NAME_PADDING + "%s";

  /**
   * Special version number indicates none of version is available to read.
   */
  public static final int NON_EXISTING_VERSION = 0;

  public static final int IGNORE_VERSION = -1;

  public static final int BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS = 24;
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
   * Default value of numVersionPreserve, by default we should use cluster level config instead of store level config.
   */
  public static int NUM_VERSION_PRESERVE_NOT_SET = 0;
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
   * It's currently sorted and there is code run under the assumption that the last element in the list is the largest.
   * check out {VeniceHelixAdmin#getIncrementalPushVersion}
   * Please make it in mind if you want to change this logic
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
  private boolean accessControlled = true;

  /**
   * strategies used to compress/decompress Record's value
   */
  private CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;

  /**
   * Enable/Disable client-side record decompression (default: true)
   */
  private boolean clientDecompressionEnabled = true;

  /**
   * Whether current store supports large value (typically more than 1MB).
   * By default, the chunking feature is disabled.
   */
  private boolean chunkingEnabled = false;

  /**
   * Whether cache in Router is enabled for current store.
   */
  private boolean singleGetRouterCacheEnabled = false;

  /**
   * Whether cache in Router is enabled for batch get in the current store.
   */
  private boolean batchGetRouterCacheEnabled = false;

  /**
   * Batch get key number limit, and Venice will use cluster-level config if it is not positive.
   */
  private int batchGetLimit = -1;

  /**
   * How many versions this store preserve at most. By default it's 0 means we use the cluster level config to
   * determine how many version is preserved.
   */
  private int numVersionsToPreserve = NUM_VERSION_PRESERVE_NOT_SET;

  /**
   * a flag to see if the store supports incremental push or not
   */
  private boolean incrementalPushEnabled = false;

  /**
   * Whether or not the store is in the process of migration.
   */
  private boolean migrating = false;

  /**
   * Whether or not write-path computation feature is enabled for this store
   */
  private boolean writeComputationEnabled = false;

  /**
   * Whether read-path computation is enabled for this store.
   */
  private boolean readComputationEnabled = false;

  /**
   * Maximum number of hours allowed for the store to transition from bootstrap to online state.
   */
  private int bootstrapToOnlineTimeoutInHours = BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS;

  /** Whether or not to use leader follower state transition model
   * for upcoming version.
   */
  private boolean leaderFollowerModelEnabled = false;

  /**
   * Strategies to store backup versions.
   */
  private BackupStrategy backupStrategy = BackupStrategy.DELETE_ON_NEW_PUSH_START;

  /**
   * Whether or not value schema auto registration enabled from push job for this store.
   */
  private boolean schemaAutoRegisteFromPushJobEnabled = false;

  /**
   * Whether or not value schema auto registration enabled from Admin interface for this store.
   */
  private boolean superSetSchemaAutoGenerationForReadComputeEnabled = false;

  /**
   * For read compute stores with auto super-set schema enabled, stores the latest super-set value schema ID.
   */
  private int latestSuperSetValueSchemaId = -1;

  /**
   * Whether or not storage disk quota is enabled for a hybrid store.
   */
  private boolean hybridStoreDiskQuotaEnabled = false;

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

  public Optional<CompressionStrategy> getVersionCompressionStrategy(int versionNumber) {
    if (versionNumber == NON_EXISTING_VERSION) {
      return Optional.empty();
    }

    return getVersion(versionNumber).map(version -> version.getCompressionStrategy());
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

  public void setPartitionCount(int partitionCount) {
    this.partitionCount = partitionCount;
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

  public CompressionStrategy getCompressionStrategy() {
    return compressionStrategy;
  }

  public void setCompressionStrategy(CompressionStrategy compressionStrategy) {
    this.compressionStrategy = compressionStrategy;
  }

  public boolean getClientDecompressionEnabled() {
    return clientDecompressionEnabled;
  }

  public void setClientDecompressionEnabled(boolean clientDecompressionEnabled) {
    this.clientDecompressionEnabled = clientDecompressionEnabled;
  }

  public boolean isChunkingEnabled() {
    return chunkingEnabled;
  }

  public void setChunkingEnabled(boolean chunkingEnabled) {
    this.chunkingEnabled = chunkingEnabled;
  }

  public boolean isSingleGetRouterCacheEnabled() {
    return singleGetRouterCacheEnabled;
  }

  public void setSingleGetRouterCacheEnabled(boolean singleGetRouterCacheEnabled) {
    this.singleGetRouterCacheEnabled = singleGetRouterCacheEnabled;
  }

  public boolean isBatchGetRouterCacheEnabled() { return batchGetRouterCacheEnabled; }

  public void setBatchGetRouterCacheEnabled(boolean batchGetRouterCacheEnabled) {
    this.batchGetRouterCacheEnabled = batchGetRouterCacheEnabled;
  }

  public int getBatchGetLimit() {
    return batchGetLimit;
  }

  public void setBatchGetLimit(int batchGetLimit) {
    this.batchGetLimit = batchGetLimit;
  }

  public boolean isIncrementalPushEnabled() {
    return incrementalPushEnabled;
  }

  public void setIncrementalPushEnabled(boolean incrementalPushEnabled) {
    this.incrementalPushEnabled = incrementalPushEnabled;
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

  public boolean isMigrating() {
    return migrating;
  }

  public void setMigrating(boolean migrating) {
    this.migrating = migrating;
  }

  public int getNumVersionsToPreserve() {
    return numVersionsToPreserve;
  }

  public void setNumVersionsToPreserve(int numVersionsToPreserve) {
    this.numVersionsToPreserve = numVersionsToPreserve;
  }

  public boolean isWriteComputationEnabled() {
    return writeComputationEnabled;
  }

  public void setWriteComputationEnabled(boolean writeComputationEnabled) {
    this.writeComputationEnabled = writeComputationEnabled;
  }

  public boolean isReadComputationEnabled() { return readComputationEnabled; }

  public void setReadComputationEnabled(boolean readComputationEnabled) {
    this.readComputationEnabled = readComputationEnabled;
  }

  public int getBootstrapToOnlineTimeoutInHours() {
    return bootstrapToOnlineTimeoutInHours;
  }

  public void setBootstrapToOnlineTimeoutInHours(int bootstrapToOnlineTimeoutInHours) {
    this.bootstrapToOnlineTimeoutInHours = bootstrapToOnlineTimeoutInHours;
  }

  public boolean isLeaderFollowerModelEnabled() {
    return leaderFollowerModelEnabled;
  }

  public void setLeaderFollowerModelEnabled(boolean leaderFollowerModelEnabled) {
    this.leaderFollowerModelEnabled = leaderFollowerModelEnabled;
  }

  public void setBufferReplayForHybridForVersion(int versionNum, boolean enabled) {
    Optional<Version> version = getVersion(versionNum);
    if (! version.isPresent()) {
      throw new VeniceException("Unknown version: " + versionNum + " in store: " + name);
    }
    version.get().setBufferReplayEnabledForHybrid(enabled);
  }

  public BackupStrategy getBackupStrategy() {
    return backupStrategy;
  }

  public void setBackupStrategy(BackupStrategy value) {
    backupStrategy = value;
  }

  public boolean isSchemaAutoRegisterFromPushJobEnabled() {
    return schemaAutoRegisteFromPushJobEnabled;
  }

  public void setSchemaAutoRegisterFromPushJobEnabled(boolean value) {
    schemaAutoRegisteFromPushJobEnabled = value;
  }

  public boolean isSuperSetSchemaAutoGenerationForReadComputeEnabled() {
    return superSetSchemaAutoGenerationForReadComputeEnabled;
  }

  public void setSuperSetSchemaAutoGenerationForReadComputeEnabled(boolean value) {
    superSetSchemaAutoGenerationForReadComputeEnabled = value;
  }

  public int getLatestSuperSetValueSchemaId() {
    return latestSuperSetValueSchemaId;
  }

  public void setLatestSuperSetValueSchemaId(int valueSchemaId) {
    latestSuperSetValueSchemaId = valueSchemaId;
  }

  public boolean isHybridStoreDiskQuotaEnabled() {
    return hybridStoreDiskQuotaEnabled;
  }

  public void setHybridStoreDiskQuotaEnabled(boolean enabled) {
    hybridStoreDiskQuotaEnabled = enabled;
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

    //update version compression type
    version.setCompressionStrategy(compressionStrategy);

    //update version Helix state model
    version.setLeaderFollowerModelEnabled(leaderFollowerModelEnabled);

    version.setChunkingEnabled(chunkingEnabled);

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

  public Optional<Version> getVersion(int versionNumber) {
    return versions.stream().filter(version -> version.getNumber() == versionNumber).findAny();
  }

  public VersionStatus getVersionStatus(int versionNumber) {
    Optional<Version> version = getVersion(versionNumber);
    if (!version.isPresent()) {
      return VersionStatus.ERROR.NOT_CREATED;
    }

    return version.get().getStatus();
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

  public List<Version> retrieveVersionsToDelete(int clusterNumVersionsToPreserve) {
    int curNumVersionsToPreserve = clusterNumVersionsToPreserve;
    if (numVersionsToPreserve != NUM_VERSION_PRESERVE_NOT_SET) {
      curNumVersionsToPreserve = numVersionsToPreserve;
    }
    // when numVersionsToPreserve is less than 1, it usually means a config issue.
    // Setting it to zero, will cause the store to be deleted as soon as push completes.
    if(curNumVersionsToPreserve < 1) {
      throw new IllegalArgumentException("At least 1 version should be preserved. Parameter " + curNumVersionsToPreserve);
    }

    if(versions.size() == 0) {
      return new ArrayList<>();
    }

    // The code assumes that Versions are sorted in increasing order by addVersion and increaseVersion
    int lastElementIndex = versions.size() - 1;
    List<Version> versionsToDelete = new ArrayList<>();

    /**
     * The current version need not be the last largest version (eg we rolled back to a earlier version).
     * The versions which can be deleted are:
     *     a) ONLINE versions except the current version given we preserve numVersionsToPreserve versions.
     *     b) ERROR version (ideally should not be there as AbstractPushmonitor#handleErrorPush deletes those)
     *     c) STARTED versions if its not the last one and the store is not migrating.
     */
    for (int i = lastElementIndex; i >= 0; i--) {
      Version version = versions.get(i);

      if (version.getNumber() == currentVersion) { // currentVersion is always preserved
        curNumVersionsToPreserve--;
      } else if (VersionStatus.canDelete(version.getStatus())) {  // ERROR versions are always deleted
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
        // So if the store is not migrating, it's stuck in STARTED, it means somehow the controller did not update the version status properly.
        versionsToDelete.add(version);
      }
      // TODO here we don't deal with the PUSHED version, just keep all of them, need to consider collect them too in the future.
    }
    return versionsToDelete;
  }

  public boolean isSystemStore() {
    return getName().startsWith(SYSTEM_STORE_NAME_PADDING);
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
    result = 31 * result + (compressionStrategy.hashCode());
    result = 31 * result + (clientDecompressionEnabled ? 1 : 0);
    result = 31 * result + (chunkingEnabled ? 1 : 0);
    result = 31 * result + (singleGetRouterCacheEnabled ? 1 : 0);
    result = 31 * result + (batchGetRouterCacheEnabled ? 1 : 0);
    result = 31 * result + batchGetLimit;
    result = 31 * result + numVersionsToPreserve;
    result = 31 * result + (incrementalPushEnabled ? 1 : 0);
    result = 31 * result + (migrating ? 1 : 0);
    result = 31 * result + (writeComputationEnabled ? 1 : 0);
    result = 31 * result + (readComputationEnabled ? 1 : 0);
    result = 31 * result + bootstrapToOnlineTimeoutInHours;
    result = 31 * result + (leaderFollowerModelEnabled ? 1: 0);
    result = 31 * result + backupStrategy.hashCode();
    result = 31 * result + (schemaAutoRegisteFromPushJobEnabled ? 1 : 0);
    result = 31 * result + (superSetSchemaAutoGenerationForReadComputeEnabled ? 1 : 0);
    result = 31 * result + latestSuperSetValueSchemaId;
    result = 31 * result + (hybridStoreDiskQuotaEnabled ? 1 : 0);
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
    if (compressionStrategy != store.compressionStrategy) return false;
    if (clientDecompressionEnabled != store.clientDecompressionEnabled) return false;
    if (chunkingEnabled != store.chunkingEnabled) return false;
    if (singleGetRouterCacheEnabled != store.singleGetRouterCacheEnabled) return false;
    if (batchGetRouterCacheEnabled != store.batchGetRouterCacheEnabled) return false;
    if (batchGetLimit != store.batchGetLimit) return false;
    if (numVersionsToPreserve != store.numVersionsToPreserve) return false;
    if (incrementalPushEnabled != store.incrementalPushEnabled) return false;
    if (migrating != store.migrating) return false;
    if (writeComputationEnabled != store.writeComputationEnabled) return false;
    if (readComputationEnabled != store.readComputationEnabled) return false;
    if (bootstrapToOnlineTimeoutInHours != store.bootstrapToOnlineTimeoutInHours) return false;
    if (leaderFollowerModelEnabled != store.leaderFollowerModelEnabled) return false;
    if (backupStrategy != store.backupStrategy) return false;
    if (schemaAutoRegisteFromPushJobEnabled != store.schemaAutoRegisteFromPushJobEnabled) return false;
    if (superSetSchemaAutoGenerationForReadComputeEnabled != store.schemaAutoRegisteFromPushJobEnabled) return false;
    if (latestSuperSetValueSchemaId != store.latestSuperSetValueSchemaId) return false;
    if (hybridStoreDiskQuotaEnabled != store.hybridStoreDiskQuotaEnabled) return false;
    return !(hybridStoreConfig != null ? !hybridStoreConfig.equals(store.hybridStoreConfig) : store.hybridStoreConfig != null);
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
    clonedStore.setAccessControlled(accessControlled);
    clonedStore.setCompressionStrategy(compressionStrategy);
    clonedStore.setClientDecompressionEnabled(clientDecompressionEnabled);
    clonedStore.setChunkingEnabled(chunkingEnabled);
    clonedStore.setSingleGetRouterCacheEnabled(singleGetRouterCacheEnabled);
    clonedStore.setBatchGetRouterCacheEnabled(batchGetRouterCacheEnabled);
    clonedStore.setBatchGetLimit(batchGetLimit);
    clonedStore.setNumVersionsToPreserve(numVersionsToPreserve);
    clonedStore.setIncrementalPushEnabled(incrementalPushEnabled);
    clonedStore.setLargestUsedVersionNumber(largestUsedVersionNumber);
    clonedStore.setMigrating(migrating);
    clonedStore.setWriteComputationEnabled(writeComputationEnabled);
    clonedStore.setReadComputationEnabled(readComputationEnabled);
    clonedStore.setBootstrapToOnlineTimeoutInHours(bootstrapToOnlineTimeoutInHours);
    clonedStore.setLeaderFollowerModelEnabled(leaderFollowerModelEnabled);
    clonedStore.setBackupStrategy(backupStrategy);
    clonedStore.setSchemaAutoRegisterFromPushJobEnabled(schemaAutoRegisteFromPushJobEnabled);
    clonedStore.setSuperSetSchemaAutoGenerationForReadComputeEnabled(superSetSchemaAutoGenerationForReadComputeEnabled);
    clonedStore.setLatestSuperSetValueSchemaId(latestSuperSetValueSchemaId);
    clonedStore.setHybridStoreDiskQuotaEnabled(hybridStoreDiskQuotaEnabled);
    for (Version v : this.versions) {
      clonedStore.forceAddVersion(v.cloneVersion());
    }

    /**
     * Add version can overwrite the value of {@link largestUsedVersionNumber}, so in order to clone the
     * object properly, it's important to call the {@link #setLargestUsedVersionNumber(int)} setter after
     * calling {@link #forceAddVersion(Version)}.
     */
    clonedStore.setLargestUsedVersionNumber(largestUsedVersionNumber);

    return clonedStore;
  }

  private void checkDisableStoreWrite(String action, int version) {
    if (!enableWrites) {
      throw new StoreDisabledException(name, action, version);
    }
  }
}
