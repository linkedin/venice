package com.linkedin.venice.meta;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.StoreDisabledException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.systemstore.schemas.StoreVersion;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


/**
 * This is an abstraction of metadata maintained per Store.
 */
public abstract class Store {
  /**
   * Special version number indicates none of version is available to read.
   */
  public static final int NON_EXISTING_VERSION = 0;

  /**
   * Default value of numVersionPreserve, by default we should use cluster level config instead of store level config.
   */
  public static int NUM_VERSION_PRESERVE_NOT_SET = 0;

  public static final String SYSTEM_STORE_NAME_PREFIX = "venice_system_store_";
  public static final String SYSTEM_STORE_FORMAT = SYSTEM_STORE_NAME_PREFIX + "%s";
  public static int DEFAULT_REPLICATION_FACTOR = 3;
  //Only for testing
  public static void setDefaultReplicationFactor(int defaultReplicationFactor) {
    DEFAULT_REPLICATION_FACTOR = defaultReplicationFactor;
  }

  /**
   * Default storage quota 20GB
   */
  public static long DEFAULT_STORAGE_QUOTA = (long)20 * (1 << 30);

  public static final long UNLIMITED_STORAGE_QUOTA = -1;
  /**
   * Default read quota 1800 QPS per node
   */
  public static long DEFAULT_READ_QUOTA = 1800;

  public static void setDefaultStorageQuota(long storageQuota) {
    Store.DEFAULT_STORAGE_QUOTA = storageQuota;
  }

  public static void setDefaultReadQuota(long readQuota) {
    Store.DEFAULT_READ_QUOTA = readQuota;
  }

  public static final int IGNORE_VERSION = -1;

  public static final int BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS = 24;

  public static long DEFAULT_RT_RETENTION_TIME = TimeUnit.DAYS.toMillis(5);

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


  /**
   * This function is used to pre-fill the default value defined in schema.
   * So far, it only support the followings:
   * 1. Only top-level fields are supported.
   * 2. All the primitive types are supported.
   * 3. For union, only `null` default is supported.
   * 4. For array/map, only empty list/map are supported.
   *
   * Other than the above, this function will throw exception.
   *
   * TODO: once the whole stack migrates to the modern avro version (avro-1.7+), we could leverage
   * SpecificRecord builder to prefill the default value, which will be much more powerful.
   * @param recordType
   * @param <T>
   * @return
   */
  static <T extends SpecificRecord> T prefillAvroRecordWithDefaultValue(T recordType) {
    Schema schema = recordType.getSchema();
    for (Schema.Field field : schema.getFields()) {
      if (field.defaultValue() != null) {
        // has default
        Object defaultValue = AvroCompatibilityHelper.getSpecificDefaultValue(field);
        Schema.Type fieldType = field.schema().getType();
        switch (fieldType) {
          case NULL:
            throw new VeniceException("Default value for `null` type is not expected");
          case BOOLEAN:
          case INT:
          case LONG:
          case FLOAT:
          case DOUBLE:
          case STRING:
            recordType.put(field.pos(), defaultValue);
            break;
          case UNION:
            if (null == defaultValue) {
              recordType.put(field.pos(), null);
            } else {
              throw new VeniceException("Non 'null' default value is not supported for union type: " + field.name());
            }
            break;
          case ARRAY:
            Collection collection = (Collection)defaultValue;
            if (collection.isEmpty()) {
              recordType.put(field.pos(), new ArrayList<>());
            } else {
              throw new VeniceException("Non 'empty array' default value is not supported for array type: " + field.name());
            }
            break;
          case MAP:
            Map map = (Map)defaultValue;
            if (map.isEmpty()) {
              recordType.put(field.pos(), new HashMap<>());
            } else {
              throw new VeniceException("Non 'empty map' default value is not supported for map type: " + field.name());
            }
            break;
          case ENUM:
          case FIXED:
          case BYTES:
          case RECORD:
            throw new VeniceException("Default value for field: " + field.name() + " with type: " + fieldType + " is not supported");
        }
      }
    }

    return recordType;
  }

  /**
   * This field is to let current class talk to the inherited classes for version related operations.
   */
  private Supplier<List<StoreVersion>> storeVersionsSupplier;

  /**
   * This function should be invoked only once.
   */
  protected synchronized void setupVersionSupplier(Supplier<List<StoreVersion>> versionsSupplier) {
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

  public abstract String getName();

  public abstract String getOwner();

  public abstract void setOwner(String owner);

  public abstract long getCreatedTime();

  public abstract int getCurrentVersion();

  public abstract void setCurrentVersion(int currentVersion);

  public abstract void setCurrentVersionWithoutCheck(int currentVersion);

  public abstract PersistenceType getPersistenceType();

  public abstract void setPersistenceType(PersistenceType persistenceType);

  public abstract RoutingStrategy getRoutingStrategy();

  public abstract ReadStrategy getReadStrategy();

  public abstract OfflinePushStrategy getOffLinePushStrategy();

  public abstract int getLargestUsedVersionNumber();

  public abstract void setLargestUsedVersionNumber(int largestUsedVersionNumber);

  public abstract long getStorageQuotaInByte();

  public abstract void setStorageQuotaInByte(long storageQuotaInByte);

  public abstract int getPartitionCount();

  public abstract void setPartitionCount(int partitionCount);

  public abstract PartitionerConfig getPartitionerConfig();

  public abstract void setPartitionerConfig(PartitionerConfig value);

  public abstract boolean isEnableWrites();

  public abstract void setEnableWrites(boolean enableWrites);

  public abstract boolean isEnableReads();

  public abstract void setEnableReads(boolean enableReads);

  public abstract long getReadQuotaInCU();

  public abstract void setReadQuotaInCU(long readQuotaInCU);

  public abstract HybridStoreConfig getHybridStoreConfig();

  public abstract void setHybridStoreConfig(HybridStoreConfig hybridStoreConfig);

  public abstract boolean isHybrid();

  public abstract CompressionStrategy getCompressionStrategy();

  public abstract void setCompressionStrategy(CompressionStrategy compressionStrategy);

  public abstract boolean getClientDecompressionEnabled();

  public abstract void setClientDecompressionEnabled(boolean clientDecompressionEnabled);

  public abstract boolean isChunkingEnabled();

  public abstract void setChunkingEnabled(boolean chunkingEnabled);

  public abstract int getBatchGetLimit();

  public abstract void setBatchGetLimit(int batchGetLimit);

  public abstract boolean isIncrementalPushEnabled();

  public abstract void setIncrementalPushEnabled(boolean incrementalPushEnabled);

  public abstract boolean isAccessControlled();

  public abstract void setAccessControlled(boolean accessControlled);

  public abstract boolean isMigrating();

  public abstract void setMigrating(boolean migrating);

  public abstract int getNumVersionsToPreserve();

  public abstract void setNumVersionsToPreserve(int numVersionsToPreserve);

  public abstract boolean isWriteComputationEnabled();

  public abstract void setWriteComputationEnabled(boolean writeComputationEnabled);

  public abstract boolean isReadComputationEnabled();

  public abstract void setReadComputationEnabled(boolean readComputationEnabled);

  public abstract int getBootstrapToOnlineTimeoutInHours();

  public abstract void setBootstrapToOnlineTimeoutInHours(int bootstrapToOnlineTimeoutInHours);

  public abstract boolean isLeaderFollowerModelEnabled();

  public abstract void setLeaderFollowerModelEnabled(boolean leaderFollowerModelEnabled);

  public abstract String getPushStreamSourceAddress();

  public abstract void setPushStreamSourceAddress(String sourceAddress);

  public abstract boolean isNativeReplicationEnabled();

  public abstract void setNativeReplicationEnabled(boolean nativeReplicationEnabled);

  public abstract BackupStrategy getBackupStrategy();

  public abstract void setBackupStrategy(BackupStrategy value);

  public abstract boolean isSchemaAutoRegisterFromPushJobEnabled();

  public abstract void setSchemaAutoRegisterFromPushJobEnabled(boolean value);

  public abstract int getLatestSuperSetValueSchemaId();

  public abstract void setLatestSuperSetValueSchemaId(int valueSchemaId);

  public abstract boolean isHybridStoreDiskQuotaEnabled();

  public abstract void setHybridStoreDiskQuotaEnabled(boolean enabled);

  public abstract ETLStoreConfig getEtlStoreConfig();

  public abstract void setEtlStoreConfig(ETLStoreConfig etlStoreConfig);

  public abstract boolean isStoreMetadataSystemStoreEnabled();

  public abstract void setStoreMetadataSystemStoreEnabled(boolean storeMetadataSystemStoreEnabled);

  public abstract boolean isStoreMetaSystemStoreEnabled();

  public abstract void setStoreMetaSystemStoreEnabled(boolean storeMetaSystemStoreEnabled);

  public abstract IncrementalPushPolicy getIncrementalPushPolicy();

  public abstract void setIncrementalPushPolicy(IncrementalPushPolicy incrementalPushPolicy);

  public abstract long getLatestVersionPromoteToCurrentTimestamp();

  public abstract void setLatestVersionPromoteToCurrentTimestamp(long latestVersionPromoteToCurrentTimestamp);

  public abstract long getBackupVersionRetentionMs();

  public abstract void setBackupVersionRetentionMs(long backupVersionRetentionMs);

  public abstract long getRetentionTime();

  public abstract int getReplicationFactor();

  public abstract void setReplicationFactor(int replicationFactor);

  public abstract boolean isMigrationDuplicateStore();

  public abstract void setMigrationDuplicateStore(boolean migrationDuplicateStore);

  public abstract String getNativeReplicationSourceFabric();

  public abstract void setNativeReplicationSourceFabric(String nativeReplicationSourceFabric);

  public abstract Map<String, SystemStoreAttributes> getSystemStores();

  public abstract void setSystemStores(Map<String, SystemStoreAttributes> systemStores);

  protected abstract void putSystemStore(VeniceSystemStoreType systemStoreType, SystemStoreAttributes systemStoreAttributes);

  public abstract boolean isDaVinciPushStatusStoreEnabled();

  public abstract void setDaVinciPushStatusStoreEnabled(boolean daVinciPushStatusStoreEnabled);

  public abstract Store cloneStore();

  public List<Version> getVersions() {
    checkVersionSupplier();
    if (storeVersionsSupplier.get().isEmpty()) {
      return Collections.emptyList();
    }
    return storeVersionsSupplier.get().stream().map(sv -> new Version(sv)).collect(Collectors.toList());
  }

  public void setVersions(List<Version> versions) {
    checkVersionSupplier();
    storeVersionsSupplier.get().clear();
    versions.forEach(v -> storeVersionsSupplier.get().add(v.dataModel()));
  }

  public Optional<CompressionStrategy> getVersionCompressionStrategy(int versionNumber) {
    if (versionNumber == NON_EXISTING_VERSION) {
      return Optional.empty();
    }

    return getVersion(versionNumber).map(version -> version.getCompressionStrategy());
  }

  public void setBufferReplayForHybridForVersion(int versionNum, boolean enabled) {
    Optional<Version> version = getVersion(versionNum);
    if (! version.isPresent()) {
      throw new VeniceException("Unknown version: " + versionNum + " in store: " + getName());
    }
    version.get().setBufferReplayEnabledForHybrid(enabled);
  }

  public void addVersion(Version version) {
    addVersion(version, true, false);
  }

  protected void forceAddVersion(Version version, boolean isClonedVersion){
    addVersion(version, false, isClonedVersion);
  }

  protected void checkDisableStoreWrite(String action, int version) {
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
    for (; index < storeVersionsSupplier.get().size(); index++) {
      if (storeVersionsSupplier.get().get(index).number == version.getNumber()) {
        throw new VeniceException("Version is repeated. Store: " + getName() + " Version: " + version.getNumber());
      }
      if (storeVersionsSupplier.get().get(index).number > version.getNumber()) {
        break;
      }
    }

    if (!isClonedVersion) {
      // For new version, apply store level config on it.
      //update version compression type
      version.setCompressionStrategy(getCompressionStrategy());

      //update version Helix state model
      version.setLeaderFollowerModelEnabled(isLeaderFollowerModelEnabled());

      version.setChunkingEnabled(isChunkingEnabled());

      version.setPartitionerConfig(getPartitionerConfig());

      version.setNativeReplicationEnabled(isNativeReplicationEnabled());

      version.setIncrementalPushPolicy(getIncrementalPushPolicy());

      version.setReplicationFactor(getReplicationFactor());

      version.setNativeReplicationSourceFabric(getNativeReplicationSourceFabric());

      version.setIncrementalPushEnabled(isIncrementalPushEnabled());

      version.setUseVersionLevelIncrementalPushEnabled(true);

      version.setHybridStoreConfig(getHybridStoreConfig());

      version.setUseVersionLevelHybridConfig(true);
    }

    storeVersionsSupplier.get().add(index, version.dataModel());
    if (version.getNumber() > getLargestUsedVersionNumber()) {
      setLargestUsedVersionNumber(version.getNumber());
    }
  }

  public Version deleteVersion(int versionNumber) {
    checkVersionSupplier();
    for (int i = 0; i < storeVersionsSupplier.get().size(); i++) {
      Version version = new Version(storeVersionsSupplier.get().get(i));
      if (version.getNumber() == versionNumber) {
        storeVersionsSupplier.get().remove(i);
        return version;
      }
    }
    return null;
  }

  public boolean containsVersion(int versionNumber) {
    checkVersionSupplier();
    for (int i = 0; i < storeVersionsSupplier.get().size(); i++) {
      Version version = new Version(storeVersionsSupplier.get().get(i));
      if (version.getNumber() == versionNumber) {
        return true;
      }
    }
    return false;
  }

  public void updateVersionStatus(int versionNumber, VersionStatus status) {
    checkVersionSupplier();
    if (status.equals(VersionStatus.ONLINE)) {
      checkDisableStoreWrite("become ONLINE", versionNumber);
    }
    for (int i = storeVersionsSupplier.get().size() - 1; i >= 0; i--) {
      Version version = new Version(storeVersionsSupplier.get().get(i));
      if (version.getNumber() == versionNumber) {
        version.setStatus(status);
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
    checkVersionSupplier();
    for (StoreVersion storeVersion : storeVersionsSupplier.get()) {
      Version version = new Version(storeVersion);
      if (version.getNumber() == versionNumber) {
        return Optional.of(version);
      }
    }
    return Optional.empty();
  }

  public VersionStatus getVersionStatus(int versionNumber) {
    Optional<Version> version = getVersion(versionNumber);
    if (!version.isPresent()) {
      return VersionStatus.ERROR.NOT_CREATED;
    }

    return version.get().getStatus();
  }

  private Version increaseVersion(String pushJobId, boolean createNewVersion) {
    int versionNumber = getLargestUsedVersionNumber() + 1;
    checkDisableStoreWrite("increase", versionNumber);
    Version version = new Version(getName(), versionNumber, pushJobId);
    if (createNewVersion) {
      addVersion(version);
      return version.cloneVersion();
    } else {
      return version;
    }
  }

  public List<Version> retrieveVersionsToDelete(int clusterNumVersionsToPreserve) {
    checkVersionSupplier();
    int curNumVersionsToPreserve = clusterNumVersionsToPreserve;
    if (getNumVersionsToPreserve() != NUM_VERSION_PRESERVE_NOT_SET) {
      curNumVersionsToPreserve = getNumVersionsToPreserve();
    }
    // when numVersionsToPreserve is less than 1, it usually means a config issue.
    // Setting it to zero, will cause the store to be deleted as soon as push completes.
    if(curNumVersionsToPreserve < 1) {
      throw new IllegalArgumentException("At least 1 version should be preserved. Parameter " + curNumVersionsToPreserve);
    }

    int versionCnt = storeVersionsSupplier.get().size();
    if(versionCnt == 0) {
      return new ArrayList<>();
    }

    // The code assumes that Versions are sorted in increasing order by addVersion and increaseVersion
    int lastElementIndex = versionCnt - 1;
    List<Version> versionsToDelete = new ArrayList<>();

    /**
     * The current version need not be the last largest version (eg we rolled back to a earlier version).
     * The versions which can be deleted are:
     *     a) ONLINE versions except the current version given we preserve numVersionsToPreserve versions.
     *     b) ERROR version (ideally should not be there as AbstractPushmonitor#handleErrorPush deletes those)
     *     c) STARTED versions if its not the last one and the store is not migrating.
     */
    for (int i = lastElementIndex; i >= 0; i--) {
      Version version = new Version(storeVersionsSupplier.get().get(i));

      if (version.getNumber() == getCurrentVersion()) { // currentVersion is always preserved
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
    return isSystemStore(getName());
  }

  public static boolean isSystemStore(String storeName) {
    return storeName.startsWith(SYSTEM_STORE_NAME_PREFIX);
  }

  public void fixMissingFields() {
    checkVersionSupplier();
    for (StoreVersion storeVersion : storeVersionsSupplier.get()) {
      Version version = new Version(storeVersion);
      if (version.getPartitionerConfig() == null) {
        version.setPartitionerConfig(getPartitionerConfig());
      }
      if (version.getPartitionCount() == 0) {
        version.setPartitionCount(getPartitionCount());
      }
    }
  }
}
