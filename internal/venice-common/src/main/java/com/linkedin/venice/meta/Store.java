package com.linkedin.venice.meta;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.StoreVersionNotFoundException;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * This interface defines all the public APIs, and if you need to add accessors to
 * some new fields, this interface needs to be changed accordingly.
 *
 * IMPORTANT: getter functions must start with `is` for boolean result and `get` for other types, and setter functions
 *            must start with `set`!
 */
public interface Store {
  /**
   * Special version number indicates none of version is available to read.
   */
  int NON_EXISTING_VERSION = 0;

  /**
   * Default value of numVersionPreserve, by default we should use cluster level config instead of store level config.
   */
  int NUM_VERSION_PRESERVE_NOT_SET = 0;

  String SYSTEM_STORE_NAME_PREFIX = "venice_system_store_";
  String SYSTEM_STORE_FORMAT = SYSTEM_STORE_NAME_PREFIX + "%s";

  long UNLIMITED_STORAGE_QUOTA = -1;

  int IGNORE_VERSION = -1;

  int BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS = 24;

  long DEFAULT_RT_RETENTION_TIME = TimeUnit.DAYS.toMillis(5);

  int DEFAULT_BATCH_GET_LIMIT = 150;

  static boolean isSystemStore(String storeName) {
    return storeName.startsWith(SYSTEM_STORE_NAME_PREFIX);
  }

  String getName();

  String getOwner();

  void setOwner(String owner);

  long getCreatedTime();

  int getCurrentVersion();

  void setCurrentVersion(int currentVersion);

  void setCurrentVersionWithoutCheck(int currentVersion);

  long getLowWatermark();

  void setLowWatermark(long lowWatermark);

  PersistenceType getPersistenceType();

  void setPersistenceType(PersistenceType persistenceType);

  RoutingStrategy getRoutingStrategy();

  ReadStrategy getReadStrategy();

  OfflinePushStrategy getOffLinePushStrategy();

  int getLargestUsedVersionNumber();

  void setLargestUsedVersionNumber(int largestUsedVersionNumber);

  int getLargestUsedRTVersionNumber();

  void setLargestUsedRTVersionNumber(int largestUsedRTVersionNumber);

  long getStorageQuotaInByte();

  void setStorageQuotaInByte(long storageQuotaInByte);

  int getPartitionCount();

  void setPartitionCount(int partitionCount);

  PartitionerConfig getPartitionerConfig();

  void setPartitionerConfig(PartitionerConfig value);

  boolean isEnableWrites();

  void setEnableWrites(boolean enableWrites);

  boolean isEnableReads();

  void setEnableReads(boolean enableReads);

  long getReadQuotaInCU();

  void setReadQuotaInCU(long readQuotaInCU);

  HybridStoreConfig getHybridStoreConfig();

  void setHybridStoreConfig(HybridStoreConfig hybridStoreConfig);

  Map<String, ViewConfig> getViewConfigs();

  void setViewConfigs(Map<String, ViewConfig> viewConfigMap);

  boolean isHybrid();

  CompressionStrategy getCompressionStrategy();

  void setCompressionStrategy(CompressionStrategy compressionStrategy);

  boolean getClientDecompressionEnabled();

  void setClientDecompressionEnabled(boolean clientDecompressionEnabled);

  boolean isChunkingEnabled();

  void setChunkingEnabled(boolean chunkingEnabled);

  boolean isRmdChunkingEnabled();

  void setRmdChunkingEnabled(boolean rmdChunkingEnabled);

  int getBatchGetLimit();

  void setBatchGetLimit(int batchGetLimit);

  boolean isIncrementalPushEnabled();

  void setIncrementalPushEnabled(boolean incrementalPushEnabled);

  boolean isSeparateRealTimeTopicEnabled();

  void setSeparateRealTimeTopicEnabled(boolean separateRealTimeTopicEnabled);

  boolean isAccessControlled();

  void setAccessControlled(boolean accessControlled);

  boolean isMigrating();

  void setMigrating(boolean migrating);

  int getNumVersionsToPreserve();

  void setNumVersionsToPreserve(int numVersionsToPreserve);

  boolean isWriteComputationEnabled();

  void setWriteComputationEnabled(boolean writeComputationEnabled);

  boolean isReadComputationEnabled();

  void setReadComputationEnabled(boolean readComputationEnabled);

  int getBootstrapToOnlineTimeoutInHours();

  void setBootstrapToOnlineTimeoutInHours(int bootstrapToOnlineTimeoutInHours);

  String getPushStreamSourceAddress();

  void setPushStreamSourceAddress(String sourceAddress);

  boolean isNativeReplicationEnabled();

  int getRmdVersion();

  void setRmdVersion(int rmdVersion);

  void setNativeReplicationEnabled(boolean nativeReplicationEnabled);

  BackupStrategy getBackupStrategy();

  void setBackupStrategy(BackupStrategy value);

  boolean isSchemaAutoRegisterFromPushJobEnabled();

  void setSchemaAutoRegisterFromPushJobEnabled(boolean value);

  int getLatestSuperSetValueSchemaId();

  void setLatestSuperSetValueSchemaId(int valueSchemaId);

  boolean isHybridStoreDiskQuotaEnabled();

  void setHybridStoreDiskQuotaEnabled(boolean enabled);

  ETLStoreConfig getEtlStoreConfig();

  void setEtlStoreConfig(ETLStoreConfig etlStoreConfig);

  boolean isStoreMetadataSystemStoreEnabled();

  void setStoreMetadataSystemStoreEnabled(boolean storeMetadataSystemStoreEnabled);

  boolean isStoreMetaSystemStoreEnabled();

  void setStoreMetaSystemStoreEnabled(boolean storeMetaSystemStoreEnabled);

  long getLatestVersionPromoteToCurrentTimestamp();

  void setLatestVersionPromoteToCurrentTimestamp(long latestVersionPromoteToCurrentTimestamp);

  long getBackupVersionRetentionMs();

  void setBackupVersionRetentionMs(long backupVersionRetentionMs);

  long getRetentionTime();

  int getReplicationFactor();

  void setReplicationFactor(int replicationFactor);

  boolean isMigrationDuplicateStore();

  void setMigrationDuplicateStore(boolean migrationDuplicateStore);

  String getNativeReplicationSourceFabric();

  void setNativeReplicationSourceFabric(String nativeReplicationSourceFabric);

  boolean isActiveActiveReplicationEnabled();

  void setActiveActiveReplicationEnabled(boolean activeActiveReplicationEnabled);

  Map<String, SystemStoreAttributes> getSystemStores();

  void setSystemStores(Map<String, SystemStoreAttributes> systemStores);

  void putSystemStore(VeniceSystemStoreType systemStoreType, SystemStoreAttributes systemStoreAttributes);

  boolean isDaVinciPushStatusStoreEnabled();

  void setDaVinciPushStatusStoreEnabled(boolean daVinciPushStatusStoreEnabled);

  Store cloneStore();

  List<Version> getVersions();

  @JsonIgnore
  default IntSet getVersionNumbers() {
    List<Version> versions = getVersions();
    IntSet versionNumbers = new IntOpenHashSet(versions.size());
    for (Version version: versions) {
      versionNumbers.add(version.getNumber());
    }
    return versionNumbers;
  }

  void setVersions(List<Version> versions);

  void addVersion(Version version);

  void addVersion(Version version, boolean isClonedVersion, int currentRTVersionNumber);

  void forceAddVersion(Version version, boolean isClonedVersion);

  void checkDisableStoreWrite(String action, int version);

  Version deleteVersion(int versionNumber);

  boolean containsVersion(int versionNumber);

  void updateVersionStatus(int versionNumber, VersionStatus status);

  int peekNextVersionNumber();

  /**
   * @param versionNumber for which to get the {@link Version}
   * @return the {@link Version} corresponding to the provided {@param versionNumber}, or null if no such version exists
   */
  @Nullable
  Version getVersion(int versionNumber);

  @Nonnull
  Version getVersionOrThrow(int versionNumber) throws StoreVersionNotFoundException;

  VersionStatus getVersionStatus(int versionNumber);

  List<Version> retrieveVersionsToDelete(int clusterNumVersionsToPreserve);

  boolean isSystemStore();

  void fixMissingFields();

  boolean isStorageNodeReadQuotaEnabled();

  void setStorageNodeReadQuotaEnabled(boolean storageNodeReadQuotaEnabled);

  boolean isCompactionEnabled();

  void setCompactionEnabled(boolean compactionEnabled);

  long getCompactionThresholdMilliseconds();

  void setCompactionThresholdMilliseconds(long compactionThreshold);

  long getMinCompactionLagSeconds();

  void setMinCompactionLagSeconds(long minCompactionLagSeconds);

  long getMaxCompactionLagSeconds();

  void setMaxCompactionLagSeconds(long maxCompactionLagSeconds);

  int getMaxRecordSizeBytes();

  void setMaxRecordSizeBytes(int maxRecordSizeBytes);

  int getMaxNearlineRecordSizeBytes();

  void setMaxNearlineRecordSizeBytes(int maxNearlineRecordSizeBytes);

  void setUnusedSchemaDeletionEnabled(boolean unusedSchemaDeletionEnabled);

  boolean isUnusedSchemaDeletionEnabled();

  boolean isBlobTransferEnabled();

  void setBlobTransferEnabled(boolean blobTransferEnabled);

  boolean isNearlineProducerCompressionEnabled();

  void setNearlineProducerCompressionEnabled(boolean compressionEnabled);

  int getNearlineProducerCountPerWriter();

  void setNearlineProducerCountPerWriter(int producerCnt);

  String getTargetSwapRegion();

  int getTargetSwapRegionWaitTime();

  void setTargetSwapRegion(String targetRegion);

  void setTargetSwapRegionWaitTime(int waitTime);

  void setIsDavinciHeartbeatReported(boolean isReported);

  boolean getIsDavinciHeartbeatReported();

  void updateVersionForDaVinciHeartbeat(int versionNumber, boolean reported);

  boolean isGlobalRtDivEnabled();

  void setGlobalRtDivEnabled(boolean globalRtDivEnabled);

  boolean isTTLRepushEnabled();

  void setTTLRepushEnabled(boolean ttlRepushEnabled);
}
