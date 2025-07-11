package com.linkedin.venice.meta;

import static com.linkedin.venice.meta.Store.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS;
import static com.linkedin.venice.meta.Store.NUM_VERSION_PRESERVE_NOT_SET;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 * Json-serializable class for sending store information to the controller client
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StoreInfo {
  public static StoreInfo fromStore(Store store) {
    if (store == null) {
      return null;
    }
    StoreInfo storeInfo = new StoreInfo();
    // Sort the properties alphabetically
    storeInfo.setAccessControlled(store.isAccessControlled());
    storeInfo.setActiveActiveReplicationEnabled(store.isActiveActiveReplicationEnabled());
    storeInfo.setBackupStrategy(store.getBackupStrategy());
    storeInfo.setBackupVersionRetentionMs(store.getBackupVersionRetentionMs());
    storeInfo.setBatchGetLimit(store.getBatchGetLimit());
    storeInfo.setBootstrapToOnlineTimeoutInHours(store.getBootstrapToOnlineTimeoutInHours());
    storeInfo.setChunkingEnabled(store.isChunkingEnabled());
    storeInfo.setRmdChunkingEnabled(store.isRmdChunkingEnabled());
    storeInfo.setClientDecompressionEnabled(store.getClientDecompressionEnabled());
    storeInfo.setCompressionStrategy(store.getCompressionStrategy());
    storeInfo.setCurrentVersion(store.getCurrentVersion());
    storeInfo.setDaVinciPushStatusStoreEnabled(store.isDaVinciPushStatusStoreEnabled());
    storeInfo.setEnableStoreReads(store.isEnableReads());
    storeInfo.setEnableStoreWrites(store.isEnableWrites());
    storeInfo.setEtlStoreConfig(store.getEtlStoreConfig());
    if (store.isHybrid()) {
      storeInfo.setHybridStoreConfig(store.getHybridStoreConfig());
    }
    storeInfo.setHybridStoreDiskQuotaEnabled(store.isHybridStoreDiskQuotaEnabled());
    storeInfo.setIncrementalPushEnabled(store.isIncrementalPushEnabled());
    storeInfo.setLargestUsedVersionNumber(store.getLargestUsedVersionNumber());
    storeInfo.setLargestUsedRTVersionNumber(store.getLargestUsedRTVersionNumber());
    storeInfo.setLatestSuperSetValueSchemaId(store.getLatestSuperSetValueSchemaId());
    storeInfo.setLowWatermark(store.getLowWatermark());
    storeInfo.setMigrating(store.isMigrating());
    storeInfo.setMigrationDuplicateStore(store.isMigrationDuplicateStore());
    storeInfo.setName(store.getName());
    storeInfo.setNativeReplicationEnabled(store.isNativeReplicationEnabled());
    storeInfo.setNativeReplicationSourceFabric(store.getNativeReplicationSourceFabric());
    storeInfo.setNumVersionsToPreserve(store.getNumVersionsToPreserve());
    storeInfo.setOwner(store.getOwner());
    storeInfo.setPartitionCount(store.getPartitionCount());
    storeInfo.setPartitionerConfig(store.getPartitionerConfig());
    storeInfo.setPushStreamSourceAddress(store.getPushStreamSourceAddress());
    storeInfo.setReadComputationEnabled(store.isReadComputationEnabled());
    storeInfo.setReadQuotaInCU(store.getReadQuotaInCU());
    storeInfo.setReplicationFactor(store.getReplicationFactor());
    storeInfo.setSchemaAutoRegisterFromPushJobEnabled(store.isSchemaAutoRegisterFromPushJobEnabled());
    storeInfo.setStorageQuotaInByte(store.getStorageQuotaInByte());
    storeInfo.setStoreMetaSystemStoreEnabled(store.isStoreMetaSystemStoreEnabled());
    storeInfo.setStoreMetadataSystemStoreEnabled(store.isStoreMetadataSystemStoreEnabled());
    storeInfo.setVersions(store.getVersions());
    storeInfo.setWriteComputationEnabled(store.isWriteComputationEnabled());
    storeInfo.setReplicationMetadataVersionId(store.getRmdVersion());
    storeInfo.setViewConfigs(store.getViewConfigs());
    storeInfo.setStorageNodeReadQuotaEnabled(store.isStorageNodeReadQuotaEnabled());
    storeInfo.setCompactionEnabled(store.isCompactionEnabled());
    storeInfo.setMinCompactionLagSeconds(store.getMinCompactionLagSeconds());
    storeInfo.setMaxCompactionLagSeconds(store.getMaxCompactionLagSeconds());
    storeInfo.setMaxRecordSizeBytes(store.getMaxRecordSizeBytes());
    storeInfo.setMaxNearlineRecordSizeBytes(store.getMaxNearlineRecordSizeBytes());
    storeInfo.setUnusedSchemaDeletionEnabled(store.isUnusedSchemaDeletionEnabled());
    storeInfo.setBlobTransferEnabled(store.isBlobTransferEnabled());
    storeInfo.setNearlineProducerCompressionEnabled(store.isNearlineProducerCompressionEnabled());
    storeInfo.setNearlineProducerCountPerWriter(store.getNearlineProducerCountPerWriter());
    storeInfo.setTargetRegionSwap(store.getTargetSwapRegion());
    storeInfo.setTargetRegionSwapWaitTime(store.getTargetSwapRegionWaitTime());
    storeInfo.setIsDavinciHeartbeatReported(store.getIsDavinciHeartbeatReported());
    storeInfo.setGlobalRtDivEnabled(store.isGlobalRtDivEnabled());
    return storeInfo;
  }

  /**
   * Store name.
   */
  private String name;
  /**
   * Owner of this store.
   */
  private String owner;
  /**
   * The number of version which is used currently.
   */
  private int currentVersion = 0;

  /**
   * The map represent the current versions in different colos.
   */
  private Map<String, Integer> coloToCurrentVersions;

  /**
   * Highest version number that has been claimed by an upstream (VPJ) system which will create the corresponding kafka topic.
   */
  private int reservedVersion = 0;
  /**
   * Default partition count for all of versions in this store. Once first version is activated, the number will be
   * assigned.
   */
  private int partitionCount = 0;

  /**
   * Low watermark which indicates the last successful incremental push seen by this store.
   */
  private long lowWatermark = 0;

  /**
   * If a store is enableStoreWrites, new version can not be created for it.
   */
  private boolean enableStoreWrites = true;
  /**
   * If a store is enableStoreReads, store has not version available to serve read requests.
   */
  private boolean enableStoreReads = true;
  /**
   * List of non-retired versions.
   */
  private List<Version> versions;

  /**
   * Maximum capacity a store version is able to have
   */
  private long storageQuotaInByte;

  /**
   * Whether a hybrid store will bypass being added db overhead ratio when updating storage quota
   * should only be true when using AdminTool to update storage quota for hybrid stores.
   */
  private boolean hybridStoreOverheadBypass;

  /**
   * Quota for read request hit this store. Measurement is capacity unit.
   */
  private long readQuotaInCU;

  /**
   * Configurations for hybrid stores.
   */
  private HybridStoreConfig hybridStoreConfig;

  /**
   * Store-level ACL switch. When disabled, Venice Router should accept every request.
   */
  private boolean accessControlled = false;

  /**
   * Whether the chunking is enabled, and this is for large value store.
   */
  private boolean chunkingEnabled = false;

  /**
   * Whether the replication metadata chunking is enabled for Active/Active replication enabled store.
   */
  private boolean rmdChunkingEnabled = false;

  /**
   * Whether cache is enabled in Router.
   */
  private boolean singleGetRouterCacheEnabled = false;

  /**
   * Whether batch-get cache is enabled in Router.
   */
  private boolean batchGetRouterCacheEnabled = false;

  /**
   * Batch get limit for current store.
   */
  private int batchGetLimit;

  /**
   * Largest used version number. Topics corresponding to store-versions equal to or lesser than this
   * version number will not trigger new OfflinePushJobs.
   */
  private int largestUsedVersionNumber;

  /**
   * Largest used version number of the RT topic.
   */
  private int largestUsedRTVersionNumber;

  /**
   * a flag to see if the store supports incremental push or not
   */
  private boolean incrementalPushEnabled;

  /**
   * strategies used to compress/decompress Record's value
   */
  private CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;

  /**
   * Enable/Disable client-side record decompression (default: true)
   */
  private boolean clientDecompressionEnabled = true;

  /**
   * How many versions this store preserve at most. By default it's 0 means we use the cluster level config to
   * determine how many version is preserved.
   */
  private int numVersionsToPreserve = NUM_VERSION_PRESERVE_NOT_SET;

  /**
   * Whether or not the store is in the process of migration.
   */
  private boolean migrating = false;

  /**
   * Whether or not write-path computation feature is enabled for this store
   */
  private boolean writeComputationEnabled = false;

  /**
   * RMD (Replication metadata) version ID on the store-level. Default is -1.
   */
  public int replicationMetadataVersionId = -1;

  /**
   * Whether read-path computation is enabled for this store.
   */
  private boolean readComputationEnabled = false;

  /**
   * Maximum number of hours allowed for the store to transition from bootstrap to online state.
   */
  private int bootstrapToOnlineTimeoutInHours = BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS;

  /**
   * Whether native replication should be enabled for this store.
   */
  private boolean nativeReplicationEnabled = false;

  /**
   * Address to the kafka broker which holds the source of truth topic for this store version.
   */
  private String pushStreamSourceAddress = "";

  /**
   * Strategies to store backup versions of a store.
   */
  private BackupStrategy backupStrategy = BackupStrategy.KEEP_MIN_VERSIONS;

  /**
   * Whether or not value schema auto registration from Push job enabled for this store.
   */
  private boolean schemaAutoRegisterFromPushJobEnabled = false;

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

  private ETLStoreConfig etlStoreConfig;
  /**
   * Partitioner info of this store.
   */
  private PartitionerConfig partitionerConfig;

  private long backupVersionRetentionMs;

  private int replicationFactor;

  /**
   * Whether or not the store is a duplicate store in the process of migration.
   */
  private boolean migrationDuplicateStore = false;

  /**
   * The source fabric name to be uses in native replication. Remote consumption will happen from kafka in this fabric.
   */
  private String nativeReplicationSourceFabric = "";

  /**
   * Whether or not metadata system store is enabled for this Venice store.
   */
  private boolean storeMetadataSystemStoreEnabled;

  /**
   * Whether or not meta system store is enabled for this Venice store.
   */
  private boolean storeMetaSystemStoreEnabled;

  /**
   * Whether or not Da Vinci push status system store is enabled for this Venice store.
   */
  private boolean daVinciPushStatusStoreEnabled;

  /**
   * Whether or not active/active replication is currently enabled for this store.
   */
  private boolean activeActiveReplicationEnabled;

  private String kafkaBrokerUrl;

  private Map<String, ViewConfig> viewConfigs = new HashMap<>();

  /**
   * Whether storage node read quota is enabled for this store.
   */
  private boolean storageNodeReadQuotaEnabled;

  /**
   * Reasons for why or why not the store is dead
   */
  private List<String> storeDeadStatusReasons = new ArrayList<>();

  /**
   * flag to indicate if the store is dead
   */
  private boolean isStoreDead;

  private boolean compactionEnabled;

  private long minCompactionLagSeconds;

  private long maxCompactionLagSeconds;

  private int maxRecordSizeBytes = VeniceWriter.UNLIMITED_MAX_RECORD_SIZE;

  private int maxNearlineRecordSizeBytes = VeniceWriter.UNLIMITED_MAX_RECORD_SIZE;

  private boolean unusedSchemaDeletionEnabled;

  private boolean blobTransferEnabled;

  private boolean nearlineProducerCompressionEnabled;
  private int nearlineProducerCountPerWriter;
  private String targetRegionSwap;
  private int targetRegionSwapWaitTime;
  private boolean isDavinciHeartbeatReported;
  private boolean globalRtDivEnabled = false;

  public StoreInfo() {
  }

  /**
   * Store Name
   * @return
   */
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setKafkaBrokerUrl(String kafkaBrokerUrl) {
    this.kafkaBrokerUrl = kafkaBrokerUrl;
  }

  public String getKafkaBrokerUrl() {
    return kafkaBrokerUrl;
  }

  /**
   * Store Owner
   * @return
   */
  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  /**
   * The version of the store which is currently being served
   * @return
   */
  public int getCurrentVersion() {
    return currentVersion;
  }

  public void setCurrentVersion(int currentVersion) {
    this.currentVersion = currentVersion;
  }

  public Map<String, Integer> getColoToCurrentVersions() {
    return coloToCurrentVersions;
  }

  public void setColoToCurrentVersions(Map<String, Integer> coloToCurrentVersions) {
    this.coloToCurrentVersions = coloToCurrentVersions;
  }

  /**
   * The highest version number that has been reserved.
   * Any component that did not reserve a version must create or reserve versions higher than this
   * @return
   */
  public int getReservedVersion() {
    return reservedVersion;
  }

  public void setReservedVersion(int reservedVersion) {
    this.reservedVersion = reservedVersion;
  }

  /**
   * The number of partitions for this store
   * @return
   */
  public int getPartitionCount() {
    return partitionCount;
  }

  public void setPartitionCount(int partitionCount) {
    this.partitionCount = partitionCount;
  }

  public long getLowWatermark() {
    return lowWatermark;
  }

  public void setLowWatermark(long lowWatermark) {
    this.lowWatermark = lowWatermark;
  }

  /**
   * Whether the store is enableStoreWrites, a enableStoreWrites store cannot have new versions pushed
   * @return
   */
  public boolean isEnableStoreWrites() {
    return enableStoreWrites;
  }

  public void setEnableStoreWrites(boolean enableStoreWrites) {
    this.enableStoreWrites = enableStoreWrites;
  }

  public boolean isEnableStoreReads() {
    return enableStoreReads;
  }

  public void setEnableStoreReads(boolean enableStoreReads) {
    this.enableStoreReads = enableStoreReads;
  }

  /**
   * List of available versions for this store
   * @return
   */
  public List<Version> getVersions() {
    return versions;
  }

  public Optional<Version> getVersion(int versionNum) {
    for (Version v: getVersions()) {
      if (v.getNumber() == versionNum) {
        return Optional.of(v);
      }
    }
    return Optional.empty();
  }

  public void setVersions(List<Version> versions) {
    this.versions = versions;
  }

  public long getStorageQuotaInByte() {
    return storageQuotaInByte;
  }

  public void setStorageQuotaInByte(long storageQuotaInByte) {
    this.storageQuotaInByte = storageQuotaInByte;
  }

  public boolean getHybridStoreOverheadBypass() {
    return hybridStoreOverheadBypass;
  }

  public void setHybridStoreOverheadBypass(boolean overheadBypass) {
    this.hybridStoreOverheadBypass = overheadBypass;
  }

  public long getReadQuotaInCU() {
    return readQuotaInCU;
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

  public boolean isAccessControlled() {
    return accessControlled;
  }

  public void setAccessControlled(boolean accessControlled) {
    this.accessControlled = accessControlled;
  }

  public boolean isChunkingEnabled() {
    return chunkingEnabled;
  }

  public void setChunkingEnabled(boolean chunkingEnabled) {
    this.chunkingEnabled = chunkingEnabled;
  }

  public boolean isRmdChunkingEnabled() {
    return rmdChunkingEnabled;
  }

  public void setRmdChunkingEnabled(boolean rmdChunkingEnabled) {
    this.rmdChunkingEnabled = rmdChunkingEnabled;
  }

  public boolean isSingleGetRouterCacheEnabled() {
    return singleGetRouterCacheEnabled;
  }

  public void setSingleGetRouterCacheEnabled(boolean singleGetRouterCacheEnabled) {
    this.singleGetRouterCacheEnabled = singleGetRouterCacheEnabled;
  }

  public boolean isBatchGetRouterCacheEnabled() {
    return batchGetRouterCacheEnabled;
  }

  public void setBatchGetRouterCacheEnabled(boolean batchGetRouterCacheEnabled) {
    this.batchGetRouterCacheEnabled = batchGetRouterCacheEnabled;
  }

  public int getBatchGetLimit() {
    return batchGetLimit;
  }

  public void setBatchGetLimit(int batchGetLimit) {
    this.batchGetLimit = batchGetLimit;
  }

  public int getLargestUsedVersionNumber() {
    return largestUsedVersionNumber;
  }

  public void setLargestUsedVersionNumber(int largestUsedVersionNumber) {
    this.largestUsedVersionNumber = largestUsedVersionNumber;
  }

  public int getLargestUsedRTVersionNumber() {
    return largestUsedRTVersionNumber;
  }

  public void setLargestUsedRTVersionNumber(int largestUsedRTVersionNumber) {
    this.largestUsedRTVersionNumber = largestUsedRTVersionNumber;
  }

  public boolean isIncrementalPushEnabled() {
    return incrementalPushEnabled;
  }

  public void setIncrementalPushEnabled(boolean incrementalPushEnabled) {
    this.incrementalPushEnabled = incrementalPushEnabled;
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

  public int getNumVersionsToPreserve() {
    return numVersionsToPreserve;
  }

  public void setNumVersionsToPreserve(int numVersionsToPreserve) {
    this.numVersionsToPreserve = numVersionsToPreserve;
  }

  public boolean isMigrating() {
    return migrating;
  }

  public void setMigrating(boolean migrating) {
    this.migrating = migrating;
  }

  public boolean isWriteComputationEnabled() {
    return writeComputationEnabled;
  }

  public void setWriteComputationEnabled(boolean writeComputationEnabled) {
    this.writeComputationEnabled = writeComputationEnabled;
  }

  public Map<String, ViewConfig> getViewConfigs() {
    return viewConfigs;
  }

  public void setViewConfigs(Map<String, ViewConfig> viewConfigs) {
    this.viewConfigs = viewConfigs;
  }

  public int getReplicationMetadataVersionId() {
    return replicationMetadataVersionId;
  }

  public void setReplicationMetadataVersionId(int replicationMetadataVersionId) {
    this.replicationMetadataVersionId = replicationMetadataVersionId;
  }

  public boolean isReadComputationEnabled() {
    return readComputationEnabled;
  }

  public void setReadComputationEnabled(boolean readComputationEnabled) {
    this.readComputationEnabled = readComputationEnabled;
  }

  public int getBootstrapToOnlineTimeoutInHours() {
    return bootstrapToOnlineTimeoutInHours;
  }

  public void setBootstrapToOnlineTimeoutInHours(int bootstrapToOnlineTimeoutInHours) {
    this.bootstrapToOnlineTimeoutInHours = bootstrapToOnlineTimeoutInHours;
  }

  public void setBackupStrategy(BackupStrategy value) {
    backupStrategy = value;
  }

  public BackupStrategy getBackupStrategy() {
    return backupStrategy;
  }

  public boolean isSchemaAutoRegisterFromPushJobEnabled() {
    return schemaAutoRegisterFromPushJobEnabled;
  }

  public void setSchemaAutoRegisterFromPushJobEnabled(boolean value) {
    schemaAutoRegisterFromPushJobEnabled = value;
  }

  public boolean isSuperSetSchemaAutoGenerationForReadComputeEnabled() {
    return superSetSchemaAutoGenerationForReadComputeEnabled;
  }

  public void setSuperSetSchemaAutoGenerationForReadComputeEnabled(boolean value) {
    superSetSchemaAutoGenerationForReadComputeEnabled = value;
  }

  public String getPushStreamSourceAddress() {
    return this.pushStreamSourceAddress;
  }

  public void setPushStreamSourceAddress(String sourceAddress) {
    this.pushStreamSourceAddress = sourceAddress;
  }

  public boolean isNativeReplicationEnabled() {
    return this.nativeReplicationEnabled;
  }

  public void setNativeReplicationEnabled(boolean nativeReplicationEnabled) {
    this.nativeReplicationEnabled = nativeReplicationEnabled;
  }

  public void setLatestSuperSetValueSchemaId(int valueSchemaId) {
    latestSuperSetValueSchemaId = valueSchemaId;
  }

  public int getLatestSuperSetValueSchemaId() {
    return latestSuperSetValueSchemaId;
  }

  public boolean isHybridStoreDiskQuotaEnabled() {
    return hybridStoreDiskQuotaEnabled;
  }

  public void setHybridStoreDiskQuotaEnabled(boolean enabled) {
    hybridStoreDiskQuotaEnabled = enabled;
  }

  public ETLStoreConfig getEtlStoreConfig() {
    return etlStoreConfig;
  }

  public void setEtlStoreConfig(ETLStoreConfig etlStoreConfig) {
    this.etlStoreConfig = etlStoreConfig;
  }

  public PartitionerConfig getPartitionerConfig() {
    return partitionerConfig;
  }

  public void setPartitionerConfig(PartitionerConfig partitionerConfig) {
    this.partitionerConfig = partitionerConfig;
  }

  public long getBackupVersionRetentionMs() {
    return backupVersionRetentionMs;
  }

  public void setBackupVersionRetentionMs(long backupVersionRetentionMs) {
    this.backupVersionRetentionMs = backupVersionRetentionMs;
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  public void setReplicationFactor(int replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  public boolean isMigrationDuplicateStore() {
    return migrationDuplicateStore;
  }

  public void setMigrationDuplicateStore(boolean migrationDuplicateStore) {
    this.migrationDuplicateStore = migrationDuplicateStore;
  }

  public String getNativeReplicationSourceFabric() {
    return this.nativeReplicationSourceFabric;
  }

  public void setNativeReplicationSourceFabric(String nativeReplicationSourceFabric) {
    this.nativeReplicationSourceFabric = nativeReplicationSourceFabric;
  }

  public boolean isStoreMetadataSystemStoreEnabled() {
    return storeMetadataSystemStoreEnabled;
  }

  public void setStoreMetadataSystemStoreEnabled(boolean storeMetadataSystemStoreEnabled) {
    this.storeMetadataSystemStoreEnabled = storeMetadataSystemStoreEnabled;
  }

  public boolean isStoreMetaSystemStoreEnabled() {
    return storeMetaSystemStoreEnabled;
  }

  public void setStoreMetaSystemStoreEnabled(boolean storeMetaSystemStoreEnabled) {
    this.storeMetaSystemStoreEnabled = storeMetaSystemStoreEnabled;
  }

  public boolean isDaVinciPushStatusStoreEnabled() {
    return daVinciPushStatusStoreEnabled;
  }

  public void setDaVinciPushStatusStoreEnabled(boolean daVinciPushStatusStoreEnabled) {
    this.daVinciPushStatusStoreEnabled = daVinciPushStatusStoreEnabled;
  }

  public boolean isActiveActiveReplicationEnabled() {
    return activeActiveReplicationEnabled;
  }

  public void setActiveActiveReplicationEnabled(boolean activeActiveReplicationEnabled) {
    this.activeActiveReplicationEnabled = activeActiveReplicationEnabled;
  }

  public boolean isStorageNodeReadQuotaEnabled() {
    return storageNodeReadQuotaEnabled;
  }

  public void setStorageNodeReadQuotaEnabled(boolean storageNodeReadQuotaEnabled) {
    this.storageNodeReadQuotaEnabled = storageNodeReadQuotaEnabled;
  }

  public boolean isCompactionEnabled() {
    return this.compactionEnabled;
  }

  public void setCompactionEnabled(boolean compactionEnabled) {
    this.compactionEnabled = compactionEnabled;
  }

  public long getMinCompactionLagSeconds() {
    return minCompactionLagSeconds;
  }

  public void setMinCompactionLagSeconds(long minCompactionLagSeconds) {
    this.minCompactionLagSeconds = minCompactionLagSeconds;
  }

  public long getMaxCompactionLagSeconds() {
    return maxCompactionLagSeconds;
  }

  public void setMaxCompactionLagSeconds(long maxCompactionLagSeconds) {
    this.maxCompactionLagSeconds = maxCompactionLagSeconds;
  }

  public int getMaxRecordSizeBytes() {
    return this.maxRecordSizeBytes;
  }

  public void setMaxRecordSizeBytes(int maxRecordSizeBytes) {
    this.maxRecordSizeBytes = maxRecordSizeBytes;
  }

  public int getMaxNearlineRecordSizeBytes() {
    return this.maxNearlineRecordSizeBytes;
  }

  public void setMaxNearlineRecordSizeBytes(int maxNearlineRecordSizeBytes) {
    this.maxNearlineRecordSizeBytes = maxNearlineRecordSizeBytes;
  }

  public void setUnusedSchemaDeletionEnabled(boolean unusedSchemaDeletionEnabled) {
    this.unusedSchemaDeletionEnabled = unusedSchemaDeletionEnabled;
  }

  public boolean isUnusedSchemaDeletionEnabled() {
    return this.unusedSchemaDeletionEnabled;
  }

  public void setBlobTransferEnabled(boolean blobTransferEnabled) {
    this.blobTransferEnabled = blobTransferEnabled;
  }

  public boolean isBlobTransferEnabled() {
    return this.blobTransferEnabled;
  }

  public boolean isNearlineProducerCompressionEnabled() {
    return nearlineProducerCompressionEnabled;
  }

  public void setNearlineProducerCompressionEnabled(boolean nearlineProducerCompressionEnabled) {
    this.nearlineProducerCompressionEnabled = nearlineProducerCompressionEnabled;
  }

  public int getNearlineProducerCountPerWriter() {
    return nearlineProducerCountPerWriter;
  }

  public void setNearlineProducerCountPerWriter(int nearlineProducerCountPerWriter) {
    this.nearlineProducerCountPerWriter = nearlineProducerCountPerWriter;
  }

  public String getTargetRegionSwap() {
    return this.targetRegionSwap;
  }

  public void setTargetRegionSwap(String targetRegion) {
    this.targetRegionSwap = targetRegion;
  }

  public int getTargetRegionSwapWaitTime() {
    return this.targetRegionSwapWaitTime;
  }

  public void setTargetRegionSwapWaitTime(int waitTime) {
    this.targetRegionSwapWaitTime = waitTime;
  }

  public void setIsDavinciHeartbeatReported(boolean isReported) {
    this.isDavinciHeartbeatReported = isReported;
  }

  public boolean getIsDavinciHeartbeatReported() {
    return this.isDavinciHeartbeatReported;
  }

  public void setIsStoreDead(boolean isStoreDead) {
    this.isStoreDead = isStoreDead;
  }

  public boolean getIsStoreDead() {
    return this.isStoreDead;
  }

  public void setStoreDeadStatusReasons(List<String> reasons) {
    this.storeDeadStatusReasons = reasons == null ? Collections.emptyList() : new ArrayList<>(reasons);
  }

  public List<String> getStoreDeadStatusReasons() {
    return storeDeadStatusReasons;
  }

  public void setGlobalRtDivEnabled(boolean globalRtDivEnabled) {
    this.globalRtDivEnabled = globalRtDivEnabled;
  }

  public boolean isGlobalRtDivEnabled() {
    return this.globalRtDivEnabled;
  }
}
