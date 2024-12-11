package com.linkedin.venice.controllerapi;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.ACCESS_CONTROLLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ACTIVE_ACTIVE_REPLICATION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.AMPLIFICATION_FACTOR;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.AUTO_SCHEMA_REGISTER_FOR_PUSHJOB_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BACKUP_STRATEGY;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BACKUP_VERSION_RETENTION_MS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BATCH_GET_LIMIT;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BLOB_TRANSFER_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BUFFER_REPLAY_POLICY;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.CHUNKING_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLIENT_DECOMPRESSION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.COMPRESSION_STRATEGY;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.DATA_REPLICATION_POLICY;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.DISABLE_DAVINCI_PUSH_STATUS_STORE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.DISABLE_META_STORE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.DISABLE_STORE_VIEW;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ENABLE_READS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ENABLE_STORE_MIGRATION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ENABLE_WRITES;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ETLED_PROXY_USER_ACCOUNT;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.FUTURE_VERSION_ETL_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.HYBRID_STORE_DISK_QUOTA_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.HYBRID_STORE_OVERHEAD_BYPASS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.INCREMENTAL_PUSH_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.IS_DAVINCI_HEARTBEAT_REPORTED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.LARGEST_USED_VERSION_NUMBER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.LATEST_SUPERSET_SCHEMA_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.MAX_COMPACTION_LAG_SECONDS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.MAX_NEARLINE_RECORD_SIZE_BYTES;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.MAX_RECORD_SIZE_BYTES;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.MIGRATION_DUPLICATE_STORE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.MIN_COMPACTION_LAG_SECONDS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NATIVE_REPLICATION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NATIVE_REPLICATION_SOURCE_FABRIC;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NEARLINE_PRODUCER_COMPRESSION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NEARLINE_PRODUCER_COUNT_PER_WRITER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NUM_VERSIONS_TO_PRESERVE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OFFSET_LAG_TO_GO_ONLINE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OWNER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PARTITIONER_CLASS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PARTITIONER_PARAMS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PARTITION_COUNT;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PERSONA_NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PUSH_STREAM_SOURCE_ADDRESS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.READ_COMPUTATION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.READ_QUOTA_IN_CU;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REAL_TIME_TOPIC_NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REGIONS_FILTER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REGULAR_VERSION_ETL_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REPLICATE_ALL_CONFIGS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REPLICATION_FACTOR;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REPLICATION_METADATA_PROTOCOL_VERSION_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REWIND_TIME_IN_SECONDS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.RMD_CHUNKING_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SEPARATE_REAL_TIME_TOPIC_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORAGE_NODE_READ_QUOTA_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORAGE_QUOTA_IN_BYTE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_MIGRATION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_VIEW;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_VIEW_CLASS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_VIEW_NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_VIEW_PARAMS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TARGET_SWAP_REGION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TARGET_SWAP_REGION_WAIT_TIME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TIME_LAG_TO_GO_ONLINE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.UNUSED_SCHEMA_DELETION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.UPDATED_CONFIGS_LIST;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VERSION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.WRITE_COMPUTATION_ENABLED;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.ETLStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;


public class UpdateStoreQueryParams extends QueryParams {
  public UpdateStoreQueryParams(Map<String, String> initialParams) {
    super(initialParams);
  }

  public UpdateStoreQueryParams() {
    super();
    /**
     * By default, parent controllers will not replicate unchanged store configs to child controllers.
     */
    setReplicateAllConfigs(false);
  }

  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  /**
   * This method must be updated everytime a new store property is introduced
   * @param srcStore The original store
   */
  public UpdateStoreQueryParams(StoreInfo srcStore, boolean storeMigrating) {
    // Copy everything except for currentVersion, daVinciPushStatusStoreEnabled, latestSuperSetValueSchemaId,
    // storeMetaSystemStoreEnabled, storeMetadataSystemStoreEnabled
    UpdateStoreQueryParams updateStoreQueryParams =
        new UpdateStoreQueryParams().setAccessControlled(srcStore.isAccessControlled())
            .setActiveActiveReplicationEnabled(srcStore.isActiveActiveReplicationEnabled())
            .setBackupStrategy(srcStore.getBackupStrategy())
            .setBackupVersionRetentionMs(srcStore.getBackupVersionRetentionMs())
            .setBatchGetLimit(srcStore.getBatchGetLimit())
            .setBootstrapToOnlineTimeoutInHours(srcStore.getBootstrapToOnlineTimeoutInHours())
            .setChunkingEnabled(srcStore.isChunkingEnabled())
            .setRmdChunkingEnabled(srcStore.isRmdChunkingEnabled())
            .setClientDecompressionEnabled(srcStore.getClientDecompressionEnabled())
            .setCompressionStrategy(srcStore.getCompressionStrategy())
            .setEnableReads(srcStore.isEnableStoreReads())
            .setEnableWrites(srcStore.isEnableStoreWrites())
            .setHybridStoreDiskQuotaEnabled(srcStore.isHybridStoreDiskQuotaEnabled())
            .setIncrementalPushEnabled(srcStore.isIncrementalPushEnabled())
            .setLargestUsedVersionNumber(srcStore.getLargestUsedVersionNumber())
            .setNativeReplicationEnabled(srcStore.isNativeReplicationEnabled())
            .setNativeReplicationSourceFabric(srcStore.getNativeReplicationSourceFabric())
            .setNumVersionsToPreserve(srcStore.getNumVersionsToPreserve())
            .setOwner(srcStore.getOwner())
            .setPartitionCount(srcStore.getPartitionCount())
            .setPushStreamSourceAddress(srcStore.getPushStreamSourceAddress())
            .setReadComputationEnabled(srcStore.isReadComputationEnabled())
            .setReadQuotaInCU(srcStore.getReadQuotaInCU())
            .setReplicationFactor(srcStore.getReplicationFactor())
            .setAutoSchemaPushJobEnabled(srcStore.isSchemaAutoRegisterFromPushJobEnabled())
            .setStorageQuotaInByte(srcStore.getStorageQuotaInByte())
            .setWriteComputationEnabled(srcStore.isWriteComputationEnabled())
            .setStorageNodeReadQuotaEnabled(srcStore.isStorageNodeReadQuotaEnabled())
            .setBlobTransferEnabled(srcStore.isBlobTransferEnabled())
            .setMaxRecordSizeBytes(srcStore.getMaxRecordSizeBytes())
            .setMaxNearlineRecordSizeBytes(srcStore.getMaxNearlineRecordSizeBytes())
            .setTargetRegionSwap(srcStore.getTargetRegionSwap())
            .setTargetRegionSwapWaitTime(srcStore.getTargetRegionSwapWaitTime())
            // TODO: This needs probably some refinement, but since we only support one kind of view type today, this is
            // still easy to parse
            .setStoreViews(
                srcStore.getViewConfigs()
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())));

    if (srcStore.getReplicationMetadataVersionId() != -1) {
      updateStoreQueryParams.setReplicationMetadataVersionID(srcStore.getReplicationMetadataVersionId());
    }

    if (storeMigrating) {
      updateStoreQueryParams.setLargestUsedVersionNumber(0) // Decrease the largestUsedVersionNumber to trigger
                                                            // bootstrap in dest cluster
          .setStoreMigration(true)
          .setMigrationDuplicateStore(true); // Mark as duplicate store, to which L/F SN refers to avoid multi leaders
    }

    ETLStoreConfig etlStoreConfig = srcStore.getEtlStoreConfig();
    if (etlStoreConfig != null) {
      updateStoreQueryParams.setEtledProxyUserAccount(etlStoreConfig.getEtledUserProxyAccount());
      updateStoreQueryParams.setRegularVersionETLEnabled(etlStoreConfig.isRegularVersionETLEnabled());
      updateStoreQueryParams.setFutureVersionETLEnabled(etlStoreConfig.isFutureVersionETLEnabled());
    }

    HybridStoreConfig hybridStoreConfig = srcStore.getHybridStoreConfig();
    if (hybridStoreConfig != null) {
      updateStoreQueryParams.setHybridOffsetLagThreshold(hybridStoreConfig.getOffsetLagThresholdToGoOnline());
      updateStoreQueryParams.setHybridRewindSeconds(hybridStoreConfig.getRewindTimeInSeconds());
      updateStoreQueryParams
          .setHybridTimeLagThreshold(hybridStoreConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds());
      updateStoreQueryParams.setHybridDataReplicationPolicy(hybridStoreConfig.getDataReplicationPolicy());
      updateStoreQueryParams.setHybridBufferReplayPolicy(hybridStoreConfig.getBufferReplayPolicy());
    }

    PartitionerConfig partitionerConfig = srcStore.getPartitionerConfig();
    if (partitionerConfig != null) {
      updateStoreQueryParams.setPartitionerClass(partitionerConfig.getPartitionerClass());
      updateStoreQueryParams.setPartitionerParams(partitionerConfig.getPartitionerParams());
      updateStoreQueryParams.setAmplificationFactor(partitionerConfig.getAmplificationFactor());
    }

    this.params.putAll(updateStoreQueryParams.params);
  }

  public boolean isDifferent(UpdateStoreQueryParams newParams) {
    boolean isDifferent = false;
    for (Map.Entry<String, String> entry: newParams.params.entrySet()) {
      if (!Objects.equals(this.params.get(entry.getKey()), entry.getValue())) {
        isDifferent = true;
      }
    }
    return isDifferent;
  }

  public UpdateStoreQueryParams setOwner(String owner) {
    params.put(OWNER, owner);
    return this;
  }

  public Optional<String> getOwner() {
    return Optional.ofNullable(params.get(OWNER));
  }

  public UpdateStoreQueryParams setPartitionCount(int partitionCount) {
    return putInteger(PARTITION_COUNT, partitionCount);
  }

  public Optional<Integer> getPartitionCount() {
    return getInteger(PARTITION_COUNT);
  }

  public UpdateStoreQueryParams setPartitionerClass(String partitionerClass) {
    return putString(PARTITIONER_CLASS, partitionerClass);
  }

  public Optional<String> getPartitionerClass() {
    return getString(PARTITIONER_CLASS);
  }

  public UpdateStoreQueryParams setPartitionerParams(Map<String, String> partitionerParams) {
    return (UpdateStoreQueryParams) putStringMap(PARTITIONER_PARAMS, partitionerParams);
  }

  public Optional<Map<String, String>> getPartitionerParams() {
    return getStringMap(PARTITIONER_PARAMS);
  }

  public UpdateStoreQueryParams setAmplificationFactor(int amplificationFactor) {
    return putInteger(AMPLIFICATION_FACTOR, amplificationFactor);
  }

  public Optional<Integer> getAmplificationFactor() {
    return getInteger(AMPLIFICATION_FACTOR);
  }

  public UpdateStoreQueryParams setCurrentVersion(int currentVersion) {
    return putInteger(VERSION, currentVersion);
  }

  public Optional<Integer> getCurrentVersion() {
    return getInteger(VERSION);
  }

  public UpdateStoreQueryParams setLargestUsedVersionNumber(int largestUsedVersionNumber) {
    return putInteger(LARGEST_USED_VERSION_NUMBER, largestUsedVersionNumber);
  }

  public Optional<Integer> getLargestUsedVersionNumber() {
    return getInteger(LARGEST_USED_VERSION_NUMBER);
  }

  public UpdateStoreQueryParams setEnableReads(boolean enableReads) {
    return putBoolean(ENABLE_READS, enableReads);
  }

  public Optional<Boolean> getEnableReads() {
    return getBoolean(ENABLE_READS);
  }

  public UpdateStoreQueryParams setEnableWrites(boolean enableWrites) {
    return putBoolean(ENABLE_WRITES, enableWrites);
  }

  public UpdateStoreQueryParams setDisableMetaStore() {
    return putBoolean(DISABLE_META_STORE, true);
  }

  public UpdateStoreQueryParams setDisableDavinciPushStatusStore() {
    return putBoolean(DISABLE_DAVINCI_PUSH_STATUS_STORE, true);
  }

  public Optional<Boolean> disableMetaStore() {
    return getBoolean(DISABLE_META_STORE);
  }

  public Optional<Boolean> disableDavinciPushStatusStore() {
    return getBoolean(DISABLE_DAVINCI_PUSH_STATUS_STORE);
  }

  public Optional<Boolean> getEnableWrites() {
    return getBoolean(ENABLE_WRITES);
  }

  public UpdateStoreQueryParams setStorageQuotaInByte(long storageQuotaInByte) {
    return putLong(STORAGE_QUOTA_IN_BYTE, storageQuotaInByte);
  }

  public Optional<Long> getStorageQuotaInByte() {
    return getLong(STORAGE_QUOTA_IN_BYTE);
  }

  public UpdateStoreQueryParams setHybridStoreOverheadBypass(boolean overheadBypass) {
    return putBoolean(HYBRID_STORE_OVERHEAD_BYPASS, overheadBypass);
  }

  public Optional<Boolean> getHybridStoreDiskQuotaEnabled() {
    return getBoolean(HYBRID_STORE_DISK_QUOTA_ENABLED);
  }

  public UpdateStoreQueryParams setHybridStoreDiskQuotaEnabled(boolean enabled) {
    return putBoolean(HYBRID_STORE_DISK_QUOTA_ENABLED, enabled);
  }

  public Optional<Boolean> getHybridStoreOverheadBypass() {
    return getBoolean(HYBRID_STORE_OVERHEAD_BYPASS);
  }

  public UpdateStoreQueryParams setReadQuotaInCU(long readQuotaInCU) {
    return putLong(READ_QUOTA_IN_CU, readQuotaInCU);
  }

  public Optional<Long> getReadQuotaInCU() {
    return getLong(READ_QUOTA_IN_CU);
  }

  public UpdateStoreQueryParams setHybridRewindSeconds(long hybridRewindSeconds) {
    return putLong(REWIND_TIME_IN_SECONDS, hybridRewindSeconds);
  }

  public Optional<Long> getHybridRewindSeconds() {
    return getLong(REWIND_TIME_IN_SECONDS);
  }

  public UpdateStoreQueryParams setHybridOffsetLagThreshold(long hybridOffsetLagThreshold) {
    return putLong(OFFSET_LAG_TO_GO_ONLINE, hybridOffsetLagThreshold);
  }

  public Optional<Long> getHybridOffsetLagThreshold() {
    return getLong(OFFSET_LAG_TO_GO_ONLINE);
  }

  public UpdateStoreQueryParams setHybridTimeLagThreshold(long hybridTimeLagThreshold) {
    return putLong(TIME_LAG_TO_GO_ONLINE, hybridTimeLagThreshold);
  }

  public Optional<Long> getHybridTimeLagThreshold() {
    return getLong(TIME_LAG_TO_GO_ONLINE);
  }

  public UpdateStoreQueryParams setHybridDataReplicationPolicy(DataReplicationPolicy dataReplicationPolicy) {
    params.put(DATA_REPLICATION_POLICY, dataReplicationPolicy.name());
    return this;
  }

  public Optional<DataReplicationPolicy> getHybridDataReplicationPolicy() {
    return Optional.ofNullable(params.get(DATA_REPLICATION_POLICY)).map(DataReplicationPolicy::valueOf);
  }

  public UpdateStoreQueryParams setHybridBufferReplayPolicy(BufferReplayPolicy bufferReplayPolicy) {
    params.put(BUFFER_REPLAY_POLICY, bufferReplayPolicy.name());
    return this;
  }

  public Optional<BufferReplayPolicy> getHybridBufferReplayPolicy() {
    return Optional.ofNullable(params.get(BUFFER_REPLAY_POLICY)).map(BufferReplayPolicy::valueOf);
  }

  public UpdateStoreQueryParams setRealTimeTopicName(String realTimeTopicName) {
    return putString(REAL_TIME_TOPIC_NAME, realTimeTopicName);
  }

  public Optional<String> getRealTimeTopicName() {
    return getString(REAL_TIME_TOPIC_NAME);
  }

  public UpdateStoreQueryParams setAccessControlled(boolean accessControlled) {
    return putBoolean(ACCESS_CONTROLLED, accessControlled);
  }

  public Optional<Boolean> getAccessControlled() {
    return getBoolean(ACCESS_CONTROLLED);
  }

  public UpdateStoreQueryParams setCompressionStrategy(CompressionStrategy compressionStrategy) {
    params.put(COMPRESSION_STRATEGY, compressionStrategy.name());
    return this;
  }

  public Optional<CompressionStrategy> getCompressionStrategy() {
    return Optional.ofNullable(params.get(COMPRESSION_STRATEGY)).map(CompressionStrategy::valueOf);
  }

  public UpdateStoreQueryParams setClientDecompressionEnabled(boolean clientDecompressionEnabled) {
    putBoolean(CLIENT_DECOMPRESSION_ENABLED, clientDecompressionEnabled);
    return this;
  }

  public Optional<Boolean> getClientDecompressionEnabled() {
    return getBoolean(CLIENT_DECOMPRESSION_ENABLED);
  }

  public UpdateStoreQueryParams setChunkingEnabled(boolean chunkingEnabled) {
    return putBoolean(CHUNKING_ENABLED, chunkingEnabled);
  }

  public Optional<Boolean> getChunkingEnabled() {
    return getBoolean(CHUNKING_ENABLED);
  }

  public UpdateStoreQueryParams setRmdChunkingEnabled(boolean rmdChunkingEnabled) {
    return putBoolean(RMD_CHUNKING_ENABLED, rmdChunkingEnabled);
  }

  public Optional<Boolean> getRmdChunkingEnabled() {
    return getBoolean(RMD_CHUNKING_ENABLED);
  }

  public UpdateStoreQueryParams setIncrementalPushEnabled(boolean incrementalPushEnabled) {
    return putBoolean(INCREMENTAL_PUSH_ENABLED, incrementalPushEnabled);
  }

  public Optional<Boolean> getIncrementalPushEnabled() {
    return getBoolean(INCREMENTAL_PUSH_ENABLED);
  }

  public UpdateStoreQueryParams setSeparateRealTimeTopicEnabled(boolean separateRealTimeTopicEnabled) {
    return putBoolean(SEPARATE_REAL_TIME_TOPIC_ENABLED, separateRealTimeTopicEnabled);
  }

  public Optional<Boolean> getSeparateRealTimeTopicEnabled() {
    return getBoolean(SEPARATE_REAL_TIME_TOPIC_ENABLED);
  }

  public UpdateStoreQueryParams setBatchGetLimit(int batchGetLimit) {
    return putInteger(BATCH_GET_LIMIT, batchGetLimit);
  }

  public Optional<Integer> getBatchGetLimit() {
    return getInteger(BATCH_GET_LIMIT);
  }

  public UpdateStoreQueryParams setNumVersionsToPreserve(int numVersionsToPreserve) {
    return putInteger(NUM_VERSIONS_TO_PRESERVE, numVersionsToPreserve);
  }

  public Optional<Integer> getNumVersionsToPreserve() {
    return getInteger(NUM_VERSIONS_TO_PRESERVE);
  }

  public UpdateStoreQueryParams setStoreMigration(boolean migrating) {
    return putBoolean(STORE_MIGRATION, migrating).putBoolean(ENABLE_STORE_MIGRATION, migrating);
  }

  public Optional<Boolean> getStoreMigration() {
    Optional<Boolean> storeMigration = getBoolean(ENABLE_STORE_MIGRATION);
    if (storeMigration.isPresent()) {
      return storeMigration;
    }
    return getBoolean(STORE_MIGRATION);
  }

  public UpdateStoreQueryParams setWriteComputationEnabled(boolean writeComputationEnabled) {
    return putBoolean(WRITE_COMPUTATION_ENABLED, writeComputationEnabled);
  }

  public Optional<Boolean> getWriteComputationEnabled() {
    return getBoolean(WRITE_COMPUTATION_ENABLED);
  }

  public UpdateStoreQueryParams setReplicationMetadataVersionID(int replicationMetadataVersionID) {
    return putInteger(REPLICATION_METADATA_PROTOCOL_VERSION_ID, replicationMetadataVersionID);
  }

  public Optional<Integer> getReplicationMetadataVersionID() {
    return getInteger(REPLICATION_METADATA_PROTOCOL_VERSION_ID);
  }

  public UpdateStoreQueryParams setReadComputationEnabled(boolean readComputationEnabled) {
    return putBoolean(READ_COMPUTATION_ENABLED, readComputationEnabled);
  }

  public Optional<Boolean> getReadComputationEnabled() {
    return getBoolean(READ_COMPUTATION_ENABLED);
  }

  public UpdateStoreQueryParams setBootstrapToOnlineTimeoutInHours(int bootstrapToOnlineTimeoutInHours) {
    return putInteger(BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS, bootstrapToOnlineTimeoutInHours);
  }

  public Optional<Integer> getBootstrapToOnlineTimeoutInHours() {
    return getInteger(BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS);
  }

  public UpdateStoreQueryParams setNativeReplicationEnabled(boolean nativeReplicationEnabled) {
    return putBoolean(NATIVE_REPLICATION_ENABLED, nativeReplicationEnabled);
  }

  public UpdateStoreQueryParams setStoreViews(Map<String, String> viewMap) {
    return (UpdateStoreQueryParams) putStringMap(STORE_VIEW, viewMap);
  }

  public Optional<Map<String, String>> getStoreViews() {
    return getStringMap(STORE_VIEW);
  }

  public UpdateStoreQueryParams setPushStreamSourceAddress(String pushStreamSourceAddress) {
    return putString(PUSH_STREAM_SOURCE_ADDRESS, pushStreamSourceAddress);
  }

  public UpdateStoreQueryParams setAutoSchemaPushJobEnabled(boolean autoSchemaPushJobEnabled) {
    return putBoolean(AUTO_SCHEMA_REGISTER_FOR_PUSHJOB_ENABLED, autoSchemaPushJobEnabled);
  }

  public Optional<Boolean> getAutoSchemaRegisterPushJobEnabled() {
    return getBoolean(AUTO_SCHEMA_REGISTER_FOR_PUSHJOB_ENABLED);
  }

  public UpdateStoreQueryParams setBackupStrategy(BackupStrategy backupStrategy) {
    params.put(BACKUP_STRATEGY, backupStrategy.name());
    return this;
  }

  public Optional<BackupStrategy> getBackupStrategy() {
    return Optional.ofNullable(params.get(BACKUP_STRATEGY)).map(BackupStrategy::valueOf);
  }

  public UpdateStoreQueryParams setRegularVersionETLEnabled(boolean regularVersionETLEnabled) {
    return putBoolean(REGULAR_VERSION_ETL_ENABLED, regularVersionETLEnabled);
  }

  public Optional<Boolean> getRegularVersionETLEnabled() {
    return getBoolean(REGULAR_VERSION_ETL_ENABLED);
  }

  public UpdateStoreQueryParams setFutureVersionETLEnabled(boolean futureVersionETLEnabled) {
    return putBoolean(FUTURE_VERSION_ETL_ENABLED, futureVersionETLEnabled);
  }

  public Optional<Boolean> getFutureVersionETLEnabled() {
    return getBoolean(FUTURE_VERSION_ETL_ENABLED);
  }

  public UpdateStoreQueryParams setEtledProxyUserAccount(String etledProxyUserAccount) {
    params.put(ETLED_PROXY_USER_ACCOUNT, etledProxyUserAccount);
    return this;
  }

  public Optional<String> getETLedProxyUserAccount() {
    return Optional.ofNullable(params.get(ETLED_PROXY_USER_ACCOUNT));
  }

  public Optional<Boolean> getNativeReplicationEnabled() {
    return getBoolean(NATIVE_REPLICATION_ENABLED);
  }

  public Optional<String> getPushStreamSourceAddress() {
    return getString(PUSH_STREAM_SOURCE_ADDRESS);
  }

  public UpdateStoreQueryParams setBackupVersionRetentionMs(long backupVersionRetentionMs) {
    putLong(BACKUP_VERSION_RETENTION_MS, backupVersionRetentionMs);
    return this;
  }

  public Optional<Long> getBackupVersionRetentionMs() {
    return getLong(BACKUP_VERSION_RETENTION_MS);
  }

  public UpdateStoreQueryParams setReplicationFactor(int replicationFactor) {
    putInteger(REPLICATION_FACTOR, replicationFactor);
    return this;
  }

  public Optional<Integer> getReplicationFactor() {
    return getInteger(REPLICATION_FACTOR);
  }

  public UpdateStoreQueryParams setMigrationDuplicateStore(boolean migrationDuplicateStore) {
    return putBoolean(MIGRATION_DUPLICATE_STORE, migrationDuplicateStore);
  }

  public Optional<Boolean> getMigrationDuplicateStore() {
    return getBoolean(MIGRATION_DUPLICATE_STORE);
  }

  public UpdateStoreQueryParams setNativeReplicationSourceFabric(String nativeReplicationSourceFabric) {
    return putString(NATIVE_REPLICATION_SOURCE_FABRIC, nativeReplicationSourceFabric);
  }

  public Optional<String> getNativeReplicationSourceFabric() {
    return getString(NATIVE_REPLICATION_SOURCE_FABRIC);
  }

  public UpdateStoreQueryParams setUpdatedConfigsList(List<String> updatedConfigsList) {
    return putStringList(UPDATED_CONFIGS_LIST, updatedConfigsList);
  }

  public Optional<List<String>> getUpdatedConfigsList() {
    return getStringList(UPDATED_CONFIGS_LIST);
  }

  public UpdateStoreQueryParams setReplicateAllConfigs(boolean replicateAllConfigs) {
    return putBoolean(REPLICATE_ALL_CONFIGS, replicateAllConfigs);
  }

  public Optional<Boolean> getReplicateAllConfigs() {
    return getBoolean(REPLICATE_ALL_CONFIGS);
  }

  public UpdateStoreQueryParams setActiveActiveReplicationEnabled(boolean activeActiveReplicationEnabled) {
    return putBoolean(ACTIVE_ACTIVE_REPLICATION_ENABLED, activeActiveReplicationEnabled);
  }

  public Optional<Boolean> getActiveActiveReplicationEnabled() {
    return getBoolean(ACTIVE_ACTIVE_REPLICATION_ENABLED);
  }

  public void cloneConfig(String configKey, UpdateStoreQueryParams sourceParams) {
    this.params.put(configKey, sourceParams.params.get(configKey));
  }

  public UpdateStoreQueryParams setRegionsFilter(String regionsFilter) {
    return putString(REGIONS_FILTER, regionsFilter);
  }

  public Optional<String> getRegionsFilter() {
    return getString(REGIONS_FILTER);
  }

  public UpdateStoreQueryParams setStoragePersona(String personaName) {
    return putString(PERSONA_NAME, personaName);
  }

  public Optional<String> getStoragePersona() {
    return getString(PERSONA_NAME);
  }

  public UpdateStoreQueryParams setLatestSupersetSchemaId(int latestSupersetSchemaId) {
    return putInteger(LATEST_SUPERSET_SCHEMA_ID, latestSupersetSchemaId);
  }

  public Optional<Integer> getLatestSupersetSchemaId() {
    return getInteger(LATEST_SUPERSET_SCHEMA_ID);
  }

  public UpdateStoreQueryParams setStorageNodeReadQuotaEnabled(boolean storageNodeReadQuotaEnabled) {
    return putBoolean(STORAGE_NODE_READ_QUOTA_ENABLED, storageNodeReadQuotaEnabled);
  }

  public Optional<Boolean> getStorageNodeReadQuotaEnabled() {
    return getBoolean(STORAGE_NODE_READ_QUOTA_ENABLED);
  }

  public UpdateStoreQueryParams setMinCompactionLagSeconds(long minCompactionLagSeconds) {
    return putLong(MIN_COMPACTION_LAG_SECONDS, minCompactionLagSeconds);
  }

  public Optional<Long> getMinCompactionLagSeconds() {
    return getLong(MIN_COMPACTION_LAG_SECONDS);
  }

  public Optional<String> getViewName() {
    return getString(STORE_VIEW_NAME);
  }

  public UpdateStoreQueryParams setViewName(String viewName) {
    return (UpdateStoreQueryParams) add(STORE_VIEW_NAME, viewName);
  }

  public Optional<String> getViewClassName() {
    return getString(STORE_VIEW_CLASS);
  }

  public UpdateStoreQueryParams setViewClassName(String viewClassName) {
    return (UpdateStoreQueryParams) add(STORE_VIEW_CLASS, viewClassName);
  }

  public Optional<Map<String, String>> getViewClassParams() {
    return getStringMap(STORE_VIEW_PARAMS);
  }

  public UpdateStoreQueryParams setViewClassParams(Map<String, String> partitionerParams) {
    return (UpdateStoreQueryParams) putStringMap(STORE_VIEW_PARAMS, partitionerParams);
  }

  public Optional<Boolean> getDisableStoreView() {
    return getBoolean(DISABLE_STORE_VIEW);
  }

  public UpdateStoreQueryParams setDisableStoreView() {
    return (UpdateStoreQueryParams) add(DISABLE_STORE_VIEW, true);
  }

  public UpdateStoreQueryParams setMaxCompactionLagSeconds(long maxCompactionLagSeconds) {
    return putLong(MAX_COMPACTION_LAG_SECONDS, maxCompactionLagSeconds);
  }

  public Optional<Long> getMaxCompactionLagSeconds() {
    return getLong(MAX_COMPACTION_LAG_SECONDS);
  }

  public UpdateStoreQueryParams setMaxRecordSizeBytes(int maxRecordSizeBytes) {
    return putInteger(MAX_RECORD_SIZE_BYTES, maxRecordSizeBytes);
  }

  public Optional<Integer> getMaxRecordSizeBytes() {
    return getInteger(MAX_RECORD_SIZE_BYTES);
  }

  public UpdateStoreQueryParams setMaxNearlineRecordSizeBytes(int maxNearlineRecordSizeBytes) {
    return putInteger(MAX_NEARLINE_RECORD_SIZE_BYTES, maxNearlineRecordSizeBytes);
  }

  public Optional<Integer> getMaxNearlineRecordSizeBytes() {
    return getInteger(MAX_NEARLINE_RECORD_SIZE_BYTES);
  }

  public UpdateStoreQueryParams setUnusedSchemaDeletionEnabled(boolean unusedSchemaDeletionEnabled) {
    return putBoolean(UNUSED_SCHEMA_DELETION_ENABLED, unusedSchemaDeletionEnabled);
  }

  public Optional<Boolean> getUnusedSchemaDeletionEnabled() {
    return getBoolean(UNUSED_SCHEMA_DELETION_ENABLED);
  }

  public UpdateStoreQueryParams setBlobTransferEnabled(boolean blobTransferEnabled) {
    return putBoolean(BLOB_TRANSFER_ENABLED, blobTransferEnabled);
  }

  public Optional<Boolean> getBlobTransferEnabled() {
    return getBoolean(BLOB_TRANSFER_ENABLED);
  }

  public UpdateStoreQueryParams setNearlineProducerCompressionEnabled(boolean compressionEnabled) {
    return putBoolean(NEARLINE_PRODUCER_COMPRESSION_ENABLED, compressionEnabled);
  }

  public Optional<Boolean> getNearlineProducerCompressionEnabled() {
    return getBoolean(NEARLINE_PRODUCER_COMPRESSION_ENABLED);
  }

  public UpdateStoreQueryParams setNearlineProducerCountPerWriter(int producerCnt) {
    return putInteger(NEARLINE_PRODUCER_COUNT_PER_WRITER, producerCnt);
  }

  public Optional<Integer> getNearlineProducerCountPerWriter() {
    return getInteger(NEARLINE_PRODUCER_COUNT_PER_WRITER);
  }

  public UpdateStoreQueryParams setTargetRegionSwap(String targetRegion) {
    return putString(TARGET_SWAP_REGION, targetRegion);
  }

  public Optional<String> getTargetSwapRegion() {
    return getString(TARGET_SWAP_REGION);
  }

  public UpdateStoreQueryParams setTargetRegionSwapWaitTime(int waitTime) {
    return putInteger(TARGET_SWAP_REGION_WAIT_TIME, waitTime);
  }

  public Optional<Integer> getTargetRegionSwapWaitTime() {
    return getInteger(TARGET_SWAP_REGION_WAIT_TIME);
  }

  public UpdateStoreQueryParams setIsDavinciHeartbeatReported(boolean isReported) {
    return putBoolean(IS_DAVINCI_HEARTBEAT_REPORTED, isReported);
  }

  public Optional<Boolean> getIsDavinciHeartbeatReported() {
    return getBoolean(IS_DAVINCI_HEARTBEAT_REPORTED);
  }

  // ***************** above this line are getters and setters *****************
  private UpdateStoreQueryParams putInteger(String name, int value) {
    return (UpdateStoreQueryParams) add(name, value);
  }

  private Optional<Integer> getInteger(String name) {
    return Optional.ofNullable(params.get(name)).map(Integer::valueOf);
  }

  private UpdateStoreQueryParams putLong(String name, long value) {
    return (UpdateStoreQueryParams) add(name, value);
  }

  private UpdateStoreQueryParams putBoolean(String name, boolean value) {
    return (UpdateStoreQueryParams) add(name, value);
  }

  private Optional<Boolean> getBoolean(String name) {
    return Optional.ofNullable(params.get(name)).map(Boolean::valueOf);
  }

  private UpdateStoreQueryParams putString(String name, String value) {
    return (UpdateStoreQueryParams) add(name, value);
  }

  private UpdateStoreQueryParams putStringList(String name, List<String> value) {
    try {
      return (UpdateStoreQueryParams) add(name, OBJECT_MAPPER.writeValueAsString(value));
    } catch (JsonProcessingException e) {
      throw new VeniceException(e.getMessage());
    }
  }

  private Optional<List<String>> getStringList(String name) {
    if (!params.containsKey(name)) {
      return Optional.empty();
    } else {
      try {
        return Optional.of(OBJECT_MAPPER.readValue(params.get(name), List.class));
      } catch (IOException e) {
        throw new VeniceException(e.getMessage());
      }
    }
  }
}
