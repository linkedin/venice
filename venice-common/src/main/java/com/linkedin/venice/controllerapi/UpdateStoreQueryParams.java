package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.ETLStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.IncrementalPushPolicy;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.StoreInfo;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;

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

  private ObjectMapper mapper = new ObjectMapper();

  /**
   * This method must be updated everytime a new store property is introduced
   * @param srcStore The original store
   */
  public UpdateStoreQueryParams(StoreInfo srcStore, boolean storeMigrating) {
    // Copy everything except for currentVersion, daVinciPushStatusStoreEnabled, latestSuperSetValueSchemaId,
    // storeMetaSystemStoreEnabled, storeMetadataSystemStoreEnabled
    UpdateStoreQueryParams updateStoreQueryParams =
        new UpdateStoreQueryParams()
            .setAccessControlled(srcStore.isAccessControlled())
            .setActiveActiveReplicationEnabled(srcStore.isActiveActiveReplicationEnabled())
            .setApplyTargetVersionFilterForIncPush(srcStore.isApplyTargetVersionFilterForIncPush())
            .setBackupStrategy(srcStore.getBackupStrategy())
            .setBackupVersionRetentionMs(srcStore.getBackupVersionRetentionMs())
            .setBatchGetLimit(srcStore.getBatchGetLimit())
            .setBootstrapToOnlineTimeoutInHours(srcStore.getBootstrapToOnlineTimeoutInHours())
            .setChunkingEnabled(srcStore.isChunkingEnabled())
            .setClientDecompressionEnabled(srcStore.getClientDecompressionEnabled())
            .setCompressionStrategy(srcStore.getCompressionStrategy())
            .setEnableReads(srcStore.isEnableStoreReads())
            .setEnableWrites(srcStore.isEnableStoreWrites())
            .setHybridStoreDiskQuotaEnabled(srcStore.isHybridStoreDiskQuotaEnabled())
            .setIncrementalPushEnabled(srcStore.isIncrementalPushEnabled())
            .setIncrementalPushPolicy(srcStore.getIncrementalPushPolicy())
            .setLargestUsedVersionNumber(srcStore.getLargestUsedVersionNumber())
            .setLeaderFollowerModel(srcStore.isLeaderFollowerModelEnabled())
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
            .setWriteComputationEnabled(srcStore.isWriteComputationEnabled());

    if (srcStore.getReplicationMetadataVersionId() != -1) {
      updateStoreQueryParams.setReplicationMetadataVersionID(srcStore.getReplicationMetadataVersionId());
    }

    if (storeMigrating) {
      updateStoreQueryParams
          .setLargestUsedVersionNumber(0) // Decrease the largestUsedVersionNumber to trigger bootstrap in dest cluster
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
      updateStoreQueryParams.setHybridTimeLagThreshold(hybridStoreConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds());
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
    for (Map.Entry<String, String> entry : newParams.params.entrySet()) {
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

  public Optional<Boolean> getHybridStoreDiskQuotaEnabled() { return getBoolean(HYBRID_STORE_DISK_QUOTA_ENABLED); }

  public UpdateStoreQueryParams setHybridStoreDiskQuotaEnabled(boolean enabled) {
    return putBoolean(HYBRID_STORE_DISK_QUOTA_ENABLED, enabled);
  }

  public Optional<Boolean> getHybridStoreOverheadBypass() { return getBoolean(HYBRID_STORE_OVERHEAD_BYPASS); }

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

  public UpdateStoreQueryParams setIncrementalPushEnabled(boolean incrementalPushEnabled) {
    return putBoolean(INCREMENTAL_PUSH_ENABLED, incrementalPushEnabled);
  }
  public Optional<Boolean> getIncrementalPushEnabled() {
    return getBoolean(INCREMENTAL_PUSH_ENABLED);
  }

  public UpdateStoreQueryParams setBatchGetLimit(int batchGetLimit) {
    return putInteger(BATCH_GET_LIMIT, batchGetLimit);
  }
  public Optional<Integer> getBatchGetLimit() {
    return getInteger(BATCH_GET_LIMIT);
  }

  public UpdateStoreQueryParams setNumVersionsToPreserve(int numVersionsToPreserve){
    return putInteger(NUM_VERSIONS_TO_PRESERVE, numVersionsToPreserve);
  }
  public Optional<Integer> getNumVersionsToPreserve(){
    return getInteger(NUM_VERSIONS_TO_PRESERVE);
  }

  public UpdateStoreQueryParams setStoreMigration(boolean migrating) {
    return putBoolean(STORE_MIGRATION, migrating);
  }
  public Optional<Boolean> getStoreMigration() {
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

  public UpdateStoreQueryParams setLeaderFollowerModel(boolean leaderFollowerModelEnabled) {
    return putBoolean(LEADER_FOLLOWER_MODEL_ENABLED, leaderFollowerModelEnabled);
  }

  public UpdateStoreQueryParams setNativeReplicationEnabled(boolean nativeReplicationEnabled) {
    return putBoolean(NATIVE_REPLICATION_ENABLED, nativeReplicationEnabled);
  }

  public UpdateStoreQueryParams setPushStreamSourceAddress(String pushStreamSourceAddress) {
    return putString(PUSH_STREAM_SOURCE_ADDRESS, pushStreamSourceAddress);
  }

  public Optional<Boolean> getLeaderFollowerModelEnabled() {
    return getBoolean(LEADER_FOLLOWER_MODEL_ENABLED);
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

  public UpdateStoreQueryParams setIncrementalPushPolicy(IncrementalPushPolicy incrementalPushPolicy) {
    params.put(INCREMENTAL_PUSH_POLICY, incrementalPushPolicy.name());
    return this;
  }
  public Optional<IncrementalPushPolicy> getIncrementalPushPolicy() {
    return Optional.ofNullable(params.get(INCREMENTAL_PUSH_POLICY)).map(IncrementalPushPolicy::valueOf);
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

  public UpdateStoreQueryParams setApplyTargetVersionFilterForIncPush(boolean applyTargetVersionFilterForIncPush) {
    return putBoolean(APPLY_TARGET_VERSION_FILTER_FOR_INC_PUSH, applyTargetVersionFilterForIncPush);
  }

  public Optional<Boolean> applyTargetVersionFilterForIncPush() {
    return getBoolean(APPLY_TARGET_VERSION_FILTER_FOR_INC_PUSH);
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

  //***************** above this line are getters and setters *****************
  private UpdateStoreQueryParams putInteger(String name, int value) {
    return (UpdateStoreQueryParams) add(name, value);
  }

  private Optional<Integer> getInteger(String name) {
    return Optional.ofNullable(params.get(name)).map(Integer::valueOf);
  }

  private UpdateStoreQueryParams putLong(String name, long value) {
    return (UpdateStoreQueryParams) add(name, value);
  }

  private Optional<Long> getLong(String name) {
    return Optional.ofNullable(params.get(name)).map(Long::valueOf);
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

  private Optional<String> getString(String name) {
    return Optional.ofNullable(params.get(name));
  }

  private UpdateStoreQueryParams putStringList(String name, List<String> value) {
    try {
      return (UpdateStoreQueryParams) add(
          name,
          mapper.writeValueAsString(value)
      );
    } catch (JsonProcessingException e) {
      throw new VeniceException(e.getMessage());
    }
  }

  private Optional<List<String>> getStringList(String name) {
    if (!params.containsKey(name)) {
      return Optional.empty();
    } else {
      try {
        return Optional.of(mapper.readValue(params.get(name), List.class));
      } catch (IOException e) {
        throw new VeniceException(e.getMessage());
      }
    }
  }
}
