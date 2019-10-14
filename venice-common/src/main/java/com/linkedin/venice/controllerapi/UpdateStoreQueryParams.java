package com.linkedin.venice.controllerapi;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.StoreInfo;
import java.util.Map;
import java.util.Optional;

import static com.linkedin.venice.controllerapi.ControllerApiConstants.*;

public class UpdateStoreQueryParams extends QueryParams {
  public UpdateStoreQueryParams(Map<String, String> initialParams) {
    super(initialParams);
  }

  public UpdateStoreQueryParams() {
    super();
  }

  /**
   * Useful for store migration
   * @param srcStore The original store
   */
  public UpdateStoreQueryParams(StoreInfo srcStore) {
    // Copy everything except current version number and largest used version number
    // This method must be updated everytime a new store property is introduced
    UpdateStoreQueryParams updateStoreQueryParams =
        new UpdateStoreQueryParams()
            .setStorageQuotaInByte(srcStore.getStorageQuotaInByte())
            .setHybridStoreOverheadBypass(srcStore.getHybridStoreOverheadBypass())
            .setReadQuotaInCU(srcStore.getReadQuotaInCU())
            .setAccessControlled(srcStore.isAccessControlled())
            .setChunkingEnabled(srcStore.isChunkingEnabled())
            .setSingleGetRouterCacheEnabled(srcStore.isSingleGetRouterCacheEnabled())
            .setBatchGetRouterCacheEnabled(srcStore.isBatchGetRouterCacheEnabled())
            .setBatchGetLimit(srcStore.getBatchGetLimit())
            .setOwner(srcStore.getOwner())
            .setCompressionStrategy(srcStore.getCompressionStrategy())
            .setClientDecompressionEnabled(srcStore.getClientDecompressionEnabled())
            .setEnableReads(srcStore.isEnableStoreReads())
            .setEnableWrites(srcStore.isEnableStoreWrites())
            .setPartitionCount(srcStore.getPartitionCount())
            .setIncrementalPushEnabled(srcStore.isIncrementalPushEnabled())
            .setNumVersionsToPreserve(srcStore.getNumVersionsToPreserve())
            .setLargestUsedVersionNumber(srcStore.getLargestUsedVersionNumber())
            .setStoreMigration(srcStore.isMigrating())
            .setWriteComputationEnabled(srcStore.isWriteComputationEnabled())
            .setReadComputationEnabled(srcStore.isReadComputationEnabled())
            .setBootstrapToOnlineTimeoutInHours(srcStore.getBootstrapToOnlineTimeoutInHours())
            .setLeaderFollowerModel(srcStore.isLeaderFollowerModelEnabled())
            .setAutoSchemaPushJobEnabled(srcStore.isSchemaAutoRegisterFromPushJobEnabled())
            .setAutoSupersetSchemaEnabledFromReadComputeStore(srcStore.isSuperSetSchemaAutoGenerationForReadComputeEnabled())
            .setBackupStrategy(srcStore.getBackupStrategy());


    HybridStoreConfig hybridStoreConfig = srcStore.getHybridStoreConfig();
    if (hybridStoreConfig != null) {
      updateStoreQueryParams.setHybridOffsetLagThreshold(hybridStoreConfig.getOffsetLagThresholdToGoOnline());
      updateStoreQueryParams.setHybridRewindSeconds(hybridStoreConfig.getRewindTimeInSeconds());
    }

    this.params.putAll(updateStoreQueryParams.params);
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

  public UpdateStoreQueryParams setSingleGetRouterCacheEnabled(boolean singleGetRouterCacheEnabled) {
    return putBoolean(SINGLE_GET_ROUTER_CACHE_ENABLED, singleGetRouterCacheEnabled);
  }
  public Optional<Boolean> getSingleGetRouterCacheEnabled() {
    return getBoolean(SINGLE_GET_ROUTER_CACHE_ENABLED);
  }

  public UpdateStoreQueryParams setBatchGetRouterCacheEnabled(boolean batchGetRouterCacheEnabled) {
    return putBoolean(BATCH_GET_ROUTER_CACHE_ENABLED, batchGetRouterCacheEnabled);
  }
  public Optional<Boolean> getBatchGetRouterCacheEnabled() {
    return getBoolean(BATCH_GET_ROUTER_CACHE_ENABLED);
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

  public Optional<Boolean> getLeaderFollowerModelEnabled() {
    return getBoolean(LEADER_FOLLOWER_MODEL_ENABLED);
  }


  public UpdateStoreQueryParams setAutoSchemaPushJobEnabled(boolean autoSchemaPushJobEnabled) {
    return putBoolean(AUTO_SCHEMA_REGISTER_FOR_PUSHJOB_ENABLED, autoSchemaPushJobEnabled);
  }

  public Optional<Boolean> getAutoSchemaRegisterPushJobEnabled() {
    return getBoolean(AUTO_SCHEMA_REGISTER_FOR_PUSHJOB_ENABLED);
  }

  public UpdateStoreQueryParams setAutoSupersetSchemaEnabledFromReadComputeStore(boolean autoSchemaAdminEnabled) {
    return putBoolean(AUTO_SUPERSET_SCHEMA_FOR_READ_COMPUTE_STORE_ENABLED, autoSchemaAdminEnabled);
  }

  public Optional<Boolean> getAutoSupersetSchemaEnabledForReadComputeStore() {
    return getBoolean(AUTO_SUPERSET_SCHEMA_FOR_READ_COMPUTE_STORE_ENABLED);
  }

  public UpdateStoreQueryParams setBackupStrategy(BackupStrategy backupStrategy) {
    params.put(BACKUP_STRATEGY, backupStrategy.name());
    return this;
  }

  public Optional<BackupStrategy> getBackupStrategy() {
    return Optional.ofNullable(params.get(BACKUP_STRATEGY)).map(BackupStrategy::valueOf);
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
}
