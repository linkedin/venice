package com.linkedin.venice.controllerapi;

import com.linkedin.venice.compression.CompressionStrategy;
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

  public UpdateStoreQueryParams setChunkingEnabled(boolean chunkingEnabled) {
    return putBoolean(CHUNKING_ENABLED, chunkingEnabled);
  }
  public Optional<Boolean> getChunkingEnabled() {
    return getBoolean(CHUNKING_ENABLED);
  }

  public UpdateStoreQueryParams setRouterCacheEnabled(boolean routerCacheEnabled) {
    return putBoolean(ROUTER_CACHE_ENABLED, routerCacheEnabled);
  }
  public Optional<Boolean> getRouterCacheEnabled() {
    return getBoolean(ROUTER_CACHE_ENABLED);
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
