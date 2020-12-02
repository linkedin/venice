package com.linkedin.venice.meta;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.Map;
import java.util.Objects;


/**
 * This SystemStore class is a wrapper of the corresponding zk shared system store and the regular venice store to provide
 * the exact same interface as the regular Venice Store for System Stores.
 * For the shared properties across all the same kind of system stores, they will be fetched from the zk shared system store,
 * such as partition count, replication factor and so on.
 * For the non-shared properties, such as current version/versions/largestUsedVersionNumber, they will fetched from
 * the regular venice store.
 *
 * Need to keep in mind that all the shared properties are not mutable here, and if we want to change the shared properties,
 * we need to modify the zk shared store (a regular Venice store) instead of here.
 */
public class SystemStore extends Store {
  private final Store zkSharedStore;
  private final VeniceSystemStoreType systemStoreType;
  private final Store veniceStore;

  public SystemStore(final Store zkSharedStore, final VeniceSystemStoreType systemStoreType, final Store veniceStore) {
    this.zkSharedStore = zkSharedStore;
    this.systemStoreType = systemStoreType;
    this.veniceStore = veniceStore;
    setupVersionSupplier(() -> fetchAndBackfillSystemStoreAttributes().dataModel().versions);
  }

  public Store getVeniceStore() {
    return this.veniceStore;
  }

  @Override
  public String getName() {
    return systemStoreType.getSystemStoreName(veniceStore.getName());
  }

  @Override
  public String getOwner() {
    return zkSharedStore.getOwner();
  }

  private void throwUnsupportedOperationException(String method) {
    throw new VeniceException("Method: '" + method + "' is not supported in system store: " + getName() +
        " since the system store properties are shared across all the system stores");
  }

  private synchronized SystemStoreAttributes fetchAndBackfillSystemStoreAttributes() {
    SystemStoreAttributes systemStoreAttributes = veniceStore.getSystemStores().get(systemStoreType.getPrefix());
    if (null == systemStoreAttributes) {
      veniceStore.putSystemStore(systemStoreType, new SystemStoreAttributes());
      systemStoreAttributes = veniceStore.getSystemStores().get(systemStoreType.getPrefix());
    }

    return systemStoreAttributes;
  }


  @Override
  public void setOwner(String owner) {
    throwUnsupportedOperationException("setOwner");
  }

  @Override
  public long getCreatedTime() {
    return zkSharedStore.getCreatedTime();
  }

  @Override
  public int getCurrentVersion() {
    SystemStoreAttributes systemStoreAttributes = fetchAndBackfillSystemStoreAttributes();
    return systemStoreAttributes.getCurrentVersion();
  }

  @Override
  public void setCurrentVersion(int currentVersion) {
    setCurrentVersionWithoutCheck(currentVersion);
  }

  @Override
  public void setCurrentVersionWithoutCheck(int currentVersion) {
    SystemStoreAttributes systemStoreAttributes = fetchAndBackfillSystemStoreAttributes();
    systemStoreAttributes.setCurrentVersion(currentVersion);
  }

  @Override
  public PersistenceType getPersistenceType() {
    return zkSharedStore.getPersistenceType();
  }

  @Override
  public void setPersistenceType(PersistenceType persistenceType) {
    throwUnsupportedOperationException("setPersistenceType");
  }

  @Override
  public RoutingStrategy getRoutingStrategy() {
    return zkSharedStore.getRoutingStrategy();
  }

  @Override
  public ReadStrategy getReadStrategy() {
    return zkSharedStore.getReadStrategy();
  }

  @Override
  public OfflinePushStrategy getOffLinePushStrategy() {
    return zkSharedStore.getOffLinePushStrategy();
  }

  @Override
  public int getLargestUsedVersionNumber() {
    SystemStoreAttributes systemStoreAttributes = fetchAndBackfillSystemStoreAttributes();
    return systemStoreAttributes.getLargestUsedVersionNumber();
  }

  @Override
  public void setLargestUsedVersionNumber(int largestUsedVersionNumber) {
    SystemStoreAttributes systemStoreAttributes = fetchAndBackfillSystemStoreAttributes();
    systemStoreAttributes.setLargestUsedVersionNumber(largestUsedVersionNumber);
  }

  @Override
  public long getStorageQuotaInByte() {
    return zkSharedStore.getStorageQuotaInByte();
  }

  @Override
  public void setStorageQuotaInByte(long storageQuotaInByte) {
    throwUnsupportedOperationException("setStorageQuotaInByte");
  }

  @Override
  public int getPartitionCount() {
    return zkSharedStore.getPartitionCount();
  }

  @Override
  public void setPartitionCount(int partitionCount) {
    throwUnsupportedOperationException("setPartitionCount");
  }

  @Override
  public PartitionerConfig getPartitionerConfig() {
    return zkSharedStore.getPartitionerConfig();
  }

  @Override
  public void setPartitionerConfig(PartitionerConfig value) {
    throwUnsupportedOperationException("setPartitionerConfig");
  }

  @Override
  public boolean isEnableWrites() {
    return zkSharedStore.isEnableWrites();
  }

  @Override
  public void setEnableWrites(boolean enableWrites) {
    throwUnsupportedOperationException("setEnableWrites");
  }

  @Override
  public boolean isEnableReads() {
    return zkSharedStore.isEnableReads();
  }

  @Override
  public void setEnableReads(boolean enableReads) {
    throwUnsupportedOperationException("setEnableReads");
  }

  @Override
  public long getReadQuotaInCU() {
    return zkSharedStore.getReadQuotaInCU();
  }

  @Override
  public void setReadQuotaInCU(long readQuotaInCU) {
    throwUnsupportedOperationException("setReadQuotaInCU");
  }

  @Override
  public HybridStoreConfig getHybridStoreConfig() {
    return zkSharedStore.getHybridStoreConfig();
  }

  @Override
  public void setHybridStoreConfig(HybridStoreConfig hybridStoreConfig) {
    throwUnsupportedOperationException("setHybridStoreConfig");
  }

  @Override
  public boolean isHybrid() {
    return zkSharedStore.isHybrid();
  }

  @Override
  public CompressionStrategy getCompressionStrategy() {
    return zkSharedStore.getCompressionStrategy();
  }

  @Override
  public void setCompressionStrategy(CompressionStrategy compressionStrategy) {
    throwUnsupportedOperationException("setCompressionStrategy");
  }

  @Override
  public boolean getClientDecompressionEnabled() {
    return zkSharedStore.getClientDecompressionEnabled();
  }

  @Override
  public void setClientDecompressionEnabled(boolean clientDecompressionEnabled) {
    throwUnsupportedOperationException("setClientDecompressionEnabled");
  }

  @Override
  public boolean isChunkingEnabled() {
    return zkSharedStore.isChunkingEnabled();
  }

  @Override
  public void setChunkingEnabled(boolean chunkingEnabled) {
    throwUnsupportedOperationException("setChunkingEnabled");
  }

  @Override
  public int getBatchGetLimit() {
    return zkSharedStore.getBatchGetLimit();
  }

  @Override
  public void setBatchGetLimit(int batchGetLimit) {
    throwUnsupportedOperationException("setBatchGetLimit");
  }

  @Override
  public boolean isIncrementalPushEnabled() {
    return zkSharedStore.isIncrementalPushEnabled();
  }

  @Override
  public void setIncrementalPushEnabled(boolean incrementalPushEnabled) {
    throwUnsupportedOperationException("setIncrementalPushEnabled");
  }

  @Override
  public boolean isAccessControlled() {
    return zkSharedStore.isAccessControlled();
  }

  @Override
  public void setAccessControlled(boolean accessControlled) {
    throwUnsupportedOperationException("setAccessControlled");
  }

  /**
   * TODO: need to consider how we could migrate system store accordingly when migrating a user's store.
   * @return
   */
  @Override
  public boolean isMigrating() {
    return veniceStore.isMigrating();
  }

  @Override
  public void setMigrating(boolean migrating) {
    throwUnsupportedOperationException("setMigrating");
  }

  @Override
  public int getNumVersionsToPreserve() {
    return zkSharedStore.getNumVersionsToPreserve();
  }

  @Override
  public void setNumVersionsToPreserve(int numVersionsToPreserve) {
    throwUnsupportedOperationException("setNumVersionsToPreserve");
  }

  @Override
  public boolean isWriteComputationEnabled() {
    return zkSharedStore.isWriteComputationEnabled();
  }

  @Override
  public void setWriteComputationEnabled(boolean writeComputationEnabled) {
    throwUnsupportedOperationException("setWriteComputationEnabled");
  }

  @Override
  public boolean isReadComputationEnabled() {
    return zkSharedStore.isReadComputationEnabled();
  }

  @Override
  public void setReadComputationEnabled(boolean readComputationEnabled) {
    throwUnsupportedOperationException("setReadComputationEnabled");
  }

  @Override
  public int getBootstrapToOnlineTimeoutInHours() {
    return zkSharedStore.getBootstrapToOnlineTimeoutInHours();
  }

  @Override
  public void setBootstrapToOnlineTimeoutInHours(int bootstrapToOnlineTimeoutInHours) {
    throwUnsupportedOperationException("setBootstrapToOnlineTimeoutInHours");
  }

  @Override
  public boolean isLeaderFollowerModelEnabled() {
    return zkSharedStore.isLeaderFollowerModelEnabled();
  }

  @Override
  public void setLeaderFollowerModelEnabled(boolean leaderFollowerModelEnabled) {
    throwUnsupportedOperationException("setLeaderFollowerModelEnabled");
  }

  @Override
  public String getPushStreamSourceAddress() {
    return zkSharedStore.getPushStreamSourceAddress();
  }

  @Override
  public void setPushStreamSourceAddress(String sourceAddress) {
    throwUnsupportedOperationException("setPushStreamSourceAddress");
  }

  @Override
  public boolean isNativeReplicationEnabled() {
    return zkSharedStore.isNativeReplicationEnabled();
  }

  @Override
  public void setNativeReplicationEnabled(boolean nativeReplicationEnabled) {
    throwUnsupportedOperationException("setNativeReplicationEnabled");
  }

  @Override
  public BackupStrategy getBackupStrategy() {
    return zkSharedStore.getBackupStrategy();
  }

  @Override
  public void setBackupStrategy(BackupStrategy value) {
    throwUnsupportedOperationException("setBackupStrategy");
  }

  @Override
  public boolean isSchemaAutoRegisterFromPushJobEnabled() {
    return zkSharedStore.isSchemaAutoRegisterFromPushJobEnabled();
  }

  @Override
  public void setSchemaAutoRegisterFromPushJobEnabled(boolean value) {
    throwUnsupportedOperationException("setSchemaAutoRegisterFromPushJobEnabled");
  }

  @Override
  public int getLatestSuperSetValueSchemaId() {
    return zkSharedStore.getLatestSuperSetValueSchemaId();
  }

  @Override
  public void setLatestSuperSetValueSchemaId(int valueSchemaId) {
    throwUnsupportedOperationException("setLatestSuperSetValueSchemaId");
  }

  @Override
  public boolean isHybridStoreDiskQuotaEnabled() {
    return zkSharedStore.isHybridStoreDiskQuotaEnabled();
  }

  @Override
  public void setHybridStoreDiskQuotaEnabled(boolean enabled) {
    throwUnsupportedOperationException("setHybridStoreDiskQuotaEnabled");
  }

  @Override
  public ETLStoreConfig getEtlStoreConfig() {
    return zkSharedStore.getEtlStoreConfig();
  }

  @Override
  public void setEtlStoreConfig(ETLStoreConfig etlStoreConfig) {
    throwUnsupportedOperationException("setEtlStoreConfig");
  }

  @Override
  public boolean isStoreMetadataSystemStoreEnabled() {
    return zkSharedStore.isStoreMetadataSystemStoreEnabled();
  }

  @Override
  public void setStoreMetadataSystemStoreEnabled(boolean storeMetadataSystemStoreEnabled) {
    throwUnsupportedOperationException("setStoreMetadataSystemStoreEnabled");
  }

  @Override
  public IncrementalPushPolicy getIncrementalPushPolicy() {
    return zkSharedStore.getIncrementalPushPolicy();
  }

  @Override
  public void setIncrementalPushPolicy(IncrementalPushPolicy incrementalPushPolicy) {
    throwUnsupportedOperationException("setIncrementalPushPolicy");
  }

  @Override
  public long getLatestVersionPromoteToCurrentTimestamp() {
    SystemStoreAttributes systemStoreAttributes = fetchAndBackfillSystemStoreAttributes();
    return systemStoreAttributes.getLatestVersionPromoteToCurrentTimestamp();
  }

  @Override
  public void setLatestVersionPromoteToCurrentTimestamp(long latestVersionPromoteToCurrentTimestamp) {
    SystemStoreAttributes systemStoreAttributes = fetchAndBackfillSystemStoreAttributes();
    systemStoreAttributes.setLatestVersionPromoteToCurrentTimestamp(latestVersionPromoteToCurrentTimestamp);
  }

  @Override
  public long getBackupVersionRetentionMs() {
    return zkSharedStore.getBackupVersionRetentionMs();
  }

  @Override
  public void setBackupVersionRetentionMs(long backupVersionRetentionMs) {
    throwUnsupportedOperationException("setBackupVersionRetentionMs");
  }

  @Override
  public long getRetentionTime() {
    return zkSharedStore.getRetentionTime();
  }

  @Override
  public int getReplicationFactor() {
    return zkSharedStore.getReplicationFactor();
  }

  @Override
  public void setReplicationFactor(int replicationFactor) {
    throwUnsupportedOperationException("setReplicationFactor");
  }

  //TODO: evaluate how to support store migration properly
  @Override
  public boolean isMigrationDuplicateStore() {
    return veniceStore.isMigrationDuplicateStore();
  }

  //TODO: evaluate how to support store migration properly
  @Override
  public void setMigrationDuplicateStore(boolean migrationDuplicateStore) {
    throwUnsupportedOperationException("setMigrationDuplicateStore");
  }

  @Override
  public String getNativeReplicationSourceFabric() {
    return zkSharedStore.getNativeReplicationSourceFabric();
  }

  @Override
  public void setNativeReplicationSourceFabric(String nativeReplicationSourceFabric) {
    throwUnsupportedOperationException("setNativeReplicationSourceFabric");
  }

  @Override
  public Map<String, SystemStoreAttributes> getSystemStores() {
    throw new VeniceException("Method: 'getSystemStores' is not supported inside SystemStore");
  }

  @Override
  public void setSystemStores(Map<String, SystemStoreAttributes> systemStores) {
    throw new VeniceException("Method: 'setSystemStores' is not supported inside SystemStore");
  }

  @Override
  protected void putSystemStore(VeniceSystemStoreType systemStoreType, SystemStoreAttributes systemStoreAttributes) {
    throw new VeniceException("Method: 'putSystemStore' is not supported inside SystemStore");
  }

    @Override
  public boolean isDaVinciPushStatusStoreEnabled() {
    return zkSharedStore.isDaVinciPushStatusStoreEnabled();
  }

  @Override
  public void setDaVinciPushStatusStoreEnabled(boolean daVinciPushStatusStoreEnabled) {
    throwUnsupportedOperationException("setDaVinciPushStatusStoreEnabled");
  }

  @Override
  public Store cloneStore() {
    return new SystemStore(zkSharedStore.cloneStore(), systemStoreType, veniceStore.cloneStore());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SystemStore that = (SystemStore) o;
    return zkSharedStore.equals(that.zkSharedStore) && systemStoreType == that.systemStoreType && veniceStore.equals(
        that.veniceStore);
  }

  @Override
  public int hashCode() {
    return Objects.hash(zkSharedStore, systemStoreType, veniceStore);
  }
}
