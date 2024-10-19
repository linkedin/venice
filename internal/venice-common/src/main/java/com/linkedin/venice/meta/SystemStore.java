package com.linkedin.venice.meta;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.systemstore.schemas.StoreVersion;
import java.util.List;
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
public class SystemStore extends AbstractStore {
  private static final SystemStoreAttributes DEFAULT_READ_ONLY_SYSTEM_STORE_ATTRIBUTE =
      new ReadOnlyStore.ReadOnlySystemStoreAttributes(new SystemStoreAttributesImpl());
  private final Store zkSharedStore;
  private final VeniceSystemStoreType systemStoreType;
  private final Store veniceStore;

  public SystemStore(final Store zkSharedStore, final VeniceSystemStoreType systemStoreType, final Store veniceStore) {
    this.zkSharedStore = zkSharedStore;
    this.systemStoreType = systemStoreType;
    this.veniceStore = veniceStore;
    setupVersionSupplier(new StoreVersionSupplier() {
      @Override
      public List<StoreVersion> getForUpdate() {
        return fetchAndBackfillSystemStoreAttributes(false).dataModel().versions;
      }

      @Override
      public List<Version> getForRead() {
        return fetchAndBackfillSystemStoreAttributes(true).getVersions();
      }
    });
  }

  public Store getZkSharedStore() {
    return zkSharedStore;
  }

  public VeniceSystemStoreType getSystemStoreType() {
    return systemStoreType;
  }

  public SerializableSystemStore getSerializableSystemStore() {
    Store zkSharedStoreClone = zkSharedStore.cloneStore();
    Store veniceStoreClone = veniceStore.cloneStore();
    if (!(zkSharedStoreClone instanceof ZKStore && veniceStoreClone instanceof ZKStore)) {
      throw new UnsupportedOperationException(
          "SystemStore is only serializable if the underlying Store object is ZKStore");
    }
    return new SerializableSystemStore((ZKStore) zkSharedStoreClone, systemStoreType, (ZKStore) veniceStoreClone);
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
    throw new VeniceException(
        "Method: '" + method + "' is not supported in system store: " + getName()
            + " since the system store properties are shared across all the system stores");
  }

  /**
   * If the corresponding system store doesn't exist, the behavior is different if {@param readAccess} is true or false.
   * If {@param readAccess} is true, it will return {@link #DEFAULT_READ_ONLY_SYSTEM_STORE_ATTRIBUTE} for read purpose.
   * Else, it will populate the system store and return it back.
   *
   * @param readAccess
   * @return
   */
  private synchronized SystemStoreAttributes fetchAndBackfillSystemStoreAttributes(boolean readAccess) {
    SystemStoreAttributes systemStoreAttributes = veniceStore.getSystemStores().get(systemStoreType.getPrefix());
    if (systemStoreAttributes == null) {
      if (readAccess) {
        return DEFAULT_READ_ONLY_SYSTEM_STORE_ATTRIBUTE;
      }
      veniceStore.putSystemStore(systemStoreType, new SystemStoreAttributesImpl());
      systemStoreAttributes = veniceStore.getSystemStores().get(systemStoreType.getPrefix());
    }
    if (readAccess && !(systemStoreAttributes instanceof ReadOnlyStore.ReadOnlySystemStoreAttributes)) {
      // Make it read-only
      return new ReadOnlyStore.ReadOnlySystemStoreAttributes(systemStoreAttributes);
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
    SystemStoreAttributes systemStoreAttributes = fetchAndBackfillSystemStoreAttributes(true);
    return systemStoreAttributes.getCurrentVersion();
  }

  @Override
  public void setCurrentVersion(int currentVersion) {
    setLatestVersionPromoteToCurrentTimestamp(System.currentTimeMillis());
    setCurrentVersionWithoutCheck(currentVersion);
  }

  @Override
  public void setCurrentVersionWithoutCheck(int currentVersion) {
    SystemStoreAttributes systemStoreAttributes = fetchAndBackfillSystemStoreAttributes(false);
    systemStoreAttributes.setCurrentVersion(currentVersion);
  }

  @Override
  public long getLowWatermark() {
    return zkSharedStore.getLowWatermark();
  }

  @Override
  public void setLowWatermark(long lowWatermark) {
    throwUnsupportedOperationException("setLowWatermark");
  }

  @Override
  public PersistenceType getPersistenceType() {
    return zkSharedStore.getPersistenceType();
  }

  @Override
  public void setPersistenceType(PersistenceType persistenceType) {
    /**
     * Do nothing to avoid the failure since {@literal VeniceHelixAdmin#addVersion} would always
     * setup store-level persistence type, which is not quite useful..
     */
    // throwUnsupportedOperationException("setPersistenceType");
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
    SystemStoreAttributes systemStoreAttributes = fetchAndBackfillSystemStoreAttributes(true);
    return systemStoreAttributes.getLargestUsedVersionNumber();
  }

  @Override
  public void setLargestUsedVersionNumber(int largestUsedVersionNumber) {
    SystemStoreAttributes systemStoreAttributes = fetchAndBackfillSystemStoreAttributes(false);
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

  /**
   * System stores are the internal stores, and even the corresponding user store is not writable, and the
   * internal system stores could be writable, which is controlled by the {@link #zkSharedStore}.
   */
  @Override
  public boolean isEnableWrites() {
    return zkSharedStore.isEnableWrites();
  }

  @Override
  public void setEnableWrites(boolean enableWrites) {
    // do nothing since it will follow the ZK shared Venice store.
  }

  /**
   * System stores are the internal stores, and even the corresponding user store is not readable, and the
   * internal system stores could be readable, which is controlled by the {@link #zkSharedStore}.
   */
  @Override
  public boolean isEnableReads() {
    return zkSharedStore.isEnableReads();
  }

  @Override
  public void setEnableReads(boolean enableReads) {
    // Do nothing since it will follow the ZK shared Venice store
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
  public Map<String, ViewConfig> getViewConfigs() {
    return zkSharedStore.getViewConfigs();
  }

  @Override
  public void setViewConfigs(Map<String, ViewConfig> viewConfigList) {
    throwUnsupportedOperationException("setViewConfig");
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
  public boolean isRmdChunkingEnabled() {
    return zkSharedStore.isRmdChunkingEnabled();
  }

  @Override
  public void setRmdChunkingEnabled(boolean rmdChunkingEnabled) {
    throwUnsupportedOperationException("setRmdChunkingEnabled");
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
  public boolean isSeparateRealTimeTopicEnabled() {
    return zkSharedStore.isSeparateRealTimeTopicEnabled();
  }

  @Override
  public void setSeparateRealTimeTopicEnabled(boolean separateRealTimeTopicEnabled) {
    throwUnsupportedOperationException("setSeparateRealTimeTopicEnabled");
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
   * The system store will be migrated together with the corresponding Venice store.
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
  public int getRmdVersion() {
    return zkSharedStore.getRmdVersion();
  }

  @Override
  public void setRmdVersion(int rmdVersion) {
    throwUnsupportedOperationException("Cannot set RmdVersionID here.");
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
    // TODO zkSharedStore.isStoreMetadataSystemStoreEnabled() should never be true. Perhaps we should enforce that here
    // or somewhere.
    return zkSharedStore.isStoreMetadataSystemStoreEnabled();
  }

  @Override
  public void setStoreMetadataSystemStoreEnabled(boolean storeMetadataSystemStoreEnabled) {
    throwUnsupportedOperationException("setStoreMetadataSystemStoreEnabled");
  }

  @Override
  public boolean isStoreMetaSystemStoreEnabled() {
    return zkSharedStore.isStoreMetaSystemStoreEnabled();
  }

  @Override
  public void setStoreMetaSystemStoreEnabled(boolean storeMetaSystemStoreEnabled) {
    throwUnsupportedOperationException("setStoreMetaSystemStoreEnabled");
  }

  @Override
  public long getLatestVersionPromoteToCurrentTimestamp() {
    SystemStoreAttributes systemStoreAttributes = fetchAndBackfillSystemStoreAttributes(true);
    return systemStoreAttributes.getLatestVersionPromoteToCurrentTimestamp();
  }

  @Override
  public void setLatestVersionPromoteToCurrentTimestamp(long latestVersionPromoteToCurrentTimestamp) {
    SystemStoreAttributes systemStoreAttributes = fetchAndBackfillSystemStoreAttributes(false);
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

  // TODO: evaluate how to support store migration properly
  @Override
  public boolean isMigrationDuplicateStore() {
    return veniceStore.isMigrationDuplicateStore();
  }

  // TODO: evaluate how to support store migration properly
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
  public boolean isActiveActiveReplicationEnabled() {
    return zkSharedStore.isActiveActiveReplicationEnabled();
  }

  @Override
  public void setActiveActiveReplicationEnabled(boolean activeActiveReplicationEnabled) {
    throwUnsupportedOperationException("setActiveActiveReplicationEnabled");
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
  public void putSystemStore(VeniceSystemStoreType systemStoreType, SystemStoreAttributes systemStoreAttributes) {
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
  public boolean isStorageNodeReadQuotaEnabled() {
    return zkSharedStore.isStorageNodeReadQuotaEnabled();
  }

  @Override
  public void setStorageNodeReadQuotaEnabled(boolean storageNodeReadQuotaEnabled) {
    throwUnsupportedOperationException("setStorageNodeReadQuotaEnabled");
  }

  @Override
  public long getMinCompactionLagSeconds() {
    return zkSharedStore.getMinCompactionLagSeconds();
  }

  @Override
  public void setMinCompactionLagSeconds(long minCompactionLagSeconds) {
    throwUnsupportedOperationException("setMinCompactionLagSeconds");
  }

  @Override
  public void setUnusedSchemaDeletionEnabled(boolean unusedSchemaDeletionEnabled) {
    throwUnsupportedOperationException("setUnusedSchemaDeletionEnabled");
  }

  @Override
  public boolean isUnusedSchemaDeletionEnabled() {
    return zkSharedStore.isUnusedSchemaDeletionEnabled();
  }

  @Override
  public long getMaxCompactionLagSeconds() {
    return zkSharedStore.getMaxCompactionLagSeconds();
  }

  @Override
  public void setBlobTransferEnabled(boolean blobTransferEnabled) {
    throwUnsupportedOperationException("Blob transfer not supported");
  }

  @Override
  public boolean isBlobTransferEnabled() {
    return zkSharedStore.isBlobTransferEnabled();
  }

  @Override
  public void setMaxCompactionLagSeconds(long maxCompactionLagSeconds) {
    throwUnsupportedOperationException("setMaxCompactionLagSeconds");
  }

  @Override
  public int getMaxRecordSizeBytes() {
    return zkSharedStore.getMaxRecordSizeBytes();
  }

  @Override
  public void setMaxRecordSizeBytes(int maxRecordSizeBytes) {
    throwUnsupportedOperationException("setMaxRecordSizeBytes");
  }

  @Override
  public int getMaxNearlineRecordSizeBytes() {
    return zkSharedStore.getMaxNearlineRecordSizeBytes();
  }

  @Override
  public void setMaxNearlineRecordSizeBytes(int maxNearlineRecordSizeBytes) {
    throwUnsupportedOperationException("setMaxNearlineRecordSizeBytes");
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
    return zkSharedStore.equals(that.zkSharedStore) && systemStoreType == that.systemStoreType
        && veniceStore.equals(that.veniceStore);
  }

  @Override
  public int hashCode() {
    return Objects.hash(zkSharedStore, systemStoreType, veniceStore);
  }
}
