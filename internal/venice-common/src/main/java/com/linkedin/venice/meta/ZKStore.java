package com.linkedin.venice.meta;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.StoreDisabledException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.systemstore.schemas.StoreProperties;
import com.linkedin.venice.systemstore.schemas.StoreVersion;
import com.linkedin.venice.utils.AvroCompatibilityUtils;
import com.linkedin.venice.utils.AvroRecordUtils;
import com.linkedin.venice.utils.StoreUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.avro.util.Utf8;


/**
 * Class defines the store of Venice.
 * <p>
 * This class is NOT thread safe. Concurrency request to ZKStore instance should be controlled in repository level.
 * When adding fields to Store Metadata (stored in ZK or metadata system store), you need to modify avro schema in this folder:
 * venice-common/src/main/resources/avro/StoreMeta/StoreMetaValue
 * Before we enable the zk shared metadata system schema store auto creation:
 * {@link com.linkedin.venice.ConfigKeys#CONTROLLER_ZK_SHARED_META_SYSTEM_SCHEMA_STORE_AUTO_CREATION_ENABLED},
 * we could continue to modify v1 schema.
 * But once this is enabled, we need to evolve the value schema by adding a new version, and DON'T EVER CHANGE
 * THE EXISTING SCHEMA WITHOUT CAREFUL CONSIDERATION!!!
 * Also make sure json serialization still works.
 *
 * So the steps will become the following when you want to add a new field to Store Metadata:
 * 1. Evolve the value schema of metadata system schema mentioned by the above section, and always have a default for the
 *    newly added fields.
 * 2. Add getter/setter methods to {@link Store}.
 *
 *
 * When you want to add a simple field to Store metadata, you just need to create getter/setter for the new field.
 * When you try to add a method starting with 'get', the default json serialization will do serialization by this
 * method, which could produce some unexpected serialization result, so if it is not for serialization purpose, please
 * specify {@link com.fasterxml.jackson.annotation.JsonIgnore} to ignore the method, whose name is starting with 'get'.
 *
 * TODO: we need to refactor this class to separate Store operations from Store POJO, which is being used by JSON
 * TODO: Since metadata keeps increasing, maybe we would like to refactor it to builder pattern.
 *
 * TODO: In the future, we could consider to use avro json serialization directly to make it simpler.
 */
public class ZKStore extends AbstractStore implements DataModelBackedStructure<StoreProperties> {
  /**
   * Internal data model
   */
  private final StoreProperties storeProperties;

  public ZKStore(
      String name,
      String owner,
      long createdTimeMs,
      PersistenceType persistenceType,
      RoutingStrategy routingStrategy,
      ReadStrategy readStrategy,
      OfflinePushStrategy offlinePushStrategy,
      int replicationFactor) {
    this(
        name,
        owner,
        createdTimeMs,
        persistenceType,
        routingStrategy,
        readStrategy,
        offlinePushStrategy,
        NON_EXISTING_VERSION,
        DEFAULT_STORAGE_QUOTA,
        DEFAULT_READ_QUOTA,
        null,
        new PartitionerConfigImpl(), // Every store comes with default partitioner settings.
        replicationFactor);
  }

  public ZKStore(
      String name,
      String owner,
      long createdTime,
      PersistenceType persistenceType,
      RoutingStrategy routingStrategy,
      ReadStrategy readStrategy,
      OfflinePushStrategy offlinePushStrategy,
      int currentVersion,
      long storageQuotaInByte,
      long readQuotaInCU,
      HybridStoreConfig hybridStoreConfig,
      PartitionerConfig partitionerConfig,
      int replicationFactor) {
    if (!Store.isValidStoreName(name)) {
      throw new VeniceException("Invalid store name: " + name);
    }

    this.storeProperties = AvroRecordUtils.prefillAvroRecordWithDefaultValue(new StoreProperties());
    this.storeProperties.replicationFactor = replicationFactor;

    this.storeProperties.name = name;
    this.storeProperties.owner = owner;
    this.storeProperties.createdTime = createdTime;
    if (persistenceType != null) {
      this.storeProperties.persistenceType = persistenceType.value;
    }
    if (routingStrategy != null) {
      this.storeProperties.routingStrategy = routingStrategy.value;
    }
    if (readStrategy != null) {
      this.storeProperties.readStrategy = readStrategy.value;
    }
    if (offlinePushStrategy != null) {
      this.storeProperties.offlinePushStrategy = offlinePushStrategy.value;
    }
    this.storeProperties.versions = new ArrayList<>();
    this.storeProperties.storageQuotaInByte = storageQuotaInByte;
    this.storeProperties.currentVersion = currentVersion;
    this.storeProperties.readQuotaInCU = readQuotaInCU;
    if (hybridStoreConfig != null) {
      this.storeProperties.hybridConfig = hybridStoreConfig.dataModel();
    }
    this.storeProperties.leaderFollowerModelEnabled = true;
    // This makes sure when deserializing existing stores from ZK, we will use default partitioner setting.
    if (partitionerConfig == null) {
      partitionerConfig = new PartitionerConfigImpl();
    }
    this.storeProperties.partitionerConfig = partitionerConfig.dataModel();

    // default ETL config
    this.storeProperties.etlConfig = new ETLStoreConfigImpl().dataModel();
    this.storeProperties.latestVersionPromoteToCurrentTimestamp = System.currentTimeMillis();

    this.storeProperties.leaderFollowerModelEnabled = true;

    setupVersionSupplier(new StoreVersionSupplier() {
      @Override
      public List<StoreVersion> getForUpdate() {
        return storeProperties.versions;
      }

      @Override
      public List<Version> getForRead() {
        return storeProperties.versions.stream()
            .map(sv -> new ReadOnlyStore.ReadOnlyVersion(new VersionImpl(sv)))
            .collect(Collectors.toList());
      }
    });
  }

  public ZKStore(StoreProperties storeProperties) {
    if (!Store.isValidStoreName(storeProperties.name.toString())) {
      throw new VeniceException("Invalid store name: " + storeProperties.name.toString());
    }
    this.storeProperties = storeProperties;
    this.storeProperties.leaderFollowerModelEnabled = true;
    setupVersionSupplier(new StoreVersionSupplier() {
      @Override
      public List<StoreVersion> getForUpdate() {
        return storeProperties.versions;
      }

      @Override
      public List<Version> getForRead() {
        return storeProperties.versions.stream()
            .map(v -> new ReadOnlyStore.ReadOnlyVersion(new VersionImpl(v)))
            .collect(Collectors.toList());
      }
    });
  }

  public ZKStore(Store store) {
    this(
        store.getName(),
        store.getOwner(),
        store.getCreatedTime(),
        store.getPersistenceType(),
        store.getRoutingStrategy(),
        store.getReadStrategy(),
        store.getOffLinePushStrategy(),
        store.getCurrentVersion(),
        store.getStorageQuotaInByte(),
        store.getReadQuotaInCU(),
        store.getHybridStoreConfig() == null ? null : store.getHybridStoreConfig().clone(),
        store.getPartitionerConfig() == null ? null : store.getPartitionerConfig().clone(),
        store.getReplicationFactor());
    setEnableReads(store.isEnableReads());
    setEnableWrites(store.isEnableWrites());
    setPartitionCount(store.getPartitionCount());
    setLowWatermark(store.getLowWatermark());
    setAccessControlled(store.isAccessControlled());
    setCompressionStrategy(store.getCompressionStrategy());
    setClientDecompressionEnabled(store.getClientDecompressionEnabled());
    setChunkingEnabled(store.isChunkingEnabled());
    setRmdChunkingEnabled(store.isRmdChunkingEnabled());
    setBatchGetLimit(store.getBatchGetLimit());
    setNumVersionsToPreserve(store.getNumVersionsToPreserve());
    setIncrementalPushEnabled(store.isIncrementalPushEnabled());
    setLargestUsedVersionNumber(store.getLargestUsedVersionNumber());
    setMigrating(store.isMigrating());
    setWriteComputationEnabled(store.isWriteComputationEnabled());
    setReadComputationEnabled(store.isReadComputationEnabled());
    setBootstrapToOnlineTimeoutInHours(store.getBootstrapToOnlineTimeoutInHours());
    setNativeReplicationEnabled(store.isNativeReplicationEnabled());
    setBackupStrategy(store.getBackupStrategy());
    setSchemaAutoRegisterFromPushJobEnabled(store.isSchemaAutoRegisterFromPushJobEnabled());
    setLatestSuperSetValueSchemaId(store.getLatestSuperSetValueSchemaId());
    setHybridStoreDiskQuotaEnabled(store.isHybridStoreDiskQuotaEnabled());
    setEtlStoreConfig(store.getEtlStoreConfig());
    setStoreMetadataSystemStoreEnabled(store.isStoreMetadataSystemStoreEnabled());
    setLatestVersionPromoteToCurrentTimestamp(store.getLatestVersionPromoteToCurrentTimestamp());
    setBackupVersionRetentionMs(store.getBackupVersionRetentionMs());
    setReplicationFactor(store.getReplicationFactor());
    setMigrationDuplicateStore(store.isMigrationDuplicateStore());
    setNativeReplicationSourceFabric(store.getNativeReplicationSourceFabric());
    setDaVinciPushStatusStoreEnabled(store.isDaVinciPushStatusStoreEnabled());
    setStoreMetaSystemStoreEnabled(store.isStoreMetaSystemStoreEnabled());
    setActiveActiveReplicationEnabled(store.isActiveActiveReplicationEnabled());
    setRmdVersion(store.getRmdVersion());
    setViewConfigs(store.getViewConfigs());
    setStorageNodeReadQuotaEnabled(store.isStorageNodeReadQuotaEnabled());
    setMinCompactionLagSeconds(store.getMinCompactionLagSeconds());
    setMaxCompactionLagSeconds(store.getMaxCompactionLagSeconds());

    for (Version storeVersion: store.getVersions()) {
      forceAddVersion(storeVersion.cloneVersion(), true);
    }

    /**
     * Add version can overwrite the value of {@link largestUsedVersionNumber}, so in order to clone the
     * object properly, it's important to call the {@link #setLargestUsedVersionNumber(int)} setter after
     * calling {@link #forceAddVersion(Version)}.
     */
    setLargestUsedVersionNumber(store.getLargestUsedVersionNumber());

    // Clone systemStores
    Map<String, SystemStoreAttributes> clonedSystemStores = new HashMap<>();
    store.getSystemStores().forEach((k, v) -> clonedSystemStores.put(k, v.clone()));
    setSystemStores(clonedSystemStores);
  }

  @Override
  public StoreProperties dataModel() {
    return this.storeProperties;
  }

  @Override
  public String getName() {
    return this.storeProperties.name.toString();
  }

  @Override
  public String getOwner() {
    if (this.storeProperties.owner == null) {
      return null;
    }
    return this.storeProperties.owner.toString();
  }

  @Override
  public void setOwner(String owner) {
    this.storeProperties.owner = owner;
  }

  @SuppressWarnings("unused") // Used by Serializer/De-serializer for storing to Zoo Keeper
  @Override
  public long getCreatedTime() {
    return this.storeProperties.createdTime;
  }

  @Override
  public int getCurrentVersion() {
    return this.storeProperties.currentVersion;
  }

  /**
   * Set current serving version number of this store. If store is disabled to write, thrown {@link
   * StoreDisabledException}.
   */
  @Override
  public void setCurrentVersion(int currentVersion) {
    checkDisableStoreWrite("setStoreCurrentVersion", currentVersion);
    // Update the latest version promotion to current timestamp, which is useful for backup version retention.
    setLatestVersionPromoteToCurrentTimestamp(System.currentTimeMillis());
    setCurrentVersionWithoutCheck(currentVersion);
  }

  @SuppressWarnings("unused") // Used by Serializer/De-serializer for storing to Zoo Keeper
  @JsonProperty("currentVersion")
  @Override
  public void setCurrentVersionWithoutCheck(int currentVersion) {
    this.storeProperties.currentVersion = currentVersion;
  }

  @Override
  public long getLowWatermark() {
    return storeProperties.lowWatermark;
  }

  @Override
  public void setLowWatermark(long lowWatermark) {
    this.storeProperties.lowWatermark = lowWatermark;
  }

  @SuppressWarnings("unused") // Used by Serializer/De-serializer for storing to Zoo Keeper
  @Override
  public PersistenceType getPersistenceType() {
    return PersistenceType.getPersistenceTypeFromInt(this.storeProperties.persistenceType);
  }

  @Override
  public void setPersistenceType(PersistenceType persistenceType) {
    this.storeProperties.persistenceType = persistenceType.ordinal();
  }

  @SuppressWarnings("unused") // Used by Serializer/De-serializer for storing to Zoo Keeper
  @Override
  public RoutingStrategy getRoutingStrategy() {
    return RoutingStrategy.getRoutingStrategyFromInt(this.storeProperties.routingStrategy);
  }

  @SuppressWarnings("unused") // Used by Serializer/De-serializer for storing to Zoo Keeper
  @Override
  public ReadStrategy getReadStrategy() {
    return ReadStrategy.getReadStrategyFromInt(this.storeProperties.readStrategy);
  }

  @SuppressWarnings("unused") // Used by Serializer/De-serializer for storing to Zoo Keeper
  @Override
  public OfflinePushStrategy getOffLinePushStrategy() {
    return OfflinePushStrategy.getOfflinePushStrategyFromInt(this.storeProperties.offlinePushStrategy);
  }

  @SuppressWarnings("unused") // Used by Serializer/De-serializer for storing to Zoo Keeper
  @Override
  public void setVersions(List<Version> versions) {
    super.setVersions(versions);
    // Backward capability for the old store in ZK.
    if (this.storeProperties.largestUsedVersionNumber == 0 && !versions.isEmpty()) {
      this.storeProperties.largestUsedVersionNumber = versions.get(versions.size() - 1).getNumber();
    }
  }

  @SuppressWarnings("unused") // Used by Serializer/De-serializer for storing to Zoo Keeper
  @Override
  public int getLargestUsedVersionNumber() {
    return this.storeProperties.largestUsedVersionNumber;
  }

  @SuppressWarnings("unused") // Used by Serializer/De-serializer for storing to Zoo Keeper
  @Override
  public void setLargestUsedVersionNumber(int largestUsedVersionNumber) {
    this.storeProperties.largestUsedVersionNumber = largestUsedVersionNumber;
  }

  @SuppressWarnings("unused") // Used by Serializer/De-serializer for storing to Zoo Keeper
  @Override
  public long getStorageQuotaInByte() {
    // This is a safeguard in case that some old stores do not have storage quota field
    return (this.storeProperties.storageQuotaInByte <= 0
        && this.storeProperties.storageQuotaInByte != UNLIMITED_STORAGE_QUOTA)
            ? DEFAULT_STORAGE_QUOTA
            : this.storeProperties.storageQuotaInByte;
  }

  @SuppressWarnings("unused") // Used by Serializer/De-serializer for storing to Zoo Keeper
  @Override
  public void setStorageQuotaInByte(long storageQuotaInByte) {
    this.storeProperties.storageQuotaInByte = storageQuotaInByte;
  }

  @Override
  public int getPartitionCount() {
    return this.storeProperties.partitionCount;
  }

  @Override
  public void setPartitionCount(int partitionCount) {
    this.storeProperties.partitionCount = partitionCount;
  }

  @Override
  public PartitionerConfig getPartitionerConfig() {
    if (this.storeProperties.partitionerConfig == null) {
      return null;
    }
    return new PartitionerConfigImpl(this.storeProperties.partitionerConfig);
  }

  @Override
  public void setPartitionerConfig(PartitionerConfig value) {
    if (value != null) {
      this.storeProperties.partitionerConfig = value.dataModel();
    }
  }

  @Override
  public boolean isEnableWrites() {
    return this.storeProperties.enableWrites;
  }

  @Override
  public void setEnableWrites(boolean enableWrites) {
    this.storeProperties.enableWrites = enableWrites;
    if (enableWrites) {
      setPushedVersionsOnline();
    }
  }

  @Override
  public boolean isEnableReads() {
    return this.storeProperties.enableReads;
  }

  @Override
  public void setEnableReads(boolean enableReads) {
    this.storeProperties.enableReads = enableReads;
  }

  @Override
  public long getReadQuotaInCU() {
    // In case the store haven't been assigned a quota, use this value as the default quota instead of using 0.
    // If the store was created before we releasing quota feature, JSON framework wil give 0 as the default value
    // while deserializing the store from ZK.
    return this.storeProperties.readQuotaInCU <= 0 ? DEFAULT_READ_QUOTA : this.storeProperties.readQuotaInCU;
  }

  @Override
  public void setReadQuotaInCU(long readQuotaInCU) {
    this.storeProperties.readQuotaInCU = readQuotaInCU;
  }

  @Override
  public HybridStoreConfig getHybridStoreConfig() {
    if (this.storeProperties.hybridConfig == null) {
      return null;
    }
    return new HybridStoreConfigImpl(this.storeProperties.hybridConfig);
  }

  @Override
  public void setHybridStoreConfig(HybridStoreConfig hybridStoreConfig) {
    if (hybridStoreConfig == null) {
      /**
       * The reason to support `null` param here is that we allow revert a hybrid store to be a non-hybrid store.
       */
      this.storeProperties.hybridConfig = null;
    } else {
      this.storeProperties.hybridConfig = hybridStoreConfig.dataModel();
    }
  }

  @JsonProperty("views")
  @Override
  public Map<String, ViewConfig> getViewConfigs() {
    if (this.storeProperties.views == null) {
      return null;
    }

    return this.storeProperties.views.entrySet()
        .stream()
        .collect(Collectors.toMap(e -> e.getKey().toString(), e -> new ViewConfigImpl(e.getValue())));
  }

  @JsonProperty("views")
  @Override
  public void setViewConfigs(Map<String, ViewConfig> viewConfigList) {
    if (viewConfigList == null) {
      this.storeProperties.views = new HashMap<>();
    } else {
      this.storeProperties.views = viewConfigList.entrySet()
          .stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().dataModel()));
    }
  }

  @Override
  public boolean isHybrid() {
    return this.storeProperties.hybridConfig != null;
  }

  @Override
  public CompressionStrategy getCompressionStrategy() {
    return CompressionStrategy.valueOf(this.storeProperties.compressionStrategy);
  }

  @Override
  public void setCompressionStrategy(CompressionStrategy compressionStrategy) {
    this.storeProperties.compressionStrategy = compressionStrategy.getValue();
  }

  @Override
  public boolean getClientDecompressionEnabled() {
    return this.storeProperties.clientDecompressionEnabled;
  }

  public void setClientDecompressionEnabled(boolean clientDecompressionEnabled) {
    this.storeProperties.clientDecompressionEnabled = clientDecompressionEnabled;
  }

  @Override
  public boolean isChunkingEnabled() {
    return this.storeProperties.chunkingEnabled;
  }

  @Override
  public void setChunkingEnabled(boolean chunkingEnabled) {
    this.storeProperties.chunkingEnabled = chunkingEnabled;
  }

  @Override
  public boolean isRmdChunkingEnabled() {
    return this.storeProperties.rmdChunkingEnabled;
  }

  @Override
  public void setRmdChunkingEnabled(boolean rmdChunkingEnabled) {
    this.storeProperties.rmdChunkingEnabled = rmdChunkingEnabled;
  }

  @Override
  public int getBatchGetLimit() {
    return this.storeProperties.batchGetLimit;
  }

  @Override
  public void setBatchGetLimit(int batchGetLimit) {
    this.storeProperties.batchGetLimit = batchGetLimit;
  }

  @Override
  public boolean isIncrementalPushEnabled() {
    return this.storeProperties.incrementalPushEnabled;
  }

  @Override
  public void setIncrementalPushEnabled(boolean incrementalPushEnabled) {
    this.storeProperties.incrementalPushEnabled = incrementalPushEnabled;
  }

  /**
   * @deprecated The store level accessControlled flag is no longer valid to be used to skip ACL checks.
   */
  @Override
  public boolean isAccessControlled() {
    return this.storeProperties.accessControlled;
  }

  /**
   * @deprecated The store level accessControlled flag is no longer valid to be used to skip ACL checks.
   */
  @Override
  public void setAccessControlled(boolean accessControlled) {
    this.storeProperties.accessControlled = accessControlled;
  }

  @Override
  public boolean isMigrating() {
    return this.storeProperties.migrating;
  }

  @Override
  public void setMigrating(boolean migrating) {
    this.storeProperties.migrating = migrating;
  }

  @Override
  public int getNumVersionsToPreserve() {
    return this.storeProperties.numVersionsToPreserve;
  }

  @Override
  public void setNumVersionsToPreserve(int numVersionsToPreserve) {
    this.storeProperties.numVersionsToPreserve = numVersionsToPreserve;
  }

  @Override
  public boolean isWriteComputationEnabled() {
    return this.storeProperties.writeComputationEnabled;
  }

  @Override
  public void setWriteComputationEnabled(boolean writeComputationEnabled) {
    this.storeProperties.writeComputationEnabled = writeComputationEnabled;
  }

  @Override
  public boolean isReadComputationEnabled() {
    return this.storeProperties.readComputationEnabled;
  }

  @Override
  public void setReadComputationEnabled(boolean readComputationEnabled) {
    this.storeProperties.readComputationEnabled = readComputationEnabled;
  }

  @Override
  public int getBootstrapToOnlineTimeoutInHours() {
    return this.storeProperties.bootstrapToOnlineTimeoutInHours;
  }

  @Override
  public void setBootstrapToOnlineTimeoutInHours(int bootstrapToOnlineTimeoutInHours) {
    this.storeProperties.bootstrapToOnlineTimeoutInHours = bootstrapToOnlineTimeoutInHours;
  }

  @Override
  public String getPushStreamSourceAddress() {
    return this.storeProperties.pushStreamSourceAddress.toString();
  }

  @Override
  public void setPushStreamSourceAddress(String sourceAddress) {
    this.storeProperties.pushStreamSourceAddress = sourceAddress;
  }

  @Override
  public boolean isNativeReplicationEnabled() {
    return this.storeProperties.nativeReplicationEnabled;
  }

  @Override
  public int getRmdVersion() {
    return this.storeProperties.replicationMetadataVersionID;
  }

  @Override
  public void setRmdVersion(int rmdVersion) {
    this.storeProperties.replicationMetadataVersionID = rmdVersion;
  }

  @Override
  public void setNativeReplicationEnabled(boolean nativeReplicationEnabled) {
    this.storeProperties.nativeReplicationEnabled = nativeReplicationEnabled;
  }

  @Override
  public BackupStrategy getBackupStrategy() {
    return BackupStrategy.fromInt(this.storeProperties.backupStrategy);
  }

  @Override
  public void setBackupStrategy(BackupStrategy value) {
    this.storeProperties.backupStrategy = value.ordinal();
  }

  @Override
  public boolean isSchemaAutoRegisterFromPushJobEnabled() {
    return this.storeProperties.schemaAutoRegisteFromPushJobEnabled;
  }

  @Override
  public void setSchemaAutoRegisterFromPushJobEnabled(boolean value) {
    this.storeProperties.schemaAutoRegisteFromPushJobEnabled = value;
  }

  @Override
  public int getLatestSuperSetValueSchemaId() {
    return this.storeProperties.latestSuperSetValueSchemaId;
  }

  @Override
  public void setLatestSuperSetValueSchemaId(int valueSchemaId) {
    this.storeProperties.latestSuperSetValueSchemaId = valueSchemaId;
  }

  @Override
  public boolean isHybridStoreDiskQuotaEnabled() {
    return this.storeProperties.hybridStoreDiskQuotaEnabled;
  }

  @Override
  public void setHybridStoreDiskQuotaEnabled(boolean enabled) {
    this.storeProperties.hybridStoreDiskQuotaEnabled = enabled;
  }

  @Override
  public ETLStoreConfig getEtlStoreConfig() {
    if (this.storeProperties.etlConfig == null) {
      return null;
    }
    return new ETLStoreConfigImpl(this.storeProperties.etlConfig);
  }

  @Override
  public void setEtlStoreConfig(ETLStoreConfig etlStoreConfig) {
    if (etlStoreConfig != null) {
      this.storeProperties.etlConfig = etlStoreConfig.dataModel();
    }
  }

  @Override
  public boolean isStoreMetadataSystemStoreEnabled() {
    return this.storeProperties.storeMetadataSystemStoreEnabled;
  }

  @Override
  public void setStoreMetadataSystemStoreEnabled(boolean storeMetadataSystemStoreEnabled) {
    this.storeProperties.storeMetadataSystemStoreEnabled = storeMetadataSystemStoreEnabled;
  }

  @Override
  public boolean isStoreMetaSystemStoreEnabled() {
    return this.storeProperties.storeMetaSystemStoreEnabled;
  }

  @Override
  public void setStoreMetaSystemStoreEnabled(boolean storeMetaSystemStoreEnabled) {
    this.storeProperties.storeMetaSystemStoreEnabled = storeMetaSystemStoreEnabled;
  }

  @Override
  public long getLatestVersionPromoteToCurrentTimestamp() {
    return this.storeProperties.latestVersionPromoteToCurrentTimestamp;
  }

  @Override
  public void setLatestVersionPromoteToCurrentTimestamp(long latestVersionPromoteToCurrentTimestamp) {
    this.storeProperties.latestVersionPromoteToCurrentTimestamp = latestVersionPromoteToCurrentTimestamp;
  }

  @Override
  public long getBackupVersionRetentionMs() {
    return this.storeProperties.backupVersionRetentionMs;
  }

  @Override
  public void setBackupVersionRetentionMs(long backupVersionRetentionMs) {
    this.storeProperties.backupVersionRetentionMs = backupVersionRetentionMs;
  }

  /**
    * Get the retention time for the RT Topic. If there exists a HybridStoreConfig, return the
    * retention time from the config file. Otherwise, if write compute is enabled, then return the
    * default retention time.
    *
    * @return the retention time for the RT topic, in milliseconds.
    */
  @Override
  public long getRetentionTime() {
    HybridStoreConfig config = this.getHybridStoreConfig();
    if (config != null) {
      return StoreUtils.getExpectedRetentionTimeInMs(this, config);
    } else {
      return DEFAULT_RT_RETENTION_TIME;
    }
  }

  @Override
  public int getReplicationFactor() {
    return this.storeProperties.replicationFactor;
  }

  @Override
  public void setReplicationFactor(int replicationFactor) {
    this.storeProperties.replicationFactor = replicationFactor;
  }

  @Override
  public boolean isMigrationDuplicateStore() {
    return this.storeProperties.migrationDuplicateStore;
  }

  @Override
  public void setMigrationDuplicateStore(boolean migrationDuplicateStore) {
    this.storeProperties.migrationDuplicateStore = migrationDuplicateStore;
  }

  @Override
  public String getNativeReplicationSourceFabric() {
    return this.storeProperties.nativeReplicationSourceFabric.toString();
  }

  @Override
  public void setNativeReplicationSourceFabric(String nativeReplicationSourceFabric) {
    this.storeProperties.nativeReplicationSourceFabric = nativeReplicationSourceFabric;
  }

  @Override
  public boolean isActiveActiveReplicationEnabled() {
    return this.storeProperties.activeActiveReplicationEnabled;
  }

  @Override
  public void setActiveActiveReplicationEnabled(boolean activeActiveReplicationEnabled) {
    this.storeProperties.activeActiveReplicationEnabled = activeActiveReplicationEnabled;
  }

  @Override
  public Map<String, SystemStoreAttributes> getSystemStores() {
    Map<String, SystemStoreAttributes> systemStoreMap = new HashMap<>();
    this.storeProperties.systemStores
        .forEach((k, v) -> systemStoreMap.put(k.toString(), new SystemStoreAttributesImpl(v)));
    return systemStoreMap;
  }

  @Override
  public void setSystemStores(Map<String, SystemStoreAttributes> systemStores) {
    systemStores.forEach((k, v) -> {
      this.storeProperties.systemStores.put(new Utf8(k), v.dataModel());
    });
  }

  @Override
  public void putSystemStore(VeniceSystemStoreType systemStoreType, SystemStoreAttributes systemStoreAttributes) {
    this.storeProperties.systemStores.put(new Utf8(systemStoreType.getPrefix()), systemStoreAttributes.dataModel());
  }

  @Override
  public boolean isDaVinciPushStatusStoreEnabled() {
    return this.storeProperties.daVinciPushStatusStoreEnabled;
  }

  @Override
  public void setDaVinciPushStatusStoreEnabled(boolean daVinciPushStatusStoreEnabled) {
    this.storeProperties.daVinciPushStatusStoreEnabled = daVinciPushStatusStoreEnabled;
  }

  @Override
  public boolean isStorageNodeReadQuotaEnabled() {
    return this.storeProperties.storageNodeReadQuotaEnabled;
  }

  @Override
  public void setStorageNodeReadQuotaEnabled(boolean storageNodeReadQuotaEnabled) {
    this.storeProperties.storageNodeReadQuotaEnabled = storageNodeReadQuotaEnabled;
  }

  @Override
  public long getMinCompactionLagSeconds() {
    return this.storeProperties.minCompactionLagSeconds;
  }

  @Override
  public void setMinCompactionLagSeconds(long minCompactionLagSeconds) {
    this.storeProperties.minCompactionLagSeconds = minCompactionLagSeconds;
  }

  @Override
  public long getMaxCompactionLagSeconds() {
    return this.storeProperties.maxCompactionLagSeconds;
  }

  @Override
  public void setMaxCompactionLagSeconds(long maxCompactionLagSeconds) {
    this.storeProperties.maxCompactionLagSeconds = maxCompactionLagSeconds;
  }

  /**
   * Set all of PUSHED version to ONLINE once store is enabled to write.
   */
  private void setPushedVersionsOnline() {
    // TODO, if the PUSHED version is the latest vesion, after store is enabled to write, shall we put this version as
    // the current version?
    for (StoreVersion storeVersion: this.storeProperties.versions) {
      Version version = new VersionImpl(storeVersion);
      if (version.getStatus().equals(VersionStatus.PUSHED)) {
        updateVersionStatus(version.getNumber(), VersionStatus.ONLINE);
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ZKStore store = (ZKStore) o;
    return AvroCompatibilityUtils.compare(storeProperties, store.storeProperties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(storeProperties);
  }

  /**
   * Cloned a new store based on current data in this store.
   *
   * TODO: once the whole stack (all the users of this class) migrates to use modern avro version (1.7+), we could
   * use {@link org.apache.avro.generic.GenericData#deepCopy} to do clone the internal data model: {@link #storeProperties},
   * which will be more convenient.
   *
   * @return cloned store.
   */
  public Store cloneStore() {
    return new ZKStore(this);
  }
}
