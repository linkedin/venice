package com.linkedin.venice.controller.util;

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
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ENABLE_READS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ENABLE_WRITES;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ETLED_PROXY_USER_ACCOUNT;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.FUTURE_VERSION_ETL_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.HYBRID_STORE_DISK_QUOTA_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.INCREMENTAL_PUSH_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.LARGEST_USED_VERSION_NUMBER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.LATEST_SUPERSET_SCHEMA_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.MAX_COMPACTION_LAG_SECONDS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.MAX_RECORD_SIZE_BYTES;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.MIGRATION_DUPLICATE_STORE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.MIN_COMPACTION_LAG_SECONDS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NATIVE_REPLICATION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.NATIVE_REPLICATION_SOURCE_FABRIC;
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
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REGULAR_VERSION_ETL_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REPLICATION_FACTOR;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REPLICATION_METADATA_PROTOCOL_VERSION_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REWIND_TIME_IN_SECONDS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.RMD_CHUNKING_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORAGE_NODE_READ_QUOTA_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORAGE_QUOTA_IN_BYTE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_MIGRATION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_VIEW;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TIME_LAG_TO_GO_ONLINE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.UNUSED_SCHEMA_DELETION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VERSION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.WRITE_COMPUTATION_ENABLED;
import static com.linkedin.venice.utils.RegionUtils.parseRegionsFilterList;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.HelixVeniceClusterResources;
import com.linkedin.venice.controller.StoreViewUtils;
import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.VeniceControllerConfig;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.PartitionerSchemaMismatchException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.StoragePersonaRepository;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.ETLStoreConfig;
import com.linkedin.venice.meta.ETLStoreConfigImpl;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.PartitionerConfigImpl;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewConfigImpl;
import com.linkedin.venice.persona.StoragePersona;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.views.VeniceView;
import com.linkedin.venice.views.ViewUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class UpdateStoreUtils {
  private static final Logger LOGGER = LogManager.getLogger(UpdateStoreUtils.class);

  private UpdateStoreUtils() {
  }

  public static UpdateStoreWrapper getStoreUpdate(
      Admin admin,
      String clusterName,
      String storeName,
      UpdateStoreQueryParams params) {
    VeniceControllerMultiClusterConfig multiClusterConfigs = admin.getMultiClusterConfigs();

    // There are certain configs that are only allowed to be updated in child regions. We might still want the ability
    // to update such configs in the parent region via the Admin tool for operational reasons. So, we allow such updates
    // if the regions filter only specifies one region, which is the parent region.
    boolean onlyParentRegionFilter = false;

    // Check whether the command affects this region.
    if (params.getRegionsFilter().isPresent()) {
      Set<String> regionsFilter = parseRegionsFilterList(params.getRegionsFilter().get());
      if (!regionsFilter.contains(multiClusterConfigs.getRegionName())) {
        LOGGER.info(
            "UpdateStore command will be skipped for store: {} in cluster: {}, because the region filter is {}"
                + " which doesn't include the current region: {}",
            storeName,
            clusterName,
            regionsFilter,
            multiClusterConfigs.getRegionName());
        return null;
      }

      if (admin.isParent() && regionsFilter.size() == 1) {
        onlyParentRegionFilter = true;
      }
    }

    Store originalStore = admin.getStore(clusterName, storeName);
    if (originalStore == null) {
      throw new VeniceNoStoreException(storeName, clusterName);
    }

    UpdateStoreWrapper updateStoreWrapper = new UpdateStoreWrapper(originalStore);
    Set<CharSequence> updatedConfigs = updateStoreWrapper.updatedConfigs;
    Store updatedStore = updateStoreWrapper.updatedStore;

    Optional<String> owner = params.getOwner();
    Optional<Boolean> readability = params.getEnableReads();
    Optional<Boolean> writeability = params.getEnableWrites();
    Optional<Integer> partitionCount = params.getPartitionCount();
    Optional<String> partitionerClass = params.getPartitionerClass();
    Optional<Map<String, String>> partitionerParams = params.getPartitionerParams();
    Optional<Integer> amplificationFactor = params.getAmplificationFactor();
    Optional<Long> storageQuotaInByte = params.getStorageQuotaInByte();
    Optional<Long> readQuotaInCU = params.getReadQuotaInCU();
    Optional<Integer> currentVersion = params.getCurrentVersion();
    Optional<Integer> largestUsedVersionNumber = params.getLargestUsedVersionNumber();
    Optional<Long> hybridRewindSeconds = params.getHybridRewindSeconds();
    Optional<Long> hybridOffsetLagThreshold = params.getHybridOffsetLagThreshold();
    Optional<Long> hybridTimeLagThreshold = params.getHybridTimeLagThreshold();
    Optional<DataReplicationPolicy> hybridDataReplicationPolicy = params.getHybridDataReplicationPolicy();
    Optional<BufferReplayPolicy> hybridBufferReplayPolicy = params.getHybridBufferReplayPolicy();
    Optional<Boolean> accessControlled = params.getAccessControlled();
    Optional<CompressionStrategy> compressionStrategy = params.getCompressionStrategy();
    Optional<Boolean> clientDecompressionEnabled = params.getClientDecompressionEnabled();
    Optional<Boolean> chunkingEnabled = params.getChunkingEnabled();
    Optional<Boolean> rmdChunkingEnabled = params.getRmdChunkingEnabled();
    Optional<Integer> batchGetLimit = params.getBatchGetLimit();
    Optional<Integer> numVersionsToPreserve = params.getNumVersionsToPreserve();
    Optional<Boolean> incrementalPushEnabled = params.getIncrementalPushEnabled();
    Optional<Boolean> storeMigration = params.getStoreMigration();
    Optional<Boolean> writeComputationEnabled = params.getWriteComputationEnabled();
    Optional<Integer> replicationMetadataVersionID = params.getReplicationMetadataVersionID();
    Optional<Boolean> readComputationEnabled = params.getReadComputationEnabled();
    Optional<Integer> bootstrapToOnlineTimeoutInHours = params.getBootstrapToOnlineTimeoutInHours();
    Optional<BackupStrategy> backupStrategy = params.getBackupStrategy();
    Optional<Boolean> autoSchemaRegisterPushJobEnabled = params.getAutoSchemaRegisterPushJobEnabled();
    Optional<Boolean> hybridStoreDiskQuotaEnabled = params.getHybridStoreDiskQuotaEnabled();
    Optional<Boolean> regularVersionETLEnabled = params.getRegularVersionETLEnabled();
    Optional<Boolean> futureVersionETLEnabled = params.getFutureVersionETLEnabled();
    Optional<String> etledUserProxyAccount = params.getETLedProxyUserAccount();
    Optional<Boolean> nativeReplicationEnabled = params.getNativeReplicationEnabled();
    Optional<String> pushStreamSourceAddress = params.getPushStreamSourceAddress();
    Optional<Long> backupVersionRetentionMs = params.getBackupVersionRetentionMs();
    Optional<Integer> replicationFactor = params.getReplicationFactor();
    Optional<Boolean> migrationDuplicateStore = params.getMigrationDuplicateStore();
    Optional<String> nativeReplicationSourceFabric = params.getNativeReplicationSourceFabric();
    Optional<Boolean> activeActiveReplicationEnabled = params.getActiveActiveReplicationEnabled();
    Optional<String> personaName = params.getStoragePersona();
    Optional<Map<String, String>> storeViewConfig = params.getStoreViews();
    Optional<String> viewName = params.getViewName();
    Optional<String> viewClassName = params.getViewClassName();
    Optional<Map<String, String>> viewParams = params.getViewClassParams();
    Optional<Boolean> removeView = params.getDisableStoreView();
    Optional<Integer> latestSupersetSchemaId = params.getLatestSupersetSchemaId();
    Optional<Boolean> storageNodeReadQuotaEnabled = params.getStorageNodeReadQuotaEnabled();
    Optional<Long> minCompactionLagSeconds = params.getMinCompactionLagSeconds();
    Optional<Long> maxCompactionLagSeconds = params.getMaxCompactionLagSeconds();
    Optional<Integer> maxRecordSizeBytes = params.getMaxRecordSizeBytes();
    Optional<Boolean> unusedSchemaDeletionEnabled = params.getUnusedSchemaDeletionEnabled();
    Optional<Boolean> blobTransferEnabled = params.getBlobTransferEnabled();

    owner.map(addToUpdatedConfigs(updatedConfigs, OWNER)).ifPresent(updatedStore::setOwner);
    readability.map(addToUpdatedConfigs(updatedConfigs, ENABLE_READS)).ifPresent(updatedStore::setEnableReads);
    writeability.map(addToUpdatedConfigs(updatedConfigs, ENABLE_WRITES)).ifPresent(updatedStore::setEnableWrites);
    partitionCount.map(addToUpdatedConfigs(updatedConfigs, PARTITION_COUNT)).ifPresent(updatedStore::setPartitionCount);
    largestUsedVersionNumber.map(addToUpdatedConfigs(updatedConfigs, LARGEST_USED_VERSION_NUMBER))
        .ifPresent(updatedStore::setLargestUsedVersionNumber);
    bootstrapToOnlineTimeoutInHours.map(addToUpdatedConfigs(updatedConfigs, BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS))
        .ifPresent(updatedStore::setBootstrapToOnlineTimeoutInHours);
    storageQuotaInByte.map(addToUpdatedConfigs(updatedConfigs, STORAGE_QUOTA_IN_BYTE))
        .ifPresent(updatedStore::setStorageQuotaInByte);
    accessControlled.map(addToUpdatedConfigs(updatedConfigs, ACCESS_CONTROLLED))
        .ifPresent(updatedStore::setAccessControlled);
    compressionStrategy.map(addToUpdatedConfigs(updatedConfigs, COMPRESSION_STRATEGY))
        .ifPresent(updatedStore::setCompressionStrategy);
    clientDecompressionEnabled.map(addToUpdatedConfigs(updatedConfigs, CLIENT_DECOMPRESSION_ENABLED))
        .ifPresent(updatedStore::setClientDecompressionEnabled);
    chunkingEnabled.map(addToUpdatedConfigs(updatedConfigs, CHUNKING_ENABLED))
        .ifPresent(updatedStore::setChunkingEnabled);
    rmdChunkingEnabled.map(addToUpdatedConfigs(updatedConfigs, RMD_CHUNKING_ENABLED))
        .ifPresent(updatedStore::setRmdChunkingEnabled);
    batchGetLimit.map(addToUpdatedConfigs(updatedConfigs, BATCH_GET_LIMIT)).ifPresent(updatedStore::setBatchGetLimit);
    numVersionsToPreserve.map(addToUpdatedConfigs(updatedConfigs, NUM_VERSIONS_TO_PRESERVE))
        .ifPresent(updatedStore::setNumVersionsToPreserve);
    replicationFactor.map(addToUpdatedConfigs(updatedConfigs, REPLICATION_FACTOR))
        .ifPresent(updatedStore::setReplicationFactor);
    storeMigration.map(addToUpdatedConfigs(updatedConfigs, STORE_MIGRATION)).ifPresent(updatedStore::setMigrating);
    migrationDuplicateStore.map(addToUpdatedConfigs(updatedConfigs, MIGRATION_DUPLICATE_STORE))
        .ifPresent(updatedStore::setMigrationDuplicateStore);
    writeComputationEnabled.map(addToUpdatedConfigs(updatedConfigs, WRITE_COMPUTATION_ENABLED))
        .ifPresent(updatedStore::setWriteComputationEnabled);
    replicationMetadataVersionID.map(addToUpdatedConfigs(updatedConfigs, REPLICATION_METADATA_PROTOCOL_VERSION_ID))
        .ifPresent(updatedStore::setRmdVersion);
    readComputationEnabled.map(addToUpdatedConfigs(updatedConfigs, READ_COMPUTATION_ENABLED))
        .ifPresent(updatedStore::setReadComputationEnabled);
    nativeReplicationEnabled.map(addToUpdatedConfigs(updatedConfigs, NATIVE_REPLICATION_ENABLED))
        .ifPresent(updatedStore::setNativeReplicationEnabled);
    activeActiveReplicationEnabled.map(addToUpdatedConfigs(updatedConfigs, ACTIVE_ACTIVE_REPLICATION_ENABLED))
        .ifPresent(updatedStore::setActiveActiveReplicationEnabled);
    pushStreamSourceAddress.map(addToUpdatedConfigs(updatedConfigs, PUSH_STREAM_SOURCE_ADDRESS))
        .ifPresent(updatedStore::setPushStreamSourceAddress);
    backupStrategy.map(addToUpdatedConfigs(updatedConfigs, BACKUP_STRATEGY)).ifPresent(updatedStore::setBackupStrategy);
    autoSchemaRegisterPushJobEnabled.map(addToUpdatedConfigs(updatedConfigs, AUTO_SCHEMA_REGISTER_FOR_PUSHJOB_ENABLED))
        .ifPresent(updatedStore::setSchemaAutoRegisterFromPushJobEnabled);
    hybridStoreDiskQuotaEnabled.map(addToUpdatedConfigs(updatedConfigs, HYBRID_STORE_DISK_QUOTA_ENABLED))
        .ifPresent(updatedStore::setHybridStoreDiskQuotaEnabled);
    backupVersionRetentionMs.map(addToUpdatedConfigs(updatedConfigs, BACKUP_VERSION_RETENTION_MS))
        .ifPresent(updatedStore::setBackupVersionRetentionMs);
    nativeReplicationSourceFabric.map(addToUpdatedConfigs(updatedConfigs, NATIVE_REPLICATION_SOURCE_FABRIC))
        .ifPresent(updatedStore::setNativeReplicationSourceFabric);
    latestSupersetSchemaId.map(addToUpdatedConfigs(updatedConfigs, LATEST_SUPERSET_SCHEMA_ID))
        .ifPresent(updatedStore::setLatestSuperSetValueSchemaId);
    minCompactionLagSeconds.map(addToUpdatedConfigs(updatedConfigs, MIN_COMPACTION_LAG_SECONDS))
        .ifPresent(updatedStore::setMinCompactionLagSeconds);
    maxCompactionLagSeconds.map(addToUpdatedConfigs(updatedConfigs, MAX_COMPACTION_LAG_SECONDS))
        .ifPresent(updatedStore::setMaxCompactionLagSeconds);
    maxRecordSizeBytes.map(addToUpdatedConfigs(updatedConfigs, MAX_RECORD_SIZE_BYTES))
        .ifPresent(updatedStore::setMaxRecordSizeBytes);
    unusedSchemaDeletionEnabled.map(addToUpdatedConfigs(updatedConfigs, UNUSED_SCHEMA_DELETION_ENABLED))
        .ifPresent(updatedStore::setUnusedSchemaDeletionEnabled);
    blobTransferEnabled.map(addToUpdatedConfigs(updatedConfigs, BLOB_TRANSFER_ENABLED))
        .ifPresent(updatedStore::setBlobTransferEnabled);
    storageNodeReadQuotaEnabled.map(addToUpdatedConfigs(updatedConfigs, STORAGE_NODE_READ_QUOTA_ENABLED))
        .ifPresent(updatedStore::setStorageNodeReadQuotaEnabled);
    regularVersionETLEnabled.map(addToUpdatedConfigs(updatedConfigs, REGULAR_VERSION_ETL_ENABLED))
        .ifPresent(regularVersionETL -> {
          ETLStoreConfig etlStoreConfig = updatedStore.getEtlStoreConfig();
          if (etlStoreConfig == null) {
            etlStoreConfig = new ETLStoreConfigImpl();
          }
          etlStoreConfig.setRegularVersionETLEnabled(regularVersionETL);
          updatedStore.setEtlStoreConfig(etlStoreConfig);
        });
    futureVersionETLEnabled.map(addToUpdatedConfigs(updatedConfigs, FUTURE_VERSION_ETL_ENABLED))
        .ifPresent(futureVersionETL -> {
          ETLStoreConfig etlStoreConfig = updatedStore.getEtlStoreConfig();
          if (etlStoreConfig == null) {
            etlStoreConfig = new ETLStoreConfigImpl();
          }
          etlStoreConfig.setFutureVersionETLEnabled(futureVersionETL);
          updatedStore.setEtlStoreConfig(etlStoreConfig);
        });
    etledUserProxyAccount.map(addToUpdatedConfigs(updatedConfigs, ETLED_PROXY_USER_ACCOUNT))
        .ifPresent(etlProxyAccount -> {
          ETLStoreConfig etlStoreConfig = updatedStore.getEtlStoreConfig();
          if (etlStoreConfig == null) {
            etlStoreConfig = new ETLStoreConfigImpl();
          }
          etlStoreConfig.setEtledUserProxyAccount(etlProxyAccount);
          updatedStore.setEtlStoreConfig(etlStoreConfig);
        });

    if (incrementalPushEnabled.isPresent()) {
      if (isInferredStoreUpdateAllowed(admin, storeName)) {
        LOGGER.info(
            "Incremental push cannot be configured for store {} in cluster {}. It is inferred through other configs",
            storeName,
            clusterName);
      } else {
        updatedConfigs.add(INCREMENTAL_PUSH_ENABLED);
        updatedStore.setIncrementalPushEnabled(incrementalPushEnabled.get());
      }
    }

    // No matter what, set native replication to enabled in multi-region mode if the store currently doesn't enable it,
    // and it is not explicitly asked to be updated
    if (multiClusterConfigs.isMultiRegion() && !originalStore.isNativeReplicationEnabled()) {
      updateInferredConfig(
          admin,
          updatedStore,
          NATIVE_REPLICATION_ENABLED,
          updatedConfigs,
          () -> updatedStore.setNativeReplicationEnabled(true));
    }

    PartitionerConfig newPartitionerConfig = mergeNewSettingsIntoOldPartitionerConfig(
        originalStore,
        partitionerClass,
        partitionerParams,
        amplificationFactor);

    if (newPartitionerConfig != originalStore.getPartitionerConfig()) {
      partitionerClass.ifPresent(p -> updatedConfigs.add(PARTITIONER_CLASS));
      partitionerParams.ifPresent(p -> updatedConfigs.add(PARTITIONER_PARAMS));
      amplificationFactor.ifPresent(p -> updatedConfigs.add(AMPLIFICATION_FACTOR));
      updatedStore.setPartitionerConfig(newPartitionerConfig);
    }

    HelixVeniceClusterResources resources = admin.getHelixVeniceClusterResources(clusterName);
    VeniceControllerClusterConfig clusterConfig = resources.getConfig();

    readQuotaInCU.map(addToUpdatedConfigs(updatedConfigs, READ_QUOTA_IN_CU)).ifPresent(readQuota -> {
      ZkRoutersClusterManager routersClusterManager = resources.getRoutersClusterManager();
      int routerCount = routersClusterManager.getLiveRoutersCount();
      int defaultReadQuotaPerRouter = clusterConfig.getDefaultReadQuotaPerRouter();

      if (Math.max(defaultReadQuotaPerRouter, routerCount * defaultReadQuotaPerRouter) < readQuota) {
        throw new VeniceException(
            "Cannot update read quota for store " + storeName + " in cluster " + clusterName + ". Read quota "
                + readQuota + " requested is more than the cluster quota.");
      }

      updatedStore.setReadQuotaInCU(readQuota);
    });

    if (currentVersion.isPresent()) {
      if (admin.isParent() && !onlyParentRegionFilter) {
        LOGGER.warn(
            "Skipping current version update in parent region for store: {} in cluster: {}",
            storeName,
            clusterName);
      } else {
        updatedConfigs.add(VERSION);
        updatedStore.setCurrentVersion(currentVersion.get());
      }
    }

    HybridStoreConfig originalHybridStoreConfig = originalStore.getHybridStoreConfig();
    HybridStoreConfig newHybridStoreConfig = mergeNewSettingsIntoOldHybridStoreConfig(
        originalStore,
        hybridRewindSeconds,
        hybridOffsetLagThreshold,
        hybridTimeLagThreshold,
        hybridDataReplicationPolicy,
        hybridBufferReplayPolicy);
    if (!AdminUtils.isHybrid(newHybridStoreConfig) && AdminUtils.isHybrid(originalHybridStoreConfig)) {
      /**
       * If all the hybrid config values are negative, it indicates that the store is being set back to batch-only store.
       * We cannot remove the RT topic immediately because with NR and AA, existing current version is
       * still consuming the RT topic.
       */
      updatedStore.setHybridStoreConfig(null);

      updatedConfigs.add(REWIND_TIME_IN_SECONDS);
      updatedConfigs.add(OFFSET_LAG_TO_GO_ONLINE);
      updatedConfigs.add(TIME_LAG_TO_GO_ONLINE);
      updatedConfigs.add(DATA_REPLICATION_POLICY);
      updatedConfigs.add(BUFFER_REPLAY_POLICY);

      updateInferredConfigsForHybridToBatch(admin, clusterConfig, updatedStore, updatedConfigs);
    } else if (AdminUtils.isHybrid(newHybridStoreConfig)) {
      if (!originalStore.isHybrid()) {
        updateInferredConfigsForBatchToHybrid(admin, clusterConfig, updatedStore, updatedConfigs);
      }

      // If a store is being made Active-Active and a data-replication policy has not been defined, set a default one.
      if (updatedStore.isActiveActiveReplicationEnabled() && !originalStore.isActiveActiveReplicationEnabled()
          && !hybridDataReplicationPolicy.isPresent()) {
        updateInferredConfig(admin, updatedStore, DATA_REPLICATION_POLICY, updatedConfigs, () -> {
          LOGGER.info(
              "Data replication policy was not explicitly set when converting store to Active-Active store: {}."
                  + " Setting it to active-active replication policy.",
              storeName);
          newHybridStoreConfig.setDataReplicationPolicy(DataReplicationPolicy.ACTIVE_ACTIVE);
        });
      }

      if (AdminUtils.isHybrid(originalHybridStoreConfig)) {
        if (originalHybridStoreConfig.getRewindTimeInSeconds() != newHybridStoreConfig.getRewindTimeInSeconds()) {
          updatedConfigs.add(REWIND_TIME_IN_SECONDS);
        }

        if (originalHybridStoreConfig.getOffsetLagThresholdToGoOnline() != newHybridStoreConfig
            .getOffsetLagThresholdToGoOnline()) {
          updatedConfigs.add(OFFSET_LAG_TO_GO_ONLINE);
        }

        if (originalHybridStoreConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds() != newHybridStoreConfig
            .getProducerTimestampLagThresholdToGoOnlineInSeconds()) {
          updatedConfigs.add(TIME_LAG_TO_GO_ONLINE);
        }

        if (originalHybridStoreConfig.getDataReplicationPolicy() != newHybridStoreConfig.getDataReplicationPolicy()) {
          updatedConfigs.add(DATA_REPLICATION_POLICY);
        }

        if (originalHybridStoreConfig.getBufferReplayPolicy() != newHybridStoreConfig.getBufferReplayPolicy()) {
          updatedConfigs.add(BUFFER_REPLAY_POLICY);
        }
      } else {
        updatedConfigs.add(REWIND_TIME_IN_SECONDS);
        updatedConfigs.add(OFFSET_LAG_TO_GO_ONLINE);
        updatedConfigs.add(TIME_LAG_TO_GO_ONLINE);
        updatedConfigs.add(DATA_REPLICATION_POLICY);
        updatedConfigs.add(BUFFER_REPLAY_POLICY);
      }

      updateInferredConfig(
          admin,
          updatedStore,
          INCREMENTAL_PUSH_ENABLED,
          updatedConfigs,
          () -> updatedStore.setIncrementalPushEnabled(
              isIncrementalPushEnabled(clusterConfig.isMultiRegion(), newHybridStoreConfig)));

      updatedStore.setHybridStoreConfig(newHybridStoreConfig);
    }

    if (!updatedStore.isChunkingEnabled() && updatedStore.isWriteComputationEnabled()) {
      updateInferredConfig(admin, updatedStore, CHUNKING_ENABLED, updatedConfigs, () -> {
        LOGGER.info("Enabling chunking because write compute is enabled for store: " + storeName);
        updatedStore.setChunkingEnabled(true);
      });
    }

    if (!updatedStore.isRmdChunkingEnabled() && updatedStore.isWriteComputationEnabled()) {
      updateInferredConfig(admin, updatedStore, RMD_CHUNKING_ENABLED, updatedConfigs, () -> {
        LOGGER.info("Enabling RMD chunking because write compute is enabled for Active/Active store: " + storeName);
        updatedStore.setRmdChunkingEnabled(true);
      });
    }

    if (!updatedStore.isRmdChunkingEnabled() && updatedStore.isActiveActiveReplicationEnabled()) {
      updateInferredConfig(admin, updatedStore, RMD_CHUNKING_ENABLED, updatedConfigs, () -> {
        LOGGER.info("Enabling RMD chunking because Active/Active is enabled for Active/Active store: " + storeName);
        updatedStore.setRmdChunkingEnabled(true);
      });
    }

    if (params.disableMetaStore().isPresent() && params.disableMetaStore().get()) {
      LOGGER.info("Disabling meta system store for store: {} of cluster: {}", storeName, clusterName);
      updatedConfigs.add(DISABLE_META_STORE);
      updatedStore.setStoreMetaSystemStoreEnabled(false);
      updatedStore.setStoreMetadataSystemStoreEnabled(false);
    }

    if (params.disableDavinciPushStatusStore().isPresent() && params.disableDavinciPushStatusStore().get()) {
      updatedConfigs.add(DISABLE_DAVINCI_PUSH_STATUS_STORE);
      LOGGER.info("Disabling davinci push status store for store: {} of cluster: {}", storeName, clusterName);
      updatedStore.setDaVinciPushStatusStoreEnabled(false);
    }

    if (storeViewConfig.isPresent() && viewName.isPresent()) {
      throw new VeniceException("Cannot update a store view and overwrite store view setup together!");
    }

    if (viewName.isPresent()) {
      Map<String, ViewConfig> updatedViewSettings;
      if (!removeView.isPresent()) {
        if (!viewClassName.isPresent()) {
          throw new VeniceException("View class name is required when configuring a view.");
        }
        // If View parameter is not provided, use emtpy map instead. It does not inherit from existing config.
        ViewConfig viewConfig = new ViewConfigImpl(viewClassName.get(), viewParams.orElse(Collections.emptyMap()));
        validateStoreViewConfig(originalStore, viewConfig);
        updatedViewSettings = addNewViewConfigsIntoOldConfigs(originalStore, viewName.get(), viewConfig);
      } else {
        updatedViewSettings = removeViewConfigFromStoreViewConfigMap(originalStore, viewName.get());
      }
      updatedStore.setViewConfigs(updatedViewSettings);
      updatedConfigs.add(STORE_VIEW);
    }

    if (storeViewConfig.isPresent()) {
      // Validate and overwrite store views if they're getting set
      validateStoreViewConfigs(storeViewConfig.get(), updatedStore);
      updatedStore.setViewConfigs(StoreViewUtils.convertStringMapViewToViewConfigMap(storeViewConfig.get()));
      updatedConfigs.add(STORE_VIEW);
    }

    if (personaName.isPresent()) {
      updatedConfigs.add(PERSONA_NAME);
    }

    validateStoreConfigs(admin, clusterName, updatedStore);
    validateStoreUpdate(admin, multiClusterConfigs, clusterName, originalStore, updatedStore);
    validatePersona(admin, clusterName, updatedStore, personaName);

    return updateStoreWrapper;
  }

  private static <T> Function<T, T> addToUpdatedConfigs(Set<CharSequence> updatedConfigs, String config) {
    return (configValue) -> {
      updatedConfigs.add(config);
      return configValue;
    };
  }

  static void updateInferredConfig(
      Admin admin,
      Store store,
      String configName,
      Set<CharSequence> updatedConfigs,
      Runnable updater) {
    if (!isInferredStoreUpdateAllowed(admin, store.getName())) {
      return;
    }

    if (!updatedConfigs.contains(configName)) {
      updater.run();
      updatedConfigs.add(configName);
    }
  }

  static void updateInferredConfigsForHybridToBatch(
      Admin admin,
      VeniceControllerClusterConfig clusterConfig,
      Store updatedStore,
      Set<CharSequence> updatedConfigs) {
    updateInferredConfig(
        admin,
        updatedStore,
        INCREMENTAL_PUSH_ENABLED,
        updatedConfigs,
        () -> updatedStore.setIncrementalPushEnabled(false));
    updateInferredConfig(
        admin,
        updatedStore,
        NATIVE_REPLICATION_SOURCE_FABRIC,
        updatedConfigs,
        () -> updatedStore
            .setNativeReplicationSourceFabric(clusterConfig.getNativeReplicationSourceFabricAsDefaultForBatchOnly()));
    updateInferredConfig(
        admin,
        updatedStore,
        ACTIVE_ACTIVE_REPLICATION_ENABLED,
        updatedConfigs,
        () -> updatedStore.setActiveActiveReplicationEnabled(false));
  }

  static void updateInferredConfigsForBatchToHybrid(
      Admin admin,
      VeniceControllerClusterConfig clusterConfig,
      Store updatedStore,
      Set<CharSequence> updatedConfigs) {
    String clusterName = clusterConfig.getClusterName();
    String storeName = updatedStore.getName();

    updateInferredConfig(
        admin,
        updatedStore,
        NATIVE_REPLICATION_SOURCE_FABRIC,
        updatedConfigs,
        () -> updatedStore
            .setNativeReplicationSourceFabric(clusterConfig.getNativeReplicationSourceFabricAsDefaultForHybrid()));

    /*
     * Enable/disable active-active replication for user hybrid stores if the cluster level config
     * for new hybrid stores is on.
     */
    updateInferredConfig(
        admin,
        updatedStore,
        ACTIVE_ACTIVE_REPLICATION_ENABLED,
        updatedConfigs,
        () -> updatedStore.setActiveActiveReplicationEnabled(
            updatedStore.isActiveActiveReplicationEnabled()
                || (clusterConfig.isActiveActiveReplicationEnabledAsDefaultForHybrid()
                    && !updatedStore.isSystemStore())));

    if (updatedStore.getPartitionCount() == 0) {
      updateInferredConfig(admin, updatedStore, PARTITION_COUNT, updatedConfigs, () -> {
        int updatedPartitionCount = PartitionUtils.calculatePartitionCount(
            storeName,
            updatedStore.getStorageQuotaInByte(),
            0,
            clusterConfig.getPartitionSize(),
            clusterConfig.getMinNumberOfPartitionsForHybrid(),
            clusterConfig.getMaxNumberOfPartitions(),
            clusterConfig.isPartitionCountRoundUpEnabled(),
            clusterConfig.getPartitionCountRoundUpSize());
        updatedStore.setPartitionCount(updatedPartitionCount);
        LOGGER.info(
            "Enforcing default hybrid partition count: {} for a new hybrid store: {}",
            updatedPartitionCount,
            storeName);
      });
    }

    /**
     * If a store:
     *     (1) Is being converted to hybrid;
     *     (2) Is not partial update enabled for now;
     *     (3) Does not request to change partial update config;
     * It means partial update is not enabled, and there is no explict intention to change it. In this case, we will
     * check cluster default config based on the replication policy to determine whether to try to enable partial update.
     */
    final boolean shouldEnablePartialUpdateBasedOnClusterConfig = (updatedStore.isActiveActiveReplicationEnabled()
        ? clusterConfig.isEnablePartialUpdateForHybridActiveActiveUserStores()
        : clusterConfig.isEnablePartialUpdateForHybridNonActiveActiveUserStores());
    if (shouldEnablePartialUpdateBasedOnClusterConfig) {
      LOGGER.info("Controller will enable partial update based on cluster config for store: " + storeName);
      /**
       * When trying to turn on partial update based on cluster config, if schema generation failed, we will not fail the
       * whole request, but just do NOT turn on partial update, as other config update should still be respected.
       */
      try {
        PrimaryControllerConfigUpdateUtils.addUpdateSchemaForStore(admin, clusterName, updatedStore.getName(), true);
        updateInferredConfig(admin, updatedStore, WRITE_COMPUTATION_ENABLED, updatedConfigs, () -> {
          updatedStore.setWriteComputationEnabled(true);
        });
      } catch (Exception e) {
        LOGGER.warn(
            "Caught exception when trying to enable partial update base on cluster config, will not enable partial update for store: "
                + storeName,
            e);
      }
    }
  }

  /**
   * Check if a store can support incremental pushes based on other configs. The following rules define when incremental
   * push is allowed:
   * <ol>
   *   <li>If the system is running in single-region mode, the store must by hybrid</li>
   *   <li>If the system is running in multi-region mode,</li>
   *   <ol type="i">
   *     <li>Hybrid + Active-Active + {@link DataReplicationPolicy} is {@link DataReplicationPolicy#ACTIVE_ACTIVE}</li>
   *     <li>Hybrid + {@link DataReplicationPolicy} is {@link DataReplicationPolicy#AGGREGATE}</li>
   *     <li>Hybrid + {@link DataReplicationPolicy} is {@link DataReplicationPolicy#NONE}</li>
   *   </ol>
   * <ol/>
   * @param multiRegion whether the system is running in multi-region mode
   * @param hybridStoreConfig The hybrid store config after applying all updates
   * @return {@code true} if incremental push is allowed, {@code false} otherwise
   */
  static boolean isIncrementalPushEnabled(boolean multiRegion, HybridStoreConfig hybridStoreConfig) {
    // Only hybrid stores can support incremental push
    if (!AdminUtils.isHybrid(hybridStoreConfig)) {
      return false;
    }

    // If the system is running in multi-region mode, we need to validate the data replication policies
    if (!multiRegion) {
      return true;
    }

    DataReplicationPolicy dataReplicationPolicy = hybridStoreConfig.getDataReplicationPolicy();
    return dataReplicationPolicy == DataReplicationPolicy.ACTIVE_ACTIVE
        || dataReplicationPolicy == DataReplicationPolicy.AGGREGATE
        || dataReplicationPolicy == DataReplicationPolicy.NONE;
  }

  static void validateStoreConfigs(Admin admin, String clusterName, Store store) {
    String storeName = store.getName();
    String errorMessagePrefix = "Store update error for " + storeName + " in cluster: " + clusterName + ": ";

    if (!store.isHybrid()) {
      // Inc push + non hybrid not supported
      if (store.isIncrementalPushEnabled()) {
        throw new VeniceHttpException(
            HttpStatus.SC_BAD_REQUEST,
            errorMessagePrefix + "Incremental push is only supported for hybrid stores",
            ErrorType.INVALID_CONFIG);
      }

      // WC is only supported for hybrid stores
      if (store.isWriteComputationEnabled()) {
        throw new VeniceHttpException(
            HttpStatus.SC_BAD_REQUEST,
            errorMessagePrefix + "Write computation is only supported for hybrid stores",
            ErrorType.INVALID_CONFIG);
      }
    } else {
      HybridStoreConfig hybridStoreConfig = store.getHybridStoreConfig();
      // All fields of hybrid store config must have valid values
      if (hybridStoreConfig.getRewindTimeInSeconds() < 0) {
        throw new VeniceHttpException(
            HttpStatus.SC_BAD_REQUEST,
            errorMessagePrefix + "Rewind time cannot be negative for a hybrid store",
            ErrorType.INVALID_CONFIG);
      }

      if (hybridStoreConfig.getOffsetLagThresholdToGoOnline() < 0
          && hybridStoreConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds() < 0) {
        throw new VeniceHttpException(
            HttpStatus.SC_BAD_REQUEST,
            errorMessagePrefix
                + "Both offset lag threshold and producer timestamp lag threshold cannot be negative for a hybrid store",
            ErrorType.INVALID_CONFIG);
      }

      VeniceControllerConfig controllerConfig = admin.getMultiClusterConfigs().getControllerConfig(clusterName);
      DataReplicationPolicy dataReplicationPolicy = hybridStoreConfig.getDataReplicationPolicy();
      // Incremental push + NON_AGGREGATE DRP is not supported in multi-region mode
      if (controllerConfig.isMultiRegion() && store.isIncrementalPushEnabled()
          && dataReplicationPolicy == DataReplicationPolicy.NON_AGGREGATE) {
        throw new VeniceHttpException(
            HttpStatus.SC_BAD_REQUEST,
            errorMessagePrefix
                + "Incremental push is not supported for hybrid stores with non-aggregate data replication policy",
            ErrorType.INVALID_CONFIG);
      }

      // ACTIVE_ACTIVE DRP is only supported when activeActiveReplicationEnabled = true
      if (dataReplicationPolicy == DataReplicationPolicy.ACTIVE_ACTIVE && !store.isActiveActiveReplicationEnabled()) {
        throw new VeniceHttpException(
            HttpStatus.SC_BAD_REQUEST,
            errorMessagePrefix
                + "Data replication policy ACTIVE_ACTIVE is only supported for hybrid stores with active-active replication enabled",
            ErrorType.INVALID_CONFIG);
      }
    }

    // Storage quota can not be less than 0
    if (store.getStorageQuotaInByte() < 0 && store.getStorageQuotaInByte() != Store.UNLIMITED_STORAGE_QUOTA) {
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          "Storage quota can not be less than 0",
          ErrorType.INVALID_CONFIG);
    }

    // Read quota can not be less than 0
    if (store.getReadQuotaInCU() < 0) {
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          "Read quota can not be less than 0",
          ErrorType.INVALID_CONFIG);
    }

    // Active-active replication is only supported for stores that also have native replication
    if (store.isActiveActiveReplicationEnabled() && !store.isNativeReplicationEnabled()) {
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          "Active/Active Replication cannot be enabled for store " + store.getName()
              + " since Native Replication is not enabled on it.",
          ErrorType.INVALID_CONFIG);
    }

    // Active-Active and write-compute are not supported when amplification factor is more than 1
    PartitionerConfig partitionerConfig = store.getPartitionerConfig();
    if (partitionerConfig == null) {
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          errorMessagePrefix + "Partitioner Config cannot be null",
          ErrorType.INVALID_CONFIG);
    }

    if (partitionerConfig.getAmplificationFactor() > 1
        && (store.isActiveActiveReplicationEnabled() || store.isWriteComputationEnabled())) {
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          errorMessagePrefix
              + "Active-active replication or write computation is not supported for stores with amplification factor > 1",
          ErrorType.INVALID_CONFIG);
    }

    // Before setting partitioner config, verify the updated partitionerConfig can be built
    try {
      Properties partitionerParams = new Properties();
      for (Map.Entry<String, String> param: partitionerConfig.getPartitionerParams().entrySet()) {
        partitionerParams.setProperty(param.getKey(), param.getValue());
      }

      PartitionUtils.getVenicePartitioner(
          partitionerConfig.getPartitionerClass(),
          new VeniceProperties(partitionerParams),
          admin.getKeySchema(clusterName, storeName).getSchema());
    } catch (PartitionerSchemaMismatchException e) {
      String errorMessage = errorMessagePrefix + e.getMessage();
      LOGGER.error(errorMessage);
      throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, errorMessage, ErrorType.INVALID_SCHEMA);
    } catch (Exception e) {
      String errorMessage = errorMessagePrefix + "Partitioner Configs invalid, please verify that partitioner "
          + "configs like classpath and parameters are correct!";
      LOGGER.error(errorMessage);
      throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, errorMessage, ErrorType.INVALID_CONFIG);
    }

    // Validate if the latest superset schema id is an existing value schema
    int latestSupersetSchemaId = store.getLatestSuperSetValueSchemaId();
    if (latestSupersetSchemaId != SchemaData.INVALID_VALUE_SCHEMA_ID) {
      if (admin.getValueSchema(clusterName, storeName, latestSupersetSchemaId) == null) {
        throw new VeniceHttpException(
            HttpStatus.SC_BAD_REQUEST,
            "Unknown value schema id: " + latestSupersetSchemaId + " in store: " + storeName,
            ErrorType.INVALID_CONFIG);
      }
    }

    if (store.getMaxCompactionLagSeconds() < store.getMinCompactionLagSeconds()) {
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          "Store's max compaction lag seconds: " + store.getMaxCompactionLagSeconds() + " shouldn't be smaller than "
              + "store's min compaction lag seconds: " + store.getMinCompactionLagSeconds(),
          ErrorType.INVALID_CONFIG);
    }

    ETLStoreConfig etlStoreConfig = store.getEtlStoreConfig();
    if (etlStoreConfig != null
        && (etlStoreConfig.isRegularVersionETLEnabled() || etlStoreConfig.isFutureVersionETLEnabled())) {
      if (StringUtils.isEmpty(etlStoreConfig.getEtledUserProxyAccount())) {
        throw new VeniceHttpException(
            HttpStatus.SC_BAD_REQUEST,
            "Cannot enable ETL for this store because etled user proxy account is not set",
            ErrorType.INVALID_CONFIG);
      }
    }
  }

  private static void validateStoreUpdate(
      Admin admin,
      VeniceControllerMultiClusterConfig multiClusterConfig,
      String clusterName,
      Store originalStore,
      Store updatedStore) {
    validateStorePartitionCountUpdate(admin, multiClusterConfig, clusterName, originalStore, updatedStore);
    validateStorePartitionerUpdate(clusterName, originalStore, updatedStore);

    if (updatedStore.isWriteComputationEnabled() && !originalStore.isWriteComputationEnabled()) {
      // Dry-run generating update schemas before sending admin messages to enable partial update because
      // update schema generation may fail due to some reasons. If that happens, abort the store update process.
      PrimaryControllerConfigUpdateUtils.addUpdateSchemaForStore(admin, clusterName, originalStore.getName(), true);
    }
  }

  private static void validateStoreViewConfigs(Map<String, String> stringMap, Store store) {
    Map<String, ViewConfig> configs = StoreViewUtils.convertStringMapViewToViewConfigMap(stringMap);
    for (Map.Entry<String, ViewConfig> viewConfigEntry: configs.entrySet()) {
      validateStoreViewConfig(store, viewConfigEntry.getValue());
    }
  }

  private static void validateStoreViewConfig(Store store, ViewConfig viewConfig) {
    // TODO: Pass a proper properties object here. Today this isn't used in this context
    VeniceView view =
        ViewUtils.getVeniceView(viewConfig.getViewClassName(), new Properties(), store, viewConfig.getViewParameters());
    view.validateConfigs();
  }

  /**
   * Used by both the {@link VeniceHelixAdmin} and the {@link VeniceParentHelixAdmin}
   *
   * @param oldStore Existing Store that is the source for updates. This object will not be modified by this method.
   * @param hybridRewindSeconds Optional is present if the returned object should include a new rewind time
   * @param hybridOffsetLagThreshold Optional is present if the returned object should include a new offset lag threshold
   * @param hybridTimeLagThreshold
   * @param hybridDataReplicationPolicy
   * @param bufferReplayPolicy
   * @return null if oldStore has no hybrid configs and optionals are not present,
   *   otherwise a fully specified {@link HybridStoreConfig}
   */
  static HybridStoreConfig mergeNewSettingsIntoOldHybridStoreConfig(
      Store oldStore,
      Optional<Long> hybridRewindSeconds,
      Optional<Long> hybridOffsetLagThreshold,
      Optional<Long> hybridTimeLagThreshold,
      Optional<DataReplicationPolicy> hybridDataReplicationPolicy,
      Optional<BufferReplayPolicy> bufferReplayPolicy) {
    HybridStoreConfig mergedHybridStoreConfig;
    if (oldStore.isHybrid()) { // for an existing hybrid store, just replace any specified values
      HybridStoreConfig oldHybridConfig = oldStore.getHybridStoreConfig().clone();
      mergedHybridStoreConfig = new HybridStoreConfigImpl(
          hybridRewindSeconds.orElseGet(oldHybridConfig::getRewindTimeInSeconds),
          hybridOffsetLagThreshold.orElseGet(oldHybridConfig::getOffsetLagThresholdToGoOnline),
          hybridTimeLagThreshold.orElseGet(oldHybridConfig::getProducerTimestampLagThresholdToGoOnlineInSeconds),
          hybridDataReplicationPolicy.orElseGet(oldHybridConfig::getDataReplicationPolicy),
          bufferReplayPolicy.orElseGet(oldHybridConfig::getBufferReplayPolicy));
    } else {
      mergedHybridStoreConfig = new HybridStoreConfigImpl(
          hybridRewindSeconds.orElse(-1L),
          // If not specified, offset/time lag threshold will be -1 and will not be used to determine whether
          // a partition is ready to serve
          hybridOffsetLagThreshold.orElse(-1L),
          hybridTimeLagThreshold.orElse(-1L),
          hybridDataReplicationPolicy.orElse(DataReplicationPolicy.NON_AGGREGATE),
          bufferReplayPolicy.orElse(BufferReplayPolicy.REWIND_FROM_EOP));
    }

    if (mergedHybridStoreConfig.getRewindTimeInSeconds() > 0
        && mergedHybridStoreConfig.getOffsetLagThresholdToGoOnline() < 0
        && mergedHybridStoreConfig.getProducerTimestampLagThresholdToGoOnlineInSeconds() < 0) {
      throw new VeniceException(
          "Both offset lag threshold and time lag threshold are negative when setting hybrid configs for store "
              + oldStore.getName());
    }

    if (!AdminUtils.isHybrid(mergedHybridStoreConfig)) {
      return null;
    }

    return mergedHybridStoreConfig;
  }

  public static void validateStorePartitionCountUpdate(
      Admin admin,
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      String clusterName,
      Store originalStore,
      int newPartitionCount) {
    Store updatedStore = originalStore.cloneStore();
    updatedStore.setPartitionCount(newPartitionCount);
    validateStorePartitionCountUpdate(admin, multiClusterConfigs, clusterName, originalStore, updatedStore);
  }

  static void validateStorePartitionCountUpdate(
      Admin admin,
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      String clusterName,
      Store originalStore,
      Store updatedStore) {
    String storeName = originalStore.getName();
    String errorMessagePrefix = "Store update error for " + storeName + " in cluster: " + clusterName + ": ";
    VeniceControllerClusterConfig clusterConfig = admin.getHelixVeniceClusterResources(clusterName).getConfig();

    int newPartitionCount = updatedStore.getPartitionCount();
    if (newPartitionCount < 0) {
      String errorMessage = errorMessagePrefix + "Partition count: " + newPartitionCount + " should NOT be negative";
      LOGGER.error(errorMessage);
      throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, errorMessage, ErrorType.INVALID_CONFIG);
    }

    if (updatedStore.isHybrid() && newPartitionCount == 0) {
      String errorMessage = errorMessagePrefix + "Partition count cannot be 0 for hybrid store";
      LOGGER.error(errorMessage);
      throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, errorMessage, ErrorType.INVALID_CONFIG);
    }

    if (originalStore.isHybrid() && updatedStore.isHybrid() && originalStore.getPartitionCount() != newPartitionCount) {
      String errorMessage = errorMessagePrefix + "Cannot change partition count for this hybrid store";
      LOGGER.error(errorMessage);
      throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, errorMessage, ErrorType.INVALID_CONFIG);
    }

    int minPartitionNum = clusterConfig.getMinNumberOfPartitions();
    if (newPartitionCount < minPartitionNum && newPartitionCount != 0) {
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          "Partition count must be at least " + minPartitionNum + " for store: " + storeName
              + ". If a specific partition count is not required, set it to 0.",
          ErrorType.INVALID_CONFIG);
    }

    int maxPartitionNum = clusterConfig.getMaxNumberOfPartitions();
    if (newPartitionCount > maxPartitionNum) {
      String errorMessage =
          errorMessagePrefix + "Partition count: " + newPartitionCount + " should be less than max: " + maxPartitionNum;
      LOGGER.error(errorMessage);
      throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, errorMessage, ErrorType.INVALID_CONFIG);
    }

    if (updatedStore.isHybrid()) {
      // Allow the update if the new partition count matches RT partition count
      TopicManager topicManager;
      if (admin.isParent()) {
        // RT might not exist in parent colo. Get RT partition count from a child colo.
        String childDatacenter = Utils.parseCommaSeparatedStringToList(clusterConfig.getChildDatacenters()).get(0);
        topicManager = admin.getTopicManager(multiClusterConfigs.getChildDataCenterKafkaUrlMap().get(childDatacenter));
      } else {
        topicManager = admin.getTopicManager();
      }
      PubSubTopic realTimeTopic = admin.getPubSubTopicRepository().getTopic(Version.composeRealTimeTopic(storeName));
      if (!topicManager.containsTopic(realTimeTopic)
          || topicManager.getPartitionCount(realTimeTopic) == newPartitionCount) {
        LOGGER.info("Allow updating store " + storeName + " partition count to " + newPartitionCount);
        return;
      }
      String errorMessage = errorMessagePrefix + "Cannot change partition count for this hybrid store";
      LOGGER.error(errorMessage);
      throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, errorMessage, ErrorType.INVALID_CONFIG);
    }
  }

  static void validateStorePartitionerUpdate(String clusterName, Store existingStore, Store updatedStore) {
    String storeName = existingStore.getName();
    String errorMessagePrefix = "Store update error for " + storeName + " in cluster: " + clusterName + ": ";

    if (!existingStore.isHybrid() || !updatedStore.isHybrid()) {
      // Allow partitioner changes for non-hybrid stores
      return;
    }

    PartitionerConfig existingPartitionerConfig = existingStore.getPartitionerConfig();
    PartitionerConfig updatedPartitionerConfig = updatedStore.getPartitionerConfig();

    if (!existingPartitionerConfig.getPartitionerClass().equals(updatedPartitionerConfig.getPartitionerClass())) {
      String errorMessage = errorMessagePrefix + "Partitioner class cannot be changed for hybrid store";
      LOGGER.error(errorMessage);
      throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, errorMessage, ErrorType.INVALID_CONFIG);
    }

    if (!existingPartitionerConfig.getPartitionerParams().equals(updatedPartitionerConfig.getPartitionerParams())) {
      String errorMessage = errorMessagePrefix + "Partitioner params cannot be changed for hybrid store";
      LOGGER.error(errorMessage);
      throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, errorMessage, ErrorType.INVALID_CONFIG);
    }
  }

  static void validatePersona(Admin admin, String clusterName, Store updatedStore, Optional<String> personaName) {
    String storeName = updatedStore.getName();
    StoragePersonaRepository repository =
        admin.getHelixVeniceClusterResources(clusterName).getStoragePersonaRepository();
    StoragePersona personaToValidate = null;
    StoragePersona existingPersona = repository.getPersonaContainingStore(storeName);

    if (personaName.isPresent()) {
      personaToValidate = admin.getStoragePersona(clusterName, personaName.get());
      if (personaToValidate == null) {
        String errMsg = "UpdateStore command failed for store " + storeName + ".  The provided StoragePersona "
            + personaName.get() + " does not exist.";
        throw new VeniceException(errMsg);
      }
    } else if (existingPersona != null) {
      personaToValidate = existingPersona;
    }

    if (personaToValidate != null) {
      repository.validateAddUpdatedStore(personaToValidate, Optional.of(updatedStore));
    }
  }

  static PartitionerConfig mergeNewSettingsIntoOldPartitionerConfig(
      Store oldStore,
      Optional<String> partitionerClass,
      Optional<Map<String, String>> partitionerParams,
      Optional<Integer> amplificationFactor) {

    if (!partitionerClass.isPresent() && !partitionerParams.isPresent() && !amplificationFactor.isPresent()) {
      return oldStore.getPartitionerConfig();
    }

    PartitionerConfig originalPartitionerConfig;
    if (oldStore.getPartitionerConfig() == null) {
      originalPartitionerConfig = new PartitionerConfigImpl();
    } else {
      originalPartitionerConfig = oldStore.getPartitionerConfig();
    }
    return new PartitionerConfigImpl(
        partitionerClass.orElse(originalPartitionerConfig.getPartitionerClass()),
        partitionerParams.orElse(originalPartitionerConfig.getPartitionerParams()),
        amplificationFactor.orElse(originalPartitionerConfig.getAmplificationFactor()));
  }

  static Map<String, ViewConfig> addNewViewConfigsIntoOldConfigs(
      Store oldStore,
      String viewClass,
      ViewConfig viewConfig) throws VeniceException {
    // Add new view config into the existing config map. The new configs will override existing ones which share the
    // same key.
    Map<String, ViewConfig> oldViewConfigMap = oldStore.getViewConfigs();
    if (oldViewConfigMap == null) {
      oldViewConfigMap = new HashMap<>();
    }
    Map<String, ViewConfig> mergedConfigs = new HashMap<>(oldViewConfigMap);
    mergedConfigs.put(viewClass, viewConfig);
    return mergedConfigs;
  }

  static Map<String, ViewConfig> removeViewConfigFromStoreViewConfigMap(Store oldStore, String viewClass)
      throws VeniceException {
    Map<String, ViewConfig> oldViewConfigMap = oldStore.getViewConfigs();
    if (oldViewConfigMap == null) {
      // TODO: We might want to return a null instead of empty map
      oldViewConfigMap = new HashMap<>();
    }
    Map<String, ViewConfig> mergedConfigs = new HashMap<>(oldViewConfigMap);
    mergedConfigs.remove(viewClass);
    return mergedConfigs;
  }

  /**
   * This function is the entry-point of all operations that are necessary after the successful execution of the store
   * update. These should only be executed in the primary controller.
   * @param admin The main {@link Admin} object for this component
   * @param clusterName The name of the cluster where the store is being updated
   * @param storeName The name of the store that was updated
   */
  public static void handlePostUpdateActions(Admin admin, String clusterName, String storeName) {
    PrimaryControllerConfigUpdateUtils.registerInferredSchemas(admin, clusterName, storeName);
  }

  /**
   * Check if direct store config updates are allowed in this controller. In multi-region mode, parent controller
   * decides what store configs get applied to a store. In a single-region mode, the child controller makes this
   * decision.
   * In a multi-region mode, the child controller must not do any inferencing and must only apply the configs that were
   * applied by the parent controller, except for child-region-only stores - i.e. participant store.
   */
  static boolean isInferredStoreUpdateAllowed(Admin admin, String storeName) {
    // For system stores, do not allow any inferencing
    if (VeniceSystemStoreUtils.isSystemStore(storeName)) {
      return false;
    }

    if (!admin.isPrimary()) {
      return false;
    }

    // Parent controller can only apply the updates if it is processing updates in VeniceParentHelixAdmin (i.e. not via
    // the Admin channel)
    return !admin.isParent() || admin instanceof VeniceParentHelixAdmin;
  }
}
