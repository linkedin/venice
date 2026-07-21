package com.linkedin.venice.controller.storeconfig;

import static com.linkedin.venice.controller.kafka.consumer.AdminConsumptionTask.IGNORED_CURRENT_VERSION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ACCESS_CONTROLLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.AMPLIFICATION_FACTOR;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.AUTO_SCHEMA_REGISTER_FOR_PUSHJOB_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BACKUP_STRATEGY;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BACKUP_VERSION_RETENTION_MS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BATCH_GET_LIMIT;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BLOB_DB_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BLOB_TRANSFER_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BLOB_TRANSFER_IN_SERVER_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.CHUNKING_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.CLIENT_DECOMPRESSION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.COMPACTION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.COMPACTION_THRESHOLD_MILLISECONDS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.COMPRESSION_STRATEGY;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.DISABLE_DAVINCI_PUSH_STATUS_STORE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.DISABLE_META_STORE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ENABLE_READS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ENABLE_STORE_MIGRATION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ENABLE_WRITES;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ENCRYPTION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ENUM_SCHEMA_EVOLUTION_ALLOWED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ETLED_PROXY_USER_ACCOUNT;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ETL_ACTIVE_FABRICS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.ETL_STRATEGY;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.EXTERNAL_STORAGE_READ_MODE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.FLINK_VENICE_VIEWS_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.FUTURE_VERSION_ETL_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.GLOBAL_RT_DIV_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.HYBRID_STORE_DISK_QUOTA_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.INGESTION_PAUSED_REGIONS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.INGESTION_PAUSE_MODE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.IS_DAVINCI_HEARTBEAT_REPORTED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.LARGEST_USED_RT_VERSION_NUMBER;
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
import static com.linkedin.venice.controllerapi.ControllerApiConstants.OWNER;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PARTITIONER_CLASS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PARTITIONER_PARAMS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PARTITION_COUNT;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PERSONA_NAME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PREVIOUS_CURRENT_VERSION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.PUSH_STREAM_SOURCE_ADDRESS;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.READ_COMPUTATION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.READ_QUOTA_IN_CU;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REGULAR_VERSION_ETL_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REPLICATION_FACTOR;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.REPLICATION_METADATA_PROTOCOL_VERSION_ID;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.RMD_CHUNKING_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.SEPARATE_REAL_TIME_TOPIC_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORAGE_MODE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORAGE_NODE_READ_QUOTA_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORAGE_QUOTA_IN_BYTE;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_LIFECYCLE_HOOKS_LIST;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_MIGRATION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.STORE_VIEW;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TARGET_REGION_PROMOTED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TARGET_SWAP_REGION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.TARGET_SWAP_REGION_WAIT_TIME;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.UNUSED_SCHEMA_DELETION_ENABLED;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.VERSION;
import static com.linkedin.venice.controllerapi.ControllerApiConstants.WRITE_COMPUTATION_ENABLED;
import static com.linkedin.venice.utils.RegionUtils.parseRegionsFilterList;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.HelixVeniceClusterResources;
import com.linkedin.venice.controller.HybridStoreConfigPolicy;
import com.linkedin.venice.controller.StoreViewUtils;
import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.VeniceParentHelixAdmin;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.PartitionerConfigRecord;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreLifecycleHooksRecord;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreViewConfigRecord;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.util.ParentControllerConfigUpdateUtils;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.PartitionerSchemaMismatchException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.StoragePersonaRepository;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.hooks.StoreLifecycleEventOutcome;
import com.linkedin.venice.hooks.StoreLifecycleHooks;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.ETLStoreConfig;
import com.linkedin.venice.meta.ETLStoreConfigImpl;
import com.linkedin.venice.meta.ExternalStorageReadMode;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.IngestionPauseMode;
import com.linkedin.venice.meta.LifecycleHooksRecord;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.ReadOnlyStore;
import com.linkedin.venice.meta.StorageMode;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.VeniceETLStrategy;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewConfigImpl;
import com.linkedin.venice.persona.StoragePersona;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.utils.CollectionUtils;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.StoreUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Holds the parallel update-store implementations lifted out of {@code VeniceHelixAdmin}
 * (child-side, mutates the store directly) and {@code VeniceParentHelixAdmin} (parent-side,
 * writes an UPDATE_STORE admin message). Both methods are mechanical lifts of their originals
 * — every implicit {@code this.X()} call is rewritten as {@code admin.X()}, and helper
 * visibilities on the admins were widened where needed (some have since been narrowed back to
 * package-private now that this class is the sole external caller). The bodies are not quite
 * byte-identical to their originals anymore: the partitioner-merge, lifecycle-hooks
 * validation, and hybrid-config merge are delegated to {@link PartitionerConfigPolicy},
 * {@link StoreLifecycleHooksPolicy}, and
 * {@link com.linkedin.venice.controller.HybridStoreConfigPolicy} respectively, rather than
 * inlined. The two bodies are kept in the same file so future deduplication of the shared
 * param-unpacking and validation has both versions in one place.
 *
 * <p>Suggested structural follow-up before the dedup: replace the {@code VeniceHelixAdmin} /
 * {@code VeniceParentHelixAdmin} parameter with a narrow {@code StoreConfigMutator} interface
 * containing only the setters and helpers this class actually calls
 * ({@code setStoreOwner}, {@code setStoreReadability}, {@code storeMetadataUpdate},
 * {@code getPubSubTopicRepository}, {@code getMultiClusterConfigs}, etc.), implemented by
 * both admins. The current shape — {@code applyOnChild(this, ...)} /
 * {@code applyOnParent(this, ...)} — passes the entire admin in, which the Venice style
 * guide explicitly discourages
 * (<i>"Avoid passing {@code this} into classes … consider whether a class actually needs a
 * handle of an instance of an entire other class, or whether it could make do with an
 * instance of a more constrained interface"</i>). That over-coupling is what forced the
 * visibility widening of dozens of admin setters during the original lift, which then had to
 * be partly walked back. A constrained mutator interface removes both problems in one move:
 * the admins keep their setters package-private, the updater can only reach into the
 * surface area it declares, and the parent/child dedup that follows operates against a
 * single typed seam rather than two concrete admin classes whose APIs happen to overlap.
 */
public final class StoreConfigUpdater {
  private static final Logger LOGGER = LogManager.getLogger(StoreConfigUpdater.class);

  private StoreConfigUpdater() {
  }

  /**
   * Child-side update-store: mutates the store metadata in this region. Lifted from the body of
   * {@code VeniceHelixAdmin.updateStore}; the public wrapper there still performs the
   * {@code checkControllerLeadershipFor} call and acquires the per-store write lock.
   */
  public static void applyOnChild(
      VeniceHelixAdmin admin,
      String clusterName,
      String storeName,
      UpdateStoreQueryParams params) {
    // There are certain configs that are only allowed to be updated in child regions. We might still want the ability
    // to update such configs in the parent region via the Admin tool for operational reasons. So, we allow such updates
    // if the regions filter only specifies one region, which is the parent region.
    boolean onlyParentRegionFilter = false;

    // Check whether the command affects this region.
    if (params.getRegionsFilter().isPresent()) {
      Set<String> regionsFilter = parseRegionsFilterList(params.getRegionsFilter().get());
      if (!regionsFilter.contains(admin.getMultiClusterConfigs().getRegionName())) {
        LOGGER.info(
            "UpdateStore command will be skipped for store: {} in cluster: {}, because the region filter is {}"
                + " which doesn't include the current region: {}",
            storeName,
            clusterName,
            regionsFilter,
            admin.getMultiClusterConfigs().getRegionName());
        return;
      }

      if (admin.isParent() && regionsFilter.size() == 1) {
        onlyParentRegionFilter = true;
      }
    }

    Store originalStore = admin.getStore(clusterName, storeName);
    if (originalStore == null) {
      throw new VeniceNoStoreException(storeName, clusterName);
    }

    Optional<Boolean> encryptionEnabled = params.getEncryptionEnabled();
    if (!params.getStoreMigration().orElse(false)) {
      encryptionEnabled.ifPresent(
          aBoolean -> validateEncryptionEnabledUpdate(
              originalStore,
              admin.getHelixVeniceClusterResources(clusterName).getConfig().isEncryptionCluster(),
              aBoolean,
              clusterName,
              storeName));
    }

    if (originalStore.isHybrid() && !admin.getMultiClusterConfigs()
        .getControllerConfig(clusterName)
        .isSkipHybridStoreRTTopicCompactionPolicyUpdateEnabled()) {
      // If this is a hybrid store, always try to disable compaction if RT topic exists.
      try {
        PubSubTopic rtTopic = admin.getPubSubTopicRepository().getTopic(Utils.getRealTimeTopicName(originalStore));
        admin.getTopicManager().updateTopicCompactionPolicy(rtTopic, false);
      } catch (PubSubTopicDoesNotExistException e) {
        LOGGER.error("Could not find realtime topic for hybrid store {}", storeName);
      }
    }

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
    Optional<Integer> largestUsedRTVersionNumber = params.getLargestUsedRTVersionNumber();
    Optional<Long> hybridRewindSeconds = params.getHybridRewindSeconds();
    Optional<Long> hybridOffsetLagThreshold = params.getHybridOffsetLagThreshold();
    Optional<Long> hybridTimeLagThreshold = params.getHybridTimeLagThreshold();
    Optional<DataReplicationPolicy> hybridDataReplicationPolicy = params.getHybridDataReplicationPolicy();
    Optional<BufferReplayPolicy> hybridBufferReplayPolicy = params.getHybridBufferReplayPolicy();
    Optional<String> realTimeTopicName = params.getRealTimeTopicName();
    Optional<Boolean> accessControlled = params.getAccessControlled();
    Optional<CompressionStrategy> compressionStrategy = params.getCompressionStrategy();
    Optional<Boolean> clientDecompressionEnabled = params.getClientDecompressionEnabled();
    Optional<Boolean> chunkingEnabled = params.getChunkingEnabled();
    Optional<Boolean> rmdChunkingEnabled = params.getRmdChunkingEnabled();
    Optional<Integer> batchGetLimit = params.getBatchGetLimit();
    Optional<Integer> numVersionsToPreserve = params.getNumVersionsToPreserve();
    Optional<Boolean> incrementalPushEnabled = params.getIncrementalPushEnabled();
    Optional<Boolean> separateRealTimeTopicEnabled = params.getSeparateRealTimeTopicEnabled();
    Optional<Boolean> storeMigration = params.getStoreMigration();
    Optional<Boolean> writeComputationEnabled = params.getWriteComputationEnabled();
    Optional<Integer> replicationMetadataVersionID = params.getReplicationMetadataVersionID();
    Optional<Boolean> readComputationEnabled = params.getReadComputationEnabled();
    Optional<Integer> bootstrapToOnlineTimeoutInHours = params.getBootstrapToOnlineTimeoutInHours();
    Optional<BackupStrategy> backupStrategy = params.getBackupStrategy();
    Optional<IngestionPauseMode> ingestionPauseMode = params.getIngestionPauseMode();
    Optional<List<String>> ingestionPausedRegions = params.getIngestionPausedRegions();
    Optional<StorageMode> storageMode = params.getStorageMode();
    Optional<ExternalStorageReadMode> externalStorageReadMode = params.getExternalStorageReadMode();
    Optional<Boolean> autoSchemaRegisterPushJobEnabled = params.getAutoSchemaRegisterPushJobEnabled();
    Optional<Boolean> hybridStoreDiskQuotaEnabled = params.getHybridStoreDiskQuotaEnabled();
    Optional<Boolean> regularVersionETLEnabled = params.getRegularVersionETLEnabled();
    Optional<Boolean> futureVersionETLEnabled = params.getFutureVersionETLEnabled();
    Optional<String> etledUserProxyAccount = params.getETLedProxyUserAccount();
    Optional<VeniceETLStrategy> etlStrategy = params.getETLStrategy();
    Optional<List<String>> etlActiveFabrics = params.getEtlActiveFabrics();
    Optional<Boolean> nativeReplicationEnabled = params.getNativeReplicationEnabled();
    Optional<String> pushStreamSourceAddress = params.getPushStreamSourceAddress();
    Optional<Long> backupVersionRetentionMs = params.getBackupVersionRetentionMs();
    Optional<Integer> replicationFactor = params.getReplicationFactor();
    Optional<Boolean> migrationDuplicateStore = params.getMigrationDuplicateStore();
    Optional<String> nativeReplicationSourceFabric = params.getNativeReplicationSourceFabric();
    Optional<Boolean> activeActiveReplicationEnabled = params.getActiveActiveReplicationEnabled();
    Optional<String> personaName = params.getStoragePersona();
    Optional<Map<String, String>> storeViews = params.getStoreViews();
    Optional<Integer> latestSupersetSchemaId = params.getLatestSupersetSchemaId();
    Optional<Boolean> storageNodeReadQuotaEnabled = params.getStorageNodeReadQuotaEnabled();
    Optional<Boolean> compactionEnabled = params.getCompactionEnabled();
    Optional<Long> compactionThresholdMilliseconds = params.getCompactionThresholdMilliseconds();
    Optional<Long> minCompactionLagSeconds = params.getMinCompactionLagSeconds();
    Optional<Long> maxCompactionLagSeconds = params.getMaxCompactionLagSeconds();
    Optional<Integer> maxRecordSizeBytes = params.getMaxRecordSizeBytes();
    Optional<Integer> maxNearlineRecordSizeBytes = params.getMaxNearlineRecordSizeBytes();
    Optional<Boolean> unusedSchemaDeletionEnabled = params.getUnusedSchemaDeletionEnabled();
    Optional<Boolean> blobTransferEnabled = params.getBlobTransferEnabled();
    Optional<String> blobTransferInServerEnabled = params.getBlobTransferInServerEnabled();
    Optional<String> blobDbEnabled = params.getBlobDbEnabled();
    Optional<Boolean> nearlineProducerCompressionEnabled = params.getNearlineProducerCompressionEnabled();
    Optional<Integer> nearlineProducerCountPerWriter = params.getNearlineProducerCountPerWriter();
    Optional<String> targetSwapRegion = params.getTargetSwapRegion();
    Optional<Integer> targetSwapRegionWaitTime = params.getTargetRegionSwapWaitTime();
    Optional<Boolean> isDavinciHeartbeatReported = params.getIsDavinciHeartbeatReported();
    Optional<Boolean> targetRegionPromoted = params.getTargetRegionPromoted();
    Optional<Boolean> globalRtDivEnabled = params.isGlobalRtDivEnabled();
    Optional<Boolean> ttlRepushEnabled = params.isTTLRepushEnabled();
    Optional<Boolean> enumSchemaEvolutionAllowed = params.isEnumSchemaEvolutionAllowed();
    Optional<List<LifecycleHooksRecord>> storeLifecycleHooks = params.getStoreLifecycleHooks();
    Optional<Boolean> flinkVeniceViewsEnabled = params.getFlinkVeniceViewsEnabled();
    Optional<Integer> previousCurrentVersion = params.getPreviousCurrentVersion();

    final Optional<HybridStoreConfig> newHybridStoreConfig;
    if (hybridRewindSeconds.isPresent() || hybridOffsetLagThreshold.isPresent() || hybridTimeLagThreshold.isPresent()
        || hybridDataReplicationPolicy.isPresent() || hybridBufferReplayPolicy.isPresent()) {
      HybridStoreConfig hybridConfig = HybridStoreConfigPolicy.mergeNewSettingsIntoOldHybridStoreConfig(
          originalStore,
          hybridRewindSeconds,
          hybridOffsetLagThreshold,
          hybridTimeLagThreshold,
          hybridDataReplicationPolicy,
          hybridBufferReplayPolicy,
          realTimeTopicName);
      newHybridStoreConfig = Optional.ofNullable(hybridConfig);
    } else {
      newHybridStoreConfig = Optional.empty();
    }

    try {
      if (owner.isPresent()) {
        admin.setStoreOwner(clusterName, storeName, owner.get());
      }

      if (readability.isPresent()) {
        admin.setStoreReadability(clusterName, storeName, readability.get());
      }

      if (writeability.isPresent()) {
        admin.setStoreWriteability(clusterName, storeName, writeability.get());
      }

      if (partitionCount.isPresent()) {
        admin.setStorePartitionCount(clusterName, storeName, partitionCount.get());
      }

      /**
       * If either of these three fields is not present, we should use store's original value to construct correct
       * updated partitioner config.
       */
      if (partitionerClass.isPresent() || partitionerParams.isPresent() || amplificationFactor.isPresent()) {
        PartitionerConfig updatedPartitionerConfig = PartitionerConfigPolicy.mergeNewSettingsIntoOldPartitionerConfig(
            originalStore,
            partitionerClass,
            partitionerParams,
            amplificationFactor);
        admin.setStorePartitionerConfig(clusterName, storeName, updatedPartitionerConfig);
      }

      if (storageQuotaInByte.isPresent()) {
        admin.setStoreStorageQuota(clusterName, storeName, storageQuotaInByte.get());
      }

      if (readQuotaInCU.isPresent()) {
        HelixVeniceClusterResources resources = admin.getHelixVeniceClusterResources(clusterName);
        ZkRoutersClusterManager routersClusterManager = resources.getRoutersClusterManager();
        int routerCount = routersClusterManager.getLiveRoutersCount();
        VeniceControllerClusterConfig clusterConfig = admin.getHelixVeniceClusterResources(clusterName).getConfig();
        int defaultReadQuotaPerRouter = clusterConfig.getDefaultReadQuotaPerRouter();

        if (Math.max(defaultReadQuotaPerRouter, routerCount * defaultReadQuotaPerRouter) < readQuotaInCU.get()) {
          throw new VeniceException(
              "Cannot update read quota for store " + storeName + " in cluster " + clusterName + ". Read quota "
                  + readQuotaInCU.get() + " requested is more than the cluster quota.");
        }
        admin.setStoreReadQuota(clusterName, storeName, readQuotaInCU.get());
      }

      if (currentVersion.isPresent()) {
        admin.setStoreCurrentVersion(clusterName, storeName, currentVersion.get(), onlyParentRegionFilter);
      }

      if (largestUsedVersionNumber.isPresent()) {
        admin.setStoreLargestUsedVersion(clusterName, storeName, largestUsedVersionNumber.get());
      }

      if (largestUsedRTVersionNumber.isPresent()) {
        admin.setStoreLargestUsedRTVersion(clusterName, storeName, largestUsedRTVersionNumber.get());
      }

      if (bootstrapToOnlineTimeoutInHours.isPresent()) {
        admin.setBootstrapToOnlineTimeoutInHours(clusterName, storeName, bootstrapToOnlineTimeoutInHours.get());
      }

      VeniceControllerClusterConfig clusterConfig = admin.getHelixVeniceClusterResources(clusterName).getConfig();
      if (newHybridStoreConfig.isPresent()) {
        // To fix the final variable problem in the lambda expression
        final HybridStoreConfig finalHybridConfig = newHybridStoreConfig.get();
        admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
          if (!HybridStoreConfigPolicy.isHybrid(finalHybridConfig)) {
            /**
             * If all the hybrid config values are negative, it indicates that the store is being set back to batch-only store.
             * We cannot remove the RT topic immediately because with NR and AA, existing current version is
             * still consuming the RT topic.
             */
            store.setHybridStoreConfig(null);
            store.setIncrementalPushEnabled(false);
            // Enable/disable native replication for batch-only stores if the cluster level config for new batch
            // stores is on
            store.setNativeReplicationSourceFabric(
                clusterConfig.getNativeReplicationSourceFabricAsDefaultForBatchOnly());
            store.setActiveActiveReplicationEnabled(false);
          } else {
            // Batch-only store is being converted to hybrid store.
            if (!store.isHybrid()) {
              /*
               * Enable/disable native replication for hybrid stores if the cluster level config
               * for new hybrid stores is on
               */
              store
                  .setNativeReplicationSourceFabric(clusterConfig.getNativeReplicationSourceFabricAsDefaultForHybrid());
              /*
               * Enable/disable active-active replication for user hybrid stores if the cluster level config
               * for new hybrid stores is on
               */
              store.setActiveActiveReplicationEnabled(
                  store.isActiveActiveReplicationEnabled()
                      || (clusterConfig.isActiveActiveReplicationEnabledAsDefaultForHybrid()
                          && !store.isSystemStore()));
            }
            store.setHybridStoreConfig(finalHybridConfig);
            PubSubTopic rtTopic = admin.getPubSubTopicRepository().getTopic(Utils.getRealTimeTopicName(store));
            if (admin.getTopicManager().containsTopicAndAllPartitionsAreOnline(rtTopic)) {
              // RT already exists, ensure the retention is correct
              admin.getTopicManager()
                  .updateTopicRetention(rtTopic, StoreUtils.getExpectedRetentionTimeInMs(store, finalHybridConfig));
            }
          }
          return store;
        });
      }

      if (accessControlled.isPresent()) {
        admin.setAccessControl(clusterName, storeName, accessControlled.get());
      }

      if (compressionStrategy.isPresent()) {
        admin.setStoreCompressionStrategy(clusterName, storeName, compressionStrategy.get());
      }

      if (clientDecompressionEnabled.isPresent()) {
        admin.setClientDecompressionEnabled(clusterName, storeName, clientDecompressionEnabled.get());
      }

      if (chunkingEnabled.isPresent()) {
        admin.setChunkingEnabled(clusterName, storeName, chunkingEnabled.get());
      }

      if (rmdChunkingEnabled.isPresent()) {
        admin.setRmdChunkingEnabled(clusterName, storeName, rmdChunkingEnabled.get());
      }

      if (batchGetLimit.isPresent()) {
        admin.setBatchGetLimit(clusterName, storeName, batchGetLimit.get());
      }

      if (numVersionsToPreserve.isPresent()) {
        admin.setNumVersionsToPreserve(clusterName, storeName, numVersionsToPreserve.get());
      }

      if (incrementalPushEnabled.isPresent()) {
        if (incrementalPushEnabled.get()) {
          admin.enableHybridModeOrUpdateSettings(clusterName, storeName);
        }
        admin.setIncrementalPushEnabled(clusterName, storeName, incrementalPushEnabled.get());
      }

      if (separateRealTimeTopicEnabled.isPresent()) {
        admin.setSeparateRealTimeTopicEnabled(clusterName, storeName, separateRealTimeTopicEnabled.get());
      }

      if (replicationFactor.isPresent()) {
        admin.setReplicationFactor(clusterName, storeName, replicationFactor.get());
      }

      if (storeMigration.isPresent()) {
        admin.setStoreMigration(clusterName, storeName, storeMigration.get());
      }

      if (migrationDuplicateStore.isPresent()) {
        admin.setMigrationDuplicateStore(clusterName, storeName, migrationDuplicateStore.get());
      }

      if (writeComputationEnabled.isPresent()) {
        admin.setWriteComputationEnabled(clusterName, storeName, writeComputationEnabled.get());
      }

      if (replicationMetadataVersionID.isPresent()) {
        admin.setReplicationMetadataVersionID(clusterName, storeName, replicationMetadataVersionID.get());
      }

      if (readComputationEnabled.isPresent()) {
        admin.setReadComputationEnabled(clusterName, storeName, readComputationEnabled.get());
      }

      if (nativeReplicationEnabled.isPresent()) {
        admin.setNativeReplicationEnabled(clusterName, storeName, nativeReplicationEnabled.get());
      }

      if (activeActiveReplicationEnabled.isPresent()) {
        admin.setActiveActiveReplicationEnabled(clusterName, storeName, activeActiveReplicationEnabled.get());
      }

      if (pushStreamSourceAddress.isPresent()) {
        admin.setPushStreamSourceAddress(clusterName, storeName, pushStreamSourceAddress.get());
      }

      if (backupStrategy.isPresent()) {
        admin.setBackupStrategy(clusterName, storeName, backupStrategy.get());
      }

      if (ingestionPausedRegions.isPresent() && !ingestionPauseMode.isPresent()) {
        throw new VeniceHttpException(
            HttpStatus.SC_BAD_REQUEST,
            "--ingestion-paused-regions requires --ingestion-pause-mode to be set",
            ErrorType.BAD_REQUEST);
      }

      ingestionPauseMode.ifPresent(mode -> {
        List<String> regions = ingestionPausedRegions.orElse(Collections.emptyList());
        List<String> persistedRegions = mode == IngestionPauseMode.NOT_PAUSED ? Collections.emptyList() : regions;
        if (admin.isParent()) {
          // Parent controller persists the full pause state (mode + regions) as source of truth.
          // Push creation is blocked on the parent whenever any pause is configured, regardless
          // of which regions are targeted, to minimize impact when ingestion resumes.
          admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
            store.setIngestionPauseMode(mode);
            store.setIngestionPausedRegions(persistedRegions);
            return store;
          });
        } else {
          // Child controller only applies the pause mode locally if regions list is empty
          // (all regions) or this controller's region is in the list. The filtered mode is what
          // the servers in this region read to decide whether to pause their Kafka consumers.
          // The regions list itself is still persisted for observability (so operators can see
          // the full pause state from any controller).
          boolean appliesToThisRegion = regions.isEmpty() || regions.contains(admin.getRegionName());
          IngestionPauseMode effectiveMode = appliesToThisRegion ? mode : IngestionPauseMode.NOT_PAUSED;
          admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
            store.setIngestionPauseMode(effectiveMode);
            store.setIngestionPausedRegions(persistedRegions);
            return store;
          });
        }
      });

      storageMode.ifPresent(mode -> admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
        // storageMode on UpdateStore is a store-level default; existing versions are unaffected.
        // The default is copied to StoreVersion.storageMode at version-creation time
        // (AbstractStore.addVersion).
        store.setStorageMode(mode);
        return store;
      }));

      externalStorageReadMode
          .ifPresent(mode -> admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
            store.setExternalStorageReadMode(mode);
            return store;
          }));

      if (storeLifecycleHooks.isPresent()) {
        List<LifecycleHooksRecord> validatedStoreLifecycleHooks =
            StoreLifecycleHooksPolicy.validateLifecycleHooks(originalStore, storeLifecycleHooks);
        admin.setStoreLifecycleHooks(clusterName, storeName, validatedStoreLifecycleHooks);
      }

      autoSchemaRegisterPushJobEnabled
          .ifPresent(value -> admin.setAutoSchemaRegisterPushJobEnabled(clusterName, storeName, value));
      hybridStoreDiskQuotaEnabled
          .ifPresent(value -> admin.setHybridStoreDiskQuotaEnabled(clusterName, storeName, value));
      if (regularVersionETLEnabled.isPresent() || futureVersionETLEnabled.isPresent()
          || etledUserProxyAccount.isPresent() || etlStrategy.isPresent() || etlActiveFabrics.isPresent()) {
        ETLStoreConfig etlStoreConfig = new ETLStoreConfigImpl(
            etledUserProxyAccount.orElseGet(() -> originalStore.getEtlStoreConfig().getEtledUserProxyAccount()),
            regularVersionETLEnabled.orElseGet(() -> originalStore.getEtlStoreConfig().isRegularVersionETLEnabled()),
            futureVersionETLEnabled.orElseGet(() -> originalStore.getEtlStoreConfig().isFutureVersionETLEnabled()),
            etlStrategy.orElseGet(() -> originalStore.getEtlStoreConfig().getETLStrategy()).getValue(),
            etlActiveFabrics.orElseGet(() -> originalStore.getEtlStoreConfig().getEtlActiveFabrics()));
        admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
          store.setEtlStoreConfig(etlStoreConfig);
          return store;
        });
        if (admin.getExternalETLService().isPresent() && !admin.isParent()) {
          boolean isSourceCluster = !originalStore.isMigrating();
          if (!isSourceCluster) {
            isSourceCluster = admin.getHelixVeniceClusterResources(clusterName).isSourceCluster(clusterName, storeName);
          }
          ETLStoreConfig oldETLStoreConfig = originalStore.getEtlStoreConfig();
          String localFabric = admin.getMultiClusterConfigs().getRegionName();
          // false when oldETLStoreConfig is null (no prior ETL config = wasn't active anywhere).
          boolean isActiveInOldConfig = oldETLStoreConfig != null
              && VeniceHelixAdmin.isFabricInActiveList(oldETLStoreConfig.getEtlActiveFabrics(), localFabric);
          boolean isActiveInNewConfig =
              VeniceHelixAdmin.isFabricInActiveList(etlStoreConfig.getEtlActiveFabrics(), localFabric);
          if (etlStrategy.isPresent() && etlStrategy.get() == VeniceETLStrategy.EXTERNAL_WITH_VENICE_TRIGGER) {
            boolean firstTimeEnablingETLWithVeniceTrigger = oldETLStoreConfig == null
                || oldETLStoreConfig.getETLStrategy() != VeniceETLStrategy.EXTERNAL_WITH_VENICE_TRIGGER;
            if (firstTimeEnablingETLWithVeniceTrigger && isSourceCluster && isActiveInNewConfig) {
              // This is first time setting ETL strategy to EXTERNAL_WITH_VENICE_TRIGGER, so trigger ETL for all
              // relevant
              // store versions (current and future).
              admin.onboardETLForExistingStoreVersion(new ReadOnlyStore(originalStore));
            }
          }
          if (isSourceCluster) {
            boolean oldStrategyIsVeniceTrigger = oldETLStoreConfig != null
                && oldETLStoreConfig.getETLStrategy() == VeniceETLStrategy.EXTERNAL_WITH_VENICE_TRIGGER;
            boolean oldETLEnabled = oldETLStoreConfig != null
                && (oldETLStoreConfig.isRegularVersionETLEnabled() || oldETLStoreConfig.isFutureVersionETLEnabled());
            boolean newStrategyIsVeniceTrigger =
                etlStoreConfig.getETLStrategy() == VeniceETLStrategy.EXTERNAL_WITH_VENICE_TRIGGER;
            boolean newETLDisabled =
                !etlStoreConfig.isRegularVersionETLEnabled() && !etlStoreConfig.isFutureVersionETLEnabled();
            // Offboard when old strategy was EXTERNAL_WITH_VENICE_TRIGGER, and either:
            // 1. The strategy is changing away from EXTERNAL_WITH_VENICE_TRIGGER, or
            // 2. ETL is being disabled (both flags set to false) while strategy remains EXTERNAL_WITH_VENICE_TRIGGER
            boolean shouldOffboard =
                oldStrategyIsVeniceTrigger && (!newStrategyIsVeniceTrigger || (newETLDisabled && oldETLEnabled));
            if (shouldOffboard && isActiveInOldConfig) {
              try {
                admin.offboardETLForExistingStoreVersion(new ReadOnlyStore(originalStore));
              } catch (Exception e) {
                LOGGER.warn("Failed to offboard ETL for store: {} in cluster: {}, skipping", storeName, clusterName, e);
              }
            }
            // Fabric-list-only transition: strategy is venice_trigger and ETL is enabled before AND
            // after, but this fabric's membership in the allowlist flipped. Onboard if just added,
            // offboard if just removed.
            if (oldStrategyIsVeniceTrigger && newStrategyIsVeniceTrigger && oldETLEnabled && !newETLDisabled) {
              if (!isActiveInOldConfig && isActiveInNewConfig) {
                admin.onboardETLForExistingStoreVersion(new ReadOnlyStore(originalStore));
              } else if (isActiveInOldConfig && !isActiveInNewConfig) {
                try {
                  admin.offboardETLForExistingStoreVersion(new ReadOnlyStore(originalStore));
                } catch (Exception e) {
                  LOGGER.warn(
                      "Failed to offboard ETL on fabric-list change for store: {} in cluster: {}, skipping",
                      storeName,
                      clusterName,
                      e);
                }
              }
            }
          }
        }
      }
      if (backupVersionRetentionMs.isPresent()) {
        admin.setBackupVersionRetentionMs(clusterName, storeName, backupVersionRetentionMs.get());
      }

      if (nativeReplicationSourceFabric.isPresent()) {
        admin.setNativeReplicationSourceFabric(clusterName, storeName, nativeReplicationSourceFabric.get());
      }

      if (params.disableMetaStore().isPresent() && params.disableMetaStore().get()) {
        admin.disableMetaSystemStore(clusterName, storeName);
      }

      if (params.disableDavinciPushStatusStore().isPresent() && params.disableDavinciPushStatusStore().get()) {
        admin.disableDavinciPushStatusStore(clusterName, storeName);
      }

      if (personaName.isPresent()) {
        StoragePersonaRepository repository =
            admin.getHelixVeniceClusterResources(clusterName).getStoragePersonaRepository();
        repository.addStoresToPersona(personaName.get(), Arrays.asList(storeName));
      }

      if (storeViews.isPresent()) {
        admin.addStoreViews(clusterName, storeName, storeViews.get());
      }

      if (latestSupersetSchemaId.isPresent()) {
        admin.setLatestSupersetSchemaId(clusterName, storeName, latestSupersetSchemaId.get());
      }

      compactionEnabled.ifPresent(aBoolean -> admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
        store.setCompactionEnabled(aBoolean);
        return store;
      }));

      compactionThresholdMilliseconds
          .ifPresent(aLong -> admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
            store.setCompactionThresholdMilliseconds(aLong);
            return store;
          }));

      encryptionEnabled.ifPresent(aBoolean -> admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
        store.setEncryptionEnabled(aBoolean);
        return store;
      }));

      if (minCompactionLagSeconds.isPresent()) {
        admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
          store.setMinCompactionLagSeconds(minCompactionLagSeconds.get());
          return store;
        });
      }

      if (maxCompactionLagSeconds.isPresent()) {
        admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
          store.setMaxCompactionLagSeconds(maxCompactionLagSeconds.get());
          return store;
        });
      }

      maxRecordSizeBytes.ifPresent(aInt -> admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
        store.setMaxRecordSizeBytes(aInt);
        return store;
      }));

      maxNearlineRecordSizeBytes
          .ifPresent(aInt -> admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
            store.setMaxNearlineRecordSizeBytes(aInt);
            return store;
          }));

      unusedSchemaDeletionEnabled
          .ifPresent(aBoolean -> admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
            store.setUnusedSchemaDeletionEnabled(aBoolean);
            return store;
          }));

      storageNodeReadQuotaEnabled
          .ifPresent(aBoolean -> admin.setStorageNodeReadQuotaEnabled(clusterName, storeName, aBoolean));

      blobTransferEnabled
          .ifPresent(aBoolean -> admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
            store.setBlobTransferEnabled(aBoolean);
            return store;
          }));

      blobTransferInServerEnabled
          .ifPresent(aString -> admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
            store.setBlobTransferInServerEnabled(aString);
            return store;
          }));

      blobDbEnabled.ifPresent(aString -> admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
        store.setBlobDbEnabled(aString);
        return store;
      }));

      nearlineProducerCompressionEnabled
          .ifPresent(aBoolean -> admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
            store.setNearlineProducerCompressionEnabled(aBoolean);
            return store;
          }));

      nearlineProducerCountPerWriter
          .ifPresent(aInt -> admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
            store.setNearlineProducerCountPerWriter(aInt);
            return store;
          }));

      targetSwapRegion.ifPresent(aString -> admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
        store.setTargetSwapRegion((aString));
        return store;
      }));

      targetSwapRegionWaitTime
          .ifPresent(aInt -> admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
            store.setTargetSwapRegionWaitTime(aInt);
            return store;
          }));

      isDavinciHeartbeatReported
          .ifPresent(aBool -> admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
            store.setIsDavinciHeartbeatReported(aBool);
            return store;
          }));

      if (targetRegionPromoted.orElse(false)) {
        // Best-effort pre-check: only call storeMetadataUpdate when there is a future version that
        // has not yet been promoted. Skips the ZK write when the version is missing (inner lambda
        // would be a no-op) or already promoted. The inner lambda re-checks atomically for correctness.
        Store currentStore = admin.getStore(clusterName, storeName);
        int futureVersionNum = currentStore.getLargestUsedVersionNumber();
        Version currentFutureVersion = currentStore.getVersion(futureVersionNum);
        if (currentFutureVersion != null && !currentFutureVersion.isTargetRegionPromoted()) {
          admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
            int versionNum = store.getLargestUsedVersionNumber();
            Version futureVersion = store.getVersion(versionNum);
            if (futureVersion != null && !futureVersion.isTargetRegionPromoted()) {
              // Use store.setVersionTargetRegionPromoted rather than futureVersion.setTargetRegionPromoted
              // because getVersion() returns a ReadOnlyVersion wrapper whose setters throw.
              // The store-level method uses storeVersionsSupplier.getForUpdate() to bypass that wrapper.
              store.setVersionTargetRegionPromoted(versionNum, true);
            }
            return store;
          });
        }
      }

      globalRtDivEnabled.ifPresent(aBool -> admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
        store.setGlobalRtDivEnabled(aBool);
        return store;
      }));

      ttlRepushEnabled.ifPresent(aBool -> admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
        store.setTTLRepushEnabled(aBool);
        return store;
      }));

      enumSchemaEvolutionAllowed
          .ifPresent(aBool -> admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
            store.setEnumSchemaEvolutionAllowed(aBool);
            return store;
          }));

      flinkVeniceViewsEnabled
          .ifPresent(aBool -> admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
            store.setFlinkVeniceViewsEnabled(aBool);
            return store;
          }));

      previousCurrentVersion
          .ifPresent(version -> admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> {
            store.setPreviousCurrentVersion(version);
            return store;
          }));

      LOGGER.info("Finished updating store: {} in cluster: {}", storeName, clusterName);
    } catch (VeniceException e) {
      LOGGER.error(
          "Caught exception when updating store: {} in cluster: {}. Will attempt to rollback changes.",
          storeName,
          clusterName,
          e);
      // rollback to original store
      admin.storeMetadataUpdate(clusterName, storeName, (store, resources) -> originalStore);
      PubSubTopic rtTopic = admin.getPubSubTopicRepository().getTopic(Utils.getRealTimeTopicName(originalStore));
      if (originalStore.isHybrid() && newHybridStoreConfig.isPresent()
          && admin.getTopicManager().containsTopicAndAllPartitionsAreOnline(rtTopic)) {
        // Ensure the topic retention is rolled back too
        admin.getTopicManager()
            .updateTopicRetention(
                rtTopic,
                StoreUtils.getExpectedRetentionTimeInMs(originalStore, originalStore.getHybridStoreConfig()));
      }
      LOGGER.info(
          "Successfully rolled back changes to store: {} in cluster: {}. Will now throw the original exception: {}.",
          storeName,
          clusterName,
          e.getClass().getSimpleName());
      throw e;
    }
  }

  /**
   * Encryption enable/disable policy:
   * <ul>
   *   <li>Once a store has encryption enabled, it cannot be disabled.</li>
   *   <li>Encryption can only be enabled for a store that lives in an encryption cluster.</li>
   * </ul>
   * System stores are exempt.
   */
  static void validateEncryptionEnabledUpdate(
      Store currStore,
      boolean isEncryptionCluster,
      boolean requestedEncryptionEnabled,
      String clusterName,
      String storeName) {
    if (currStore.isSystemStore()) {
      return;
    }
    if (!requestedEncryptionEnabled && currStore.isEncryptionEnabled()) {
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          "Cannot disable encryption for store " + storeName + " in cluster " + clusterName
              + " once it has been enabled.",
          ErrorType.BAD_REQUEST);
    }
    if (requestedEncryptionEnabled && !isEncryptionCluster) {
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          "Cannot enable encryption for store " + storeName + " because cluster " + clusterName
              + " is not an encryption cluster.",
          ErrorType.BAD_REQUEST);
    }
  }

  /**
   * Parent-side update-store: builds an UPDATE_STORE admin message that gets written to the admin
   * channel for child controllers to consume. Lifted from the body inside
   * {@code VeniceParentHelixAdmin.updateStore}'s {@code acquireAdminMessageLock} try-finally; the
   * lock acquire/release stays in the public wrapper there.
   */
  public static void applyOnParent(
      VeniceParentHelixAdmin admin,
      String clusterName,
      String storeName,
      UpdateStoreQueryParams params) {
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
    Optional<Integer> largestUsedRTVersionNumber = params.getLargestUsedRTVersionNumber();
    Optional<Long> hybridRewindSeconds = params.getHybridRewindSeconds();
    Optional<Long> hybridOffsetLagThreshold = params.getHybridOffsetLagThreshold();
    Optional<Long> hybridTimeLagThreshold = params.getHybridTimeLagThreshold();
    Optional<DataReplicationPolicy> hybridDataReplicationPolicy = params.getHybridDataReplicationPolicy();
    Optional<BufferReplayPolicy> hybridBufferReplayPolicy = params.getHybridBufferReplayPolicy();
    Optional<String> realTimeTopicName = params.getRealTimeTopicName();
    Optional<Boolean> accessControlled = params.getAccessControlled();
    Optional<CompressionStrategy> compressionStrategy = params.getCompressionStrategy();
    Optional<Boolean> clientDecompressionEnabled = params.getClientDecompressionEnabled();
    Optional<Boolean> chunkingEnabled = params.getChunkingEnabled();
    Optional<Boolean> rmdChunkingEnabled = params.getRmdChunkingEnabled();
    Optional<Integer> batchGetLimit = params.getBatchGetLimit();
    Optional<Integer> numVersionsToPreserve = params.getNumVersionsToPreserve();
    Optional<Boolean> incrementalPushEnabled = params.getIncrementalPushEnabled();
    Optional<Boolean> separateRealTimeTopicEnabled = params.getSeparateRealTimeTopicEnabled();
    Optional<Boolean> storeMigration = params.getStoreMigration();
    Optional<Boolean> writeComputationEnabled = params.getWriteComputationEnabled();
    Optional<Integer> replicationMetadataVersionID = params.getReplicationMetadataVersionID();
    Optional<Boolean> readComputationEnabled = params.getReadComputationEnabled();
    Optional<Integer> bootstrapToOnlineTimeoutInHours = params.getBootstrapToOnlineTimeoutInHours();
    Optional<BackupStrategy> backupStrategy = params.getBackupStrategy();
    Optional<IngestionPauseMode> ingestionPauseMode = params.getIngestionPauseMode();
    Optional<List<String>> ingestionPausedRegions = params.getIngestionPausedRegions();
    Optional<StorageMode> storageMode = params.getStorageMode();
    Optional<ExternalStorageReadMode> externalStorageReadMode = params.getExternalStorageReadMode();
    Optional<Boolean> autoSchemaRegisterPushJobEnabled = params.getAutoSchemaRegisterPushJobEnabled();
    Optional<Boolean> hybridStoreDiskQuotaEnabled = params.getHybridStoreDiskQuotaEnabled();
    Optional<Boolean> regularVersionETLEnabled = params.getRegularVersionETLEnabled();
    Optional<Boolean> futureVersionETLEnabled = params.getFutureVersionETLEnabled();
    Optional<String> etledUserProxyAccount = params.getETLedProxyUserAccount();
    Optional<VeniceETLStrategy> etlStrategy = params.getETLStrategy();
    Optional<List<String>> etlActiveFabrics = params.getEtlActiveFabrics();
    Optional<Boolean> nativeReplicationEnabled = params.getNativeReplicationEnabled();
    Optional<String> pushStreamSourceAddress = params.getPushStreamSourceAddress();
    Optional<Long> backupVersionRetentionMs = params.getBackupVersionRetentionMs();
    Optional<Integer> replicationFactor = params.getReplicationFactor();
    Optional<Boolean> migrationDuplicateStore = params.getMigrationDuplicateStore();
    Optional<String> nativeReplicationSourceFabric = params.getNativeReplicationSourceFabric();
    Optional<Boolean> activeActiveReplicationEnabled = params.getActiveActiveReplicationEnabled();
    Optional<String> regionsFilter = params.getRegionsFilter();
    Optional<String> personaName = params.getStoragePersona();
    Optional<Map<String, String>> storeViewConfig = params.getStoreViews();
    Optional<String> viewName = params.getViewName();
    Optional<String> viewClassName = params.getViewClassName();
    Optional<Map<String, String>> viewParams = params.getViewClassParams();
    Optional<Boolean> removeView = params.getDisableStoreView();
    Optional<Integer> latestSupersetSchemaId = params.getLatestSupersetSchemaId();
    Optional<Boolean> unusedSchemaDeletionEnabled = params.getUnusedSchemaDeletionEnabled();
    Optional<List<LifecycleHooksRecord>> storeLifecycleHooks = params.getStoreLifecycleHooks();
    Optional<Boolean> flinkVeniceViewsEnabled = params.getFlinkVeniceViewsEnabled();

    /**
     * Check whether parent controllers will only propagate the update configs to child controller, or all unchanged
     * configs should be replicated to children too.
     */
    Optional<Boolean> replicateAll = params.getReplicateAllConfigs();
    Optional<Boolean> storageNodeReadQuotaEnabled = params.getStorageNodeReadQuotaEnabled();
    Optional<Boolean> compactionEnabled = params.getCompactionEnabled();
    Optional<Long> compactionThreshold = params.getCompactionThresholdMilliseconds();
    Optional<Boolean> encryptionEnabled = params.getEncryptionEnabled();
    Optional<Long> minCompactionLagSeconds = params.getMinCompactionLagSeconds();
    Optional<Long> maxCompactionLagSeconds = params.getMaxCompactionLagSeconds();
    Optional<Integer> maxRecordSizeBytes = params.getMaxRecordSizeBytes();
    Optional<Integer> maxNearlineRecordSizeBytes = params.getMaxNearlineRecordSizeBytes();
    boolean replicateAllConfigs = replicateAll.isPresent() && replicateAll.get();
    List<CharSequence> updatedConfigsList = new LinkedList<>();
    String errorMessagePrefix = "Store update error for " + storeName + " in cluster: " + clusterName + ": ";

    Store currStore = admin.getVeniceHelixAdmin().getStore(clusterName, storeName);
    if (currStore == null) {
      LOGGER.error(errorMessagePrefix + "store does not exist, and thus cannot be updated.");
      throw new VeniceNoStoreException(storeName, clusterName);
    }
    if (!storeMigration.orElse(false)) {
      encryptionEnabled.ifPresent(
          aBoolean -> validateEncryptionEnabledUpdate(
              currStore,
              admin.getControllerConfig(clusterName).isEncryptionCluster(),
              aBoolean,
              clusterName,
              storeName));
    }
    UpdateStore setStore = (UpdateStore) AdminMessageType.UPDATE_STORE.getNewInstance();
    setStore.clusterName = clusterName;
    setStore.storeName = storeName;
    setStore.owner = owner.map(admin.addToUpdatedConfigList(updatedConfigsList, OWNER)).orElseGet(currStore::getOwner);

    boolean isUpdateForStoreMigration = storeMigration.orElse(false);
    if (!isUpdateForStoreMigration && !currStore.isHybrid()
        && (hybridRewindSeconds.isPresent() || hybridOffsetLagThreshold.isPresent())) {
      // Today target colo pushjob cannot handle hybrid stores, so if a batch push is running, fail the request
      Optional<String> currentPushTopic = admin.getTopicForCurrentPushJob(clusterName, storeName, false, false);
      if (currentPushTopic.isPresent()) {
        String errorMessage =
            "Cannot convert to hybrid as there is already a pushjob running with topic " + currentPushTopic.get();
        LOGGER.error(errorMessage);
        throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, errorMessage, ErrorType.BAD_REQUEST);
      }
    }
    // Invalid config update on hybrid will not be populated to admin channel so subsequent updates on the store won't
    // be blocked by retry mechanism.
    if (currStore.isHybrid() && (partitionerClass.isPresent() || partitionerParams.isPresent())) {
      String errorMessage = errorMessagePrefix + "Cannot change partitioner class and parameters for hybrid stores";
      LOGGER.error(errorMessage);
      throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, errorMessage, ErrorType.BAD_REQUEST);
    }

    if (partitionCount.isPresent()) {
      admin.getVeniceHelixAdmin().preCheckStorePartitionCountUpdate(clusterName, currStore, partitionCount.get());
      setStore.partitionNum = partitionCount.get();
      updatedConfigsList.add(PARTITION_COUNT);
    } else {
      setStore.partitionNum = currStore.getPartitionCount();
    }

    /**
     * TODO: We should build an UpdateStoreHelper that takes current store config and update command as input, and
     *       return whether the update command is valid.
     */
    admin.validateActiveActiveReplicationEnableConfigs(
        activeActiveReplicationEnabled,
        nativeReplicationEnabled,
        currStore);

    setStore.nativeReplicationEnabled =
        nativeReplicationEnabled.map(admin.addToUpdatedConfigList(updatedConfigsList, NATIVE_REPLICATION_ENABLED))
            .orElseGet(currStore::isNativeReplicationEnabled);
    setStore.pushStreamSourceAddress =
        pushStreamSourceAddress.map(admin.addToUpdatedConfigList(updatedConfigsList, PUSH_STREAM_SOURCE_ADDRESS))
            .orElseGet(currStore::getPushStreamSourceAddress);

    if (storeViewConfig.isPresent() && viewName.isPresent()) {
      throw new VeniceException("Cannot update a store view and overwrite store view setup together!");
    }
    if (viewName.isPresent()) {
      Map<String, StoreViewConfigRecord> updatedViewSettings;
      if (!removeView.isPresent()) {
        if (!viewClassName.isPresent()) {
          throw new VeniceException("View class name is required when configuring a view.");
        }
        // If View parameter is not provided, use emtpy map instead. It does not inherit from existing config.
        ViewConfig viewConfig = new ViewConfigImpl(viewClassName.get(), viewParams.orElseGet(Collections::emptyMap));
        ViewConfig validatedViewConfig =
            admin.validateAndDecorateStoreViewConfig(currStore, viewConfig, viewName.get());
        updatedViewSettings =
            VeniceHelixAdmin.addNewViewConfigsIntoOldConfigs(currStore, viewName.get(), validatedViewConfig);
      } else {
        updatedViewSettings = VeniceHelixAdmin.removeViewConfigFromStoreViewConfigMap(currStore, viewName.get());
      }
      setStore.views = updatedViewSettings;
      updatedConfigsList.add(STORE_VIEW);
    }

    if (storeViewConfig.isPresent()) {
      // Validate and overwrite store views if they're getting set
      Map<String, ViewConfig> validatedViewConfigs =
          admin.validateAndDecorateStoreViewConfigs(storeViewConfig.get(), currStore);
      setStore.views = StoreViewUtils.convertViewConfigMapToStoreViewRecordMap(validatedViewConfigs);
      updatedConfigsList.add(STORE_VIEW);
    }

    if (flinkVeniceViewsEnabled.isPresent()) {
      setStore.flinkVeniceViewsEnabled = flinkVeniceViewsEnabled.get();
      updatedConfigsList.add(FLINK_VENICE_VIEWS_ENABLED);
    }

    // Only update fields that are set, other fields will be read from the original store's partitioner config.
    PartitionerConfig updatedPartitionerConfig = PartitionerConfigPolicy
        .mergeNewSettingsIntoOldPartitionerConfig(currStore, partitionerClass, partitionerParams, amplificationFactor);
    if (partitionerClass.isPresent() || partitionerParams.isPresent() || amplificationFactor.isPresent()) {
      // Update updatedConfigsList.
      partitionerClass.ifPresent(p -> updatedConfigsList.add(PARTITIONER_CLASS));
      partitionerParams.ifPresent(p -> updatedConfigsList.add(PARTITIONER_PARAMS));
      amplificationFactor.ifPresent(p -> updatedConfigsList.add(AMPLIFICATION_FACTOR));
      // Create PartitionConfigRecord for admin channel transmission.
      PartitionerConfigRecord partitionerConfigRecord = new PartitionerConfigRecord();
      partitionerConfigRecord.partitionerClass = updatedPartitionerConfig.getPartitionerClass();
      partitionerConfigRecord.partitionerParams =
          CollectionUtils.getCharSequenceMapFromStringMap(updatedPartitionerConfig.getPartitionerParams());
      partitionerConfigRecord.amplificationFactor = updatedPartitionerConfig.getAmplificationFactor();
      // Before setting partitioner config, verify the updated partitionerConfig can be built
      try {
        PartitionUtils.getVenicePartitioner(
            partitionerConfigRecord.partitionerClass.toString(),
            VeniceProperties.fromCharSequenceMap(partitionerConfigRecord.partitionerParams),
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
      setStore.partitionerConfig = partitionerConfigRecord;
    }

    List<LifecycleHooksRecord> newLifecycleHooks =
        StoreLifecycleHooksPolicy.validateLifecycleHooks(currStore, storeLifecycleHooks);
    if (newLifecycleHooks.isEmpty()) {
      setStore.storeLifecycleHooks = Collections.emptyList();
    } else {
      List<StoreLifecycleHooksRecord> convertedLifecycleHooks = new ArrayList<>();
      VeniceProperties controllerProps =
          admin.getVeniceHelixAdmin().getMultiClusterConfigs().getCommonConfig().getProps();
      for (LifecycleHooksRecord record: newLifecycleHooks) {
        StoreLifecycleHooks storeLifecycleHook;
        try {
          storeLifecycleHook = ReflectUtils.callConstructor(
              ReflectUtils.loadClass(record.getStoreLifecycleHooksClassName()),
              new Class<?>[] { VeniceProperties.class },
              new Object[] { controllerProps });
        } catch (Exception e) {
          throw new VeniceException(
              "Failed to instantiate lifecycle hook class: " + record.getStoreLifecycleHooksClassName(),
              e);
        }
        if (storeLifecycleHook.validateHookParams(
            clusterName,
            storeName,
            record.getStoreLifecycleHooksParams()) != StoreLifecycleEventOutcome.PROCEED) {
          throw new VeniceException("Params for store lifecycle hooks is invalid");
        }
        convertedLifecycleHooks.add(
            new StoreLifecycleHooksRecord(
                record.getStoreLifecycleHooksClassName(),
                CollectionUtils.getCharSequenceMapFromStringMap(record.getStoreLifecycleHooksParams())));
        updatedConfigsList.add(STORE_LIFECYCLE_HOOKS_LIST);
      }
      setStore.storeLifecycleHooks = convertedLifecycleHooks;
    }

    setStore.enableReads = readability.map(admin.addToUpdatedConfigList(updatedConfigsList, ENABLE_READS))
        .orElseGet(currStore::isEnableReads);
    setStore.enableWrites = writeability.map(admin.addToUpdatedConfigList(updatedConfigsList, ENABLE_WRITES))
        .orElseGet(currStore::isEnableWrites);

    setStore.readQuotaInCU = readQuotaInCU.map(admin.addToUpdatedConfigList(updatedConfigsList, READ_QUOTA_IN_CU))
        .orElseGet(currStore::getReadQuotaInCU);

    // We need to be careful when handling currentVersion.
    // Since it is not synced between parent and local controller,
    // It is very likely to override local values unintentionally.
    setStore.currentVersion =
        currentVersion.map(admin.addToUpdatedConfigList(updatedConfigsList, VERSION)).orElse(IGNORED_CURRENT_VERSION);

    VeniceControllerClusterConfig controllerConfig =
        admin.getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName).getConfig();
    boolean storeBeingConvertedToHybrid = HybridStoreConfigPolicy.applyHybridAndReplicationConfigUpdates(
        storeName,
        currStore,
        setStore,
        controllerConfig,
        hybridRewindSeconds,
        hybridOffsetLagThreshold,
        hybridTimeLagThreshold,
        hybridDataReplicationPolicy,
        hybridBufferReplayPolicy,
        realTimeTopicName,
        activeActiveReplicationEnabled,
        incrementalPushEnabled,
        updatedConfigsList,
        LOGGER);

    /**
     * Set storage quota according to store properties. For hybrid stores, rocksDB has the overhead ratio as we
     * do append-only and compaction will happen later.
     * We expose actual disk usage to users, instead of multiplying/dividing the overhead ratio by situations.
     */
    setStore.storageQuotaInByte =
        storageQuotaInByte.map(admin.addToUpdatedConfigList(updatedConfigsList, STORAGE_QUOTA_IN_BYTE))
            .orElseGet(currStore::getStorageQuotaInByte);

    setStore.accessControlled =
        accessControlled.map(admin.addToUpdatedConfigList(updatedConfigsList, ACCESS_CONTROLLED))
            .orElseGet(currStore::isAccessControlled);
    setStore.compressionStrategy =
        compressionStrategy.map(admin.addToUpdatedConfigList(updatedConfigsList, COMPRESSION_STRATEGY))
            .map(CompressionStrategy::getValue)
            .orElseGet(() -> currStore.getCompressionStrategy().getValue());
    setStore.clientDecompressionEnabled =
        clientDecompressionEnabled.map(admin.addToUpdatedConfigList(updatedConfigsList, CLIENT_DECOMPRESSION_ENABLED))
            .orElseGet(currStore::getClientDecompressionEnabled);
    setStore.batchGetLimit = batchGetLimit.map(admin.addToUpdatedConfigList(updatedConfigsList, BATCH_GET_LIMIT))
        .orElseGet(currStore::getBatchGetLimit);
    setStore.numVersionsToPreserve =
        numVersionsToPreserve.map(admin.addToUpdatedConfigList(updatedConfigsList, NUM_VERSIONS_TO_PRESERVE))
            .orElseGet(currStore::getNumVersionsToPreserve);
    setStore.isMigrating = storeMigration
        .map(VeniceParentHelixAdmin.addToUpdatedConfigList(updatedConfigsList, STORE_MIGRATION, ENABLE_STORE_MIGRATION))

        .orElseGet(currStore::isMigrating);
    setStore.replicationMetadataVersionID = replicationMetadataVersionID
        .map(admin.addToUpdatedConfigList(updatedConfigsList, REPLICATION_METADATA_PROTOCOL_VERSION_ID))
        .orElseGet(currStore::getRmdVersion);
    setStore.readComputationEnabled =
        readComputationEnabled.map(admin.addToUpdatedConfigList(updatedConfigsList, READ_COMPUTATION_ENABLED))
            .orElseGet(currStore::isReadComputationEnabled);
    setStore.bootstrapToOnlineTimeoutInHours = bootstrapToOnlineTimeoutInHours
        .map(admin.addToUpdatedConfigList(updatedConfigsList, BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS))
        .orElseGet(currStore::getBootstrapToOnlineTimeoutInHours);
    setStore.leaderFollowerModelEnabled = true; // do not mess up during upgrades
    setStore.backupStrategy = (backupStrategy.map(admin.addToUpdatedConfigList(updatedConfigsList, BACKUP_STRATEGY))
        .orElseGet(currStore::getBackupStrategy)).ordinal();

    setStore.schemaAutoRegisterFromPushJobEnabled = autoSchemaRegisterPushJobEnabled
        .map(admin.addToUpdatedConfigList(updatedConfigsList, AUTO_SCHEMA_REGISTER_FOR_PUSHJOB_ENABLED))
        .orElseGet(currStore::isSchemaAutoRegisterFromPushJobEnabled);

    setStore.hybridStoreDiskQuotaEnabled = hybridStoreDiskQuotaEnabled
        .map(admin.addToUpdatedConfigList(updatedConfigsList, HYBRID_STORE_DISK_QUOTA_ENABLED))
        .orElseGet(currStore::isHybridStoreDiskQuotaEnabled);

    regularVersionETLEnabled.map(admin.addToUpdatedConfigList(updatedConfigsList, REGULAR_VERSION_ETL_ENABLED));
    futureVersionETLEnabled.map(admin.addToUpdatedConfigList(updatedConfigsList, FUTURE_VERSION_ETL_ENABLED));
    etledUserProxyAccount.map(admin.addToUpdatedConfigList(updatedConfigsList, ETLED_PROXY_USER_ACCOUNT));
    etlStrategy.map(admin.addToUpdatedConfigList(updatedConfigsList, ETL_STRATEGY));
    etlActiveFabrics.map(admin.addToUpdatedConfigList(updatedConfigsList, ETL_ACTIVE_FABRICS));
    setStore.ETLStoreConfig = admin.mergeNewSettingIntoOldETLStoreConfig(
        currStore,
        regularVersionETLEnabled,
        futureVersionETLEnabled,
        etledUserProxyAccount,
        etlStrategy,
        etlActiveFabrics);

    setStore.largestUsedVersionNumber =
        largestUsedVersionNumber.map(admin.addToUpdatedConfigList(updatedConfigsList, LARGEST_USED_VERSION_NUMBER))
            .orElseGet(currStore::getLargestUsedVersionNumber);

    setStore.largestUsedRTVersionNumber =
        largestUsedRTVersionNumber.map(admin.addToUpdatedConfigList(updatedConfigsList, LARGEST_USED_RT_VERSION_NUMBER))
            .orElse(null);

    setStore.backupVersionRetentionMs =
        backupVersionRetentionMs.map(admin.addToUpdatedConfigList(updatedConfigsList, BACKUP_VERSION_RETENTION_MS))
            .orElseGet(currStore::getBackupVersionRetentionMs);
    setStore.replicationFactor =
        replicationFactor.map(admin.addToUpdatedConfigList(updatedConfigsList, REPLICATION_FACTOR))
            .orElseGet(currStore::getReplicationFactor);
    setStore.migrationDuplicateStore =
        migrationDuplicateStore.map(admin.addToUpdatedConfigList(updatedConfigsList, MIGRATION_DUPLICATE_STORE))
            .orElseGet(currStore::isMigrationDuplicateStore);
    setStore.nativeReplicationSourceFabric = nativeReplicationSourceFabric
        .map(admin.addToUpdatedConfigList(updatedConfigsList, NATIVE_REPLICATION_SOURCE_FABRIC))
        .orElseGet((currStore::getNativeReplicationSourceFabric));

    setStore.disableMetaStore = params.disableMetaStore()
        .map(admin.addToUpdatedConfigList(updatedConfigsList, DISABLE_META_STORE))
        .orElse(false);

    setStore.disableDavinciPushStatusStore = params.disableDavinciPushStatusStore()
        .map(admin.addToUpdatedConfigList(updatedConfigsList, DISABLE_DAVINCI_PUSH_STATUS_STORE))
        .orElse(false);

    setStore.storagePersona =
        personaName.map(admin.addToUpdatedConfigList(updatedConfigsList, PERSONA_NAME)).orElse(null);

    setStore.blobTransferEnabled = params.getBlobTransferEnabled()
        .map(admin.addToUpdatedConfigList(updatedConfigsList, BLOB_TRANSFER_ENABLED))
        .orElseGet(currStore::isBlobTransferEnabled);

    setStore.blobTransferInServerEnabled = params.getBlobTransferInServerEnabled()
        .map(admin.addToUpdatedConfigList(updatedConfigsList, BLOB_TRANSFER_IN_SERVER_ENABLED))
        .orElseGet(currStore::getBlobTransferInServerEnabled);

    setStore.blobDbEnabled = params.getBlobDbEnabled()
        .map(admin.addToUpdatedConfigList(updatedConfigsList, BLOB_DB_ENABLED))
        .orElseGet(currStore::getBlobDbEnabled);

    setStore.previousCurrentVersion = params.getPreviousCurrentVersion()
        .map(admin.addToUpdatedConfigList(updatedConfigsList, PREVIOUS_CURRENT_VERSION))
        .orElseGet(currStore::getPreviousCurrentVersion);

    setStore.separateRealTimeTopicEnabled = separateRealTimeTopicEnabled
        .map(admin.addToUpdatedConfigList(updatedConfigsList, SEPARATE_REAL_TIME_TOPIC_ENABLED))
        .orElseGet(currStore::isSeparateRealTimeTopicEnabled);

    setStore.nearlineProducerCompressionEnabled = params.getNearlineProducerCompressionEnabled()
        .map(admin.addToUpdatedConfigList(updatedConfigsList, NEARLINE_PRODUCER_COMPRESSION_ENABLED))
        .orElseGet(currStore::isNearlineProducerCompressionEnabled);

    setStore.nearlineProducerCountPerWriter = params.getNearlineProducerCountPerWriter()
        .map(admin.addToUpdatedConfigList(updatedConfigsList, NEARLINE_PRODUCER_COUNT_PER_WRITER))
        .orElseGet(currStore::getNearlineProducerCountPerWriter);

    setStore.targetSwapRegion = params.getTargetSwapRegion()
        .map(admin.addToUpdatedConfigList(updatedConfigsList, TARGET_SWAP_REGION))
        .orElse(null);

    setStore.targetSwapRegionWaitTime = params.getTargetRegionSwapWaitTime()
        .map(admin.addToUpdatedConfigList(updatedConfigsList, TARGET_SWAP_REGION_WAIT_TIME))
        .orElseGet((currStore::getTargetSwapRegionWaitTime));

    setStore.isDaVinciHeartBeatReported = params.getIsDavinciHeartbeatReported()
        .map(admin.addToUpdatedConfigList(updatedConfigsList, IS_DAVINCI_HEARTBEAT_REPORTED))
        .orElseGet((currStore::getIsDavinciHeartbeatReported));

    // targetRegionPromoted is write-only-true: only track it as an updated config when explicitly
    // set to true. An explicit false (e.g. from a replicateAllConfigs snapshot of an un-promoted
    // store) is treated as a no-op on both parent and child, so propagating it to updatedConfigsList
    // would create an "updated" config that children intentionally won't apply.
    setStore.targetRegionPromoted = params.getTargetRegionPromoted()
        .filter(Boolean::booleanValue)
        .map(admin.addToUpdatedConfigList(updatedConfigsList, TARGET_REGION_PROMOTED))
        .orElseGet(() -> {
          Version futureVersion = currStore.getVersion(currStore.getLargestUsedVersionNumber());
          return futureVersion != null && futureVersion.isTargetRegionPromoted();
        });

    setStore.globalRtDivEnabled = params.isGlobalRtDivEnabled()
        .map(admin.addToUpdatedConfigList(updatedConfigsList, GLOBAL_RT_DIV_ENABLED))
        .orElseGet((currStore::isGlobalRtDivEnabled));

    setStore.enumSchemaEvolutionAllowed = params.isEnumSchemaEvolutionAllowed()
        .map(admin.addToUpdatedConfigList(updatedConfigsList, ENUM_SCHEMA_EVOLUTION_ALLOWED))
        .orElseGet((currStore::isEnumSchemaEvolutionAllowed));

    // Check whether the passed param is valid or not
    if (latestSupersetSchemaId.isPresent()) {
      if (latestSupersetSchemaId.get() != SchemaData.INVALID_VALUE_SCHEMA_ID) {
        if (admin.getVeniceHelixAdmin().getValueSchema(clusterName, storeName, latestSupersetSchemaId.get()) == null) {
          throw new VeniceException(
              "Unknown value schema id: " + latestSupersetSchemaId.get() + " in store: " + storeName);
        }
      }
    }

    setStore.latestSuperSetValueSchemaId =
        latestSupersetSchemaId.map(admin.addToUpdatedConfigList(updatedConfigsList, LATEST_SUPERSET_SCHEMA_ID))
            .orElseGet(currStore::getLatestSuperSetValueSchemaId);
    setStore.storageNodeReadQuotaEnabled = storageNodeReadQuotaEnabled
        .map(admin.addToUpdatedConfigList(updatedConfigsList, STORAGE_NODE_READ_QUOTA_ENABLED))
        .orElseGet(currStore::isStorageNodeReadQuotaEnabled);
    setStore.unusedSchemaDeletionEnabled = unusedSchemaDeletionEnabled
        .map(admin.addToUpdatedConfigList(updatedConfigsList, UNUSED_SCHEMA_DELETION_ENABLED))
        .orElseGet(currStore::isUnusedSchemaDeletionEnabled);
    setStore.compactionEnabled =
        compactionEnabled.map(admin.addToUpdatedConfigList(updatedConfigsList, COMPACTION_ENABLED))
            .orElseGet(currStore::isCompactionEnabled);
    setStore.compactionThresholdMilliseconds =
        compactionThreshold.map(admin.addToUpdatedConfigList(updatedConfigsList, COMPACTION_THRESHOLD_MILLISECONDS))
            .orElseGet(currStore::getCompactionThresholdMilliseconds);
    setStore.encryptionEnabled =
        encryptionEnabled.map(admin.addToUpdatedConfigList(updatedConfigsList, ENCRYPTION_ENABLED))
            .orElseGet(currStore::isEncryptionEnabled);
    setStore.minCompactionLagSeconds =
        minCompactionLagSeconds.map(admin.addToUpdatedConfigList(updatedConfigsList, MIN_COMPACTION_LAG_SECONDS))
            .orElseGet(currStore::getMinCompactionLagSeconds);
    setStore.maxCompactionLagSeconds =
        maxCompactionLagSeconds.map(admin.addToUpdatedConfigList(updatedConfigsList, MAX_COMPACTION_LAG_SECONDS))
            .orElseGet(currStore::getMaxCompactionLagSeconds);
    if (setStore.maxCompactionLagSeconds < setStore.minCompactionLagSeconds) {
      throw new VeniceException(
          "Store's max compaction lag seconds: " + setStore.maxCompactionLagSeconds + " shouldn't be smaller than "
              + "store's min compaction lag seconds: " + setStore.minCompactionLagSeconds);
    }
    setStore.maxRecordSizeBytes =
        maxRecordSizeBytes.map(admin.addToUpdatedConfigList(updatedConfigsList, MAX_RECORD_SIZE_BYTES))
            .orElseGet(currStore::getMaxRecordSizeBytes);
    setStore.maxNearlineRecordSizeBytes =
        maxNearlineRecordSizeBytes.map(admin.addToUpdatedConfigList(updatedConfigsList, MAX_NEARLINE_RECORD_SIZE_BYTES))
            .orElseGet(currStore::getMaxNearlineRecordSizeBytes);

    // Key URN compression runtime logic has been removed, but the Avro UpdateStore message
    // still requires these fields to be non-null for serialization.
    setStore.keyUrnCompressionEnabled = currStore.isKeyUrnCompressionEnabled();
    setStore.keyUrnFields = currStore.getKeyUrnFields().stream().map(Objects::toString).collect(Collectors.toList());

    IngestionPauseMode resolvedPauseMode =
        ingestionPauseMode.map(admin.addToUpdatedConfigList(updatedConfigsList, INGESTION_PAUSE_MODE))
            .orElse(currStore.getIngestionPauseMode());
    setStore.ingestionPauseMode = resolvedPauseMode.getValue();
    // Auto-clear the regions list when resuming (NOT_PAUSED) so stale region data doesn't
    // leak past the resume. Otherwise, use the explicitly provided regions or fall back to
    // the current store's regions.
    List<String> resolvedRegions = resolvedPauseMode == IngestionPauseMode.NOT_PAUSED
        ? Collections.emptyList()
        : ingestionPausedRegions.map(admin.addToUpdatedConfigList(updatedConfigsList, INGESTION_PAUSED_REGIONS))
            .orElseGet(currStore::getIngestionPausedRegions);
    setStore.ingestionPausedRegions = resolvedRegions != null ? new ArrayList<>(resolvedRegions) : new ArrayList<>();

    setStore.storageMode = storageMode.map(admin.addToUpdatedConfigList(updatedConfigsList, STORAGE_MODE))
        .orElse(currStore.getStorageMode())
        .getValue();
    setStore.externalStorageReadMode =
        externalStorageReadMode.map(admin.addToUpdatedConfigList(updatedConfigsList, EXTERNAL_STORAGE_READ_MODE))
            .orElse(currStore.getExternalStorageReadMode())
            .getValue();

    StoragePersonaRepository repository =
        admin.getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName).getStoragePersonaRepository();
    StoragePersona personaToValidate = null;
    StoragePersona existingPersona = repository.getPersonaContainingStore(currStore.getName());

    if (params.getStoragePersona().isPresent()) {
      personaToValidate = admin.getVeniceHelixAdmin().getStoragePersona(clusterName, params.getStoragePersona().get());
      if (personaToValidate == null) {
        String errMsg = "UpdateStore command failed for store " + storeName + ".  The provided StoragePersona "
            + params.getStoragePersona().get() + " does not exist.";
        throw new VeniceException(errMsg);
      }
    } else if (existingPersona != null) {
      personaToValidate = existingPersona;
    }

    if (personaToValidate != null) {
      /**
       * Create a new copy of the store with an updated quota, and validate this.
       */
      Store updatedQuotaStore = admin.getVeniceHelixAdmin().getStore(clusterName, storeName);
      updatedQuotaStore.setStorageQuotaInByte(setStore.getStorageQuotaInByte());
      repository.validateAddUpdatedStore(personaToValidate, Optional.of(updatedQuotaStore));
    }

    /**
     * Fabrics filter is not a store config, so we don't need to add it into {@link UpdateStore#updatedConfigsList}
     */
    setStore.regionsFilter = regionsFilter.orElse(null);

    // Update Partial Update config.
    boolean partialUpdateConfigUpdated = ParentControllerConfigUpdateUtils.checkAndMaybeApplyPartialUpdateConfig(
        admin,
        clusterName,
        storeName,
        writeComputationEnabled,
        setStore,
        storeBeingConvertedToHybrid);
    if (partialUpdateConfigUpdated) {
      updatedConfigsList.add(WRITE_COMPUTATION_ENABLED);
    }
    boolean partialUpdateJustEnabled = setStore.writeComputationEnabled && !currStore.isWriteComputationEnabled();
    // Update Chunking config.
    boolean chunkingConfigUpdated = ParentControllerConfigUpdateUtils
        .checkAndMaybeApplyChunkingConfigChange(admin, clusterName, storeName, chunkingEnabled, setStore);
    if (chunkingConfigUpdated) {
      updatedConfigsList.add(CHUNKING_ENABLED);
    }

    // Update RMD Chunking config.
    boolean rmdChunkingConfigUpdated = ParentControllerConfigUpdateUtils
        .checkAndMaybeApplyRmdChunkingConfigChange(admin, clusterName, storeName, rmdChunkingEnabled, setStore);
    if (rmdChunkingConfigUpdated) {
      updatedConfigsList.add(RMD_CHUNKING_ENABLED);
    }

    // Validate Amplification Factor config based on latest A/A and partial update status.
    if ((setStore.getActiveActiveReplicationEnabled() || setStore.getWriteComputationEnabled())
        && updatedPartitionerConfig.getAmplificationFactor() > 1) {
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          "Non-default amplification factor is not compatible with active-active replication and/or partial update.",
          ErrorType.BAD_REQUEST);
    }

    if (!HybridStoreConfigPolicy.isHybrid(currStore.getHybridStoreConfig())
        && HybridStoreConfigPolicy.isHybrid(setStore.getHybridStoreConfig()) && setStore.getPartitionNum() == 0) {
      // This is a new hybrid store and partition count is not specified.
      VeniceControllerClusterConfig config =
          admin.getVeniceHelixAdmin().getHelixVeniceClusterResources(clusterName).getConfig();
      setStore.setPartitionNum(
          PartitionUtils.calculatePartitionCount(
              storeName,
              setStore.getStorageQuotaInByte(),
              0,
              config.getPartitionSize(),
              config.getMinNumberOfPartitionsForHybrid(),
              config.getMaxNumberOfPartitions(),
              config.isPartitionCountRoundUpEnabled(),
              config.getPartitionCountRoundUpSize()));
      LOGGER.info(
          "Enforcing default hybrid partition count:{} for a new hybrid store:{}.",
          setStore.getPartitionNum(),
          storeName);
      updatedConfigsList.add(PARTITION_COUNT);
    }

    /**
     * Pre-flight check for incremental push config update. We only allow incremental push config to be turned on
     * when store is A/A. Otherwise, we should fail store update.
     */
    if (setStore.hybridStoreConfig != null && setStore.incrementalPushEnabled
        && !setStore.activeActiveReplicationEnabled) {
      throw new VeniceHttpException(
          HttpStatus.SC_BAD_REQUEST,
          "Hybrid store config invalid. Cannot have incremental push enabled while A/A not enabled",
          ErrorType.BAD_REQUEST);
    }

    /**
     * By default, parent controllers will not try to replicate the unchanged store configs to child controllers;
     * an updatedConfigsList will be used to represent which configs are updated by users.
     */
    setStore.replicateAllConfigs = replicateAllConfigs;
    if (!replicateAllConfigs) {
      if (updatedConfigsList.isEmpty()) {
        String errMsg = "UpdateStore command failed for store " + storeName + ". The command didn't change any specific"
            + " store config and didn't specify \"--replicate-all-configs\" flag.";
        LOGGER.error(errMsg);
        throw new VeniceException(errMsg);
      }
      setStore.updatedConfigsList = new ArrayList<>(updatedConfigsList);
    } else {
      setStore.updatedConfigsList = Collections.emptyList();
    }

    final boolean readComputeJustEnabled =
        readComputationEnabled.orElse(false) && !currStore.isReadComputationEnabled();
    boolean needToGenerateSupersetSchema =
        !currStore.isSystemStore() && (readComputeJustEnabled || partialUpdateJustEnabled);
    if (needToGenerateSupersetSchema) {
      // dry run to make sure superset schema generation can work
      admin.getSupersetSchemaGenerator(clusterName)
          .generateSupersetSchemaFromSchemas(admin.getValueSchemas(clusterName, storeName));
    }

    AdminOperation message = new AdminOperation();
    message.operationType = AdminMessageType.UPDATE_STORE.getValue();
    message.payloadUnion = setStore;
    admin.sendAdminMessageAndWaitForConsumed(clusterName, storeName, message);

    if (needToGenerateSupersetSchema) {
      admin.addSupersetSchemaForStore(clusterName, storeName, currStore.isActiveActiveReplicationEnabled());
    }
    if (partialUpdateJustEnabled) {
      LOGGER.info("Enabling partial update for the first time on store: {} in cluster: {}", storeName, clusterName);
      ParentControllerConfigUpdateUtils.addUpdateSchemaForStore(admin, clusterName, storeName, false);
    }

    /**
     * If active-active replication is getting enabled for the store, generate and register the Replication metadata schema
     * for all existing value schemas.
     */
    final boolean activeActiveReplicationJustEnabled =
        activeActiveReplicationEnabled.orElse(false) && !currStore.isActiveActiveReplicationEnabled();
    if (activeActiveReplicationJustEnabled) {
      admin.updateReplicationMetadataSchemaForAllValueSchema(clusterName, storeName);
    }
  }
}
