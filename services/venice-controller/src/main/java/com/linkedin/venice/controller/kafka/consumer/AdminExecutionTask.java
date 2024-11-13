package com.linkedin.venice.controller.kafka.consumer;

import static com.linkedin.venice.controller.kafka.consumer.AdminConsumptionTask.IGNORED_CURRENT_VERSION;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.ExecutionIdAccessor;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.kafka.protocol.admin.AbortMigration;
import com.linkedin.venice.controller.kafka.protocol.admin.AddVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.ConfigureActiveActiveReplicationForCluster;
import com.linkedin.venice.controller.kafka.protocol.admin.ConfigureNativeReplicationForCluster;
import com.linkedin.venice.controller.kafka.protocol.admin.CreateStoragePersona;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteAllVersions;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteOldVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteStoragePersona;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteStore;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteUnusedValueSchemas;
import com.linkedin.venice.controller.kafka.protocol.admin.DerivedSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.DisableStoreRead;
import com.linkedin.venice.controller.kafka.protocol.admin.EnableStoreRead;
import com.linkedin.venice.controller.kafka.protocol.admin.KillOfflinePushJob;
import com.linkedin.venice.controller.kafka.protocol.admin.MetaSystemStoreAutoCreationValidation;
import com.linkedin.venice.controller.kafka.protocol.admin.MetadataSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.MigrateStore;
import com.linkedin.venice.controller.kafka.protocol.admin.PauseStore;
import com.linkedin.venice.controller.kafka.protocol.admin.PushStatusSystemStoreAutoCreationValidation;
import com.linkedin.venice.controller.kafka.protocol.admin.ResumeStore;
import com.linkedin.venice.controller.kafka.protocol.admin.RollForwardCurrentVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.RollbackCurrentVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.SetStoreCurrentVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.SetStoreOwner;
import com.linkedin.venice.controller.kafka.protocol.admin.SetStorePartitionCount;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.SupersetSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStoragePersona;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore;
import com.linkedin.venice.controller.kafka.protocol.admin.ValueSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.stats.AdminConsumptionStats;
import com.linkedin.venice.controllerapi.UpdateStoragePersonaQueryParams;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceRetriableException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.VeniceUserStoreType;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.CollectionUtils;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.logging.log4j.Logger;


/**
 * This class is used to create {@link Callable} that execute {@link AdminOperation}s for a given store.
 */
public class AdminExecutionTask implements Callable<Void> {
  /**
   * This logger is intended to be passed from the {@link AdminConsumptionTask} in order to produce logs with the
   * tag AdminConsumptionTask#CONSUMER_TASK_ID_FORMAT.
   */
  private final Logger LOGGER;
  private final String clusterName;
  private final String regionName;
  private final String storeName;
  private final Queue<AdminOperationWrapper> internalTopic;
  private final VeniceHelixAdmin admin;
  private final ExecutionIdAccessor executionIdAccessor;
  private final boolean isParentController;
  private final AdminConsumptionStats stats;
  private final ConcurrentHashMap<String, Long> lastSucceededExecutionIdMap;
  private final long lastPersistedExecutionId;

  AdminExecutionTask(
      Logger LOGGER,
      String clusterName,
      String storeName,
      ConcurrentHashMap<String, Long> lastSucceededExecutionIdMap,
      long lastPersistedExecutionId,
      Queue<AdminOperationWrapper> internalTopic,
      VeniceHelixAdmin admin,
      ExecutionIdAccessor executionIdAccessor,
      boolean isParentController,
      AdminConsumptionStats stats,
      String regionName) {
    this.LOGGER = LOGGER;
    this.clusterName = clusterName;
    this.storeName = storeName;
    this.lastSucceededExecutionIdMap = lastSucceededExecutionIdMap;
    this.lastPersistedExecutionId = lastPersistedExecutionId;
    this.internalTopic = internalTopic;
    this.admin = admin;
    this.executionIdAccessor = executionIdAccessor;
    this.isParentController = isParentController;
    this.stats = stats;
    this.regionName = regionName;
  }

  @Override
  public Void call() {
    try {
      while (!internalTopic.isEmpty()) {
        if (!admin.isLeaderControllerFor(clusterName)) {
          throw new VeniceRetriableException(
              "This controller is no longer the leader of: " + clusterName
                  + ". The consumption task should unsubscribe soon");
        }
        AdminOperationWrapper adminOperationWrapper = internalTopic.peek();
        if (adminOperationWrapper.getStartProcessingTimestamp() == null) {
          adminOperationWrapper.setStartProcessingTimestamp(System.currentTimeMillis());
          stats.recordAdminMessageStartProcessingLatency(
              Math.max(
                  0,
                  adminOperationWrapper.getStartProcessingTimestamp()
                      - adminOperationWrapper.getLocalBrokerTimestamp()));
        }
        processMessage(adminOperationWrapper.getAdminOperation());
        long completionTimestamp = System.currentTimeMillis();
        long processLatency = Math.max(0, completionTimestamp - adminOperationWrapper.getStartProcessingTimestamp());
        if (AdminMessageType.valueOf(adminOperationWrapper.getAdminOperation()) == AdminMessageType.ADD_VERSION) {
          stats.recordAdminMessageAddVersionProcessLatency(processLatency);
        } else {
          stats.recordAdminMessageProcessLatency(processLatency);
        }
        stats.recordAdminMessageTotalLatency(
            Math.max(0, completionTimestamp - adminOperationWrapper.getProducerTimestamp()));
        internalTopic.remove();
      }
    } catch (Exception e) {
      // Retry of the admin operation is handled automatically by keeping the failed admin operation inside the queue.
      // The queue with the problematic operation will be delegated and retried by the worker thread in the next cycle.
      AdminOperationWrapper adminOperationWrapper = internalTopic.peek();
      String logMessage =
          "when processing admin message for store " + storeName + " with offset " + adminOperationWrapper.getOffset()
              + " and execution id " + adminOperationWrapper.getAdminOperation().executionId;
      if (e instanceof VeniceRetriableException) {
        // These retriable exceptions are expected, therefore logging at the info level should be sufficient.
        stats.recordFailedRetriableAdminConsumption();
        LOGGER.info("Retriable exception thrown {}", logMessage, e);
      } else {
        stats.recordFailedAdminConsumption();
        LOGGER.error("Error {}", logMessage, e);
      }
      throw e;
    }
    return null;
  }

  private void processMessage(AdminOperation adminOperation) {
    long lastSucceededExecutionId = lastSucceededExecutionIdMap.getOrDefault(storeName, lastPersistedExecutionId);
    if (adminOperation.executionId <= lastSucceededExecutionId) {
      /**
       * Since {@link AdminOffsetManager} will only keep the latest several
       * producer guids, which could not filter very old messages.
       * {@link #lastSucceededExecutionId} is monotonically increasing, and being persisted to Zookeeper after processing
       * each message, so it is safe to filter out all the processed messages.
       */
      LOGGER.warn(
          "Execution id of message: {} for store {} is smaller than last succeeded execution id: {}, so will skip it",
          adminOperation,
          storeName,
          lastSucceededExecutionId);
      return;
    }
    try {
      switch (AdminMessageType.valueOf(adminOperation)) {
        case STORE_CREATION:
          handleStoreCreation((StoreCreation) adminOperation.payloadUnion);
          break;
        case VALUE_SCHEMA_CREATION:
          handleValueSchemaCreation((ValueSchemaCreation) adminOperation.payloadUnion);
          break;
        case DISABLE_STORE_WRITE:
          handleDisableStoreWrite((PauseStore) adminOperation.payloadUnion);
          break;
        case ENABLE_STORE_WRITE:
          handleEnableStoreWrite((ResumeStore) adminOperation.payloadUnion);
          break;
        case KILL_OFFLINE_PUSH_JOB:
          handleKillOfflinePushJob((KillOfflinePushJob) adminOperation.payloadUnion);
          break;
        case DISABLE_STORE_READ:
          handleDisableStoreRead((DisableStoreRead) adminOperation.payloadUnion);
          break;
        case ENABLE_STORE_READ:
          handleEnableStoreRead((EnableStoreRead) adminOperation.payloadUnion);
          break;
        case DELETE_ALL_VERSIONS:
          handleDeleteAllVersions((DeleteAllVersions) adminOperation.payloadUnion);
          break;
        case SET_STORE_CURRENT_VERSION:
          handleSetStoreCurrentVersion((SetStoreCurrentVersion) adminOperation.payloadUnion);
          break;
        case SET_STORE_OWNER:
          handleSetStoreOwner((SetStoreOwner) adminOperation.payloadUnion);
          break;
        case SET_STORE_PARTITION:
          handleSetStorePartitionCount((SetStorePartitionCount) adminOperation.payloadUnion);
          break;
        case UPDATE_STORE:
          handleSetStore((UpdateStore) adminOperation.payloadUnion);
          break;
        case DELETE_STORE:
          handleDeleteStore((DeleteStore) adminOperation.payloadUnion);
          break;
        case DELETE_OLD_VERSION:
          handleDeleteOldVersion((DeleteOldVersion) adminOperation.payloadUnion);
          break;
        case MIGRATE_STORE:
          handleStoreMigration((MigrateStore) adminOperation.payloadUnion);
          break;
        case ABORT_MIGRATION:
          handleAbortMigration((AbortMigration) adminOperation.payloadUnion);
          break;
        case ADD_VERSION:
          handleAddVersion((AddVersion) adminOperation.payloadUnion);
          break;
        case DERIVED_SCHEMA_CREATION:
          handleDerivedSchemaCreation((DerivedSchemaCreation) adminOperation.payloadUnion);
          break;
        case SUPERSET_SCHEMA_CREATION:
          handleSupersetSchemaCreation((SupersetSchemaCreation) adminOperation.payloadUnion);
          break;
        case CONFIGURE_NATIVE_REPLICATION_FOR_CLUSTER:
          handleEnableNativeReplicationForCluster((ConfigureNativeReplicationForCluster) adminOperation.payloadUnion);
          break;
        case CONFIGURE_ACTIVE_ACTIVE_REPLICATION_FOR_CLUSTER:
          handleEnableActiveActiveReplicationForCluster(
              (ConfigureActiveActiveReplicationForCluster) adminOperation.payloadUnion);
          break;
        case REPLICATION_METADATA_SCHEMA_CREATION:
          handleReplicationMetadataSchemaCreation((MetadataSchemaCreation) adminOperation.payloadUnion);
          break;
        case META_SYSTEM_STORE_AUTO_CREATION_VALIDATION:
          handleMetaSystemStoreCreationValidation((MetaSystemStoreAutoCreationValidation) adminOperation.payloadUnion);
          break;
        case PUSH_STATUS_SYSTEM_STORE_AUTO_CREATION_VALIDATION:
          handlePushStatusSystemStoreCreationValidation(
              (PushStatusSystemStoreAutoCreationValidation) adminOperation.payloadUnion);
          break;
        case CREATE_STORAGE_PERSONA:
          handleCreateStoragePersona((CreateStoragePersona) adminOperation.payloadUnion);
          break;
        case DELETE_STORAGE_PERSONA:
          handleDeleteStoragePersona((DeleteStoragePersona) adminOperation.payloadUnion);
          break;
        case UPDATE_STORAGE_PERSONA:
          handleUpdateStoragePersona((UpdateStoragePersona) adminOperation.payloadUnion);
          break;
        case DELETE_UNUSED_VALUE_SCHEMA:
          handleDeleteUnusedValueSchema((DeleteUnusedValueSchemas) adminOperation.payloadUnion);
          break;
        case ROLLBACK_CURRENT_VERSION:
          handleRollbackCurrentVersion((RollbackCurrentVersion) adminOperation.payloadUnion);
          break;
        case ROLLFORWARD_CURRENT_VERSION:
          handleRollForwardToFutureVersion((RollForwardCurrentVersion) adminOperation.payloadUnion);
          break;
        default:
          throw new VeniceException("Unknown admin operation type: " + adminOperation.operationType);
      }
    } catch (VeniceUnsupportedOperationException e) {
      LOGGER.info(
          "Ignoring the {} caught when processing {} with detailed message: {}",
          e.getClass().getSimpleName(),
          AdminMessageType.valueOf(adminOperation),
          e.getMessage());
    }
    executionIdAccessor.updateLastSucceededExecutionIdMap(clusterName, storeName, adminOperation.executionId);
    lastSucceededExecutionIdMap.put(storeName, adminOperation.executionId);
  }

  private void handleStoreCreation(StoreCreation message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    String owner = message.owner.toString();
    String keySchema = message.keySchema.definition.toString();
    String valueSchema = message.valueSchema.definition.toString();

    // Check whether the store exists or not, the duplicate message could be
    // introduced by Kafka retry
    if (admin.hasStore(clusterName, storeName)) {
      LOGGER.info("Adding store: {}, which already exists, so just skip this message: {}", storeName, message);
    } else {
      // Adding store
      admin.createStore(
          clusterName,
          storeName,
          owner,
          keySchema,
          valueSchema,
          VeniceSystemStoreUtils.isSystemStore(storeName));
      LOGGER.info("Added store: {} to cluster: {}", storeName, clusterName);
    }
  }

  private void handleValueSchemaCreation(ValueSchemaCreation message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    String schemaStr = message.schema.definition.toString();
    final int schemaId = message.schemaId;

    SchemaEntry valueSchemaEntry = admin.addValueSchema(clusterName, storeName, schemaStr, schemaId);
    LOGGER.info(
        "Added value schema {} to store {} in cluster {} with schema ID {}",
        schemaStr,
        storeName,
        clusterName,
        valueSchemaEntry.getId());
  }

  private void handleDerivedSchemaCreation(DerivedSchemaCreation message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    String derivedSchemaStr = message.schema.definition.toString();
    int valueSchemaId = message.valueSchemaId;
    int derivedSchemaId = message.derivedSchemaId;

    admin.addDerivedSchema(clusterName, storeName, valueSchemaId, derivedSchemaId, derivedSchemaStr);
    LOGGER.info(
        "Added derived schema:\n {}\n to store: {}, value schema id: {}, derived schema id: {}",
        derivedSchemaStr,
        storeName,
        valueSchemaId,
        derivedSchemaId);
  }

  private void handleSupersetSchemaCreation(SupersetSchemaCreation message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    String valueSchemaStr = message.valueSchema.definition.toString();
    int valueSchemaId = message.valueSchemaId;
    String supersetSchemaStr = message.supersetSchema.definition.toString();
    int supersetSchemaId = message.supersetSchemaId;

    admin.addSupersetSchema(clusterName, storeName, valueSchemaStr, valueSchemaId, supersetSchemaStr, supersetSchemaId);
    LOGGER.info(
        "Added value schema:\n {}\n to store: {}, value schema id: {}, also added superset schema: {}, superset schema id: {}",
        valueSchemaStr,
        storeName,
        valueSchemaId,
        supersetSchemaStr,
        supersetSchemaId);
  }

  private void handleDisableStoreWrite(PauseStore message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    admin.setStoreWriteability(clusterName, storeName, false);

    LOGGER.info("Disabled store to write: {} in cluster: {}", storeName, clusterName);
  }

  private void handleEnableStoreWrite(ResumeStore message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    admin.setStoreWriteability(clusterName, storeName, true);

    LOGGER.info("Enabled store to write: {} in cluster: {}", storeName, clusterName);
  }

  private void handleDisableStoreRead(DisableStoreRead message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    admin.setStoreReadability(clusterName, storeName, false);

    LOGGER.info("Disabled store to read: {} in cluster: {}", storeName, clusterName);
  }

  private void handleEnableStoreRead(EnableStoreRead message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    admin.setStoreReadability(clusterName, storeName, true);
    LOGGER.info("Enabled store to read: {} in cluster: {}", storeName, clusterName);
  }

  private void handleKillOfflinePushJob(KillOfflinePushJob message) {
    if (isParentController) {
      // Do nothing for Parent Controller
      return;
    }
    String clusterName = message.clusterName.toString();
    String kafkaTopic = message.kafkaTopic.toString();
    admin.killOfflinePush(clusterName, kafkaTopic, false);

    LOGGER.info("Killed job with topic: {} in cluster: {}", kafkaTopic, clusterName);
  }

  private void handleDeleteAllVersions(DeleteAllVersions message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    admin.deleteAllVersionsInStore(clusterName, storeName);
    LOGGER.info("Deleted all of version in store: {} in cluster: {}", storeName, clusterName);
  }

  private void handleDeleteOldVersion(DeleteOldVersion message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    int versionNum = message.versionNum;
    // Delete an old version for a Venice store.
    admin.deleteOldVersionInStore(clusterName, storeName, versionNum);
    LOGGER.info("Deleted version: {} in store: {} in cluster: {}", versionNum, storeName, clusterName);
  }

  private void handleSetStoreCurrentVersion(SetStoreCurrentVersion message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    int version = message.currentVersion;
    admin.setStoreCurrentVersion(clusterName, storeName, version);

    LOGGER.info("Set store: {} version to {} in cluster: {}", storeName, version, clusterName);
  }

  private void handleSetStoreOwner(SetStoreOwner message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    String owner = message.owner.toString();
    admin.setStoreOwner(clusterName, storeName, owner);

    LOGGER.info("Set store: {} owner to {} in cluster: {}", storeName, owner, clusterName);
  }

  private void handleSetStorePartitionCount(SetStorePartitionCount message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    int partitionNum = message.partitionNum;
    admin.setStorePartitionCount(clusterName, storeName, partitionNum);

    LOGGER.info("Set store: {} partition number to {} in cluster: {}", storeName, partitionNum, clusterName);
  }

  private void handleSetStore(UpdateStore message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();

    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setOwner(message.owner.toString())
        .setEnableReads(message.enableReads)
        .setEnableWrites(message.enableWrites)
        .setPartitionCount(message.partitionNum);
    if (message.partitionerConfig != null) {
      params.setPartitionerClass(message.partitionerConfig.partitionerClass.toString())
          .setPartitionerParams(
              CollectionUtils.getStringMapFromCharSequenceMap(message.partitionerConfig.partitionerParams))
          .setAmplificationFactor(message.partitionerConfig.amplificationFactor);
    }
    params.setStorageQuotaInByte(message.storageQuotaInByte)
        .setHybridStoreOverheadBypass(message.hybridStoreOverheadBypass)
        .setReadQuotaInCU(message.readQuotaInCU);

    if (message.currentVersion != IGNORED_CURRENT_VERSION) {
      params.setCurrentVersion(message.currentVersion);
    }

    if (message.hybridStoreConfig != null) {
      params.setHybridRewindSeconds(message.hybridStoreConfig.rewindTimeInSeconds)
          .setHybridOffsetLagThreshold(message.hybridStoreConfig.offsetLagThresholdToGoOnline)
          .setHybridTimeLagThreshold(message.hybridStoreConfig.producerTimestampLagThresholdToGoOnlineInSeconds)
          .setHybridDataReplicationPolicy(
              DataReplicationPolicy.valueOf(message.hybridStoreConfig.dataReplicationPolicy))
          .setHybridBufferReplayPolicy(BufferReplayPolicy.valueOf(message.hybridStoreConfig.bufferReplayPolicy));
    }
    params.setAccessControlled(message.accessControlled)
        .setCompressionStrategy(CompressionStrategy.valueOf(message.compressionStrategy))
        .setClientDecompressionEnabled(message.clientDecompressionEnabled)
        .setChunkingEnabled(message.chunkingEnabled)
        .setRmdChunkingEnabled(message.rmdChunkingEnabled)
        .setBatchGetLimit(message.batchGetLimit)
        .setNumVersionsToPreserve(message.numVersionsToPreserve)
        .setIncrementalPushEnabled(message.incrementalPushEnabled)
        .setSeparateRealTimeTopicEnabled(message.separateRealTimeTopicEnabled)
        .setStoreMigration(message.isMigrating)
        .setWriteComputationEnabled(message.writeComputationEnabled)
        .setReadComputationEnabled(message.readComputationEnabled)
        .setBootstrapToOnlineTimeoutInHours(message.bootstrapToOnlineTimeoutInHours)
        .setBackupStrategy(BackupStrategy.fromInt(message.backupStrategy))
        .setAutoSchemaPushJobEnabled(message.schemaAutoRegisterFromPushJobEnabled)
        .setHybridStoreDiskQuotaEnabled(message.hybridStoreDiskQuotaEnabled)
        .setReplicationFactor(message.replicationFactor)
        .setMigrationDuplicateStore(message.migrationDuplicateStore)
        .setLatestSupersetSchemaId(message.latestSuperSetValueSchemaId)
        .setBlobTransferEnabled(message.blobTransferEnabled)
        .setUnusedSchemaDeletionEnabled(message.unusedSchemaDeletionEnabled)
        .setNearlineProducerCompressionEnabled(message.nearlineProducerCompressionEnabled)
        .setNearlineProducerCountPerWriter(message.nearlineProducerCountPerWriter);

    if (message.ETLStoreConfig != null) {
      params.setRegularVersionETLEnabled(message.ETLStoreConfig.regularVersionETLEnabled)
          .setFutureVersionETLEnabled(message.ETLStoreConfig.futureVersionETLEnabled)
          .setEtledProxyUserAccount(message.ETLStoreConfig.etledUserProxyAccount.toString());
    }

    if (message.views != null) {
      // TODO: This probably needs to be a bit more robust, for now this is fine.
      params.setStoreViews(
          message.views.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())));
    }

    if (message.largestUsedVersionNumber != null) {
      params.setLargestUsedVersionNumber(message.largestUsedVersionNumber);
    }

    params.setNativeReplicationEnabled(message.nativeReplicationEnabled)
        .setPushStreamSourceAddress(
            message.pushStreamSourceAddress == null ? null : message.pushStreamSourceAddress.toString())
        .setBackupVersionRetentionMs(message.backupVersionRetentionMs);

    params.setNativeReplicationSourceFabric(
        message.nativeReplicationSourceFabric == null ? null : message.nativeReplicationSourceFabric.toString());
    params.setActiveActiveReplicationEnabled(message.activeActiveReplicationEnabled);
    params.setRegionsFilter(message.regionsFilter == null ? null : message.regionsFilter.toString());

    if (message.disableMetaStore) {
      params.setDisableMetaStore();
    }

    if (message.disableDavinciPushStatusStore) {
      params.setDisableDavinciPushStatusStore();
    }

    if (message.storagePersona != null) {
      params.setStoragePersona(message.storagePersona.toString());
    }

    params.setStorageNodeReadQuotaEnabled(message.storageNodeReadQuotaEnabled);
    params.setMinCompactionLagSeconds(message.minCompactionLagSeconds);
    params.setMaxCompactionLagSeconds(message.maxCompactionLagSeconds);
    params.setMaxRecordSizeBytes(message.maxRecordSizeBytes);
    params.setMaxNearlineRecordSizeBytes(message.maxNearlineRecordSizeBytes);

    final UpdateStoreQueryParams finalParams;
    if (message.replicateAllConfigs) {
      finalParams = params;
    } else {
      if (message.updatedConfigsList == null || message.updatedConfigsList.isEmpty()) {
        throw new VeniceException(
            "UpdateStore failed for store " + storeName + ". replicateAllConfigs flag was off "
                + "but there was no config updates.");
      }
      finalParams = new UpdateStoreQueryParams();
      for (CharSequence updatedConfigName: message.updatedConfigsList) {
        finalParams.cloneConfig(updatedConfigName.toString(), params);
      }
    }
    /**
     * Pass the region filter to the final update store params
     */
    params.getRegionsFilter().ifPresent(finalParams::setRegionsFilter);

    if (checkPreConditionForReplicateUpdateStore(
        clusterName,
        storeName,
        message.isMigrating,
        message.enableReads,
        message.enableWrites,
        message.migrationDuplicateStore)) {
      admin.replicateUpdateStore(clusterName, storeName, finalParams);
    }
    admin.updateStore(clusterName, storeName, finalParams);

    LOGGER.info("Set store: {} in cluster: {}", storeName, clusterName);
  }

  private void handleDeleteStore(DeleteStore message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    int largestUsedVersionNumber = message.largestUsedVersionNumber;
    if (admin.hasStore(clusterName, storeName) && admin.getStore(clusterName, storeName).isMigrating()) {
      /** Ignore largest used version in original store in parent.
       * This is necessary if client pushes a new version during store migration, in which case the original store "largest used version"
       * in parent controller will not increase, but cloned store in parent and both original and cloned stores in child ontrollers will.
       * Without this change the "delete store" message will not be processed in child controller due to version mismatch.
       *
       * It will also allow Venice admin to issue delete store command to parent controller,
       * in case something goes wrong during store migration
       *
       * TODO: revise this logic when store migration is redesigned, new design should avoid this edge case altogether.
       */
      admin.deleteStore(clusterName, storeName, Store.IGNORE_VERSION, true);
    } else {
      admin.deleteStore(clusterName, storeName, largestUsedVersionNumber, true);
    }

    LOGGER.info("Deleted store: {} in cluster: {}", storeName, clusterName);
  }

  private void handleStoreMigration(MigrateStore message) {
    String srcClusterName = message.srcClusterName.toString();
    String destClusterName = message.destClusterName.toString();
    String storeName = message.storeName.toString();
    if (this.isParentController) {
      // Src and dest parent controllers communicate
      admin.migrateStore(srcClusterName, destClusterName, storeName);
    } else {
      // Child controllers need to update migration src and dest cluster in storeConfig
      // Otherwise, storeConfig in the fabric won't have src and dest cluster info
      admin.setStoreConfigForMigration(storeName, srcClusterName, destClusterName);
    }
  }

  private void handleAbortMigration(AbortMigration message) {
    String srcClusterName = message.srcClusterName.toString();
    String destClusterName = message.destClusterName.toString();
    String storeName = message.storeName.toString();

    admin.abortMigration(srcClusterName, destClusterName, storeName);
  }

  private void handleAddVersion(AddVersion message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    String pushJobId = message.pushJobId.toString();
    int repushSourceVersion = message.repushSourceVersion;
    int versionNumber = message.versionNum;
    int numberOfPartitions = message.numberOfPartitions;
    Version.PushType pushType = Version.PushType.valueOf(message.pushType);
    String remoteKafkaBootstrapServers =
        message.pushStreamSourceAddress == null ? null : message.pushStreamSourceAddress.toString();
    long rewindTimeInSecondsOverride = message.rewindTimeInSecondsOverride;
    int replicationMetadataVersionId = message.timestampMetadataVersionId;
    // Log the message
    LOGGER.info(
        "Processing add version message for store: {} in cluster: {} with version number: {}.",
        storeName,
        clusterName,
        versionNumber);
    if (isParentController) {
      if (checkPreConditionForReplicateAddVersion(clusterName, storeName)) {
        // Parent controller mirrors new version to src or dest cluster if the store is migrating
        admin.replicateAddVersionAndStartIngestion(
            clusterName,
            storeName,
            pushJobId,
            versionNumber,
            numberOfPartitions,
            pushType,
            remoteKafkaBootstrapServers,
            rewindTimeInSecondsOverride,
            replicationMetadataVersionId);
      }
    } else {
      boolean skipConsumption = message.targetedRegions != null && !message.targetedRegions.isEmpty()
          && message.targetedRegions.stream().map(Object::toString).noneMatch(regionName::equals);
      if (skipConsumption) {
        // for targeted region push, only allow specified region to process add version message
        LOGGER.info(
            "Skip the add version message for store {} in region {} since this is targeted region push and "
                + "local region is not the targeted region list {}",
            storeName,
            regionName,
            message.targetedRegions.toString());
      } else {
        // New version for regular Venice store.
        admin.addVersionAndStartIngestion(
            clusterName,
            storeName,
            pushJobId,
            versionNumber,
            numberOfPartitions,
            pushType,
            remoteKafkaBootstrapServers,
            rewindTimeInSecondsOverride,
            replicationMetadataVersionId,
            message.versionSwapDeferred,
            repushSourceVersion);
      }
    }
  }

  private void handleEnableNativeReplicationForCluster(ConfigureNativeReplicationForCluster message) {
    LOGGER.info(
        "Received message to configure native replication for cluster: {} but ignoring it as native replication is the only mode",
        message.clusterName);
  }

  private void handleEnableActiveActiveReplicationForCluster(ConfigureActiveActiveReplicationForCluster message) {
    String clusterName = message.clusterName.toString();
    VeniceUserStoreType storeType = VeniceUserStoreType.valueOf(message.storeType.toString().toUpperCase());
    boolean enableActiveActiveReplication = message.enabled;
    Optional<String> regionsFilter =
        (message.regionsFilter == null) ? Optional.empty() : Optional.of(message.regionsFilter.toString());
    admin.configureActiveActiveReplication(
        clusterName,
        storeType,
        Optional.empty(),
        enableActiveActiveReplication,
        regionsFilter);
  }

  private boolean checkPreConditionForReplicateAddVersion(String clusterName, String storeName) {
    return admin.getStore(clusterName, storeName).isMigrating();
  }

  private boolean checkPreConditionForReplicateUpdateStore(
      String clusterName,
      String storeName,
      boolean isMigrating,
      boolean isEnableReads,
      boolean isEnableWrites,
      boolean isMigrationDuplicateStore) {
    if (this.isParentController && admin.hasStore(clusterName, storeName)) {
      Store store = admin.getStore(clusterName, storeName);
      if (store.isMigrating()) {
        boolean storeMigrationUpdated = isMigrating != store.isMigrating();
        boolean readabilityUpdated = isEnableReads != store.isEnableReads();
        boolean writeabilityUpdated = isEnableWrites != store.isEnableWrites();
        boolean migrationDuplicateStoreUpdated = isMigrationDuplicateStore != store.isMigrationDuplicateStore();
        if (storeMigrationUpdated || readabilityUpdated || writeabilityUpdated || migrationDuplicateStoreUpdated) {
          // No need to mirror these updates
          return false;
        }
        return true;
      }
    }
    return false;
  }

  private void handleReplicationMetadataSchemaCreation(MetadataSchemaCreation message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    int valueSchemaId = message.valueSchemaId;
    String replicationMetadataSchemaStr = message.metadataSchema.definition.toString();
    int replicationMetadataVersionId = message.timestampMetadataVersionId;

    admin.addReplicationMetadataSchema(
        clusterName,
        storeName,
        valueSchemaId,
        replicationMetadataVersionId,
        replicationMetadataSchemaStr);
    LOGGER.info(
        "Added replication metadata schema for store {}, value schema ID {}, "
            + "replication metadata version ID {}, and the added replication metadata schema: {}",
        storeName,
        valueSchemaId,
        replicationMetadataVersionId,
        replicationMetadataSchemaStr);
  }

  private void handleMetaSystemStoreCreationValidation(MetaSystemStoreAutoCreationValidation message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    admin.validateAndMaybeRetrySystemStoreAutoCreation(clusterName, storeName, VeniceSystemStoreType.META_STORE);
  }

  private void handlePushStatusSystemStoreCreationValidation(PushStatusSystemStoreAutoCreationValidation message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    admin.validateAndMaybeRetrySystemStoreAutoCreation(
        clusterName,
        storeName,
        VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE);
  }

  private void handleCreateStoragePersona(CreateStoragePersona message) {
    String clusterName = message.getClusterName().toString();
    String personaName = message.getName().toString();
    long quotaNumber = message.getQuotaNumber();
    Set<String> storesToEnforce =
        message.getStoresToEnforce().stream().map(s -> s.toString()).collect(Collectors.toSet());
    Set<String> owners = message.getOwners().stream().map(s -> s.toString()).collect(Collectors.toSet());
    admin.createStoragePersona(clusterName, personaName, quotaNumber, storesToEnforce, owners);
  }

  private void handleDeleteStoragePersona(DeleteStoragePersona message) {
    String clusterName = message.getClusterName().toString();
    String personaName = message.getName().toString();
    admin.deleteStoragePersona(clusterName, personaName);
  }

  private void handleRollForwardToFutureVersion(RollForwardCurrentVersion message) {
    String clusterName = message.getClusterName().toString();
    String storeName = message.getStoreName().toString();
    String regionFilter = message.getRegionsFilter().toString();
    admin.rollForwardToFutureVersion(clusterName, storeName, regionFilter);
  }

  private void handleRollbackCurrentVersion(RollbackCurrentVersion message) {
    String clusterName = message.getClusterName().toString();
    String storeName = message.getStoreName().toString();
    String regionFilter = message.getRegionsFilter().toString();
    admin.rollbackToBackupVersion(clusterName, storeName, regionFilter);
  }

  private void handleDeleteUnusedValueSchema(DeleteUnusedValueSchemas message) {
    String clusterName = message.getClusterName().toString();
    String storeName = message.getStoreName().toString();
    Set<Integer> schemaIds = new HashSet<>(message.getSchemaIds());
    admin.deleteValueSchemas(clusterName, storeName, schemaIds);
  }

  private void handleUpdateStoragePersona(UpdateStoragePersona message) {
    String clusterName = message.getClusterName().toString();
    String personaName = message.getName().toString();
    UpdateStoragePersonaQueryParams queryParams = new UpdateStoragePersonaQueryParams();
    if (message.getQuotaNumber() != null) {
      queryParams.setQuota(message.getQuotaNumber());
    }
    if (message.getStoresToEnforce() != null) {
      queryParams.setStoresToEnforce(
          new HashSet<>(message.getStoresToEnforce().stream().map(s -> s.toString()).collect(Collectors.toSet())));
    }
    if (message.getOwners() != null) {
      queryParams
          .setOwners(new HashSet<>(message.getOwners().stream().map(s -> s.toString()).collect(Collectors.toSet())));
    }
    admin.updateStoragePersona(clusterName, personaName, queryParams);
  }

}
