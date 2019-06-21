package com.linkedin.venice.controller.kafka.consumer;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.ExecutionIdAccessor;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controller.kafka.offsets.AdminOffsetManager;
import com.linkedin.venice.controller.kafka.protocol.admin.AbortMigration;
import com.linkedin.venice.controller.kafka.protocol.admin.AddVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteAllVersions;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteOldVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteStore;
import com.linkedin.venice.controller.kafka.protocol.admin.DisableStoreRead;
import com.linkedin.venice.controller.kafka.protocol.admin.EnableStoreRead;
import com.linkedin.venice.controller.kafka.protocol.admin.KillOfflinePushJob;
import com.linkedin.venice.controller.kafka.protocol.admin.MigrateStore;
import com.linkedin.venice.controller.kafka.protocol.admin.PauseStore;
import com.linkedin.venice.controller.kafka.protocol.admin.ResumeStore;
import com.linkedin.venice.controller.kafka.protocol.admin.SetStoreCurrentVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.SetStoreOwner;
import com.linkedin.venice.controller.kafka.protocol.admin.SetStorePartitionCount;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore;
import com.linkedin.venice.controller.kafka.protocol.admin.ValueSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.stats.AdminConsumptionStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceRetriableException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.meta.BackupStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.schema.SchemaEntry;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Callable;
import org.apache.log4j.Logger;

import static com.linkedin.venice.controller.kafka.consumer.AdminConsumptionTask.*;


/**
 * This class is used to create {@link Callable} that execute {@link AdminOperation}s for a given store.
 */
public class AdminExecutionTask implements Callable<Void> {

  /**
   * This logger is intended to be passed from the {@link AdminConsumptionTask} in order to produce logs with the
   * tag AdminConsumptionTask#CONSUMER_TASK_ID_FORMAT.
   */
  private final Logger logger;
  private final String clusterName;
  private final String storeName;
  private final Queue<AdminOperationWrapper> internalTopic;
  private final VeniceHelixAdmin admin;
  private final ExecutionIdAccessor executionIdAccessor;
  private final boolean isParentController;
  private final AdminConsumptionStats stats;

  private long lastSucceededExecutionId;

  AdminExecutionTask(
      Logger logger,
      String clusterName,
      String storeName,
      long lastSucceededExecutionId,
      Queue<AdminOperationWrapper> internalTopic,
      VeniceHelixAdmin admin,
      ExecutionIdAccessor executionIdAccessor,
      boolean isParentController,
      AdminConsumptionStats stats) {
    this.logger = logger;
    this.clusterName = clusterName;
    this.storeName = storeName;
    this.lastSucceededExecutionId = lastSucceededExecutionId;
    this.internalTopic = internalTopic;
    this.admin = admin;
    this.executionIdAccessor = executionIdAccessor;
    this.isParentController = isParentController;
    this.stats = stats;
  }

  @Override
  public Void call() {
    while (!internalTopic.isEmpty() && admin.isMasterController(clusterName)) {
      AdminOperationWrapper adminOperationWrapper = internalTopic.peek();
      try {
        if (adminOperationWrapper.getStartProcessingTimestamp() == null) {
          adminOperationWrapper.setStartProcessingTimestamp(System.currentTimeMillis());
          stats.recordAdminMessageStartProcessingLatency(Math.max(0,
              adminOperationWrapper.getStartProcessingTimestamp() - adminOperationWrapper.getLocalBrokerTimestamp()));
        }
        processMessage(adminOperationWrapper.getAdminOperation());
        long completionTimestamp = System.currentTimeMillis();
        long processLatency = Math.max(0, completionTimestamp - adminOperationWrapper.getStartProcessingTimestamp());
        if (AdminMessageType.valueOf(adminOperationWrapper.getAdminOperation()) == AdminMessageType.ADD_VERSION) {
          stats.recordAdminMessageProcessLatency(processLatency);
        } else {
          stats.recordAdminMessageProcessLatency(processLatency);
        }
        stats.recordAdminMessageTotalLatency(Math.max(0,
            completionTimestamp - adminOperationWrapper.getProducerTimestamp()));
        internalTopic.poll();
      } catch (Exception e) {
        // Retry of the admin operation is handled automatically by keeping the failed admin operation inside the queue.
        // The queue with the problematic operation will be delegated and retried by the worker thread in the next cycle.
        String logMessage = "when processing admin message for store " + storeName + " with offset "
            + adminOperationWrapper.getOffset() + " and execution id " + adminOperationWrapper.getAdminOperation().executionId;
        if (e instanceof VeniceRetriableException) {
          // These retriable exceptions are expected, therefore logging at the info level should be sufficient.
          stats.recordFailedRetriableAdminConsumption();
          logger.info("Retriable exception thrown " + logMessage, e);
        } else {
          stats.recordFailedAdminConsumption();
          logger.error("Error " + logMessage, e);
        }
        throw e;
      }
    }
    return null;
  }

  private void processMessage(AdminOperation adminOperation) {
    if (adminOperation.executionId <= lastSucceededExecutionId) {
      /**
       * Since {@link AdminOffsetManager} will only keep the latest several
       * producer guids, which could not filter very old messages.
       * {@link #lastSucceededExecutionId} is monotonically increasing, and being persisted to Zookeeper after processing
       * each message, so it is safe to filter out all the processed messages.
       */
      logger.warn("Execution id of message: " + adminOperation
          + " for store " + storeName + " is smaller than last succeeded execution id: "
          + lastSucceededExecutionId + ", so will skip it");
      return;
    }
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
      default:
        throw new VeniceException("Unknown admin operation type: " + adminOperation.operationType);
    }
    lastSucceededExecutionId = adminOperation.executionId;
    executionIdAccessor.updateLastSucceededExecutionIdMap(clusterName, storeName, lastSucceededExecutionId);
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
      logger.info("Adding store: " + storeName + ", which already exists, so just skip this message: " + message);
    } else {
      // Adding store
      admin.addStore(clusterName, storeName, owner, keySchema, valueSchema);
      logger.info("Added store: " + storeName + " to cluster: " + clusterName);
    }
  }

  private void handleValueSchemaCreation(ValueSchemaCreation message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    String schemaStr = message.schema.definition.toString();
    int schemaId = message.schemaId;

    SchemaEntry valueSchemaEntry = admin.addValueSchema(clusterName, storeName, schemaStr, schemaId);
    logger.info("Added value schema: " + schemaStr + " to store: " + storeName + ", schema id: " + valueSchemaEntry.getId());
  }

  private void handleDisableStoreWrite(PauseStore message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    admin.setStoreWriteability(clusterName, storeName, false);

    logger.info("Disabled store to write: " + storeName + " in cluster: " + clusterName);
  }

  private void handleEnableStoreWrite(ResumeStore message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    admin.setStoreWriteability(clusterName, storeName, true);

    logger.info("Enabled store to write: " + storeName + " in cluster: " + clusterName);
  }

  private void handleDisableStoreRead(DisableStoreRead message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    admin.setStoreReadability(clusterName, storeName, false);

    logger.info("Disabled store to read: " + storeName + " in cluster: " + clusterName);
  }

  private void handleEnableStoreRead(EnableStoreRead message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    admin.setStoreReadability(clusterName, storeName, true);
    logger.info("Enabled store to read: " + storeName + " in cluster: " + clusterName);
  }

  private void handleKillOfflinePushJob(KillOfflinePushJob message) {
    if (isParentController) {
      // Do nothing for Parent Controller
      return;
    }
    String clusterName = message.clusterName.toString();
    String kafkaTopic = message.kafkaTopic.toString();
    admin.killOfflinePush(clusterName, kafkaTopic);

    logger.info("Killed job with topic: " + kafkaTopic + " in cluster: " + clusterName);
  }

  private void handleDeleteAllVersions(DeleteAllVersions message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    admin.deleteAllVersionsInStore(clusterName, storeName);
    logger.info("Deleted all of version in store:" + storeName + " in cluster: " + clusterName);
  }

  private void handleDeleteOldVersion(DeleteOldVersion message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    int versionNum = message.versionNum;
    admin.deleteOldVersionInStore(clusterName, storeName, versionNum);
    logger.info("Deleted version: " + versionNum + " in store:" + storeName + " in cluster: " + clusterName);
  }

  private void handleSetStoreCurrentVersion(SetStoreCurrentVersion message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    int version = message.currentVersion;
    admin.setStoreCurrentVersion(clusterName, storeName, version);

    logger.info("Set store: " + storeName + " version to"  + version + " in cluster: " + clusterName);
  }

  private void handleSetStoreOwner(SetStoreOwner message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    String owner = message.owner.toString();
    admin.setStoreOwner(clusterName, storeName, owner);

    logger.info("Set store: " + storeName + " owner to " + owner + " in cluster: " + clusterName);
  }

  private void handleSetStorePartitionCount(SetStorePartitionCount message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    int partitionNum = message.partitionNum;
    admin.setStorePartitionCount(clusterName, storeName, partitionNum);

    logger.info("Set store: " + storeName + " partition number to " + partitionNum + " in cluster: " + clusterName);
  }

  private void handleSetStore(UpdateStore message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    admin.updateStore(clusterName, storeName,
        Optional.of(message.owner.toString()),
        Optional.of(message.enableReads),
        Optional.of(message.enableWrites),
        Optional.of(message.partitionNum),
        Optional.of(message.storageQuotaInByte),
        Optional.of(message.readQuotaInCU),
        message.currentVersion == IGNORED_CURRENT_VERSION
            ? Optional.empty()
            : Optional.of(message.currentVersion),
        Optional.empty(), // We explicitly forbid setting the largestUsedVersionNumber globally, so it is not included in the admin protocol
        message.hybridStoreConfig == null
            ? Optional.empty()
            : Optional.of(message.hybridStoreConfig.rewindTimeInSeconds),
        message.hybridStoreConfig == null
            ? Optional.empty()
            : Optional.of(message.hybridStoreConfig.offsetLagThresholdToGoOnline),
        Optional.of(message.accessControlled),
        CompressionStrategy.optionalValueOf(message.compressionStrategy),
        Optional.of(message.chunkingEnabled),
        Optional.of(message.singleGetRouterCacheEnabled),
        Optional.of(message.batchGetRouterCacheEnabled),
        Optional.of(message.batchGetLimit),
        Optional.of(message.numVersionsToPreserve),
        Optional.of(message.incrementalPushEnabled),
        Optional.of(message.isMigrating),
        Optional.of(message.writeComputationEnabled),
        Optional.of(message.readComputationEnabled),
        Optional.of(message.bootstrapToOnlineTimeoutInHours),
        Optional.of(message.leaderFollowerModelEnabled),
        Optional.of(BackupStrategy.fromInt(message.backupStrategy)));

    logger.info("Set store: " + storeName + " in cluster: " + clusterName);
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
       * TODO: revise this logic when remove {@link com.linkedin.venice.controller.VeniceHelixAdmin#addVersion}
       *       side effect in {@link com.linkedin.venice.controller.kafka.TopicMonitor}
       */
      admin.deleteStore(clusterName, storeName, Store.IGNORE_VERSION);
    } else {
      admin.deleteStore(clusterName, storeName, largestUsedVersionNumber);
    }

    logger.info("Deleted store: " + storeName + " in cluster: " + clusterName);
  }

  private void handleStoreMigration(MigrateStore message) {
    if (this.isParentController) {
      // Parent controller should not process this message again
      return;
    }

    String srcClusterName = message.srcClusterName.toString();
    String destClusterName = message.destClusterName.toString();
    String storeName = message.storeName.toString();

    admin.migrateStore(srcClusterName, destClusterName, storeName);
  }

  private void handleAbortMigration(AbortMigration message) {
    String srcClusterName = message.srcClusterName.toString();
    String destClusterName = message.destClusterName.toString();
    String storeName = message.storeName.toString();

    admin.abortMigration(srcClusterName, destClusterName, storeName);
  }

  private void handleAddVersion(AddVersion message) {
    if (isParentController) {
      // No op, parent controller only needs to verify this message was produced successfully.
      return;
    }
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    String pushJobId = message.pushJobId.toString();
    int versionNumber = message.versionNum;
    int numberOfPartitions = message.numberOfPartitions;
    try {
      admin.addVersionAndStartIngestion(clusterName, storeName, pushJobId, versionNumber, numberOfPartitions);
    } catch (VeniceUnsupportedOperationException unsupportedOperationException) {
      // No op and ignore the add version message since add version via admin protocol is disabled
    }
  }
}
