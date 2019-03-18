package com.linkedin.venice.controller;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumptionTask;
import com.linkedin.venice.controller.kafka.consumer.VeniceControllerConsumerFactory;
import com.linkedin.venice.controller.kafka.offsets.AdminOffsetManager;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;

import com.linkedin.venice.controller.kafka.protocol.admin.AbortMigration;
import com.linkedin.venice.controller.kafka.protocol.admin.AddVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteAllVersions;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteOldVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteStore;
import com.linkedin.venice.controller.kafka.protocol.admin.DisableStoreRead;
import com.linkedin.venice.controller.kafka.protocol.admin.EnableStoreRead;
import com.linkedin.venice.controller.kafka.protocol.admin.HybridStoreConfigRecord;
import com.linkedin.venice.controller.kafka.protocol.admin.KillOfflinePushJob;
import com.linkedin.venice.controller.kafka.protocol.admin.MigrateStore;
import com.linkedin.venice.controller.kafka.protocol.admin.PauseStore;
import com.linkedin.venice.controller.kafka.protocol.admin.ResumeStore;
import com.linkedin.venice.controller.kafka.protocol.admin.SchemaMeta;
import com.linkedin.venice.controller.kafka.protocol.admin.SetStoreCurrentVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.SetStoreOwner;
import com.linkedin.venice.controller.kafka.protocol.admin.SetStorePartitionCount;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.UpdateStore;
import com.linkedin.venice.controller.kafka.protocol.admin.ValueSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.enums.SchemaType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controller.migration.MigrationPushStrategyZKAccessor;
import com.linkedin.venice.controllerapi.AdminCommandExecution;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceHttpException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.helix.ParentHelixOfflinePushAccessor;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.meta.*;
import com.linkedin.venice.participant.protocol.ParticipantMessageKey;
import com.linkedin.venice.participant.protocol.ParticipantMessageStoreUtils;
import com.linkedin.venice.participant.protocol.ParticipantMessageValue;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.pushmonitor.OfflinePushStatus;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.status.protocol.PushJobStatusRecordValue;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import java.util.Optional;

import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.http.HttpStatus;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * This class is a wrapper of {@link VeniceHelixAdmin}, which will be used in parent controller.
 * There should be only one single Parent Controller, which is the endpoint for all the admin data
 * update.
 * For every admin update operation, it will first push admin operation messages to Kafka,
 * then wait for the admin consumer to consume the message.
 */
public class VeniceParentHelixAdmin implements Admin {
  // Latest value schema id for push job status
  public static final int LATEST_PUSH_JOB_STATUS_VALUE_SCHEMA_ID = 2;

  private static final long SLEEP_INTERVAL_FOR_DATA_CONSUMPTION_IN_MS = 1000;
  private static final long SLEEP_INTERVAL_FOR_ASYNC_SETUP_MS = 1000;
  private static final int MAX_ASYNC_SETUP_RETRY_COUNT = 5;
  private static final Logger logger = Logger.getLogger(VeniceParentHelixAdmin.class);
  private static final String VENICE_INTERNAL_STORE_OWNER = "venice-internal";
  private static final int PUSH_JOB_STATUS_STORE_PARTITION_NUM = 3;
  private static final int PARTICIPANT_MESSAGE_STORE_PARTITION_NUM = 3;
  private static final String PUSH_JOB_STATUS_STORE_DESCRIPTOR = "push job status store";
  private static final String PARTICIPANT_MESSAGE_STORE_DESCRIPTOR = "participant message store";
  private static final int VERSION_ID_UNSET = -1;
  //Store version number to retain in Parent Controller to limit 'Store' ZNode size.
  protected static final int STORE_VERSION_RETENTION_COUNT = 5;
  public static final int MAX_PUSH_STATUS_PER_STORE_TO_KEEP = 10;

  protected final Map<String, Boolean> asyncSetupEnabledMap;
  private final VeniceHelixAdmin veniceHelixAdmin;
  private final Map<String, VeniceWriter<byte[], byte[]>> veniceWriterMap;
  private final OffsetManager offsetManager;
  private final byte[] emptyKeyByteArr = new byte[0];
  private final AdminOperationSerializer adminOperationSerializer = new AdminOperationSerializer();
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final Lock lock = new ReentrantLock();
  private final Map<String, AdminCommandExecutionTracker> adminCommandExecutionTrackers;
  private final boolean addVersionViaAdminProtocol;
  private long lastTopicCreationTime = -1;
  private Time timer = new SystemTime();

  private final MigrationPushStrategyZKAccessor pushStrategyZKAccessor;

  private ParentHelixOfflinePushAccessor offlinePushAccessor;

  /**
   * Here is the way how Parent Controller is keeping errored topics when {@link #maxErroredTopicNumToKeep} > 0:
   * 1. For errored topics, {@link #getOffLineJobStatus(String, String, Map, TopicManager)} won't truncate them;
   * 2. For errored topics, {@link #killOfflinePush(String, String)} won't truncate them;
   * 3. {@link #getTopicForCurrentPushJob(String, String)} will truncate the errored topics based on {@link #maxErroredTopicNumToKeep};
   *
   * It means error topic retiring is only be triggered by next push.
   *
   * When {@link #maxErroredTopicNumToKeep} is 0, errored topics will be truncated right away when job is finished.
   */
  private int maxErroredTopicNumToKeep;

  /**
   * Variable to store offset of last message for each cluster.
   * Before executing any request, this class will check whether last offset has been consumed or not:
   * If not, the current request will return with error after some time;
   * If yes, execute current request;
   *
   * Since current design will return error if the submitted message could not be consumed after some time.
   * During this time, if master controller changes to another host, the new controller could push another
   * message even the admin consumption task is still blocking by the bad message.
   *
   * TODO: Maybe we can initialize lastOffset to be the correct value when startup.
   */
  private Map<String, Long> clusterToLastOffsetMap;

  private final int waitingTimeForConsumptionMs;

  public VeniceParentHelixAdmin(VeniceHelixAdmin veniceHelixAdmin, VeniceControllerMultiClusterConfig multiClusterConfigs) {
    this.veniceHelixAdmin = veniceHelixAdmin;
    this.multiClusterConfigs = multiClusterConfigs;
    this.waitingTimeForConsumptionMs = multiClusterConfigs.getParentControllerWaitingTimeForConsumptionMs();
    this.veniceWriterMap = new ConcurrentHashMap<>();
    this.offsetManager = new AdminOffsetManager(this.veniceHelixAdmin.getZkClient(), this.veniceHelixAdmin.getAdapterSerializer());
    this.adminCommandExecutionTrackers = new HashMap<>();
    this.clusterToLastOffsetMap = new HashMap<>();
    this.asyncSetupEnabledMap = new HashMap<>();
    for (String cluster : multiClusterConfigs.getClusters()) {
      VeniceControllerConfig config = multiClusterConfigs.getConfigForCluster(cluster);
      adminCommandExecutionTrackers.put(cluster,
          new AdminCommandExecutionTracker(config.getClusterName(), veniceHelixAdmin.getExecutionIdAccessor(),
              getControllerClientMap(config.getClusterName())));
      clusterToLastOffsetMap.put(cluster, -1L);
    }
    this.pushStrategyZKAccessor = new MigrationPushStrategyZKAccessor(veniceHelixAdmin.getZkClient(),
        veniceHelixAdmin.getAdapterSerializer());
    this.maxErroredTopicNumToKeep = multiClusterConfigs.getParentControllerMaxErroredTopicNumToKeep();
    this.offlinePushAccessor =
        new ParentHelixOfflinePushAccessor(veniceHelixAdmin.getZkClient(), veniceHelixAdmin.getAdapterSerializer());
    this.addVersionViaAdminProtocol = multiClusterConfigs.getCommonConfig().isAddVersionViaAdminProtocolEnabled();
    // Start store migration monitor background thread
    startStoreMigrationMonitor();
  }

  // For testing purpose
  protected void setMaxErroredTopicNumToKeep(int maxErroredTopicNumToKeep) {
    this.maxErroredTopicNumToKeep = maxErroredTopicNumToKeep;
  }

  public void setVeniceWriterForCluster(String clusterName, VeniceWriter writer) {
    veniceWriterMap.putIfAbsent(clusterName, writer);
  }

  @Override
  public synchronized void start(String clusterName) {
    veniceHelixAdmin.start(clusterName);
    asyncSetupEnabledMap.put(clusterName, true);
    // We might not be able to call a lot of functions of veniceHelixAdmin since
    // current controller might not be the master controller for the given clusterName
    // Even current controller is master controller, it will take some time to become 'master'
    // since VeniceHelixAdmin.start won't wait for state becomes 'Master', but a lot of
    // VeniceHelixAdmin functions have 'mastership' check.

    // Check whether the admin topic exists or not
    String topicName = AdminTopicUtils.getTopicNameFromClusterName(clusterName);
    TopicManager topicManager = getTopicManager();
    if (topicManager.containsTopic(topicName)) {
      logger.info("Admin topic: " + topicName + " for cluster: " + clusterName + " already exists.");
    } else {
      // Create Kafka topic
      topicManager.createTopic(topicName, AdminTopicUtils.PARTITION_NUM_FOR_ADMIN_TOPIC, multiClusterConfigs.getKafkaReplicaFactor());
      logger.info("Created admin topic: " + topicName + " for cluster: " + clusterName);
    }

    // Initialize producer
    veniceWriterMap.computeIfAbsent(clusterName, (key) -> {
      /**
       * Venice just needs to check seq id in {@link com.linkedin.venice.controller.kafka.consumer.AdminConsumptionTask} to catch the following scenarios:
       * 1. Data missing;
       * 2. Data out of order;
       * 3. Data duplication;
       */
      return getVeniceWriterFactory().getBasicVeniceWriter(topicName, getTimer());
    });

    if (clusterName.equals(multiClusterConfigs.getPushJobStatusStoreClusterName())) {
      if (!multiClusterConfigs.getPushJobStatusStoreClusterName().isEmpty()
          && !multiClusterConfigs.getPushJobStatusStoreName().isEmpty()) {
        asyncSetupForInternalRTStore(multiClusterConfigs.getPushJobStatusStoreClusterName(),
            multiClusterConfigs.getPushJobStatusStoreName(), PUSH_JOB_STATUS_STORE_DESCRIPTOR,
            PushJobStatusRecordKey.SCHEMA$.toString(), PushJobStatusRecordValue.SCHEMA$.toString(),
            PUSH_JOB_STATUS_STORE_PARTITION_NUM);
      }
    }

    if (multiClusterConfigs.getCommonConfig().isParticipantMessageStoreEnabled()) {
      asyncSetupForInternalRTStore(clusterName, ParticipantMessageStoreUtils.getStoreNameForCluster(clusterName),
          PARTICIPANT_MESSAGE_STORE_DESCRIPTOR, ParticipantMessageKey.SCHEMA$.toString(),
          ParticipantMessageValue.SCHEMA$.toString(), PARTICIPANT_MESSAGE_STORE_PARTITION_NUM);
    }
  }

  /**
   * Setup the venice RT store used internally for hosting push job status records or participant messages.
   * If the store already exists and is in the correct state then only verification is performed.
   */
  private void asyncSetupForInternalRTStore(String clusterName, String storeName, String storeDescriptor,
      String keySchema, String valueSchema, int partitionCount) {
    CompletableFuture.runAsync(() -> {
      int retryCount = 0;
      boolean isStoreReady = false;
      while (!isStoreReady && asyncSetupEnabledMap.get(clusterName) && retryCount < MAX_ASYNC_SETUP_RETRY_COUNT) {
        try {
          if (retryCount > 0) {
            timer.sleep(SLEEP_INTERVAL_FOR_ASYNC_SETUP_MS);
          }
          isStoreReady = verifyAndCreateInternalStore(clusterName, storeName, storeDescriptor, keySchema, valueSchema,
              partitionCount);
        } catch (VeniceException e) {
          logger.info("VeniceException occurred during " + storeDescriptor + " setup with store "
              + storeName + " in cluster " + clusterName, e);
        } catch (Exception e) {
          logger.warn("Exception occurred aborting " + storeDescriptor + " setup with store "
              + storeName + " in cluster " + clusterName, e);
          break;
        } finally {
          retryCount++;
          logger.info("Async " + storeDescriptor + " setup attempts: " + retryCount + "/" + MAX_ASYNC_SETUP_RETRY_COUNT);
        }
      }
      if (isStoreReady) {
        logger.info(storeDescriptor + " has been successfully created or it already exists");
      } else {
        logger.error("Unable to create or find the " + storeDescriptor);
      }
    });
  }

  /**
   * Verify the state of the internal store for push job status. The master controller will also create and configure
   * the store if the desired state is not met.
   * @param clusterName the name of the cluster that push status store belongs to.
   * @param storeName the name of the push status store.
   * @return {@code true} if the push status store is ready, {@code false} otherwise.
   */
  private boolean verifyAndCreateInternalStore(String clusterName, String storeName, String storeDescriptor,
      String keySchema, String valueSchema, int partitionCount) {
    Store store = getStore(clusterName, storeName);
    boolean storeReady = false;
    UpdateStoreQueryParams updateStoreQueryParams;
    if (isMasterController(clusterName)) {
      if (store == null) {
        addStore(clusterName, storeName, VENICE_INTERNAL_STORE_OWNER, keySchema, valueSchema);
        store = getStore(clusterName, storeName);
        if (store == null) {
          throw new VeniceException("Unable to create or fetch the " + storeDescriptor);
        }
      }
      if (store.getPartitionCount() == 0) {
        updateStoreQueryParams = new UpdateStoreQueryParams();
        updateStoreQueryParams.setPartitionCount(partitionCount);
        updateStore(clusterName, storeName, updateStoreQueryParams);
      }
      if (!store.isHybrid()) {
        updateStoreQueryParams = new UpdateStoreQueryParams();
        updateStoreQueryParams.setHybridOffsetLagThreshold(100L);
        updateStoreQueryParams.setHybridRewindSeconds(TimeUnit.DAYS.toMillis(7));
        updateStore(clusterName, storeName, updateStoreQueryParams);
        store = getStore(clusterName, storeName);
        if (!store.isHybrid()) {
          throw new VeniceException("Unable to update the " + storeDescriptor + " to a hybrid store");
        }
      }
      if (store.getVersions().isEmpty()) {
        int replicationFactor = getReplicationFactor(clusterName, storeName);
        Version version =
            incrementVersionIdempotent(clusterName, storeName, Version.guidBasedDummyPushId(),
                partitionCount, replicationFactor, true);
        writeEndOfPush(clusterName, storeName, version.getNumber(), true);
        store = getStore(clusterName, storeName);
        if (store.getVersions().isEmpty()) {
          throw new VeniceException("Unable to initialize a version for the " + storeDescriptor);
        }
      }
      String pushJobStatusRtTopic = getRealTimeTopic(clusterName, storeName);
      if (!pushJobStatusRtTopic.equals(Version.composeRealTimeTopic(storeName))) {
        throw new VeniceException("Unexpected real time topic name for the " + storeDescriptor);
      }
      storeReady = true;
    } else {
      if (store != null && store.isHybrid() && !store.getVersions().isEmpty()) {
        storeReady = true;
      }
    }
    return storeReady;
  }

  @Override
  public boolean isClusterValid(String clusterName) {
    return veniceHelixAdmin.isClusterValid(clusterName);
  }

  private void sendAdminMessageAndWaitForConsumed(String clusterName, AdminOperation message) {
    if (!veniceWriterMap.containsKey(clusterName)) {
      throw new VeniceException("Cluster: " + clusterName + " is not started yet!");
    }
    AdminCommandExecutionTracker adminCommandExecutionTracker = adminCommandExecutionTrackers.get(clusterName);
    AdminCommandExecution execution =
        adminCommandExecutionTracker.createExecution(AdminMessageType.valueOf(message).name());
    message.executionId = execution.getExecutionId();
    VeniceWriter<byte[], byte[]> veniceWriter = veniceWriterMap.get(clusterName);
    byte[] serializedValue = adminOperationSerializer.serialize(message);
    try {
      Future<RecordMetadata> future = veniceWriter.put(emptyKeyByteArr, serializedValue, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
      RecordMetadata meta = future.get();

      long lastOffset = meta.offset();
      clusterToLastOffsetMap.put(clusterName, lastOffset);
      logger.info("Sent message: " + message + " to kafka, offset: " + lastOffset);
      waitingLastOffsetToBeConsumed(clusterName);
      adminCommandExecutionTracker.startTrackingExecution(execution);
    } catch (Exception e) {
      throw new VeniceException("Got exception during sending message to Kafka -- " + e.getMessage(), e);
    }
  }

  private void waitingLastOffsetToBeConsumed(String clusterName) {
    String topicName = AdminTopicUtils.getTopicNameFromClusterName(clusterName);

    // Blocking until consumer consumes the new message or timeout
    long startTime = SystemTime.INSTANCE.getMilliseconds();
    long lastOffset = clusterToLastOffsetMap.get(clusterName);
    while (true) {
      long consumedOffset = offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID).getOffset();
      if (consumedOffset >= lastOffset) {
        break;
      }
      // Check whether timeout
      long currentTime = SystemTime.INSTANCE.getMilliseconds();
      if (currentTime - startTime > waitingTimeForConsumptionMs) {
        Exception lastException = veniceHelixAdmin.getLastException(clusterName);
        String exceptionMsg = null == lastException ? "null" : lastException.getMessage();
        String errMsg = "Timeout " + waitingTimeForConsumptionMs + "ms waiting for admin consumption to catch up.";
        errMsg += "  consumedOffset=" + consumedOffset + " lastOffset=" + lastOffset;
        errMsg += "  Last exception: " + exceptionMsg;
        if (getAdminCommandExecutionTracker(clusterName).isPresent()){
          errMsg += "  RunningExecutions: " + getAdminCommandExecutionTracker(clusterName).get().executionsAsString();
        }
        throw new VeniceException(errMsg, lastException);
      }

      logger.info("Waiting until consumed " + lastOffset + ", currently at " + consumedOffset);
      Utils.sleep(SLEEP_INTERVAL_FOR_DATA_CONSUMPTION_IN_MS);
    }
    logger.info("The latest message has been consumed, offset: " + lastOffset);
  }

  private void acquireLock(String clusterName) {
    try {
      // First to check whether last offset has been consumed or not
      waitingLastOffsetToBeConsumed(clusterName);
      boolean acquired = lock.tryLock(waitingTimeForConsumptionMs, TimeUnit.MILLISECONDS);
      if (!acquired) {
        throw new VeniceException("Failed to acquire lock, and some other operation is ongoing");
      }
    } catch (InterruptedException e) {
      throw new VeniceException("Got interrupted during acquiring lock", e);
    }
  }

  private void releaseLock() {
    lock.unlock();
  }

  @Override
  public void addStore(String clusterName, String storeName, String owner, String keySchema, String valueSchema) {
    acquireLock(clusterName);
    try {
      veniceHelixAdmin.checkPreConditionForAddStore(clusterName, storeName, keySchema, valueSchema);
      logger.info("Adding store: " + storeName + " to cluster: " + clusterName);

      // Write store creation message to Kafka
      StoreCreation storeCreation = (StoreCreation) AdminMessageType.STORE_CREATION.getNewInstance();
      storeCreation.clusterName = clusterName;
      storeCreation.storeName = storeName;
      storeCreation.owner = owner;
      storeCreation.keySchema = new SchemaMeta();
      storeCreation.keySchema.schemaType = SchemaType.AVRO_1_4.getValue();
      storeCreation.keySchema.definition = keySchema;
      storeCreation.valueSchema = new SchemaMeta();
      storeCreation.valueSchema.schemaType = SchemaType.AVRO_1_4.getValue();
      storeCreation.valueSchema.definition = valueSchema;

      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.STORE_CREATION.getValue();
      message.payloadUnion = storeCreation;
      sendAdminMessageAndWaitForConsumed(clusterName, message);
    } finally {
      releaseLock();
    }
  }

  @Override
  public void deleteStore(String clusterName, String storeName, int largestUsedVersionNumber) {
    acquireLock(clusterName);
    try {
      Store store = veniceHelixAdmin.checkPreConditionForDeletion(clusterName, storeName);
      DeleteStore deleteStore = (DeleteStore) AdminMessageType.DELETE_STORE.getNewInstance();
      deleteStore.clusterName = clusterName;
      deleteStore.storeName = storeName;
      // Tell each prod colo the largest used version number in corp to make it consistent.
      deleteStore.largestUsedVersionNumber = store.getLargestUsedVersionNumber();
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.DELETE_STORE.getValue();
      message.payloadUnion = deleteStore;

      sendAdminMessageAndWaitForConsumed(clusterName, message);
    } finally {
      releaseLock();
    }
  }

  @Override
  public Version addVersion(String clusterName,
                            String storeName,
                            int versionNumber,
                            int numberOfPartition,
                            int replicationFactor) {
    throw new VeniceUnsupportedOperationException("addVersion");
  }

  @Override
  public void addVersionAndStartIngestion(
      String clusterName, String storeName, String pushJobId, int versionNumber, int numberOfPartitions) {
    throw new VeniceUnsupportedOperationException("addVersionAndStartIngestion");
  }

  /**
   * Since there is no offline push running in Parent Controller,
   * the old store versions won't be cleaned up by job completion action, so Parent Controller chooses
   * to clean it up when the new store version gets created.
   * It is OK to clean up the old store versions in Parent Controller without notifying Child Controller since
   * store version in Parent Controller doesn't maintain actual version status, and only for tracking
   * the store version creation history.
   */
  protected void cleanupHistoricalVersions(String clusterName, String storeName) {
    HelixReadWriteStoreRepository storeRepo = veniceHelixAdmin.getVeniceHelixResource(clusterName)
        .getMetadataRepository();
    storeRepo.lock();
    try {
      Store store = storeRepo.getStore(storeName);
      if (null == store) {
        logger.info("The store to clean up: " + storeName + " doesn't exist");
        return;
      }
      List<Version> versions = store.getVersions();
      final int versionCount = versions.size();
      if (versionCount <= STORE_VERSION_RETENTION_COUNT) {
        return;
      }
      List<Version> clonedVersions = new ArrayList<>(versions);
      clonedVersions.stream()
          .sorted()
          .limit(versionCount - STORE_VERSION_RETENTION_COUNT)
          .forEach(v -> store.deleteVersion(v.getNumber()));
      storeRepo.updateStore(store);
    } finally {
      storeRepo.unLock();
    }
  }

  /**
 * Check whether any topic for this store exists or not.
 * The existing topic could be introduced by two cases:
 * 1. The previous job push is still running;
 * 2. The previous job push fails to delete this topic;
 *
 * For the 1st case, it is expected to refuse the new data push,
 * and for the 2nd case, customer should reach out Venice team to fix this issue for now.
 **/
  protected List<String> existingTopicsForStore(String storeName) {
    List<String> outputList = new ArrayList<>();
    TopicManager topicManager = getTopicManager();
    Set<String> topics = topicManager.listTopics();
    String storeNameForCurrentTopic;
    for (String topic: topics) {
      if (AdminTopicUtils.isAdminTopic(topic) || AdminTopicUtils.isKafkaInternalTopic(topic) || Version.isRealTimeTopic(topic)) {
        continue;
      }
      try {
        storeNameForCurrentTopic = Version.parseStoreFromKafkaTopicName(topic);
      } catch (Exception e) {
        logger.warn("Failed to parse StoreName from topic: " + topic, e);
        continue;
      }
      if (storeNameForCurrentTopic.equals(storeName)) {
        outputList.add(topic);
      }
    }
    return outputList;
  }

  /**
   * Get the latest existing Kafka topic for the specified store
   * @param storeName
   * @return
   */
  protected Optional<String> getLatestKafkaTopic(String storeName) {
    List<String> existingTopics = existingTopicsForStore(storeName);
    if (existingTopics.isEmpty()) {
      return Optional.empty();
    }
    existingTopics.sort((t1, t2) -> {
      int v1 = Version.parseVersionFromKafkaTopicName(t1);
      int v2 = Version.parseVersionFromKafkaTopicName(t2);
      return v2 - v1;
    });
    return Optional.of(existingTopics.get(0));
  }

  protected Optional<String> getTopicForCurrentPushJob(String clusterName, String storeName) {
    return getTopicForCurrentPushJob(clusterName, storeName, false);
  }

  /**
   * If there is no ongoing push for specified store currently, this function will return {@link Optional#empty()},
   * else will return the ongoing Kafka topic. It will also try to clean up legacy topics.
   */
  protected Optional<String> getTopicForCurrentPushJob(String clusterName, String storeName, boolean isIncrementalPush) {
    Optional<String> latestKafkaTopic = getLatestKafkaTopic(storeName);
    /**
     * Check current topic retention to decide whether the previous job is already done or not
     */
    if (latestKafkaTopic.isPresent()) {
      logger.debug("Latest kafka topic for store: " + storeName + " is " + latestKafkaTopic.get());


      if (!isTopicTruncated(latestKafkaTopic.get())) {
        /**
         * Check whether the corresponding version exists or not, since it is possible that last push
         * meets Kafka topic creation timeout.
         * When Kafka topic creation timeout happens, topic/job could be still running, but the version
         * should not exist according to the logic in {@link VeniceHelixAdmin#addVersion}.
         *
         * If the corresponding version doesn't exist, this function will issue command to kill job to deprecate
         * the incomplete topic/job.
         */
        Store store = getStore(clusterName, storeName);
        Optional<Version> version = store.getVersion(Version.parseVersionFromKafkaTopicName(latestKafkaTopic.get()));
        if (! version.isPresent()) {
          // The corresponding version doesn't exist.
          killOfflinePush(clusterName, latestKafkaTopic.get());
          logger.info("Found topic: " + latestKafkaTopic.get() + " without the corresponding version, will kill it");
          return Optional.empty();
        }

        /**
         * If Parent Controller could not infer the job status from topic retention policy, it will check the actual
         * job status by sending requests to each individual datacenter.
         * If the job is still running, Parent Controller will block current push.
         */
        final long SLEEP_MS_BETWEEN_RETRY = TimeUnit.SECONDS.toMillis(10);
        ExecutionStatus jobStatus = ExecutionStatus.PROGRESS;
        Map<String, String> extraInfo = new HashMap<>();

        int retryTimes = 5;
        int current = 0;
        while (current++ < retryTimes) {
          OfflinePushStatusInfo offlineJobStatus = getOffLinePushStatus(clusterName, latestKafkaTopic.get());
          jobStatus = offlineJobStatus.getExecutionStatus();
          extraInfo = offlineJobStatus.getExtraInfo();
          if (!extraInfo.containsValue(ExecutionStatus.UNKNOWN.toString())) {
            break;
          }
          // Retry since there is a connection failure when querying job status against each datacenter
          try {
            timer.sleep(SLEEP_MS_BETWEEN_RETRY);
          } catch (InterruptedException e) {
            throw new VeniceException("Received InterruptedException during sleep between 'getOffLinePushStatus' calls");
          }
        }
        if (extraInfo.containsValue(ExecutionStatus.UNKNOWN.toString())) {
          // TODO: Do we need to throw exception here??
          logger.error("Failed to get job status for topic: " + latestKafkaTopic.get() + " after retrying " + retryTimes
              + " times, extra info: " + extraInfo);
        }
        if (!jobStatus.isTerminal()) {
          logger.info(
              "Job status: " + jobStatus + " for Kafka topic: " + latestKafkaTopic.get() + " is not terminal, extra info: " + extraInfo);
          return latestKafkaTopic;
        } else {
          /**
           * If the job status of latestKafkaTopic is terminal and it is not an incremental push,
           * it will be truncated in {@link #getOffLinePushStatus(String, String)}.
           */
          if (!isIncrementalPush) {
            truncateTopicsBasedOnMaxErroredTopicNumToKeep(storeName);
          }
        }
      }
    }
    return Optional.empty();
  }

  /**
   * Only keep {@link #maxErroredTopicNumToKeep} non-truncated topics ordered by version
   * N.B. This method was originally introduced to debug KMM issues. But now it works
   * as a general method for cleaning up leaking topics. ({@link #maxErroredTopicNumToKeep}
   * is always 0.)
   *
   * TODO: rename the method once we remove the rest of KMM debugging logic.
   */
  protected void truncateTopicsBasedOnMaxErroredTopicNumToKeep(String storeName) {
    List<String> topics = existingTopicsForStore(storeName);
    // Based on current logic, only 'errored' topics were not truncated.
    List<String> sortedNonTruncatedTopics = topics.stream().filter(topic -> !isTopicTruncated(topic)).sorted((t1, t2) -> {
      int v1 = Version.parseVersionFromKafkaTopicName(t1);
      int v2 = Version.parseVersionFromKafkaTopicName(t2);
      return v1 - v2;
    }).collect(Collectors.toList());
    if (sortedNonTruncatedTopics.size() <= maxErroredTopicNumToKeep) {
      logger.info("Non-truncated topics size: " + sortedNonTruncatedTopics.size() +
          " isn't bigger than maxErroredTopicNumToKeep: " + maxErroredTopicNumToKeep + ", so no topic will be truncated this time");
      return;
    }
    int topicNumToTruncate = sortedNonTruncatedTopics.size() - maxErroredTopicNumToKeep;
    int truncatedTopicCnt = 0;
    for (String topic: sortedNonTruncatedTopics) {
      if (++truncatedTopicCnt > topicNumToTruncate) {
        break;
      }
      truncateKafkaTopic(topic);
      logger.info("Errored topic: " + topic + " got truncated");
    }
  }

  // TODO: this function isn't able to block parallel push in theory since it is not synchronized.
  // TODO: this function should be removed, keeping for now as a record on how we used to use mayThrottleTopicCreation and createOfflinePushStatus
  public Version incrementVersion(String clusterName,
                                  String storeName,
                                  String pushJobId,
                                  int numberOfPartition,
                                  int replicationFactor) {
    // TODO: consider to move version creation to admin protocol
    // Right now, TopicMonitor in each prod colo will monitor new Kafka topic and
    // create new corresponding store versions
    // Adding version in Parent Controller won't start offline push job.

    /*
     * Check the offline job status for this topic, and if it has already been terminated, we will skip it.
     * Otherwise, an exception will be thrown to stop concurrent data pushes.
     *
     * {@link #getOffLinePushStatus(String, String)} will remove the topic if the offline job has been terminated,
     * so we don't need to explicitly remove it here.
     */
    Optional<String> currentPush = getTopicForCurrentPushJob(clusterName, storeName);
    if (currentPush.isPresent()) {
      throw new VeniceException("Topic: " + currentPush.get() + " exists for store: " + storeName +
          ", please wait for previous job to be finished, and reach out Venice team if it is" +
          " not this case");
    }

    mayThrottleTopicCreation(timer);
    Version newVersion = veniceHelixAdmin.addVersion(clusterName, storeName, pushJobId, VeniceHelixAdmin.VERSION_ID_UNSET,
        numberOfPartition, replicationFactor, false, false, false);
    cleanupHistoricalVersions(clusterName, storeName);
    createOfflinePushStatus(clusterName, storeName, newVersion.getNumber(), numberOfPartition, replicationFactor);
    return newVersion;
  }

  protected synchronized void createOfflinePushStatus(String cluster, String storeName, int version, int partitionCount, int replicationFactor){
    VeniceHelixResources resources = veniceHelixAdmin.getVeniceHelixResource(cluster);
    ReadWriteStoreRepository storeRepository = resources.getMetadataRepository();
    Store store = storeRepository.getStore(storeName);
    String topicName = Version.composeKafkaTopic(storeName, version);
    OfflinePushStatus status = new OfflinePushStatus(topicName, partitionCount, replicationFactor, store.getOffLinePushStrategy());
    offlinePushAccessor.createOfflinePushStatus(cluster, status);
    // Collect the old push status.
    List<String> names = offlinePushAccessor.getAllPushNames(cluster);
    List<Integer> versionNumbers = names
        .stream()
        .filter(topic -> Version.parseStoreFromKafkaTopicName(topic).equals(storeName))
        .map(Version::parseVersionFromKafkaTopicName)
        .collect(Collectors.toList());
    while (versionNumbers.size() > MAX_PUSH_STATUS_PER_STORE_TO_KEEP) {
      int oldestVersionNumber = Collections.min(versionNumbers);
      versionNumbers.remove(Integer.valueOf(oldestVersionNumber));
      String oldestTopicName = Version.composeKafkaTopic(storeName, oldestVersionNumber);
      offlinePushAccessor.deleteOfflinePushStatus(cluster, oldestTopicName);
    }
  }

  // TODO throttle for topic creation may no longer be needed, if so remove this method.
  /**
   * This method will throttle the topic creation operation. During each time window, it only allow ONE topic to be created.
   * Other topic creation request will be blocked until time goes to the next time window.
   */
  protected synchronized void mayThrottleTopicCreation(Time timer) {
    long timeSinceLastTopicCreation = timer.getMilliseconds() - lastTopicCreationTime;
    if (lastTopicCreationTime < 0) {
      // First time to create topic on this controller. Considering the failover case that another controller could
      // just create a topic then failed, this controller take over the cluster and start to create a new topic,
      // The time interval between those two creation might be less than TopicCreationThrottlingTimeWindowMs.
      // So we should sleep at least TopicCreationThrottlingTimeWindowMs here to prevent creating topic too frequently.
      timeSinceLastTopicCreation = 0;
    }
    if (timeSinceLastTopicCreation < multiClusterConfigs.getTopicCreationThrottlingTimeWindowMs()) {
      try {
        timer.sleep(multiClusterConfigs.getTopicCreationThrottlingTimeWindowMs() - timeSinceLastTopicCreation);
      } catch (InterruptedException e) {
        throw new VeniceException(
            "Topic creation throttler is interrupted while blocking too frequent topic creation operation.", e);
      }
    }
    lastTopicCreationTime = timer.getMilliseconds();
  }

  /**
   * TODO: Remove the old behavior and refactor once add version via the admin protocol is stable.
   * TODO: refactor so that this method and the counterpart in {@link VeniceHelixAdmin} should have same behavior
   */
  @Override
  public Version incrementVersionIdempotent(String clusterName, String storeName, String pushJobId,
      int numberOfPartitions, int replicationFactor, boolean offlinePush, boolean isIncrementalPush,
      boolean sendStartOfPush) {

    Optional<String> currentPushTopic = getTopicForCurrentPushJob(clusterName, storeName, isIncrementalPush);
    if (currentPushTopic.isPresent()) {
      int currentPushVersion = Version.parseVersionFromKafkaTopicName(currentPushTopic.get());
      Optional<Version> version = getStore(clusterName, storeName).getVersion(currentPushVersion);
      if (!version.isPresent()) {
        throw new VeniceException("A corresponding version should exist with the ongoing push with topic "
            + currentPushTopic);
      }
      String existingPushJobId = version.get().getPushJobId();
      if (!existingPushJobId.equals(pushJobId)) {
        throw new VeniceException(
            "Unable to start the push with pushJobId " + pushJobId + " for store " + storeName
                + ". An ongoing push with pushJobId " + existingPushJobId + " and topic " + currentPushTopic
                + " is found and it must be terminated before another push can be started.");
      }
    }
    // This is a ParentAdmin, so ignore the passed in offlinePush parameter and DO NOT try to start an offline push
    Version newVersion = veniceHelixAdmin.incrementVersionIdempotent(clusterName, storeName, pushJobId, numberOfPartitions,
        replicationFactor, false, isIncrementalPush, sendStartOfPush);;
    if (addVersionViaAdminProtocol && !isIncrementalPush) {
      acquireLock(clusterName);
      try {
        sendAddVersionAdminMessage(clusterName, storeName, pushJobId, newVersion.getNumber(), numberOfPartitions);
      } finally {
        releaseLock();
      }
    }
    cleanupHistoricalVersions(clusterName, storeName);

    return newVersion;
  }

  protected void sendAddVersionAdminMessage(String clusterName, String storeName, String pushJobId, int versionNum,
      int numberOfPartitions) {
    AddVersion addVersion = (AddVersion) AdminMessageType.ADD_VERSION.getNewInstance();
    addVersion.clusterName = clusterName;
    addVersion.storeName = storeName;
    addVersion.pushJobId = pushJobId;
    addVersion.versionNum = versionNum;
    addVersion.numberOfPartitions = numberOfPartitions;

    AdminOperation message = new AdminOperation();
    message.operationType = AdminMessageType.ADD_VERSION.getValue();
    message.payloadUnion = addVersion;

    sendAdminMessageAndWaitForConsumed(clusterName, message);
  }

  @Override
  public synchronized String getRealTimeTopic(String clusterName, String storeName){
    return veniceHelixAdmin.getRealTimeTopic(clusterName, storeName);
  }

  /**
   * A couple of extra checks are needed in parent controller
   * 1. check batch job statuses across child controllers. (We cannot only check the version status
   * in parent controller since they are marked as STARTED)
   * 2. check if the topic is marked to be truncated or not. (This could be removed if we don't
   * preserve incremental push topic in parent Kafka anymore
   */
  @Override
  public synchronized Version getIncrementalPushVersion(String clusterName, String storeName) {
    Version incrementalPushVersion = veniceHelixAdmin.getIncrementalPushVersion(clusterName, storeName);
    String incrementalPushTopic = incrementalPushVersion.kafkaTopicName();
    ExecutionStatus status = getOffLinePushStatus(clusterName, incrementalPushTopic, Optional.empty()).getExecutionStatus();

    return getIncrementalPushVersion(incrementalPushVersion, status);
  }

  //This method is only for internal / test use case
  Version getIncrementalPushVersion(Version incrementalPushVersion, ExecutionStatus status) {
    String incrementalPushTopic = incrementalPushVersion.kafkaTopicName();
    String storeName = incrementalPushVersion.getStoreName();

    if (!status.isTerminal()) {
      throw new VeniceException("Cannot start incremental push since batch push is on going." + " store: " + storeName);
    }

    if(status == ExecutionStatus.ERROR || veniceHelixAdmin.isTopicTruncated(incrementalPushTopic)) {
      throw new VeniceException("Cannot start incremental push since previous batch push has failed. Please run another bash job."
          + " store: " + storeName);
    }

    return incrementalPushVersion;
  }

  @Override
  public int getCurrentVersion(String clusterName, String storeName) {
    throw new VeniceUnsupportedOperationException("getCurrentVersion", "Please use getCurrentVersionsForMultiColos in Parent controller.");
  }

  /**
   * Query the current version for the given store. In parent colo, Venice do not update the current version because
   * there is not offline push monitor. So parent controller will query each prod controller and return the map.
   */
  @Override
  public Map<String, Integer> getCurrentVersionsForMultiColos(String clusterName, String storeName) {
    Map<String, ControllerClient> controllerClients = getControllerClientMap(clusterName);
    return getCurrentVersionForMultiColos(clusterName, storeName, controllerClients);
  }

  protected Map<String, Integer> getCurrentVersionForMultiColos(String clusterName, String storeName,
      Map<String, ControllerClient> controllerClients) {
    Set<String> prodColos = controllerClients.keySet();
    Map<String, Integer> result = new HashMap<>();
    for (String colo : prodColos) {
      StoreResponse response = controllerClients.get(colo).getStore(storeName);
      if (response.isError()) {
        logger.error(
            "Could not query store from colo: " + colo + " for cluster: " + clusterName + ". " + response.getError());
        result.put(colo, AdminConsumptionTask.IGNORED_CURRENT_VERSION);
      } else {
        result.put(colo,response.getStore().getCurrentVersion());
      }
    }
    return result;
  }

  @Override
  public Version peekNextVersion(String clusterName, String storeName) {
    throw new VeniceUnsupportedOperationException("peekNextVersion");
  }

  @Override
  public List<Version> deleteAllVersionsInStore(String clusterName, String storeName) {
    acquireLock(clusterName);
    try {
      veniceHelixAdmin.checkPreConditionForDeletion(clusterName, storeName);

      DeleteAllVersions deleteAllVersions = (DeleteAllVersions) AdminMessageType.DELETE_ALL_VERSIONS.getNewInstance();
      deleteAllVersions.clusterName = clusterName;
      deleteAllVersions.storeName = storeName;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.DELETE_ALL_VERSIONS.getValue();
      message.payloadUnion = deleteAllVersions;

      sendAdminMessageAndWaitForConsumed(clusterName, message);
      return Collections.emptyList();
    } finally {
      releaseLock();
    }
  }

  @Override
  public void deleteOldVersionInStore(String clusterName, String storeName, int versionNum) {
    acquireLock(clusterName);
    try {
      veniceHelixAdmin.checkPreConditionForSingleVersionDeletion(clusterName, storeName, versionNum);

      DeleteOldVersion deleteOldVersion = (DeleteOldVersion) AdminMessageType.DELETE_OLD_VERSION.getNewInstance();
      deleteOldVersion.clusterName = clusterName;
      deleteOldVersion.storeName = storeName;
      deleteOldVersion.versionNum = versionNum;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.DELETE_OLD_VERSION.getValue();
      message.payloadUnion = deleteOldVersion;

      sendAdminMessageAndWaitForConsumed(clusterName, message);
    } finally {
      releaseLock();
    }
  }

  @Override
  public List<Version> versionsForStore(String clusterName, String storeName) {
    return veniceHelixAdmin.versionsForStore(clusterName, storeName);
  }

  @Override
  public List<Store> getAllStores(String clusterName) {
    return veniceHelixAdmin.getAllStores(clusterName);
  }

  @Override
  public Map<String, String> getAllStoreStatuses(String clusterName) {
    throw new VeniceUnsupportedOperationException("getAllStoreStatuses");
  }

  @Override
  public Store getStore(String clusterName, String storeName) {
    return veniceHelixAdmin.getStore(clusterName, storeName);
  }

  @Override
  public boolean hasStore(String clusterName, String storeName) {
    return veniceHelixAdmin.hasStore(clusterName, storeName);
  }

  @Override
  public void setStoreCurrentVersion(String clusterName,
                                String storeName,
                                int versionNumber) {
    acquireLock(clusterName);
    try {
      veniceHelixAdmin.checkPreConditionForUpdateStoreMetadata(clusterName, storeName);

      SetStoreCurrentVersion setStoreCurrentVersion = (SetStoreCurrentVersion) AdminMessageType.SET_STORE_CURRENT_VERSION.getNewInstance();
      setStoreCurrentVersion.clusterName = clusterName;
      setStoreCurrentVersion.storeName = storeName;
      setStoreCurrentVersion.currentVersion = versionNumber;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.SET_STORE_CURRENT_VERSION.getValue();
      message.payloadUnion = setStoreCurrentVersion;

      sendAdminMessageAndWaitForConsumed(clusterName, message);
    } finally {
      releaseLock();
    }
  }

  @Override
  public synchronized void setStoreLargestUsedVersion(String clusterName, String storeName, int versionNumber) {
    throw new VeniceUnsupportedOperationException("setStoreLargestUsedVersion", "This is only supported in the Child Controller.");
  }


  @Override
  public void setStoreOwner(String clusterName, String storeName, String owner) {
    acquireLock(clusterName);
    try {
      veniceHelixAdmin.checkPreConditionForUpdateStoreMetadata(clusterName, storeName);

      SetStoreOwner setStoreOwner = (SetStoreOwner) AdminMessageType.SET_STORE_OWNER.getNewInstance();
      setStoreOwner.clusterName = clusterName;
      setStoreOwner.storeName = storeName;
      setStoreOwner.owner = owner;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.SET_STORE_OWNER.getValue();
      message.payloadUnion = setStoreOwner;

      sendAdminMessageAndWaitForConsumed(clusterName, message);
    } finally {
      releaseLock();
    }
  }

  @Override
  public void setStorePartitionCount(String clusterName, String storeName, int partitionCount) {
    acquireLock(clusterName);
    try {
      veniceHelixAdmin.checkPreConditionForUpdateStoreMetadata(clusterName, storeName);

      SetStorePartitionCount setStorePartition = (SetStorePartitionCount) AdminMessageType.SET_STORE_PARTITION.getNewInstance();
      setStorePartition.clusterName = clusterName;
      setStorePartition.storeName = storeName;
      setStorePartition.partitionNum = partitionCount;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.SET_STORE_PARTITION.getValue();
      message.payloadUnion = setStorePartition;

      sendAdminMessageAndWaitForConsumed(clusterName, message);
    } finally {
      releaseLock();
    }
  }

  @Override
  public void setStoreReadability(String clusterName, String storeName, boolean desiredReadability) {
    acquireLock(clusterName);
    try {
      veniceHelixAdmin.checkPreConditionForUpdateStoreMetadata(clusterName, storeName);

      AdminOperation message = new AdminOperation();

      if (desiredReadability) {
        message.operationType = AdminMessageType.ENABLE_STORE_READ.getValue();
        EnableStoreRead enableStoreRead = (EnableStoreRead) AdminMessageType.ENABLE_STORE_READ.getNewInstance();
        enableStoreRead.clusterName = clusterName;
        enableStoreRead.storeName = storeName;
        message.payloadUnion = enableStoreRead;
      } else {
        message.operationType = AdminMessageType.DISABLE_STORE_READ.getValue();
        DisableStoreRead disableStoreRead = (DisableStoreRead) AdminMessageType.DISABLE_STORE_READ.getNewInstance();
        disableStoreRead.clusterName = clusterName;
        disableStoreRead.storeName = storeName;
        message.payloadUnion = disableStoreRead;
      }

      sendAdminMessageAndWaitForConsumed(clusterName, message);
    } finally {
      releaseLock();
    }
  }

  @Override
  public void setStoreWriteability(String clusterName, String storeName, boolean desiredWriteability) {
    acquireLock(clusterName);
    try {
      veniceHelixAdmin.checkPreConditionForUpdateStoreMetadata(clusterName, storeName);

      AdminOperation message = new AdminOperation();

      if (desiredWriteability) {
        message.operationType = AdminMessageType.ENABLE_STORE_WRITE.getValue();
        ResumeStore resumeStore = (ResumeStore) AdminMessageType.ENABLE_STORE_WRITE.getNewInstance();
        resumeStore.clusterName = clusterName;
        resumeStore.storeName = storeName;
        message.payloadUnion = resumeStore;
      } else {
        message.operationType = AdminMessageType.DISABLE_STORE_WRITE.getValue();
        PauseStore pauseStore = (PauseStore) AdminMessageType.DISABLE_STORE_WRITE.getNewInstance();
        pauseStore.clusterName = clusterName;
        pauseStore.storeName = storeName;
        message.payloadUnion = pauseStore;
      }

      sendAdminMessageAndWaitForConsumed(clusterName, message);
    } finally {
      releaseLock();
    }
  }

  @Override
  public void setStoreReadWriteability(String clusterName, String storeName, boolean isAccessible) {
    setStoreReadability(clusterName, storeName, isAccessible);
    setStoreWriteability(clusterName, storeName, isAccessible);
  }

  @Override
  public void updateStore(String clusterName,
      String storeName,
      Optional<String> owner,
      Optional<Boolean> readability,
      Optional<Boolean> writeability,
      Optional<Integer> partitionCount,
      Optional<Long> storageQuotaInByte,
      Optional<Long> readQuotaInCU,
      Optional<Integer> currentVersion,
      Optional<Integer> largestUsedVersionNumber,
      Optional<Long> hybridRewindSeconds,
      Optional<Long> hybridOffsetLagThreshold,
      Optional<Boolean> accessControlled,
      Optional<CompressionStrategy> compressionStrategy,
      Optional<Boolean> chunkingEnabled,
      Optional<Boolean> singleGetRouterCacheEnabled,
      Optional<Boolean> batchGetRouterCacheEnabled,
      Optional<Integer> batchGetLimit,
      Optional<Integer> numVersionsToPreserve,
      Optional<Boolean> incrementalPushEnabled,
      Optional<Boolean> storeMigration,
      Optional<Boolean> writeComputationEnabled,
      Optional<Boolean> readComputationEnabled,
      Optional<Integer> bootstrapToOnlineTimeoutInHours,
      Optional<Boolean> leaderFollowerModelEnabled) {
    acquireLock(clusterName);

    try {
      Store store = veniceHelixAdmin.getStore(clusterName, storeName);

      if (store.isMigrating()) {
        if (!(storeMigration.isPresent() || readability.isPresent() || writeability.isPresent())) {
          String errMsg = "This update operation is not allowed during store migration!";
          logger.warn(errMsg + " Store name: " + storeName);
          throw new VeniceException(errMsg);
        }
      }

      UpdateStore setStore = (UpdateStore) AdminMessageType.UPDATE_STORE.getNewInstance();
      setStore.clusterName = clusterName;
      setStore.storeName = storeName;
      setStore.owner = owner.isPresent() ? owner.get() : store.getOwner();
      if (partitionCount.isPresent() && store.isHybrid()){
        throw new VeniceHttpException(HttpStatus.SC_BAD_REQUEST, "Cannot change partition count for hybrid stores");
      }
      setStore.partitionNum = partitionCount.isPresent() ? partitionCount.get() : store.getPartitionCount();
      setStore.enableReads = readability.isPresent() ? readability.get() : store.isEnableReads();
      setStore.enableWrites = writeability.isPresent() ? writeability.get() : store.isEnableWrites();
      setStore.storageQuotaInByte =
          storageQuotaInByte.isPresent() ? storageQuotaInByte.get() : store.getStorageQuotaInByte();
      setStore.readQuotaInCU = readQuotaInCU.isPresent() ? readQuotaInCU.get() : store.getReadQuotaInCU();
      //We need to to be careful when handling currentVersion.
      //Since it is not synced between parent and local controller,
      //It is very likely to override local values unintentionally.
      setStore.currentVersion = currentVersion.isPresent()?currentVersion.get(): AdminConsumptionTask.IGNORED_CURRENT_VERSION;

      HybridStoreConfig hybridStoreConfig = VeniceHelixAdmin.mergeNewSettingsIntoOldHybridStoreConfig(
          store, hybridRewindSeconds, hybridOffsetLagThreshold);
      if (null == hybridStoreConfig) {
        setStore.hybridStoreConfig = null;
      } else {
        HybridStoreConfigRecord hybridStoreConfigRecord = new HybridStoreConfigRecord();
        hybridStoreConfigRecord.offsetLagThresholdToGoOnline = hybridStoreConfig.getOffsetLagThresholdToGoOnline();
        hybridStoreConfigRecord.rewindTimeInSeconds = hybridStoreConfig.getRewindTimeInSeconds();
        setStore.hybridStoreConfig = hybridStoreConfigRecord;
      }

      setStore.accessControlled = accessControlled.isPresent() ? accessControlled.get() : store.isAccessControlled();
      setStore.compressionStrategy = compressionStrategy.isPresent()
          ? compressionStrategy.get().getValue() : store.getCompressionStrategy().getValue();
      setStore.chunkingEnabled = chunkingEnabled.isPresent() ? chunkingEnabled.get() : store.isChunkingEnabled();
      setStore.singleGetRouterCacheEnabled = singleGetRouterCacheEnabled.isPresent() ? singleGetRouterCacheEnabled.get() : store.isSingleGetRouterCacheEnabled();
      setStore.batchGetRouterCacheEnabled =
          batchGetRouterCacheEnabled.isPresent() ? batchGetRouterCacheEnabled.get() : store.isBatchGetRouterCacheEnabled();

      veniceHelixAdmin.checkWhetherStoreWillHaveConflictConfigForCaching(store, incrementalPushEnabled,
          null == hybridStoreConfig ? Optional.empty() : Optional.of(hybridStoreConfig),
          singleGetRouterCacheEnabled, batchGetRouterCacheEnabled);
      setStore.batchGetLimit = batchGetLimit.isPresent() ? batchGetLimit.get() : store.getBatchGetLimit();
      setStore.numVersionsToPreserve =
          numVersionsToPreserve.isPresent() ? numVersionsToPreserve.get() : store.getNumVersionsToPreserve();
      setStore.incrementalPushEnabled =
          incrementalPushEnabled.isPresent() ? incrementalPushEnabled.get() : store.isIncrementalPushEnabled();
      setStore.isMigrating = storeMigration.isPresent() ? storeMigration.get() : store.isMigrating();
      setStore.writeComputationEnabled = writeComputationEnabled.isPresent() ? writeComputationEnabled.get() : store.isWriteComputationEnabled();
      setStore.readComputationEnabled = readComputationEnabled.isPresent() ? readComputationEnabled.get() : store.isReadComputationEnabled();
      setStore.bootstrapToOnlineTimeoutInHours = bootstrapToOnlineTimeoutInHours.isPresent() ?
          bootstrapToOnlineTimeoutInHours.get() : store.getBootstrapToOnlineTimeoutInHours();
      setStore.leaderFollowerModelEnabled = leaderFollowerModelEnabled.isPresent() ? leaderFollowerModelEnabled.get() : store.isLeaderFollowerModelEnabled();
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.UPDATE_STORE.getValue();
      message.payloadUnion = setStore;
      sendAdminMessageAndWaitForConsumed(clusterName, message);
    } finally {
      releaseLock();
    }
  }

  @Override
  public double getStorageEngineOverheadRatio(String clusterName) {
    return veniceHelixAdmin.getStorageEngineOverheadRatio(clusterName);
  }

  @Override
  public SchemaEntry getKeySchema(String clusterName, String storeName) {
    return veniceHelixAdmin.getKeySchema(clusterName, storeName);
  }

  @Override
  public Collection<SchemaEntry> getValueSchemas(String clusterName, String storeName) {
    return veniceHelixAdmin.getValueSchemas(clusterName, storeName);
  }

  @Override
  public int getValueSchemaId(String clusterName, String storeName, String valueSchemaStr) {
    return veniceHelixAdmin.getValueSchemaId(clusterName, storeName, valueSchemaStr);
  }

  @Override
  public SchemaEntry getValueSchema(String clusterName, String storeName, int id) {
    return veniceHelixAdmin.getValueSchema(clusterName, storeName, id);
  }

  @Override
  public SchemaEntry addValueSchema(String clusterName, String storeName, String valueSchemaStr, DirectionalSchemaCompatibilityType expectedCompatibilityType) {
    acquireLock(clusterName);
    try {
      int newValueSchemaId = veniceHelixAdmin.checkPreConditionForAddValueSchemaAndGetNewSchemaId(
          clusterName, storeName, valueSchemaStr, expectedCompatibilityType);
      logger.info("Adding value schema: " + valueSchemaStr + " to store: " + storeName + " in cluster: " + clusterName);

      ValueSchemaCreation valueSchemaCreation = (ValueSchemaCreation) AdminMessageType.VALUE_SCHEMA_CREATION.getNewInstance();
      valueSchemaCreation.clusterName = clusterName;
      valueSchemaCreation.storeName = storeName;
      SchemaMeta schemaMeta = new SchemaMeta();
      schemaMeta.definition = valueSchemaStr;
      schemaMeta.schemaType = SchemaType.AVRO_1_4.getValue();
      valueSchemaCreation.schema = schemaMeta;
      valueSchemaCreation.schemaId = newValueSchemaId;

      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.VALUE_SCHEMA_CREATION.getValue();
      message.payloadUnion = valueSchemaCreation;

      sendAdminMessageAndWaitForConsumed(clusterName, message);

      int actualValueSchemaId = getValueSchemaId(clusterName, storeName, valueSchemaStr);
      if (actualValueSchemaId != newValueSchemaId) {
        throw new VeniceException("Something bad happens, the expected new value schema id is: " + newValueSchemaId + ", but got: " + actualValueSchemaId);
      }

      return new SchemaEntry(actualValueSchemaId, valueSchemaStr);
    } finally {
      releaseLock();
    }
  }

  @Override
  public SchemaEntry addValueSchema(String clusterName, String storeName, String valueSchemaStr, int schemaId, DirectionalSchemaCompatibilityType expectedCompatibilityType) {
    throw new VeniceUnsupportedOperationException("addValueSchema");
  }

  @Override
  public List<String> getStorageNodes(String clusterName) {
    throw new VeniceUnsupportedOperationException("getStorageNodes");
  }

  @Override
  public Map<String, String> getStorageNodesStatus(String clusterName) {
    throw new VeniceUnsupportedOperationException("getStorageNodesStatus");
  }

  @Override
  public void removeStorageNode(String clusterName, String instanceId) {
    throw new VeniceUnsupportedOperationException("removeStorageNode");
  }

  private Map<String, ControllerClient> getControllerClientMap(String clusterName){
    Map<String, ControllerClient> controllerClients = new HashMap<>();
    VeniceControllerConfig veniceControllerConfig = multiClusterConfigs.getConfigForCluster(clusterName);
    veniceControllerConfig.getChildClusterMap().entrySet().
      forEach(entry -> controllerClients.put(entry.getKey(), new ControllerClient(clusterName, entry.getValue())));
    veniceControllerConfig.getChildClusterD2Map().entrySet().
      forEach(entry -> controllerClients.put(entry.getKey(),
          new D2ControllerClient(veniceControllerConfig.getD2ServiceName(), clusterName, entry.getValue())));

    return controllerClients;
  }

  /**
   * Queries child clusters for status.
   * Of all responses, return highest of (in order) NOT_CREATED, NEW, STARTED, PROGRESS.
   * If all responses are COMPLETED, returns COMPLETED.
   * If any response is ERROR and all responses are terminal (COMPLETED or ERROR), returns ERROR
   * If any response is ERROR and any response is not terminal, returns PROGRESS
   * ARCHIVED is treated as NOT_CREATED
   *
   * If error in querying half or more of clusters, returns PROGRESS. (so that polling will continue)
   *
   * @param clusterName
   * @param kafkaTopic
   * @return
   */
  @Override
  public OfflinePushStatusInfo getOffLinePushStatus(String clusterName, String kafkaTopic) {
    Map<String, ControllerClient> controllerClients = getControllerClientMap(clusterName);
    return getOffLineJobStatus(clusterName, kafkaTopic, controllerClients, getTopicManager());
  }

  @Override
  public OfflinePushStatusInfo getOffLinePushStatus(String clusterName, String kafkaTopic, Optional<String> incrementalPushVersion) {
    Map<String, ControllerClient> controllerClients = getControllerClientMap(clusterName);
    return getOffLineJobStatus(clusterName, kafkaTopic, controllerClients, getTopicManager(), Optional.empty());
  }

  protected OfflinePushStatusInfo getOffLineJobStatus(String clusterName, String kafkaTopic,
    Map<String, ControllerClient> controllerClients, TopicManager topicManager) {
    return getOffLineJobStatus(clusterName, kafkaTopic, controllerClients, topicManager, Optional.empty());
  }

  protected OfflinePushStatusInfo getOffLineJobStatus(String clusterName, String kafkaTopic,
      Map<String, ControllerClient> controllerClients, TopicManager topicManager, Optional<String> incrementalPushVersion) {
    Set<String> childClusters = controllerClients.keySet();
    ExecutionStatus currentReturnStatus = ExecutionStatus.NEW; // This status is not used for anything... Might make sense to remove it, but anyhow.
    Optional<String> currentReturnStatusDetails = Optional.empty();
    List<ExecutionStatus> statuses = new ArrayList<>();
    Map<String, String> extraInfo = new HashMap<>();
    Map<String, String> extraDetails = new HashMap<>();
    int failCount = 0;
    for (String cluster : childClusters){
      ControllerClient controllerClient = controllerClients.get(cluster);
      String masterControllerUrl = controllerClient.getMasterControllerUrl();
      JobStatusQueryResponse response = ControllerClient.retryableRequest(controllerClient, 2, // TODO: Make retry count configurable
          c -> c.queryJobStatus(kafkaTopic, incrementalPushVersion));
      if (response.isError()){
        failCount += 1;
        logger.warn("Couldn't query " + cluster + " for job " + kafkaTopic + " status: " + response.getError());
        extraInfo.put(cluster, ExecutionStatus.UNKNOWN.toString());
        extraDetails.put(cluster, masterControllerUrl + " " + response.getError());
      } else {
        ExecutionStatus status = ExecutionStatus.valueOf(response.getStatus());
        statuses.add(status);
        extraInfo.put(cluster, response.getStatus());
        Optional<String> statusDetails = response.getOptionalStatusDetails();
        if (statusDetails.isPresent()) {
          extraDetails.put(cluster, masterControllerUrl + " " + statusDetails.get());
        }
      }
    }

    /**
     * TODO: remove guava library dependency since it could cause a lot of indirect dependency conflicts.
     */
    // Sort the per-datacenter status in this order, and return the first one in the list
    // Edge case example: if one cluster is stuck in NOT_CREATED, then
    //   as another cluster goes from PROGRESS to COMPLETED
    //   the aggregate status will go from PROGRESS back down to NOT_CREATED.
    List<ExecutionStatus> priorityOrderList = Arrays.asList(
        ExecutionStatus.PROGRESS,
        ExecutionStatus.STARTED,
        ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED,
        ExecutionStatus.NEW,
        ExecutionStatus.NOT_CREATED,
        ExecutionStatus.END_OF_PUSH_RECEIVED,
        ExecutionStatus.ERROR,
        ExecutionStatus.WARNING,
        ExecutionStatus.COMPLETED,
        ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED,
        ExecutionStatus.ARCHIVED);
    Collections.sort(statuses, Comparator.comparingInt(priorityOrderList::indexOf));
    if (statuses.size() > 0){
      currentReturnStatus = statuses.get(0);
    }

    int successCount = childClusters.size() - failCount;
    if (! (successCount >= (childClusters.size()/2)+1)) { // Strict majority must be reachable, otherwise keep polling
      currentReturnStatus = ExecutionStatus.PROGRESS;
    }

    if (currentReturnStatus.isTerminal()) {
      // If there is a temporary datacenter connection failure, we want H2V to report failure while allowing the push
      // to succeed in remaining datacenters.  If we want to allow the push to succeed in asyc in the remaining datacenter
      // then put the topic delete into an else block under `if (failcount > 0)`
      if (failCount > 0){
        currentReturnStatus = ExecutionStatus.ERROR;
        currentReturnStatusDetails = Optional.of(failCount + "/" + childClusters.size() + " DCs unreachable. ");
      }

      // TODO: Set parent controller's version status based on currentReturnStatus
      // COMPLETED -> ONLINE
      // ERROR -> ERROR
      //TODO: remove this if statement since it was only for debugging purpose
      if (maxErroredTopicNumToKeep > 0 && currentReturnStatus.equals(ExecutionStatus.ERROR)) {
        currentReturnStatusDetails = Optional.of(currentReturnStatusDetails.orElse("") + "Parent Kafka topic won't be truncated");
        logger.info("The errored kafka topic: " + kafkaTopic + " won't be truncated since it will be used to investigate"
            + "some Kafka related issue");
      } else {
        //truncate the topic if this is not an incremental push enabled store or this is a failed batch push
        if ((!incrementalPushVersion.isPresent() && currentReturnStatus == ExecutionStatus.ERROR) ||
            !veniceHelixAdmin.getVeniceHelixResource(clusterName).
            getMetadataRepository().
            getStore(Version.parseStoreFromKafkaTopicName(kafkaTopic)).isIncrementalPushEnabled()) {
            logger.info("Truncating kafka topic: " + kafkaTopic + " with job status: " + currentReturnStatus);
            truncateKafkaTopic(kafkaTopic);
            currentReturnStatusDetails = Optional.of(currentReturnStatusDetails.orElse("") + "Parent Kafka topic truncated");
          }
        }
    }

    return new OfflinePushStatusInfo(currentReturnStatus, extraInfo, currentReturnStatusDetails, extraDetails);
  }

  /**
   * Queries child clusters for job progress.  Prepends the cluster name to the task ID and provides an aggregate
   * Map of progress for all tasks.
   * @param clusterName
   * @param kafkaTopic
   * @return
   */
  @Override
  public Map<String, Long> getOfflinePushProgress(String clusterName, String kafkaTopic){
    Map<String, ControllerClient> controllerClients = getControllerClientMap(clusterName);
    return getOfflineJobProgress(clusterName, kafkaTopic, controllerClients);
  }

  protected static Map<String, Long> getOfflineJobProgress(String clusterName, String kafkaTopic, Map<String, ControllerClient> controllerClients){
    Map<String, Long> aggregateProgress = new HashMap<>();
    for (Map.Entry<String, ControllerClient> clientEntry : controllerClients.entrySet()){
      String childCluster = clientEntry.getKey();
      ControllerClient client = clientEntry.getValue();
      JobStatusQueryResponse statusResponse = client.queryJobStatus(kafkaTopic);
      if (statusResponse.isError()){
        logger.warn("Failed to query " + childCluster + " for job progress on topic " + kafkaTopic + ".  " + statusResponse.getError());
      } else {
        Map<String, Long> clusterProgress = statusResponse.getPerTaskProgress();
        for (String task : clusterProgress.keySet()){
          aggregateProgress.put(childCluster + "_" + task, clusterProgress.get(task));
        }
      }
    }
    return aggregateProgress;
  }

  @Override
  public String getKafkaBootstrapServers(boolean isSSL) {
    return veniceHelixAdmin.getKafkaBootstrapServers(isSSL);
  }

  @Override
  public boolean isSSLEnabledForPush(String clusterName, String storeName) {
    return veniceHelixAdmin.isSSLEnabledForPush(clusterName, storeName);
  }

  @Override
  public boolean isSslToKafka() {
    return veniceHelixAdmin.isSslToKafka();
  }

  @Override
  public TopicManager getTopicManager() {
    return veniceHelixAdmin.getTopicManager();
  }

  @Override
  public boolean isMasterController(String clusterName) {
    return veniceHelixAdmin.isMasterController(clusterName);
  }

  @Override
  public int calculateNumberOfPartitions(String clusterName, String storeName, long storeSize) {
    return veniceHelixAdmin.calculateNumberOfPartitions(clusterName, storeName, storeSize);
  }

  @Override
  public int getReplicationFactor(String clusterName, String storeName) {
    return veniceHelixAdmin.getReplicationFactor(clusterName, storeName);
  }

  @Override
  public int getDatacenterCount(String clusterName){
    return multiClusterConfigs.getConfigForCluster(clusterName).getChildClusterMap().size();
  }

  @Override
  public List<Replica> getBootstrapReplicas(String clusterName, String kafkaTopic) {
    throw new VeniceException("getBootstrapReplicas is not supported!");
  }

  @Override
  public List<Replica> getErrorReplicas(String clusterName, String kafkaTopic) {
    throw new VeniceException("getErrorReplicas is not supported!");
  }

  @Override
  public List<Replica> getReplicas(String clusterName, String kafkaTopic) {
    throw new VeniceException("getReplicas is not supported!");
  }

  @Override
  public List<Replica> getReplicasOfStorageNode(String clusterName, String instanceId) {
    throw new VeniceException("getReplicasOfStorageNode is not supported!");
  }

  @Override
  public NodeRemovableResult isInstanceRemovable(String clusterName, String instanceId, boolean isFromInstanceView) {
    throw new VeniceException("isInstanceRemovable is not supported!");
  }

  @Override
  public NodeRemovableResult isInstanceRemovable(String clusterName, String helixNodeId, int minActiveReplicas, boolean isInstanceView) {
    throw new VeniceException("isInstanceRemovable is not supported!");
  }

  @Override
  public Instance getMasterController(String clusterName) {
    return veniceHelixAdmin.getMasterController(clusterName);
  }

  @Override
  public void addInstanceToWhitelist(String clusterName, String helixNodeId) {
    throw new VeniceException("addInstanceToWhitelist is not supported!");
  }

  @Override
  public void removeInstanceFromWhiteList(String clusterName, String helixNodeId) {
    throw new VeniceException("removeInstanceFromWhiteList is not supported!");
  }

  @Override
  public Set<String> getWhitelist(String clusterName) {
    throw new VeniceException("getWhitelist is not supported!");
  }

  @Override
  public void killOfflinePush(String clusterName, String kafkaTopic) {
    acquireLock(clusterName);
    try {
      veniceHelixAdmin.checkPreConditionForKillOfflinePush(clusterName, kafkaTopic);
      logger.info("Killing offline push job for topic: " + kafkaTopic + " in cluster: " + clusterName);
      /**
       * When parent controller wants to keep some errored topics, this function won't remove topic,
       * but relying on the next push to clean up this topic if it hasn't been removed by {@link #getOffLineJobStatus}.
       *
       * The reason is that every errored push will call this function.
       */
      if (0 == maxErroredTopicNumToKeep) {
        // Truncate Kafka topic
        logger.info("Truncating topic when kill offline push job, topic: " + kafkaTopic);
        truncateKafkaTopic(kafkaTopic);
      }

      // TODO: Set parent controller's version status (to ERROR, most likely?)

      KillOfflinePushJob killJob = (KillOfflinePushJob) AdminMessageType.KILL_OFFLINE_PUSH_JOB.getNewInstance();
      killJob.clusterName = clusterName;
      killJob.kafkaTopic = kafkaTopic;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.KILL_OFFLINE_PUSH_JOB.getValue();
      message.payloadUnion = killJob;

      sendAdminMessageAndWaitForConsumed(clusterName, message);
    } finally {
      releaseLock();
    }
  }

  @Override
  public StorageNodeStatus getStorageNodesStatus(String clusterName, String instanceId) {
    throw new VeniceUnsupportedOperationException("getStorageNodesStatus");
  }

  @Override
  public boolean isStorageNodeNewerOrEqualTo(String clusterName, String instanceId,
                                             StorageNodeStatus oldServerStatus) {
    throw new VeniceUnsupportedOperationException("isStorageNodeNewerOrEqualTo");
  }

  @Override
  public void setDelayedRebalanceTime(String clusterName, long delayedTime) {
    throw new VeniceUnsupportedOperationException("setDelayedRebalanceTime");
  }

  @Override
  public long getDelayedRebalanceTime(String clusterName) {
    throw new VeniceUnsupportedOperationException("getDelayedRebalanceTime");
  }

  public void setAdminConsumerService(String clusterName, AdminConsumerService service){
    veniceHelixAdmin.setAdminConsumerService(clusterName, service);
  }

  @Override
  public void skipAdminMessage(String clusterName, long offset, boolean skipDIV){
    veniceHelixAdmin.skipAdminMessage(clusterName, offset, skipDIV);
  }

  @Override
  public long getLastSucceedExecutionId(String clustername) {
    return veniceHelixAdmin.getLastSucceedExecutionId(clustername);
  }

  @Override
  public void setLastException(String clusterName, Exception e) {

  }

  protected Time getTimer() {
    return timer;
  }

  protected void setTimer(Time timer) {
    this.timer = timer;
  }

  @Override
  public Exception getLastException(String clusterName) {
    return null;
  }

  @Override
  public Optional<AdminCommandExecutionTracker> getAdminCommandExecutionTracker(String clusterName) {
    if(adminCommandExecutionTrackers.containsKey(clusterName)){
      return Optional.of(adminCommandExecutionTrackers.get(clusterName));
    }else{
      return Optional.empty();
    }
  }

  @Override
  public RoutersClusterConfig getRoutersClusterConfig(String clusterName) {
    throw new VeniceUnsupportedOperationException("getRoutersClusterConfig");
  }

  @Override
  public void updateRoutersClusterConfig(String clusterName, Optional<Boolean> isThrottlingEnable,
      Optional<Boolean> isQuotaRebalancedEnable, Optional<Boolean> isMaxCapaictyProtectionEnabled,
      Optional<Integer> expectedRouterCount) {
    throw new VeniceUnsupportedOperationException("updateRoutersClusterConfig");
  }

  @Override
  public Map<String, String> getAllStorePushStrategyForMigration() {
    return pushStrategyZKAccessor.getAllPushStrategies();
  }

  @Override
  public void setStorePushStrategyForMigration(String voldemortStoreName, String strategy) {
    pushStrategyZKAccessor.setPushStrategy(voldemortStoreName, strategy);
  }

  @Override
  public List<String> getClusterOfStoreInMasterController(String storeName) {
    return veniceHelixAdmin.getClusterOfStoreInMasterController(storeName);
  }

  @Override
  public Pair<String, String> discoverCluster(String storeName) {
    return veniceHelixAdmin.discoverCluster(storeName);
  }

  @Override
  public Map<String, String> findAllBootstrappingVersions(String clusterName) {
    throw new VeniceUnsupportedOperationException("findAllBootstrappingVersions");
  }

  public VeniceWriterFactory getVeniceWriterFactory() {
    return veniceHelixAdmin.getVeniceWriterFactory();
  }

  @Override
  public VeniceControllerConsumerFactory getVeniceConsumerFactory() {
    return veniceHelixAdmin.getVeniceConsumerFactory();
  }

  @Override
  public synchronized void stop(String clusterName) {
    veniceHelixAdmin.stop(clusterName);
    // Close the admin producer for this cluster
    VeniceWriter<byte[], byte[]> veniceWriter = veniceWriterMap.get(clusterName);
    if (null != veniceWriter) {
      veniceWriter.close();
    }
    asyncSetupEnabledMap.put(clusterName, false);
  }

  @Override
  public void stopVeniceController() {
    veniceHelixAdmin.stopVeniceController();
  }

  @Override
  public synchronized void close() {
    veniceWriterMap.keySet().forEach(this::stop);
    veniceHelixAdmin.close();
  }

  @Override
  public boolean isMasterControllerOfControllerCluster() {
    return veniceHelixAdmin.isMasterControllerOfControllerCluster();
  }

  @Override
  public boolean isTopicTruncated(String kafkaTopicName) {
    return veniceHelixAdmin.isTopicTruncated(kafkaTopicName);
  }

  @Override
  public boolean isTopicTruncatedBasedOnRetention(long retention) {
    return veniceHelixAdmin.isTopicTruncatedBasedOnRetention(retention);
  }

  public void truncateKafkaTopic(String kafkaTopicName) {
    veniceHelixAdmin.truncateKafkaTopic(kafkaTopicName);
  }

  @Override
  public void updatePushProperties(String cluster, String storeName, int version, Map<String, String> properties) {
    veniceHelixAdmin.checkControllerMastership(cluster);
    String topicName = Version.composeKafkaTopic(storeName, version);
    OfflinePushStatus status = offlinePushAccessor.getOfflinePushStatus(cluster, topicName);
    status.setPushProperties(properties);
    offlinePushAccessor.updateOfflinePushStatus(cluster, status);
  }

  @Override
  public boolean isResourceStillAlive(String resourceName) {
    throw new VeniceException("VeniceParentHelixAdmin#isResourceStillAlive is not supported!");
  }

  public ParentHelixOfflinePushAccessor getOfflinePushAccessor() {
    return offlinePushAccessor;
  }

  /* Used by test only*/
  protected void setOfflinePushAccessor(ParentHelixOfflinePushAccessor offlinePushAccessor) {
    this.offlinePushAccessor = offlinePushAccessor;
  }

  @Override
  public void updateClusterDiscovery(String storeName, String oldCluster, String newCluster) {
    veniceHelixAdmin.updateClusterDiscovery(storeName, oldCluster, newCluster);
  }

  public void sendPushJobStatusMessage(PushJobStatusRecordKey key, PushJobStatusRecordValue value) {
    veniceHelixAdmin.sendPushJobStatusMessage(key, value);
  }

  public void writeEndOfPush(String clusterName, String storeName, int versionNumber, boolean alsoWriteStartOfPush) {
    veniceHelixAdmin.writeEndOfPush(clusterName, storeName, versionNumber, alsoWriteStartOfPush);
  }

  public void migrateStore(String srcClusterName, String destClusterName, String storeName) {
    if (srcClusterName.equals(destClusterName)) {
      throw new VeniceException("Source cluster and destination cluster cannot be the same!");
    }

    MigrateStore migrateStore = (MigrateStore) AdminMessageType.MIGRATE_STORE.getNewInstance();
    migrateStore.srcClusterName = srcClusterName;
    migrateStore.destClusterName = destClusterName;
    migrateStore.storeName = storeName;

    // Set src store migration flag
    String srcControllerUrl = this.getMasterController(srcClusterName).getUrl(false);
    ControllerClient srcControllerClient = new ControllerClient(srcClusterName, srcControllerUrl);
    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setStoreMigration(true);
    srcControllerClient.updateStore(storeName, params);

    // Update migration src and dest cluster in storeConfig
    veniceHelixAdmin.setStoreConfigForMigration(storeName, srcClusterName, destClusterName);

    // Trigger store migration operation
    AdminOperation message = new AdminOperation();
    message.operationType = AdminMessageType.MIGRATE_STORE.getValue();
    message.payloadUnion = migrateStore;
    sendAdminMessageAndWaitForConsumed(destClusterName, message);
  }

  @Override
  public void abortMigration(String srcClusterName, String destClusterName, String storeName) {
    if (srcClusterName.equals(destClusterName)) {
      throw new VeniceException("Source cluster and destination cluster cannot be the same!");
    }

    AbortMigration abortMigration = (AbortMigration) AdminMessageType.ABORT_MIGRATION.getNewInstance();
    abortMigration.srcClusterName = srcClusterName;
    abortMigration.destClusterName = destClusterName;
    abortMigration.storeName = storeName;

    // Trigger store migration operation
    AdminOperation message = new AdminOperation();
    message.operationType = AdminMessageType.ABORT_MIGRATION.getValue();
    message.payloadUnion = abortMigration;
    sendAdminMessageAndWaitForConsumed(srcClusterName, message);
  }

  public List<String> getChildControllerUrls(String clusterName) {
    return new ArrayList<>(multiClusterConfigs.getConfigForCluster(clusterName).getChildClusterMap().values());
  }

  /**
   * This thread will run in the background and update cluster discovery information when necessary
   */
  private void startStoreMigrationMonitor() {
    Thread thread = new Thread(() -> {
      Map<String, ControllerClient> srcControllerClients = new HashMap<>();
      Map<String, List<ControllerClient>> childControllerClientsMap = new HashMap<>();

      while (true) {
        try {
          Utils.sleep(10000);

          // Get a list of clusters that this controller is responsible for
          List<String> activeClusters = this.multiClusterConfigs.getClusters()
              .stream()
              .filter(cluster -> this.isMasterController(cluster))
              .collect(Collectors.toList());

          // Get a list of stores from storeConfig that are migrating
          List<StoreConfig> allStoreConfigs = veniceHelixAdmin.getStoreConfigRepo().getAllStoreConfigs();
          List<String> migratingStores = allStoreConfigs.stream()
              .filter(storeConfig -> storeConfig.getMigrationSrcCluster() != null)  // Store might be migrating
              .filter(storeConfig -> storeConfig.getMigrationSrcCluster().equals(storeConfig.getCluster())) // Migration not complete
              .filter(storeConfig -> storeConfig.getMigrationDestCluster() != null)
              .filter(storeConfig -> activeClusters.contains(storeConfig.getMigrationDestCluster())) // This controller is eligible for this store
              .map(storeConfig -> storeConfig.getStoreName())
              .collect(Collectors.toList());

          // For each migrating stores, check if store migration is complete.
          // If so, update cluster discovery according to storeConfig
          for (String storeName : migratingStores) {
            StoreConfig storeConfig = veniceHelixAdmin.getStoreConfigRepo().getStoreConfig(storeName).get();
            String srcClusterName = storeConfig.getMigrationSrcCluster();
            String destClusterName = storeConfig.getMigrationDestCluster();
            String clusterDiscovered = storeConfig.getCluster();

            if (clusterDiscovered.equals(destClusterName)) {
              // Migration complete already
              continue;
            }

            List<ControllerClient> childControllerClients = childControllerClientsMap.computeIfAbsent(srcClusterName,
                sCN -> {
                  List<ControllerClient> child_controller_clients = new ArrayList<>();

                  for (String childControllerUrl : this.getChildControllerUrls(sCN)) {
                    ControllerClient childControllerClient = new ControllerClient(sCN, childControllerUrl);
                    child_controller_clients.add(childControllerClient);
                  }

                  return child_controller_clients;
                });


            // Check if latest version is online in each child cluster
            int readyCount = 0;

            for (ControllerClient childController : childControllerClients) {
              String childClusterDiscovered = childController.discoverCluster(storeName).getCluster();
              if (childClusterDiscovered.equals(destClusterName)) {
                // CLuster discovery information has been updated,
                // which means store migration in this particular cluster is successful
                readyCount++;
              }
            }

            if (readyCount == childControllerClients.size()) {
              // All child clusters have completed store migration
              // Finish off store migration in the parent cluster by creating a clone store

              // Get original store properties
              ControllerClient srcControllerClient = srcControllerClients.computeIfAbsent(srcClusterName,
                  src_cluster_name -> new ControllerClient(src_cluster_name,
                      this.getMasterController(src_cluster_name).getUrl(false)));
              StoreInfo srcStore = srcControllerClient.getStore(storeName).getStore();
              String srcKeySchema = srcControllerClient.getKeySchema(storeName).getSchemaStr();
              MultiSchemaResponse.Schema[] srcValueSchemasResponse =
                  srcControllerClient.getAllValueSchema(storeName).getSchemas();

              // Finally finish off store migration in parent controller
              veniceHelixAdmin.cloneStore(srcClusterName, destClusterName, srcStore, srcKeySchema,
                  srcValueSchemasResponse);

              logger.info("All child clusters have " + storeName + " cloned store ready in " + destClusterName
                  + ". Will update cluster discovery in parent.");
              veniceHelixAdmin.updateClusterDiscovery(storeName, srcClusterName, destClusterName);
              continue;
            }
          }
        } catch (Exception e) {
          logger.error("Caught exception in store migration monitor", e);
        }
      }
    });

    thread.start();
  }
}
