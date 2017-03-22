package com.linkedin.venice.controller;

import com.google.common.collect.Ordering;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.offsets.AdminOffsetManager;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;

import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteAllVersions;
import com.linkedin.venice.controller.kafka.protocol.admin.DisableStoreRead;
import com.linkedin.venice.controller.kafka.protocol.admin.EnableStoreRead;
import com.linkedin.venice.controller.kafka.protocol.admin.KillOfflinePushJob;
import com.linkedin.venice.controller.kafka.protocol.admin.PauseStore;
import com.linkedin.venice.controller.kafka.protocol.admin.ResumeStore;
import com.linkedin.venice.controller.kafka.protocol.admin.SchemaMeta;
import com.linkedin.venice.controller.kafka.protocol.admin.SetStoreCurrentVersion;
import com.linkedin.venice.controller.kafka.protocol.admin.SetStoreOwner;
import com.linkedin.venice.controller.kafka.protocol.admin.SetStorePartitionCount;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.ValueSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.enums.SchemaType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controllerapi.AdminCommandExecution;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.HelixReadWriteStoreRepository;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import java.util.Optional;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
  private static final long SLEEP_INTERVAL_FOR_DATA_CONSUMPTION_IN_MS = 1000;
  private static final Logger logger = Logger.getLogger(VeniceParentHelixAdmin.class);
  //Store version number to retain in Parent Controller to limit 'Store' ZNode size.
  protected static final int STORE_VERSION_RETENTION_COUNT = 5;

  private final VeniceHelixAdmin veniceHelixAdmin;
  private final Map<String, VeniceWriter<byte[], byte[]>> veniceWriterMap;
  private final OffsetManager offsetManager;
  private final byte[] emptyKeyByteArr = new byte[0];
  private final AdminOperationSerializer adminOperationSerializer = new AdminOperationSerializer();
  private final VeniceControllerConfig veniceControllerConfig;
  private final Lock lock = new ReentrantLock();
  private final AdminCommandExecutionTracker adminCommandExecutionTracker;
  /**
   * Variable to store offset of last message.
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
  private long lastOffset = -1;

  private final int waitingTimeForConsumptionMs;

  public VeniceParentHelixAdmin(VeniceHelixAdmin veniceHelixAdmin, VeniceControllerConfig config) {
    this.veniceHelixAdmin = veniceHelixAdmin;
    this.veniceControllerConfig = config;
    this.waitingTimeForConsumptionMs = config.getParentControllerWaitingTimeForConsumptionMs();
    this.veniceWriterMap = new ConcurrentHashMap<>();
    this.offsetManager = new AdminOffsetManager(this.veniceHelixAdmin.getZkClient(), this.veniceHelixAdmin.getAdapterSerializer());
    this.adminCommandExecutionTracker =
        new AdminCommandExecutionTracker(config.getClusterName(), veniceHelixAdmin.getExecutionIdAccessor(),
            getControllerClientMap(config.getClusterName()));
  }

  public void setVeniceWriterForCluster(String clusterName, VeniceWriter writer) {
    veniceWriterMap.putIfAbsent(clusterName, writer);
  }

  @Override
  public synchronized void start(String clusterName) {
    veniceHelixAdmin.start(clusterName);

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
      topicManager.createTopic(topicName, AdminTopicUtils.PARTITION_NUM_FOR_ADMIN_TOPIC, veniceControllerConfig.getKafkaReplicaFactor());
      logger.info("Created admin topic: " + topicName + " for cluster: " + clusterName);
    }

    // Initialize producer
    veniceWriterMap.computeIfAbsent(clusterName, (key) -> {
      // Initialize VeniceWriter (Kafka producer)
      Properties props = new Properties();
      props.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, veniceControllerConfig.getKafkaBootstrapServers());
      /**
       * No need to do checksum validation since Kafka will do message-level checksum validation by default.
       * Venice just needs to check seq id in {@link com.linkedin.venice.controller.kafka.consumer.AdminConsumptionTask} to catch the following scenarios:
       * 1. Data missing;
       * 2. Data out of order;
       * 3. Data duplication;
       */
      props.put(VeniceWriter.CHECK_SUM_TYPE, CheckSumType.NONE.name());
      VeniceProperties veniceWriterProperties = new VeniceProperties(props);
      return new VeniceWriter<>(veniceWriterProperties, topicName, new DefaultSerializer(), new DefaultSerializer());
    });
  }

  @Override
  public boolean isClusterValid(String clusterName) {
    return veniceHelixAdmin.isClusterValid(clusterName);
  }

  private void sendAdminMessageAndWaitForConsumed(String clusterName, AdminOperation message) {
    if (!veniceWriterMap.containsKey(clusterName)) {
      throw new VeniceException("Cluster: " + clusterName + " is not started yet!");
    }
    AdminCommandExecution execution =
        adminCommandExecutionTracker.createExecution(AdminMessageType.valueOf(message).name());
    message.executionId = execution.getExecutionId();
    VeniceWriter<byte[], byte[]> veniceWriter = veniceWriterMap.get(clusterName);
    byte[] serializedValue = adminOperationSerializer.serialize(message);
    try {
      Future<RecordMetadata> future = veniceWriter.put(emptyKeyByteArr, serializedValue, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
      RecordMetadata meta = future.get();

      lastOffset = meta.offset();
      logger.info("Sent message: " + message + " to kafka, offset: " + lastOffset);
      waitingLastOffsetToBeConsumed(clusterName);
      adminCommandExecutionTracker.startTrackingExecution(execution);
    } catch (Exception e) {
      throw new VeniceException("Got exception during sending message to Kafka", e);
    }
  }

  private void waitingLastOffsetToBeConsumed(String clusterName) {
    String topicName = AdminTopicUtils.getTopicNameFromClusterName(clusterName);

    // Blocking until consumer consumes the new message or timeout
    long startTime = SystemTime.INSTANCE.getMilliseconds();
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
        if (getAdminCommandExecutionTracker().isPresent()){
          errMsg += "  RunningExecutions: " + getAdminCommandExecutionTracker().get().executionsAsString();
        }
        throw new VeniceException(errMsg, lastException);
      }

      logger.info("Waiting util consumed " + lastOffset + ", currently at " + consumedOffset);
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
      veniceHelixAdmin.checkPreConditionForAddStore(clusterName, storeName, owner, keySchema, valueSchema);
      logger.info("Adding store: " + storeName + " to cluster: " + clusterName);

      // Write store creation message to Kafka
      StoreCreation storeCreation = (StoreCreation) AdminMessageType.STORE_CREATION.getNewInstance();
      storeCreation.clusterName = clusterName;
      storeCreation.storeName = storeName;
      storeCreation.owner = owner;
      storeCreation.keySchema = new SchemaMeta();
      storeCreation.keySchema.schemaType = SchemaType.AVRO_1_4.ordinal();
      storeCreation.keySchema.definition = keySchema;
      storeCreation.valueSchema = new SchemaMeta();
      storeCreation.valueSchema.schemaType = SchemaType.AVRO_1_4.ordinal();
      storeCreation.valueSchema.definition = valueSchema;

      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.STORE_CREATION.ordinal();
      message.payloadUnion = storeCreation;

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
    throw new VeniceException("addVersion is not supported yet!");
  }

  /**
   * Since there is no {@link com.linkedin.venice.job.OfflineJob} running in Parent Controller,
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

  @Override
  public Version incrementVersion(String clusterName,
                                  String storeName,
                                  int numberOfPartition,
                                  int replicationFactor) {
    // TODO: consider to move version creation to admin protocol
    // Right now, TopicMonitor in each prod colo will monitor new Kafka topic and
    // create new corresponding store versions
    // Adding version in Parent Controller won't start offline push job.

    /**
     * Check whether any topic for this store exists or not.
     * The existing topic could be introduced by two cases:
     * 1. The previous job push is still running;
     * 2. The previous job push fails to delete this topic;
     *
     * For the 1st case, it is expected to refuse the new data push,
     * and for the 2nd case, customer should reach out Venice team to fix this issue for now.
     **/
    TopicManager topicManager = getTopicManager();
    Set<String> topics = topicManager.listTopics();
    String storeNameForCurrentTopic;
    for (String topic: topics) {
      if (AdminTopicUtils.isAdminTopic(topic)) {
        continue;
      }
      try {
        storeNameForCurrentTopic = Version.parseStoreFromKafkaTopicName(topic);
      } catch (Exception e) {
        logger.warn("Failed to parse StoreName from topic: " + topic, e);
        continue;
      }
      if (storeNameForCurrentTopic.equals(storeName)) {
        /**
         * Check the offline job status for this topic, and if it has already been terminated, we will skip it.
         * Otherwise, an exception will be thrown to stop concurrent data pushes.
         *
         * {@link #getOffLinePushStatus(String, String)} will remove the topic if the offline job has been terminated,
         * so we don't need to explicitly remove it here.
          */
        OfflinePushStatusInfo offlineJobStatus = getOffLinePushStatus(clusterName, topic);
        if (offlineJobStatus.getExecutionStatus().isTerminal()) {
          logger.info("Offline job for the existing topic: " + topic + " is already done, just skip it");
          continue;
        }
        throw new VeniceException("Topic: " + topic + " exists for store: " + storeName +
            ", please wait for previous job to be finished, and reach out Venice team if it is" +
            " not this case");
      }
    }
    Version newVersion = veniceHelixAdmin.addVersion(clusterName, storeName, VeniceHelixAdmin.VERSION_ID_UNSET,
        numberOfPartition, replicationFactor, false);
    cleanupHistoricalVersions(clusterName, storeName);
    return newVersion;
  }

  @Override
  public int getCurrentVersion(String clusterName, String storeName) {
    throw new VeniceException("getCurrentVersion is not supported!");
  }

  @Override
  public Version peekNextVersion(String clusterName, String storeName) {
    throw new VeniceException("peekNextVersion is not supported!");
  }

  @Override
  public List<Version> deleteAllVersionsInStore(String clusterName, String storeName) {
    acquireLock(clusterName);
    try {
      Store store = veniceHelixAdmin.getStore(clusterName, storeName);
      if (store == null) {
        throw new VeniceNoStoreException(storeName);
      }
      DeleteAllVersions deleteAllVersions = (DeleteAllVersions) AdminMessageType.DELETE_ALL_VERSIONS.getNewInstance();
      deleteAllVersions.clusterName = clusterName;
      deleteAllVersions.storeName = storeName;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.DELETE_ALL_VERSIONS.ordinal();
      message.payloadUnion = deleteAllVersions;

      sendAdminMessageAndWaitForConsumed(clusterName, message);
      return Collections.emptyList();
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
  public Store getStore(String clusterName, String storeName) {
    return veniceHelixAdmin.getStore(clusterName, storeName);
  }

  @Override
  public boolean hasStore(String clusterName, String storeName) {
    return veniceHelixAdmin.hasStore(clusterName, storeName);
  }

  @Override
  public void setCurrentVersion(String clusterName,
                                String storeName,
                                int versionNumber) {
    acquireLock(clusterName);
    try {
      Store store = veniceHelixAdmin.getStore(clusterName, storeName);
      if (store == null) {
        throw new VeniceNoStoreException(storeName);
      }
      SetStoreCurrentVersion setStoreCurrentVersion = (SetStoreCurrentVersion) AdminMessageType.SET_STORE_CURRENT_VERSION.getNewInstance();
      setStoreCurrentVersion.clusterName = clusterName;
      setStoreCurrentVersion.storeName = storeName;
      setStoreCurrentVersion.currentVersion = versionNumber;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.SET_STORE_CURRENT_VERSION.ordinal();
      message.payloadUnion = setStoreCurrentVersion;

      sendAdminMessageAndWaitForConsumed(clusterName, message);
    } finally {
      releaseLock();
    }
  }

  @Override
  public void setStoreOwner(String clusterName, String storeName, String owner) {
    acquireLock(clusterName);
    try {
      Store store = veniceHelixAdmin.getStore(clusterName, storeName);
      if (store == null) {
        throw new VeniceNoStoreException(storeName);
      }
      SetStoreOwner setStoreOwner = (SetStoreOwner) AdminMessageType.SET_STORE_OWNER.getNewInstance();
      setStoreOwner.clusterName = clusterName;
      setStoreOwner.storeName = storeName;
      setStoreOwner.owner = owner;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.SET_STORE_OWNER.ordinal();
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
      Store store = veniceHelixAdmin.getStore(clusterName, storeName);
      if (store == null) {
        throw new VeniceNoStoreException(storeName);
      }
      SetStorePartitionCount setStorePartition = (SetStorePartitionCount) AdminMessageType.SET_STORE_PARTITION.getNewInstance();
      setStorePartition.clusterName = clusterName;
      setStorePartition.storeName = storeName;
      setStorePartition.partitionNum = partitionCount;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.SET_STORE_OWNER.ordinal();
      message.payloadUnion = setStorePartition;

      sendAdminMessageAndWaitForConsumed(clusterName, message);
    } finally {
      releaseLock();
    }
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
  public SchemaEntry addValueSchema(String clusterName, String storeName, String valueSchemaStr) {
    acquireLock(clusterName);
    try {
      int newValueSchemaId = veniceHelixAdmin.checkPreConditionForAddValueSchemaAndGetNewSchemaId(clusterName, storeName, valueSchemaStr);
      logger.info("Adding value schema: " + valueSchemaStr + " to store: " + storeName + " in cluster: " + clusterName);

      ValueSchemaCreation valueSchemaCreation = (ValueSchemaCreation) AdminMessageType.VALUE_SCHEMA_CREATION.getNewInstance();
      valueSchemaCreation.clusterName = clusterName;
      valueSchemaCreation.storeName = storeName;
      SchemaMeta schemaMeta = new SchemaMeta();
      schemaMeta.definition = valueSchemaStr;
      schemaMeta.schemaType = SchemaType.AVRO_1_4.ordinal();
      valueSchemaCreation.schema = schemaMeta;
      valueSchemaCreation.schemaId = newValueSchemaId;

      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.VALUE_SCHEMA_CREATION.ordinal();
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
  public SchemaEntry addValueSchema(String clusterName, String storeName, String valueSchemaStr, int schemaId) {
    throw new VeniceException("addValueSchema by specifying schema id is not supported!");
  }

  @Override
  public List<String> getStorageNodes(String clusterName) {
    throw new VeniceException("getStorageNodes is not supported!");
  }

  private Map<String, ControllerClient> getControllerClientMap(String clusterName){
    Map<String, ControllerClient> controllerClients = new HashMap<>();

    veniceControllerConfig.getChildClusterMap().entrySet().
      forEach(entry -> controllerClients.put(entry.getKey(), new ControllerClient(clusterName, entry.getValue())));
    veniceControllerConfig.getChildClusterD2Map().entrySet().
      forEach(entry -> controllerClients.put(entry.getKey(), new D2ControllerClient(clusterName, entry.getValue())));

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

  protected static OfflinePushStatusInfo getOffLineJobStatus(String clusterName, String kafkaTopic,
      Map<String, ControllerClient> controllerClients, TopicManager topicManager) {
    Set<String> childClusters = controllerClients.keySet();
    ExecutionStatus currentReturnStatus = ExecutionStatus.NOT_CREATED;
    List<ExecutionStatus> statuses = new ArrayList<>();
    Map<String, String> extraInfo = new HashMap<>();
    int failCount = 0;
    for (String cluster : childClusters){
      JobStatusQueryResponse response = controllerClients.get(cluster).queryJobStatus(clusterName, kafkaTopic);
      if (response.isError()){
        failCount += 1;
        logger.warn("Couldn't query " + cluster + " for job " + kafkaTopic + " status: " + response.getError());
        extraInfo.put(cluster, ExecutionStatus.UNKNOWN.toString());
      } else {
        ExecutionStatus thisStatus = ExecutionStatus.valueOf(response.getStatus());
        statuses.add(thisStatus);
        extraInfo.put(cluster, thisStatus.toString());
      }
    }

    // Sort the per-datacenter status in this order, and return the first one in the list
    // Edge case example: if one cluster is stuck in NOT_CREATED, then
    //   as another cluster goes from PROGRESS to COMPLETED
    //   the aggregate status will go from PROGRESS back down to NOT_CREATED.
    Ordering<ExecutionStatus> priorityOrder = Ordering.explicit(Arrays.asList(
        ExecutionStatus.PROGRESS,
        ExecutionStatus.STARTED,
        ExecutionStatus.NEW,
        ExecutionStatus.NOT_CREATED,
        ExecutionStatus.ERROR,
        ExecutionStatus.COMPLETED,
        ExecutionStatus.ARCHIVED));
    Collections.sort(statuses, priorityOrder::compare);
    if (statuses.size()>0){
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
      }
      logger.info("Deleting kafka topic: " + kafkaTopic + " with job status: " + currentReturnStatus);
      topicManager.syncDeleteTopic(kafkaTopic);
    }

    return new OfflinePushStatusInfo(currentReturnStatus, extraInfo);
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
      JobStatusQueryResponse statusResponse = client.queryJobStatus(clusterName, kafkaTopic);
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
  public String getKafkaBootstrapServers() {
    return veniceHelixAdmin.getKafkaBootstrapServers();
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
    return veniceControllerConfig.getChildClusterMap().size();
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
  public boolean isInstanceRemovable(String clusterName, String instanceId) {
    throw new VeniceException("isInstanceRemovable is not supported!");
  }

  @Override
  public boolean isInstanceRemovable(String clusterName, String helixNodeId, int minActiveReplicas) {
    throw new VeniceException("isInstanceRemovable is not supported!");
  }

  @Override
  public Instance getMasterController(String clusterName) {
    return veniceHelixAdmin.getMasterController(clusterName);
  }

  @Override
  public void disableStoreWrite(String clusterName, String storeName) {
    acquireLock(clusterName);
    try {
      veniceHelixAdmin.checkPreConditionForDisableStoreAndGetStore(clusterName, storeName);
      logger.info("Disabling store to write: " + storeName + " in cluster: " + clusterName);

      // Did not change PauseStore message name to keep message protocol compatible.
      PauseStore pauseStore = (PauseStore) AdminMessageType.DISABLE_STORE_WRITE.getNewInstance();
      pauseStore.clusterName = clusterName;
      pauseStore.storeName = storeName;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.DISABLE_STORE_WRITE.ordinal();
      message.payloadUnion = pauseStore;

      sendAdminMessageAndWaitForConsumed(clusterName, message);
    } finally {
      releaseLock();
    }
  }

  @Override
  public void enableStoreWrite(String clusterName, String storeName) {
    acquireLock(clusterName);
    try {
      veniceHelixAdmin.checkPreConditionForDisableStoreAndGetStore(clusterName, storeName);
      logger.info("Enabling store to write: " + storeName + " in cluster: " + clusterName);

      // Did not change resumeStore message name to keep message protocol compatible.
      ResumeStore resumeStore = (ResumeStore) AdminMessageType.ENABLE_STORE_WRITE.getNewInstance();
      resumeStore.clusterName = clusterName;
      resumeStore.storeName = storeName;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.ENABLE_STORE_WRITE.ordinal();
      message.payloadUnion = resumeStore;

      sendAdminMessageAndWaitForConsumed(clusterName, message);
    } finally {
      releaseLock();
    }
  }

  @Override
  public void disableStoreRead(String clusterName, String storeName) {
    acquireLock(clusterName);
    try {
      veniceHelixAdmin.checkPreConditionForDisableStoreAndGetStore(clusterName, storeName);
      logger.info("Disabling store to read: " + storeName + " in cluster: " + clusterName);

      DisableStoreRead disableStoreRead = (DisableStoreRead) AdminMessageType.DIABLE_STORE_READ.getNewInstance();
      disableStoreRead.clusterName = clusterName;
      disableStoreRead.storeName = storeName;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.DIABLE_STORE_READ.ordinal();
      message.payloadUnion = disableStoreRead;

      sendAdminMessageAndWaitForConsumed(clusterName, message);
    } finally {
      releaseLock();
    }
  }

  @Override
  public void enableStoreRead(String clusterName, String storeName) {
    acquireLock(clusterName);
    try {
      veniceHelixAdmin.checkPreConditionForDisableStoreAndGetStore(clusterName, storeName);
      logger.info("Enabling store to read: " + storeName + " in cluster: " + clusterName);

      EnableStoreRead enableStoreRead = (EnableStoreRead) AdminMessageType.ENABLE_STORE_READ.getNewInstance();
      enableStoreRead.clusterName = clusterName;
      enableStoreRead.storeName = storeName;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.ENABLE_STORE_READ.ordinal();
      message.payloadUnion = enableStoreRead;

      sendAdminMessageAndWaitForConsumed(clusterName, message);
    } finally {
      releaseLock();
    }
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
      // Remove Kafka topic
      TopicManager topicManager = getTopicManager();
      logger.info("Deleting topic when kill offline push job, topic: " + kafkaTopic);
      topicManager.syncDeleteTopic(kafkaTopic);

      KillOfflinePushJob killJob = (KillOfflinePushJob) AdminMessageType.KILL_OFFLINE_PUSH_JOB.getNewInstance();
      killJob.clusterName = clusterName;
      killJob.kafkaTopic = kafkaTopic;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.KILL_OFFLINE_PUSH_JOB.ordinal();
      message.payloadUnion = killJob;

      sendAdminMessageAndWaitForConsumed(clusterName, message);
    } finally {
      releaseLock();
    }
  }

  @Override
  public StorageNodeStatus getStorageNodeStatus(String clusterName, String instanceId) {
    throw new VeniceException("getStorageNodeStatus is not supported!");
  }

  @Override
  public boolean isStorageNodeNewerOrEqualTo(String clusterName, String instanceId,
                                             StorageNodeStatus oldServerStatus) {
    throw new VeniceException("isStorageNodeNewerOrEqualTo is not supported!");
  }

  @Override
  public void setDelayedRebalanceTime(String clusterName, long delayedTime) {
    throw new VeniceException("setDelayedRebalanceTime is not supported!");
  }

  @Override
  public long getDelayedRebalanceTime(String clusterName) {
    throw new VeniceException("getDelayedRebalanceTime is not supported!");
  }

  public void setAdminConsumerService(String clusterName, AdminConsumerService service){
    veniceHelixAdmin.setAdminConsumerService(clusterName, service);
  }

  @Override
  public void skipAdminMessage(String clusterName, long offset){
    veniceHelixAdmin.skipAdminMessage(clusterName, offset);
  }

  @Override
  public long getLastSucceedExecutionId(String clustername) {
    return veniceHelixAdmin.getLastSucceedExecutionId(clustername);
  }

  @Override
  public void setLastException(String clusterName, Exception e) {

  }

  @Override
  public Exception getLastException(String clusterName) {
    return null;
  }

  @Override
  public Optional<AdminCommandExecutionTracker> getAdminCommandExecutionTracker() {
    return Optional.of(adminCommandExecutionTracker);
  }

  @Override
  public synchronized void stop(String clusterName) {
    veniceHelixAdmin.stop(clusterName);
    // Close the admin producer for this cluster
    VeniceWriter<byte[], byte[]> veniceWriter = veniceWriterMap.get(clusterName);
    if (null != veniceWriter) {
      veniceWriter.close();
    }
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
}
