package com.linkedin.venice.controller;

import com.google.common.collect.Ordering;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.KillOfflinePushJob;
import com.linkedin.venice.controller.kafka.protocol.admin.PauseStore;
import com.linkedin.venice.controller.kafka.protocol.admin.ResumeStore;
import com.linkedin.venice.controller.kafka.protocol.admin.SchemaMeta;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.ValueSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.enums.SchemaType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.Replica;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.ApacheKafkaProducer;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import org.apache.kafka.clients.producer.ProducerConfig;
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
import org.apache.tools.ant.taskdefs.Exec;


/**
 * This class is a wrapper of {@link VeniceHelixAdmin}, which will be used in parent controller.
 * There should be only one single Parent Controller, which is the endpoint for all the admin data
 * update.
 * For every admin update operation, it will first push admin operation messages to Kafka,
 * then wait for the admin consumer to consume the message.
 */
public class VeniceParentHelixAdmin implements Admin {
  public static final String KAFKA_ACKS_CONFIG = ApacheKafkaProducer.PROPERTIES_KAFKA_PREFIX + ProducerConfig.ACKS_CONFIG;
  private static final long SLEEP_INTERVAL_FOR_DATA_CONSUMPTION_IN_MS = 1000;
  private static final Logger logger = Logger.getLogger(VeniceParentHelixAdmin.class);

  private final VeniceHelixAdmin veniceHelixAdmin;
  private final Map<String, VeniceWriter<byte[], byte[]>> veniceWriterMap;
  private final byte[] emptyKeyByteArr = new byte[0];
  private final AdminOperationSerializer adminOperationSerializer = new AdminOperationSerializer();
  private final OffsetManager offsetManager;
  private final VeniceControllerConfig veniceControllerConfig;
  private final Lock lock = new ReentrantLock();
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

  public VeniceParentHelixAdmin(VeniceHelixAdmin veniceHelixAdmin, OffsetManager offsetManager, VeniceControllerConfig config) {
    this.veniceHelixAdmin = veniceHelixAdmin;
    this.veniceWriterMap = new ConcurrentHashMap<>();
    this.offsetManager = offsetManager;
    this.veniceControllerConfig = config;
    this.waitingTimeForConsumptionMs = config.getParentControllerWaitingTimeForConsumptionMs();
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
    Set<String> topicSet = topicManager.listTopics();
    if (topicSet.contains(topicName)) {
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
    VeniceWriter<byte[], byte[]> veniceWriter = veniceWriterMap.get(clusterName);
    byte[] serializedValue = adminOperationSerializer.serialize(message);
    try {
      Future<RecordMetadata> future = veniceWriter.put(emptyKeyByteArr, serializedValue, AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION);
      RecordMetadata meta = future.get();

      lastOffset = meta.offset();
      logger.info("Sent message: " + message + " to kafka, offset: " + lastOffset);
      waitingLastOffsetToBeConsumed(clusterName);
    } catch (Exception e) {
      throw new VeniceException("Got exception during sending message to Kafka", e);
    }
  }

  private void waitingLastOffsetToBeConsumed(String clusterName) {
    String topicName = AdminTopicUtils.getTopicNameFromClusterName(clusterName);

    // Blocking until some consumer consumes the new message or timeout
    long startTime = SystemTime.INSTANCE.getMilliseconds();
    while (true) {
      // Check whether timeout
      long currentTime = SystemTime.INSTANCE.getMilliseconds();
      if (currentTime - startTime > waitingTimeForConsumptionMs) {
        throw new VeniceException("Some operation is going on, and the server could not finish current request", veniceHelixAdmin.getLastException(clusterName));
      }
      long consumedOffset = offsetManager.getLastOffset(topicName, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID).getOffset();
      if (consumedOffset >= lastOffset) {
        break;
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

  @Override
  public Version incrementVersion(String clusterName,
                                               String storeName,
                                               int numberOfPartition,
                                               int replicationFactor) {
    // TODO: consider to move version creation to admin protocol
    // Right now, TopicMonitor in each prod colo will monitor new Kafka topic and
    // create new corresponding store versions
    // TODO: clean up kafka topic in parent Kafka cluster
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
    for (String topic: topics) {
      if (AdminTopicUtils.isAdminTopic(topic)) {
        continue;
      }
      try {
        String storeNameForCurrentTopic = Version.parseStoreFromKafkaTopicName(topic);
        if (storeNameForCurrentTopic.equals(storeName)) {
          throw new VeniceException("Topic: " + topic + " exists for store: " + storeName +
              ", please wait for previous job to be finished, and reach out Venice team if it is" +
              " not this case");
        }
      } catch (Exception e) {
        logger.warn("Failed to parse StoreName from topic: " + topic);
      }
    }
    return veniceHelixAdmin.addVersion(clusterName, storeName, VeniceHelixAdmin.VERSION_ID_UNSET, numberOfPartition, replicationFactor, false);
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
    throw new VeniceException("setCurrentVersion is not supported yet!");
  }

  @Override
  public void startOfflinePush(String clusterName,
                               String kafkaTopic,
                               int numberOfPartition,
                               int replicaFactor,
                               OfflinePushStrategy strategy) {
    throw new VeniceException("startOfflinePush is not supported!");
  }

  @Override
  public void deleteHelixResource(String clusterName, String kafkaTopic) {
    throw new VeniceException("deleteHelixResource is not supported yet!");
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
    Map<String, Set<String>> childColoClusters = veniceControllerConfig.getChildClusterMap();
    Map<String, ControllerClient> controllerClients = new HashMap<>();
    for (String colo : childColoClusters.keySet()) {
      String veniceUrls = String.join(",", childColoClusters.get(colo));
      ControllerClient client = new ControllerClient(clusterName, veniceUrls);
      controllerClients.put(colo, client);
    }
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
  public ExecutionStatus getOffLineJobStatus(String clusterName, String kafkaTopic) {
    Map<String, ControllerClient> controllerClients = getControllerClientMap(clusterName);
    return getOffLineJobStatus(clusterName, kafkaTopic, controllerClients, getTopicManager());
  }

  protected static ExecutionStatus getOffLineJobStatus(String clusterName, String kafkaTopic, Map<String, ControllerClient> controllerClients, TopicManager topicManager) {

    Set<String> childClusters = controllerClients.keySet();
    ExecutionStatus currentReturnStatus = ExecutionStatus.NOT_CREATED;
    List<ExecutionStatus> statuses = new ArrayList<>();
    int failCount = 0;
    for (String cluster : childClusters){
      JobStatusQueryResponse response = controllerClients.get(cluster).queryJobStatus(clusterName, kafkaTopic);
      if (response.isError()){
        failCount += 1;
        logger.warn("Couldn't query " + cluster + " for job " + kafkaTopic + " status: " + response.getError());
      } else {
        ExecutionStatus thisStatus = ExecutionStatus.valueOf(response.getStatus());
        statuses.add(thisStatus);
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
      topicManager.deleteTopic(kafkaTopic);
    }

    return currentReturnStatus;
  }

  /**
   * Queries child clusters for job progress.  Prepends the cluster name to the task ID and provides an aggregate
   * Map of progress for all tasks.
   * @param clusterName
   * @param kafkaTopic
   * @return
   */
  @Override
  public Map<String, Long> getOfflineJobProgress(String clusterName, String kafkaTopic){
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
  public Instance getMasterController(String clusterName) {
    return veniceHelixAdmin.getMasterController(clusterName);
  }

  @Override
  public void pauseStore(String clusterName, String storeName) {
    acquireLock(clusterName);
    try {
      veniceHelixAdmin.checkPreConditionForPauseStoreAndGetStore(clusterName, storeName, true);
      logger.info("Pausing store: " + storeName + " in cluster: " + clusterName);

      PauseStore pauseStore = (PauseStore) AdminMessageType.PAUSE_STORE.getNewInstance();
      pauseStore.clusterName = clusterName;
      pauseStore.storeName = storeName;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.PAUSE_STORE.ordinal();
      message.payloadUnion = pauseStore;

      sendAdminMessageAndWaitForConsumed(clusterName, message);
    } finally {
      releaseLock();
    }
  }

  @Override
  public void resumeStore(String clusterName, String storeName) {
    acquireLock(clusterName);
    try {
      veniceHelixAdmin.checkPreConditionForPauseStoreAndGetStore(clusterName, storeName, false);
      logger.info("Resuming store: " + storeName + " in cluster: " + clusterName);

      ResumeStore resumeStore = (ResumeStore) AdminMessageType.RESUME_STORE.getNewInstance();
      resumeStore.clusterName = clusterName;
      resumeStore.storeName = storeName;
      AdminOperation message = new AdminOperation();
      message.operationType = AdminMessageType.RESUME_STORE.ordinal();
      message.payloadUnion = resumeStore;

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
  public void killOfflineJob(String clusterName, String kafkaTopic) {
    acquireLock(clusterName);
    try {
      veniceHelixAdmin.checkPreConditionForKillOfflineJob(clusterName, kafkaTopic);
      logger.info("Killing offline push job for topic: " + kafkaTopic + " in cluster: " + clusterName);
      // Remove Kafka topic
      TopicManager topicManager = getTopicManager();
      logger.info("Deleting topic when kill offline push job, topic: " + kafkaTopic);
      topicManager.deleteTopic(kafkaTopic);

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
  public void setLastException(String clusterName, Exception e) {

  }

  @Override
  public Exception getLastException(String clusterName) {
    return null;
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
