package com.linkedin.venice.controller.kafka.consumer;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerService;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.DeleteAllVersions;
import com.linkedin.venice.controller.kafka.protocol.admin.DisableStoreRead;
import com.linkedin.venice.controller.kafka.protocol.admin.EnableStoreRead;
import com.linkedin.venice.controller.kafka.protocol.admin.KillOfflinePushJob;
import com.linkedin.venice.controller.kafka.protocol.admin.PauseStore;
import com.linkedin.venice.controller.kafka.protocol.admin.ResumeStore;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.ValueSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controller.stats.ControllerStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.validation.DataValidationException;
import com.linkedin.venice.exceptions.validation.DuplicateDataException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.consumer.VeniceConsumerFactory;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.validation.OffsetRecordTransformer;
import com.linkedin.venice.kafka.validation.ProducerTracker;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.Utils;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.Logger;

/**
 * This class is used to create a task, which will consume the admin messages from the special admin topics.
 */
public class AdminConsumptionTask implements Runnable, Closeable {
  private static final String CONSUMER_TASK_ID_FORMAT = AdminConsumptionTask.class.getSimpleName() + " for [ Topic: %s ]";
  public static int READ_CYCLE_DELAY_MS = 1000;

  private final Logger logger = Logger.getLogger(AdminConsumptionTask.class);

  private final String clusterName;
  private final String topic;
  private final String consumerTaskId;
  private final OffsetManager offsetManager;
  private final Admin admin;
  private final boolean isParentController;
  private final AtomicBoolean isRunning;
  private final AdminOperationSerializer deserializer;
  private ControllerStats controllerStats;
  private final long failureRetryTimeoutMs;

  private boolean isSubscribed;
  private KafkaConsumerWrapper consumer;
  private OffsetRecord lastOffset;
  private volatile long offsetToSkip = -1L;
  private volatile long lastFailedOffset = -1L;
  private boolean topicExists;
  // Used to store state info to offset record
  private Optional<OffsetRecordTransformer> offsetRecordTransformer = Optional.empty();

  /**
   * Keeps track of every upstream producer this consumer task has seen so far.
   */
  private final Map<GUID, ProducerTracker> producerTrackerMap;

  public AdminConsumptionTask(String clusterName,
                              VeniceConsumerFactory consumerFactory,
                              String kafkaBootstrapServers,
                              Admin admin,
                              OffsetManager offsetManager,
                              long failureRetryTimeoutMs,
                              boolean isParentController) {
    this.clusterName = clusterName;
    this.topic = AdminTopicUtils.getTopicNameFromClusterName(clusterName);
    this.consumerTaskId = String.format(CONSUMER_TASK_ID_FORMAT, this.topic);
    this.admin = admin;
    this.isParentController = isParentController;
    this.failureRetryTimeoutMs = failureRetryTimeoutMs;

    this.deserializer = new AdminOperationSerializer();

    this.isRunning = new AtomicBoolean(true);
    this.isSubscribed = false;
    this.lastOffset = new OffsetRecord();
    this.topicExists = false;
    this.controllerStats = ControllerStats.getInstance();

    Properties kafkaConsumerProperties = VeniceControllerService.getKafkaConsumerProperties(kafkaBootstrapServers, clusterName);
    this.consumer = consumerFactory.getConsumer(kafkaConsumerProperties);
    this.offsetManager = offsetManager;
    this.producerTrackerMap = new HashMap<>();
  }

  // For testing
  public void setControllerStats(ControllerStats stats) {
    this.controllerStats = stats;
  }

  @Override
  public synchronized void close() throws IOException {
    isRunning.getAndSet(false);
  }

  @Override
  public void run() { //TODO: clean up this method.  We've got nested loops checking the same conditions
    logger.info("Running consumer: " + consumerTaskId);
    int noTopicCounter = 0;
    while (isRunning.get()) {
      try {
        Utils.sleep(READ_CYCLE_DELAY_MS);
        // check whether current controller is the master controller for the given cluster
        if (admin.isMasterController(clusterName)) {
          if (!isSubscribed) {
            // check whether the admin topic exists or not
            if (!whetherTopicExists(topic)) {
              if (noTopicCounter % 60 == 0) { // To reduce log bloat, only log once per minute
                logger.info("Admin topic: " + topic + " hasn't been created yet");
              }
              noTopicCounter++;
              continue;
            }
            lastOffset = offsetManager.getLastOffset(topic, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
            // First let's try to restore the state retrieved from the OffsetManager
            lastOffset.getProducerPartitionStateMap().entrySet().stream().forEach(entry -> {
                  GUID producerGuid = GuidUtils.getGuidFromCharSequence(entry.getKey());
                  ProducerTracker producerTracker = producerTrackerMap.get(producerGuid);
                  if (null == producerTracker) {
                    producerTracker = new ProducerTracker(producerGuid);
                  }
                  producerTracker.setPartitionState(AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID, entry.getValue());
                  producerTrackerMap.put(producerGuid, producerTracker);
                }
            );
            // Subscribe the admin topic
            consumer.subscribe(topic, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID, lastOffset);
            isSubscribed = true;
            logger.info("Subscribe to topic name: " + topic + ", offset: " + lastOffset.getOffset());
          }
          ConsumerRecords records = consumer.poll(READ_CYCLE_DELAY_MS);
          if (null == records) {
            logger.info("Received null records");
            continue;
          }
          logger.debug("Received record num: " + records.count());
          Iterator<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> recordsIterator = records.iterator();
          while (isRunning.get() && admin.isMasterController(clusterName) && recordsIterator.hasNext()) {
            ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record = recordsIterator.next();
            int retryCount = 0;
            long retryStartTime = System.currentTimeMillis();
            while (isRunning.get() && admin.isMasterController(clusterName)) {
              try {
                boolean isRetry = retryCount > 0;
                processMessage(record, isRetry);
                break;
              } catch (Exception e) {
                // Retry should happen in message level, not in batch
                retryCount += 1; // increment and report count if we have a failure
                controllerStats.recordFailedAdminConsumption(retryCount);
                lastFailedOffset=record.offset();
                logger.error("Error when processing admin message with offset "+record.offset()+", will retry", e);
                admin.setLastException(clusterName, e);
                if (System.currentTimeMillis() - retryStartTime >= failureRetryTimeoutMs) {
                  logger.error("Failure processing admin message for more than " + TimeUnit.MILLISECONDS.toMinutes(failureRetryTimeoutMs) + " minutes", e);
                  skipMessage(record);
                  break;
                }
                Utils.sleep(READ_CYCLE_DELAY_MS);
              }
            }
          }
        } else {
          // Current controller is not the master controller for the given cluster
          if (isSubscribed) {
            consumer.unSubscribe(topic, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
            isSubscribed = false;
            logger.info("Unsubscribe from topic name: " + topic);
          }
        }
      } catch (Exception e) {
        logger.error("Got exception while running admin consumption task", e);
      }
    }
    // Release resources
    internalClose();
  }

  private void internalClose() {
    if (isSubscribed) {
      consumer.unSubscribe(topic, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
      isSubscribed = false;
      logger.info("Unsubscribe from topic name: " + topic);
    }
    logger.info("Closed consumer for admin topic: " + topic);
    consumer.close();
  }

  private boolean whetherTopicExists(String topicName) {
    if (topicExists) {
      return true;
    }
    // Check it again if it is false
    topicExists = admin.getTopicManager().containsTopic(topicName);
    return topicExists;
  }

  /**
   * This function is used to check whether current message is valid to process.
   * If some significant DIV issue happens, this function will throw exception.
   *
   * @param record
   * @param isRetry whether current record is a normal record, or a retry record because of exception during
   *                handling current record.
   * @return
   *  false : we can safely skip current message.
   *  true : normal admin message, which should be processed.
   */
  private boolean checkAndValidateMessage(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record, boolean isRetry) {
    if (!shouldProcessRecord(record)){
      persistRecordOffset(record); // We don't process the data validation control messages for example
      return false;
    }

    KafkaKey kafkaKey = record.key();
    KafkaMessageEnvelope kafkaValue = record.value();
    try {
      final GUID producerGUID = kafkaValue.producerMetadata.producerGUID;
      ProducerTracker producerTracker = producerTrackerMap.get(producerGUID);
      if (producerTracker == null) {
        producerTracker = new ProducerTracker(producerGUID);
        producerTrackerMap.put(producerGUID, producerTracker);
      }
      offsetRecordTransformer = Optional.of(producerTracker.addMessage(AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID, kafkaKey, kafkaValue));
    } catch (DuplicateDataException e) {
      if (isRetry) {
        // When retrying, it is valid to receive DuplicateDataException,
        // and we should proceed to handle this message instead of skipping it.
        return true;
      } else {
        logger.info("Skipping a duplicate record in topic: " + topic + "', offset: " + record.offset());
        persistRecordOffset(record);
        return false;
      }
    } catch (DataValidationException dve) {
      logger.error("Received data validation error", dve);
      controllerStats.recordAdminTopicDIVErrorReportCount();
      throw dve;
    }

    return true;
  }

  private void processMessage(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record, boolean isRetry) {
    if (! checkAndValidateMessage(record, isRetry)) {
      return;
    }
    KafkaKey kafkaKey = record.key();
    KafkaMessageEnvelope kafkaValue = record.value();

    if (kafkaKey.isControlMessage()) {
      logger.info("Receive control message: " + kafkaValue);
      persistRecordOffset(record);
      return;
    }
    // check message type
    MessageType messageType = MessageType.valueOf(kafkaValue);
    if (MessageType.PUT != messageType) {
      throw new VeniceException("Received unknown message type: " + messageType);
    }
    Put put = (Put) kafkaValue.payloadUnion;
    AdminOperation adminMessage = deserializer.deserialize(put.putValue.array(), put.schemaId);
    if (logger.isDebugEnabled()) {
      logger.debug("Received message: " + adminMessage);
    }
    switch (AdminMessageType.valueOf(adminMessage)) {
      case STORE_CREATION:
        handleStoreCreation((StoreCreation) adminMessage.payloadUnion);
        break;
      case VALUE_SCHEMA_CREATION:
        handleValueSchemaCreation((ValueSchemaCreation) adminMessage.payloadUnion);
        break;
      case DISABLE_STORE_WRITE:
        handleDisableStoreWrite((PauseStore) adminMessage.payloadUnion);
        break;
      case ENABLE_STORE_WRITE:
        handleEnableStoreWrite((ResumeStore) adminMessage.payloadUnion);
        break;
      case KILL_OFFLINE_PUSH_JOB:
        handleKillOfflinePushJob((KillOfflinePushJob) adminMessage.payloadUnion);
        break;
      case DIABLE_STORE_READ:
        handleDisableStoreRead((DisableStoreRead) adminMessage.payloadUnion);
        break;
      case ENABLE_STORE_READ:
        handleEnableStoreRead((EnableStoreRead) adminMessage.payloadUnion);
        break;
      case DELETE_ALL_VERSIONS:
        handleDeleteAllVersions((DeleteAllVersions) adminMessage.payloadUnion);
        break;
      default:
        throw new VeniceException("Unknown admin operation type: " + adminMessage.operationType);
    }
    persistRecordOffset(record);
  }

  private void skipMessage(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record) {
    try {
      Put put = (Put) record.value().payloadUnion;
      AdminOperation adminMessage = deserializer.deserialize(put.putValue.array(), put.schemaId);
      logger.warn("Skipping consumption of message: " + adminMessage);
    } catch (Exception e){
      logger.warn("Skipping consumption of message: " + record.toString());
    }
    persistRecordOffset(record);
  }

  private void persistRecordOffset(ConsumerRecord record){
    long recordOffset = record.offset();
    if (recordOffset > lastOffset.getOffset()) {
      lastOffset.setOffset(recordOffset);
      if (offsetRecordTransformer.isPresent()) {
        lastOffset = offsetRecordTransformer.get().transform(lastOffset);
      }
      offsetManager.recordOffset(topic, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID, lastOffset);
    }
  }

  public void skipMessageWithOffset(long offset){
    if (offset == lastFailedOffset) {
      if (offset > lastOffset.getOffset()) {
        offsetToSkip = offset;
      } else {
        throw new VeniceException("Cannot skip an offset that has already been consumed.  Last consumed offset is: " + lastOffset.getOffset());
      }
    } else {
      throw new VeniceException("Cannot skip an offset that isn't failing.  Last failed offset is: " + lastFailedOffset);
    }
  }

  private boolean shouldProcessRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record) {
    // check topic
    String recordTopic = record.topic();
    if (!topic.equals(recordTopic)) {
      throw new VeniceException(consumerTaskId + " received message from different topic: " + recordTopic + ", expected: " + topic);
    }
    // check partition
    int recordPartition = record.partition();
    if (AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID != recordPartition) {
      throw new VeniceException(consumerTaskId + " received message from different partition: " + recordPartition + ", expected: " + AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
    }
    long recordOffset = record.offset();
    // should skip?  Note, at this point we are guaranteed to be on the correct topic and partition
    if (recordOffset == offsetToSkip){
      logger.warn("Skipping admin message with offset: " + recordOffset + " per instruction to skip.");
      return false;
    }
    // check offset
    if (lastOffset.getOffset() >= recordOffset) {
      logger.error(consumerTaskId + ", current record has been processed, " +
          "last known offset: " + lastOffset.getOffset() + ", current offset: " + recordOffset);
      return false;
    }

    return true;
  }

  private void handleStoreCreation(StoreCreation message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    String owner = message.owner.toString();
    String keySchema = message.keySchema.definition.toString();
    String valueSchema = message.valueSchema.definition.toString();

    /* // failure path for testing.  Enable this code path to run the disabled test in TestAdminConsumptionTask
    if (storeName.equals("store-that-fails")){
      throw new VeniceException("Tried to create failure store named store-that-fails");
    } */

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
    admin.disableStoreWrite(clusterName, storeName);

    logger.info("Disabled store to write: " + storeName + " in cluster: " + clusterName);
  }

  private void handleEnableStoreWrite(ResumeStore message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    admin.enableStoreWrite(clusterName, storeName);

    logger.info("Enabled store to write: " + storeName + " in cluster: " + clusterName);
  }

  private void handleDisableStoreRead(DisableStoreRead message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    admin.disableStoreRead(clusterName, storeName);

    logger.info("Disabled store to read: " + storeName + " in cluster: " + clusterName);
  }

  private void handleEnableStoreRead(EnableStoreRead message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    admin.enableStoreRead(clusterName, storeName);

    logger.info("Enabled store to read: " + storeName + " in cluster: " + clusterName);
  }

  private void handleKillOfflinePushJob(KillOfflinePushJob message) {
    if (isParentController) {
      // Do nothing for Parent Controller
      return;
    }
    String clusterName = message.clusterName.toString();
    String kafkaTopic = message.kafkaTopic.toString();
    admin.killOfflineJob(clusterName, kafkaTopic);

    logger.info("Killed job with topic: " + kafkaTopic + " in cluster: " + clusterName);
  }

  public void handleDeleteAllVersions(DeleteAllVersions message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    admin.deleteAllVersionsInStore(clusterName, storeName);
    logger.info("Deleted all of version in store:" + storeName + " in cluster:" + clusterName);
  }
}
