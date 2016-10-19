package com.linkedin.venice.controller.kafka.consumer;

import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.controller.kafka.protocol.admin.AdminOperation;
import com.linkedin.venice.controller.kafka.protocol.admin.KillOfflinePushJob;
import com.linkedin.venice.controller.kafka.protocol.admin.PauseStore;
import com.linkedin.venice.controller.kafka.protocol.admin.ResumeStore;
import com.linkedin.venice.controller.kafka.protocol.admin.StoreCreation;
import com.linkedin.venice.controller.kafka.protocol.admin.ValueSchemaCreation;
import com.linkedin.venice.controller.kafka.protocol.enums.AdminMessageType;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.controller.stats.ControllerStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetManager;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.Logger;

import javax.validation.constraints.NotNull;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

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
  private final ControllerStats controllerStats;

  private boolean isSubscribed;
  private KafkaConsumerWrapper consumer;
  private long lastOffset;
  private boolean topicExists;

  public AdminConsumptionTask(@NotNull String clusterName,
                              @NotNull KafkaConsumerWrapper consumer,
                              @NotNull OffsetManager offsetManager,
                              @NotNull Admin admin,
                              boolean isParentController) {
    this.clusterName = clusterName;
    this.topic = AdminTopicUtils.getTopicNameFromClusterName(clusterName);
    this.consumerTaskId = String.format(CONSUMER_TASK_ID_FORMAT, this.topic);
    this.consumer = consumer;
    this.offsetManager = offsetManager;
    this.admin = admin;
    this.isParentController = isParentController;

    this.deserializer = new AdminOperationSerializer();

    this.isRunning = new AtomicBoolean(true);
    this.isSubscribed = false;
    this.lastOffset = -1;
    this.topicExists = false;
    this.controllerStats = ControllerStats.getInstance();
  }

  @Override
  public synchronized void close() throws IOException {
    isRunning.getAndSet(false);
  }

  @Override
  public void run() {
    logger.info("Running consumer: " + consumerTaskId);

    while (isRunning.get()) {
      try {
        // check whether current controller is the master controller for the given cluster
        if (admin.isMasterController(clusterName)) {
          if (!isSubscribed) {
            // check whether the admin topic exists or not
            if (!whetherTopicExists(topic)) {
              Utils.sleep(READ_CYCLE_DELAY_MS);
              logger.info("Admin topic: " + topic + " hasn't been created yet");
              continue;
            }
            // Subscribe the admin topic
            lastOffset = offsetManager.getLastOffset(topic, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID).getOffset();
            consumer.subscribe(topic, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID, new OffsetRecord(lastOffset));
            isSubscribed = true;
            logger.info("Subscribe to topic name: " + topic + ", offset: " + lastOffset);
          }
          ConsumerRecords records = consumer.poll(READ_CYCLE_DELAY_MS);
          if (null == records) {
            logger.info("Received null records");
            continue;
          }
          logger.debug("Received record num: " + records.count());
          Iterator<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> recordsIterator = records.iterator();

          while (isRunning.get() && admin.isMasterController(clusterName) && recordsIterator.hasNext()) {
            int retryCount = 0;
            ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record = recordsIterator.next();
            boolean retry = true;
            // TODO: we might need to consider to reload Offset, so that
            // admin tool is able to let consumer skip the bad message by updating consumed offset.
            /**
             * If there are some bad messages in the topic, the following steps could be considered to skip them:
             * 1. Update the admin topic offset to skip those messages;
             * 2. Restart master controller;
             */
            while (isRunning.get() && admin.isMasterController(clusterName) && retry) {
              try {
                processMessage(record);
                retry = false;
                retryCount = 0; // reset count if we process successfully
                break;
              } catch (Exception e) {
                // Retry should happen in message level, not in batch
                retryCount += 1; // increment and report count if we have a failure
                controllerStats.recordFailedAdminConsumption(retryCount);
                // Something bad happens, we need to keep retrying here,
                // since next 'poll' function call won't retrieve the same message any more
                logger.error("Error when processing admin message, will retry", e);
                admin.setLastException(clusterName, e);
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
          Utils.sleep(READ_CYCLE_DELAY_MS);
        }
      } catch (Exception e) {
        logger.error("Got exception while running admin consumption task", e);
        Utils.sleep(READ_CYCLE_DELAY_MS);
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
    consumer.close();
    logger.info("Closed consumer for admin topic: " + topic);
  }

  private boolean whetherTopicExists(String topicName) {
    if (topicExists) {
      return true;
    }
    // Check it again if it is false
    topicExists = admin.getTopicManager()
        .listTopics()
        .contains(topicName);
    return topicExists;
  }

  private void processMessage(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record) {
    // TODO: Add data validation logic here
    if (shouldProcessRecord(record)) {
      Put put = (Put) record.value().payloadUnion;
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
        case PAUSE_STORE:
          handlePauseStore((PauseStore) adminMessage.payloadUnion);
          break;
        case RESUME_STORE:
          handleResumeStore((ResumeStore) adminMessage.payloadUnion);
          break;
        case KILL_OFFLINE_PUSH_JOB:
          handleKillOfflinePushJob((KillOfflinePushJob) adminMessage.payloadUnion);
          break;
        default:
          throw new VeniceException("Unknown admin operation type: " + adminMessage.operationType);
      }
    }
    // Persist offset
    // This could happen if some exception gets thrown during handling those messages,
    // and the upstream will keep retrying with the same messages.
    long recordOffset = record.offset();
    if (recordOffset > lastOffset) {
      lastOffset = recordOffset;
      offsetManager.recordOffset(topic, AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID, new OffsetRecord(lastOffset));
    }
  }

  private boolean shouldProcessRecord(ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record) {
    // check topic
    String recordTopic = record.topic();
    if (!topic.equals(recordTopic)) {
      logger.error(consumerTaskId + " received message from different topic: " + recordTopic + ", expected: " + topic);
      return false;
    }
    // check partition
    int recordPartition = record.partition();
    if (AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID != recordPartition) {
      logger.error(consumerTaskId + " received message from different partition: " + recordPartition + ", expected: " + AdminTopicUtils.ADMIN_TOPIC_PARTITION_ID);
      return false;
    }
    // check offset
    long recordOffset = record.offset();
    if (lastOffset >= recordOffset) {
      logger.info(consumerTaskId + ", current record has been processed, last known offset: " + lastOffset + ", current offset: " + recordOffset);
      return false;
    }
    // check message type
    KafkaMessageEnvelope kafkaValue = record.value();
    MessageType messageType = MessageType.valueOf(kafkaValue);
    // TODO: Add data validation logic here, and there should be more MessageType here to handle, such as Control Message.
    if (MessageType.PUT != messageType) {
      logger.error("Received unknown message type: " + messageType);
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

  private void handlePauseStore(PauseStore message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    admin.pauseStore(clusterName, storeName);

    logger.info("Paused store: " + storeName + " in cluster: " + clusterName);
  }

  private void handleResumeStore(ResumeStore message) {
    String clusterName = message.clusterName.toString();
    String storeName = message.storeName.toString();
    admin.resumeStore(clusterName, storeName);

    logger.info("Resumed store: " + storeName + " in cluster: " + clusterName);
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
}
