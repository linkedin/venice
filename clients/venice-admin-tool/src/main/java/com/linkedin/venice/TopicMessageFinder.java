package com.linkedin.venice;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.store.QueryTool;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is used to find messages for a given key from a specified topic.
 */
public class TopicMessageFinder {
  private static final Logger LOGGER = LogManager.getLogger(TopicMessageFinder.class);

  public static long find(
      ControllerClient controllerClient,
      PubSubConsumerAdapter consumer,
      String topic,
      String keyString,
      long startTimestampEpochMs,
      long endTimestampEpochMs,
      long progressInterval) {
    String storeName;
    int version = -1;
    if (Version.isVersionTopic(topic)) {
      storeName = Version.parseStoreFromKafkaTopicName(topic);
      version = Version.parseVersionFromKafkaTopicName(topic);
    } else {
      storeName = Version.parseStoreFromRealTimeTopic(topic);
    }
    // fetch key schema
    String keySchemaStr = controllerClient.getKeySchema(storeName).getSchemaStr();
    LOGGER.info("The key schema for store: {} : {}", storeName, keySchemaStr);
    StoreInfo storeInfo = controllerClient.getStore(storeName).getStore();
    int partitionCount = storeInfo.getPartitionCount();
    // Parse key string and figure out the right partition
    byte[] serializedKey = serializeKey(keyString, keySchemaStr);

    // Partition assignment is always based on the non-chunked key.
    int assignedPartition = new DefaultVenicePartitioner().getPartitionId(serializedKey, partitionCount);
    LOGGER.info("Assigned partition: {} for key: {}", assignedPartition, keyString);

    if (version != -1) {
      if (storeInfo.getVersion(version).isPresent()) {
        if (storeInfo.getVersion(version).get().isChunkingEnabled()) {
          serializedKey = new KeyWithChunkingSuffixSerializer().serializeNonChunkedKey(serializedKey);
        }
        partitionCount = storeInfo.getVersion(version).get().getPartitionCount();
      } else {
        throw new RuntimeException("Couldn't find version: " + version + " from store: " + storeName);
      }
    }
    if (partitionCount == 0) {
      throw new VeniceException("Invalid partition count: " + partitionCount);
    }
    LOGGER.info("Got partition count: {}", partitionCount);

    long startOffset;
    long endOffset;

    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    PubSubTopicPartition assignedPubSubTopicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), assignedPartition);

    // fetch start and end offset
    startOffset = consumer.offsetForTime(assignedPubSubTopicPartition, startTimestampEpochMs);
    if (endTimestampEpochMs == Long.MAX_VALUE || endTimestampEpochMs > System.currentTimeMillis()) {
      endOffset = consumer.endOffset(assignedPubSubTopicPartition);
    } else {
      endOffset = consumer.offsetForTime(assignedPubSubTopicPartition, endTimestampEpochMs);
    }
    LOGGER.info("Got start offset: {} and end offset: {} for the specified time range", startOffset, endOffset);
    return consume(consumer, assignedPubSubTopicPartition, startOffset, endOffset, progressInterval, serializedKey);
  }

  static long consume(
      PubSubConsumerAdapter consumer,
      PubSubTopicPartition assignedPubSubTopicPartition,
      long startOffset,
      long endOffset,
      long progressInterval,
      byte[] serializedKey) {
    long recordCnt = 0;
    long lastReportRecordCnt = 0;
    consumer.subscribe(assignedPubSubTopicPartition, startOffset);
    boolean done = false;
    while (!done) {
      Map<PubSubTopicPartition, List<DefaultPubSubMessage>> messages = consumer.poll(10000);
      if (messages.isEmpty()) {
        break;
      }
      long lastRecordTimestamp = 0;
      for (DefaultPubSubMessage record: messages.get(assignedPubSubTopicPartition)) {
        if (record.getPosition().getNumericOffset() >= endOffset) {
          done = true;
          break;
        }
        KafkaKey kafkaKey = record.getKey();
        if (Arrays.equals(kafkaKey.getKey(), serializedKey)) {
          KafkaMessageEnvelope value = record.getValue();
          LOGGER.info("Offset: {}, Value: {}", record.getPosition(), value.toString());
        }
        lastRecordTimestamp = record.getPubSubMessageTime();
        recordCnt++;
      }
      if (recordCnt - lastReportRecordCnt >= progressInterval) {
        LOGGER.info(
            "Consumed {} messages from topic partition: {}, and last consumed timestamp: {}",
            recordCnt,
            assignedPubSubTopicPartition,
            new Date(lastRecordTimestamp));
        lastReportRecordCnt = recordCnt;
      }
    }
    return recordCnt;
  }

  public static byte[] serializeKey(String keyString, String keySchemaStr) {
    keyString = QueryTool.removeQuotes(keyString);
    Schema keySchema = AvroCompatibilityHelper.parse(keySchemaStr);
    Object key = QueryTool.convertKey(keyString, keySchema);
    RecordSerializer keySerializer = SerializerDeserializerFactory.getAvroGenericSerializer(keySchema);

    return keySerializer.serialize(key);
  }
}
