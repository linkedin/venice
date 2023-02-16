package com.linkedin.venice;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.store.QueryTool;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.ApacheKafkaConsumer;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.consumer.PubSubConsumer;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is used to find messages for a given key from a specified topic.
 */
public class TopicMessageFinder {
  private static final Logger LOGGER = LogManager.getLogger(TopicMessageFinder.class);

  public static void find(
      ControllerClient controllerClient,
      Properties consumerProps,
      String topic,
      String keyString,
      long startTimestamp,
      long endTimestamp,
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

    int assignedPartition = new DefaultVenicePartitioner().getPartitionId(serializedKey, partitionCount);
    LOGGER.info("Assigned partition: {} for key: {}", assignedPartition, keyString);

    TopicPartition topicPartition = new TopicPartition(topic, assignedPartition);
    long startOffset;
    long endOffset;
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

    try (KafkaConsumer consumer = new KafkaConsumer(consumerProps)) {
      // fetch start and end offset
      OffsetAndTimestamp otForStartTimestamp = ((Map<TopicPartition, OffsetAndTimestamp>) consumer
          .offsetsForTimes(Collections.singletonMap(topicPartition, startTimestamp))).get(topicPartition);
      startOffset = otForStartTimestamp == null ? -1 : otForStartTimestamp.offset();
      OffsetAndTimestamp otForEndTimestamp = ((Map<TopicPartition, OffsetAndTimestamp>) consumer
          .offsetsForTimes(Collections.singletonMap(topicPartition, endTimestamp))).get(topicPartition);
      endOffset = otForEndTimestamp != null
          ? otForEndTimestamp.offset()
          : (Long) consumer.endOffsets(Collections.singletonList(topicPartition)).get(topicPartition);
    }
    LOGGER.info("Got start offset: {} and end offset: {} for the specified time range", startOffset, endOffset);

    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    PubSubTopicPartition assignedPubSubTopicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), assignedPartition);
    consume(
        new ApacheKafkaConsumer(consumerProps),
        assignedPubSubTopicPartition,
        startOffset,
        endOffset,
        progressInterval,
        serializedKey);
  }

  /** For unit tests */
  static void consume(
      PubSubConsumer c,
      PubSubTopicPartition assignedPubSubTopicPartition,
      long startOffset,
      long endOffset,
      long progressInterval,
      byte[] serializedKey) {
    try (PubSubConsumer consumer = c) {
      long recordCnt = 0;
      long lastReportRecordCnt = 0;
      consumer.subscribe(assignedPubSubTopicPartition, startOffset);
      boolean done = false;
      KafkaKeySerializer keySerializer = new KafkaKeySerializer();
      KafkaValueSerializer valueSerializer = new OptimizedKafkaValueSerializer();
      while (!done) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(10000);
        if (records.isEmpty()) {
          break;
        }
        long lastRecordTimestamp = 0;
        for (ConsumerRecord<byte[], byte[]> record: records) {
          if (record.offset() >= endOffset) {
            done = true;
            break;
          }
          KafkaKey kafkaKey = keySerializer.deserialize(null, record.key());
          if (Arrays.equals(kafkaKey.getKey(), serializedKey)) {
            KafkaMessageEnvelope value = valueSerializer.deserialize(null, record.value());
            LOGGER.info("Offset: {}, Value: {}", record.offset(), value.toString());
          }
          lastRecordTimestamp = record.timestamp();
        }
        recordCnt += records.count();
        if (recordCnt - lastReportRecordCnt >= progressInterval) {
          LOGGER.info(
              "Consumed {} messages from topic partition: {}, and last consumed timestamp: {}",
              recordCnt,
              assignedPubSubTopicPartition,
              new Date(lastRecordTimestamp));
          lastReportRecordCnt = recordCnt;
        }
      }
    }
  }

  public static byte[] serializeKey(String keyString, String keySchemaStr) {
    keyString = QueryTool.removeQuotes(keyString);
    Schema keySchema = AvroCompatibilityHelper.parse(keySchemaStr);
    Object key = QueryTool.convertKey(keyString, keySchema);
    RecordSerializer keySerializer = SerializerDeserializerFactory.getAvroGenericSerializer(keySchema);

    return keySerializer.serialize(key);
  }
}
