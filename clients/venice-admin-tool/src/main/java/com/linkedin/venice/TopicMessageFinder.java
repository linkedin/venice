package com.linkedin.venice;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.store.QueryTool;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
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
    KeyPartitionInfo keyPartitionInfo = findPartitionIdForKey(
        controllerClient,
        storeName,
        version,
        keyString,
        controllerClient.getKeySchema(storeName).getSchemaStr());
    int partitionCount = keyPartitionInfo.getPartitionCount();
    byte[] serializedKey = keyPartitionInfo.getSerializedKey();
    // Partition assignment is always based on the non-chunked key.
    int assignedPartition = keyPartitionInfo.getPartitionId();
    LOGGER.info("Assigned partition: {} for key: {}", assignedPartition, keyString);
    StoreInfo storeInfo = keyPartitionInfo.getStoreInfo();
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

    PubSubPosition startPosition;
    PubSubPosition endPosition;

    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    PubSubTopicPartition assignedPubSubTopicPartition =
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(topic), assignedPartition);

    // fetch start and end offset
    startPosition = consumer.getPositionByTimestamp(assignedPubSubTopicPartition, startTimestampEpochMs);
    if (endTimestampEpochMs == Long.MAX_VALUE || endTimestampEpochMs > System.currentTimeMillis()) {
      endPosition = consumer.endPosition(assignedPubSubTopicPartition);
    } else {
      endPosition = consumer.getPositionByTimestamp(assignedPubSubTopicPartition, endTimestampEpochMs);
    }
    LOGGER.info("Got start position: {} and end position: {} for the specified time range", startPosition, endPosition);
    return consume(consumer, assignedPubSubTopicPartition, startPosition, endPosition, progressInterval, serializedKey);
  }

  static long consume(
      PubSubConsumerAdapter consumer,
      PubSubTopicPartition assignedPubSubTopicPartition,
      PubSubPosition startPosition,
      PubSubPosition endPosition,
      long progressInterval,
      byte[] serializedKey) {
    long recordCnt = 0;
    long lastReportRecordCnt = 0;
    consumer.subscribe(assignedPubSubTopicPartition, startPosition, true);
    boolean done = false;
    while (!done) {
      Map<PubSubTopicPartition, List<DefaultPubSubMessage>> messages = consumer.poll(10000);
      if (messages.isEmpty()) {
        break;
      }
      long lastRecordTimestamp = 0;
      for (DefaultPubSubMessage record: messages.get(assignedPubSubTopicPartition)) {
        if (consumer.positionDifference(assignedPubSubTopicPartition, record.getPosition(), endPosition) >= 0) {
          done = true;
          break;
        }
        KafkaKey kafkaKey = record.getKey();
        if (Arrays.equals(kafkaKey.getKey(), serializedKey)) {
          KafkaMessageEnvelope value = record.getValue();
          LOGGER.info("Position: {}, Value: {}", record.getPosition(), value.toString());
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

  protected static KeyPartitionInfo findPartitionIdForKey(
      ControllerClient controllerClient,
      String storeName,
      int versionNumber,
      String key,
      String keySchemaStr) {
    StoreResponse storeResponse = controllerClient.getStore(storeName);
    if (storeResponse == null) {
      throw new VeniceNoStoreException("Store " + storeName + " does not exist.");
    }
    StoreInfo storeInfo = storeResponse.getStore();
    if (storeInfo == null) {
      throw new VeniceNoStoreException("Store " + storeName + " does not exist.");
    }

    Optional<Version> versionInfo = storeInfo.getVersion(versionNumber);

    int partitionCount;
    PartitionerConfig partitionerConfig;
    if (versionInfo.isPresent()) {
      Version version = versionInfo.get();
      LOGGER.info(
          "Found store: {} version: {}. Will use partitioner config: {} and partition count: {} from this version",
          storeName,
          versionNumber,
          version.getPartitionerConfig(),
          version.getPartitionCount());
      partitionerConfig = version.getPartitionerConfig();
      partitionCount = version.getPartitionCount();
    } else {
      LOGGER.info(
          "Store: {} version: {} not found. Will use partitioner config: {} from store level config",
          storeName,
          versionNumber,
          storeInfo.getPartitionerConfig());
      partitionerConfig = storeInfo.getPartitionerConfig();
      partitionCount = storeInfo.getPartitionCount();
    }

    if (partitionCount <= 0) {
      LOGGER.error("Partition count for store: {} is not set.", storeName);
      throw new VeniceException("Partition count for store: " + storeName + " is not set.");
    }

    Properties params = new Properties();
    params.putAll(partitionerConfig.getPartitionerParams());
    VeniceProperties partitionerProperties = new VeniceProperties(params);
    VenicePartitioner partitioner =
        PartitionUtils.getVenicePartitioner(partitionerConfig.getPartitionerClass(), partitionerProperties);
    LOGGER.info("The key schema for store: {} : {}", storeName, keySchemaStr);

    byte[] keyBytes = TopicMessageFinder.serializeKey(key, keySchemaStr);
    int partitionId = partitioner.getPartitionId(keyBytes, partitionCount);
    LOGGER.info("Partition ID for key: {} in store: {} is: {}", key, storeName, partitionId);

    return new KeyPartitionInfo(storeInfo, keyBytes, partitionId, partitionCount);
  }

  protected static class KeyPartitionInfo {
    private final StoreInfo storeInfo;
    private final byte[] serializedKey;
    private final int partitionId;
    private final int partitionCount;

    KeyPartitionInfo(StoreInfo storeInfo, byte[] serializedKey, int partitionId, int partitionCount) {
      this.storeInfo = storeInfo;
      this.serializedKey = serializedKey;
      this.partitionId = partitionId;
      this.partitionCount = partitionCount;
    }

    int getPartitionId() {
      return partitionId;
    }

    byte[] getSerializedKey() {
      return serializedKey;
    }

    int getPartitionCount() {
      return partitionCount;
    }

    StoreInfo getStoreInfo() {
      return storeInfo;
    }
  }
}
