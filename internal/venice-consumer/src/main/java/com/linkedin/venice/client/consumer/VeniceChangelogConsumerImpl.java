package com.linkedin.venice.client.consumer;

import static com.linkedin.venice.schema.rmd.RmdConstants.*;

import com.linkedin.venice.client.change.capture.protocol.RecordChangeEvent;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.validation.UnsupportedMessageTypeException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.views.ChangeCaptureView;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceChangelogConsumerImpl<K, V> implements VeniceChangelogConsumer<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(VeniceChangelogConsumerImpl.class);
  private final String storeName;
  private final Consumer<KafkaKey, KafkaMessageEnvelope> kafkaConsumer;
  private String currentTopic;
  private final int partitionCount;
  private final SchemaReader schemaReader;
  private final String viewClassName;
  private final Set<Integer> subscribedPartitions;

  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private final RecordDeserializer<K> keyDeserializer;

  private final Map<Integer, List<Long>> currentVersionHighWatermarks = new HashMap<>();

  private final D2ControllerClient d2ControllerClient;

  private final ReplicationMetadataSchemaRepository replicationMetadataSchemaRepository;

  private boolean isReadFromChangeCaptureTopic;

  private final RecordDeserializer<RecordChangeEvent> recordChangeDeserializer =
      FastSerializerDeserializerFactory.getFastAvroSpecificDeserializer(
          AvroProtocolDefinition.RECORD_CHANGE_EVENT.getCurrentProtocolVersionSchema(),
          RecordChangeEvent.class);

  private Map<Integer, List<Long>> currentVersionTempHighWatermarks = new HashMap<>();

  public VeniceChangelogConsumerImpl(
      ChangelogClientConfig changelogClientConfig,
      Consumer<KafkaKey, KafkaMessageEnvelope> kafkaConsumer) {
    this.kafkaConsumer = kafkaConsumer;
    this.storeName = changelogClientConfig.getStoreName();
    this.d2ControllerClient = changelogClientConfig.getD2ControllerClient();
    StoreResponse storeResponse = changelogClientConfig.getD2ControllerClient().getStore(storeName);
    if (storeResponse.isError()) {
      throw new VeniceException(
          "Failed to get store info for store: " + storeName + " with error: " + storeResponse.getError());
    }
    StoreInfo store = storeResponse.getStore();
    int currentVersion = store.getCurrentVersion();
    this.partitionCount = store.getPartitionCount();
    this.viewClassName = changelogClientConfig.getViewClassName();
    if (viewClassName.equals(ChangeCaptureView.class.getCanonicalName())) {
      this.currentTopic =
          Version.composeKafkaTopic(storeName, currentVersion) + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX;
      this.isReadFromChangeCaptureTopic = true;
    } else {
      this.currentTopic = Version.composeKafkaTopic(storeName, currentVersion);
      this.isReadFromChangeCaptureTopic = false;
    }
    this.replicationMetadataSchemaRepository = new ReplicationMetadataSchemaRepository(d2ControllerClient);
    this.schemaReader = changelogClientConfig.getSchemaReader();
    this.subscribedPartitions = new HashSet<>();
    Schema keySchema = schemaReader.getKeySchema();
    this.keyDeserializer = FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(keySchema, keySchema);
    LOGGER.info(
        "Start a change log consumer client for store: {}, with partition count: {} and view class: {} ",
        storeName,
        partitionCount,
        viewClassName);
  }

  public VeniceChangelogConsumerImpl(ChangelogClientConfig changelogClientConfig) {
    this(changelogClientConfig, new KafkaConsumer<>(changelogClientConfig.getConsumerProperties()));
  }

  @Override
  public CompletableFuture<Void> subscribe(Set<Integer> partitions) {
    return CompletableFuture.supplyAsync(() -> {
      Set<TopicPartition> topicPartitionSet = kafkaConsumer.assignment();
      List<TopicPartition> topicPartitionList = getPartitionListToSubscribe(partitions, topicPartitionSet);
      kafkaConsumer.assign(topicPartitionList);
      kafkaConsumer.seekToBeginning(topicPartitionList);
      subscribedPartitions.addAll(partitions);
      return null;
    });
  }

  @Override
  public CompletableFuture<Void> subscribeAll() {
    Set<Integer> allPartitions = new HashSet<>();
    for (int partition = 0; partition < partitionCount; partition++) {
      allPartitions.add(partition);
    }
    return this.subscribe(allPartitions);
  }

  private List<TopicPartition> getPartitionListToSubscribe(
      Set<Integer> partitions,
      Set<TopicPartition> topicPartitionSet) {
    List<TopicPartition> topicPartitionList = new ArrayList<>(topicPartitionSet);
    for (Integer partition: partitions) {
      TopicPartition topicPartition = new TopicPartition(currentTopic, partition);
      if (!topicPartitionSet.contains(topicPartition)) {
        topicPartitionList.add(topicPartition);
      }
    }
    return topicPartitionList;
  }

  @Override
  public void unsubscribe(Set<Integer> partitions) {
    for (Integer partition: partitions) {
      TopicPartition topicPartition = new TopicPartition(currentTopic, partition);
      Set<TopicPartition> topicPartitionSet = kafkaConsumer.assignment();
      if (topicPartitionSet.contains(topicPartition)) {
        List<TopicPartition> topicPartitionList = new ArrayList<>(topicPartitionSet);
        if (topicPartitionList.remove(topicPartition)) {
          kafkaConsumer.assign(topicPartitionList);
        }
      }
      subscribedPartitions.remove(topicPartition.partition());
    }
  }

  @Override
  public void unsubscribeAll() {
    Set<Integer> allPartitions = new HashSet<>();
    for (int partition = 0; partition < partitionCount; partition++) {
      allPartitions.add(partition);
    }
    this.unsubscribe(allPartitions);
  }

  @Override
  public Collection<PubSubMessage> poll(long timeoutInMs) {
    List<PubSubMessage> pubSubMessages = new ArrayList<>();
    ConsumerRecords<KafkaKey, KafkaMessageEnvelope> consumerRecords = kafkaConsumer.poll(timeoutInMs);
    for (ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord: consumerRecords) {
      PubSubTopicPartition pubSubTopicPartition = getPubSubTopicPartitionFromConsumerRecord(consumerRecord);
      Long offset = consumerRecord.offset();
      int payloadSize = consumerRecord.serializedKeySize() + consumerRecord.serializedValueSize();
      long timestamp = consumerRecord.timestamp();
      if (consumerRecord.key().isControlMessage()) {
        ControlMessage controlMessage = (ControlMessage) consumerRecord.value().payloadUnion;
        ControlMessageType controlMessageType = ControlMessageType.valueOf(controlMessage);
        // Stop processing messages from current version once we get version swap message.
        if (controlMessageType.equals(ControlMessageType.VERSION_SWAP)) {
          VersionSwap versionSwap = (VersionSwap) controlMessage.controlMessageUnion;
          handleVersionControlMessage(versionSwap, pubSubTopicPartition);
          return pubSubMessages;
        } else if (controlMessageType.equals(ControlMessageType.END_OF_PUSH)) {
          handleEndOfPushControlMessage(pubSubTopicPartition);
          return pubSubMessages;
        }
      } else {
        byte[] keyBytes = consumerRecord.key().getKey();
        K currentKey = keyDeserializer.deserialize(keyBytes);
        if (isReadFromChangeCaptureTopic) {
          MessageType messageType = MessageType.valueOf(consumerRecord.value());
          if (messageType.equals(MessageType.PUT)) {
            Put put = (Put) consumerRecord.value().payloadUnion;
            byte[] valueBytes = put.putValue.array();
            RecordChangeEvent recordChangeEvent = recordChangeDeserializer.deserialize(valueBytes);
            if (!filterRecordByVersionSwapHighWatermarks(
                recordChangeEvent.replicationCheckpointVector,
                pubSubTopicPartition)) {
              PubSubMessage<K, ChangeEvent<V>, Long> pubSubMessage = convertChangeEventToPubSubMessage(
                  recordChangeEvent,
                  currentKey,
                  pubSubTopicPartition,
                  offset,
                  timestamp,
                  payloadSize);
              if (viewClassName.equals(ChangeCaptureView.class.getCanonicalName())) {
                pubSubMessages.add(pubSubMessage);
              } else {
                pubSubMessages.add(
                    new ImmutablePubSubMessage(
                        pubSubMessage.getKey(),
                        pubSubMessage.getValue().currentValue,
                        pubSubTopicPartition,
                        offset,
                        timestamp,
                        payloadSize));
              }
            }
          } else {
            throw new UnsupportedMessageTypeException(
                "Unrecognized message type for change event message: " + messageType);
          }
        } else {
          PubSubMessage<K, V, Long> pubSubMessage = convertRecordToPubSubMessage(
              consumerRecord.value(),
              currentKey,
              pubSubTopicPartition,
              offset,
              timestamp,
              payloadSize);
          pubSubMessages.add(pubSubMessage);
        }
      }
    }
    return pubSubMessages;
  }

  private PubSubMessage<K, V, Long> convertRecordToPubSubMessage(
      KafkaMessageEnvelope kafkaMessageEnvelope,
      K currentKey,
      PubSubTopicPartition pubSubTopicPartition,
      Long offset,
      Long timestamp,
      int payloadSize) {
    MessageType messageType = MessageType.valueOf(kafkaMessageEnvelope.messageType);
    V currentValue;
    switch (messageType) {
      case PUT:
        Put put = (Put) kafkaMessageEnvelope.payloadUnion;
        Schema valueSchema = schemaReader.getValueSchema(put.schemaId);
        RecordDeserializer<V> valueDeserializer =
            FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(valueSchema, valueSchema);
        currentValue = valueDeserializer.deserialize(put.putValue);
        MultiSchemaResponse.Schema replicationMetadataSchema = replicationMetadataSchemaRepository
            .getReplicationMetadataSchemaById(storeName, put.replicationMetadataVersionId);
        RecordDeserializer<GenericRecord> deserializer = SerializerDeserializerFactory
            .getAvroGenericDeserializer(Schema.parse(replicationMetadataSchema.getSchemaStr()));
        GenericRecord replicationMetadataRecord = deserializer.deserialize(put.replicationMetadataPayload);
        GenericData.Array replicationCheckpointVector =
            (GenericData.Array) replicationMetadataRecord.get(REPLICATION_CHECKPOINT_VECTOR_FIELD);
        List<Long> offsetVector = new ArrayList<>();
        for (int i = 0; i < replicationCheckpointVector.size(); i++) {
          offsetVector.add((Long) replicationCheckpointVector.get(i));
        }
        currentVersionTempHighWatermarks.putIfAbsent(pubSubTopicPartition.getPartitionNumber(), offsetVector);
        currentVersionTempHighWatermarks.computeIfPresent(pubSubTopicPartition.getPartitionNumber(), (k, v) -> {
          for (int i = 0; i < offsetVector.size(); i++) {
            if (i < v.size()) {
              if (offsetVector.get(i) > v.get(i)) {
                v.set(i, offsetVector.get(i));
              }
            } else {
              v.add(offsetVector.get(i));
            }
          }
          return v;
        });
        System.out.println(currentVersionTempHighWatermarks);
        break;
      case DELETE:
        currentValue = null;
        break;
      default:
        throw new UnsupportedMessageTypeException("Unrecognized message type " + messageType);
    }
    return new ImmutablePubSubMessage<>(currentKey, currentValue, pubSubTopicPartition, offset, timestamp, payloadSize);
  }

  private PubSubMessage<K, ChangeEvent<V>, Long> convertChangeEventToPubSubMessage(
      RecordChangeEvent recordChangeEvent,
      K currentKey,
      PubSubTopicPartition pubSubTopicPartition,
      Long offset,
      Long timestamp,
      int payloadSize) {
    V currentValue = null;
    if (recordChangeEvent.currentValue != null && recordChangeEvent.currentValue.getSchemaId() > 0) {
      currentValue = deserializeValueFromBytes(
          recordChangeEvent.currentValue.getValue(),
          recordChangeEvent.currentValue.getSchemaId());
    }
    V previousValue = null;
    if (recordChangeEvent.previousValue != null && recordChangeEvent.previousValue.getSchemaId() > 0) {
      previousValue = deserializeValueFromBytes(
          recordChangeEvent.previousValue.getValue(),
          recordChangeEvent.previousValue.getSchemaId());
    }
    ChangeEvent<V> changeEvent = new ChangeEvent<>(previousValue, currentValue);
    return new ImmutablePubSubMessage<>(currentKey, changeEvent, pubSubTopicPartition, offset, timestamp, payloadSize);
  }

  private V deserializeValueFromBytes(ByteBuffer byteBuffer, int valueSchemaId) {
    Schema currentValueSchema = schemaReader.getValueSchema(valueSchemaId);
    RecordDeserializer<V> valueDeserializer =
        FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(currentValueSchema, currentValueSchema);
    if (byteBuffer != null) {
      return valueDeserializer.deserialize(byteBuffer);
    }
    return null;
  }

  private boolean filterRecordByVersionSwapHighWatermarks(
      List<Long> recordCheckpointVector,
      PubSubTopicPartition pubSubTopicPartition) {
    int partitionId = pubSubTopicPartition.getPartitionNumber();
    if (recordCheckpointVector != null && currentVersionHighWatermarks.containsKey(partitionId)) {
      List<Long> partitionCurrentVersionHighWatermarks = currentVersionHighWatermarks.get(partitionId);
      if (recordCheckpointVector.size() > partitionCurrentVersionHighWatermarks.size()) {
        return false;
      }
      // Only filter the record when all regions fall behind.
      for (int i = 0; i < recordCheckpointVector.size(); i++) {
        if (recordCheckpointVector.get(i) > partitionCurrentVersionHighWatermarks.get(i)) {
          return false;
        }
      }
      return true;
    }
    // Has not met version swap message after client initialization.
    return false;
  }

  private void handleVersionControlMessage(VersionSwap versionSwap, PubSubTopicPartition pubSubTopicPartition) {
    LOGGER.info("Obtain version swap message: {}", versionSwap);
    String newServingVersionTopic = versionSwap.newServingVersionTopic.toString();
    currentVersionHighWatermarks
        .computeIfAbsent(pubSubTopicPartition.getPartitionNumber(), k -> versionSwap.getLocalHighWatermarks());
    if (isReadFromChangeCaptureTopic) {
      switchToNewTopic(newServingVersionTopic + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);
    } else {
      switchToNewTopic(newServingVersionTopic);
    }
  }

  private void switchToNewTopic(String newTopic) {
    Set<Integer> partitions = new HashSet<>(subscribedPartitions);
    unsubscribe(subscribedPartitions);
    currentTopic = newTopic;
    try {
      subscribe(partitions).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new VeniceException("Subscribe to new topic:" + newTopic + " is not successful, error: " + e);
    }
  }

  private void handleEndOfPushControlMessage(PubSubTopicPartition pubSubTopicPartition) {
    isReadFromChangeCaptureTopic = true;
    int partitionId = pubSubTopicPartition.getPartitionNumber();
    currentVersionHighWatermarks.put(partitionId, currentVersionTempHighWatermarks.get(partitionId));
    // Jump to change capture topic.
    switchToNewTopic(currentTopic + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);
  }

  private PubSubTopicPartition getPubSubTopicPartitionFromConsumerRecord(
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord) {
    return new PubSubTopicPartitionImpl(
        pubSubTopicRepository.getTopic(consumerRecord.topic()),
        consumerRecord.partition());
  }

  @Override
  public void close() {
    this.unsubscribeAll();
    kafkaConsumer.close();
  }

  public static class ChangeEvent<T> {
    private final T previousValue;
    private final T currentValue;

    public ChangeEvent(T previousValue, T currentValue) {
      this.previousValue = previousValue;
      this.currentValue = currentValue;
    }

    public T getPreviousValue() {
      return previousValue;
    }

    public T getCurrentValue() {
      return currentValue;
    }
  }
}
