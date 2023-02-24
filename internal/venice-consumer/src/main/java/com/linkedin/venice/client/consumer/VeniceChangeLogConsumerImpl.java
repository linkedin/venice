package com.linkedin.venice.client.consumer;

import com.linkedin.venice.client.change.capture.protocol.RecordChangeEvent;
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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceChangeLogConsumerImpl<K, V> implements VeniceChangelogConsumer<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(VeniceChangeLogConsumerImpl.class);
  private String storeName;
  private Consumer<KafkaKey, KafkaMessageEnvelope> kafkaConsumer;
  private String currentTopic;
  private int partitionCount;
  private SchemaReader schemaReader;
  private String viewClassName;
  private Set<Integer> subscribedPartitions;

  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  private RecordDeserializer<K> keyDeserializer;

  private Map<Integer, List<Long>> currentVersionHighWatermarks = new HashMap<>();

  private final RecordDeserializer<RecordChangeEvent> recordChangeDeserializer =
      FastSerializerDeserializerFactory.getFastAvroSpecificDeserializer(
          AvroProtocolDefinition.RECORD_CHANGE_EVENT.getCurrentProtocolVersionSchema(),
          RecordChangeEvent.class);

  public VeniceChangeLogConsumerImpl(
      ChangelogClientConfig clientConfig,
      Consumer<KafkaKey, KafkaMessageEnvelope> kafkaConsumer) {
    this.kafkaConsumer = kafkaConsumer;
    this.storeName = clientConfig.getStoreName();
    StoreResponse storeResponse = clientConfig.getD2ControllerClient().getStore(storeName);
    StoreInfo store = storeResponse.getStore();
    int currentVersion = store.getCurrentVersion();
    this.partitionCount = store.getPartitionCount();
    this.viewClassName = clientConfig.getViewClassName();
    if (viewClassName.equals(ChangeCaptureView.class.getCanonicalName())) {
      this.currentTopic =
          Version.composeKafkaTopic(storeName, currentVersion) + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX;
    } else {
      this.currentTopic = Version.composeKafkaTopic(storeName, currentVersion);
    }
    this.schemaReader = clientConfig.getSchemaReader();
    this.subscribedPartitions = new HashSet<>();
    Schema keySchema = schemaReader.getKeySchema();
    this.keyDeserializer = FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(keySchema, keySchema);
  }

  public VeniceChangeLogConsumerImpl(ChangelogClientConfig clientConfig) {
    this(clientConfig, new KafkaConsumer<>(clientConfig.getConsumerProperties()));
  }

  @Override
  public CompletableFuture<Void> subscribe(Set<Integer> partitions) {
    CompletableFuture<Void> completableFuture = CompletableFuture.supplyAsync(() -> {
      Set<TopicPartition> topicPartitionSet = kafkaConsumer.assignment();
      List<TopicPartition> topicPartitionList = getPartitionListToSubscribe(partitions, topicPartitionSet);
      kafkaConsumer.assign(topicPartitionList);
      kafkaConsumer.seekToBeginning(topicPartitionList);
      subscribedPartitions.addAll(partitions);
      return null;
    });
    return completableFuture;
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
        boolean isVersionSwap = handleControlMessage(controlMessage, pubSubTopicPartition);
        // Stop processing messages from current version once we get version swap message.
        if (isVersionSwap) {
          return pubSubMessages;
        }
      } else {
        byte[] keyBytes = consumerRecord.key().getKey();
        K currentKey = keyDeserializer.deserialize(keyBytes);
        if (viewClassName.equals(ChangeCaptureView.class.getCanonicalName())) {
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
              pubSubMessages.add(pubSubMessage);
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
    V currentValue = null;
    switch (messageType) {
      case PUT:
        Put put = (Put) kafkaMessageEnvelope.payloadUnion;
        Schema valueSchema = schemaReader.getValueSchema(put.schemaId);
        RecordDeserializer<V> valueDeserializer =
            FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(valueSchema, valueSchema);
        currentValue = valueDeserializer.deserialize(put.putValue);
        break;
      case DELETE:
        currentValue = null;
        break;
      default:
        throw new UnsupportedMessageTypeException("Unrecognized message type " + messageType);
    }
    return new ImmutablePubSubMessage(currentKey, currentValue, pubSubTopicPartition, offset, timestamp, payloadSize);
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
    return new ImmutablePubSubMessage(currentKey, changeEvent, pubSubTopicPartition, offset, timestamp, payloadSize);
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

  private boolean handleControlMessage(ControlMessage controlMessage, PubSubTopicPartition pubSubTopicPartition) {
    final ControlMessageType controlMessageType = ControlMessageType.valueOf(controlMessage);
    boolean isVersionSwap = false;
    if (controlMessageType.equals(ControlMessageType.VERSION_SWAP)) {
      VersionSwap versionSwap = (VersionSwap) controlMessage.controlMessageUnion;
      LOGGER.info("Obtain version swap message: {}", versionSwap);
      String newServingVersionTopic = versionSwap.newServingVersionTopic.toString();
      currentVersionHighWatermarks
          .computeIfAbsent(pubSubTopicPartition.getPartitionNumber(), k -> versionSwap.getLocalHighWatermarks());
      Set<Integer> partitions = new HashSet<>();
      for (Integer partition: subscribedPartitions) {
        partitions.add(partition);
      }
      unsubscribe(subscribedPartitions);
      if (viewClassName.equals(ChangeCaptureView.class.getCanonicalName())) {
        currentTopic = newServingVersionTopic + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX;
      } else {
        currentTopic = newServingVersionTopic;
      }
      try {
        subscribe(partitions).get();
      } catch (InterruptedException e) {
        throw new VeniceException(e);
      } catch (ExecutionException e) {
        throw new VeniceException(e);
      }
      isVersionSwap = true;
    }
    return isVersionSwap;
  }

  private PubSubTopicPartition getPubSubTopicPartitionFromConsumerRecord(ConsumerRecord consumerRecord) {
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
    private T previousValue;
    private T currentValue;

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
