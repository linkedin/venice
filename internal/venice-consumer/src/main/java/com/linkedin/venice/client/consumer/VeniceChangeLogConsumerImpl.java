package com.linkedin.venice.client.consumer;

import com.linkedin.venice.client.change.capture.protocol.RecordChangeEvent;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Store;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
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

  private final RecordDeserializer<RecordChangeEvent> recordChangeDeserializer =
      FastSerializerDeserializerFactory.getFastAvroSpecificDeserializer(
          AvroProtocolDefinition.RECORD_CHANGE_EVENT.getCurrentProtocolVersionSchema(),
          RecordChangeEvent.class);

  public VeniceChangeLogConsumerImpl(ChangelogClientConfig clientConfig) {
    this.storeName = clientConfig.getStoreName();
    this.kafkaConsumer = new KafkaConsumer<>(clientConfig.getConsumerProperties());
    Store store = clientConfig.getStoreRepo().getStore(storeName);
    int currentVersion = store.getCurrentVersion();
    this.partitionCount = store.getPartitionCount();
    this.viewClassName = clientConfig.getViewClassName();
    if (viewClassName.equals(ChangeCaptureView.class.getCanonicalName())) {
      this.currentTopic =
          store.getVersion(currentVersion).get().kafkaTopicName() + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX;
    } else {
      this.currentTopic = store.getVersion(currentVersion).get().kafkaTopicName();
    }
    this.schemaReader = clientConfig.getSchemaReader();
    this.subscribedPartitions = new HashSet<>();
    Schema keySchema = schemaReader.getKeySchema();
    this.keyDeserializer = FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(keySchema, keySchema);
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
    Collection<PubSubMessage> pubSubMessages = new ArrayList<>();
    ConsumerRecords<KafkaKey, KafkaMessageEnvelope> consumerRecords = kafkaConsumer.poll(timeoutInMs);
    for (ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord: consumerRecords) {
      PubSubTopicPartition pubSubTopicPartition = getPubSubTopicPartitionFromConsumerRecord(consumerRecord);
      Long offset = consumerRecord.offset();
      int payloadSize = consumerRecord.serializedKeySize() + consumerRecord.serializedValueSize();
      long timestamp = consumerRecord.timestamp();
      if (viewClassName.equals(ChangeCaptureView.class.getCanonicalName())) {
        if (consumerRecord.key().isControlMessage()) {
          ControlMessage controlMessage = (ControlMessage) consumerRecord.value().payloadUnion;
          handleControlMessage(controlMessage);
        } else {
          byte[] keyBytes = consumerRecord.key().getKey();
          K currentKey = keyDeserializer.deserialize(keyBytes);
          MessageType messageType = MessageType.valueOf(consumerRecord.value());
          switch (messageType) {
            case PUT:
              Put put = (Put) consumerRecord.value().payloadUnion;
              PubSubMessage<K, ChangeEvent<V>, Long> pubSubMessage = convertChangeEventToPubSubMessage(
                  put,
                  currentKey,
                  pubSubTopicPartition,
                  offset,
                  timestamp,
                  payloadSize);
              pubSubMessages.add(pubSubMessage);
              break;
          }
        }
      } else {
        if (consumerRecord.key().isControlMessage()) {
          ControlMessage controlMessage = (ControlMessage) consumerRecord.value().payloadUnion;
          handleControlMessage(controlMessage);
        } else {
          byte[] keyBytes = consumerRecord.key().getKey();
          K currentKey = keyDeserializer.deserialize(keyBytes);
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
    }
    return new ImmutablePubSubMessage(currentKey, currentValue, pubSubTopicPartition, offset, timestamp, payloadSize);
  }

  private PubSubMessage<K, ChangeEvent<V>, Long> convertChangeEventToPubSubMessage(
      Put put,
      K currentKey,
      PubSubTopicPartition pubSubTopicPartition,
      Long offset,
      Long timestamp,
      int payloadSize) {
    byte[] valueBytes = put.putValue.array();
    RecordChangeEvent recordChangeEvent = recordChangeDeserializer.deserialize(valueBytes);
    Schema currentValueSchema = schemaReader.getValueSchema(1);
    RecordDeserializer<V> valueDeserializer =
        FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(currentValueSchema, currentValueSchema);
    V currentValue = null;
    if (recordChangeEvent.currentValue != null) {
      currentValue = valueDeserializer.deserialize(recordChangeEvent.currentValue.getValue());
    }
    V previousValue = null;
    if (recordChangeEvent.previousValue != null) {
      previousValue = valueDeserializer.deserialize(recordChangeEvent.previousValue.getValue());
    }
    ChangeEvent<V> changeEvent = new ChangeEvent<>(previousValue, currentValue);
    return new ImmutablePubSubMessage(currentKey, changeEvent, pubSubTopicPartition, offset, timestamp, payloadSize);
  }

  private void handleControlMessage(ControlMessage controlMessage) {
    final ControlMessageType controlMessageType = ControlMessageType.valueOf(controlMessage);
    if (controlMessageType.equals(ControlMessageType.VERSION_SWAP)) {
      VersionSwap versionSwap = (VersionSwap) controlMessage.controlMessageUnion;
      LOGGER.info("Obtain version swap message: " + versionSwap);
      String newServingVersionTopic = versionSwap.newServingVersionTopic.toString();
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
    }
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
