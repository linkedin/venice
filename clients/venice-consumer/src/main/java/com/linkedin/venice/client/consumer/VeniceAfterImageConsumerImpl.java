package com.linkedin.venice.client.consumer;

import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD;

import com.linkedin.davinci.storage.chunking.GenericChunkingAdapter;
import com.linkedin.davinci.storage.chunking.SpecificRecordChunkingAdapter;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.exceptions.validation.UnsupportedMessageTypeException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.StartOfPush;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.views.ChangeCaptureView;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceAfterImageConsumerImpl<K, V> extends VeniceChangelogConsumerImpl<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(VeniceAfterImageConsumerImpl.class);

  private boolean isReadFromChangeCaptureTopic;

  private final CompressorFactory compressorFactory = new CompressorFactory();
  private VeniceCompressor currentCompressor = NO_OP_COMPRESSOR;

  public VeniceAfterImageConsumerImpl(
      ChangelogClientConfig changelogClientConfig,
      Consumer<KafkaKey, KafkaMessageEnvelope> kafkaConsumer) {
    super(changelogClientConfig, kafkaConsumer);
    this.currentTopic = Version.composeKafkaTopic(storeName, storeCurrentVersion);
    Class<V> valueClass = changelogClientConfig.getInnerClientConfig().getSpecificValueClass();
    if (valueClass != null) {
      // If a value class is supplied, we'll use a Specific record adapter
      CHUNKING_ADAPTER = new SpecificRecordChunkingAdapter(valueClass);
    } else {
      CHUNKING_ADAPTER = GenericChunkingAdapter.INSTANCE;
    }
  }

  public VeniceAfterImageConsumerImpl(ChangelogClientConfig changelogClientConfig) {
    this(changelogClientConfig, new KafkaConsumer<>(changelogClientConfig.getConsumerProperties()));
  }

  @Override
  public Collection<PubSubMessage<K, ChangeEvent<V>, Long>> poll(long timeoutInMs) {
    List<PubSubMessage<K, ChangeEvent<V>, Long>> pubSubMessages = new ArrayList<>();
    ConsumerRecords<KafkaKey, KafkaMessageEnvelope> consumerRecords = kafkaConsumer.poll(timeoutInMs);
    for (ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord: consumerRecords) {
      PubSubTopicPartition pubSubTopicPartition = getPubSubTopicPartitionFromConsumerRecord(consumerRecord);
      long offset = consumerRecord.offset();
      int payloadSize = consumerRecord.serializedKeySize() + consumerRecord.serializedValueSize();
      long timestamp = consumerRecord.timestamp();
      if (consumerRecord.key().isControlMessage()) {
        ControlMessage controlMessage = (ControlMessage) consumerRecord.value().payloadUnion;
        if (handleControlMessage(controlMessage, pubSubTopicPartition)) {
          return pubSubMessages;
        }
      } else {
        byte[] keyBytes = consumerRecord.key().getKey();
        if (isReadFromChangeCaptureTopic) {
          Optional<PubSubMessage<K, ChangeEvent<V>, Long>> pubSubMessage =
              convertConsumerRecordToPubSubChangeEventMessage(consumerRecord, pubSubTopicPartition);
          if (pubSubMessage.isPresent()) {
            pubSubMessages.add(
                new ImmutablePubSubMessage<>(
                    pubSubMessage.get().getKey(),
                    new ChangeEvent<>(null, pubSubMessage.get().getValue().getCurrentValue()),
                    pubSubTopicPartition,
                    offset,
                    timestamp,
                    keyBytes.length + currentValuePayloadSize[pubSubTopicPartition.getPartitionNumber()]));
          }
        } else {
          PubSubMessage<K, ChangeEvent<V>, Long> pubSubMessage = convertRecordToPubSubMessage(
              consumerRecord.value(),
              keyBytes,
              pubSubTopicPartition,
              offset,
              timestamp,
              payloadSize);
          // Filter out events which we could not assemble into a fully formed pubsub message (this screens out chunked
          // events).
          if (pubSubMessage != null) {
            pubSubMessages.add(pubSubMessage);
          }
        }
      }
    }
    return pubSubMessages;
  }

  // TODO: Find a better way to avoid data gap between version topic and change capture topic due to log compaction.
  private boolean handleControlMessage(ControlMessage controlMessage, PubSubTopicPartition pubSubTopicPartition) {
    ControlMessageType controlMessageType = ControlMessageType.valueOf(controlMessage);
    if (controlMessageType.equals(ControlMessageType.START_OF_PUSH)) {
      StartOfPush startOfPush = (StartOfPush) controlMessage.controlMessageUnion;
      byte[] dictionary = null;
      if (startOfPush.compressionDictionary != null) {
        dictionary = startOfPush.compressionDictionary.array();
      }
      // TODO: This relies on consuming the beginning of the version topic. This is what some libraries do anyway under
      // the hood, but it seems clumsy here. Should refactor
      currentCompressor = compressorFactory.createVersionSpecificCompressorIfNotExist(
          CompressionStrategy.valueOf(startOfPush.compressionStrategy),
          pubSubTopicPartition.getPubSubTopic().getName(),
          dictionary);
    }
    if (controlMessageType.equals(ControlMessageType.END_OF_PUSH)) {
      isReadFromChangeCaptureTopic = true;
      int partitionId = pubSubTopicPartition.getPartitionNumber();
      LOGGER.info(
          "Obtain End of Push message and current local high watermarks: {}",
          currentVersionTempHighWatermarks.get(partitionId));
      if (currentVersionTempHighWatermarks.containsKey(partitionId)) {
        currentVersionHighWatermarks.put(partitionId, currentVersionTempHighWatermarks.get(partitionId));
      }
      // Jump to change capture topic.
      switchToNewTopic(currentTopic + ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);
      return true;
    } else {
      return handleVersionSwapControlMessage(controlMessage, pubSubTopicPartition);
    }
  }

  private PubSubMessage<K, ChangeEvent<V>, Long> convertRecordToPubSubMessage(
      KafkaMessageEnvelope kafkaMessageEnvelope,
      byte[] currentKey,
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
        try {
          currentValue = (V) bufferAndAssembleRecordChangeEvent(
              pubSubTopicPartition,
              put.getSchemaId(),
              currentKey,
              currentCompressor.decompress(put.getPutValue()).array(),
              offset,
              CHUNKING_ADAPTER,
              Lazy.of(() -> FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(valueSchema, valueSchema)),
              put.schemaId,
              readOnlySchemaRepository);
          if (currentValue == null) {
            // This was an event which we had to buffer, so don't return a result. We'll terminate early here as we
            // should
            // only do book keeping on consumed offset watermarks for those events which we return to the client.
            return null;
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        if (put.replicationMetadataVersionId > 0) {
          MultiSchemaResponse.Schema replicationMetadataSchema = replicationMetadataSchemaRepository
              .getReplicationMetadataSchemaById(storeName, put.replicationMetadataVersionId);
          RecordDeserializer<GenericRecord> deserializer = SerializerDeserializerFactory
              .getAvroGenericDeserializer(Schema.parse(replicationMetadataSchema.getSchemaStr()));
          GenericRecord replicationMetadataRecord = deserializer.deserialize(put.replicationMetadataPayload);
          GenericData.Array replicationCheckpointVector =
              (GenericData.Array) replicationMetadataRecord.get(REPLICATION_CHECKPOINT_VECTOR_FIELD);
          List<Long> offsetVector = new ArrayList<>();
          for (Object o: replicationCheckpointVector) {
            offsetVector.add((Long) o);
          }
          int partitionId = pubSubTopicPartition.getPartitionNumber();
          if (!currentVersionTempHighWatermarks.containsKey(partitionId)) {
            currentVersionTempHighWatermarks.put(partitionId, offsetVector);
          } else {
            List<Long> previousHighWatermarks = currentVersionTempHighWatermarks.get(partitionId);
            for (int i = 0; i < offsetVector.size(); i++) {
              if (i < previousHighWatermarks.size()) {
                if (offsetVector.get(i) > previousHighWatermarks.get(i)) {
                  previousHighWatermarks.set(i, offsetVector.get(i));
                }
              } else {
                previousHighWatermarks.add(offsetVector.get(i));
              }
            }
          }
        }
        break;
      case DELETE:
        currentValue = null;
        break;
      default:
        throw new UnsupportedMessageTypeException("Unrecognized message type " + messageType);
    }
    return new ImmutablePubSubMessage<>(
        keyDeserializer.deserialize(currentKey),
        new ChangeEvent<>(null, currentValue),
        pubSubTopicPartition,
        offset,
        timestamp,
        payloadSize);
  }

}
