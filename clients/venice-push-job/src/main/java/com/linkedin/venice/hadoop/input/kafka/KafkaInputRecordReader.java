package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.utils.Utils.EMPTY_BYTE_BUFFER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_PUBSUB_INPUT_SECONDARY_COMPARATOR_USE_LOCAL_LOGICAL_INDEX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_SOURCE_TOPIC_CHUNKING_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUBSUB_INPUT_SECONDARY_COMPARATOR_USE_LOCAL_LOGICAL_INDEX;

import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.chunking.ChunkKeyValueTransformer;
import com.linkedin.venice.chunking.ChunkKeyValueTransformerImpl;
import com.linkedin.venice.chunking.RawKeyBytesAndChunkedKeySuffix;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperKey;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.avro.MapperValueType;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import com.linkedin.venice.vpj.pubsub.input.PubSubSplitIterator;
import com.linkedin.venice.vpj.pubsub.input.PubSubSplitIterator.PubSubInputRecord;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Reads data from a Kafka-backed PubSub topic partition and converts each message
 * into {@link KafkaInputMapperKey}/{@link KafkaInputMapperValue}. All generic iteration,
 * polling, end-bound checks, and progress tracking are delegated to {@link PubSubSplitIterator}.
 */
public class KafkaInputRecordReader implements RecordReader<KafkaInputMapperKey, KafkaInputMapperValue>, AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(KafkaInputRecordReader.class);
  private static final long LOG_RECORD_INTERVAL = 100_000L;
  private static final PubSubTopicRepository PUBSUB_TOPIC_REPOSITORY = new PubSubTopicRepository();
  private final PubSubTopicPartition topicPartition;
  private final PubSubSplitIterator pubSubSplitIterator;
  private final boolean isSourceVersionChunkingEnabled;
  private final Schema keySchema;
  private ChunkKeyValueTransformer chunkKeyValueTransformer;
  private final DataWriterTaskTracker taskTracker;
  private boolean ownedConsumer = true;

  public KafkaInputRecordReader(InputSplit split, JobConf job, DataWriterTaskTracker taskTracker) {
    this(split, job, taskTracker, createConsumer(job));
  }

  private static PubSubConsumerAdapter createConsumer(JobConf job) {
    VeniceProperties consumerProps = KafkaInputUtils.getConsumerProperties(job);
    return PubSubClientsFactory.createConsumerFactory(consumerProps)
        .create(
            new PubSubConsumerAdapterContext.Builder().setConsumerName("KafkaInputRecordReader-" + job.getJobName())
                .setVeniceProperties(consumerProps)
                .setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry.fromPropertiesOrDefault(consumerProps))
                .setPubSubTopicRepository(PUBSUB_TOPIC_REPOSITORY)
                .setPubSubMessageDeserializer(
                    new PubSubMessageDeserializer(
                        KafkaInputUtils.getKafkaValueSerializer(job),
                        new LandFillObjectPool<>(KafkaMessageEnvelope::new),
                        new LandFillObjectPool<>(KafkaMessageEnvelope::new)))
                .build());
  }

  public KafkaInputRecordReader(
      InputSplit split,
      JobConf job,
      DataWriterTaskTracker taskTracker,
      PubSubConsumerAdapter consumer) {

    if (!(split instanceof KafkaInputSplit)) {
      throw new VeniceException("InputSplit for RecordReader is not valid split type.");
    }
    KafkaInputSplit inputSplit = (KafkaInputSplit) split;

    this.isSourceVersionChunkingEnabled = job.getBoolean(KAFKA_INPUT_SOURCE_TOPIC_CHUNKING_ENABLED, false);
    String keySchemaString = job.get(KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP);
    if (keySchemaString == null) {
      throw new VeniceException("Expect a value for the config property: " + KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP);
    }
    this.keySchema = Schema.parse(keySchemaString);
    this.taskTracker = taskTracker;
    // Consumer ownership: if caller provides consumer, we don't own it
    this.ownedConsumer = (consumer == null);
    consumer = (consumer != null) ? consumer : createConsumer(job);
    PubSubPartitionSplit pubSubSplit = inputSplit.getSplit();
    this.topicPartition = pubSubSplit.getPubSubTopicPartition();
    boolean useLogicalIndexOffset = job.getBoolean(
        PUBSUB_INPUT_SECONDARY_COMPARATOR_USE_LOCAL_LOGICAL_INDEX,
        DEFAULT_PUBSUB_INPUT_SECONDARY_COMPARATOR_USE_LOCAL_LOGICAL_INDEX);

    // Build iterator directly from the split
    this.pubSubSplitIterator = new PubSubSplitIterator(consumer, pubSubSplit, useLogicalIndexOffset);

    LOGGER.info(
        "KafkaInputRecordReader started for split index: {} topicPartition: {} start: {} end(exclusive): {} "
            + "total records: {} useLogicalIndexOffset: {}",
        pubSubSplit.getSplitIndex(),
        this.topicPartition,
        pubSubSplit.getStartPubSubPosition(),
        pubSubSplit.getEndPubSubPosition(),
        pubSubSplit.getNumberOfRecords(),
        useLogicalIndexOffset);
  }

  @Override
  public boolean next(KafkaInputMapperKey mapperKey, KafkaInputMapperValue mapperValue) throws IOException {
    // Pull the next decoded unit from the iterator. Null => no more data.
    PubSubInputRecord rec = pubSubSplitIterator.next();
    if (rec == null) {
      return false;
    }

    DefaultPubSubMessage pubSubMessage = rec.getPubSubMessage();
    long offset = rec.getOffset();

    KafkaKey pubSubMessageKey = pubSubMessage.getKey();
    KafkaMessageEnvelope pubSubMessageValue = pubSubMessage.getValue();

    // Set offsets on output key/value
    mapperKey.setOffset(offset);
    mapperValue.setOffset(offset);

    MessageType messageType = MessageType.valueOf(pubSubMessageValue);

    // Key handling (chunked or raw)
    if (isSourceVersionChunkingEnabled) {
      RawKeyBytesAndChunkedKeySuffix parts =
          splitCompositeKey(pubSubMessageKey.getKey(), messageType, getSchemaIdFromValue(pubSubMessageValue));
      mapperKey.key = parts.getRawKeyBytes();
      mapperValue.chunkedKeySuffix = parts.getChunkedKeySuffixBytes();
    } else {
      mapperKey.key = ByteBuffer.wrap(pubSubMessageKey.getKey(), 0, pubSubMessageKey.getKeyLength());
    }

    // Value handling
    switch (messageType) {
      case PUT: {
        Put put = (Put) pubSubMessageValue.payloadUnion;
        mapperValue.valueType = MapperValueType.PUT;
        mapperValue.value = put.putValue;
        mapperValue.schemaId = put.schemaId;
        mapperValue.replicationMetadataPayload = put.replicationMetadataPayload;
        mapperValue.replicationMetadataVersionId = put.replicationMetadataVersionId;
        break;
      }
      case DELETE: {
        Delete delete = (Delete) pubSubMessageValue.payloadUnion;
        mapperValue.valueType = MapperValueType.DELETE;
        mapperValue.value = EMPTY_BYTE_BUFFER;
        mapperValue.replicationMetadataPayload = delete.replicationMetadataPayload;
        mapperValue.replicationMetadataVersionId = delete.replicationMetadataVersionId;
        mapperValue.schemaId = delete.schemaId;
        break;
      }
      default:
        throw new IOException(
            "Unexpected message type: " + messageType + " in " + topicPartition + " at " + pubSubMessage.getPosition());
    }

    // Optional task-level tracking/logging
    if (taskTracker != null) {
      taskTracker.trackPutOrDeleteRecord();
      long count = taskTracker.getTotalPutOrDeleteRecordsCount();
      if (count > 0 && count % LOG_RECORD_INTERVAL == 0) {
        LOGGER.info("Processed {} records in {}", count, topicPartition);
      }
    }

    return true;
  }

  private int getSchemaIdFromValue(KafkaMessageEnvelope kafkaMessageEnvelope) throws IOException {
    MessageType messageType = MessageType.valueOf(kafkaMessageEnvelope);
    switch (messageType) {
      case PUT:
        return ((Put) kafkaMessageEnvelope.payloadUnion).schemaId;
      case DELETE:
        return ((Delete) kafkaMessageEnvelope.payloadUnion).schemaId;
      default:
        throw new IOException("Unexpected '" + messageType + "' message from topic partition: " + topicPartition);
    }
  }

  private RawKeyBytesAndChunkedKeySuffix splitCompositeKey(
      byte[] compositeKeyBytes,
      MessageType messageType,
      final int schemaId) {
    if (this.chunkKeyValueTransformer == null) {
      this.chunkKeyValueTransformer = new ChunkKeyValueTransformerImpl(keySchema);
    }
    ChunkKeyValueTransformer.KeyType keyType = ChunkKeyValueTransformer.getKeyType(messageType, schemaId);
    return chunkKeyValueTransformer.splitChunkedKey(compositeKeyBytes, keyType);
  }

  @Override
  public KafkaInputMapperKey createKey() {
    return new KafkaInputMapperKey();
  }

  @Override
  public KafkaInputMapperValue createValue() {
    return new KafkaInputMapperValue();
  }

  @Override
  public long getPos() {
    // Return the last-read logical position
    return pubSubSplitIterator.recordsRead() - 1;
  }

  @Override
  public void close() {
    // Close only if we own the consumer; otherwise the caller manages its lifecycle.
    if (ownedConsumer && pubSubSplitIterator != null) {
      pubSubSplitIterator.close();
    }
  }

  @Override
  public float getProgress() {
    // Delegated progress + throttled logging occurs inside iterator
    return pubSubSplitIterator.getProgress();
  }

  @VisibleForTesting
  boolean hasPendingData() {
    // Check if the iterator has more data to read
    return pubSubSplitIterator.hasNext();
  }
}
