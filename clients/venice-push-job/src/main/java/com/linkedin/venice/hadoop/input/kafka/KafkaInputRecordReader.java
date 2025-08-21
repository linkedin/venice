package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_PUBSUB_INPUT_SECONDARY_COMPARATOR_USE_NUMERIC_RECORD_OFFSET;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_SOURCE_TOPIC_CHUNKING_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUBSUB_INPUT_SECONDARY_COMPARATOR_USE_NUMERIC_RECORD_OFFSET;

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
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * We borrowed some idea from the open-sourced attic-crunch lib:
 * https://github.com/apache/attic-crunch/blob/master/crunch-kafka/src/main/java/org/apache/crunch/kafka/record/KafkaRecordReader.java
 *
 * This class is used to read data off a Kafka topic partition.
 * It will return the key bytes as unchanged, and extract the following fields and wrap them
 * up as {@link KafkaInputMapperValue} as the value:
 * 1. Value bytes.
 * 2. Schema Id.
 * 3. Offset.
 * 4. Value type, which could be 'PUT' or 'DELETE'.
 */
public class KafkaInputRecordReader implements RecordReader<KafkaInputMapperKey, KafkaInputMapperValue>, AutoCloseable {
  private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[0]);
  private static final Logger LOGGER = LogManager.getLogger(KafkaInputRecordReader.class);
  private static final Long CONSUMER_POLL_TIMEOUT = TimeUnit.SECONDS.toMillis(1); // 1 second
  private static final long LOG_RECORD_INTERVAL = 100000; // 100K
  /**
   * Retry when the poll is returning empty result.
   */
  private static final int CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES = 12;
  private static final long EMPTY_POLL_SLEEP_TIME_MS = TimeUnit.SECONDS.toMillis(5);

  private static final PubSubTopicRepository PUBSUB_TOPIC_REPOSITORY = new PubSubTopicRepository();

  private final PubSubConsumerAdapter consumer;
  private final PubSubTopicPartition topicPartition;
  private final PubSubPosition startingPosition;
  private final PubSubPosition endingPosition;
  private final boolean isSourceVersionChunkingEnabled;
  private final Schema keySchema;
  private ChunkKeyValueTransformer chunkKeyValueTransformer;
  private PubSubPosition currentPosition;

  private final long totalRecordsToRead;
  private final int splitIndex;
  private final long splitStartingRecordIndex;
  private final boolean shouldUseLocallyBuiltIndexAsOffset;
  private long recordsReadSoFar = 0;

  /**
   * Iterator pointing to the current messages fetched from the Kafka topic partition.
   */
  private Iterator<DefaultPubSubMessage> recordIterator;

  private final DataWriterTaskTracker taskTracker;
  /**
   * Whether the consumer being used in this class is owned or not, and this is used to decide
   * whether the consumer should be closed when closing {@link KafkaInputRecordReader}.
   */
  private boolean ownedConsumer = true;

  private float lastLoggedProgress = -1.0f;

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
    this.ownedConsumer = false;
    if (!(split instanceof KafkaInputSplit)) {
      throw new VeniceException("InputSplit for RecordReader is not valid split type.");
    }
    KafkaInputSplit inputSplit = (KafkaInputSplit) split;
    this.consumer = consumer;
    PubSubPartitionSplit pubSubPartitionSplit = inputSplit.getSplit();
    this.topicPartition = pubSubPartitionSplit.getPubSubTopicPartition();
    this.startingPosition = pubSubPartitionSplit.getStartPubSubPosition();
    this.currentPosition = pubSubPartitionSplit.getStartPubSubPosition();
    this.endingPosition = pubSubPartitionSplit.getEndPubSubPosition();
    this.totalRecordsToRead = pubSubPartitionSplit.getNumberOfRecords();
    this.splitIndex = pubSubPartitionSplit.getSplitIndex();
    this.splitStartingRecordIndex = pubSubPartitionSplit.getStartIndex();
    this.shouldUseLocallyBuiltIndexAsOffset = !job.getBoolean(
        PUBSUB_INPUT_SECONDARY_COMPARATOR_USE_NUMERIC_RECORD_OFFSET,
        DEFAULT_PUBSUB_INPUT_SECONDARY_COMPARATOR_USE_NUMERIC_RECORD_OFFSET);
    this.isSourceVersionChunkingEnabled = job.getBoolean(KAFKA_INPUT_SOURCE_TOPIC_CHUNKING_ENABLED, false);
    String keySchemaString = job.get(KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP);
    if (keySchemaString == null) {
      throw new VeniceException("Expect a value for the config property: " + KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP);
    }
    this.keySchema = Schema.parse(keySchemaString);
    // In case the Kafka Consumer is passed by the caller, we will unsubscribe all the existing subscriptions first
    if (!ownedConsumer) {
      this.consumer.batchUnsubscribe(this.consumer.getAssignment());
    }
    this.consumer.subscribe(topicPartition, currentPosition, true);
    this.taskTracker = taskTracker;
    LOGGER.info(
        "KafkaInputRecordReader started for split index: {} topicPartition: {} starting offset: {} ending offset: {} total records: {} isUsingLocallyBuiltIndexAsOffset: {}",
        this.splitIndex,
        this.topicPartition,
        this.startingPosition,
        this.endingPosition,
        this.totalRecordsToRead,
        this.shouldUseLocallyBuiltIndexAsOffset);
  }

  /**
   * This function will skip all the Control Messages right now.
   */
  @Override
  public boolean next(KafkaInputMapperKey key, KafkaInputMapperValue value) throws IOException {
    DefaultPubSubMessage pubSubMessage;
    while (hasPendingData()) {
      try {
        loadRecords();
      } catch (InterruptedException e) {
        throw new IOException(
            "Got interrupted while loading records from topic partition: " + topicPartition + " with current offset: "
                + currentPosition);
      }
      pubSubMessage = recordIterator.hasNext() ? recordIterator.next() : null;
      if (pubSubMessage != null) {
        currentPosition = pubSubMessage.getPosition();
        if (shouldUseLocallyBuiltIndexAsOffset) {
          long offset = splitStartingRecordIndex + recordsReadSoFar;
          key.setOffset(offset);
          value.setOffset(offset);
        } else {
          key.setOffset(currentPosition.getNumericOffset());
          value.setOffset(currentPosition.getNumericOffset());
        }
        recordsReadSoFar++;

        KafkaKey kafkaKey = pubSubMessage.getKey();
        KafkaMessageEnvelope kafkaMessageEnvelope = pubSubMessage.getValue();
        if (kafkaKey.isControlMessage()) {
          // Skip all the control messages
          continue;
        }

        MessageType messageType = MessageType.valueOf(kafkaMessageEnvelope);
        if (isSourceVersionChunkingEnabled) {
          RawKeyBytesAndChunkedKeySuffix rawKeyAndChunkedKeySuffix =
              splitCompositeKey(kafkaKey.getKey(), messageType, getSchemaIdFromValue(kafkaMessageEnvelope));
          key.key = rawKeyAndChunkedKeySuffix.getRawKeyBytes();

          value.chunkedKeySuffix = rawKeyAndChunkedKeySuffix.getChunkedKeySuffixBytes();
        } else {
          key.key = ByteBuffer.wrap(kafkaKey.getKey(), 0, kafkaKey.getKeyLength());
        }
        switch (messageType) {
          case PUT:
            Put put = (Put) kafkaMessageEnvelope.payloadUnion;
            value.valueType = MapperValueType.PUT;
            value.value = put.putValue;
            value.schemaId = put.schemaId;
            value.replicationMetadataPayload = put.replicationMetadataPayload;
            value.replicationMetadataVersionId = put.replicationMetadataVersionId;
            break;
          case DELETE:
            Delete delete = (Delete) kafkaMessageEnvelope.payloadUnion;
            value.valueType = MapperValueType.DELETE;
            value.value = EMPTY_BYTE_BUFFER;
            value.replicationMetadataPayload = delete.replicationMetadataPayload;
            value.replicationMetadataVersionId = delete.replicationMetadataVersionId;
            value.schemaId = delete.schemaId;
            break;
          default:
            throw new IOException(
                "Unexpected '" + messageType + "' message from Kafka topic partition: " + topicPartition
                    + " with offset: " + pubSubMessage.getPosition());
        }
        if (taskTracker != null) {
          taskTracker.trackPutOrDeleteRecord();
          long recordCount = taskTracker.getTotalPutOrDeleteRecordsCount();
          if (recordCount > 0 && recordCount % LOG_RECORD_INTERVAL == 0) {
            LOGGER.info(
                "KafkaInputRecordReader for TopicPartition: {} has processed {} records",
                this.topicPartition,
                recordCount);
          }
        }
        return true;
      } else {
        // We have pending data, but we are unable to fetch any records so throw an exception and stop the job
        throw new IOException(
            "Unable to read additional data from Kafka. See logs for details. Partition " + topicPartition
                + " Current Offset: " + currentPosition + " End Offset: " + endingPosition);
      }
    }
    return false;
  }

  private int getSchemaIdFromValue(KafkaMessageEnvelope kafkaMessageEnvelope) throws IOException {
    MessageType messageType = MessageType.valueOf(kafkaMessageEnvelope);
    switch (messageType) {
      case PUT:
        Put put = (Put) kafkaMessageEnvelope.payloadUnion;
        return put.schemaId;
      case DELETE:
        /**
         * The chunk cleanup message will use schema id: {@link com.linkedin.venice.serialization.avro.AvroProtocolDefinition#CHUNK},
         * so we will extract the schema id from the payload, instead of -1.
         */
        Delete delete = (Delete) kafkaMessageEnvelope.payloadUnion;
        return delete.schemaId;
      default:
        throw new IOException("Unexpected '" + messageType + "' message from Kafka topic partition: " + topicPartition);
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
    return recordsReadSoFar - 1; // Return the last read record logical position
  }

  @Override
  public void close() {
    if (ownedConsumer) {
      this.consumer.close();
    }
  }

  @Override
  public float getProgress() {
    // not most accurate but gives reasonable estimate
    if (totalRecordsToRead <= 0) {
      return 1.0f;
    }
    float progress = Math.min(1.0f, ((float) recordsReadSoFar / totalRecordsToRead));

    // Only log progress if it has changed by at least 1% or this is the first time
    if (lastLoggedProgress < 0.0f || Math.abs(progress - lastLoggedProgress) >= 0.01f) {
      LOGGER.info(
          "KafkaInputRecordReader for TopicPartition: {} has read {} records out of total {} records, progress: {}%",
          this.topicPartition,
          recordsReadSoFar,
          totalRecordsToRead,
          String.format("%.2f", progress * 100));
      lastLoggedProgress = progress;
    }

    return progress;
  }

  protected boolean hasPendingData() {
    if (recordsReadSoFar > totalRecordsToRead) {
      return false;
    }
    // Use positionDifference instead of compare. If a topic partition has no more data,
    // currentPosition will never reach or exceed endingPosition. By definition,
    // endingPosition is set to "last record offset + 1".
    return consumer.positionDifference(topicPartition, endingPosition, currentPosition) > 1;
  }

  /**
   * Loads new records into the record iterator
   */
  private void loadRecords() throws InterruptedException {
    if ((recordIterator == null) || !recordIterator.hasNext()) {
      Map<PubSubTopicPartition, List<DefaultPubSubMessage>> messages = new HashMap<>();
      int retry = 0;
      while (retry++ < CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES) {
        messages = consumer.poll(CONSUMER_POLL_TIMEOUT);
        if (!messages.isEmpty()) {
          break;
        }
        Thread.sleep(EMPTY_POLL_SLEEP_TIME_MS);
      }
      if (messages.isEmpty()) {
        StringBuilder sb = new StringBuilder();
        sb.append("Consumer#poll still returns empty result after retrying ")
            .append(CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES)
            .append(" times, ")
            .append("topic partition: ")
            .append(topicPartition)
            .append(" and current offset: ")
            .append(currentPosition);
        throw new VeniceException(sb.toString());
      }

      recordIterator = Utils.iterateOnMapOfLists(messages);
    }
  }
}
