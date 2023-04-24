package com.linkedin.venice.hadoop.input.kafka;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.MRJobCounterHelper;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperKey;
import com.linkedin.venice.hadoop.input.kafka.avro.KafkaInputMapperValue;
import com.linkedin.venice.hadoop.input.kafka.avro.MapperValueType;
import com.linkedin.venice.hadoop.input.kafka.chunk.ChunkKeyValueTransformer;
import com.linkedin.venice.hadoop.input.kafka.chunk.ChunkKeyValueTransformerImpl;
import com.linkedin.venice.hadoop.input.kafka.chunk.RawKeyBytesAndChunkedKeySuffix;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.consumer.ApacheKafkaConsumerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.kafka.KafkaPubSubMessageDeserializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import com.linkedin.venice.writer.VeniceWriter;
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
import org.apache.hadoop.mapred.Reporter;
import org.apache.kafka.common.TopicPartition;
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
  public static final String KIF_RECORD_READER_KAFKA_CONFIG_PREFIX = "kif.record.reader.kafka.";

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
  private final TopicPartition topicPartition;
  private final PubSubTopicPartition pubSubTopicPartition;
  private final long maxNumberOfRecords;
  private final long startingOffset;
  private long currentOffset;
  private final long endingOffset;
  private final boolean isChunkingEnabled;
  private final Schema keySchema;
  private ChunkKeyValueTransformer chunkKeyValueTransformer;
  /**
   * Iterator pointing to the current messages fetched from the Kafka topic partition.
   */
  private Iterator<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> recordIterator;

  private final Reporter reporter;

  public KafkaInputRecordReader(InputSplit split, JobConf job, Reporter reporter) {
    this(
        split,
        job,
        reporter,
        new ApacheKafkaConsumerAdapterFactory().create(
            KafkaInputUtils.getConsumerProperties(job),
            false,
            new KafkaPubSubMessageDeserializer(
                new OptimizedKafkaValueSerializer(),
                new LandFillObjectPool<>(KafkaMessageEnvelope::new),
                new LandFillObjectPool<>(KafkaMessageEnvelope::new)),
            null),
        PUBSUB_TOPIC_REPOSITORY);
  }

  /** For unit tests */
  KafkaInputRecordReader(
      InputSplit split,
      JobConf job,
      Reporter reporter,
      PubSubConsumerAdapter consumer,
      PubSubTopicRepository pubSubTopicRepository) {
    if (!(split instanceof KafkaInputSplit)) {
      throw new VeniceException("InputSplit for RecordReader is not valid split type.");
    }
    KafkaInputSplit inputSplit = (KafkaInputSplit) split;
    this.consumer = consumer;
    this.topicPartition = inputSplit.getTopicPartition();
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic(topicPartition.topic());
    this.pubSubTopicPartition = new PubSubTopicPartitionImpl(pubSubTopic, topicPartition.partition());
    this.startingOffset = inputSplit.getStartingOffset();
    this.currentOffset = inputSplit.getStartingOffset() - 1;
    this.endingOffset = inputSplit.getEndingOffset();
    this.isChunkingEnabled = job.getBoolean(VeniceWriter.ENABLE_CHUNKING, false);
    String keySchemaString = job.get(VenicePushJob.KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP);
    if (keySchemaString == null) {
      throw new VeniceException(
          "Expect a value for the config property: " + VenicePushJob.KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP);
    }
    this.keySchema = Schema.parse(keySchemaString);
    /**
     * Not accurate since the topic partition could be log compacted.
     */
    this.maxNumberOfRecords = endingOffset - startingOffset;
    this.consumer.subscribe(pubSubTopicPartition, currentOffset);
    this.reporter = reporter;
    LOGGER.info(
        "KafkaInputRecordReader started for TopicPartition: {} starting offset: {} ending offset: {}",
        this.topicPartition,
        this.startingOffset,
        this.endingOffset);
  }

  /**
   * This function will skip all the Control Messages right now.
   */
  @Override
  public boolean next(KafkaInputMapperKey key, KafkaInputMapperValue value) throws IOException {
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> pubSubMessage;
    while (hasPendingData()) {
      try {
        loadRecords();
      } catch (InterruptedException e) {
        throw new IOException(
            "Got interrupted while loading records from topic partition: " + topicPartition + " with current offset: "
                + currentOffset);
      }
      pubSubMessage = recordIterator.hasNext() ? recordIterator.next() : null;
      if (pubSubMessage != null) {
        currentOffset = pubSubMessage.getOffset();

        KafkaKey kafkaKey = pubSubMessage.getKey();
        KafkaMessageEnvelope kafkaMessageEnvelope = pubSubMessage.getValue();

        if (kafkaKey.isControlMessage()) {
          // Skip all the control messages
          continue;
        }

        MessageType messageType = MessageType.valueOf(kafkaMessageEnvelope);

        key.offset = pubSubMessage.getOffset();
        if (isChunkingEnabled) {
          RawKeyBytesAndChunkedKeySuffix rawKeyAndChunkedKeySuffix =
              splitCompositeKey(kafkaKey.getKey(), messageType, getSchemaIdFromValue(kafkaMessageEnvelope));
          key.key = rawKeyAndChunkedKeySuffix.getRawKeyBytes();

          value.chunkedKeySuffix = rawKeyAndChunkedKeySuffix.getChunkedKeySuffixBytes();
        } else {
          key.key = ByteBuffer.wrap(kafkaKey.getKey(), 0, kafkaKey.getKeyLength());
        }
        value.offset = pubSubMessage.getOffset();
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
                    + " with offset: " + pubSubMessage.getOffset());
        }
        if (reporter != null && !reporter.equals(Reporter.NULL)) {
          MRJobCounterHelper.incrTotalPutOrDeleteRecordCount(reporter, 1);
          long recordCount = MRJobCounterHelper.getTotalPutOrDeleteRecordsCount(reporter);
          if (recordCount % LOG_RECORD_INTERVAL == 0) {
            LOGGER.info(
                "KafkaInputRecordReader for TopicPartition: {} has processed {} records",
                this.topicPartition,
                recordCount);
          }
        }
        return true;
      } else {
        // We have pending data but we are unable to fetch any records so throw an exception and stop the job
        throw new IOException(
            "Unable to read additional data from Kafka. See logs for details. Partition " + topicPartition
                + " Current Offset: " + currentOffset + " End Offset: " + endingOffset);
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
        return -1;
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
    ChunkKeyValueTransformer.KeyType keyType;
    if (schemaId == AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion()) {
      keyType = ChunkKeyValueTransformer.KeyType.WITH_VALUE_CHUNK;
    } else if (schemaId == AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion()) {
      keyType = ChunkKeyValueTransformer.KeyType.WITH_CHUNK_MANIFEST;
    } else if (schemaId > 0 || messageType == MessageType.DELETE) {
      keyType = ChunkKeyValueTransformer.KeyType.WITH_FULL_VALUE;
    } else {
      throw new VeniceException("Cannot categorize key type with schema ID: " + schemaId);
    }
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
    return currentOffset;
  }

  @Override
  public void close() {
    this.consumer.close();
  }

  @Override
  public float getProgress() {
    // not most accurate but gives reasonable estimate
    return ((float) (currentOffset - startingOffset + 1)) / maxNumberOfRecords;
  }

  private boolean hasPendingData() {
    /**
     * Offset range is exclusive at the end which means the ending offset is one higher
     * than the actual physical last offset
     */
    return currentOffset < endingOffset - 1;
  }

  /**
   * Loads new records into the record iterator
   */
  private void loadRecords() throws InterruptedException {
    if ((recordIterator == null) || !recordIterator.hasNext()) {
      Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> messages = new HashMap<>();
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
            .append(currentOffset);
        throw new VeniceException(sb.toString());
      }

      recordIterator = Utils.iterateOnMapOfLists(messages);
    }
  }
}
