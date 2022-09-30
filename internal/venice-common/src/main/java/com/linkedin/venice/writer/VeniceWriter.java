package com.linkedin.venice.writer;

import static com.linkedin.venice.ConfigKeys.INSTANCE_ID;
import static com.linkedin.venice.ConfigKeys.LISTENER_PORT;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.annotation.Threadsafe;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.RecordTooLargeException;
import com.linkedin.venice.exceptions.TopicAuthorizationVeniceException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.EndOfIncrementalPush;
import com.linkedin.venice.kafka.protocol.EndOfSegment;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.LeaderMetadata;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.StartOfIncrementalPush;
import com.linkedin.venice.kafka.protocol.StartOfPush;
import com.linkedin.venice.kafka.protocol.StartOfSegment;
import com.linkedin.venice.kafka.protocol.TopicSwitch;
import com.linkedin.venice.kafka.protocol.Update;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.validation.Segment;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.storage.protocol.ChunkId;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.apache.avro.specific.FixedSize;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Class which acts as the primary writer API.
 */
@Threadsafe
public class VeniceWriter<K, V, U> extends AbstractVeniceWriter<K, V, U> {

  // log4j logger
  private final Logger logger;

  // Config names
  public static final String VENICE_WRITER_CONFIG_PREFIX = "venice.writer.";
  public static final String CLOSE_TIMEOUT_MS = VENICE_WRITER_CONFIG_PREFIX + "close.timeout.ms";
  public static final String CHECK_SUM_TYPE = VENICE_WRITER_CONFIG_PREFIX + "checksum.type";
  public static final String ENABLE_CHUNKING = VENICE_WRITER_CONFIG_PREFIX + "chunking.enabled";
  public static final String MAX_ATTEMPTS_WHEN_TOPIC_MISSING =
      VENICE_WRITER_CONFIG_PREFIX + "max.attemps.when.topic.missing";
  public static final String SLEEP_TIME_MS_WHEN_TOPIC_MISSING =
      VENICE_WRITER_CONFIG_PREFIX + "sleep.time.ms.when.topic.missing";
  /**
   * A negative value or 0 will disable the feature.
   */
  public static final String MAX_ELAPSED_TIME_FOR_SEGMENT_IN_MS =
      VENICE_WRITER_CONFIG_PREFIX + "max.elapsed.time.for.segment.in.ms";

  /**
   * Chunk size. Default: {@value DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES}
   *
   * N.B.: This must be configured in relation to the following configs:
   *
   * 1. Kafka broker's global setting: `message.max.bytes` (default is 1000012, or ~976 KB)
   * 2. Kafka broker's topic setting: `max.message.bytes` (default is 1000012, or ~976 KB)
   * 3. Kafka producer's setting: `max.request.size` (default is 1048576)
   */
  public static final String MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES =
      VENICE_WRITER_CONFIG_PREFIX + "max.size.for.user.payload.per.message.in.bytes";

  // Config value defaults

  /**
   * 950 KB for user payload should be conservative enough.
   */
  public static final int DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES = 950 * 1024;

  /**
   * This controls the Kafka producer's close timeout.
   */
  public static final int DEFAULT_CLOSE_TIMEOUT_MS = 30 * Time.MS_PER_SECOND;

  /**
   * Default checksum type. N.B.: Only MD5 (and having no checksums) supports checkpointing mid-checksum.
   */
  public static final String DEFAULT_CHECK_SUM_TYPE = CheckSumType.MD5.name();

  /**
   * Default number of attempts when trying to produce to a Kafka topic and an exception is caught saying
   * the topic is missing.
   */
  public static final int DEFAULT_MAX_ATTEMPTS_WHEN_TOPIC_MISSING = 30;

  /**
   * Sleep time in between retry attempts when a topic is missing. Under default settings, the writer will
   * stall 30 * 10 seconds = 300 seconds = 5 minutes (assuming the failure from Kafka is instantaneous,
   * which of course it isn't, therefore this is a minimum stall time, not a max).
   */
  public static final int DEFAULT_SLEEP_TIME_MS_WHEN_TOPIC_MISSING = 10 * Time.MS_PER_SECOND;

  /**
   * The default value of the "upstreamOffset" field in avro record {@link LeaderMetadata}.
   *
   * Even though we have set the default value for "upstreamOffset" field as -1, the initial value for the long field
   * "upstreamOffset" is still 0 when we construct a LeaderMetadata record. Default field values are primarily used
   * when reading records that don't have those fields, typically when we deserialize a record from older version to
   * newer version.
   */
  public static final long DEFAULT_UPSTREAM_OFFSET =
      (long) AvroCompatibilityHelper.getSpecificDefaultValue(LeaderMetadata.SCHEMA$.getField("upstreamOffset"));

  public static final int DEFAULT_UPSTREAM_KAFKA_CLUSTER_ID =
      (int) AvroCompatibilityHelper.getSpecificDefaultValue(LeaderMetadata.SCHEMA$.getField("upstreamKafkaClusterId"));

  /**
   * A static counter shared by all VeniceWriter instances to track the number of active VeniceWriter
   */
  public static final AtomicLong OPEN_VENICE_WRITER_COUNT = new AtomicLong(0);

  /**
   * A static counter shared by all VeniceWriter instances to track the number of VeniceWriter that fails to close
   */
  public static final AtomicLong VENICE_WRITER_CLOSE_FAILED_COUNT = new AtomicLong(0);

  private static final long DEFAULT_MAX_ELAPSED_TIME_FOR_SEGMENT_IN_MS =
      TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

  /**
   * This sentinel value indicates that the venice samza apps do not support the logical timestamp.
   */
  public static final long VENICE_DEFAULT_LOGICAL_TS = -1;

  /**
   * This sentinel value indicates that the venice samza apps have not provided the logical timestamp.
   */
  public static final long APP_DEFAULT_LOGICAL_TS = -2;

  /**
   * This sentinel value indicates that metadata version id is not present.
   */
  public static final int VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID = -1;

  /**
   * This sentinel value indicates that value schema id is not present.
   */
  public static final int VENICE_DEFAULT_VALUE_SCHEMA_ID = -1;

  private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);

  public static final LeaderMetadataWrapper DEFAULT_LEADER_METADATA_WRAPPER =
      new LeaderMetadataWrapper(DEFAULT_UPSTREAM_OFFSET, DEFAULT_UPSTREAM_KAFKA_CLUSTER_ID);

  // Immutable state
  private final VeniceKafkaSerializer<K> keySerializer;
  private final VeniceKafkaSerializer<V> valueSerializer;
  private final VeniceKafkaSerializer<U> writeComputeSerializer;
  private final KafkaProducerWrapper producer;
  private final GUID producerGUID;
  private final Time time;
  private final VenicePartitioner partitioner;
  private final int numberOfPartitions;
  private final int closeTimeOut;
  private final CheckSumType checkSumType;
  private final int maxSizeForUserPayloadPerMessageInBytes;
  private final int maxAttemptsWhenTopicMissing;
  private final long sleepTimeMsWhenTopicMissing;
  private final long maxElapsedTimeForSegmentInMs;
  /**
   * Map of partition to {@link Segment}, which keeps track of all segment-related state.
   * IMPORTANT NOTE: Never remove previously maintained segment from the VeniceWriter, it doesn't make sense to reuse
   *                 the VeniceWriter to send new data with a reset producer metadata, follower replicas would think
   *                 new data are "duplicated" because they are using stale producer metadata (e.g. segment number 0
   *                 and sequence number 0)
   */
  private final Map<Integer, Segment> segmentsMap = new VeniceConcurrentHashMap<>();
  /**
   * Map of partition to its segment creation time in milliseconds.
   * -1: the current segment is ended
   */
  private final long[] segmentsCreationTimeArray;
  private final KeyWithChunkingSuffixSerializer keyWithChunkingSuffixSerializer = new KeyWithChunkingSuffixSerializer();
  private final ChunkedValueManifestSerializer chunkedValueManifestSerializer =
      new ChunkedValueManifestSerializer(true);
  private final Map<CharSequence, CharSequence> defaultDebugInfo;
  private final boolean elapsedTimeForClosingSegmentEnabled;
  private final Object[] partitionLocks;

  private String writerId;
  /**
   * N.B.: chunking enabled flag is mutable only if this VeniceWriter is currently used in pass-through mode and hasn't
   * been used in non pass-through mode yet; once VW starts referring to the chunking setting in non pass-through mode,
   * the chunking setting will become immutable.
   *
   * The only way to update chunking config is when a START_OF_PUSH message is processed in pass-through mode, chunking
   * setting will be updated to {@link StartOfPush#chunked}.
   */
  private volatile boolean isChunkingEnabled;
  private volatile boolean isChunkingSet;
  private volatile boolean isChunkingFlagInvoked;

  protected VeniceWriter(
      VeniceWriterOptions params,
      VeniceProperties props,
      Supplier<KafkaProducerWrapper> producerWrapperSupplier) {
    super(params.getTopicName());
    this.keySerializer = params.getKeySerializer();
    this.valueSerializer = params.getValueSerializer();
    this.writeComputeSerializer = params.getWriteComputeSerializer();
    this.time = params.getTime();
    this.partitioner = params.getPartitioner();
    this.closeTimeOut = props.getInt(CLOSE_TIMEOUT_MS, DEFAULT_CLOSE_TIMEOUT_MS);
    this.checkSumType = CheckSumType.valueOf(props.getString(CHECK_SUM_TYPE, DEFAULT_CHECK_SUM_TYPE));
    this.isChunkingEnabled = props.getBoolean(ENABLE_CHUNKING, false);
    this.isChunkingSet = props.containsKey(ENABLE_CHUNKING);
    this.maxSizeForUserPayloadPerMessageInBytes = props
        .getInt(MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES, DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES);
    if (maxSizeForUserPayloadPerMessageInBytes > DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES) {
      if (!isChunkingEnabled) {
        throw new VeniceException(
            MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES + " cannot be set higher than "
                + DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES + " unless " + ENABLE_CHUNKING + " is true");
      } else if (isChunkingEnabled && !Version.isVersionTopic(topicName)) {
        throw new VeniceException(
            MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES + " cannot be set higher than "
                + DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES + " unless " + ENABLE_CHUNKING
                + " is true and the topic is Version Topic");
      }
    }
    this.isChunkingFlagInvoked = false;
    this.maxAttemptsWhenTopicMissing =
        props.getInt(MAX_ATTEMPTS_WHEN_TOPIC_MISSING, DEFAULT_MAX_ATTEMPTS_WHEN_TOPIC_MISSING);
    this.sleepTimeMsWhenTopicMissing =
        props.getInt(SLEEP_TIME_MS_WHEN_TOPIC_MISSING, DEFAULT_SLEEP_TIME_MS_WHEN_TOPIC_MISSING);
    this.maxElapsedTimeForSegmentInMs =
        props.getLong(MAX_ELAPSED_TIME_FOR_SEGMENT_IN_MS, DEFAULT_MAX_ELAPSED_TIME_FOR_SEGMENT_IN_MS);
    this.elapsedTimeForClosingSegmentEnabled = maxElapsedTimeForSegmentInMs > 0;
    this.defaultDebugInfo = Utils.getDebugInfo();

    // if INSTANCE_ID is not set, we'd use "hostname:port" as the default writer id
    if (props.containsKey(INSTANCE_ID)) {
      this.writerId = props.getString(INSTANCE_ID);
    } else {
      this.writerId = Utils.getHostName();

      if (props.containsKey(LISTENER_PORT)) {
        this.writerId += ":" + props.getInt(LISTENER_PORT);
      }
    }

    try {
      this.producer = producerWrapperSupplier.get();
      // We cache the number of partitions, as it is expected to be immutable, and the call to Kafka is expensive.
      // Also avoiding a metadata call to kafka here as the partitionsFor() call sometimes may get blocked indefinitely
      // if the
      // kafka broker is overloaded and does not respond in timely manner.
      if (params.getPartitionCount().isPresent()) {
        this.numberOfPartitions = params.getPartitionCount().get();
      } else {
        this.numberOfPartitions = producer.getNumberOfPartitions(topicName, 30, TimeUnit.SECONDS);
      }
      this.segmentsCreationTimeArray = new long[this.numberOfPartitions];
      // Prepare locks for all partitions instead of using map to avoid the searching and creation cost during
      // ingestion.
      this.partitionLocks = new Object[numberOfPartitions];
      for (int i = 0; i < numberOfPartitions; i++) {
        partitionLocks[i] = new Object();
        segmentsCreationTimeArray[i] = -1L;
      }
      this.producerGUID = GuidUtils.getGUID(props);
      this.logger = LogManager
          .getLogger(VeniceWriter.class.getSimpleName() + " [" + GuidUtils.getHexFromGuid(producerGUID) + "]");
      OPEN_VENICE_WRITER_COUNT.incrementAndGet();
    } catch (Exception e) {
      throw new VeniceException(
          "Error while constructing VeniceWriter for store name: " + topicName + ", props: " + props.toString(),
          e);
    }
  }

  /**
   * Close the {@link VeniceWriter}
   * @param gracefulClose whether to end the segments and send END_OF_SEGMENT control message.
   *
   */
  @Override
  public void close(boolean gracefulClose) {
    try {
      // If {@link #broadcastEndOfPush(Map)} was already called, the {@link #endAllSegments(boolean)}
      // will not do anything (it's idempotent). Segments should not be ended if there are still data missing.
      if (gracefulClose) {
        endAllSegments(true);
      }
      // DO NOT call the {@link #KafkaProducerWrapper.close(int) version from here.}
      // For non shared producer mode gracefulClose will flush the producer

      producer.close(topicName, closeTimeOut, gracefulClose);
      OPEN_VENICE_WRITER_COUNT.decrementAndGet();
    } catch (Exception e) {
      logger.warn("Swallowed an exception while trying to close the VeniceWriter for topic: {}", topicName, e);
      VENICE_WRITER_CLOSE_FAILED_COUNT.incrementAndGet();
    }
  }

  @Override
  public void close() {
    close(true);
  }

  public KafkaProducerWrapper getProducer() {
    return producer;
  }

  /**
   * Call flush on the internal {@link KafkaProducerWrapper}.
   */
  @Override
  public void flush() {
    producer.flush();
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "{topicName: " + topicName + ", producerGUID: " + producerGUID
        + ", numberOfPartitions: " + numberOfPartitions + "}";
  }

  public GUID getProducerGUID() {
    return producerGUID;
  }

  /**
   * @return the Kafka topic name that this {@link VeniceWriter} instance writes into.
   */
  @Override
  public String getTopicName() {
    return topicName;
  }

  /**
   * Execute a standard "delete" on the key.
   *
   * @param key - The key to delete in storage.
   * @param callback - callback will be executed after Kafka producer completes on sending the message.
   * @return a java.util.concurrent.Future Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  public Future<RecordMetadata> delete(K key, Callback callback) {
    return delete(key, callback, DEFAULT_LEADER_METADATA_WRAPPER, APP_DEFAULT_LOGICAL_TS, Optional.empty());
  }

  /**
   * Execute a standard "delete" on the key.
   *
   * @param key - The key to delete in storage.
   * @param logicalTs - An timestamp field to indicate when this record was produced from apps point of view.
   * @param callback - callback will be executed after Kafka producer completes on sending the message.
   * @return a java.util.concurrent.Future Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  public Future<RecordMetadata> delete(K key, long logicalTs, Callback callback) {
    return delete(key, callback, DEFAULT_LEADER_METADATA_WRAPPER, logicalTs, Optional.empty());
  }

  /**
   * Execute a standard "delete" on the key.
   *
   * @param key - The key to delete in storage.
   * @param callback - Callback function invoked by Kafka producer after sending the message.
   * @param leaderMetadataWrapper - The leader Metadata of this message in the source topic:
   *                         -1:  VeniceWriter is sending this message in a Samza app to the real-time topic; or it's
   *                              sending the message in VPJ plugin to the version topic;
   *                         >=0: Leader replica consumes a delete message from real-time topic, VeniceWriter in leader
   *                              is sending this message to version topic with extra info: offset in the real-time topic.
   * @return a java.util.concurrent.Future Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  public Future<RecordMetadata> delete(K key, Callback callback, LeaderMetadataWrapper leaderMetadataWrapper) {
    return delete(key, callback, leaderMetadataWrapper, APP_DEFAULT_LOGICAL_TS, Optional.empty());
  }

  /**
   * Execute a standard "delete" on the key.
   *
   * @param key - The key to delete in storage.
   * @param callback - callback will be executed after Kafka producer completes on sending the message.
   * @param leaderMetadataWrapper - The leader Metadata of this message in the source topic:
   *                             -1:  VeniceWriter is sending this message in a Samza app to the real-time topic; or it's
   *                                  sending the message in VPJ plugin to the version topic;
   *                             >=0: Leader replica consumes a delete message from real-time topic, VeniceWriter in leader
   *                                  is sending this message to version topic with extra info: offset in the real-time topic.
   * @param logicalTs - An timestamp field to indicate when this record was produced from apps point of view.
   * @return a java.util.concurrent.Future Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  public Future<RecordMetadata> delete(
      K key,
      Callback callback,
      LeaderMetadataWrapper leaderMetadataWrapper,
      long logicalTs) {
    return delete(key, callback, leaderMetadataWrapper, logicalTs, Optional.empty());
  }

  /**
   * Execute a standard "delete" on the key.
   *
   * @param key - The key to delete in storage.
   * @param callback - callback will be executed after Kafka producer completes on sending the message.
   * @param leaderMetadataWrapper - The leader Metadata of this message in the source topic:
   *                             -1:  VeniceWriter is sending this message in a Samza app to the real-time topic; or it's
   *                                  sending the message in VPJ plugin to the version topic;
   *                             >=0: Leader replica consumes a delete message from real-time topic, VeniceWriter in leader
   *                                  is sending this message to version topic with extra info: offset in the real-time topic.
   * @param deleteMetadata - a DeleteMetadata containing replication metadata related fields.
   * @return a java.util.concurrent.Future Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  public Future<RecordMetadata> delete(
      K key,
      Callback callback,
      LeaderMetadataWrapper leaderMetadataWrapper,
      DeleteMetadata deleteMetadata) {
    return delete(key, callback, leaderMetadataWrapper, APP_DEFAULT_LOGICAL_TS, Optional.ofNullable(deleteMetadata));
  }

  @Override
  public Future<RecordMetadata> delete(K key, Callback callback, DeleteMetadata deleteMetadata) {
    return delete(
        key,
        callback,
        DEFAULT_LEADER_METADATA_WRAPPER,
        APP_DEFAULT_LOGICAL_TS,
        Optional.ofNullable(deleteMetadata));
  }

  /**
   * Execute a standard "delete" on the key.
   *
   * @param key - The key to delete in storage.
   * @param callback - Callback function invoked by Kafka producer after sending the message.
   * @param leaderMetadataWrapper - The leader Metadata of this message in the source topic:
   *                         -1:  VeniceWriter is sending this message in a Samza app to the real-time topic; or it's
   *                              sending the message in VPJ plugin to the version topic;
   *                         >=0: Leader replica consumes a delete message from real-time topic, VeniceWriter in leader
   *                              is sending this message to version topic with extra info: offset in the real-time topic.
   * @param logicalTs - An timestamp field to indicate when this record was produced from apps point of view.
   * @param deleteMetadata - an optional DeleteMetadata containing replication metadata related fields.
   * @return a java.util.concurrent.Future Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  private Future<RecordMetadata> delete(
      K key,
      Callback callback,
      LeaderMetadataWrapper leaderMetadataWrapper,
      long logicalTs,
      Optional<DeleteMetadata> deleteMetadata) {
    byte[] serializedKey = keySerializer.serialize(topicName, key);
    isChunkingFlagInvoked = true;

    int rmdPayloadSize = deleteMetadata.isPresent() ? deleteMetadata.get().getSerializedSize() : 0;
    if (serializedKey.length + rmdPayloadSize > maxSizeForUserPayloadPerMessageInBytes) {
      throw new RecordTooLargeException(
          "This record exceeds the maximum size. " + getSizeReport(serializedKey.length, 0, rmdPayloadSize));
    }

    if (isChunkingEnabled) {
      serializedKey = keyWithChunkingSuffixSerializer.serializeNonChunkedKey(serializedKey);
    }

    if (callback instanceof ChunkAwareCallback) {
      ((ChunkAwareCallback) callback).setChunkingInfo(serializedKey, null, null);
    }

    KafkaKey kafkaKey = new KafkaKey(MessageType.DELETE, serializedKey);

    int partition = getPartition(kafkaKey);

    Delete delete = new Delete();
    if (deleteMetadata.isPresent()) {
      delete.schemaId = deleteMetadata.get().getValueSchemaId();
      delete.replicationMetadataVersionId = deleteMetadata.get().getRmdVersionId();
      delete.replicationMetadataPayload = deleteMetadata.get().getRmdPayload();
    } else {
      delete.schemaId = VENICE_DEFAULT_VALUE_SCHEMA_ID;
      delete.replicationMetadataVersionId = VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID;
      delete.replicationMetadataPayload = EMPTY_BYTE_BUFFER;
    }

    return sendMessage(
        producerMetadata -> kafkaKey,
        MessageType.DELETE,
        delete,
        partition,
        callback,
        leaderMetadataWrapper,
        Optional.of(logicalTs));
  }

  /**
   * Execute a standard "put" on the key.
   *
   * @param key   - The key to put in storage.
   * @param value - The value to be associated with the given key
   * @param valueSchemaId - value schema id for the given value
   * @param callback - Callback function invoked by Kafka producer after sending the message
   * @return a java.util.concurrent.Future Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  @Override
  public Future<RecordMetadata> put(K key, V value, int valueSchemaId, Callback callback) {
    return put(
        key,
        value,
        valueSchemaId,
        callback,
        DEFAULT_LEADER_METADATA_WRAPPER,
        APP_DEFAULT_LOGICAL_TS,
        Optional.empty());
  }

  @Override
  public Future<RecordMetadata> put(K key, V value, int valueSchemaId, Callback callback, PutMetadata putMetadata) {
    return put(
        key,
        value,
        valueSchemaId,
        callback,
        DEFAULT_LEADER_METADATA_WRAPPER,
        APP_DEFAULT_LOGICAL_TS,
        Optional.ofNullable(putMetadata));
  }

  /**
   * Execute a standard "put" on the key.
   *
   * @param key   - The key to put in storage.
   * @param value - The value to be associated with the given key
   * @param valueSchemaId - value schema id for the given value
   * @param logicalTs - An optional timestamp field to indicate when this record was produced from apps view.
   * @param callback - Callback function invoked by Kafka producer after sending the message
   * @return a java.util.concurrent.Future Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  public Future<RecordMetadata> put(K key, V value, int valueSchemaId, long logicalTs, Callback callback) {
    return put(key, value, valueSchemaId, callback, DEFAULT_LEADER_METADATA_WRAPPER, logicalTs, Optional.empty());
  }

  /**
   * VeniceWriter in the leader replica should call this API to fulfill extra metadata information --- upstreamOffset.
   *
   * UpstreamOffset is the offset of PUT message in the source topic:
   * -1:  VeniceWriter is sending this message in a Samza app to the real-time topic; or it's
   *      sending the message in VPJ plugin to the version topic;
   * >=0: Leader replica consumes a put message from real-time topic, VeniceWriter in leader
   *      is sending this message to version topic with extra info: offset in the real-time topic.
   */
  public Future<RecordMetadata> put(
      K key,
      V value,
      int valueSchemaId,
      Callback callback,
      LeaderMetadataWrapper leaderMetadataWrapper) {
    return put(key, value, valueSchemaId, callback, leaderMetadataWrapper, APP_DEFAULT_LOGICAL_TS, Optional.empty());
  }

  /**
   * Execute a standard "put" on the key.
   *
   * VeniceReducer and VeniceSystemProducer should call this API.
   *
   * @param key   - The key to put in storage.
   * @param value - The value to be associated with the given key
   * @param valueSchemaId - value schema id for the given value
   * @param callback - Callback function invoked by Kafka producer after sending the message
   * @param leaderMetadataWrapper - The leader Metadata of this message in the source topic:
   *    -1:  VeniceWriter is sending this message in a Samza app to the real-time topic; or it's
   *         sending the message in VPJ plugin to the version topic;
   *    >=0: Leader replica consumes a put message from real-time topic, VeniceWriter in leader
   *         is sending this message to version topic with extra info: offset in the real-time topic.
   * @param logicalTs - An timestamp field to indicate when this record was produced from apps view.
   * @param putMetadata - an optional PutMetadata containing replication metadata related fields.
   * @return a java.util.concurrent.Future Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  public Future<RecordMetadata> put(
      K key,
      V value,
      int valueSchemaId,
      Callback callback,
      LeaderMetadataWrapper leaderMetadataWrapper,
      long logicalTs,
      Optional<PutMetadata> putMetadata) {
    byte[] serializedKey = keySerializer.serialize(topicName, key);
    byte[] serializedValue = valueSerializer.serialize(topicName, value);
    int partition = getPartition(serializedKey);

    int replicationMetadataPayloadSize = putMetadata.isPresent() ? putMetadata.get().getSerializedSize() : 0;
    isChunkingFlagInvoked = true;
    if (serializedKey.length + serializedValue.length
        + replicationMetadataPayloadSize > maxSizeForUserPayloadPerMessageInBytes) {
      if (isChunkingEnabled) {
        return putLargeValue(
            serializedKey,
            serializedValue,
            valueSchemaId,
            callback,
            partition,
            leaderMetadataWrapper,
            logicalTs,
            putMetadata);
      } else {
        throw new RecordTooLargeException(
            "This record exceeds the maximum size. "
                + getSizeReport(serializedKey.length, serializedValue.length, replicationMetadataPayloadSize));
      }
    }

    if (isChunkingEnabled) {
      serializedKey = keyWithChunkingSuffixSerializer.serializeNonChunkedKey(serializedKey);
    }

    if (callback instanceof ChunkAwareCallback) {
      ((ChunkAwareCallback) callback).setChunkingInfo(serializedKey, null, null);
    }

    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, serializedKey);

    // Initialize the SpecificRecord instances used by the Avro-based Kafka protocol
    Put putPayload = new Put();
    putPayload.putValue = ByteBuffer.wrap(serializedValue);
    putPayload.schemaId = valueSchemaId;

    if (putMetadata.isPresent()) {
      putPayload.replicationMetadataVersionId = putMetadata.get().getRmdVersionId();
      putPayload.replicationMetadataPayload = putMetadata.get().getRmdPayload();
    } else {
      putPayload.replicationMetadataVersionId = VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID;
      putPayload.replicationMetadataPayload = EMPTY_BYTE_BUFFER;
    }

    return sendMessage(
        producerMetadata -> kafkaKey,
        MessageType.PUT,
        putPayload,
        partition,
        callback,
        leaderMetadataWrapper,
        Optional.of(logicalTs));
  }

  /**
   * Write a message with the kafka message envelope (KME) passed in. This allows users re-using existing KME to
   * speed up the performance. If this is called, VeniceWriter will also reuse the existing DIV data (producer
   * metadata). It's the "pass-through" mode.
   *
   * TODO: move pass-through supports into a server-specific extension of VeniceWriter
   */
  @Deprecated
  public Future<RecordMetadata> put(
      KafkaKey kafkaKey,
      KafkaMessageEnvelope kafkaMessageEnvelope,
      Callback callback,
      int upstreamPartition,
      LeaderMetadataWrapper leaderMetadataWrapper) {
    // Self-adjust the chunking setting in pass-through mode
    verifyChunkingSetting(kafkaMessageEnvelope);

    byte[] serializedKey = kafkaKey.getKey();

    KafkaMessageEnvelopeProvider kafkaMessageEnvelopeProvider =
        getKafkaMessageEnvelopeProvider(kafkaMessageEnvelope, leaderMetadataWrapper);

    if (callback instanceof ChunkAwareCallback) {
      ((ChunkAwareCallback) callback).setChunkingInfo(serializedKey, null, null);
    }

    return sendMessage(producerMetadata -> kafkaKey, kafkaMessageEnvelopeProvider, upstreamPartition, callback, false);
  }

  private KafkaMessageEnvelopeProvider getKafkaMessageEnvelopeProvider(
      KafkaMessageEnvelope kafkaMessageEnvelope,
      LeaderMetadataWrapper leaderMetadataWrapper) {
    LeaderMetadata leaderMetadata = new LeaderMetadata();
    leaderMetadata.upstreamOffset = leaderMetadataWrapper.getUpstreamOffset();
    leaderMetadata.upstreamKafkaClusterId = leaderMetadataWrapper.getUpstreamKafkaClusterId();
    leaderMetadata.hostName = writerId;

    return () -> {
      kafkaMessageEnvelope.leaderMetadataFooter = leaderMetadata;
      kafkaMessageEnvelope.producerMetadata.upstreamOffset = -1; // This field has been deprecated
      return kafkaMessageEnvelope;
    };
  }

  /**
   * DIV pass-through mode for delete
   */
  public Future<RecordMetadata> delete(
      KafkaKey kafkaKey,
      KafkaMessageEnvelope kafkaMessageEnvelope,
      Callback callback,
      int upstreamPartition,
      LeaderMetadataWrapper leaderMetadataWrapper) {
    // Self-adjust the chunking setting in pass-through mode
    verifyChunkingSetting(kafkaMessageEnvelope);

    KafkaMessageEnvelopeProvider kafkaMessageEnvelopeProvider =
        getKafkaMessageEnvelopeProvider(kafkaMessageEnvelope, leaderMetadataWrapper);

    if (callback instanceof ChunkAwareCallback) {
      byte[] serializedKey = kafkaKey.getKey();
      ((ChunkAwareCallback) callback).setChunkingInfo(serializedKey, null, null);
    }

    return sendMessage(producerMetadata -> kafkaKey, kafkaMessageEnvelopeProvider, upstreamPartition, callback, false);
  }

  @Override
  public Future<RecordMetadata> update(K key, U update, int valueSchemaId, int derivedSchemaId, Callback callback) {
    return update(key, update, valueSchemaId, derivedSchemaId, callback, APP_DEFAULT_LOGICAL_TS);
  }

  public Future<RecordMetadata> update(
      K key,
      U update,
      int valueSchemaId,
      int derivedSchemaId,
      Callback callback,
      long logicalTs) {
    isChunkingFlagInvoked = true;
    if (isChunkingEnabled) {
      throw new VeniceException("Chunking is not supported for update operation in VeniceWriter");
    }
    byte[] serializedKey = keySerializer.serialize(topicName, key);
    byte[] serializedUpdate = writeComputeSerializer.serialize(topicName, update);
    int partition = getPartition(serializedKey);

    // large value is not supported for "update" yet
    if (serializedKey.length + serializedUpdate.length > DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES) {
      throw new RecordTooLargeException(
          "This partial update exceeds the maximum size. "
              + getSizeReport(serializedKey.length, serializedUpdate.length, 0));
    }

    KafkaKey kafkaKey = new KafkaKey((MessageType.UPDATE), serializedKey);

    Update updatePayLoad = new Update();
    updatePayLoad.updateValue = ByteBuffer.wrap(serializedUpdate);
    updatePayLoad.schemaId = valueSchemaId;
    updatePayLoad.updateSchemaId = derivedSchemaId;

    return sendMessage(
        producerMetadata -> kafkaKey,
        MessageType.UPDATE,
        updatePayLoad,
        partition,
        callback,
        DEFAULT_LEADER_METADATA_WRAPPER,
        Optional.of(logicalTs));
  }

  /**
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   */
  public void broadcastStartOfPush(Map<String, String> debugInfo) {
    broadcastStartOfPush(false, debugInfo);
  }

  public void broadcastStartOfPush(boolean sorted, Map<String, String> debugInfo) {
    broadcastStartOfPush(sorted, false, CompressionStrategy.NO_OP, debugInfo);
  }

  /**
   * @param sorted whether the messages between 'StartOfPush' control messages and 'EndOfPush' control
   *               message in current topic partition is lexicographically sorted by key bytes
   * @param chunked whether the push has chunking support enabled, in which case all keys are to be
   *                appended with {@link ChunkedKeySuffix} and the {@link Put} may contain either
   *                chunks or {@link ChunkedValueManifest} records, in addition to regular (small)
   *                values.
   * @param compressionStrategy the store-version's {@link CompressionStrategy}
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   */
  public void broadcastStartOfPush(
      boolean sorted,
      boolean chunked,
      CompressionStrategy compressionStrategy,
      Map<String, String> debugInfo) {
    broadcastStartOfPush(sorted, chunked, compressionStrategy, Optional.empty(), debugInfo);
  }

  /**
   * @param sorted whether the messages between 'StartOfPush' control messages and 'EndOfPush' control
   *               message in current topic partition is lexicographically sorted by key bytes
   * @param chunked whether the push has chunking support enabled, in which case all keys are to be
   *                appended with {@link ChunkedKeySuffix} and the {@link Put} may contain either
   *                chunks or {@link ChunkedValueManifest} records, in addition to regular (small)
   *                values.
   * @param compressionStrategy the store-version's {@link CompressionStrategy}
   * @param optionalCompressionDictionary The raw bytes of dictionary used to compress/decompress records.
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   */
  public void broadcastStartOfPush(
      boolean sorted,
      boolean chunked,
      CompressionStrategy compressionStrategy,
      Optional<ByteBuffer> optionalCompressionDictionary,
      Map<String, String> debugInfo) {
    ControlMessage controlMessage = getEmptyControlMessage(ControlMessageType.START_OF_PUSH);
    StartOfPush startOfPush = new StartOfPush();
    startOfPush.sorted = sorted;
    startOfPush.chunked = chunked;
    startOfPush.compressionStrategy = compressionStrategy.getValue();
    startOfPush.compressionDictionary = optionalCompressionDictionary.orElse(null);
    controlMessage.controlMessageUnion = startOfPush;
    broadcastControlMessage(controlMessage, debugInfo);
    // Flush start of push message to avoid data message arrives before it.
    producer.flush();
  }

  /**
   * This function might need synchronized locking. It might be possible that segments are ended while another thread is
   * broadcasting EOP. However, we could not trigger this scenario using tests.
   * This issue could surface when we support fully auto stream reprocessing in future. Users would call
   * broadcastEndOfPush inside their Samza processors which can have multiple threads.
   *
   *
   * Here is an example without synchronization:
   * - Thread A: broadcastControlMessage execution completes.
   * - Thread B: is in the middle of executing broadcastControlMessage
   * - Thread A: begins endAllSegments
   * - Segments that Thread B was writing to have now been ended. Thus causing the consumer to see messages not inside segments.
   *
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   */
  public void broadcastEndOfPush(Map<String, String> debugInfo) {
    broadcastControlMessage(getEmptyControlMessage(ControlMessageType.END_OF_PUSH), debugInfo);
    endAllSegments(true);
  }

  public void broadcastTopicSwitch(
      @Nonnull List<CharSequence> sourceKafkaCluster,
      @Nonnull String sourceTopicName,
      @Nonnull Long rewindStartTimestamp,
      Map<String, String> debugInfo) {
    Validate.notNull(sourceKafkaCluster);
    Validate.notEmpty(sourceTopicName);
    Validate.notNull(rewindStartTimestamp);
    ControlMessage controlMessage = getEmptyControlMessage(ControlMessageType.TOPIC_SWITCH);
    TopicSwitch topicSwitch = new TopicSwitch();
    topicSwitch.sourceKafkaServers = sourceKafkaCluster;
    topicSwitch.sourceTopicName = sourceTopicName;
    topicSwitch.rewindStartTimestamp = rewindStartTimestamp;
    controlMessage.controlMessageUnion = topicSwitch;
    broadcastControlMessage(controlMessage, debugInfo);
    producer.flush();
  }

  public void broadcastStartOfIncrementalPush(String version, Map<String, String> debugInfo) {
    ControlMessage controlMessage = getEmptyControlMessage(ControlMessageType.START_OF_INCREMENTAL_PUSH);
    StartOfIncrementalPush startOfIncrementalPush = new StartOfIncrementalPush();
    startOfIncrementalPush.version = version;
    controlMessage.controlMessageUnion = startOfIncrementalPush;
    broadcastControlMessage(controlMessage, debugInfo);
    producer.flush();
  }

  public void broadcastEndOfIncrementalPush(String version, Map<String, String> debugInfo) {
    ControlMessage controlMessage = getEmptyControlMessage(ControlMessageType.END_OF_INCREMENTAL_PUSH);
    EndOfIncrementalPush endOfIncrementalPush = new EndOfIncrementalPush();
    endOfIncrementalPush.version = version;
    controlMessage.controlMessageUnion = endOfIncrementalPush;
    broadcastControlMessage(controlMessage, debugInfo);
    producer.flush();
  }

  /**
   * IMPORTANT: Only invoke this function in pass-through mode.
   *
   * 1. If the isChunkingEnabled flag has never been used in the VeniceWriter (regular put will use this flag;
   *    pass-though mode doesn't use this flag), it's okay to update this chunking flag;
   * 2. If the isChunkingEnabled flag has been used, it's not allowed to update this flag anymore.
   *
   * If Start_of_Push message is produced through pass-through mode, check whether chunking config in SOP is different
   * from the chunking setting in the VeniceWriter; if so, update the chunking setting in this VW.
   *
   * Reason behind this check: in batch native replication, VeniceWriter might be created in leaders without reading
   * the chunking config in SOP message yet, so VW needs to self-adjust if SOP is produced in pass-through mode (pass-through
   * mode doesn't leverage the chunking setting in producing messages yet; once chunking setting is used, it becomes immutable)
   */
  private void verifyChunkingSetting(KafkaMessageEnvelope kafkaMessageEnvelope) {
    if (!isChunkingSet) {
      if (MessageType.valueOf(kafkaMessageEnvelope.messageType).equals(MessageType.CONTROL_MESSAGE)) {
        ControlMessage controlMessage = (ControlMessage) kafkaMessageEnvelope.payloadUnion;
        if (ControlMessageType.valueOf(controlMessage).equals(ControlMessageType.START_OF_PUSH)) {
          StartOfPush startOfPush = (StartOfPush) controlMessage.controlMessageUnion;
          synchronized (this) {
            if (!isChunkingSet) {
              if (isChunkingFlagInvoked) {
                throw new VeniceException(
                    "Chunking enabled config shouldn't be updated after VeniceWriter has explicitly produced a regular or chunked message");
              }
              logger.info(
                  "Chunking enabled config is updated from {} to {}",
                  this.isChunkingEnabled,
                  startOfPush.chunked);
              if (this.isChunkingEnabled != startOfPush.chunked) {
                this.isChunkingEnabled = startOfPush.chunked;
              }
              this.isChunkingSet = true;
            }
          }
        }
      }
    }
  }

  /**
   * Close a single partition from this writer. It will send a final EOS to the partition and remove it from segment map.
   * @param partition The partition to be closed.
   */
  public void closePartition(int partition) {
    if (segmentsMap.containsKey(partition)) {
      logger.info("Closing partition: {} in VeniceWriter.", partition);
      endSegment(partition, true);
    }
  }

  /**
   * Data message like PUT and DELETE should call this API to enable DIV check.
   */
  private Future<RecordMetadata> sendMessage(
      KeyProvider keyProvider,
      MessageType messageType,
      Object payload,
      int partition,
      Callback callback,
      LeaderMetadataWrapper leaderMetadataWrapper,
      Optional<Long> logicalTs) {
    return sendMessage(
        keyProvider,
        messageType,
        payload,
        false,
        partition,
        callback,
        true,
        leaderMetadataWrapper,
        logicalTs);
  }

  private Future<RecordMetadata> sendMessage(
      KeyProvider keyProvider,
      MessageType messageType,
      Object payload,
      boolean isEndOfSegment,
      int partition,
      Callback callback,
      boolean updateDIV,
      LeaderMetadataWrapper leaderMetadataWrapper,
      Optional<Long> logicalTs) {
    KafkaMessageEnvelopeProvider kafkaMessageEnvelopeProvider = () -> {
      KafkaMessageEnvelope kafkaValue =
          getKafkaMessageEnvelope(messageType, isEndOfSegment, partition, updateDIV, leaderMetadataWrapper, logicalTs);
      kafkaValue.payloadUnion = payload;
      return kafkaValue;
    };
    return sendMessage(keyProvider, kafkaMessageEnvelopeProvider, partition, callback, updateDIV);
  }

  /**
   * This is (and should remain!) the only function in the class which writes to Kafka. The synchronized locking
   * is important, in that it ensures that DIV-related operations are performed atomically with the write to Kafka,
   * which prevents ordering issues such as this one:
   *
   * - Thread A: calls sendMessage(msgA)
   * - Thread A: increments sequence # (now at X)
   * - Thread B: calls sendMessage(msgB)
   * - Thread B: increments sequence # (now at X+1)
   * - Thread B: produces (msgB, seq# X+1) into Kafka
   * - Thread A: produces (msgA, seq# X) into Kafka
   * - Consumer (in another process) sees: (msgB, seq# X+1) followed by (msgA, seq# X) which triggers a DIV issue
   *
   * P.S. 1: Callers which pass {@param updateDIV} == false for the purpose of retrying to produce the same message
   *         should also be synchronized, since otherwise the retries could be interleaved with other messages which
   *         have also updated the DIV.
   *         @see {@link #sendControlMessage(ControlMessage, int, Map, Callback, LeaderMetadataWrapper)}
   *
   * P.S. 2: If there is too much contention on this lock, then we can consider a finer locking strategy, where the
   *         locking is per-partition, which would also be correct as far as DIV is concerned.
   *
   * @param updateDIV if true, the partition's segment's checksum will be updated and its sequence number incremented
   *                  if false, the checksum and seq# update are omitted, which is the right thing to do during retries
   */
  private Future<RecordMetadata> sendMessage(
      KeyProvider keyProvider,
      KafkaMessageEnvelopeProvider valueProvider,
      int partition,
      Callback callback,
      boolean updateDIV) {
    synchronized (this.partitionLocks[partition]) {
      KafkaMessageEnvelope kafkaValue = valueProvider.getKafkaMessageEnvelope();
      KafkaKey key = keyProvider.getKey(kafkaValue.producerMetadata);
      if (updateDIV) {
        Segment segment = segmentsMap.get(partition);
        if (segment == null) {
          throw new VeniceException("segmentMap does not contain partition " + partition + " for topic " + topicName);
        }
        segment.addToCheckSum(key, kafkaValue);
      }
      Callback messageCallback = callback;
      if (callback == null) {
        messageCallback = new KafkaMessageCallback(kafkaValue, logger);
      } else if (callback instanceof CompletableFutureCallback) {
        CompletableFutureCallback completableFutureCallBack = (CompletableFutureCallback) callback;
        if (completableFutureCallBack.getCallback() == null) {
          completableFutureCallBack.setCallback(new KafkaMessageCallback(kafkaValue, logger));
        }
      }

      try {
        return producer.sendMessage(topicName, key, kafkaValue, partition, messageCallback);
      } catch (Exception e) {
        if (ExceptionUtils.recursiveClassEquals(e, TopicAuthorizationException.class)) {
          throw new TopicAuthorizationVeniceException(
              "You do not have permission to write to this store. Please check that ACLs are set correctly.",
              e);
        } else {
          throw e;
        }
      }
    }
  }

  /**
   * An interface which enables the key to contain parts of the {@param producerMetadata} within it, which is
   * useful for control messages and chunked values.
   */
  private interface KeyProvider {
    KafkaKey getKey(ProducerMetadata producerMetadata);
  }

  /**
   * An interface which enables the value to have ordered segment numbers and sequence numbers, which is mandatory for
   * DIV checks.
   */
  private interface KafkaMessageEnvelopeProvider {
    KafkaMessageEnvelope getKafkaMessageEnvelope();
  }

  /**
   * This function implements chunking of a large value into many small values.
   */
  private Future<RecordMetadata> putLargeValue(
      byte[] serializedKey,
      byte[] serializedValue,
      int valueSchemaId,
      Callback callback,
      int partition,
      LeaderMetadataWrapper leaderMetadataWrapper,
      long logicalTs,
      Optional<PutMetadata> putMetadata) {
    int replicationMetadataPayloadSize = putMetadata.isPresent() ? putMetadata.get().getSerializedSize() : 0;
    int sizeAvailablePerMessage = maxSizeForUserPayloadPerMessageInBytes - serializedKey.length;
    if (sizeAvailablePerMessage < maxSizeForUserPayloadPerMessageInBytes / 2) {
      /**
       * If the key size is more than half the available payload per message, then we cannot even encode a
       * {@link ChunkedValueManifest} with two keys in it, hence we would fail anyway. Might as well fail fast.
       *
       * N.B.: The above may not be strictly true, since Kafka-level compression may squeeze us under the limit,
       * but this is not likely to work deterministically, so it would be iffy to attempt it on a best-effort
       * basis. That being said, allowing keys to be as large as half the available payload size is probably
       * overly permissive anyway. Ideally, the key size should be much smaller than this.
       *
       * TODO: Implement a proper key size (and value size) quota mechanism.
       */
      throw new VeniceException(
          "Chunking cannot support this use case. The key is too large. "
              + getSizeReport(serializedKey.length, serializedValue.length, replicationMetadataPayloadSize));
    }
    int numberOfChunks = (int) Math.ceil((double) serializedValue.length / (double) sizeAvailablePerMessage);

    final ChunkedValueManifest chunkedValueManifest = new ChunkedValueManifest();
    chunkedValueManifest.schemaId = valueSchemaId;
    chunkedValueManifest.keysWithChunkIdSuffix = new ArrayList<>(numberOfChunks);
    chunkedValueManifest.size = serializedValue.length;

    int chunkStartByteIndex;
    int chunkEndByteIndex;
    KeyProvider keyProvider, firstKeyProvider, subsequentKeyProvider;
    boolean chunkAwareCallback = callback instanceof ChunkAwareCallback;
    ByteBuffer[] chunks = null;
    if (chunkAwareCallback) {
      // We only carry this state if it's going to be used, else we don't even instantiate this
      chunks = new ByteBuffer[numberOfChunks];
    }

    /**
     * This {@link ChunkedKeySuffix} instance gets mutated along the way, first in the loop, where its
     * chunk index is incremented at each iteration, and then also on the first iteration of the loop,
     * the {@link firstKeyProvider} will extract {@link ProducerMetadata} information out of the first
     * message sent, to be re-used across all chunk keys belonging to this value.
     */
    final ChunkedKeySuffix chunkedKeySuffix = new ChunkedKeySuffix();
    chunkedKeySuffix.isChunk = true;
    chunkedKeySuffix.chunkId = new ChunkId();

    subsequentKeyProvider = producerMetadata -> {
      ByteBuffer keyWithSuffix =
          ByteBuffer.wrap(keyWithChunkingSuffixSerializer.serializeChunkedKey(serializedKey, chunkedKeySuffix));
      chunkedValueManifest.keysWithChunkIdSuffix.add(keyWithSuffix);
      return new KafkaKey(MessageType.PUT, keyWithSuffix.array());
    };
    firstKeyProvider = producerMetadata -> {
      chunkedKeySuffix.chunkId.producerGUID = producerMetadata.producerGUID;
      chunkedKeySuffix.chunkId.segmentNumber = producerMetadata.segmentNumber;
      chunkedKeySuffix.chunkId.messageSequenceNumber = producerMetadata.messageSequenceNumber;
      return subsequentKeyProvider.getKey(producerMetadata);
    };
    for (int chunkIndex = 0; chunkIndex < numberOfChunks; chunkIndex++) {
      chunkStartByteIndex = chunkIndex * sizeAvailablePerMessage;
      chunkEndByteIndex = Math.min((chunkIndex + 1) * sizeAvailablePerMessage, serializedValue.length);

      /**
       * We leave 4 bytes of headroom at the beginning of the ByteBuffer so that the Venice Storage Node
       * can use this room to write the value header, without allocating a new byte array nor copying.
       */
      final int chunkLength = chunkEndByteIndex - chunkStartByteIndex;
      byte[] chunkValue = new byte[chunkLength + ByteUtils.SIZE_OF_INT];
      System.arraycopy(serializedValue, chunkStartByteIndex, chunkValue, ByteUtils.SIZE_OF_INT, chunkLength);
      ByteBuffer chunk = ByteBuffer.wrap(chunkValue);
      chunk.position(ByteUtils.SIZE_OF_INT);

      if (chunks != null) {
        chunks[chunkIndex] = chunk;
      }

      Put putPayload = new Put();
      putPayload.putValue = chunk;
      putPayload.schemaId = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
      putPayload.replicationMetadataVersionId = VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID;
      putPayload.replicationMetadataPayload = EMPTY_BYTE_BUFFER;

      chunkedKeySuffix.chunkId.chunkIndex = chunkIndex;
      keyProvider = chunkIndex == 0 ? firstKeyProvider : subsequentKeyProvider;

      try {
        /**
         * Here are the reasons to do a blocking call of 'sendMessage' with 'null' callback:
         * 1. We don't want the upper layer know the chunking logic here. Right now, if we pass the callback parameter,
         * it will cause the job failure because the message count of sent/completed in 'VeniceReducer' won't match;
         * 2. Blocking call could guarantee correctness;
         * 3. Infinite blocking means the following 'sendMessage' call will follow the config of the internal Kafka Producer,
         * such as timeout, retries and so on;
         */
        sendMessage(
            keyProvider,
            MessageType.PUT,
            putPayload,
            partition,
            null,
            DEFAULT_LEADER_METADATA_WRAPPER,
            Optional.empty()).get();
      } catch (Exception e) {
        throw new VeniceException(
            "Caught an exception while attempting to produce a chunk of a large value into Kafka... "
                + getDetailedSizeReport(
                    chunkIndex,
                    numberOfChunks,
                    sizeAvailablePerMessage,
                    serializedKey.length,
                    serializedValue.length,
                    replicationMetadataPayloadSize),
            e);
      }
    }

    // Now that we've sent all the chunks, we can take care of the final value, the manifest.
    byte[] topLevelKey = keyWithChunkingSuffixSerializer.serializeNonChunkedKey(serializedKey);
    KeyProvider manifestKeyProvider = producerMetadata -> new KafkaKey(MessageType.PUT, topLevelKey);

    Put putPayload = new Put();
    putPayload.putValue = ByteBuffer.wrap(chunkedValueManifestSerializer.serialize(topicName, chunkedValueManifest));
    putPayload.schemaId = AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();

    if (putPayload.putValue.remaining() + replicationMetadataPayloadSize > sizeAvailablePerMessage) {
      // This is a very desperate edge case...
      throw new VeniceException(
          "This message cannot be chunked, because even its manifest is too big to go through. "
              + "Please reconsider your life choices. "
              + getSizeReport(serializedKey.length, serializedValue.length, replicationMetadataPayloadSize));
    }

    if (chunkAwareCallback) {
      /** We leave a handle to the key, chunks and manifest so that the {@link ChunkAwareCallback} can act on them */
      ((ChunkAwareCallback) callback).setChunkingInfo(topLevelKey, chunks, chunkedValueManifest);
    }

    if (putMetadata.isPresent()) {
      putPayload.replicationMetadataVersionId = putMetadata.get().getRmdVersionId();
      putPayload.replicationMetadataPayload = putMetadata.get().getRmdPayload();
    } else {
      putPayload.replicationMetadataVersionId = VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID;
      putPayload.replicationMetadataPayload = EMPTY_BYTE_BUFFER;
    }

    // We only return the last future (the one for the manifest) and assume that once this one is finished,
    // all the chunks should also be finished, since they were sent first, and ordering should be guaranteed.
    return sendMessage(
        manifestKeyProvider,
        MessageType.PUT,
        putPayload,
        partition,
        callback,
        leaderMetadataWrapper,
        Optional.of(logicalTs));
  }

  private String getDetailedSizeReport(
      int chunkIndex,
      int numberOfChunks,
      int sizeAvailablePerMessage,
      int serializedKeySize,
      int serializedValueSize,
      int replicationMeatadataPayloadSize) {
    return "Current chunk index: " + chunkIndex + ", " + "Number of chunks: " + numberOfChunks + ", "
        + "Size available per message: " + sizeAvailablePerMessage + ", "
        + getSizeReport(serializedKeySize, serializedValueSize, replicationMeatadataPayloadSize);
  }

  private String getSizeReport(int serializedKeySize, int serializedValueSize, int replicationMetadataPayloadSize) {
    return "Key size: " + serializedKeySize + " bytes, " + "Value size: " + serializedValueSize + " bytes, "
        + "Replication Metadata size: " + replicationMetadataPayloadSize + " bytes, " + "Total payload size: "
        + (serializedKeySize + serializedValueSize + replicationMetadataPayloadSize) + " bytes, "
        + "Max available payload size: " + maxSizeForUserPayloadPerMessageInBytes + " bytes.";
  }

  /**
   * Send a {@link ControlMessageType#START_OF_SEGMENT} control message into the designated partition.
   *
   * @param partition the Kafka partition to write to.
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   */
  private void sendStartOfSegment(int partition, Map<String, String> debugInfo) {
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = ControlMessageType.START_OF_SEGMENT.getValue();
    StartOfSegment startOfSegment = new StartOfSegment();
    startOfSegment.checksumType = checkSumType.getValue();
    startOfSegment.upcomingAggregates = new ArrayList<>(); // TODO Add extra aggregates
    controlMessage.controlMessageUnion = startOfSegment;
    sendControlMessage(controlMessage, partition, debugInfo, null, DEFAULT_LEADER_METADATA_WRAPPER);
  }

  /**
   * Send a {@link ControlMessageType#END_OF_SEGMENT} control message into the designated partition.
   *
   * @param partition the Kafka partition to write to.
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   * @param finalSegment a boolean indicating if this is the final segment that this producer will send
   *                     into this partition.
   */
  private void sendEndOfSegment(int partition, Map<String, String> debugInfo, boolean finalSegment) {
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = ControlMessageType.END_OF_SEGMENT.getValue();
    EndOfSegment endOfSegment = new EndOfSegment();
    endOfSegment.checksumValue = ByteBuffer.wrap(segmentsMap.get(partition).getFinalCheckSum());
    endOfSegment.computedAggregates = new ArrayList<>(); // TODO Add extra aggregates
    endOfSegment.finalSegment = finalSegment;
    controlMessage.controlMessageUnion = endOfSegment;
    sendControlMessage(controlMessage, partition, debugInfo, null, DEFAULT_LEADER_METADATA_WRAPPER);
  }

  /**
   * Utility function to minimize code duplication for the simple {@link ControlMessage} which have no
   * extra metadata associated with them.
   *
   * @param controlMessageType the {@link ControlMessageType} for which to return an instance.
   * @return an instance of {@link ControlMessage} which corresponds to the requested {@param controlMessageType}.
   */
  private ControlMessage getEmptyControlMessage(ControlMessageType controlMessageType) {
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = controlMessageType.getValue();
    controlMessage.controlMessageUnion = controlMessageType.getNewInstance();
    return controlMessage;
  }

  /**
   * @param controlMessage a {@link ControlMessage} instance to persist into all Kafka partitions.
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   */
  private void broadcastControlMessage(ControlMessage controlMessage, Map<String, String> debugInfo) {
    for (int partition = 0; partition < numberOfPartitions; partition++) {
      sendControlMessage(controlMessage, partition, debugInfo, null, DEFAULT_LEADER_METADATA_WRAPPER);
    }
    logger.info(
        "Successfully broadcast {} Control Message for topic: {}",
        ControlMessageType.valueOf(controlMessage),
        topicName);
  }

  private Map<CharSequence, CharSequence> getDebugInfo(Map<String, String> debugInfoToAdd) {
    if (debugInfoToAdd == null || debugInfoToAdd.isEmpty()) {
      return defaultDebugInfo;
    }

    // Otherwise, we do a copy of the default info in order to avoid polluting it.
    Map<CharSequence, CharSequence> debugInfo = new HashMap<>(defaultDebugInfo);
    debugInfoToAdd.entrySet().stream().forEach(entry -> {
      String k = entry.getKey();
      String v = entry.getValue();
      CharSequence defaultValue = debugInfo.get(k);
      if (defaultValue != null && !defaultValue.equals(new Utf8(v))) {
        logger.warn(
            "Debug info key: '{}' will be omitted because it is already part of the default {} debug info. Default value: '{}', "
                + "supplied (omitted) value: '{}'",
            k,
            this.getClass().getSimpleName(),
            defaultValue,
            v);
      } else {
        debugInfo.put(k, v);
      }
    });

    return debugInfo;
  }

  /**
   * This function sends a control message into the prescribed partition.
   *
   * If the Kafka topic does not exist, this function will back off for {@link #sleepTimeMsWhenTopicMissing}
   * ms and try again for a total of {@link #maxAttemptsWhenTopicMissing} attempts. Note that this back off
   * and retry behavior does not happen in {@link #sendMessage(KeyProvider, MessageType, Object, int, Callback, LeaderMetadataWrapper, Optional<Long>)}
   * because that function returns a {@link Future}, and it is {@link Future#get()} which throws the relevant
   * exception. In any case, the topic should be seeded with a {@link ControlMessageType#START_OF_SEGMENT}
   * at first, and therefore, there should be no cases where a topic has not been created yet and we attempt
   * to write a data message first, prior to a control message. If a topic did disappear later on in the
   * {@link VeniceWriter}'s lifecycle, then it would be appropriate to let that {@link Future} fail.
   *
   * This function is synchronized because if the retries need to be exercised, then it would cause a DIV failure
   * if another message slipped in after the first attempt and before the eventually successful attempt.
   *
   * @param controlMessage a {@link ControlMessage} instance to persist into Kafka.
   * @param partition the Kafka partition to write to.
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   * @param callback callback to execute when the record has been acknowledged by the Kafka server (null means no callback)
   */
  public void sendControlMessage(
      ControlMessage controlMessage,
      int partition,
      Map<String, String> debugInfo,
      Callback callback,
      LeaderMetadataWrapper leaderMetadataWrapper) {
    synchronized (this.partitionLocks[partition]) {
      // Work around until we upgrade to a more modern Avro version which supports overriding the
      // String implementation.
      controlMessage.debugInfo = getDebugInfo(debugInfo);
      int attempt = 1;
      boolean updateCheckSum = true;
      while (true) {
        try {
          boolean isEndOfSegment = ControlMessageType.valueOf(controlMessage).equals(ControlMessageType.END_OF_SEGMENT);
          sendMessage(
              this::getControlMessageKey,
              MessageType.CONTROL_MESSAGE,
              controlMessage,
              isEndOfSegment,
              partition,
              callback,
              updateCheckSum,
              leaderMetadataWrapper,
              Optional.empty()).get();
          return;
        } catch (InterruptedException | ExecutionException e) {
          if (e.getMessage() != null && e.getMessage().contains(Errors.UNKNOWN_TOPIC_OR_PARTITION.message())) {
            /**
             * Not a super clean way to match the exception, but unfortunately, since it is wrapped inside of an
             * {@link ExecutionException}, there may be no other way.
             */
            String errorMessage =
                "Caught a UNKNOWN_TOPIC_OR_PARTITION error, attempt " + attempt + "/" + maxAttemptsWhenTopicMissing;
            if (attempt < maxAttemptsWhenTopicMissing) {
              attempt++;
              updateCheckSum = false; // checksum has already been updated, and should not be updated again for retries
              logger.warn("{}, will sleep {} ms before the next attempt.", errorMessage, sleepTimeMsWhenTopicMissing);
            } else {
              throw new VeniceException(errorMessage + ", will bubble up.");
            }
          } else if (e.getCause() != null && e.getCause().getClass().equals(TopicAuthorizationException.class)) {
            throw new TopicAuthorizationVeniceException(
                "You do not have permission to write to this store. Please check that ACLs are set correctly.",
                e);
          } else {
            throw new VeniceException(
                "Got an exception while trying to send a control message ("
                    + ControlMessageType.valueOf(controlMessage).name() + ")",
                e);
          }
        }
      }
    }
  }

  /**
   * This API should be only used in Leader/Standby model for store ingestion.
   *
   * Producer DIV will be recalculated (not DIV pass-through mode); checksum for the input partition in this producer
   * will also be updated.
   */
  public Future<RecordMetadata> asyncSendControlMessage(
      ControlMessage controlMessage,
      int partition,
      Map<String, String> debugInfo,
      Callback callback,
      LeaderMetadataWrapper leaderMetadataWrapper) {
    synchronized (this.partitionLocks[partition]) {
      controlMessage.debugInfo = getDebugInfo(debugInfo);
      boolean updateCheckSum = true;
      boolean isEndOfSegment = ControlMessageType.valueOf(controlMessage).equals(ControlMessageType.END_OF_SEGMENT);
      return sendMessage(
          this::getControlMessageKey,
          MessageType.CONTROL_MESSAGE,
          controlMessage,
          isEndOfSegment,
          partition,
          callback,
          updateCheckSum,
          leaderMetadataWrapper,
          Optional.empty());
    }
  }

  /**
   * For control messages, the Key part of the {@link KafkaKey} includes the producer GUID, segment and sequence number.
   *
   * N.B.: This could be optimized further by defining an Avro record to hold this data, since Avro would use
   * variable length encoding for the two integers, which would be smaller than their regular size.
   */
  private static final int CONTROL_MESSAGE_KAFKA_KEY_LENGTH =
      GUID.class.getAnnotation(FixedSize.class).value() + Integer.BYTES * 2;

  /**
   * The Key part of the {@link KafkaKey} needs to be unique in order to avoid clobbering each other during
   * Kafka's Log Compaction. Since there is no key per say associated with control messages, we generate one
   * from the producer metadata, including: GUID, segment and sequence number.
  
   * @param producerMetadata necessary to generate the key.
   * @return a {@link KafkaKey} guaranteed to be unique within its target partition.
   */
  private KafkaKey getControlMessageKey(ProducerMetadata producerMetadata) {
    return new KafkaKey(
        MessageType.CONTROL_MESSAGE,
        ByteBuffer.allocate(CONTROL_MESSAGE_KAFKA_KEY_LENGTH)
            .put(producerMetadata.producerGUID.bytes())
            .putInt(producerMetadata.segmentNumber)
            .putInt(producerMetadata.messageSequenceNumber)
            .array());
  }

  /**
   * A utility function to centralize some boiler plate code for the instantiation of
   * {@link org.apache.avro.specific.SpecificRecord} classes holding the content of our
   * Kafka values.
   *
   * Note: the payloadUnion must be set on the return object before producing into Kafka.
   *
   * @param messageType an instance of the {@link MessageType} enum.
   * @return A {@link KafkaMessageEnvelope} for producing into Kafka
   */
  protected KafkaMessageEnvelope getKafkaMessageEnvelope(
      MessageType messageType,
      boolean isEndOfSegment,
      int partition,
      boolean incrementSequenceNumber,
      LeaderMetadataWrapper leaderMetadataWrapper,
      Optional<Long> logicalTs) {
    // If single-threaded, the kafkaValue could be re-used (and clobbered). TODO: explore GC tuning later.
    KafkaMessageEnvelope kafkaValue = new KafkaMessageEnvelope();
    kafkaValue.messageType = messageType.getValue();

    ProducerMetadata producerMetadata = new ProducerMetadata();
    producerMetadata.producerGUID = producerGUID;

    Segment currentSegment = getSegment(partition, isEndOfSegment);
    producerMetadata.segmentNumber = currentSegment.getSegmentNumber();
    if (incrementSequenceNumber) {
      producerMetadata.messageSequenceNumber = currentSegment.getAndIncrementSequenceNumber();
    } else {
      producerMetadata.messageSequenceNumber = currentSegment.getSequenceNumber();
    }
    producerMetadata.messageTimestamp = time.getMilliseconds();
    producerMetadata.upstreamOffset = -1; // This field has been deprecated
    producerMetadata.logicalTimestamp = logicalTs.orElse(VENICE_DEFAULT_LOGICAL_TS);
    kafkaValue.producerMetadata = producerMetadata;
    kafkaValue.leaderMetadataFooter = new LeaderMetadata();
    kafkaValue.leaderMetadataFooter.hostName = writerId;
    kafkaValue.leaderMetadataFooter.upstreamOffset = leaderMetadataWrapper.getUpstreamOffset();
    kafkaValue.leaderMetadataFooter.upstreamKafkaClusterId = leaderMetadataWrapper.getUpstreamKafkaClusterId();

    return kafkaValue;
  }

  /**
   * @param key the {@link KafkaKey} for which we want to get the partition.
   * @return the partition number that the provided key belongs to.
   */
  private int getPartition(KafkaKey key) {
    return getPartition(key.getKey());
  }

  /**
   * @param key the {@link KafkaKey} for which we want to get the partition.
   * @return the partition number that the provided key belongs to.
   */
  private int getPartition(byte[] key) {
    return partitioner.getPartitionId(key, numberOfPartitions);
  }

  /**
   * @param partition for which we want to get the current {@link Segment}. Segment will be
   *                  ended automatically if its creation time is prior to
   *                  {@link #maxElapsedTimeForSegmentInMs}. The intention of having "shorter"
   *                  segment allows us to narrow down the "bad segment" easily when it happens.
   *
   * @return the existing {@link Segment} associated with the requested partition, or
   *         a new one if none existed previously.
   */
  private Segment getSegment(int partition, boolean sendEndOfSegment) {
    Segment currentSegment = segmentsMap.get(partition);
    if (currentSegment == null || currentSegment.isEnded()) {
      currentSegment = startSegment(partition);
    } else if (elapsedTimeForClosingSegmentEnabled) {
      // Close the current segment and create a new one if the current segment is
      // timed out. The segment won't be closed if the ongoing message itself is
      // a "end_of_segment" message.
      if (!sendEndOfSegment) {
        long currentSegmentCreationTime = segmentsCreationTimeArray[partition];
        if (currentSegmentCreationTime != -1
            && LatencyUtils.getElapsedTimeInMs(currentSegmentCreationTime) > maxElapsedTimeForSegmentInMs) {
          segmentsCreationTimeArray[partition] = -1L;
          endSegment(partition, true);
          currentSegment = startSegment(partition);
        }
      }
    }
    return currentSegment;
  }

  /**
   * This function ensures there is a valid {@link Segment} for the provided partition and that
   * it is started.
   *
   * If the segment does not already exist, or if the current segment has already been ended,
   * then this function creates a new segment. If necessary, this function also triggers the
   * production of the {@link ControlMessageType#START_OF_SEGMENT}. If the current segment has
   * already been started, then no redundant control message is sent.
   *
   * @param partition in which to start a new segment
   * @return the {@link Segment} which was just started
   */
  private Segment startSegment(int partition) {
    synchronized (this.partitionLocks[partition]) {
      Segment currentSegment = segmentsMap.get(partition);

      if (currentSegment == null) {
        currentSegment = new Segment(partition, 0, checkSumType);
        segmentsMap.put(partition, currentSegment);
      } else if (currentSegment.isEnded()) {
        int newSegmentNumber = currentSegment.getSegmentNumber() + 1;
        currentSegment = new Segment(partition, newSegmentNumber, checkSumType);
        segmentsMap.put(partition, currentSegment);
      }
      segmentsCreationTimeArray[partition] = time.getMilliseconds();
      if (!currentSegment.isStarted()) {
        sendStartOfSegment(partition, null);
        currentSegment.start();
      }

      return currentSegment;
    }
  }

  private void endAllSegments(boolean finalSegment) {
    segmentsMap.keySet().stream().forEach(partition -> endSegment(partition, finalSegment));
  }

  /**
   * @param partition in which to end the current segment
   */
  public void endSegment(int partition, boolean finalSegment) {
    synchronized (this.partitionLocks[partition]) {
      Segment currentSegment = segmentsMap.get(partition);
      if (currentSegment == null) {
        logger.warn("endSegment(partition {}) called but currentSegment == null. Ignoring.", partition);
      } else if (!currentSegment.isStarted()) {
        logger.warn("endSegment(partition {}) called but currentSegment.begun == false. Ignoring.", partition);
      } else if (currentSegment.isEnded()) {
        logger.warn("endSegment(partition {}) called but currentSegment.ended == true. Ignoring.", partition);
      } else {
        sendEndOfSegment(
            partition,
            new HashMap<>(), // TODO: Add extra debugging info
            finalSegment // TODO: This will not always be true, once we support streaming, or more than one segment per
                         // mapper in batch
        );
        currentSegment.end(finalSegment);
      }
    }
  }

  public Time getTime() {
    return time;
  }

  public int getMaxSizeForUserPayloadPerMessageInBytes() {
    return maxSizeForUserPayloadPerMessageInBytes;
  }
}
