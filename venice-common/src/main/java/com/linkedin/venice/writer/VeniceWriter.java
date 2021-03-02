package com.linkedin.venice.writer;

import com.linkedin.venice.annotation.Threadsafe;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.TopicAuthorizationVeniceException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.protocol.*;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.validation.Segment;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
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

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.avro.specific.FixedSize;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.log4j.Logger;

import static com.linkedin.venice.ConfigKeys.*;


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
  public static final String MAX_ATTEMPTS_WHEN_TOPIC_MISSING = VENICE_WRITER_CONFIG_PREFIX + "max.attemps.when.topic.missing";
  public static final String SLEEP_TIME_MS_WHEN_TOPIC_MISSING = VENICE_WRITER_CONFIG_PREFIX + "sleep.time.ms.when.topic.missing";
  public static final String MAX_ELAPSED_TIME_FOR_SEGMENT_IN_MS = VENICE_WRITER_CONFIG_PREFIX + "max.elapsed.time.for.segment.in.ms";

  /**
   * Chunk size. Default: {@value DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES}
   *
   * N.B.: This must be configured in relation to the following configs:
   *
   * 1. Kafka broker's global setting: `message.max.bytes` (default is 1000012, or ~976 KB)
   * 2. Kafka broker's topic setting: `max.message.bytes` (default is 1000012, or ~976 KB)
   * 3. Kafka producer's setting: `max.request.size` (default is 1048576)
   */
  public static final String MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES = VENICE_WRITER_CONFIG_PREFIX + "max.size.for.user.payload.per.message.in.bytes";

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
   * The default value of the "upstreamOffset" field in avro record {@link ProducerMetadata}.
   *
   * Even though we have set the default value for "upstreamOffset" field as -1, the initial value for the long field
   * "upstreamOffset" is still 0 when we construct a ProducerMetadata record. Default field values are primarily used
   * when reading records that don't have those fields, typically when we deserialize a record from older version to
   * newer version.
   */
  public static final long DEFAULT_UPSTREAM_OFFSET = ProducerMetadata.SCHEMA$.getField("upstreamOffset").defaultValue().asLong();

  private static final long DEFAULT_MAX_ELAPSED_TIME_FOR_SEGMENT_IN_MS = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

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
  /** Map of partition to {@link Segment}, which keeps track of all segment-related state. */
  private final Map<Integer, Segment> segmentsMap = new HashMap<>();
  /**
   * Map of partition to its segment creation time in milliseconds.
   * -1: the current segment is ended
   */
  private final Map<Integer, Long> segmentsCreationTimeMap = new HashMap<>();
  private final KeyWithChunkingSuffixSerializer keyWithChunkingSuffixSerializer = new KeyWithChunkingSuffixSerializer();
  private final ChunkedValueManifestSerializer chunkedValueManifestSerializer = new ChunkedValueManifestSerializer(true);
  private final Map<CharSequence, CharSequence> defaultDebugInfo;

  private String writerId;
  /**
   * N.B.: chunking enabled flag is mutable; if users attempt to update the flag after VeniceWriter is created, they
   * need to be aware that the message format sent before changing the flag is different from the message format sent
   * after changing the flag, which usually breaks the consumption side unless the consumer is aware of the exact offset
   * where the format changes.
   *
   * A recommended way to update the chunking flag after it's created: when VeniceWriter is producing in a pass-through
   * mode (a.k.a {@link VeniceWriter#put(KafkaKey, KafkaMessageEnvelope, Callback, int, long)}), it doesn't care about the
   * chunking flag, it's okay to update this flag during pass-through mode; once VeniceWriter starts producing in a non
   * pass-through fashion, the chunking flag shouldn't be updated anymore.
   */
  private volatile boolean isChunkingEnabled;
  private volatile boolean isChunkingFlagInvoked;

  protected VeniceWriter(
      VeniceProperties props,
      String topicName,
      VeniceKafkaSerializer<K> keySerializer,
      VeniceKafkaSerializer<V> valueSerializer,
      VeniceKafkaSerializer<U> writeComputeSerializer,
      VenicePartitioner partitioner,
      Time time,
      Supplier<KafkaProducerWrapper> producerWrapperSupplier) {
    super(topicName);

    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.writeComputeSerializer = writeComputeSerializer;
    this.time = time;
    this.partitioner = partitioner;
    this.closeTimeOut = props.getInt(CLOSE_TIMEOUT_MS, DEFAULT_CLOSE_TIMEOUT_MS);
    this.checkSumType = CheckSumType.valueOf(props.getString(CHECK_SUM_TYPE, DEFAULT_CHECK_SUM_TYPE));
    this.isChunkingEnabled = props.getBoolean(ENABLE_CHUNKING, false);
    this.maxSizeForUserPayloadPerMessageInBytes = props.getInt(MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES,
        DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES);
    if (maxSizeForUserPayloadPerMessageInBytes > DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES) {
      if (!isChunkingEnabled) {
        throw new VeniceException(MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES + " cannot be set higher than " + DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES + " unless "
            + ENABLE_CHUNKING + " is true");
      } else if (isChunkingEnabled && !Version.isVersionTopic(topicName)) {
        throw new VeniceException(MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES + " cannot be set higher than " + DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES + " unless "
            + ENABLE_CHUNKING + " is true and the topic is Version Topic");
      }
    }
    this.isChunkingFlagInvoked = false;
    this.maxAttemptsWhenTopicMissing = props.getInt(MAX_ATTEMPTS_WHEN_TOPIC_MISSING, DEFAULT_MAX_ATTEMPTS_WHEN_TOPIC_MISSING);
    this.sleepTimeMsWhenTopicMissing = props.getInt(SLEEP_TIME_MS_WHEN_TOPIC_MISSING, DEFAULT_SLEEP_TIME_MS_WHEN_TOPIC_MISSING);
    this.maxElapsedTimeForSegmentInMs = props.getLong(MAX_ELAPSED_TIME_FOR_SEGMENT_IN_MS, DEFAULT_MAX_ELAPSED_TIME_FOR_SEGMENT_IN_MS);
    this.defaultDebugInfo = Utils.getDebugInfo();

    //if INSTANCE_ID is not set, we'd use "hostname:port" as the default writer id
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
      this.numberOfPartitions = producer.getNumberOfPartitions(topicName);
      this.producerGUID = GuidUtils.getGUID(props);
      this.logger = Logger.getLogger(VeniceWriter.class.getSimpleName() + " [" + GuidUtils.getHexFromGuid(producerGUID) + "]");
    } catch (Exception e) {
      throw new VeniceException("Error while constructing VeniceWriter for store name: " + topicName + ", props: " + props.toString(), e);
    }
  }

  protected VeniceWriter(
      VeniceProperties props,
      String topicName,
      VeniceKafkaSerializer<K> keySerializer,
      VeniceKafkaSerializer<V> valueSerializer,
      VeniceKafkaSerializer<U> writeComputeSerializer,
      Time time) {
    this(
        props,
        topicName,
        keySerializer,
        valueSerializer,
        writeComputeSerializer,
        new DefaultVenicePartitioner(props),
        time,
        () -> new ApacheKafkaProducer(props));
  }

  /**
   * Close the {@link VeniceWriter}
   * @param shouldEndAllSegments whether to end the segments and send END_OF_SEGMENT control message.
   */
  @Override
  public void close(boolean shouldEndAllSegments) {
    //If {@link #broadcastEndOfPush(Map)} was already called, the {@link #endAllSegments(boolean)}
    // will not do anything (it's idempotent). Segments should not be ended if there are still data missing.
    if (shouldEndAllSegments) {
      endAllSegments(true);
    }
    producer.close(closeTimeOut);
  }

  public void close() {
    close(true);
  }

  /**
   * Call flush on the internal {@link KafkaProducerWrapper}.
   */
  public void flush() {
    producer.flush();
  }

  public String toString() {
    return this.getClass().getSimpleName()
        + "{topicName: " + topicName
        + ", producerGUID: " + producerGUID
        + ", numberOfPartitions: " + numberOfPartitions + "}";
  }

  public GUID getProducerGUID() {
    return producerGUID;
  }

  /**
   * @return the Kafka topic name that this {@link VeniceWriter} instance writes into.
   */
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
    return delete(key, callback, DEFAULT_UPSTREAM_OFFSET);
  }

  /**
   * Execute a standard "delete" on the key.
   *
   * @param key - The key to delete in storage.
   * @param callback - Callback function invoked by Kafka producer after sending the message.
   * @param upstreamOffset - The upstream offset of this message in the source topic:
   *                         -1:  VeniceWriter is sending this message in a Samza app to the real-time topic; or it's
   *                              sending the message in H2V plugin to the version topic;
   *                         >=0: Leader replica consumes a delete message from real-time topic, VeniceWriter in leader
   *                              is sending this message to version topic with extra info: offset in the real-time topic.
   * @return a java.util.concurrent.Future Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  public Future<RecordMetadata> delete(K key, Callback callback, long upstreamOffset) {
    byte[] serializedKey = keySerializer.serialize(topicName, key);
    isChunkingFlagInvoked = true;
    if (isChunkingEnabled) {
      serializedKey = keyWithChunkingSuffixSerializer.serializeNonChunkedKey(serializedKey);
    }

    if (callback instanceof ChunkAwareCallback) {
      ((ChunkAwareCallback) callback).setChunkingInfo(serializedKey, null, null);
    }

    KafkaKey kafkaKey = new KafkaKey(MessageType.DELETE, serializedKey);

    int partition = getPartition(kafkaKey);
    return sendMessage(producerMetadata -> kafkaKey, MessageType.DELETE, new Delete(), partition, callback, upstreamOffset);
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
   * @return a java.util.concurrent.Future Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  @Override
  public Future<RecordMetadata> put(K key, V value, int valueSchemaId, Callback callback) {
    return put(key, value, valueSchemaId, callback, DEFAULT_UPSTREAM_OFFSET);
  }

  /**
   * VeniceWriter in the leader replica should call this API to fulfill extra metadata information --- upstreamOffset.
   *
   * UpstreamOffset is the offset of PUT message in the source topic:
   * -1:  VeniceWriter is sending this message in a Samza app to the real-time topic; or it's
   *      sending the message in H2V plugin to the version topic;
   * >=0: Leader replica consumes a put message from real-time topic, VeniceWriter in leader
   *      is sending this message to version topic with extra info: offset in the real-time topic.
   */
  public Future<RecordMetadata> put(K key, V value, int valueSchemaId, Callback callback, long upstreamOffset) {
    byte[] serializedKey = keySerializer.serialize(topicName, key);
    byte[] serializedValue = valueSerializer.serialize(topicName, value);
    int partition = getPartition(serializedKey);

    isChunkingFlagInvoked = true;
    if (serializedKey.length + serializedValue.length > maxSizeForUserPayloadPerMessageInBytes) {
      if (isChunkingEnabled) {
        return putLargeValue(serializedKey, serializedValue, valueSchemaId, callback, partition, upstreamOffset);
      } else {
        throw new VeniceException("This record exceeds the maximum size. " +
            getSizeReport(serializedKey, serializedValue));
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

    return sendMessage(producerMetadata -> kafkaKey, MessageType.PUT, putPayload, partition, callback, upstreamOffset);
  }

  /**
   * Write a message with the kafka message envelope (KME) passed in. This allows users re-using existing KME to
   * speed up the performance. If this is called, VeniceWriter will also reuse the existing DIV data (producer
   * metadata). It's the "pass-through" mode.
   */
  public Future<RecordMetadata> put(KafkaKey kafkaKey, KafkaMessageEnvelope kafkaMessageEnvelope, Callback callback,
      int upstreamPartition, long upstreamOffset) {
    byte[] serializedKey = kafkaKey.getKey();

    LeaderMetadata leaderMetadata = new LeaderMetadata();
    leaderMetadata.upstreamOffset = upstreamOffset;
    leaderMetadata.hostName = writerId;

    KafkaMessageEnvelopeProvider kafkaMessageEnvelopeProvider = () -> {
      kafkaMessageEnvelope.leaderMetadataFooter = leaderMetadata;
      return kafkaMessageEnvelope;
    };

    if (callback instanceof ChunkAwareCallback) {
      ((ChunkAwareCallback) callback).setChunkingInfo(serializedKey, null, null);
    }

    return sendMessage(producerMetadata -> kafkaKey, kafkaMessageEnvelopeProvider, upstreamPartition, callback, false);
  }

  @Override
  public Future<RecordMetadata> update(K key, U update, int valueSchemaId, int derivedSchemaId, Callback callback) {
    isChunkingFlagInvoked = true;
    if (isChunkingEnabled) {
      throw new VeniceException("Chunking is not supported for update operation in VeniceWriter");
    }
    byte[] serializedKey = keySerializer.serialize(topicName, key);
    byte[] serializedUpdate = writeComputeSerializer.serialize(topicName, update);
    int partition = getPartition(serializedKey);

    //large value is not supported for "update" yet
    if (serializedKey.length + serializedUpdate.length > DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES) {
      throw new VeniceException("This partial update exceeds the maximum size. " + getSizeReport(serializedKey, serializedUpdate));
    }

    KafkaKey kafkaKey = new KafkaKey((MessageType.UPDATE), serializedKey);

    Update updatePayLoad = new Update();
    updatePayLoad.updateValue = ByteBuffer.wrap(serializedUpdate);
    updatePayLoad.schemaId = valueSchemaId;
    updatePayLoad.updateSchemaId = derivedSchemaId;

    return sendMessage(producerMetadata -> kafkaKey, MessageType.UPDATE, updatePayLoad, partition, callback, DEFAULT_UPSTREAM_OFFSET);
  }

  @Override
  public Map<String, Double> getMeasurableProducerMetrics() {
    return producer.getMeasurableProducerMetrics();
  }

  @Override
  public String getBrokerLeaderHostname(String topic, int partition) {
    return producer.getBrokerLeaderHostname(topic, partition);
  }

  /**
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   */
  public void broadcastStartOfPush(Map<String, String> debugInfo) {
    broadcastStartOfPush(false, debugInfo);
  }

  public void broadcastStartOfPush(boolean sorted, Map<String, String> debugInfo) {
    broadcastStartOfPush(sorted,false, CompressionStrategy.NO_OP, debugInfo);
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
  public void broadcastStartOfPush(boolean sorted, boolean chunked,
      CompressionStrategy compressionStrategy, Map<String, String> debugInfo) {
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
  public void broadcastStartOfPush(boolean sorted, boolean chunked,
      CompressionStrategy compressionStrategy, Optional<ByteBuffer> optionalCompressionDictionary, Map<String, String> debugInfo) {
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

  /**
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   */
  public void broadcastStartOfBufferReplay(List<Long> sourceOffsets, String sourceKafkaCluster, String sourceTopicName, Map<String, String> debugInfo) {
    ControlMessage controlMessage = getEmptyControlMessage(ControlMessageType.START_OF_BUFFER_REPLAY);
    StartOfBufferReplay startOfBufferReplay = new StartOfBufferReplay();
    startOfBufferReplay.sourceOffsets = Utils.notNull(sourceOffsets);
    startOfBufferReplay.sourceKafkaCluster = Utils.notNull(sourceKafkaCluster);
    startOfBufferReplay.sourceTopicName = Utils.notNull(sourceTopicName);
    controlMessage.controlMessageUnion = startOfBufferReplay;
    broadcastControlMessage(controlMessage, debugInfo);
    // Flush start of push message to avoid data message arrives before it.
    producer.flush();
  }

  public void broadcastTopicSwitch(List<CharSequence> sourceKafkaCluster, String sourceTopicName, Long rewindStartTimestamp,
      Map<String, String> debugInfo) {
    ControlMessage controlMessage = getEmptyControlMessage(ControlMessageType.TOPIC_SWITCH);
    TopicSwitch topicSwitch = new TopicSwitch();
    topicSwitch.sourceKafkaServers = Utils.notNull(sourceKafkaCluster);
    topicSwitch.sourceTopicName = Utils.notNull(sourceTopicName);
    topicSwitch.rewindStartTimestamp = Utils.notNull(rewindStartTimestamp);
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
   * 1. If the isChunkingEnabled flag has never been used in the VeniceWriter (regular put will use this flag;
   *    pass-though mode doesn't use this flag), it's okay to update this chunking flag;
   * 2. If the isChunkingEnabled flas has been used, it's not allowed to update this flag anymore.
   */
  public synchronized void updateChunckingEnabled(boolean isChunkingEnabled) {
    if (isChunkingFlagInvoked) {
      throw new VeniceException("Chunking enabled config shouldn't be updated after VeniceWriter has explitly produced a regular or chunked message");
    }
    logger.info("Chunking enabled config is updated from " + this.isChunkingEnabled + " to " + isChunkingEnabled);
    this.isChunkingEnabled = isChunkingEnabled;
  }

  /**
   * Data message like PUT and DELETE should call this API to enable DIV check.
   */
  private Future<RecordMetadata> sendMessage(KeyProvider keyProvider, MessageType messageType, Object payload,
      int partition, Callback callback, long upstreamOffset) {
    return sendMessage(keyProvider, messageType, payload, false, partition, callback, true, upstreamOffset);
  }

  private Future<RecordMetadata> sendMessage(KeyProvider keyProvider, MessageType messageType, Object payload,
      boolean isEndOfSegment, int partition, Callback callback, boolean updateDIV, long upstreamOffset) {
    KafkaMessageEnvelopeProvider kafkaMessageEnvelopeProvider = () -> {
      KafkaMessageEnvelope kafkaValue =
          getKafkaMessageEnvelope(messageType, isEndOfSegment, partition, updateDIV, upstreamOffset);
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
   *         @see {@link #sendControlMessage(ControlMessage, int, Map, Callback, long)}
   *
   * P.S. 2: If there is too much contention on this lock, then we can consider a finer locking strategy, where the
   *         locking is per-partition, which would also be correct as far as DIV is concerned.
   *
   * @param updateDIV if true, the partition's segment's checksum will be updated and its sequence number incremented
   *                  if false, the checksum and seq# update are omitted, which is the right thing to do during retries
   */
  private synchronized Future<RecordMetadata> sendMessage(
      KeyProvider keyProvider,
      KafkaMessageEnvelopeProvider valueProvider,
      int partition,
      Callback callback,
      boolean updateDIV) {
    KafkaMessageEnvelope kafkaValue = valueProvider.getKafkaMessageEnvelope();
    KafkaKey key = keyProvider.getKey(kafkaValue.producerMetadata);
    if (updateDIV) {
      segmentsMap.get(partition).addToCheckSum(key, kafkaValue);
    }
    Callback messageCallback = callback;
    if (null == callback) {
      messageCallback = new KafkaMessageCallback(key, kafkaValue, logger);
    } else if (callback instanceof CompletableFutureCallback) {
      CompletableFutureCallback completableFutureCallBack = (CompletableFutureCallback)callback;
      if (completableFutureCallBack.callback == null) {
        completableFutureCallBack.callback = new KafkaMessageCallback(key, kafkaValue, logger);
      }
    }

    try {
      return producer.sendMessage(topicName, key, kafkaValue, partition, messageCallback);
    } catch (Exception e) {
      if (ExceptionUtils.recursiveClassEquals(e, TopicAuthorizationException.class)) {
        throw new TopicAuthorizationVeniceException("You do not have permission to write to this store. Please check that ACLs are set correctly.", e);
      } else {
        throw e;
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
  private Future<RecordMetadata> putLargeValue(byte[] serializedKey, byte[] serializedValue, int valueSchemaId,
      Callback callback, int partition, long upstreamOffset) {
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
      throw new VeniceException("Chunking cannot support this use case. The key is too large. "
          + getSizeReport(serializedKey, serializedValue));
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
      ByteBuffer keyWithSuffix = ByteBuffer.wrap(keyWithChunkingSuffixSerializer.serializeChunkedKey(serializedKey, chunkedKeySuffix));
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
      byte[] value = new byte[chunkEndByteIndex - chunkStartByteIndex + ByteUtils.SIZE_OF_INT];
      System.arraycopy(serializedValue, chunkStartByteIndex, value, ByteUtils.SIZE_OF_INT, chunkEndByteIndex - chunkStartByteIndex);
      ByteBuffer chunk = ByteBuffer.wrap(value);
      chunk.position(ByteUtils.SIZE_OF_INT);

      if (chunks != null) {
        chunks[chunkIndex] = chunk;
      }

      Put putPayload = new Put();
      putPayload.putValue = chunk;
      putPayload.schemaId = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();

      chunkedKeySuffix.chunkId.chunkIndex = chunkIndex;
      if (0 == chunkIndex) {
        keyProvider = firstKeyProvider;
      } else {
        keyProvider = subsequentKeyProvider;
      }

      try {
        /**
         * Here are the reasons to do a blocking call of 'sendMessage' with 'null' callback:
         * 1. We don't want the upper layer know the chunking logic here. Right now, if we pass the callback parameter,
         * it will cause the job failure because the message count of sent/completed in 'VeniceReducer' won't match;
         * 2. Blocking call could guarantee correctness;
         * 3. Infinite blocking means the following 'sendMessage' call will follow the config of the internal Kafka Producer,
         * such as timeout, retries and so on;
         */
        sendMessage(keyProvider, MessageType.PUT, putPayload, partition, null, DEFAULT_UPSTREAM_OFFSET).get();
      } catch (Exception e) {
        throw new VeniceException("Caught an exception while attempting to produce a chunk of a large value into Kafka... "
            + getDetailedSizeReport(chunkIndex, numberOfChunks, sizeAvailablePerMessage, serializedKey, serializedValue), e);
      }
    }

    // Now that we've sent all the chunks, we can take care of the final value, the manifest.
    byte[] topLevelKey = keyWithChunkingSuffixSerializer.serializeNonChunkedKey(serializedKey);
    KeyProvider manifestKeyProvider = producerMetadata -> new KafkaKey(MessageType.PUT, topLevelKey);

    Put putPayload = new Put();
    putPayload.putValue = ByteBuffer.wrap(chunkedValueManifestSerializer.serialize(topicName, chunkedValueManifest));
    putPayload.schemaId = AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();

    if (putPayload.putValue.capacity() > sizeAvailablePerMessage) {
      // This is a very desperate edge case...
      throw new VeniceException("This message cannot be chunked, because even its manifest is too big to go through. "
          + "Please reconsider your life choices. " + getSizeReport(serializedKey, serializedValue));
    }

    if (chunkAwareCallback) {
      /** We leave a handle to the key, chunks and manifest so that the {@link ChunkAwareCallback} can act on them */
      ((ChunkAwareCallback) callback).setChunkingInfo(topLevelKey, chunks, chunkedValueManifest);
    }

    // We only return the last future (the one for the manifest) and assume that once this one is finished,
    // all the chunks should also be finished, since they were sent first, and ordering should be guaranteed.
    return sendMessage(manifestKeyProvider, MessageType.PUT, putPayload, partition, callback, upstreamOffset);
  }

  private String getDetailedSizeReport(int chunkIndex, int numberOfChunks, int sizeAvailablePerMessage, byte[] serializedKey, byte[] serializedValue) {
    return "Current chunk index: " + chunkIndex + ", "
        + "Number of chunks: " + numberOfChunks + ", "
        + "Size available per message: " + sizeAvailablePerMessage + ", "
        + getSizeReport(serializedKey, serializedValue);
  }

  private String getSizeReport(byte[] serializedKey, byte[] serializedValue) {
    return "Key size: " + serializedKey.length + " bytes, "
        + "Value size: " + serializedValue.length + " bytes, "
        + "Total payload size: " + (serializedKey.length + serializedValue.length) + " bytes, "
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
    sendControlMessage(controlMessage, partition, debugInfo, null, DEFAULT_UPSTREAM_OFFSET);
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
    sendControlMessage(controlMessage, partition, debugInfo, null, DEFAULT_UPSTREAM_OFFSET);
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
    for(int partition = 0; partition < numberOfPartitions ; partition ++) {
      sendControlMessage(controlMessage, partition, debugInfo, null, DEFAULT_UPSTREAM_OFFSET);
    }
    logger.info("Successfully broadcasted " + ControlMessageType.valueOf(controlMessage)
        + " Control Message for topic '" + topicName + "'.");
  }

  private Map<CharSequence, CharSequence> getDebugInfo(Map<String, String> debugInfoToAdd) {
    if (null == debugInfoToAdd || debugInfoToAdd.isEmpty()) {
      return defaultDebugInfo;
    }

    // Otherwise, we do a copy of the default info in order to avoid polluting it.
    Map<CharSequence, CharSequence> debugInfo = new HashMap<>(defaultDebugInfo);
    debugInfoToAdd.entrySet().stream().forEach(entry -> {
      String k = entry.getKey();
      String v = entry.getValue();
      CharSequence defaultValue = debugInfo.get(k);
      if (defaultValue != null && !defaultValue.equals(new Utf8(v))) {
        logger.warn("Debug info key '" + k + "' will be omitted because it is already part of the default "
            + this.getClass().getSimpleName() + " debug info. "
            + "Default value: '" + defaultValue + "', "
            + "supplied (omitted) value: '" + v + "'.");
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
   * and retry behavior does not happen in {@link #sendMessage(KeyProvider, MessageType, Object, int, Callback, long)}
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
  public synchronized void sendControlMessage(ControlMessage controlMessage, int partition, Map<String, String> debugInfo, Callback callback, long upstreamOffset) {
    // Work around until we upgrade to a more modern Avro version which supports overriding the
    // String implementation.
    controlMessage.debugInfo = getDebugInfo(debugInfo);
    int attempt = 1;
    boolean updateCheckSum = true;
    while (true) {
      try {
        boolean isEndOfSegment = ControlMessageType.valueOf(controlMessage).equals(ControlMessageType.END_OF_SEGMENT);
        sendMessage(this::getControlMessageKey, MessageType.CONTROL_MESSAGE, controlMessage, isEndOfSegment,
            partition, callback, updateCheckSum, upstreamOffset).get();
        return;
      } catch (InterruptedException|ExecutionException e) {
        if (e.getMessage().contains(Errors.UNKNOWN_TOPIC_OR_PARTITION.message())) {
          /**
           * Not a super clean way to match the exception, but unfortunately, since it is wrapped inside of an
           * {@link ExecutionException}, there may be no other way.
           */
          String errorMessage = "Caught a UNKNOWN_TOPIC_OR_PARTITION error, attempt "
              + attempt + "/" + maxAttemptsWhenTopicMissing;
          if (attempt < maxAttemptsWhenTopicMissing) {
            attempt++;
            updateCheckSum = false; // checksum has already been updated, and should not be updated again for retries
            logger.warn(errorMessage + ", will sleep " + sleepTimeMsWhenTopicMissing + " ms before the next attempt.");
          } else {
            throw new VeniceException(errorMessage + ", will bubble up.");
          }
        } else if (e.getCause() != null && e.getCause().getClass().equals(TopicAuthorizationException.class)) {
          throw new TopicAuthorizationVeniceException("You do not have permission to write to this store. Please check that ACLs are set correctly.", e);
        } else {
          throw new VeniceException("Got an exception while trying to send a control message (" +
              ControlMessageType.valueOf(controlMessage).name() + ")", e);
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
  public synchronized Future<RecordMetadata> asyncSendControlMessage(ControlMessage controlMessage, int partition, Map<String, String> debugInfo, Callback callback, long upstreamOffset) {
    controlMessage.debugInfo = getDebugInfo(debugInfo);
    boolean updateCheckSum = true;
    boolean isEndOfSegment = ControlMessageType.valueOf(controlMessage).equals(ControlMessageType.END_OF_SEGMENT);
    return sendMessage(this::getControlMessageKey, MessageType.CONTROL_MESSAGE, controlMessage, isEndOfSegment,
        partition, callback, updateCheckSum, upstreamOffset);
  }

  /**
   * For control messages, the Key part of the {@link KafkaKey} includes the producer GUID, segment and sequence number.
   *
   * N.B.: This could be optimized further by defining an Avro record to hold this data, since Avro would use
   * variable length encoding for the two integers, which would be smaller than their regular size.
   */
  private static final int CONTROL_MESSAGE_KAFKA_KEY_LENGTH = GUID.class.getAnnotation(FixedSize.class).value() + Integer.SIZE * 2;

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
  protected KafkaMessageEnvelope getKafkaMessageEnvelope(MessageType messageType, boolean isEndOfSegment, int partition,
      boolean incrementSequenceNumber, long upstreamOffset) {
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
    producerMetadata.upstreamOffset = upstreamOffset;
    kafkaValue.producerMetadata = producerMetadata;

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
    if (null == currentSegment || currentSegment.isEnded()) {
      currentSegment = startSegment(partition);
    } else {
      //Close the current segment and create a new one if the current segment is
      //timed out. The segment won't be closed if the ongoing message itself is
      //a "end_of_segment" message.
      if (!sendEndOfSegment) {
        long currentSegmentCreationTime = segmentsCreationTimeMap.get(partition);
        if (currentSegmentCreationTime != -1 && LatencyUtils.getElapsedTimeInMs(currentSegmentCreationTime) > maxElapsedTimeForSegmentInMs) {
          segmentsCreationTimeMap.put(partition, -1L);
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
  private synchronized Segment startSegment(int partition) {
    Segment currentSegment = segmentsMap.get(partition);

    if (null == currentSegment) {
      currentSegment = new Segment(partition, 0, checkSumType);
      segmentsMap.put(partition, currentSegment);
    } else if (currentSegment.isEnded()) {
      int newSegmentNumber = currentSegment.getSegmentNumber() + 1;
      currentSegment = new Segment(partition, newSegmentNumber, checkSumType);
      segmentsMap.put(partition, currentSegment);
    }
    segmentsCreationTimeMap.put(partition, time.getMilliseconds());
    if (!currentSegment.isStarted()) {
      sendStartOfSegment(partition, null);

      currentSegment.start();
    }

    return currentSegment;
  }

  private void endAllSegments(boolean finalSegment) {
    segmentsMap.keySet().stream().forEach(partition -> endSegment(partition, finalSegment));
  }

  /**
   * @param partition in which to end the current segment
   */
  public synchronized void endSegment(int partition, boolean finalSegment) {
    Segment currentSegment = segmentsMap.get(partition);
    if (null == currentSegment) {
      logger.warn("endSegment(partition " + partition + ") called but currentSegment == null. Ignoring.");
    } else if (!currentSegment.isStarted()) {
      logger.warn("endSegment(partition " + partition + ") called but currentSegment.begun == false. Ignoring.");
    } else if (currentSegment.isEnded()) {
      logger.warn("endSegment(partition " + partition + ") called but currentSegment.ended == true. Ignoring.");
    } else {
      logger.info("endSegment(partition " + partition + ") called. Proceeding.");
      sendEndOfSegment(
          partition,
          new HashMap<>(), // TODO: Add extra debugging info
          finalSegment // TODO: This will not always be true, once we support streaming, or more than one segment per mapper in batch
      );
      currentSegment.end(finalSegment);
    }
  }

  private static class KafkaMessageCallback implements Callback {
    private final KafkaKey key;
    private final KafkaMessageEnvelope value;
    private final Logger logger;
    public KafkaMessageCallback(KafkaKey key, KafkaMessageEnvelope value, Logger logger) {
      this.key = key;
      this.value = value;
      this.logger = logger;
    }
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
      if (e != null) {
        logger.error("Failed to send out message to Kafka producer: [value.messageType: " + value.messageType
            + ", value.producerMetadata: " + value.producerMetadata + "]", e);
      }
    }
  }

  /**
   * Compose a CompletableFuture and Callback together to be a {@code CompletableFutureCallback} type.
   * When the {@code CompletableFutureCallback} is called, the {@code CompletableFuture} internal state will be
   * changed and the callback will be called. The caller can pass a {@code CompletableFutureCallback} to a function
   * accepting a {@code Callback} parameter to get a {@code CompletableFuture} after the function returns.
   */
  public static class CompletableFutureCallback implements Callback {
    private final CompletableFuture<Void> completableFuture;
    private Callback callback;

    public CompletableFutureCallback(CompletableFuture<Void> completableFuture) {
      this(completableFuture, null);
    }

    public CompletableFutureCallback(CompletableFuture<Void> completableFuture, Callback callback) {
      this.completableFuture = completableFuture;
      this.callback = callback;
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
      callback.onCompletion(recordMetadata, e);
      if (e == null) {
        completableFuture.complete(null);
      } else {
        completableFuture.completeExceptionally(e);
      }
    }
  }

  public Time getTime() {
    return time;
  }
}
