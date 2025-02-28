package com.linkedin.venice.writer;

import static com.linkedin.venice.ConfigKeys.INSTANCE_ID;
import static com.linkedin.venice.ConfigKeys.LISTENER_PORT;
import static com.linkedin.venice.message.KafkaKey.CONTROL_MESSAGE_KAFKA_KEY_LENGTH;
import static com.linkedin.venice.pubsub.api.PubSubMessageHeaders.VENICE_LEADER_COMPLETION_STATE_HEADER;
import static com.linkedin.venice.pubsub.api.PubSubMessageHeaders.VENICE_TRANSPORT_PROTOCOL_HEADER;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.annotation.Threadsafe;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.RecordTooLargeException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceResourceAccessException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.guid.HeartbeatGuidV3Generator;
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
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.validation.Segment;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pubsub.api.EmptyPubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubMessageHeader;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerAdapter;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicAuthorizationException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.serialization.KeyWithChunkingSuffixSerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.ChunkedValueManifestSerializer;
import com.linkedin.venice.storage.protocol.ChunkedKeySuffix;
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.ExceptionUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.VeniceResourceCloseResult;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Class which acts as the primary writer API.
 */
@Threadsafe
public class VeniceWriter<K, V, U> extends AbstractVeniceWriter<K, V, U> {
  private static final ChunkedPayloadAndManifest EMPTY_CHUNKED_PAYLOAD_AND_MANIFEST =
      new ChunkedPayloadAndManifest(null, null);

  // use for running async close and to fetch number of partitions with timeout from producer
  private final ThreadPoolExecutor threadPoolExecutor;

  // log4j logger
  protected final Logger logger;

  // Config names
  public static final String VENICE_WRITER_CONFIG_PREFIX = "venice.writer.";
  public static final String CLOSE_TIMEOUT_MS = VENICE_WRITER_CONFIG_PREFIX + "close.timeout.ms";
  public static final String CHECK_SUM_TYPE = VENICE_WRITER_CONFIG_PREFIX + "checksum.type";
  public static final String ENABLE_CHUNKING = VENICE_WRITER_CONFIG_PREFIX + "chunking.enabled";
  public static final String ENABLE_RMD_CHUNKING =
      VENICE_WRITER_CONFIG_PREFIX + "replication.metadata.chunking.enabled";
  public static final String MAX_ATTEMPTS_WHEN_TOPIC_MISSING =
      VENICE_WRITER_CONFIG_PREFIX + "max.attemps.when.topic.missing";
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

  /**
   * Maximum Venice record size. Default: {@value UNLIMITED_MAX_RECORD_SIZE}
   *
   * Large records can cause performance issues, so this setting is used to detect and prevent them. Not to be confused
   * with Kafka record size (which is the ~1MB limit {@link VeniceWriter#MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES}
   * is designed to comply with). Venice records refer to a Venice key-value pair, which can be spread across 1+ Kafka
   * records / events. Basically: Chunking Not Needed < ~1MB < Chunking Needed < Max Record Size
   *
   * 1. If a batch push data contains records larger than this setting, the push job will fail.
   * 2. If a partial update creates records larger than this setting, consumption will be paused and manual
   * intervention will be necessary.
   */
  public static final String MAX_RECORD_SIZE_BYTES = VENICE_WRITER_CONFIG_PREFIX + "max.record.size.bytes";

  public static final String PRODUCER_THREAD_COUNT = VENICE_WRITER_CONFIG_PREFIX + "producer.thread.count";
  public static final String PRODUCER_QUEUE_SIZE = VENICE_WRITER_CONFIG_PREFIX + "producer.queue.size";

  // Config value defaults

  /**
   * 950 KB for user payload should be conservative enough.
   */
  public static final int DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES = 950 * 1024;

  /**
   * The default for {@link #maxRecordSizeBytes} is unlimited / unset (-1) just to be safe. A more specific default value
   * should be set using {@link com.linkedin.venice.ConfigKeys#DEFAULT_MAX_RECORD_SIZE_BYTES} the controller
   * config on the cluster level.
   */
  public static final int UNLIMITED_MAX_RECORD_SIZE = -1;

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

  public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  public static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(EMPTY_BYTE_ARRAY);

  public static final LeaderMetadataWrapper DEFAULT_LEADER_METADATA_WRAPPER =
      new LeaderMetadataWrapper(DEFAULT_UPSTREAM_OFFSET, DEFAULT_UPSTREAM_KAFKA_CLUSTER_ID);

  // Immutable state
  private final PubSubMessageHeaders protocolSchemaHeaders;

  protected final VeniceKafkaSerializer<K> keySerializer;
  protected final VeniceKafkaSerializer<V> valueSerializer;
  private final VeniceKafkaSerializer<U> writeComputeSerializer;
  private final PubSubProducerAdapter producerAdapter;
  private final GUID producerGUID;
  private final Time time;
  protected final VenicePartitioner partitioner;
  protected final int numberOfPartitions;
  private final int closeTimeOutInMs;
  private final CheckSumType checkSumType;
  private final int maxSizeForUserPayloadPerMessageInBytes;
  private final int maxAttemptsWhenTopicMissing;
  private final long maxElapsedTimeForSegmentInMs;
  /**
   * Map of partition to {@link Segment}, which keeps track of all segment-related state.
   * IMPORTANT NOTE: Never remove previously maintained segment from the VeniceWriter, it doesn't make sense to reuse
   *                 the VeniceWriter to send new data with a reset producer metadata, follower replicas would think
   *                 new data are "duplicated" because they are using stale producer metadata (e.g. segment number 0
   *                 and sequence number 0)
   */
  private final Segment[] segments;
  /**
   * Map of partition to its segment start time in milliseconds.
   * -1: the current segment is ended
   */
  private final long[] segmentsStartTimeArray;
  private final KeyWithChunkingSuffixSerializer keyWithChunkingSuffixSerializer = new KeyWithChunkingSuffixSerializer();
  private final ChunkedValueManifestSerializer chunkedValueManifestSerializer =
      new ChunkedValueManifestSerializer(true);
  private final Map<CharSequence, CharSequence> defaultDebugInfo;
  private final boolean elapsedTimeForClosingSegmentEnabled;
  private final Object[] partitionLocks;
  private final String writerId;

  public static class DefaultLeaderMetadata extends LeaderMetadata {
    public DefaultLeaderMetadata(CharSequence hostName) {
      this.hostName = hostName;
      this.upstreamOffset = DEFAULT_LEADER_METADATA_WRAPPER.getUpstreamOffset();
      this.upstreamKafkaClusterId = DEFAULT_LEADER_METADATA_WRAPPER.getUpstreamKafkaClusterId();
    }
  }

  /** Used to reduce memory allocation in cases where the metadata is always going to be the same. */
  private final DefaultLeaderMetadata defaultLeaderMetadata;
  private volatile boolean isClosed = false;
  private final Object closeLock = new Object();

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

  private final boolean isRmdChunkingEnabled;
  private final int maxRecordSizeBytes;

  private final ControlMessage heartBeatMessage;

  public VeniceWriter(VeniceWriterOptions params, VeniceProperties props, PubSubProducerAdapter producerAdapter) {
    this(params, props, producerAdapter, KafkaMessageEnvelope.SCHEMA$);
  }

  /**
   * This constructor is currently used only in tests, in order to override the behavior of passing the protocol schema
   * into the header of control messages.
   *
   * @param overrideProtocolSchema The schema to pass in CM headers, or null to omit that header entirely
   */
  public VeniceWriter(
      VeniceWriterOptions params,
      VeniceProperties props,
      PubSubProducerAdapter producerAdapter,
      Schema overrideProtocolSchema) {
    super(params.getTopicName());
    this.keySerializer = params.getKeyPayloadSerializer();
    this.valueSerializer = params.getValuePayloadSerializer();
    this.writeComputeSerializer = params.getWriteComputePayloadSerializer();
    this.time = params.getTime();
    this.partitioner = params.getPartitioner();
    this.closeTimeOutInMs = props.getInt(CLOSE_TIMEOUT_MS, DEFAULT_CLOSE_TIMEOUT_MS);
    this.checkSumType = CheckSumType.valueOf(props.getString(CHECK_SUM_TYPE, DEFAULT_CHECK_SUM_TYPE));
    // override factory chunking settings
    this.isChunkingEnabled = params.isChunkingEnabled();
    this.isChunkingSet = true;
    this.isRmdChunkingEnabled = params.isRmdChunkingEnabled();
    this.maxRecordSizeBytes = params.getMaxRecordSizeBytes();
    this.maxSizeForUserPayloadPerMessageInBytes = props
        .getInt(MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES, DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES);
    if (maxSizeForUserPayloadPerMessageInBytes > DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES) {
      if (!isChunkingEnabled) {
        throw new VeniceException(
            MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES + " (" + maxSizeForUserPayloadPerMessageInBytes
                + ") cannot be set higher than " + DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES + " unless "
                + ENABLE_CHUNKING + " is true");
      } else if (isChunkingEnabled && !Version.isVersionTopic(topicName)) {
        throw new VeniceException(
            MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES + " (" + maxSizeForUserPayloadPerMessageInBytes
                + ") cannot be set higher than " + DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES + " unless "
                + ENABLE_CHUNKING + " is true and the topic is Version Topic");
      }
    }
    if (maxRecordSizeBytes != UNLIMITED_MAX_RECORD_SIZE
        && maxSizeForUserPayloadPerMessageInBytes > maxRecordSizeBytes) {
      throw new VeniceException(
          MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES + " (" + maxSizeForUserPayloadPerMessageInBytes
              + ") cannot be set higher than " + MAX_RECORD_SIZE_BYTES + " (" + maxRecordSizeBytes + ')');
    }
    this.isChunkingFlagInvoked = false;
    this.maxAttemptsWhenTopicMissing =
        props.getInt(MAX_ATTEMPTS_WHEN_TOPIC_MISSING, DEFAULT_MAX_ATTEMPTS_WHEN_TOPIC_MISSING);
    this.maxElapsedTimeForSegmentInMs =
        props.getLong(MAX_ELAPSED_TIME_FOR_SEGMENT_IN_MS, DEFAULT_MAX_ELAPSED_TIME_FOR_SEGMENT_IN_MS);
    this.elapsedTimeForClosingSegmentEnabled = maxElapsedTimeForSegmentInMs > 0;
    this.defaultDebugInfo = Utils.getDebugInfo();

    // if INSTANCE_ID is not set, we'd use "hostname:port" as the default writer id
    if (props.containsKey(INSTANCE_ID)) {
      this.writerId = props.getString(INSTANCE_ID);
    } else if (props.containsKey(LISTENER_PORT)) {
      this.writerId = Utils.getHostName() + ":" + props.getInt(LISTENER_PORT);
    } else {
      this.writerId = Utils.getHostName();
    }
    this.defaultLeaderMetadata = new DefaultLeaderMetadata(this.writerId);
    this.producerGUID = GuidUtils.getGUID(props);
    this.logger = LogManager.getLogger("VeniceWriter [" + GuidUtils.getHexFromGuid(producerGUID) + "]");
    // Create a thread pool which can have max 2 threads.
    // Except during VW start and close we expect it to have zero threads to avoid unnecessary resource usage.
    this.threadPoolExecutor = new ThreadPoolExecutor(
        2,
        2,
        5,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        new DaemonThreadFactory("VW-" + topicName));
    this.threadPoolExecutor.allowCoreThreadTimeOut(true); // allow core threads to timeout

    this.protocolSchemaHeaders = overrideProtocolSchema == null
        ? EmptyPubSubMessageHeaders.SINGLETON
        : new PubSubMessageHeaders()
            .add(VENICE_TRANSPORT_PROTOCOL_HEADER, overrideProtocolSchema.toString().getBytes(StandardCharsets.UTF_8));
    try {
      this.producerAdapter = producerAdapter;
      // We cache the number of partitions, as it is expected to be immutable, and the call to Kafka is expensive.
      // Also avoiding a metadata call to kafka here as the partitionsFor() call sometimes may get blocked indefinitely
      // if the kafka broker is overloaded and does not respond in timely manner.
      if (params.getPartitionCount() != null) {
        this.numberOfPartitions = params.getPartitionCount();
      } else {
        this.numberOfPartitions =
            CompletableFuture.supplyAsync(() -> producerAdapter.getNumberOfPartitions(topicName), threadPoolExecutor)
                .get(30, TimeUnit.SECONDS);
      }
      if (this.numberOfPartitions <= 0) {
        throw new VeniceException("Invalid number of partitions: " + this.numberOfPartitions);
      }
      this.segmentsStartTimeArray = new long[this.numberOfPartitions];
      // Prepare locks for all partitions instead of using map to avoid the searching and creation cost during
      // ingestion.
      this.partitionLocks = new Object[this.numberOfPartitions];
      for (int i = 0; i < numberOfPartitions; i++) {
        partitionLocks[i] = new Object();
        segmentsStartTimeArray[i] = -1L;
      }
      this.segments = new Segment[this.numberOfPartitions];
      OPEN_VENICE_WRITER_COUNT.incrementAndGet();
      heartBeatMessage = generateHeartbeatMessage(checkSumType);
    } catch (Exception e) {
      logger.error("VeniceWriter cannot be constructed with the props: {}", props, e);
      throw new VeniceException("Error while constructing VeniceWriter for store name: " + topicName, e);
    }
  }

  public static ControlMessage generateHeartbeatMessage(CheckSumType checkSumType) {
    ControlMessage heartBeatMessage = new ControlMessage();
    heartBeatMessage.controlMessageType = ControlMessageType.START_OF_SEGMENT.getValue();
    heartBeatMessage.debugInfo = Collections.emptyMap();
    StartOfSegment sos = new StartOfSegment();
    sos.checksumType = checkSumType.getValue();
    sos.upcomingAggregates = new ArrayList<>();
    heartBeatMessage.controlMessageUnion = sos;
    return heartBeatMessage;
  }

  /**
   * Close the {@link VeniceWriter}
   * @param gracefulClose whether to end the segments and send END_OF_SEGMENT control message.
   *
   */
  @Override
  public void close(boolean gracefulClose) {
    try {
      closeAsync(gracefulClose).get();
    } catch (ExecutionException | InterruptedException e) {
      logger.warn("Future completed exceptionally in closing VeniceWriter for topic: {}", topicName, e);
    }
  }

  public CompletableFuture<VeniceResourceCloseResult> closeAsync(boolean gracefulClose) {
    return closeAsync(gracefulClose, true);
  }

  public CompletableFuture<VeniceResourceCloseResult> closeAsync(
      boolean gracefulClose,
      boolean retryOnGracefulCloseFailure) {
    /*
     * If the VeniceWriter is already closed, return a completed future. This is to avoid the case that a Thread pool
     * RejectedExecutionException when a previous closeAsync is executed and the threadPool is already terminated.
     */
    synchronized (closeLock) {
      if (isClosed) {
        return CompletableFuture.completedFuture(VeniceResourceCloseResult.ALREADY_CLOSED);
      }

      return CompletableFuture.supplyAsync(() -> {
        synchronized (closeLock) {
          if (isClosed) {
            return VeniceResourceCloseResult.ALREADY_CLOSED;
          }

          logger.info("Closing VeniceWriter for topic: {}, gracefulness: {}", topicName, gracefulClose);
          long timeAtStartOfClose = System.currentTimeMillis();
          try {
            if (gracefulClose) {
              /**
               * If {@link #broadcastEndOfPush(Map)} was already called, the {@link #endAllSegments(boolean)}
               * will not do anything (it's idempotent). Segments should not be ended if there are still data missing.
               * N.B.: This call is run async because it could block indefinitely in case the topic does not exist.
               */
              CompletableFuture<Void> endSegmentsFuture =
                  CompletableFuture.runAsync(() -> endAllSegments(true), threadPoolExecutor);
              try {
                endSegmentsFuture.get(Math.max(100, closeTimeOutInMs / 2), TimeUnit.MILLISECONDS);
              } catch (Exception e) {
                // cancel the endSegmentsFuture if it's not done in time.
                if (!endSegmentsFuture.isDone()) {
                  endSegmentsFuture.cancel(true);
                }
                logger.warn("Swallowed an exception while trying to end all segments for topic: {}", topicName, e);
              }
            }

            long timeLeftToClose = 0; // N.B.: 0 indicates an ungraceful close
            if (gracefulClose) {
              /** N.B.: We measure the time left to try to adhere strictly to the configured {@link closeTimeOutInMs} */
              long timeSinceStartOfClose = System.currentTimeMillis() - timeAtStartOfClose;
              timeLeftToClose = Math.max(0, closeTimeOutInMs - timeSinceStartOfClose);
            }

            producerAdapter.close(timeLeftToClose);
            OPEN_VENICE_WRITER_COUNT.decrementAndGet();
          } catch (Exception e) {
            handleExceptionInClose(e, gracefulClose, retryOnGracefulCloseFailure);
          } finally {
            isClosed = true;
            threadPoolExecutor.shutdown();
            long elapsedTimeInMs = System.currentTimeMillis() - timeAtStartOfClose;
            logger.info("Closed VeniceWriter for topic: {} in {} ms", topicName, elapsedTimeInMs);
          }
        }
        return VeniceResourceCloseResult.SUCCESS;
      }, threadPoolExecutor);
    }
  }

  void handleExceptionInClose(Exception e, boolean gracefulClose, boolean retryOnGracefulCloseFailure) {
    logger.warn("Swallowed an exception while trying to close the VeniceWriter for topic: {}", topicName, e);
    VENICE_WRITER_CLOSE_FAILED_COUNT.incrementAndGet();

    // For graceful close, swallow the exception and give another try to close it ungracefully.
    try {
      if (gracefulClose && retryOnGracefulCloseFailure) {
        logger.info("Ungracefully closing the VeniceWriter for topic: {}, closeTimeOut: 0 ms", topicName);
        producerAdapter.close(0);
        OPEN_VENICE_WRITER_COUNT.decrementAndGet();
      }
    } catch (Exception ex) {
      // Even ungraceful close fails, give up, swallow exception and move on.
      logger.warn("Exception in ungraceful close for topic: {}", topicName, ex);
      VENICE_WRITER_CLOSE_FAILED_COUNT.incrementAndGet();
    }
  }

  @Override
  public void close() {
    close(true);
  }

  /**
   * Call flush on the internal {@link PubSubProducerAdapter}.
   */
  @Override
  public void flush() {
    producerAdapter.flush();
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "{topicName: " + topicName + ", producerGUID: "
        + GuidUtils.getHexFromGuid(producerGUID) + ", numberOfPartitions: " + numberOfPartitions + "}";
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
   * @return a java.util.concurrent.CompletableFuture Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  public CompletableFuture<PubSubProduceResult> delete(K key, PubSubProducerCallback callback) {
    return delete(key, callback, DEFAULT_LEADER_METADATA_WRAPPER, APP_DEFAULT_LOGICAL_TS, null);
  }

  /**
   * Execute a standard "delete" on the key.
   *
   * @param key - The key to delete in storage.
   * @param logicalTs - An timestamp field to indicate when this record was produced from apps point of view.
   * @param callback - callback will be executed after Kafka producer completes on sending the message.
   * @return a java.util.concurrent.CompletableFuture Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  public CompletableFuture<PubSubProduceResult> delete(K key, long logicalTs, PubSubProducerCallback callback) {
    return delete(key, callback, DEFAULT_LEADER_METADATA_WRAPPER, logicalTs, null);
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
   * @return a java.util.concurrent.CompletableFuture Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  public CompletableFuture<PubSubProduceResult> delete(
      K key,
      PubSubProducerCallback callback,
      LeaderMetadataWrapper leaderMetadataWrapper) {
    return delete(key, callback, leaderMetadataWrapper, APP_DEFAULT_LOGICAL_TS, null);
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
   * @return a java.util.concurrent.CompletableFuture Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  public CompletableFuture<PubSubProduceResult> delete(
      K key,
      PubSubProducerCallback callback,
      LeaderMetadataWrapper leaderMetadataWrapper,
      long logicalTs) {
    return delete(key, callback, leaderMetadataWrapper, logicalTs, null);
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
   * @return a java.util.concurrent.CompletableFuture Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  public CompletableFuture<PubSubProduceResult> delete(
      K key,
      PubSubProducerCallback callback,
      LeaderMetadataWrapper leaderMetadataWrapper,
      DeleteMetadata deleteMetadata) {
    return delete(key, callback, leaderMetadataWrapper, APP_DEFAULT_LOGICAL_TS, deleteMetadata);
  }

  @Override
  public CompletableFuture<PubSubProduceResult> delete(
      K key,
      PubSubProducerCallback callback,
      DeleteMetadata deleteMetadata) {
    return delete(key, callback, DEFAULT_LEADER_METADATA_WRAPPER, APP_DEFAULT_LOGICAL_TS, deleteMetadata);
  }

  /**
   * This method produces a DELETE request to a deprecated chunk key.
   */
  public void deleteDeprecatedChunk(
      byte[] serializedKey,
      int partition,
      PubSubProducerCallback callback,
      LeaderMetadataWrapper leaderMetadataWrapper,
      DeleteMetadata deleteMetadata) {

    KafkaKey kafkaKey = new KafkaKey(MessageType.DELETE, serializedKey);
    Delete delete = new Delete();
    delete.schemaId = AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion();
    if (deleteMetadata == null) {
      delete.replicationMetadataVersionId = VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID;
      delete.replicationMetadataPayload = EMPTY_BYTE_BUFFER;
    } else {
      delete.replicationMetadataVersionId = deleteMetadata.getRmdVersionId();
      delete.replicationMetadataPayload = deleteMetadata.getRmdPayload();
    }
    sendMessage(
        producerMetadata -> kafkaKey,
        MessageType.DELETE,
        delete,
        partition,
        callback,
        leaderMetadataWrapper,
        APP_DEFAULT_LOGICAL_TS);
  }

  private CompletableFuture<PubSubProduceResult> delete(
      K key,
      PubSubProducerCallback callback,
      LeaderMetadataWrapper leaderMetadataWrapper,
      long logicalTs,
      DeleteMetadata deleteMetadata) {
    return delete(key, callback, leaderMetadataWrapper, logicalTs, deleteMetadata, null, null);
  }

  /**
   * Execute a standard "delete" on the key.
   *
   * @param key - The key to delete in storage.
   * @param callback - Callback function invoked by Kafka producer after sending the message.
   * @param leaderMetadataWrapper - The leader Metadata of this message in the source topic:
   *                         -1:  VeniceWriter is sending this message in a Samza app to the real-time topic; or it's
   *                              sending the message in VPJ plugin to the version topic;
   *                         >=0: Leader replica consumes a DELETE message from real-time topic, VeniceWriter in leader
   *                              is sending this message to version topic with extra info: offset in the real-time topic.
   * @param logicalTs - An timestamp field to indicate when this record was produced from apps point of view.
   * @param deleteMetadata - a DeleteMetadata containing replication metadata related fields (can be null).
   * @return a java.util.concurrent.CompletableFuture. Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.CompletableFuture's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  public CompletableFuture<PubSubProduceResult> delete(
      K key,
      PubSubProducerCallback callback,
      LeaderMetadataWrapper leaderMetadataWrapper,
      long logicalTs,
      DeleteMetadata deleteMetadata,
      ChunkedValueManifest oldValueManifest,
      ChunkedValueManifest oldRmdManifest) {
    byte[] serializedKey = keySerializer.serialize(topicName, key);
    int partition = getPartition(serializedKey);
    return delete(
        serializedKey,
        callback,
        leaderMetadataWrapper,
        logicalTs,
        deleteMetadata,
        oldValueManifest,
        oldRmdManifest,
        partition);
  }

  protected CompletableFuture<PubSubProduceResult> delete(
      byte[] serializedKey,
      PubSubProducerCallback callback,
      LeaderMetadataWrapper leaderMetadataWrapper,
      long logicalTs,
      DeleteMetadata deleteMetadata,
      ChunkedValueManifest oldValueManifest,
      ChunkedValueManifest oldRmdManifest,
      int partition) {

    isChunkingFlagInvoked = true;

    int rmdPayloadSize = deleteMetadata == null ? 0 : deleteMetadata.getSerializedSize();
    if (isChunkingNeededForRecord(serializedKey.length + rmdPayloadSize)) {
      throw new RecordTooLargeException(
          "This record exceeds the maximum size. " + getSizeReport(serializedKey.length, 0, rmdPayloadSize));
    }

    if (isChunkingEnabled) {
      serializedKey = keyWithChunkingSuffixSerializer.serializeNonChunkedKey(serializedKey);
    }

    if (callback instanceof ChunkAwareCallback) {
      ((ChunkAwareCallback) callback)
          .setChunkingInfo(serializedKey, null, null, null, null, oldValueManifest, oldRmdManifest);
    }

    KafkaKey kafkaKey = new KafkaKey(MessageType.DELETE, serializedKey);

    Delete delete = new Delete();
    if (deleteMetadata == null) {
      delete.schemaId = VENICE_DEFAULT_VALUE_SCHEMA_ID;
      delete.replicationMetadataVersionId = VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID;
      delete.replicationMetadataPayload = EMPTY_BYTE_BUFFER;
    } else {
      delete.schemaId = deleteMetadata.getValueSchemaId();
      delete.replicationMetadataVersionId = deleteMetadata.getRmdVersionId();
      delete.replicationMetadataPayload = deleteMetadata.getRmdPayload();
    }

    CompletableFuture<PubSubProduceResult> produceResultFuture = sendMessage(
        producerMetadata -> kafkaKey,
        MessageType.DELETE,
        delete,
        partition,
        callback,
        leaderMetadataWrapper,
        logicalTs);
    PubSubProducerCallback chunkCallback = callback == null ? null : new ErrorPropagationCallback(callback);
    DeleteMetadata deleteMetadataForOldChunk =
        new DeleteMetadata(delete.schemaId, delete.replicationMetadataVersionId, VeniceWriter.EMPTY_BYTE_BUFFER);
    deleteDeprecatedChunksFromManifest(
        oldValueManifest,
        partition,
        chunkCallback,
        leaderMetadataWrapper,
        deleteMetadataForOldChunk);
    deleteDeprecatedChunksFromManifest(
        oldRmdManifest,
        partition,
        chunkCallback,
        leaderMetadataWrapper,
        deleteMetadataForOldChunk);

    return produceResultFuture;
  }

  /**
   * Execute a standard "put" on the key.
   *
   * @param key   - The key to put in storage.
   * @param value - The value to be associated with the given key
   * @param valueSchemaId - value schema id for the given value
   * @param callback - Callback function invoked by Kafka producer after sending the message
   * @return a java.util.concurrent.Future. Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  @Override
  public CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      PubSubProducerCallback callback) {
    return put(key, value, valueSchemaId, callback, DEFAULT_LEADER_METADATA_WRAPPER, APP_DEFAULT_LOGICAL_TS, null);
  }

  @Override
  public CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      PubSubProducerCallback callback,
      PutMetadata putMetadata) {
    return put(
        key,
        value,
        valueSchemaId,
        callback,
        DEFAULT_LEADER_METADATA_WRAPPER,
        APP_DEFAULT_LOGICAL_TS,
        putMetadata);
  }

  /**
   * Execute a standard "put" on the key.
   *
   * @param key   - The key to put in storage.
   * @param value - The value to be associated with the given key
   * @param valueSchemaId - value schema id for the given value
   * @param logicalTs - A timestamp field to indicate when this record was produced from apps view.
   * @param callback - Callback function invoked by Kafka producer after sending the message
   * @return a java.util.concurrent.Future Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  public Future<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      long logicalTs,
      PubSubProducerCallback callback) {
    return put(key, value, valueSchemaId, callback, DEFAULT_LEADER_METADATA_WRAPPER, logicalTs, null);
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
  public Future<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      PubSubProducerCallback callback,
      LeaderMetadataWrapper leaderMetadataWrapper) {
    return put(key, value, valueSchemaId, callback, leaderMetadataWrapper, APP_DEFAULT_LOGICAL_TS, null);
  }

  public Future<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      PubSubProducerCallback callback,
      LeaderMetadataWrapper leaderMetadataWrapper,
      ChunkedValueManifest oldValueManifest,
      ChunkedValueManifest oldRmdManifest) {
    return put(
        key,
        value,
        valueSchemaId,
        callback,
        leaderMetadataWrapper,
        APP_DEFAULT_LOGICAL_TS,
        null,
        oldValueManifest,
        oldRmdManifest);
  }

  public CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      PubSubProducerCallback callback,
      LeaderMetadataWrapper leaderMetadataWrapper,
      long logicalTs,
      PutMetadata putMetadata) {
    return put(key, value, valueSchemaId, callback, leaderMetadataWrapper, logicalTs, putMetadata, null, null);
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
   * @param putMetadata - a PutMetadata containing replication metadata related fields (can be null).
   * @return a java.util.concurrent.Future Future for the RecordMetadata that will be assigned to this
   * record. Invoking java.util.concurrent.Future's get() on this future will block until the associated request
   * completes and then return the metadata for the record or throw any exception that occurred while sending the record.
   */
  public CompletableFuture<PubSubProduceResult> put(
      K key,
      V value,
      int valueSchemaId,
      PubSubProducerCallback callback,
      LeaderMetadataWrapper leaderMetadataWrapper,
      long logicalTs,
      PutMetadata putMetadata,
      ChunkedValueManifest oldValueManifest,
      ChunkedValueManifest oldRmdManifest) {
    byte[] serializedKey = keySerializer.serialize(topicName, key);
    byte[] serializedValue = valueSerializer.serialize(topicName, value);
    int partition = getPartition(serializedKey);
    return put(
        serializedKey,
        serializedValue,
        partition,
        valueSchemaId,
        callback,
        leaderMetadataWrapper,
        logicalTs,
        putMetadata,
        oldValueManifest,
        oldRmdManifest);
  }

  /**
   * Write a message with the kafka message envelope (KME) passed in. This allows users re-using existing KME to
   * speed up the performance. If this is called, VeniceWriter will also reuse the existing DIV data (producer
   * metadata). It's the "pass-through" mode.
   *
   * TODO: move pass-through supports into a server-specific extension of VeniceWriter
   */
  @Deprecated
  public Future<PubSubProduceResult> put(
      KafkaKey kafkaKey,
      KafkaMessageEnvelope kafkaMessageEnvelope,
      PubSubProducerCallback callback,
      int upstreamPartition,
      LeaderMetadataWrapper leaderMetadataWrapper) {
    // Self-adjust the chunking setting in pass-through mode
    verifyChunkingSetting(kafkaMessageEnvelope);

    byte[] serializedKey = kafkaKey.getKey();

    KafkaMessageEnvelopeProvider kafkaMessageEnvelopeProvider =
        getKafkaMessageEnvelopeProvider(kafkaMessageEnvelope, leaderMetadataWrapper);

    if (callback instanceof ChunkAwareCallback) {
      ((ChunkAwareCallback) callback).setChunkingInfo(serializedKey, null, null, null, null, null, null);
    }

    return sendMessage(producerMetadata -> kafkaKey, kafkaMessageEnvelopeProvider, upstreamPartition, callback, false);
  }

  /**
   * Write a record with new DIV to a predetermined partition.
   */
  protected CompletableFuture<PubSubProduceResult> put(
      byte[] serializedKey,
      byte[] serializedValue,
      int partition,
      int valueSchemaId,
      PubSubProducerCallback callback,
      LeaderMetadataWrapper leaderMetadataWrapper,
      long logicalTs,
      PutMetadata putMetadata,
      ChunkedValueManifest oldValueManifest,
      ChunkedValueManifest oldRmdManifest) {
    int replicationMetadataPayloadSize = putMetadata == null ? 0 : putMetadata.getSerializedSize();
    isChunkingFlagInvoked = true;

    /**
     * {@link RecordTooLargeException} will be thrown unless the record size fits within one of the following categories:
     * Chunking Not Needed < ~1MB < Chunking Needed < MAX_RECORD_SIZE_BYTES
     */
    int veniceRecordSize = serializedKey.length + serializedValue.length + replicationMetadataPayloadSize;
    if (isChunkingNeededForRecord(veniceRecordSize)) { // ~1MB default
      // RMD size is not checked because it's an internal component, and a user's write should not be failed due to it
      if (isChunkingEnabled && !isRecordTooLarge(serializedKey.length + serializedValue.length)) {
        return putLargeValue(
            serializedKey,
            serializedValue,
            valueSchemaId,
            callback,
            partition,
            leaderMetadataWrapper,
            logicalTs,
            putMetadata,
            oldValueManifest,
            oldRmdManifest);
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
      ((ChunkAwareCallback) callback)
          .setChunkingInfo(serializedKey, null, null, null, null, oldValueManifest, oldRmdManifest);
    }

    KafkaKey kafkaKey = new KafkaKey(MessageType.PUT, serializedKey);

    // Initialize the SpecificRecord instances used by the Avro-based Kafka protocol
    Put putPayload = buildPutPayload(serializedValue, valueSchemaId, putMetadata);
    CompletableFuture<PubSubProduceResult> produceResultFuture = sendMessage(
        producerMetadata -> kafkaKey,
        MessageType.PUT,
        putPayload,
        partition,
        callback,
        leaderMetadataWrapper,
        logicalTs);

    DeleteMetadata deleteMetadata =
        new DeleteMetadata(valueSchemaId, putPayload.replicationMetadataVersionId, VeniceWriter.EMPTY_BYTE_BUFFER);
    PubSubProducerCallback chunkCallback = callback == null ? null : new ErrorPropagationCallback(callback);
    deleteDeprecatedChunksFromManifest(
        oldValueManifest,
        partition,
        chunkCallback,
        leaderMetadataWrapper,
        deleteMetadata);
    deleteDeprecatedChunksFromManifest(oldRmdManifest, partition, chunkCallback, leaderMetadataWrapper, deleteMetadata);

    return produceResultFuture;
  }

  /**
   * This is the main method to send DIV messages to a kafka topic through VeniceWriter. The method decides whether to
   * send the messages in chunked or non-chunked mode based on the size of the message. Today, DIV is the only user of
   * this method, but it can be extended easily to support other class types in the future.
   *
   * All the messages sent through this method are of type {@link MessageType#GLOBAL_RT_DIV} in its KafkaKey and
   * all their corresponding {@link KafkaMessageEnvelope} uses {@link Put} as the payload. Inside the Put payload, the
   * actual message is stored in the putValue field and the schema id has 3 cases:
   *
   * 1. If the message is non-chunked, the schema id is set to {@link AvroProtocolDefinition#GLOBAL_RT_DIV_STATE}.
   * 2. If the message is chunk message, the schema id is set to {@link AvroProtocolDefinition#CHUNK}.
   * 3. If the message is a chunk manifest message, the schema id is set to {@link AvroProtocolDefinition#CHUNKED_VALUE_MANIFEST}.
   */
  public CompletableFuture<PubSubProduceResult> sendGlobalRtDivMessage(int partition, K key, V value) {
    if (partition < 0 || partition >= numberOfPartitions) {
      throw new VeniceException("Invalid partition: " + partition);
    }

    byte[] serializedKey = keySerializer.serialize(topicName, key);
    byte[] serializedValue = valueSerializer.serialize(topicName, value);

    if (isChunkingNeededForRecord(serializedKey.length + serializedValue.length)) {
      return sendChunkedGlobalRtDivMessage(partition, serializedKey, serializedValue);
    }

    serializedKey = keyWithChunkingSuffixSerializer.serializeNonChunkedKey(serializedKey);
    KafkaKey divKey = new KafkaKey(MessageType.GLOBAL_RT_DIV, serializedKey);

    // Initialize the SpecificRecord instances used by the Avro-based Kafka protocol
    Put putPayload =
        buildPutPayload(serializedValue, AvroProtocolDefinition.GLOBAL_RT_DIV_STATE.getCurrentProtocolVersion(), null);

    // TODO: This needs to be implemented later to support Global RT DIV
    final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
    PubSubProducerCallback callback = new CompletableFutureCallback(completableFuture);

    return sendMessage(
        producerMetadata -> divKey,
        MessageType.PUT,
        putPayload,
        partition,
        callback,
        DEFAULT_LEADER_METADATA_WRAPPER,
        APP_DEFAULT_LOGICAL_TS);
  }

  private CompletableFuture<PubSubProduceResult> sendChunkedGlobalRtDivMessage(
      int partition,
      byte[] serializedKey,
      byte[] serializedValue) {
    final Supplier<String> reportSizeGenerator = () -> getSizeReport(serializedKey.length, serializedValue.length, 0);
    // TODO: This needs to be implemented later to support Global RT DIV.
    final CompletableFuture<Void> completableFuture = new CompletableFuture<>();
    PubSubProducerCallback callback = new ErrorPropagationCallback(new CompletableFutureCallback(completableFuture));
    BiConsumer<KeyProvider, Put> sendMessageFunction = (keyProvider, putPayload) -> sendMessage(
        keyProvider,
        MessageType.PUT,
        putPayload,
        partition,
        callback,
        DEFAULT_LEADER_METADATA_WRAPPER,
        VENICE_DEFAULT_LOGICAL_TS);

    ChunkedPayloadAndManifest valueChunksAndManifest = WriterChunkingHelper.chunkPayloadAndSend(
        serializedKey,
        serializedValue,
        MessageType.GLOBAL_RT_DIV,
        true,
        AvroProtocolDefinition.GLOBAL_RT_DIV_STATE.getCurrentProtocolVersion(),
        0,
        false,
        reportSizeGenerator,
        maxSizeForUserPayloadPerMessageInBytes,
        keyWithChunkingSuffixSerializer,
        sendMessageFunction);

    final int sizeAvailablePerMessage = maxSizeForUserPayloadPerMessageInBytes - serializedKey.length;
    Put manifestPayload =
        buildManifestPayload(null, null, valueChunksAndManifest, sizeAvailablePerMessage, reportSizeGenerator);
    return sendManifestMessage(
        manifestPayload,
        serializedKey,
        MessageType.GLOBAL_RT_DIV,
        valueChunksAndManifest,
        callback,
        null,
        partition,
        null,
        null,
        DEFAULT_LEADER_METADATA_WRAPPER,
        APP_DEFAULT_LOGICAL_TS);
  }

  private Put buildPutPayload(byte[] serializedValue, int valueSchemaId, PutMetadata putMetadata) {
    Put putPayload = new Put();
    putPayload.putValue = ByteBuffer.wrap(serializedValue);
    putPayload.schemaId = valueSchemaId;

    if (putMetadata == null) {
      putPayload.replicationMetadataVersionId = VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID;
      putPayload.replicationMetadataPayload = EMPTY_BYTE_BUFFER;
    } else {
      putPayload.replicationMetadataVersionId = putMetadata.getRmdVersionId();
      putPayload.replicationMetadataPayload = putMetadata.getRmdPayload();
    }
    return putPayload;
  }

  private KafkaMessageEnvelopeProvider getKafkaMessageEnvelopeProvider(
      KafkaMessageEnvelope kafkaMessageEnvelope,
      LeaderMetadataWrapper leaderMetadataWrapper) {
    LeaderMetadata leaderMetadata = new LeaderMetadata();
    leaderMetadata.upstreamOffset = leaderMetadataWrapper.getUpstreamOffset();
    leaderMetadata.upstreamKafkaClusterId = leaderMetadataWrapper.getUpstreamKafkaClusterId();
    leaderMetadata.hostName = writerId;
    kafkaMessageEnvelope.leaderMetadataFooter = leaderMetadata;

    return () -> kafkaMessageEnvelope;
  }

  /**
   * DIV pass-through mode for delete
   */
  public Future<PubSubProduceResult> delete(
      KafkaKey kafkaKey,
      KafkaMessageEnvelope kafkaMessageEnvelope,
      PubSubProducerCallback callback,
      int upstreamPartition,
      LeaderMetadataWrapper leaderMetadataWrapper) {
    // Self-adjust the chunking setting in pass-through mode
    verifyChunkingSetting(kafkaMessageEnvelope);

    KafkaMessageEnvelopeProvider kafkaMessageEnvelopeProvider =
        getKafkaMessageEnvelopeProvider(kafkaMessageEnvelope, leaderMetadataWrapper);

    if (callback instanceof ChunkAwareCallback) {
      byte[] serializedKey = kafkaKey.getKey();
      ((ChunkAwareCallback) callback).setChunkingInfo(serializedKey, null, null, null, null, null, null);
    }

    return sendMessage(producerMetadata -> kafkaKey, kafkaMessageEnvelopeProvider, upstreamPartition, callback, false);
  }

  @Override
  public Future<PubSubProduceResult> update(
      K key,
      U update,
      int valueSchemaId,
      int derivedSchemaId,
      PubSubProducerCallback callback) {
    return update(key, update, valueSchemaId, derivedSchemaId, callback, APP_DEFAULT_LOGICAL_TS);
  }

  public Future<PubSubProduceResult> update(
      K key,
      U update,
      int valueSchemaId,
      int derivedSchemaId,
      PubSubProducerCallback callback,
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
        logicalTs);
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
    producerAdapter.flush();
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
    producerAdapter.flush();
  }

  /**
   * Broadcast control message to real-time topic partition, to be consumed by venice leader. Partition high watermarks
   * are left to local venice leader to prepare and then been produced to version topic partition.
   *
   * @param oldServingVersionTopic the version topic change capture consumer should switch from.
   * @param newServingVersionTopic the version topic change capture consumer should switch to.
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   */
  public void broadcastVersionSwap(
      @Nonnull String oldServingVersionTopic,
      @Nonnull String newServingVersionTopic,
      Map<String, String> debugInfo) {
    Validate.notEmpty(oldServingVersionTopic);
    Validate.notEmpty(newServingVersionTopic);
    ControlMessage controlMessage = getEmptyControlMessage(ControlMessageType.VERSION_SWAP);
    VersionSwap versionSwap = new VersionSwap();
    versionSwap.oldServingVersionTopic = oldServingVersionTopic;
    versionSwap.newServingVersionTopic = newServingVersionTopic;
    controlMessage.controlMessageUnion = versionSwap;
    broadcastControlMessage(controlMessage, debugInfo);
    producerAdapter.flush();
  }

  public void broadcastStartOfIncrementalPush(String version, Map<String, String> debugInfo) {
    ControlMessage controlMessage = getEmptyControlMessage(ControlMessageType.START_OF_INCREMENTAL_PUSH);
    StartOfIncrementalPush startOfIncrementalPush = new StartOfIncrementalPush();
    startOfIncrementalPush.version = version;
    controlMessage.controlMessageUnion = startOfIncrementalPush;
    broadcastControlMessage(controlMessage, debugInfo);
    producerAdapter.flush();
  }

  public void broadcastEndOfIncrementalPush(String version, Map<String, String> debugInfo) {
    ControlMessage controlMessage = getEmptyControlMessage(ControlMessageType.END_OF_INCREMENTAL_PUSH);
    EndOfIncrementalPush endOfIncrementalPush = new EndOfIncrementalPush();
    endOfIncrementalPush.version = version;
    controlMessage.controlMessageUnion = endOfIncrementalPush;
    broadcastControlMessage(controlMessage, debugInfo);
    producerAdapter.flush();
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
    if (segments[partition] != null) {
      logger.debug("Closing partition: {} in VeniceWriter.", partition);
      try {
        endSegment(partition, true).get();
      } catch (InterruptedException | ExecutionException e) {
        handleControlMessageProducingException(e, ControlMessageType.END_OF_SEGMENT);
      }
    }
  }

  /**
   * Data message like PUT and DELETE should call this API to enable DIV check.
   */
  private CompletableFuture<PubSubProduceResult> sendMessage(
      KeyProvider keyProvider,
      MessageType messageType,
      Object payload,
      int partition,
      PubSubProducerCallback callback,
      LeaderMetadataWrapper leaderMetadataWrapper,
      long logicalTs) {
    synchronized (this.partitionLocks[partition]) {
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
  }

  private CompletableFuture<PubSubProduceResult> sendMessage(
      KeyProvider keyProvider,
      MessageType messageType,
      Object payload,
      boolean isEndOfSegment,
      int partition,
      PubSubProducerCallback callback,
      boolean updateDIV,
      LeaderMetadataWrapper leaderMetadataWrapper,
      long logicalTs) {
    synchronized (this.partitionLocks[partition]) {
      KafkaMessageEnvelopeProvider kafkaMessageEnvelopeProvider = () -> {
        KafkaMessageEnvelope kafkaValue = getKafkaMessageEnvelope(
            messageType,
            isEndOfSegment,
            partition,
            updateDIV,
            leaderMetadataWrapper,
            logicalTs);
        kafkaValue.payloadUnion = payload;
        return kafkaValue;
      };
      return sendMessage(keyProvider, kafkaMessageEnvelopeProvider, partition, callback, updateDIV);
    }
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
   *         @see {@link #sendControlMessage(ControlMessage, int, Map, PubSubProducerCallback, LeaderMetadataWrapper)}
   *
   * P.S. 2: If there is too much contention on this lock, then we can consider a finer locking strategy, where the
   *         locking is per-partition, which would also be correct as far as DIV is concerned.
   *
   * @param updateDIV if true, the partition's segment's checksum will be updated and its sequence number incremented
   *                  if false, the checksum and seq# update are omitted, which is the right thing to do during retries
   */
  private CompletableFuture<PubSubProduceResult> sendMessage(
      KeyProvider keyProvider,
      KafkaMessageEnvelopeProvider valueProvider,
      int partition,
      PubSubProducerCallback callback,
      boolean updateDIV) {
    synchronized (this.partitionLocks[partition]) {
      KafkaMessageEnvelope kafkaValue = valueProvider.getKafkaMessageEnvelope();
      KafkaKey key = keyProvider.getKey(kafkaValue.producerMetadata);
      if (updateDIV) {
        Segment segment = segments[partition];
        if (segment == null) {
          throw new VeniceException("segmentMap does not contain partition " + partition + " for topic " + topicName);
        }
        segment.addToCheckSum(key, kafkaValue);
      }
      PubSubProducerCallback messageCallback = callback;
      if (callback == null) {
        messageCallback = new SendMessageErrorLoggerCallback(kafkaValue, logger);
      } else if (callback instanceof CompletableFutureCallback) {
        CompletableFutureCallback completableFutureCallBack = (CompletableFutureCallback) callback;
        if (completableFutureCallBack.getCallback() == null) {
          completableFutureCallBack.setCallback(new SendMessageErrorLoggerCallback(kafkaValue, logger));
        }
      }
      try {
        return producerAdapter.sendMessage(
            topicName,
            partition,
            key,
            kafkaValue,
            getHeaders(kafkaValue.getProducerMetadata()),
            messageCallback);
      } catch (Exception e) {
        if (ExceptionUtils.recursiveClassEquals(e, PubSubTopicAuthorizationException.class)) {
          throw new VeniceResourceAccessException(
              "You do not have permission to write to this store. Please check that ACLs are set correctly.",
              e);
        } else {
          throw e;
        }
      }
    }
  }

  /**
   * We only include the protocol schema headers on this writer's first message to each partition.
   */
  private PubSubMessageHeaders getHeaders(ProducerMetadata producerMetadata) {
    return getHeaders(producerMetadata, false, LeaderCompleteState.LEADER_NOT_COMPLETED);
  }

  /**
   * {@link PubSubMessageHeaders#VENICE_TRANSPORT_PROTOCOL_HEADER} or {@link EmptyPubSubMessageHeaders} is used for
   * all messages to a partition based on {@link VeniceWriter} param overrideProtocolSchema and whether it's a first message.
   * {@link PubSubMessageHeaders#VENICE_LEADER_COMPLETION_STATE_HEADER} is added to the above headers for HB SOS message.
   *
   * Note: In theory, we can enumerate all the possible headers like below, so that we don't need to create a new header every time
   * like how it's created for HB SOS in this method. But as it's only for HB SOS, we can ignore such optimization. But if we
   * add any extra headers that will be used in the hot code paths, we should consider creating these headers in advance.
   *
   * Protocol header + leader complete header (LEADER_COMPLETED)
   * Protocol header + leader complete header (LEADER_NOT_COMPLETED)
   * leader complete header (LEADER_COMPLETED)
   * leader complete header (LEADER_NOT_COMPLETED)
   */
  private PubSubMessageHeaders getHeaders(
      ProducerMetadata producerMetadata,
      boolean addLeaderCompleteState,
      LeaderCompleteState leaderCompleteState) {
    PubSubMessageHeaders returnPubSubMessageHeaders;
    PubSubMessageHeaders pubSubMessageHeaders =
        producerMetadata.getSegmentNumber() == 0 && producerMetadata.getMessageSequenceNumber() == 0
            ? protocolSchemaHeaders
            : EmptyPubSubMessageHeaders.SINGLETON;

    if (addLeaderCompleteState) {
      // copy protocolSchemaHeaders locally and add extra header for leaderCompleteState
      returnPubSubMessageHeaders = new PubSubMessageHeaders();
      for (PubSubMessageHeader header: pubSubMessageHeaders) {
        returnPubSubMessageHeaders.add(header);
      }
      returnPubSubMessageHeaders.add(getLeaderCompleteStateHeader(leaderCompleteState));
    } else {
      returnPubSubMessageHeaders = pubSubMessageHeaders;
    }
    return returnPubSubMessageHeaders;
  }

  public static PubSubMessageHeader getLeaderCompleteStateHeader(LeaderCompleteState leaderCompleteState) {
    // 1 byte holding 0/1 based on leaderCompleteState
    byte[] val = new byte[1];
    val[0] = (byte) (leaderCompleteState.getValue());
    return new PubSubMessageHeader(VENICE_LEADER_COMPLETION_STATE_HEADER, val);
  }

  /**
   * An interface which enables the key to contain parts of the {@param producerMetadata} within it, which is
   * useful for control messages and chunked values.
   */
  public interface KeyProvider {
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
  protected CompletableFuture<PubSubProduceResult> putLargeValue(
      byte[] serializedKey,
      byte[] serializedValue,
      int valueSchemaId,
      PubSubProducerCallback callback,
      int partition,
      LeaderMetadataWrapper leaderMetadataWrapper,
      long logicalTs,
      PutMetadata putMetadata,
      ChunkedValueManifest oldValueManifest,
      ChunkedValueManifest oldRmdManifest) {
    int replicationMetadataPayloadSize = putMetadata == null ? 0 : putMetadata.getSerializedSize();
    final Supplier<String> reportSizeGenerator =
        () -> getSizeReport(serializedKey.length, serializedValue.length, replicationMetadataPayloadSize);
    PubSubProducerCallback chunkCallback = callback == null ? null : new ErrorPropagationCallback(callback);
    BiConsumer<KeyProvider, Put> sendMessageFunction = (keyProvider, putPayload) -> sendMessage(
        keyProvider,
        MessageType.PUT,
        putPayload,
        partition,
        chunkCallback,
        DEFAULT_LEADER_METADATA_WRAPPER,
        VENICE_DEFAULT_LOGICAL_TS);
    ChunkedPayloadAndManifest valueChunksAndManifest = WriterChunkingHelper.chunkPayloadAndSend(
        serializedKey,
        serializedValue,
        MessageType.PUT,
        true,
        valueSchemaId,
        0,
        callback instanceof ChunkAwareCallback,
        reportSizeGenerator,
        maxSizeForUserPayloadPerMessageInBytes,
        keyWithChunkingSuffixSerializer,
        sendMessageFunction);
    int valueChunkCount = valueChunksAndManifest.getChunkedValueManifest().keysWithChunkIdSuffix.size();
    ChunkedPayloadAndManifest rmdChunksAndManifest = isRmdChunkingEnabled
        ? WriterChunkingHelper.chunkPayloadAndSend(
            serializedKey,
            putMetadata == null ? EMPTY_BYTE_ARRAY : ByteUtils.extractByteArray(putMetadata.getRmdPayload()),
            MessageType.PUT,
            false,
            valueSchemaId,
            valueChunkCount,
            callback instanceof ChunkAwareCallback,
            reportSizeGenerator,
            maxSizeForUserPayloadPerMessageInBytes,
            keyWithChunkingSuffixSerializer,
            sendMessageFunction)
        : EMPTY_CHUNKED_PAYLOAD_AND_MANIFEST;

    final int sizeAvailablePerMessage = maxSizeForUserPayloadPerMessageInBytes - serializedKey.length;
    Put putManifestsPayload = buildManifestPayload(
        rmdChunksAndManifest,
        putMetadata,
        valueChunksAndManifest,
        sizeAvailablePerMessage,
        reportSizeGenerator);
    CompletableFuture<PubSubProduceResult> manifestProduceFuture = sendManifestMessage(
        putManifestsPayload,
        serializedKey,
        MessageType.PUT,
        valueChunksAndManifest,
        callback,
        rmdChunksAndManifest,
        partition,
        oldValueManifest,
        oldRmdManifest,
        leaderMetadataWrapper,
        logicalTs);

    DeleteMetadata deleteMetadata = new DeleteMetadata(
        valueSchemaId,
        putManifestsPayload.replicationMetadataVersionId,
        VeniceWriter.EMPTY_BYTE_BUFFER);
    deleteDeprecatedChunksFromManifest(
        oldValueManifest,
        partition,
        chunkCallback,
        leaderMetadataWrapper,
        deleteMetadata);
    deleteDeprecatedChunksFromManifest(oldRmdManifest, partition, chunkCallback, leaderMetadataWrapper, deleteMetadata);

    return manifestProduceFuture;
  }

  private CompletableFuture<PubSubProduceResult> sendManifestMessage(
      Object manifestPayload,
      byte[] serializedKey,
      MessageType keyType,
      ChunkedPayloadAndManifest valueChunksAndManifest,
      PubSubProducerCallback callback,
      ChunkedPayloadAndManifest rmdChunksAndManifest,
      int partition,
      ChunkedValueManifest oldValueManifest,
      ChunkedValueManifest oldRmdManifest,
      LeaderMetadataWrapper leaderMetadataWrapper,
      long logicalTs) {
    // Now that we've sent all the chunks, we can take care of the final value, the manifest.
    byte[] topLevelKey = keyWithChunkingSuffixSerializer.serializeNonChunkedKey(serializedKey);
    KeyProvider manifestKeyProvider = producerMetadata -> new KafkaKey(keyType, topLevelKey);

    if (callback instanceof ChunkAwareCallback) {
      /** We leave a handle to the key, chunks and manifests so that the {@link ChunkAwareCallback} can act on them */
      ((ChunkAwareCallback) callback).setChunkingInfo(
          topLevelKey,
          valueChunksAndManifest.getPayloadChunks(),
          valueChunksAndManifest.getChunkedValueManifest(),
          rmdChunksAndManifest.getPayloadChunks(),
          rmdChunksAndManifest.getChunkedValueManifest(),
          oldValueManifest,
          oldRmdManifest);
    }

    // We only return the last future (the one for the manifest) and assume that once this one is finished,
    // all the chunks should also be finished, since they were sent first, and ordering should be guaranteed.
    return sendMessage(
        manifestKeyProvider,
        MessageType.PUT,
        manifestPayload,
        partition,
        callback,
        leaderMetadataWrapper,
        logicalTs);
  }

  private Put buildManifestPayload(
      ChunkedPayloadAndManifest rmdChunksAndManifest,
      PutMetadata putMetadata,
      ChunkedPayloadAndManifest valueChunksAndManifest,
      int sizeAvailablePerMessage,
      Supplier<String> reportSizeGenerator) {
    Put putManifestsPayload = new Put();
    putManifestsPayload.putValue =
        chunkedValueManifestSerializer.serialize(valueChunksAndManifest.getChunkedValueManifest());
    putManifestsPayload.schemaId = AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion();
    if (putMetadata == null) {
      putManifestsPayload.replicationMetadataVersionId = VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID;
      putManifestsPayload.replicationMetadataPayload = EMPTY_BYTE_BUFFER;
    } else {
      putManifestsPayload.replicationMetadataVersionId = putMetadata.getRmdVersionId();
      putManifestsPayload.replicationMetadataPayload = isRmdChunkingEnabled
          ? chunkedValueManifestSerializer.serialize(rmdChunksAndManifest.getChunkedValueManifest())
          : putMetadata.getRmdPayload();
    }
    if (putManifestsPayload.putValue.remaining()
        + putManifestsPayload.replicationMetadataPayload.remaining() > sizeAvailablePerMessage) {
      // This is a very desperate edge case...
      throw new VeniceException(
          "This message cannot be chunked, because even its manifest is too big to go through. "
              + "Please reconsider your life choices. " + reportSizeGenerator.get());
    }
    return putManifestsPayload;
  }

  /**
   * This method iterates over a {@link ChunkedValueManifest} object's chunk key list and issue DELETE request for each
   * chunk.
   */
  private void deleteDeprecatedChunksFromManifest(
      ChunkedValueManifest manifest,
      int partition,
      PubSubProducerCallback chunkCallback,
      LeaderMetadataWrapper leaderMetadataWrapper,
      DeleteMetadata deleteMetadata) {
    if (manifest == null) {
      return;
    }
    for (int i = 0; i < manifest.keysWithChunkIdSuffix.size(); i++) {
      byte[] chunkKeyBytes = manifest.keysWithChunkIdSuffix.get(i).array();
      deleteDeprecatedChunk(chunkKeyBytes, partition, chunkCallback, leaderMetadataWrapper, deleteMetadata);
    }
  }

  private String getSizeReport(int serializedKeySize, int serializedValueSize, int replicationMetadataPayloadSize) {
    return "Key size: " + serializedKeySize + " bytes, " + "Value size: " + serializedValueSize + " bytes, "
        + "Replication Metadata size: " + replicationMetadataPayloadSize + " bytes, " + "Total payload size: "
        + (serializedKeySize + serializedValueSize + replicationMetadataPayloadSize) + " bytes, "
        + "Max available payload size: " + maxSizeForUserPayloadPerMessageInBytes + " bytes, " + "Max record size: "
        + ((maxRecordSizeBytes == UNLIMITED_MAX_RECORD_SIZE) ? "unlimited" : maxRecordSizeBytes) + " bytes.";
  }

  /**
   * Send a {@link ControlMessageType#START_OF_SEGMENT} control message into the designated partition.
   *
   * @param partition the Kafka partition to write to.
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   */
  public void sendStartOfSegment(int partition, Map<String, String> debugInfo) {
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = ControlMessageType.START_OF_SEGMENT.getValue();
    StartOfSegment startOfSegment = new StartOfSegment();
    startOfSegment.checksumType = checkSumType.getValue();
    startOfSegment.upcomingAggregates = new ArrayList<>(); // TODO Add extra aggregates
    controlMessage.controlMessageUnion = startOfSegment;
    sendControlMessageWithRetriesForNonExistentTopic(
        controlMessage,
        partition,
        debugInfo,
        null,
        DEFAULT_LEADER_METADATA_WRAPPER);
  }

  /**
   * Send a {@link ControlMessageType#END_OF_SEGMENT} control message into the designated partition.
   *
   * @param partition the Kafka partition to write to.
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   * @param finalSegment a boolean indicating if this is the final segment that this producer will send
   *                     into this partition.
   */
  private CompletableFuture<PubSubProduceResult> sendEndOfSegment(
      int partition,
      Map<String, String> debugInfo,
      boolean finalSegment) {
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = ControlMessageType.END_OF_SEGMENT.getValue();
    EndOfSegment endOfSegment = new EndOfSegment();
    endOfSegment.checksumValue = ByteBuffer.wrap(segments[partition].getFinalCheckSum());
    endOfSegment.computedAggregates = new ArrayList<>(); // TODO Add extra aggregates
    endOfSegment.finalSegment = finalSegment;
    controlMessage.controlMessageUnion = endOfSegment;
    return sendControlMessage(controlMessage, partition, debugInfo, null, DEFAULT_LEADER_METADATA_WRAPPER);
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
   * If the Kafka topic does not exist, this function will try again for a total of {@link #maxAttemptsWhenTopicMissing}
   * attempts. Note that this retry behavior does not happen in
   * {@link #sendMessage(KeyProvider, MessageType, Object, int, PubSubProducerCallback, LeaderMetadataWrapper, long)}
   * because that function returns a {@link Future}, and it is {@link Future#get()} which throws the relevant
   * exception. In any case, the topic should be seeded with a {@link ControlMessageType#START_OF_SEGMENT}
   * at first, and therefore, there should be no cases where a topic has not been created yet, and we attempt
   * to write a data message first, prior to a control message. If a topic did disappear later on in the
   * {@link VeniceWriter}'s lifecycle, then it would be appropriate to let that {@link Future} fail.
   *
   * This function has a synchronized block because if the retries need to be exercised, then it would cause a DIV
   * failure if another message slipped in after the first attempt and before the eventually successful attempt.
   *
   * @param controlMessage a {@link ControlMessage} instance to persist into Kafka.
   * @param partition the Kafka partition to write to.
   * @param debugInfo arbitrary key/value pairs of information that will be propagated alongside the control message.
   * @param callback callback to execute when the record has been acknowledged by the Kafka server (null means no callback)
   */
  public void sendControlMessageWithRetriesForNonExistentTopic(
      ControlMessage controlMessage,
      int partition,
      Map<String, String> debugInfo,
      PubSubProducerCallback callback,
      LeaderMetadataWrapper leaderMetadataWrapper) {
    controlMessage.debugInfo = getDebugInfo(debugInfo);
    int attempt = 1;
    boolean updateCheckSum = true;
    ControlMessageType controlMessageType = ControlMessageType.valueOf(controlMessage);
    boolean isEndOfSegment = controlMessageType == ControlMessageType.END_OF_SEGMENT;
    synchronized (this.partitionLocks[partition]) {
      // Work around until we upgrade to a more modern Avro version which supports overriding the
      // String implementation.
      while (true) {
        try {
          sendMessage(
              this::getControlMessageKey,
              MessageType.CONTROL_MESSAGE,
              controlMessage,
              isEndOfSegment,
              partition,
              callback,
              updateCheckSum,
              leaderMetadataWrapper,
              VENICE_DEFAULT_LOGICAL_TS).get();
          return;
        } catch (InterruptedException | ExecutionException e) {
          if (ExceptionUtils.recursiveClassEquals(e, PubSubTopicDoesNotExistException.class)) {
            /**
             * Not a super clean way to match the exception, but unfortunately, since it is wrapped inside of an
             * {@link ExecutionException}, there may be no other way.
             */
            if (attempt < maxAttemptsWhenTopicMissing) {
              attempt++;
              updateCheckSum = false; // checksum has already been updated, and should not be updated again for retries
              logger.warn(
                  "Caught a UNKNOWN_TOPIC_OR_PARTITION error, attempt {}/{}",
                  attempt,
                  maxAttemptsWhenTopicMissing);
            } else {
              throw new VeniceException(
                  "Caught a UNKNOWN_TOPIC_OR_PARTITION error, attempt " + attempt + "/" + maxAttemptsWhenTopicMissing
                      + ", will bubble up.");
            }
          } else {
            handleControlMessageProducingException(e, controlMessageType);
          }
        }
      }
    }
  }

  private void handleControlMessageProducingException(Exception e, ControlMessageType controlMessageType) {
    if (ExceptionUtils.recursiveClassEquals(e, PubSubTopicAuthorizationException.class)) {
      throw new VeniceResourceAccessException(
          "You do not have permission to write to this store. Please check that ACLs are set correctly.",
          e);
    } else {
      throw new VeniceException(
          "Got an exception while trying to send a control message (" + controlMessageType.name() + ")",
          e);
    }
  }

  /**
   * Main function for sending control messages. Synchronization is most minimal and there is no error checking.
   *
   * See also:
   * {@link #sendControlMessageWithRetriesForNonExistentTopic(ControlMessage, int, Map, PubSubProducerCallback, LeaderMetadataWrapper)}
   */
  public CompletableFuture<PubSubProduceResult> sendControlMessage(
      ControlMessage controlMessage,
      int partition,
      Map<String, String> debugInfo,
      PubSubProducerCallback callback,
      LeaderMetadataWrapper leaderMetadataWrapper) {
    // Work around until we upgrade to a more modern Avro version which supports overriding the
    // String implementation.
    controlMessage.debugInfo = getDebugInfo(debugInfo);
    boolean isEndOfSegment = ControlMessageType.valueOf(controlMessage).equals(ControlMessageType.END_OF_SEGMENT);
    synchronized (this.partitionLocks[partition]) {
      return sendMessage(
          this::getControlMessageKey,
          MessageType.CONTROL_MESSAGE,
          controlMessage,
          isEndOfSegment,
          partition,
          callback,
          true,
          leaderMetadataWrapper,
          VENICE_DEFAULT_LOGICAL_TS);
    }
  }

  /**
   * This API should be only used in Leader/Standby model for store ingestion.
   *
   * Producer DIV will be recalculated (not DIV pass-through mode); checksum for the input partition in this producer
   * will also be updated.
   */
  public Future<PubSubProduceResult> asyncSendControlMessage(
      ControlMessage controlMessage,
      int partition,
      Map<String, String> debugInfo,
      PubSubProducerCallback callback,
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
          VENICE_DEFAULT_LOGICAL_TS);
    }
  }

  public static KafkaMessageEnvelope getHeartbeatKME(
      long originTimeStampMs,
      LeaderMetadataWrapper leaderMetadataWrapper,
      ControlMessage heartBeatMessage,
      String writerId) {
    ProducerMetadata producerMetadata = new ProducerMetadata();
    producerMetadata.producerGUID = HeartbeatGuidV3Generator.getInstance().getGuid();
    producerMetadata.segmentNumber = 0;
    producerMetadata.messageSequenceNumber = 0;
    producerMetadata.messageTimestamp = originTimeStampMs;
    producerMetadata.logicalTimestamp = VENICE_DEFAULT_LOGICAL_TS;

    LeaderMetadata leaderMetadataFooter = new LeaderMetadata();
    leaderMetadataFooter.hostName = writerId;
    leaderMetadataFooter.upstreamOffset = leaderMetadataWrapper.getUpstreamOffset();
    leaderMetadataFooter.upstreamKafkaClusterId = leaderMetadataWrapper.getUpstreamKafkaClusterId();

    KafkaMessageEnvelope kafkaMessageEnvelope = new KafkaMessageEnvelope();
    kafkaMessageEnvelope.messageType = MessageType.CONTROL_MESSAGE.getValue();
    kafkaMessageEnvelope.producerMetadata = producerMetadata;
    kafkaMessageEnvelope.leaderMetadataFooter = leaderMetadataFooter;
    kafkaMessageEnvelope.payloadUnion = heartBeatMessage;

    return kafkaMessageEnvelope;
  }

  public CompletableFuture<PubSubProduceResult> sendHeartbeat(
      PubSubTopicPartition topicPartition,
      PubSubProducerCallback callback,
      LeaderMetadataWrapper leaderMetadataWrapper,
      boolean addLeaderCompleteState,
      LeaderCompleteState leaderCompleteState,
      long originTimeStampMs) {
    if (isClosed) {
      CompletableFuture<PubSubProduceResult> future = new CompletableFuture<>();
      future.completedFuture(null);
      logger.warn("VeniceWriter already closed for topic partition " + topicPartition);
      return future;
    }
    KafkaMessageEnvelope kafkaMessageEnvelope =
        getHeartbeatKME(originTimeStampMs, leaderMetadataWrapper, heartBeatMessage, writerId);
    return producerAdapter.sendMessage(
        topicPartition.getPubSubTopic().getName(),
        topicPartition.getPartitionNumber(),
        KafkaKey.HEART_BEAT,
        kafkaMessageEnvelope,
        getHeaders(kafkaMessageEnvelope.getProducerMetadata(), addLeaderCompleteState, leaderCompleteState),
        callback);
  }

  public CompletableFuture<PubSubProduceResult> sendHeartbeat(
      String topicName,
      int partitionNumber,
      PubSubProducerCallback callback,
      LeaderMetadataWrapper leaderMetadataWrapper,
      boolean addLeaderCompleteState,
      LeaderCompleteState leaderCompleteState,
      long originTimeStampMs) {
    KafkaMessageEnvelope kafkaMessageEnvelope =
        getHeartbeatKME(originTimeStampMs, leaderMetadataWrapper, heartBeatMessage, writerId);
    return producerAdapter.sendMessage(
        topicName,
        partitionNumber,
        KafkaKey.HEART_BEAT,
        kafkaMessageEnvelope,
        getHeaders(kafkaMessageEnvelope.getProducerMetadata(), addLeaderCompleteState, leaderCompleteState),
        callback);
  }

  /**
   * The Key part of the {@link KafkaKey} needs to be unique in order to avoid clobbering each other during
   * Kafka's Log Compaction. Since there is no key per se associated with control messages, we generate one
   * from the producer metadata, including: GUID, segment and sequence number.
   *
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
      long logicalTs) {
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
    producerMetadata.logicalTimestamp = logicalTs;
    kafkaValue.producerMetadata = producerMetadata;
    if (leaderMetadataWrapper == DEFAULT_LEADER_METADATA_WRAPPER) {
      kafkaValue.leaderMetadataFooter = this.defaultLeaderMetadata;
    } else {
      kafkaValue.leaderMetadataFooter = new LeaderMetadata();
      kafkaValue.leaderMetadataFooter.hostName = writerId;
      kafkaValue.leaderMetadataFooter.upstreamOffset = leaderMetadataWrapper.getUpstreamOffset();
      kafkaValue.leaderMetadataFooter.upstreamKafkaClusterId = leaderMetadataWrapper.getUpstreamKafkaClusterId();
    }

    return kafkaValue;
  }

  /**
   * @param key the {@link KafkaKey} for which we want to get the partition.
   * @return the partition number that the provided key belongs to.
   */
  protected int getPartition(byte[] key) {
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
  Segment getSegment(int partition, boolean sendEndOfSegment) {
    synchronized (this.partitionLocks[partition]) {
      Segment currentSegment = segments[partition];
      if (currentSegment == null || currentSegment.isEnded()) {
        currentSegment = startSegment(partition);
      } else if (elapsedTimeForClosingSegmentEnabled) {
        // Close the current segment and create a new one if the current segment is
        // timed out. The segment won't be closed if the ongoing message itself is
        // an "end_of_segment" message.
        if (!sendEndOfSegment) {
          long currentSegmentStartTime = segmentsStartTimeArray[partition];
          if (currentSegmentStartTime != -1
              && LatencyUtils.getElapsedTimeFromMsToMs(currentSegmentStartTime) > maxElapsedTimeForSegmentInMs) {
            endSegment(partition, false);
            currentSegment = startSegment(partition);
          }
        }
      }
      return currentSegment;
    }
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
      Segment currentSegment = segments[partition];

      if (currentSegment == null) {
        currentSegment = new Segment(partition, 0, checkSumType);
      } else if (currentSegment.isEnded()) {
        int newSegmentNumber = currentSegment.getSegmentNumber() + 1;
        currentSegment = new Segment(partition, newSegmentNumber, checkSumType);
      }

      if (!currentSegment.isStarted()) {
        segments[partition] = currentSegment;
        // Record the new start time of the segment.
        segmentsStartTimeArray[partition] = time.getMilliseconds();
        sendStartOfSegment(partition, null);
        currentSegment.start();
      }

      return currentSegment;
    }
  }

  private void endAllSegments(boolean finalSegment) {
    Segment segment;
    List<CompletableFuture> futures = new ArrayList<>();
    for (int i = 0; i < segments.length; i++) {
      segment = segments[i];
      if (segment != null) {
        futures.add(endSegment(i, finalSegment));
      }
    }
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get();
    } catch (ExecutionException | InterruptedException e) {
      handleControlMessageProducingException(e, ControlMessageType.END_OF_SEGMENT);
    }
  }

  /**
   * @param partition in which to end the current segment
   * @return A CompletableFuture which either contains a result if a EOS was sent, or null if none needed to be sent.
   */
  private CompletableFuture<PubSubProduceResult> endSegment(int partition, boolean finalSegment) {
    synchronized (this.partitionLocks[partition]) {
      Segment currentSegment = segments[partition];
      if (currentSegment == null) {
        logger.debug("endSegment(partition {}) called but currentSegment == null. Ignoring.", partition);
      } else if (!currentSegment.isStarted()) {
        logger.debug("endSegment(partition {}) called but currentSegment.started == false. Ignoring.", partition);
      } else if (currentSegment.isEnded()) {
        logger.debug("endSegment(partition {}) called but currentSegment.ended == true. Ignoring.", partition);
      } else {
        try {
          return sendEndOfSegment(
              partition,
              new HashMap<>(), // TODO: Add extra debugging info
              finalSegment
          // TODO: This will not always be true, once we support streaming, or more than one segment per
          // mapper in batch
          );
        } catch (Exception e) {
          // Ignore the exception here. The pubsub producer should have retries internally, so, we don't need to add
          // more at this layer.
          logger.info("Failed to send end-of-segment message for partition: {}. Ignoring.", partition);
        } finally {
          // Mark the segment as ended. This is because if we fail to send EOS, the next write can create a new segment
          currentSegment.end(finalSegment);
        }
      }
    }
    return CompletableFuture.completedFuture(null);
  }

  public Time getTime() {
    return time;
  }

  public int getMaxRecordSizeBytes() {
    return maxRecordSizeBytes;
  }

  public boolean isRecordTooLarge(int recordSize) {
    return maxRecordSizeBytes != UNLIMITED_MAX_RECORD_SIZE && recordSize > maxRecordSizeBytes;
  }

  public boolean isChunkingNeededForRecord(int recordSize) {
    return recordSize > maxSizeForUserPayloadPerMessageInBytes;
  }

  public int getMaxSizeForUserPayloadPerMessageInBytes() {
    return maxSizeForUserPayloadPerMessageInBytes;
  }

  /**
   * @return Returns a string of format: topicName@brokerAddress
   */
  public String getDestination() {
    return topicName + "@" + producerAdapter.getBrokerAddress();
  }

  public PubSubProducerAdapter getProducerAdapter() {
    return this.producerAdapter;
  }
}
