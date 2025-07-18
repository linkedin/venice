package com.linkedin.venice.hadoop.task.datawriter;

import static com.linkedin.venice.ConfigKeys.PUSH_JOB_GUID_LEAST_SIGNIFICANT_BITS;
import static com.linkedin.venice.ConfigKeys.PUSH_JOB_GUID_MOST_SIGNIFICANT_BITS;
import static com.linkedin.venice.ConfigKeys.PUSH_JOB_VIEW_CONFIGS;
import static com.linkedin.venice.guid.GuidUtils.DEFAULT_GUID_GENERATOR_IMPLEMENTATION;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ALLOW_DUPLICATE_KEY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.COMPRESSION_STRATEGY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_IS_DUPLICATED_KEY_ALLOWED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DERIVED_SCHEMA_ID_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ENABLE_WRITE_COMPUTE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_SOURCE_COMPRESSION_STRATEGY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.RMD_SCHEMA_DIR;
import static com.linkedin.venice.vpj.VenicePushJobConstants.RMD_SCHEMA_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.STORAGE_QUOTA_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TELEMETRY_MESSAGE_INTERVAL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TOPIC_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_SCHEMA_DIR;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_SCHEMA_ID_PROP;
import static com.linkedin.venice.writer.VeniceWriter.APP_DEFAULT_LOGICAL_TS;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.annotation.NotThreadsafe;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.RecordTooLargeException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceResourceAccessException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.hadoop.InputStorageQuotaTracker;
import com.linkedin.venice.hadoop.engine.EngineTaskConfigProvider;
import com.linkedin.venice.hadoop.input.kafka.KafkaInputUtils;
import com.linkedin.venice.hadoop.schema.HDFSSchemaSource;
import com.linkedin.venice.hadoop.task.TaskTracker;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.DictionaryUtils;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.views.MaterializedView;
import com.linkedin.venice.views.VeniceView;
import com.linkedin.venice.views.ViewUtils;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.ComplexVeniceWriter;
import com.linkedin.venice.writer.DeleteMetadata;
import com.linkedin.venice.writer.PutMetadata;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * An abstraction of the task that processes all key/value pairs, checks for duplicates and emits the final key/value
 * pairs to Venice's PubSub.
 */
@NotThreadsafe
public abstract class AbstractPartitionWriter extends AbstractDataWriterTask implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(AbstractPartitionWriter.class);

  public static class VeniceWriterMessage {
    private final byte[] keyBytes;
    private final byte[] valueBytes;
    private final long logicalTimestamp;
    private final int valueSchemaId;
    private final int rmdVersionId;
    private final Consumer<AbstractVeniceWriter<byte[], byte[], byte[]>> consumer;

    public VeniceWriterMessage(
        byte[] keyBytes,
        byte[] valueBytes,
        int valueSchemaId,
        PubSubProducerCallback callback,
        boolean enableWriteCompute,
        int derivedValueSchemaId) {
      this(
          keyBytes,
          valueBytes,
          APP_DEFAULT_LOGICAL_TS,
          valueSchemaId,
          -1,
          null,
          callback,
          enableWriteCompute,
          null,
          derivedValueSchemaId);
    }

    public VeniceWriterMessage(
        byte[] keyBytes,
        byte[] valueBytes,
        long logicalTimestamp,
        int valueSchemaId,
        int rmdVersionId,
        ByteBuffer rmdPayload,
        PubSubProducerCallback callback,
        boolean enableWriteCompute,
        Schema rmdSchema,
        int derivedValueSchemaId) {
      this.keyBytes = keyBytes;
      this.valueBytes = valueBytes;
      this.valueSchemaId = valueSchemaId;
      this.rmdVersionId = rmdVersionId;
      this.logicalTimestamp = logicalTimestamp;
      this.consumer = writer -> {
        if (rmdPayload != null) {
          if (rmdPayload.remaining() == 0) {
            throw new VeniceException("Found empty replication metadata");
          }
          if (valueBytes == null) {
            DeleteMetadata deleteMetadata = new DeleteMetadata(valueSchemaId, rmdVersionId, rmdPayload);
            writer.delete(keyBytes, callback, deleteMetadata);
          } else {
            PutMetadata putMetadata = (new PutMetadata(rmdVersionId, rmdPayload));
            writer.put(keyBytes, valueBytes, valueSchemaId, callback, putMetadata);
          }
        } else if (enableWriteCompute && derivedValueSchemaId > 0) {
          writer.update(keyBytes, valueBytes, valueSchemaId, derivedValueSchemaId, callback);
        } else {
          if (this.logicalTimestamp > 0) {
            PutMetadata putMetadata = new PutMetadata(
                rmdVersionId,
                RmdSchemaGenerator.generateRecordLevelTimestampMetadata(rmdSchema, this.logicalTimestamp));
            writer.put(keyBytes, valueBytes, valueSchemaId, this.logicalTimestamp, callback, putMetadata);
          } else {
            writer.put(keyBytes, valueBytes, valueSchemaId, callback, null);
          }
        }
      };
    }

    private Consumer<AbstractVeniceWriter<byte[], byte[], byte[]>> getConsumer() {
      return consumer;
    }

    public int getRmdVersionId() {
      return rmdVersionId;
    }

    public byte[] getKeyBytes() {
      return keyBytes;
    }

    public byte[] getValueBytes() {
      return valueBytes;
    }

    public int getValueSchemaId() {
      return valueSchemaId;
    }
  }

  private long lastTimeThroughputWasLoggedInNS = System.nanoTime();
  private long lastMessageCompletedCount = 0;

  private Lazy<VeniceWriterFactory> veniceWriterFactory;
  private AbstractVeniceWriter<byte[], byte[], byte[]> veniceWriter = null;
  private VeniceWriter<byte[], byte[], byte[]> mainWriter = null;
  private ComplexVeniceWriter[] childWriters = null;
  private int valueSchemaId = -1;
  private Schema rmdSchema = null;
  private int derivedValueSchemaId = -1;
  private boolean enableWriteCompute = false;

  private VeniceProperties props;
  private long telemetryMessageInterval;
  private boolean enableUncompressedRecordSizeLimit;
  private DuplicateKeyPrinter duplicateKeyPrinter;
  private Exception sendException = null;

  /**
   * Visible for testing purpose
   *
   * IMPORTANT: Noticed that this callback is reused in different messages, do not put information that is coupled with
   *            each message inside this callback.
   */
  private PubSubProducerCallback callback = null;
  private DataWriterTaskTracker dataWriterTaskTracker = null;
  /**
   * This doesn't need to be atomic since {@link #processValuesForKey(byte[], Iterator, DataWriterTaskTracker)} will be called sequentially.
   */
  private long messageSent = 0;
  private final AtomicLong messageCompleted = new AtomicLong();
  private final AtomicLong messageErrored = new AtomicLong();
  private long timeOfLastReduceFunctionEndInNS = 0;
  private long aggregateTimeOfReduceExecutionInNS = 0;
  private long aggregateTimeOfInBetweenReduceInvocationsInNS = 0;
  private InputStorageQuotaTracker inputStorageQuotaTracker;
  private boolean exceedQuota = false;
  private boolean hasWriteAclFailure = false;
  private boolean hasDuplicateKeyWithDistinctValue = false;
  private boolean hasRecordTooLargeFailure = false;
  private boolean isDuplicateKeyAllowed = DEFAULT_IS_DUPLICATED_KEY_ALLOWED;
  private HDFSSchemaSource schemaSource;
  private Map<Integer, Schema> valueSchemaMap;
  private Map<Integer, RecordDeserializer<GenericRecord>> valueDeserializerCache;
  private final Lazy<CompressorFactory> compressorFactory = Lazy.of(CompressorFactory::new);
  private Lazy<VeniceCompressor> compressor;

  /**
   * Compute engines will kill a task if it's inactive for a configured time. This time might be is too short for the
   * partition writers to retry sending messages and too short for Venice and Kafka team to mitigate write-path
   * incidents. A background progress heartbeat task will be scheduled to keep reporting progress periodically until
   * there is error from producer.
   */
  private final ScheduledExecutorService taskProgressHeartbeatScheduler = Executors.newScheduledThreadPool(1);

  public void processValuesForKey(
      byte[] key,
      Iterator<byte[]> values,
      Iterator<Long> timestampIterator,
      DataWriterTaskTracker dataWriterTaskTracker) {
    this.dataWriterTaskTracker = dataWriterTaskTracker;
    final long timeOfLastReduceFunctionStartInNS = System.nanoTime();
    if (timeOfLastReduceFunctionEndInNS > 0) {
      // Will only be true starting from the 2nd invocation.
      aggregateTimeOfInBetweenReduceInvocationsInNS +=
          (timeOfLastReduceFunctionStartInNS - timeOfLastReduceFunctionEndInNS);
    }
    if (key.length > 0 && (!hasReportedFailure(dataWriterTaskTracker, this.isDuplicateKeyAllowed))) {
      VeniceWriterMessage message = extract(key, values, timestampIterator, rmdSchema, dataWriterTaskTracker);
      if (message != null) {
        try {
          sendMessageToKafka(dataWriterTaskTracker, message.getConsumer());
        } catch (VeniceException e) {
          if (e instanceof VeniceResourceAccessException) {
            dataWriterTaskTracker.trackWriteAclAuthorizationFailure();
            LOGGER.error(e);
            return;
          } else if (e instanceof RecordTooLargeException) {
            dataWriterTaskTracker.trackRecordTooLargeFailure();
            LOGGER.error(e);
            return;
          }
          throw e;
        }
      }
    }
    updateExecutionTimeStatus(timeOfLastReduceFunctionStartInNS);
  }

  // For testing purpose
  protected void setVeniceWriterFactory(VeniceWriterFactory factory) {
    this.veniceWriterFactory = Lazy.of(() -> factory);
  }

  public VeniceWriterFactory getVeniceWriterFactory() {
    return veniceWriterFactory.get();
  }

  protected DataWriterTaskTracker getDataWriterTaskTracker() {
    return dataWriterTaskTracker;
  }

  protected PubSubProducerCallback getCallback() {
    return callback;
  }

  protected int getDerivedValueSchemaId() {
    return derivedValueSchemaId;
  }

  protected boolean isEnableWriteCompute() {
    return enableWriteCompute;
  }

  protected VeniceWriterMessage extract(
      byte[] keyBytes,
      Iterator<byte[]> values,
      Iterator<Long> timestampIterator,
      Schema valueSchema,
      DataWriterTaskTracker dataWriterTaskTracker) {
    /**
     * Don't use {@link BytesWritable#getBytes()} since it could be padded or modified by some other records later on.
     */
    if (!values.hasNext()) {
      throw new VeniceException("There is no value corresponding to key bytes: " + ByteUtils.toHexString(keyBytes));
    }
    byte[] valueBytes = values.next();
    long timestamp = -1L;
    if (timestampIterator.hasNext()) {
      timestamp = timestampIterator.next();
    }
    if (duplicateKeyPrinter == null) {
      throw new VeniceException("'DuplicateKeyPrinter' is not initialized properly");
    }
    duplicateKeyPrinter.detectAndHandleDuplicateKeys(valueBytes, values, dataWriterTaskTracker);

    return new VeniceWriterMessage(
        keyBytes,
        valueBytes,
        timestamp,
        valueSchemaId,
        -1,
        null,
        getCallback(),
        isEnableWriteCompute(),
        valueSchema,
        getDerivedValueSchemaId());
  }

  protected boolean hasReportedFailure(DataWriterTaskTracker dataWriterTaskTracker, boolean isDuplicateKeyAllowed) {
    return exceedQuota(dataWriterTaskTracker) || hasWriteAclFailure(dataWriterTaskTracker)
        || hasDuplicatedKeyWithDistinctValueFailure(dataWriterTaskTracker, isDuplicateKeyAllowed)
        || hasRecordTooLargeFailure(dataWriterTaskTracker);
  }

  private boolean hasRecordTooLargeFailure(DataWriterTaskTracker dataWriterTaskTracker) {
    if (this.hasRecordTooLargeFailure) {
      return true;
    }
    final boolean hasRecordTooLargeFailure = (dataWriterTaskTracker.getRecordTooLargeFailureCount() > 0
        || (dataWriterTaskTracker.getUncompressedRecordTooLargeFailureCount() > 0
            && this.enableUncompressedRecordSizeLimit));
    if (hasRecordTooLargeFailure) {
      this.hasRecordTooLargeFailure = true;
    }
    return hasRecordTooLargeFailure;
  }

  private boolean hasDuplicatedKeyWithDistinctValueFailure(
      DataWriterTaskTracker dataWriterTaskTracker,
      boolean isDuplicateKeyAllowed) {
    if (isDuplicateKeyAllowed) {
      return false;
    }
    return hasDuplicateKeyWithDistinctValue(dataWriterTaskTracker);
  }

  private boolean hasWriteAclFailure(DataWriterTaskTracker dataWriterTaskTracker) {
    if (this.hasWriteAclFailure) {
      return true;
    }
    final boolean hasWriteAclFailure = dataWriterTaskTracker.getWriteAclAuthorizationFailureCount() > 0;
    if (hasWriteAclFailure) {
      this.hasWriteAclFailure = true;
    }
    return hasWriteAclFailure;
  }

  private boolean hasDuplicateKeyWithDistinctValue(DataWriterTaskTracker dataWriterTaskTracker) {
    if (this.hasDuplicateKeyWithDistinctValue) {
      return true;
    }
    final boolean hasDuplicateKeyWithDistinctValue = dataWriterTaskTracker.getDuplicateKeyWithDistinctValueCount() > 0;
    if (hasDuplicateKeyWithDistinctValue) {
      this.hasDuplicateKeyWithDistinctValue = true;
    }
    return hasDuplicateKeyWithDistinctValue;
  }

  private boolean exceedQuota(DataWriterTaskTracker dataWriterTaskTracker) {
    if (exceedQuota) {
      return true;
    }
    if (inputStorageQuotaTracker == null) {
      return false;
    }
    final long totalInputStorageSizeInBytes =
        dataWriterTaskTracker.getTotalKeySize() + dataWriterTaskTracker.getTotalValueSize();
    final boolean exceedQuota = inputStorageQuotaTracker.exceedQuota(totalInputStorageSizeInBytes);
    if (exceedQuota) {
      this.exceedQuota = exceedQuota;
    }
    return exceedQuota;
  }

  private void updateExecutionTimeStatus(long timeOfLastReduceFunctionStartInNS) {
    timeOfLastReduceFunctionEndInNS = System.nanoTime();
    aggregateTimeOfReduceExecutionInNS += (timeOfLastReduceFunctionEndInNS - timeOfLastReduceFunctionStartInNS);
  }

  private void sendMessageToKafka(
      DataWriterTaskTracker dataWriterTaskTracker,
      Consumer<AbstractVeniceWriter<byte[], byte[], byte[]>> writerConsumer) {
    maybePropagateCallbackException();
    if (veniceWriter == null) {
      veniceWriter = createBasicVeniceWriter();
    }
    writerConsumer.accept(veniceWriter);
    messageSent++;
    telemetry();
    dataWriterTaskTracker.trackRecordSentToPubSub();
  }

  protected AbstractVeniceWriter<byte[], byte[], byte[]> createBasicVeniceWriter() {
    EngineTaskConfigProvider engineTaskConfigProvider = getEngineTaskConfigProvider();
    Properties jobProps = engineTaskConfigProvider.getJobProps();
    VeniceWriterFactory veniceWriterFactoryFactory = veniceWriterFactory.get();
    boolean chunkingEnabled = props.getBoolean(VeniceWriter.ENABLE_CHUNKING, false);
    boolean rmdChunkingEnabled = props.getBoolean(VeniceWriter.ENABLE_RMD_CHUNKING, false);
    String maxRecordSizeBytesStr = (String) jobProps
        .getOrDefault(VeniceWriter.MAX_RECORD_SIZE_BYTES, String.valueOf(VeniceWriter.UNLIMITED_MAX_RECORD_SIZE));
    VenicePartitioner partitioner = PartitionUtils.getVenicePartitioner(props);

    String topicName = props.getString(TOPIC_PROP);
    VeniceWriterOptions options =
        new VeniceWriterOptions.Builder(topicName).setKeyPayloadSerializer(new DefaultSerializer())
            .setValuePayloadSerializer(new DefaultSerializer())
            .setWriteComputePayloadSerializer(new DefaultSerializer())
            .setChunkingEnabled(chunkingEnabled)
            .setRmdChunkingEnabled(rmdChunkingEnabled)
            .setTime(SystemTime.INSTANCE)
            .setPartitionCount(getPartitionCount())
            .setPartitioner(partitioner)
            .setMaxRecordSizeBytes(Integer.parseInt(maxRecordSizeBytesStr))
            .build();
    String flatViewConfigMapString = props.getString(PUSH_JOB_VIEW_CONFIGS, "");
    if (!flatViewConfigMapString.isEmpty()) {
      mainWriter = veniceWriterFactoryFactory.createVeniceWriter(options);
      return createCompositeVeniceWriter(
          veniceWriterFactoryFactory,
          mainWriter,
          flatViewConfigMapString,
          topicName,
          chunkingEnabled,
          rmdChunkingEnabled);
    } else {
      return veniceWriterFactoryFactory.createVeniceWriter(options);
    }
  }

  /**
   * Create {@link CompositeVeniceWriter} for writing to materialized views. If a
   * {@link com.linkedin.venice.partitioner.ComplexVenicePartitioner} is involved we will also initialize schema, deser,
   * and compressor in order to provide the appropriate value extractor. Calling compressor.get() eagerly to force out
   * any potential issues early and protect against property/config changes later.
   */
  private AbstractVeniceWriter<byte[], byte[], byte[]> createCompositeVeniceWriter(
      VeniceWriterFactory factory,
      VeniceWriter<byte[], byte[], byte[]> mainWriter,
      String flatViewConfigMapString,
      String topicName,
      boolean chunkingEnabled,
      boolean rmdChunkingEnabled) {
    try {
      Map<String, ViewConfig> viewConfigMap = ViewUtils.parseViewConfigMapString(flatViewConfigMapString);
      childWriters = new ComplexVeniceWriter[viewConfigMap.size()];
      String storeName = Version.parseStoreFromKafkaTopicName(topicName);
      int versionNumber = Version.parseVersionFromKafkaTopicName(topicName);
      // TODO using a dummy Version to get venice writer options could be error prone. Alternatively we could change
      // the abstract method, getWriterOptionsBuilder's signature.
      Version version = new VersionImpl(storeName, versionNumber, "ignored");
      version.setChunkingEnabled(chunkingEnabled);
      version.setRmdChunkingEnabled(rmdChunkingEnabled);
      // Default deser and decompress function for simple partitioner where value provider is never going to be used.
      BiFunction<byte[], Integer, GenericRecord> valueExtractor = (valueBytes, valueSchemaId) -> null;
      boolean complexPartitionerConfigured = false;
      int index = 0;
      for (ViewConfig viewConfig: viewConfigMap.values()) {
        VeniceView view = ViewUtils
            .getVeniceView(viewConfig.getViewClassName(), new Properties(), storeName, viewConfig.getViewParameters());
        String viewTopic = view.getTopicNamesAndConfigsForVersion(versionNumber).keySet().stream().findAny().get();
        if (view instanceof MaterializedView) {
          MaterializedView materializedView = (MaterializedView) view;
          if (materializedView.getViewPartitioner()
              .getPartitionerType() == VenicePartitioner.VenicePartitionerType.COMPLEX
              && !complexPartitionerConfigured) {
            // Initialize value schemas, deser cache and other variables needed by ComplexVenicePartitioner
            initializeSchemaSourceAndDeserCache();
            compressor.get();
            valueExtractor = (valueBytes, valueSchemaId) -> {
              byte[] decompressedBytes;
              if (compressor.get() == null) {
                decompressedBytes = valueBytes;
              } else {
                try {
                  decompressedBytes =
                      ByteUtils.extractByteArray(compressor.get().decompress(valueBytes, 0, valueBytes.length));
                } catch (IOException e) {
                  throw new VeniceException("Unable to decompress value bytes", e);
                }
              }
              return valueDeserializerCache.computeIfAbsent(valueSchemaId, this::getValueDeserializer)
                  .deserialize(decompressedBytes);
            };
            // We only need to configure these variables once per CompositeVeniceWriter
            complexPartitionerConfigured = true;
          }
          childWriters[index++] =
              factory.createComplexVeniceWriter(view.getWriterOptionsBuilder(viewTopic, version).build());
        } else {
          throw new UnsupportedOperationException("Only materialized view is supported in VPJ");
        }
      }
      return new CompositeVeniceWriter<byte[], byte[], byte[]>(
          topicName,
          mainWriter,
          childWriters,
          new ChildWriterProducerCallback(),
          valueExtractor);
    } catch (Exception e) {
      String errorMessage = String.format("Failed to create composite writer for push to store version: %s", topicName);
      LOGGER.error(errorMessage, e);
      throw new VeniceException(errorMessage);
    }
  }

  private void initializeSchemaSourceAndDeserCache() throws IOException {
    schemaSource = new HDFSSchemaSource(props.getString(VALUE_SCHEMA_DIR), props.getString(RMD_SCHEMA_DIR));
    valueSchemaMap = schemaSource.fetchValueSchemas();
    valueDeserializerCache = new VeniceConcurrentHashMap<>();
  }

  private RecordDeserializer<GenericRecord> getValueDeserializer(int valueSchemaId) {
    Schema schema = valueSchemaMap.get(valueSchemaId);
    return FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(schema, schema);
  }

  private void telemetry() {
    if (messageSent % telemetryMessageInterval == 0) {
      double timeSinceLastMeasurementInSeconds =
          (System.nanoTime() - lastTimeThroughputWasLoggedInNS) / (double) Time.NS_PER_SECOND;

      // Mapping rate measurement
      long writeThroughput = (long) (telemetryMessageInterval / timeSinceLastMeasurementInSeconds);
      LOGGER.info(
          "DataWriterComputeJob records processed: {}, total time spent: {}, current throughput: {} rec/s",
          messageSent,
          Utils.makeTimePretty(aggregateTimeOfInBetweenReduceInvocationsInNS),
          Utils.makeLargeNumberPretty(writeThroughput));

      // Produce rate measurement
      long newMessageCompletedCount = messageCompleted.get();
      long messagesProducedSinceLastLog = newMessageCompletedCount - lastMessageCompletedCount;
      long produceRate = (long) (messagesProducedSinceLastLog / timeSinceLastMeasurementInSeconds);
      LOGGER.info(
          "Kafka records produced: {}, total time spent: {}, current throughput: {} rec/s",
          newMessageCompletedCount,
          Utils.makeTimePretty(aggregateTimeOfReduceExecutionInNS),
          Utils.makeLargeNumberPretty(produceRate));

      // Bookkeeping for the next measurement iteration
      lastTimeThroughputWasLoggedInNS = System.nanoTime();
      lastMessageCompletedCount = newMessageCompletedCount;
    }
  }

  // Visible for testing
  protected boolean getExceedQuotaFlag() {
    return exceedQuota;
  }

  @Override
  public void close() throws IOException {
    try {
      LOGGER.info("Kafka message progress before flushing and closing producer:");
      logMessageProgress();
      if (veniceWriter != null) {
        boolean shouldEndAllSegments = false;
        try {
          veniceWriter.flush();
          shouldEndAllSegments = messageErrored.get() == 0 && messageSent == messageCompleted.get()
              && (dataWriterTaskTracker == null || dataWriterTaskTracker.getProgress() == TaskTracker.PROGRESS_COMPLETED
                  || dataWriterTaskTracker.getProgress() == TaskTracker.PROGRESS_NOT_SUPPORTED);
        } finally {
          veniceWriter.close(shouldEndAllSegments);
        }
        if (veniceWriter instanceof CompositeVeniceWriter) {
          if (childWriters != null) {
            for (AbstractVeniceWriter childWriter: childWriters) {
              childWriter.close(shouldEndAllSegments);
            }
          }
          if (mainWriter != null) {
            mainWriter.close(shouldEndAllSegments);
          }
        }
      }
      maybePropagateCallbackException();
      LOGGER.info("Kafka message progress after flushing and closing producer:");
      logMessageProgress();
      if (messageSent != messageCompleted.get()) {
        throw new VeniceException(
            "Message sent: " + messageSent + " doesn't match message completed: " + messageCompleted.get());
      }
      if (schemaSource != null) {
        schemaSource.close();
      }
      if (compressorFactory.isPresent()) {
        compressorFactory.get().close();
      }
    } finally {
      Utils.closeQuietlyWithErrorLogged(duplicateKeyPrinter);
      taskProgressHeartbeatScheduler.shutdownNow();
    }
    if (dataWriterTaskTracker == null) {
      LOGGER.warn("No TaskTracker set");
    } else {
      dataWriterTaskTracker.trackPartitionWriterClose();
    }
  }

  protected DuplicateKeyPrinter initDuplicateKeyPrinter(VeniceProperties props) {
    return new DuplicateKeyPrinter(props);
  }

  @Override
  protected void configureTask(VeniceProperties props) {
    this.props = props;
    this.isDuplicateKeyAllowed = props.getBoolean(ALLOW_DUPLICATE_KEY, false);
    this.valueSchemaId = props.getInt(VALUE_SCHEMA_ID_PROP);
    this.derivedValueSchemaId = (props.containsKey(DERIVED_SCHEMA_ID_PROP)) ? props.getInt(DERIVED_SCHEMA_ID_PROP) : -1;
    this.enableWriteCompute = (props.containsKey(ENABLE_WRITE_COMPUTE)) && props.getBoolean(ENABLE_WRITE_COMPUTE);
    this.duplicateKeyPrinter = initDuplicateKeyPrinter(props);
    this.telemetryMessageInterval = props.getInt(TELEMETRY_MESSAGE_INTERVAL, 10000);
    this.enableUncompressedRecordSizeLimit =
        props.getBoolean(VeniceWriter.ENABLE_UNCOMPRESSED_RECORD_SIZE_LIMIT, false);
    this.callback = new PartitionWriterProducerCallback();
    String rmdSchemaProp = props.getString(RMD_SCHEMA_PROP, "");
    if (rmdSchemaProp.isEmpty()) {
      this.rmdSchema = null;
    } else {
      this.rmdSchema = AvroCompatibilityHelper.parse(props.getString(RMD_SCHEMA_PROP));
    }
    initStorageQuotaFields(props);
    /**
     * A dummy background task that reports progress every 5 minutes.
     */
    taskProgressHeartbeatScheduler.scheduleAtFixedRate(() -> {
      if (this.dataWriterTaskTracker != null) {
        this.dataWriterTaskTracker.heartbeat();
      }
    }, 0, 5, TimeUnit.MINUTES);

    veniceWriterFactory = Lazy.of(() -> {
      Properties writerProps = this.props.toProperties();
      // Closing segments based on elapsed time should always be disabled in data writer compute jobs to prevent storage
      // nodes from consuming out of order keys when speculative execution is enabled.
      writerProps.put(VeniceWriter.MAX_ELAPSED_TIME_FOR_SEGMENT_IN_MS, -1);
      EngineTaskConfigProvider engineTaskConfigProvider = getEngineTaskConfigProvider();
      Properties jobProps = engineTaskConfigProvider.getJobProps();
      // Use the UUID bits created by the VPJ driver to build a producerGUID deterministically
      String guidGenerator = jobProps.getProperty(GuidUtils.GUID_GENERATOR_IMPLEMENTATION);
      if (guidGenerator == null || !guidGenerator.equals(DEFAULT_GUID_GENERATOR_IMPLEMENTATION)) {
        writerProps.put(GuidUtils.GUID_GENERATOR_IMPLEMENTATION, GuidUtils.DETERMINISTIC_GUID_GENERATOR_IMPLEMENTATION);
        writerProps.put(PUSH_JOB_GUID_MOST_SIGNIFICANT_BITS, jobProps.getProperty(PUSH_JOB_GUID_MOST_SIGNIFICANT_BITS));
        writerProps
            .put(PUSH_JOB_GUID_LEAST_SIGNIFICANT_BITS, jobProps.getProperty(PUSH_JOB_GUID_LEAST_SIGNIFICANT_BITS));
      }
      return new VeniceWriterFactory(writerProps);
    });

    compressor = Lazy.of(() -> {
      if (props.containsKey(KAFKA_INPUT_TOPIC)) {
        // Configure compressor using kafka input configs
        String sourceVersion = props.getString(KAFKA_INPUT_TOPIC);
        String kafkaInputBrokerUrl = props.getString(KAFKA_INPUT_BROKER_URL);
        CompressionStrategy strategy =
            CompressionStrategy.valueOf(props.getString(KAFKA_INPUT_SOURCE_COMPRESSION_STRATEGY));
        return KafkaInputUtils
            .getCompressor(compressorFactory.get(), strategy, kafkaInputBrokerUrl, sourceVersion, props);
      } else {
        CompressionStrategy strategy = CompressionStrategy.valueOf(props.getString(COMPRESSION_STRATEGY));
        if (strategy == CompressionStrategy.ZSTD_WITH_DICT) {
          String topicName = props.getString(TOPIC_PROP);
          ByteBuffer dict = DictionaryUtils.readDictionaryFromKafka(topicName, props);
          return compressorFactory.get()
              .createVersionSpecificCompressorIfNotExist(strategy, topicName, ByteUtils.extractByteArray(dict));
        } else {
          return compressorFactory.get().getCompressor(strategy);
        }
      }
    });
  }

  private void initStorageQuotaFields(VeniceProperties props) {
    Long storeStorageQuota = props.containsKey(STORAGE_QUOTA_PROP) ? props.getLong(STORAGE_QUOTA_PROP) : null;
    inputStorageQuotaTracker = new InputStorageQuotaTracker(storeStorageQuota);
    if (storeStorageQuota == null) {
      return;
    }
    if (storeStorageQuota == Store.UNLIMITED_STORAGE_QUOTA) {
      exceedQuota = false;
    } else {
      exceedQuota = inputStorageQuotaTracker.exceedQuota(getTotalIncomingDataSizeInBytes());
    }
  }

  /**
   * Return the size of serialized key and serialized value in bytes across the entire dataset. This is an optimization
   * to skip writing the data to Kafka and reduce the load on Kafka and Venice storage nodes. Not all engines can
   * support fetching this information during the execution of the job (eg Spark), but we can live with it for now. The
   * quota is checked again in the Driver after the completion of the DataWriter job, and it will kill the VenicePushJob
   * soon after.
   *
   * @return the size of serialized key and serialized value in bytes across the entire dataset
   */
  protected long getTotalIncomingDataSizeInBytes() {
    return 0;
  }

  private void setSendException(Exception e) {
    sendException = e;
  }

  protected void recordMessageErrored(Exception e) {
    messageErrored.incrementAndGet();
    if (e != null) {
      setSendException(e);
    }
  }

  private void recordMessageCompleted() {
    messageCompleted.incrementAndGet();
  }

  private void maybePropagateCallbackException() {
    if (sendException != null) {
      throw new VeniceException(sendException);
    }
  }

  protected void logMessageProgress() {
    LOGGER.info(
        "Message sent: {}, message completed: {}, message errored: {}",
        messageSent,
        messageCompleted.get(),
        messageErrored.get());
  }

  // Visible for testing
  protected void setVeniceWriter(AbstractVeniceWriter veniceWriter) {
    this.veniceWriter = veniceWriter;
  }

  // Visible for testing
  protected void setExceedQuota(boolean exceedQuota) {
    this.exceedQuota = exceedQuota;
  }

  /**
   * Using Avro Json encoder to print duplicate keys
   * in case there are tons of duplicate keys, only print first {@link #MAX_NUM_OF_LOG}
   * of them so that it won't pollute Reducer's log.
   *
   * N.B. We assume that this is an Avro record here. (Vson is considered as
   * Avro as well from Reducer's perspective) We should update this method once
   * Venice supports other format in the future
   */
  public static class DuplicateKeyPrinter implements AutoCloseable, Closeable {
    private static final int MAX_NUM_OF_LOG = 10;

    private final boolean isDupKeyAllowed;

    private int numOfDupKey = 0;

    DuplicateKeyPrinter(VeniceProperties props) {
      this.isDupKeyAllowed = props.getBoolean(ALLOW_DUPLICATE_KEY, false);
    }

    protected void detectAndHandleDuplicateKeys(
        byte[] valueBytes,
        Iterator<byte[]> values,
        DataWriterTaskTracker dataWriterTaskTracker) {
      if (numOfDupKey > MAX_NUM_OF_LOG) {
        return;
      }
      boolean shouldPrint = true; // In case there are lots of duplicate keys with the same value, only print once.
      int distinctValuesToKeyCount = 0;
      int identicalValuesToKeyCount = 0;

      while (values.hasNext()) {
        if (Arrays.equals(values.next(), valueBytes)) {
          // Identical values map to the same key. E.g. key:[ value_1, value_1]
          identicalValuesToKeyCount++;
          if (shouldPrint) {
            shouldPrint = false;
            numOfDupKey++;
            LOGGER.warn("There are multiple records for the same key");
          }
        } else {
          // Distinct values map to the same key. E.g. key:[ value_1, value_2 ]
          distinctValuesToKeyCount++;

          if (isDupKeyAllowed) {
            if (shouldPrint) {
              shouldPrint = false;
              numOfDupKey++;
              LOGGER.warn("There are multiple records for the same key");
            }
          }
        }
      }
      dataWriterTaskTracker.trackDuplicateKeyWithIdenticalValue(identicalValuesToKeyCount);
      dataWriterTaskTracker.trackDuplicateKeyWithDistinctValue(distinctValuesToKeyCount);
    }

    @Override
    public void close() {
      // Nothing to do
    }
  }

  public class PartitionWriterProducerCallback implements PubSubProducerCallback {
    @Override
    public void onCompletion(PubSubProduceResult produceResult, Exception exception) {
      if (exception != null) {
        LOGGER.error("Exception thrown in send message callback", exception);
        recordMessageErrored(exception);
      } else {
        recordMessageCompleted();
        int partition = produceResult.getPartition();
        if (partition != getTaskId()) {
          // PartitionWriter's input and output are not aligned!
          recordMessageErrored(
              new VeniceException(
                  String.format(
                      "The task is not writing to the PubSub partition that maps to it (taskId = %d, partition = %d). "
                          + "This could mean that task shuffling is buggy or that the configured %s (%s) is non-deterministic.",
                      getTaskId(),
                      partition,
                      VenicePartitioner.class.getSimpleName(),
                      props.getString(ConfigKeys.PARTITIONER_CLASS))));
        }
      }

      // Report progress so compute framework won't kill current task when it finishes
      // sending all the messages to PubSub system, but not yet flushed and closed.
      dataWriterTaskTracker.heartbeat();
    }
  }

  public class ChildWriterProducerCallback implements PubSubProducerCallback {
    @Override
    public void onCompletion(PubSubProduceResult produceResult, Exception exception) {
      if (exception != null) {
        LOGGER.error("Exception thrown in composite writer's child send message callback", exception);
      }
      // Report progress so compute framework won't kill current task when it finishes
      // sending all the messages to PubSub system, but not yet flushed and closed.
      dataWriterTaskTracker.heartbeat();
    }
  }
}
