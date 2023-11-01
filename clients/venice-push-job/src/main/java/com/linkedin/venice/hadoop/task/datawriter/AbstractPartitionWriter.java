package com.linkedin.venice.hadoop.task.datawriter;

import static com.linkedin.venice.hadoop.VenicePushJobConstants.ALLOW_DUPLICATE_KEY;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.DEFAULT_IS_DUPLICATED_KEY_ALLOWED;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.DERIVED_SCHEMA_ID_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.ENABLE_WRITE_COMPUTE;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.STORAGE_QUOTA_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.TELEMETRY_MESSAGE_INTERVAL;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.TOPIC_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.VALUE_SCHEMA_ID_PROP;
import static com.linkedin.venice.hadoop.VenicePushJobConstants.VSON_PUSH;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.annotation.NotThreadsafe;
import com.linkedin.venice.exceptions.RecordTooLargeException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceResourceAccessException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.hadoop.InputStorageQuotaTracker;
import com.linkedin.venice.hadoop.engine.EngineTaskConfigProvider;
import com.linkedin.venice.hadoop.recordreader.AbstractVeniceRecordReader;
import com.linkedin.venice.hadoop.recordreader.avro.VeniceAvroRecordReader;
import com.linkedin.venice.hadoop.recordreader.vson.VeniceVsonRecordReader;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.DeleteMetadata;
import com.linkedin.venice.writer.PutMetadata;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;
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
      this(keyBytes, valueBytes, valueSchemaId, -1, null, callback, enableWriteCompute, derivedValueSchemaId);
    }

    public VeniceWriterMessage(
        byte[] keyBytes,
        byte[] valueBytes,
        int valueSchemaId,
        int rmdVersionId,
        ByteBuffer rmdPayload,
        PubSubProducerCallback callback,
        boolean enableWriteCompute,
        int derivedValueSchemaId) {
      this.keyBytes = keyBytes;
      this.valueBytes = valueBytes;
      this.valueSchemaId = valueSchemaId;
      this.rmdVersionId = rmdVersionId;
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
          writer.put(keyBytes, valueBytes, valueSchemaId, callback, null);
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

  private AbstractVeniceWriter<byte[], byte[], byte[]> veniceWriter = null;
  private int valueSchemaId = -1;
  private int derivedValueSchemaId = -1;
  private boolean enableWriteCompute = false;

  private VeniceProperties props;
  private long telemetryMessageInterval;
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

  /**
   * Yarn will kill reducer if it's inactive for more than 10 minutes, which is too short for reducers to retry sending
   * messages and too short for Venice and Kafka team to mitigate write-path incidents. A background progress heartbeat
   * task will be scheduled to keep reporting progress every 5 minutes until there is error from producer.
   */
  private final ScheduledExecutorService taskProgressHeartbeatScheduler = Executors.newScheduledThreadPool(1);

  public void processValuesForKey(byte[] key, Iterator<byte[]> values, DataWriterTaskTracker dataWriterTaskTracker) {
    this.dataWriterTaskTracker = dataWriterTaskTracker;
    final long timeOfLastReduceFunctionStartInNS = System.nanoTime();
    if (timeOfLastReduceFunctionEndInNS > 0) {
      // Will only be true starting from the 2nd invocation.
      aggregateTimeOfInBetweenReduceInvocationsInNS +=
          (timeOfLastReduceFunctionStartInNS - timeOfLastReduceFunctionEndInNS);
    }
    if (key.length > 0 && (!hasReportedFailure(dataWriterTaskTracker, this.isDuplicateKeyAllowed))) {
      VeniceWriterMessage message = extract(key, values, dataWriterTaskTracker);
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

  protected void setCallback(PubSubProducerCallback userCallback) {
    this.callback = new DelegatingProducerCallback(userCallback);
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
      DataWriterTaskTracker dataWriterTaskTracker) {
    /**
     * Don't use {@link BytesWritable#getBytes()} since it could be padded or modified by some other records later on.
     */
    if (!values.hasNext()) {
      throw new VeniceException("There is no value corresponding to key bytes: " + ByteUtils.toHexString(keyBytes));
    }
    byte[] valueBytes = values.next();
    if (duplicateKeyPrinter == null) {
      throw new VeniceException("'DuplicateKeyPrinter' is not initialized properly");
    }
    duplicateKeyPrinter.detectAndHandleDuplicateKeys(keyBytes, valueBytes, values, dataWriterTaskTracker);
    return new VeniceWriterMessage(
        keyBytes,
        valueBytes,
        valueSchemaId,
        getCallback(),
        isEnableWriteCompute(),
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
    final boolean hasRecordTooLargeFailure = dataWriterTaskTracker.getRecordTooLargeFailureCount() > 0;
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

  private VeniceWriter<byte[], byte[], byte[]> createBasicVeniceWriter() {
    Properties writerProps = props.toProperties();
    writerProps.put(GuidUtils.GUID_GENERATOR_IMPLEMENTATION, GuidUtils.DETERMINISTIC_GUID_GENERATOR_IMPLEMENTATION);
    // Closing segments based on elapsed time should always be disabled in MR to prevent storage nodes consuming out of
    // order keys when speculative execution is in play.
    writerProps.put(VeniceWriter.MAX_ELAPSED_TIME_FOR_SEGMENT_IN_MS, -1);

    EngineTaskConfigProvider engineTaskConfigProvider = getEngineTaskConfigProvider();
    writerProps.put(ConfigKeys.PUSH_JOB_COMPUTE_JOB_ID, engineTaskConfigProvider.getJobName());
    writerProps.put(ConfigKeys.PUSH_JOB_COMPUTE_TASK_ID, engineTaskConfigProvider.getTaskId());
    VeniceWriterFactory veniceWriterFactoryFactory =
        new VeniceWriterFactory(writerProps, new ApacheKafkaProducerAdapterFactory(), null);
    boolean chunkingEnabled = props.getBoolean(VeniceWriter.ENABLE_CHUNKING, false);
    boolean rmdChunkingEnabled = props.getBoolean(VeniceWriter.ENABLE_RMD_CHUNKING, false);
    VenicePartitioner partitioner = PartitionUtils.getVenicePartitioner(props);

    VeniceWriterOptions options =
        new VeniceWriterOptions.Builder(props.getString(TOPIC_PROP)).setKeySerializer(new DefaultSerializer())
            .setValueSerializer(new DefaultSerializer())
            .setWriteComputeSerializer(new DefaultSerializer())
            .setChunkingEnabled(chunkingEnabled)
            .setRmdChunkingEnabled(rmdChunkingEnabled)
            .setTime(SystemTime.INSTANCE)
            .setPartitioner(partitioner)
            .build();
    return veniceWriterFactoryFactory.createVeniceWriter(options);
  }

  private void telemetry() {
    if (messageSent % telemetryMessageInterval == 0) {
      double timeSinceLastMeasurementInSeconds =
          (System.nanoTime() - lastTimeThroughputWasLoggedInNS) / (double) Time.NS_PER_SECOND;

      // Mapping rate measurement
      long mrFrameworkRate = (long) (telemetryMessageInterval / timeSinceLastMeasurementInSeconds);
      LOGGER.info(
          "MR Framework records processed: {}, total time spent: {}, current throughput: {} rec/s",
          messageSent,
          Utils.makeTimePretty(aggregateTimeOfInBetweenReduceInvocationsInNS),
          Utils.makeLargeNumberPretty(mrFrameworkRate));

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
          shouldEndAllSegments =
              messageErrored.get() == 0 && messageSent == messageCompleted.get() && (dataWriterTaskTracker == null
                  || dataWriterTaskTracker.getProgress() == 1.0 || dataWriterTaskTracker.getProgress() == -1);
        } finally {
          veniceWriter.close(shouldEndAllSegments);
        }
      }
      maybePropagateCallbackException();
      LOGGER.info("Kafka message progress after flushing and closing producer:");
      logMessageProgress();
      if (messageSent != messageCompleted.get()) {
        throw new VeniceException(
            "Message sent: " + messageSent + " doesn't match message completed: " + messageCompleted.get());
      }
    } finally {
      Utils.closeQuietlyWithErrorLogged(duplicateKeyPrinter);
      taskProgressHeartbeatScheduler.shutdownNow();
    }
    if (dataWriterTaskTracker != null) {
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
    initStorageQuotaFields(props);
    /**
     * A dummy background task that reports progress every 5 minutes.
     */
    taskProgressHeartbeatScheduler.scheduleAtFixedRate(() -> {
      if (this.dataWriterTaskTracker != null) {
        this.dataWriterTaskTracker.heartbeat();
      }
    }, 0, 5, TimeUnit.MINUTES);
    // Set up a default callback in case the implementation doesn't set one.
    setCallback(null);
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

  protected abstract long getTotalIncomingDataSizeInBytes();

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

    private final String topic;
    private final Schema keySchema;
    private final AbstractVeniceRecordReader<?, ?> recordReader;
    private final VeniceKafkaSerializer<?> keySerializer;
    private final GenericDatumWriter<Object> avroDatumWriter;
    private int numOfDupKey = 0;

    DuplicateKeyPrinter(VeniceProperties props) {
      this.topic = props.getString(TOPIC_PROP);
      this.isDupKeyAllowed = props.getBoolean(ALLOW_DUPLICATE_KEY, false);

      this.recordReader =
          props.getBoolean(VSON_PUSH, false) ? new VeniceVsonRecordReader(props) : new VeniceAvroRecordReader(props);
      this.keySchema = Schema.parse(recordReader.getKeySchemaStr());

      if (recordReader.getKeySerializer() == null) {
        throw new VeniceException("key serializer can not be null.");
      }

      this.keySerializer = recordReader.getKeySerializer();
      this.avroDatumWriter = new GenericDatumWriter<>(keySchema);
    }

    protected void detectAndHandleDuplicateKeys(
        byte[] keyBytes,
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
            LOGGER.warn(printDuplicateKey(keyBytes));
          }
        } else {
          // Distinct values map to the same key. E.g. key:[ value_1, value_2 ]
          distinctValuesToKeyCount++;

          if (isDupKeyAllowed) {
            if (shouldPrint) {
              shouldPrint = false;
              LOGGER.warn(printDuplicateKey(keyBytes));
            }
          }
        }
      }
      dataWriterTaskTracker.trackDuplicateKeyWithIdenticalValue(identicalValuesToKeyCount);
      dataWriterTaskTracker.trackDuplicateKeyWithDistinctValue(distinctValuesToKeyCount);
    }

    private String printDuplicateKey(byte[] keyBytes) {
      Object keyRecord = keySerializer.deserialize(topic, keyBytes);
      try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
        Encoder jsonEncoder = AvroCompatibilityHelper.newJsonEncoder(keySchema, output, false);
        avroDatumWriter.write(keyRecord, jsonEncoder);
        jsonEncoder.flush();
        output.flush();

        numOfDupKey++;
        return String.format("There are multiple records for key:\n%s", new String(output.toByteArray()));
      } catch (IOException exception) {
        throw new VeniceException(exception);
      }
    }

    @Override
    public void close() {
      Utils.closeQuietlyWithErrorLogged(recordReader);
      Utils.closeQuietlyWithErrorLogged(keySerializer);
    }
  }

  public class DelegatingProducerCallback implements PubSubProducerCallback {
    private final PubSubProducerCallback delegate;

    public DelegatingProducerCallback(PubSubProducerCallback delegate) {
      this.delegate = delegate;
    }

    @Override
    public void onCompletion(PubSubProduceResult produceResult, Exception exception) {
      if (exception != null) {
        LOGGER.error("Exception thrown in send message callback", exception);
        recordMessageErrored(exception);
      } else {
        recordMessageCompleted();
      }

      if (delegate != null) {
        delegate.onCompletion(produceResult, exception);
      }
    }

    public PubSubProducerCallback getDelegate() {
      return delegate;
    }
  }
}
