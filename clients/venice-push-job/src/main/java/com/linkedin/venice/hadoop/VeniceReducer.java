package com.linkedin.venice.hadoop;

import static com.linkedin.venice.hadoop.VenicePushJob.ALLOW_DUPLICATE_KEY;
import static com.linkedin.venice.hadoop.VenicePushJob.DEFAULT_IS_DUPLICATED_KEY_ALLOWED;
import static com.linkedin.venice.hadoop.VenicePushJob.DERIVED_SCHEMA_ID_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.ENABLE_WRITE_COMPUTE;
import static com.linkedin.venice.hadoop.VenicePushJob.STORAGE_QUOTA_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.TELEMETRY_MESSAGE_INTERVAL;
import static com.linkedin.venice.hadoop.VenicePushJob.TOPIC_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.VALUE_SCHEMA_ID_PROP;
import static com.linkedin.venice.hadoop.VenicePushJob.VSON_PUSH;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.RecordTooLargeException;
import com.linkedin.venice.exceptions.TopicAuthorizationVeniceException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.hadoop.utils.HadoopUtils;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Progressable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * {@link VeniceReducer} will be in charge of producing the messages to Kafka broker.
 * Since {@link VeniceMRPartitioner} is using the same logic of {@link com.linkedin.venice.partitioner.DefaultVenicePartitioner},
 * all the messages in the same reducer belongs to the same topic partition.
 *
 * The reason to introduce a reduce phase is that BDB-JE will benefit with sorted input in the following ways:
 * 1. BDB-JE won't generate so many BINDelta since it won't touch a lot of BINs at a time;
 * 2. The overall BDB-JE insert rate will improve a lot since the disk usage will be reduced a lot (BINDelta will be
 * much smaller than before);
 *
 */
public class VeniceReducer extends AbstractMapReduceTask
    implements Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
  public static class VeniceWriterMessage {
    private final byte[] keyBytes;
    private final byte[] valueBytes;
    private final int valueSchemaId;
    private final int rmdVersionId;
    private final ByteBuffer rmdPayload;
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
      this.rmdPayload = rmdPayload;
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

    public Consumer<AbstractVeniceWriter<byte[], byte[], byte[]>> getConsumer() {
      return consumer;
    }

    public ByteBuffer getRmdPayload() {
      return rmdPayload;
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

  public static final String MAP_REDUCE_JOB_ID_PROP = "mapred.job.id";
  private static final Logger LOGGER = LogManager.getLogger(VeniceReducer.class);

  private long lastTimeThroughputWasLoggedInNS = System.nanoTime();
  private long lastMessageCompletedCount = 0;

  private AbstractVeniceWriter<byte[], byte[], byte[]> veniceWriter = null;
  private int valueSchemaId = -1;
  private int derivedValueSchemaId = -1;
  private boolean enableWriteCompute = false;

  private VeniceProperties props;
  private JobID mapReduceJobId;
  private long telemetryMessageInterval;
  private final Set<Integer> partitionSet = ConcurrentHashMap.newKeySet();
  private DuplicateKeyPrinter duplicateKeyPrinter;
  private Exception sendException = null;

  /**
   * Visible for testing purpose
   *
   * IMPORTANT: Noticed that this callback is reused in different messages, do not put information that is coupled with
   *            each message inside this callback.
   */
  protected ReducerProduceCallback callback = null;
  private Reporter previousReporter = null;
  /**
   * This doesn't need to be atomic since {@link #reduce(BytesWritable, Iterator, OutputCollector, Reporter)} will be called sequentially.
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
  private HadoopJobClientProvider hadoopJobClientProvider = new DefaultHadoopJobClientProvider();
  private boolean isDuplicateKeyAllowed = DEFAULT_IS_DUPLICATED_KEY_ALLOWED;

  /**
   * Yarn will kill reducer if it's inactive for more than 10 minutes, which is too short for reducers to retry sending
   * messages and too short for Venice and Kafka team to mitigate write-path incidents. A background progress heartbeat
   * task will be scheduled to keep reporting progress every 5 minutes until there is error from producer.
   */
  private final ScheduledExecutorService reducerProgressHeartbeatScheduler = Executors.newScheduledThreadPool(1);

  @Override
  public void reduce(
      BytesWritable key,
      Iterator<BytesWritable> values,
      OutputCollector<BytesWritable, BytesWritable> output,
      Reporter reporter) {
    if (updatePreviousReporter(reporter)) {
      callback = new ReducerProduceCallback(reporter);
    }
    final long timeOfLastReduceFunctionStartInNS = System.nanoTime();
    if (timeOfLastReduceFunctionEndInNS > 0) {
      // Will only be true starting from the 2nd invocation.
      aggregateTimeOfInBetweenReduceInvocationsInNS +=
          (timeOfLastReduceFunctionStartInNS - timeOfLastReduceFunctionEndInNS);
    }
    if (key.getLength() > VeniceMRPartitioner.EMPTY_KEY_LENGTH
        && (!hasReportedFailure(reporter, this.isDuplicateKeyAllowed))) {
      VeniceWriterMessage message = extract(key, values, reporter);
      if (message != null) {
        try {
          sendMessageToKafka(reporter, message.getConsumer());
        } catch (VeniceException e) {
          if (e instanceof TopicAuthorizationVeniceException) {
            MRJobCounterHelper.incrWriteAclAuthorizationFailureCount(reporter, 1);
            LOGGER.error(e);
            return;
          } else if (e instanceof RecordTooLargeException) {
            MRJobCounterHelper.incrRecordTooLargeFailureCount(reporter, 1);
            LOGGER.error(e);
            return;
          }
          throw e;
        }
      }
    }
    updateExecutionTimeStatus(timeOfLastReduceFunctionStartInNS);
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

  protected VeniceWriterMessage extract(BytesWritable key, Iterator<BytesWritable> values, Reporter reporter) {
    /**
     * Don't use {@link BytesWritable#getBytes()} since it could be padded or modified by some other records later on.
     */
    byte[] keyBytes = key.copyBytes();
    if (!values.hasNext()) {
      throw new VeniceException("There is no value corresponding to key bytes: " + ByteUtils.toHexString(keyBytes));
    }
    byte[] valueBytes = values.next().copyBytes();
    if (duplicateKeyPrinter == null) {
      throw new VeniceException("'DuplicateKeyPrinter' is not initialized properly");
    }
    duplicateKeyPrinter.detectAndHandleDuplicateKeys(keyBytes, valueBytes, values, reporter);
    return new VeniceWriterMessage(
        keyBytes,
        valueBytes,
        valueSchemaId,
        getCallback(),
        isEnableWriteCompute(),
        getDerivedValueSchemaId());
  }

  protected boolean hasReportedFailure(Reporter reporter, boolean isDuplicateKeyAllowed) {
    return exceedQuota(reporter) || hasWriteAclFailure(reporter)
        || hasDuplicatedKeyWithDistinctValueFailure(reporter, isDuplicateKeyAllowed)
        || hasRecordTooLargeFailure(reporter);
  }

  private boolean hasRecordTooLargeFailure(Reporter reporter) {
    if (this.hasRecordTooLargeFailure) {
      return true;
    }
    final boolean hasRecordTooLargeFailure = MRJobCounterHelper.getRecordTooLargeFailureCount(reporter) > 0;
    if (hasRecordTooLargeFailure) {
      this.hasRecordTooLargeFailure = true;
    }
    return hasRecordTooLargeFailure;
  }

  private boolean hasDuplicatedKeyWithDistinctValueFailure(Reporter reporter, boolean isDuplicateKeyAllowed) {
    if (isDuplicateKeyAllowed) {
      return false;
    }
    return hasDuplicateKeyWithDistinctValue(reporter);
  }

  private boolean hasWriteAclFailure(Reporter reporter) {
    if (this.hasWriteAclFailure) {
      return true;
    }
    final boolean hasWriteAclFailure = MRJobCounterHelper.getWriteAclAuthorizationFailureCount(reporter) > 0;
    if (hasWriteAclFailure) {
      this.hasWriteAclFailure = true;
    }
    return hasWriteAclFailure;
  }

  private boolean hasDuplicateKeyWithDistinctValue(Reporter reporter) {
    if (this.hasDuplicateKeyWithDistinctValue) {
      return true;
    }
    final boolean hasDuplicateKeyWithDistinctValue = MRJobCounterHelper.getDuplicateKeyWithDistinctCount(reporter) > 0;
    if (hasDuplicateKeyWithDistinctValue) {
      this.hasDuplicateKeyWithDistinctValue = true;
    }
    return hasDuplicateKeyWithDistinctValue;
  }

  // Visible for testing
  boolean getExceedQuotaFlag() {
    return exceedQuota;
  }

  private boolean exceedQuota(Reporter reporter) {
    if (exceedQuota) {
      return true;
    }
    if (inputStorageQuotaTracker == null) {
      return false;
    }
    final long totalInputStorageSizeInBytes =
        MRJobCounterHelper.getTotalKeySize(reporter) + MRJobCounterHelper.getTotalValueSize(reporter);
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

  protected void sendMessageToKafka(
      Reporter reporter,
      Consumer<AbstractVeniceWriter<byte[], byte[], byte[]>> writerConsumer) {
    maybePropagateCallbackException();
    if (veniceWriter == null) {
      veniceWriter = createBasicVeniceWriter();
    }
    writerConsumer.accept(veniceWriter);
    messageSent++;
    telemetry();
    MRJobCounterHelper.incrOutputRecordCount(reporter, 1);
  }

  private boolean updatePreviousReporter(Reporter reporter) {
    if (previousReporter == null || !previousReporter.equals(reporter)) {
      previousReporter = reporter;
      return true;
    }
    return false;
  }

  private VeniceWriter<byte[], byte[], byte[]> createBasicVeniceWriter() {
    Properties writerProps = props.toProperties();
    writerProps.put(GuidUtils.GUID_GENERATOR_IMPLEMENTATION, GuidUtils.DETERMINISTIC_GUID_GENERATOR_IMPLEMENTATION);
    // Closing segments based on elapsed time should always be disabled in MR to prevent storage nodes consuming out of
    // order keys when speculative execution is in play.
    writerProps.put(VeniceWriter.MAX_ELAPSED_TIME_FOR_SEGMENT_IN_MS, -1);
    // The JobId (e.g. "job_200707121733_0003") consists of two parts. The job tracker identifier (job_200707121733)
    // and the id (0003) for the job in that specific job tracker. The job tracker identifier is converted into a long
    // by removing all the non-digit characters.
    String jobTrackerId = mapReduceJobId.getJtIdentifier().replaceAll("\\D+", "");
    try {
      writerProps.put(ConfigKeys.PUSH_JOB_MAP_REDUCE_JT_ID, Long.parseLong(jobTrackerId));
    } catch (NumberFormatException e) {
      LOGGER.warn("Unable to parse job tracker id, using default value for guid generation", e);
    }
    writerProps.put(ConfigKeys.PUSH_JOB_MAP_REDUCE_JOB_ID, mapReduceJobId.getId());
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
              && previousReporter.getProgress() == 1.0;
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
      reducerProgressHeartbeatScheduler.shutdownNow();
    }
    if (previousReporter == null) {
      LOGGER.warn("No MapReduce reporter set");
    } else {
      MRJobCounterHelper.incrReducerClosedCount(previousReporter, 1);
    }
  }

  protected DuplicateKeyPrinter initDuplicateKeyPrinter(JobConf job) {
    return new DuplicateKeyPrinter(job);
  }

  @Override
  protected void configureTask(VeniceProperties props, JobConf job) {
    this.props = props;
    this.isDuplicateKeyAllowed = props.getBoolean(ALLOW_DUPLICATE_KEY, false);
    this.mapReduceJobId = JobID.forName(job.get(MAP_REDUCE_JOB_ID_PROP));
    this.valueSchemaId = props.getInt(VALUE_SCHEMA_ID_PROP);
    this.derivedValueSchemaId = (props.containsKey(DERIVED_SCHEMA_ID_PROP)) ? props.getInt(DERIVED_SCHEMA_ID_PROP) : -1;
    this.enableWriteCompute = (props.containsKey(ENABLE_WRITE_COMPUTE)) && props.getBoolean(ENABLE_WRITE_COMPUTE);
    this.duplicateKeyPrinter = initDuplicateKeyPrinter(job);
    this.telemetryMessageInterval = props.getInt(TELEMETRY_MESSAGE_INTERVAL, 10000);
    initStorageQuotaFields(props, job);
    /**
     * A dummy background task that reports progress every 5 minutes.
     */
    reducerProgressHeartbeatScheduler.scheduleAtFixedRate(() -> {
      if (this.previousReporter != null) {
        this.previousReporter.progress();
      }
    }, 0, 5, TimeUnit.MINUTES);
  }

  private void initStorageQuotaFields(VeniceProperties props, JobConf job) {
    Long storeStorageQuota = props.containsKey(STORAGE_QUOTA_PROP) ? props.getLong(STORAGE_QUOTA_PROP) : null;
    inputStorageQuotaTracker = new InputStorageQuotaTracker(storeStorageQuota);
    if (storeStorageQuota == null) {
      return;
    }
    if (storeStorageQuota == Store.UNLIMITED_STORAGE_QUOTA) {
      exceedQuota = false;
    } else {
      exceedQuota = inputStorageQuotaTracker.exceedQuota(getTotalIncomingDataSizeInBytes(job));
    }
  }

  private long getTotalIncomingDataSizeInBytes(JobConf jobConfig) {
    JobClient hadoopJobClient = null;
    String jobIdProp = null;
    JobID jobID = null;
    RunningJob runningJob = null;
    Counters quotaCounters = null;
    try {
      hadoopJobClient = hadoopJobClientProvider.getJobClientFromConfig(jobConfig);
      jobIdProp = jobConfig.get(MAP_REDUCE_JOB_ID_PROP);
      jobID = JobID.forName(jobIdProp);
      runningJob = hadoopJobClient.getJob(jobID);
      quotaCounters = runningJob.getCounters();
      return MRJobCounterHelper.getTotalKeySize(quotaCounters) + MRJobCounterHelper.getTotalValueSize(quotaCounters);

    } catch (Exception e) {
      /**
       * Note that this will catch a NPE during tests unless the storage quota is set to
       * {@link Store.UNLIMITED_STORAGE_QUOTA}, which seems quite messed up. It manifests as {@link runningJob}
       * being null, which then prevents us from calling {@link RunningJob#getCounters()}. Obviously, this is
       * not happening in prod, though it's not completely clear why...
       *
       * TODO: Fix this so that tests are more representative of prod
       */
      throw new VeniceException(
          String.format(
              "Can't read input file size from counters; hadoopJobClient: %s; jobIdProp: %s; jobID: %s; runningJob: %s; quotaCounters: %s",
              hadoopJobClient,
              jobIdProp,
              jobID,
              runningJob,
              quotaCounters),
          e);
    } finally {
      if (hadoopJobClient != null) {
        try {
          hadoopJobClient.close();
        } catch (IOException e) {
          throw new VeniceException("Cannot close hadoopJobClient", e);
        }
      }
    }
  }

  private void maybePropagateCallbackException() {
    if (sendException != null) {
      throw new VeniceException(sendException);
    }
  }

  private void logMessageProgress() {
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
  protected void setHadoopJobClientProvider(HadoopJobClientProvider hadoopJobClientProvider) {
    this.hadoopJobClientProvider = hadoopJobClientProvider;
  }

  // Visible for testing
  protected void setExceedQuota(boolean exceedQuota) {
    this.exceedQuota = exceedQuota;
  }

  protected class ReducerProduceCallback implements PubSubProducerCallback {
    private final Reporter reporter;

    public ReducerProduceCallback(Reporter reporter) {
      this.reporter = reporter;
    }

    @Override
    public void onCompletion(PubSubProduceResult produceResult, Exception e) {
      if (e != null) {
        messageErrored.incrementAndGet();
        LOGGER.error("Exception thrown in send message callback. ", e);
        sendException = e;
      } else {
        messageCompleted.incrementAndGet();
        int partition = produceResult.getPartition();
        partitionSet.add(partition);
        if (partition != getTaskId()) {
          // Reducer input and output are not aligned!
          messageErrored.incrementAndGet();
          sendException = new VeniceException(
              String.format(
                  "The reducer is not writing to the Kafka partition that maps to its task (taskId = %d, partition = %d). "
                      + "This could mean that MR shuffling is buggy or that the configured %s (%s) is non-deterministic.",
                  getTaskId(),
                  partition,
                  VenicePartitioner.class.getSimpleName(),
                  props.getString(ConfigKeys.PARTITIONER_CLASS)));
        }
      }
      // Report progress so map-reduce framework won't kill current reducer when it finishes
      // sending all the messages to Kafka broker, but not yet flushed and closed.
      reporter.progress();
    }

    protected Progressable getProgressable() {
      return reporter;
    }
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

    DuplicateKeyPrinter(JobConf jobConf) {
      this.topic = jobConf.get(TOPIC_PROP);
      this.isDupKeyAllowed = jobConf.getBoolean(ALLOW_DUPLICATE_KEY, false);

      VeniceProperties veniceProperties = HadoopUtils.getVeniceProps(jobConf);
      this.recordReader = jobConf.getBoolean(VSON_PUSH, false)
          ? new VeniceVsonRecordReader(veniceProperties)
          : new VeniceAvroRecordReader(veniceProperties);
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
        Iterator<BytesWritable> values,
        Reporter reporter) {
      if (numOfDupKey > MAX_NUM_OF_LOG) {
        return;
      }
      boolean shouldPrint = true; // In case there are lots of duplicate keys with the same value, only print once.
      int distinctValuesToKeyCount = 0;
      int identicalValuesToKeyCount = 0;

      while (values.hasNext()) {
        if (Arrays.equals(values.next().copyBytes(), valueBytes)) {
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
      MRJobCounterHelper.incrDuplicateKeyWithIdenticalValue(reporter, identicalValuesToKeyCount);
      MRJobCounterHelper.incrDuplicateKeyWithDistinctValue(reporter, distinctValuesToKeyCount);
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
}
