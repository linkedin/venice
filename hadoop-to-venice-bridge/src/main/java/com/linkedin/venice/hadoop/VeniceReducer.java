package com.linkedin.venice.hadoop;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.RecordTooLargeException;
import com.linkedin.venice.exceptions.TopicAuthorizationVeniceException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.hadoop.utils.HadoopUtils;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import com.linkedin.venice.writer.PutMetadata;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.hadoop.util.Progressable;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.linkedin.venice.hadoop.VenicePushJob.*;


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
public class VeniceReducer
    extends AbstractMapReduceTask
    implements Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

  public static class VeniceWriterMessage {
    private final byte[] keyBytes;
    private final byte[] valueBytes;
    private final int valueSchemaId;
    private final int replicationMetadataVersionId;
    private final ByteBuffer replicationMetadataPayload;

    public VeniceWriterMessage(byte[] keyBytes, byte[] valueBytes, int valueSchemaId) {
      this(keyBytes, valueBytes, valueSchemaId, -1, null);
    }

    public VeniceWriterMessage(byte[] keyBytes, byte[] valueBytes, int valueSchemaId, int replicationMetadataVersionId, ByteBuffer replicationMetadataPayload) {
      this.keyBytes = keyBytes;
      this.valueBytes = valueBytes;
      this.valueSchemaId = valueSchemaId;
      this.replicationMetadataPayload = replicationMetadataPayload;
      this.replicationMetadataVersionId = replicationMetadataVersionId;
    }

    public ByteBuffer getReplicationMetadataPayload() {
      return replicationMetadataPayload;
    }

    public int getReplicationMetadataVersionId() {
      return replicationMetadataVersionId;
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
  private static final String NON_INITIALIZED_LEADER = "N/A";

  private long lastTimeThroughputWasLoggedInNS = System.nanoTime();
  private long lastMessageCompletedCount = 0;

  private AbstractVeniceWriter<byte[], byte[], byte[]> veniceWriter = null;
  private int valueSchemaId = -1;
  private int derivedValueSchemaId = -1;
  private boolean enableWriteCompute = false;

  private VeniceProperties props;
  private JobID mapReduceJobId;
  private long nextLoggingTime = 0;
  private long minimumLoggingIntervalInMS;
  private long telemetryMessageInterval;
  private List<String> kafkaMetricsToReportAsMrCounters;
  private final Map<Integer, String> partitionLeaderMap = new VeniceConcurrentHashMap<>();
  private DuplicateKeyPrinter duplicateKeyPrinter;
  private Exception sendException = null;

  /**
   * Visible for testing purpose
   *
   * IMPORTANT: Noticed that this callback is reused in different messages, do not put information that is coupled with
   *            each message inside this callback.
   */
  protected KafkaMessageCallback callback = null;
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

  /**
   * For incremental push to RT jobs, current version of the store will be embedded into every messages from inc push.
   */
  private Optional<Integer> targetStoreVersionForIncPush = Optional.empty();

  @Override
  public void reduce(
      BytesWritable key,
      Iterator<BytesWritable> values,
      OutputCollector<BytesWritable, BytesWritable> output,
      Reporter reporter
  ) {
    if (updatePreviousReporter(reporter)) {
      callback = new KafkaMessageCallback(reporter);
    }
    final long timeOfLastReduceFunctionStartInNS = System.nanoTime();
    if (timeOfLastReduceFunctionEndInNS > 0) {
      // Will only be true starting from the 2nd invocation.
      aggregateTimeOfInBetweenReduceInvocationsInNS += (timeOfLastReduceFunctionStartInNS - timeOfLastReduceFunctionEndInNS);
    }
    if (key.getLength() > VeniceMRPartitioner.EMPTY_KEY_LENGTH && (!hasReportedFailure(reporter, this.isDuplicateKeyAllowed))) {
      Optional<VeniceWriterMessage> message = extract(key, values, reporter);
      if (message.isPresent()) {
        try {
          if (message.get().replicationMetadataPayload != null) {
            PutMetadata putMetadata = new PutMetadata(message.get().getReplicationMetadataVersionId(), message.get().getReplicationMetadataPayload());
            sendMessageToKafka(message.get().keyBytes, message.get().valueBytes, message.get().valueSchemaId, reporter, putMetadata);
          } else {
            sendMessageToKafka(message.get().keyBytes, message.get().valueBytes, message.get().valueSchemaId, reporter);
          }
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

  protected Optional<VeniceWriterMessage> extract(BytesWritable key, Iterator<BytesWritable> values, Reporter reporter) {
    /**
     * Don't use {@link BytesWritable#getBytes()} since it could be padded or modified by some other records later on.
     */
    byte[] keyBytes = key.copyBytes();
    if (!values.hasNext()) {
      throw new VeniceException("There is no value corresponding to key bytes: " +
          ByteUtils.toHexString(keyBytes));
    }
    byte[] valueBytes = values.next().copyBytes();
    if (duplicateKeyPrinter == null) {
      throw new VeniceException("'DuplicateKeyPrinter' is not initialized properly");
    }
    duplicateKeyPrinter.detectAndHandleDuplicateKeys(keyBytes, valueBytes, values, reporter);
    return Optional.of(new VeniceWriterMessage(keyBytes, valueBytes, valueSchemaId));
  }

  protected boolean hasReportedFailure(Reporter reporter, boolean isDuplicateKeyAllowed) {
    return exceedQuota(reporter)
        || hasWriteAclFailure(reporter)
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
    final boolean hasDuplicateKeyWithDistinctValue =
        MRJobCounterHelper.getDuplicateKeyWithDistinctCount(reporter) > 0;
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

  protected void sendMessageToKafka(byte[] keyBytes, byte[] valueBytes, int valueSchemaId, Reporter reporter) {
    sendMessageToKafka(keyBytes, valueBytes, valueSchemaId, reporter, null);
  }

  protected void sendMessageToKafka(byte[] keyBytes, byte[] valueBytes, int valueSchemaId, Reporter reporter, PutMetadata putMetadata) {
    maybePropagateCallbackException();
    if (null == veniceWriter) {
      veniceWriter = createBasicVeniceWriter();
    }
    if (enableWriteCompute && derivedValueSchemaId > 0) {
      veniceWriter.update(keyBytes, valueBytes, valueSchemaId, derivedValueSchemaId, callback);
    } else {
      veniceWriter.put(keyBytes, valueBytes, valueSchemaId, callback, putMetadata);
    }
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
    writerProps.put(GuidUtils.GUID_GENERATOR_IMPLEMENTATION,
        GuidUtils.DETERMINISTIC_GUID_GENERATOR_IMPLEMENTATION);
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
    VeniceWriterFactory veniceWriterFactoryFactory = new VeniceWriterFactory(writerProps);
    boolean chunkingEnabled = props.getBoolean(VeniceWriter.ENABLE_CHUNKING);
    VenicePartitioner partitioner = PartitionUtils.getVenicePartitioner(props);
    return veniceWriterFactoryFactory.createVeniceWriter(props.getString(TOPIC_PROP), new DefaultSerializer(), new DefaultSerializer(),
        new DefaultSerializer(), Optional.of(chunkingEnabled), SystemTime.INSTANCE, partitioner, Optional.empty(), this.targetStoreVersionForIncPush);
  }

  private void telemetry() {
    if (messageSent % telemetryMessageInterval == 0) {
      double timeSinceLastMeasurementInSeconds = (System.nanoTime() - lastTimeThroughputWasLoggedInNS) / (double) Time.NS_PER_SECOND;

      // Mapping rate measurement
      long mrFrameworkRate = (long) (telemetryMessageInterval / timeSinceLastMeasurementInSeconds);
      LOGGER.info("MR Framework records processed: " + messageSent
          + ", total time spent: " + Utils.makeTimePretty(aggregateTimeOfInBetweenReduceInvocationsInNS)
          + ", current throughput: " + Utils.makeLargeNumberPretty(mrFrameworkRate) + " rec/s");

      // Produce rate measurement
      long newMessageCompletedCount = messageCompleted.get();
      long messagesProducedSinceLastLog = newMessageCompletedCount - lastMessageCompletedCount;
      long produceRate = (long) (messagesProducedSinceLastLog / timeSinceLastMeasurementInSeconds);
      LOGGER.info("Kafka records produced: " + newMessageCompletedCount
          + ", total time spent: " + Utils.makeTimePretty(aggregateTimeOfReduceExecutionInNS)
          + ", current throughput: " + Utils.makeLargeNumberPretty(produceRate) + " rec/s");

      // Bookkeeping for the next measurement iteration
      lastTimeThroughputWasLoggedInNS = System.nanoTime();
      lastMessageCompletedCount = newMessageCompletedCount;
    }

    if (nextLoggingTime < System.currentTimeMillis()) {
      LOGGER.info("Internal producer metrics (triggered by minimum time interval): " + getProducerMetricsPrettyPrint());
      nextLoggingTime = System.currentTimeMillis() + minimumLoggingIntervalInMS;
    }
  }

  @Override
  public void close() throws IOException {
    try {
      LOGGER.info("Kafka message progress before flushing and closing producer:");
      logMessageProgress();
      if (null != veniceWriter) {
        boolean shouldEndAllSegments = false;
        try {
          veniceWriter.flush();
          shouldEndAllSegments = messageErrored.get() == 0 && messageSent == messageCompleted.get() &&
              previousReporter.getProgress() == 1.0;
        } finally {
          veniceWriter.close(shouldEndAllSegments);
        }
      }
      maybePropagateCallbackException();
      LOGGER.info("Kafka message progress after flushing and closing producer:");
      logMessageProgress();
      if (messageSent != messageCompleted.get()) {
        throw new VeniceException("Message sent: " + messageSent + " doesn't match message completed: " + messageCompleted.get());
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
    this.minimumLoggingIntervalInMS = props.getLong(REDUCER_MINIMUM_LOGGING_INTERVAL_MS);
    this.duplicateKeyPrinter = initDuplicateKeyPrinter(job);
    this.telemetryMessageInterval = props.getInt(TELEMETRY_MESSAGE_INTERVAL, 10000);
    this.kafkaMetricsToReportAsMrCounters = props.getList(KAFKA_METRICS_TO_REPORT_AS_MR_COUNTERS, Collections.emptyList());
    this.targetStoreVersionForIncPush = props.containsKey(TARGET_VERSION_FOR_INCREMENTAL_PUSH)
        ? Optional.of(props.getInt(TARGET_VERSION_FOR_INCREMENTAL_PUSH)) : Optional.empty();
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
    try {
      hadoopJobClient = hadoopJobClientProvider.getJobClientFromConfig(jobConfig);
      Counters quotaCounters = hadoopJobClient.getJob(JobID.forName(jobConfig.get(MAP_REDUCE_JOB_ID_PROP))).getCounters();
      return MRJobCounterHelper.getTotalKeySize(quotaCounters) + MRJobCounterHelper.getTotalValueSize(quotaCounters);

    } catch (IOException e) {
      throw new VeniceException("Can't read input file size from counters", e);
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

  private void printKafkaProducerMetrics(Map<String, Double> producerMetrics) {
    LOGGER.info("Internal producer metrics: " + getProducerMetricsPrettyPrint(producerMetrics));
    nextLoggingTime = System.currentTimeMillis() + minimumLoggingIntervalInMS;
  }

  private String getProducerMetricsPrettyPrint() {
    if (this.veniceWriter == null) {
      LOGGER.info("Unable to get internal producer metrics because writer is uninitialized");
      return "";
    }
    return getProducerMetricsPrettyPrint(veniceWriter.getMeasurableProducerMetrics());
  }

  private String getProducerMetricsPrettyPrint(Map<String, Double> producerMetrics) {
    try (StringWriter prettyPrintWriter = new StringWriter();
        PrintWriter writer = new PrintWriter(prettyPrintWriter, true)) {
      writer.println();
      for (Map.Entry<String, Double> entry : producerMetrics.entrySet()) {
        writer.println(entry.getKey() + ": " + entry.getValue());
      }
      return prettyPrintWriter.toString();
    } catch (IOException e) {
      LOGGER.info("Failed to get internal producer metrics because of unexpected exception", e);
      return "";
    }
  }

  private void maybePropagateCallbackException() {
    if (null != sendException) {
      throw new VeniceException(
          "VenicePushJob failed with exception. Internal producer metrics: " + getProducerMetricsPrettyPrint(),
          sendException);
    }
  }

  private void logMessageProgress() {
    LOGGER.info(String.format("Message sent: %d, message completed: %d, message errored: %d",
        messageSent, messageCompleted.get(), messageErrored.get()));
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

  protected class KafkaMessageCallback implements Callback {
    private final Reporter reporter;

    public KafkaMessageCallback(Reporter reporter) {
      this.reporter = reporter;
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
      if (null != e) {
        messageErrored.incrementAndGet();
        LOGGER.error("Exception thrown in send message callback. ", e);
        LOGGER.info("Internal producer metrics when exception is encountered: " + getProducerMetricsPrettyPrint());
        sendException = e;
      } else {
        long messageCount = messageCompleted.incrementAndGet();
        int partition = recordMetadata.partition();
        partitionLeaderMap.putIfAbsent(partition, NON_INITIALIZED_LEADER);
        if (partition != getTaskId()) {
          // Reducer input and output are not aligned!
          messageErrored.incrementAndGet();
          sendException = new VeniceException(String.format(
              "The reducer is not writing to the Kafka partition that maps to its task. This could mean that MR "
                  + "shuffling is buggy or that the configured %s (%s) is non-deterministic. Active partitions: %s",
              VenicePartitioner.class.getSimpleName(),
              props.getString(ConfigKeys.PARTITIONER_CLASS),
              partitionLeaderMap.keySet().toString()));
        }
        if (messageCount % telemetryMessageInterval == 0) {
          kafkaTelemetry(reporter);
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

  private void kafkaTelemetry(Reporter reporter) {
    if (partitionLeaderMap.size() != 1) {
      /**
       * History lesson: it used to be possible to the push job in map-only mode, where each mapper would write
       * directly to Kafka, with no shuffling stage and no reducers. In that mode, mappers could write to more
       * than one partition, in which case the per-broker counters didn't make much sense, since the metrics
       * they are based on are per-producer and therefore co-mingle numbers relating to any broker.
       *
       * Nowadays, the map-only mode doesn't exist anymore, but it still wouldn't make sense to measure this
       * telemetry in case of interacting with many partitions. At this point, this check is just defensive
       * coding.
       */
      return;
    }

    Map<String, Double> producerMetrics = veniceWriter.getMeasurableProducerMetrics();

    // We already checked that we're dealing with only one partition, so this is safe
    Map.Entry<Integer, String> entry = partitionLeaderMap.entrySet().iterator().next();
    int partition = entry.getKey();
    String oldLeader = entry.getValue();

    String newLeader;
    try {
      newLeader = veniceWriter.getBrokerLeaderHostname(props.getString(TOPIC_PROP), partition);
    } catch (VeniceException e) {
      LOGGER.warn("Got an exception while determining the broker leader for partition " + partition
          + ". Will skip this round of Kafka telemetry.", e);
      return;
    }

    // Partition leadership info
    if (!oldLeader.equals(newLeader)) {
      partitionLeaderMap.put(partition, newLeader);
      if (oldLeader.equals(NON_INITIALIZED_LEADER)) {
        incrementKafkaBrokerCounter(reporter, "initial partition ownership", newLeader, 1);
      } else {
        incrementKafkaBrokerCounter(reporter, "observed partition leadership loss", oldLeader, 1);
        incrementKafkaBrokerCounter(reporter, "observed partition leadership gain", newLeader, 1);
      }
    }

    // Other producer metrics, as configured
    kafkaMetricsToReportAsMrCounters.forEach(metricName -> {
      if (producerMetrics.containsKey(metricName)) {
        try {
          Double value = producerMetrics.get(metricName);
          long longValue = Math.round(value);
          incrementKafkaBrokerCounter(reporter, metricName, newLeader, longValue);
          incrementKafkaBrokerCounter(reporter, metricName, "all brokers", longValue);
        } catch (Exception e) {
          LOGGER.warn("Failed to report kafka metric: " + metricName + " as MR counters because of exception: "
              + e.getMessage());
        }
      }
    });

    // So that we also have a log for each time we incremented a MR counter
    printKafkaProducerMetrics(producerMetrics);
  }

  private void incrementKafkaBrokerCounter(Reporter reporter, String metric, String broker, long incrementAmount) {
    reporter.incrCounter(
        MRJobCounterHelper.COUNTER_GROUP_KAFKA_BROKER,
        MRJobCounterHelper.getKafkaProducerMetricForBrokerCounterName(metric, broker),
        incrementAmount
    );
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
    private final Schema  keySchema;
    private final AbstractVeniceRecordReader<?, ?> recordReader;
    private final VeniceKafkaSerializer<?> keySerializer;
    private final GenericDatumWriter<Object> avroDatumWriter;
    private int numOfDupKey = 0;

    DuplicateKeyPrinter(JobConf jobConf) {
      this.topic = jobConf.get(TOPIC_PROP);
      this.isDupKeyAllowed = jobConf.getBoolean(ALLOW_DUPLICATE_KEY, false);

      VeniceProperties veniceProperties = HadoopUtils.getVeniceProps(jobConf);
      this.recordReader = jobConf.getBoolean(VSON_PUSH, false) ?
          new VeniceVsonRecordReader(veniceProperties) : new VeniceAvroRecordReader(veniceProperties);
      this.keySchema = Schema.parse(recordReader.getKeySchemaStr());

      if (recordReader.getKeySerializer() == null) {
        throw new VeniceException("key serializer can not be null.");
      }

      this.keySerializer = recordReader.getKeySerializer();
      this.avroDatumWriter = new GenericDatumWriter<>(keySchema);
    }

    protected void detectAndHandleDuplicateKeys(byte[] keyBytes, byte[] valueBytes, Iterator<BytesWritable> values, Reporter reporter) {
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
