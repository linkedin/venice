package com.linkedin.venice.hadoop;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.QuotaExceededException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.hadoop.ssl.SSLConfigurator;
import com.linkedin.venice.hadoop.ssl.UserCredentialsFactory;
import com.linkedin.venice.hadoop.utils.HadoopUtils;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.AbstractVeniceWriter;

import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
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
import org.apache.log4j.Logger;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import static com.linkedin.venice.hadoop.KafkaPushJob.*;
import static com.linkedin.venice.hadoop.MapReduceConstants.*;

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
public class VeniceReducer implements Reducer<BytesWritable, BytesWritable, NullWritable, NullWritable> {
  public static final String MAP_REDUCE_JOB_ID_PROP = "mapred.job.id";
  private static final Logger LOGGER = Logger.getLogger(VeniceReducer.class);
  private static final String NON_INITIALIZED_LEADER = "N/A";

  private long lastTimeThroughputWasLoggedInNS = System.nanoTime();
  private long lastMessageCompletedCount = 0;

  private AbstractVeniceWriter<byte[], byte[], byte[]> veniceWriter = null;
  private int valueSchemaId = -1;

  private VeniceProperties props;
  private JobID mapReduceJobId;
  private long nextLoggingTime = 0;
  private long minimumLoggingIntervalInMS;
  private long telemetryMessageInterval;
  private List<String> kafkaMetricsToReportAsMrCounters;
  private Map<Integer, String> partitionLeaderMap = new VeniceConcurrentHashMap<>();

  /**
   * Having dup key checking does not make sense in Mapper only mode
   * and would even cause a circular dependency issue between Mapper and reducer.
   * This flag helps disable it when running in that
   */
  private final boolean checkDupKey;
  private DuplicateKeyPrinter duplicateKeyPrinter;

  private VeniceCompressor compressor;

  private Exception sendException = null;

  private KafkaMessageCallback callback = null;
  private Reporter previousReporter = null;
  /**
   * This doesn't need to be atomic since {@link #reduce(BytesWritable, Iterator, OutputCollector, Reporter)} will be called sequentially.
   */
  private long messageSent = 0;
  private final AtomicLong messageCompleted = new AtomicLong();
  private final AtomicLong messageErrored = new AtomicLong();
  private long timeOfLastReduceFunctionStartInNS = 0;
  private long timeOfLastReduceFunctionEndInNS = 0;
  private long aggregateTimeOfReduceExecutionInNS = 0;
  private long aggregateTimeOfInBetweenReduceInvocationsInNS = 0;

  public VeniceReducer() {
    this(true);
  }

  VeniceReducer(boolean checkDupKey) {
    this.checkDupKey = checkDupKey;
  }

  @Override
  public void reduce(BytesWritable key, Iterator<BytesWritable> values, OutputCollector<NullWritable, NullWritable> output, Reporter reporter)throws IOException {
    timeOfLastReduceFunctionStartInNS = System.nanoTime();
    if (timeOfLastReduceFunctionEndInNS > 0) {
      // Will only be true starting from the 2nd invocation.
      aggregateTimeOfInBetweenReduceInvocationsInNS += (timeOfLastReduceFunctionStartInNS - timeOfLastReduceFunctionEndInNS);
    }
    /**
     * Don't use {@link BytesWritable#getBytes()} since it could be padded or modified by some other records later on.
     */
    byte[] keyBytes = key.copyBytes();
    if (!values.hasNext()) {
      throw new VeniceException("There is no value corresponding to key bytes: " +
          DatatypeConverter.printHexBinary(keyBytes));
    }
    byte[] valueBytes = values.next().copyBytes();

    sendMessageToKafka(keyBytes, valueBytes, reporter);

    if(checkDupKey) {
      duplicateKeyPrinter.handleDuplicateKeys(keyBytes, valueBytes, values, reporter);
    }
    timeOfLastReduceFunctionEndInNS = System.nanoTime();
    aggregateTimeOfReduceExecutionInNS += (timeOfLastReduceFunctionEndInNS - timeOfLastReduceFunctionStartInNS);
  }

  protected void sendMessageToKafka(byte[] keyBytes, byte[] valueBytes, Reporter reporter) {
    maybePropagateCallbackException();
    if (null == veniceWriter) {
      Properties writerProps = props.toProperties();
      writerProps.put(GuidUtils.GUID_GENERATOR_IMPLEMENTATION,
          GuidUtils.DETERMINISTIC_GUID_GENERATOR_IMPLEMENTATION);
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
      VeniceWriterFactory factory = new VeniceWriterFactory(writerProps);
      boolean chunkingEnabled = props.getBoolean(VeniceWriter.ENABLE_CHUNKING);
      veniceWriter = factory.createBasicVeniceWriter(props.getString(TOPIC_PROP), chunkingEnabled);
    }
    if (null == previousReporter || !previousReporter.equals(reporter)) {
      callback = new KafkaMessageCallback(reporter);
      previousReporter = reporter;
    }

    try {
      valueBytes = compressor.compress(valueBytes);
    } catch (IOException e) {
      throw new VeniceException("Caught an IO exception while trying to to use compression strategy: "
          + compressor.getCompressionStrategy().name(), e);
    }

    veniceWriter.put(keyBytes, valueBytes, valueSchemaId, callback);

    ++messageSent;

    telemetry();

    reporter.incrCounter(COUNTER_GROUP_KAFKA, COUNTER_OUTPUT_RECORDS, 1);
    reporter.incrCounter(COUNTER_GROUP_KAFKA, COUNTER_TOTAL_COMPRESSED_VALUE_BYTES, valueBytes.length);
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
    LOGGER.info("Kafka message progress before flushing and closing producer:");
    logMessageProgress();
    if (null != veniceWriter) {
      veniceWriter.flush();
      boolean shouldEndAllSegments = messageErrored.get() == 0 && messageSent == messageCompleted.get() &&
          previousReporter.getProgress() == 1.0;
      veniceWriter.close(shouldEndAllSegments);
    }
    maybePropagateCallbackException();
    LOGGER.info("Kafka message progress after flushing and closing producer:");
    logMessageProgress();
    if (messageSent != messageCompleted.get()) {
      throw new VeniceException("Message sent: " + messageSent + " doesn't match message completed: " + messageCompleted.get());
    }
  }

  @Override
  public void configure(JobConf job) {
    /*
     * Before producing kafka msg, reducer would check storage quota first.
     * Reducer reads input file size from mappers' counter and check it with
     * Venice controller. It stops and throws exception if the quota is exceeded.
     */
    SSLConfigurator configurator = SSLConfigurator.getSSLConfigurator(job.get(SSL_CONFIGURATOR_CLASS_CONFIG));
    try {
      Properties javaProps = configurator.setupSSLConfig(HadoopUtils.getProps(job), UserCredentialsFactory.getHadoopUserCredentials());
      props = new VeniceProperties(javaProps);
    } catch (IOException e) {
      throw new VeniceException("Could not get user credential for job:" + job.getJobName(), e);
    }
    mapReduceJobId = JobID.forName(job.get(MAP_REDUCE_JOB_ID_PROP));
    prepushStorageQuotaCheck(job, props.getLong(STORAGE_QUOTA_PROP), props.getDouble(STORAGE_ENGINE_OVERHEAD_RATIO));
    this.valueSchemaId = props.getInt(VALUE_SCHEMA_ID_PROP);
    this.minimumLoggingIntervalInMS = props.getLong(REDUCER_MINIMUM_LOGGING_INTERVAL_MS);

    if (checkDupKey) {
      this.duplicateKeyPrinter = new DuplicateKeyPrinter(job);
    }

    this.compressor =
        CompressorFactory.getCompressor(CompressionStrategy.valueOf(props.getString(COMPRESSION_STRATEGY)));

    this.telemetryMessageInterval = props.getInt(TELEMETRY_MESSAGE_INTERVAL, 10000);
    this.kafkaMetricsToReportAsMrCounters = props.getList(KAFKA_METRICS_TO_REPORT_AS_MR_COUNTERS, Collections.emptyList());
  }

  private void prepushStorageQuotaCheck(JobConf job, long maxStorageQuota, double storageEngineOverheadRatio) {
    //skip the check if the quota is unlimited
    if (maxStorageQuota == Store.UNLIMITED_STORAGE_QUOTA) {
      return;
    }

    JobClient hadoopJobClient = null;

    try {
      hadoopJobClient = new JobClient(job);
      Counters quotaCounters =
          hadoopJobClient.getJob(JobID.forName(job.get(MAP_REDUCE_JOB_ID_PROP))).getCounters();
      long incomingDataSize =
          estimatedVeniceDiskUsage(quotaCounters.findCounter(COUNTER_GROUP_QUOTA, COUNTER_TOTAL_KEY_SIZE).getValue() +
              quotaCounters.findCounter(COUNTER_GROUP_QUOTA,COUNTER_TOTAL_VALUE_SIZE).getValue(), storageEngineOverheadRatio);

      if (incomingDataSize > maxStorageQuota) {
        throw new QuotaExceededException("Venice reducer data push",
                  Long.toString(incomingDataSize), Long.toString(maxStorageQuota));
      }
    } catch (IOException e) {
      throw new VeniceException("Can't read input file size from counters", e);
    } finally {
      if (hadoopJobClient != null) {
        try {
          hadoopJobClient.close();
        } catch (IOException e) {
          throw new VeniceException("Can't close hadoopJobClient", e);
        }
      }
    }
  }

  private long estimatedVeniceDiskUsage(long inputFileSize, double storageEngineOverheadRatio) {
    return (long) (inputFileSize / storageEngineOverheadRatio);
  }

  private void printKafkaProducerMetrics(Map<String, Double> producerMetrics) {
    LOGGER.info("Internal producer metrics: " + getProducerMetricsPrettyPrint(producerMetrics));
    nextLoggingTime = System.currentTimeMillis() + minimumLoggingIntervalInMS;
  }

  private String getProducerMetricsPrettyPrint() {
    return getProducerMetricsPrettyPrint(veniceWriter.getMeasurableProducerMetrics());
  }

  private String getProducerMetricsPrettyPrint(Map<String, Double> producerMetrics) {
    StringWriter prettyPrintWriter = new StringWriter();
    PrintWriter writer = new PrintWriter(prettyPrintWriter, true);
    if (this.veniceWriter != null) {
      try {
        writer.println();
        for (Map.Entry<String, Double> entry : producerMetrics.entrySet()) {
          writer.println(entry.getKey() + ": " + entry.getValue());
        }
      } catch (Exception e) {
        LOGGER.info("Failed to get internal producer metrics because of unexpected exception", e);
      }
    } else {
      LOGGER.info("Unable to get internal producer metrics because writer is uninitialized");
    }
    return prettyPrintWriter.toString();
  }

  private void maybePropagateCallbackException() {
    if (null != sendException) {
      throw new VeniceException(
          "KafkaPushJob failed with exception. Internal producer metrics: " + getProducerMetricsPrettyPrint(),
          sendException);
    }
  }

  private void logMessageProgress() {
    LOGGER.info("Message sent: " + messageSent);
    LOGGER.info("Message completed: " + messageCompleted.get());
    LOGGER.info("Message errored: " + messageErrored.get());
  }

  // For test purpose
  protected void setVeniceWriter(AbstractVeniceWriter veniceWriter) {
    this.veniceWriter = veniceWriter;
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
        partitionLeaderMap.computeIfAbsent(partition, k -> NON_INITIALIZED_LEADER);
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
       * When running the push job in map-only mode, we can write to more than one partition, in which case
       * the per-broker counters don't make much sense, since the metrics they are based on are per-producer
       * and therefore co-mingle numbers relating to any broker.
       *
       * If there is a need to expose those metrics in map-only mode, we can revisit later.
       */
      return;
    }

    Map<String, Double> producerMetrics = veniceWriter.getMeasurableProducerMetrics();

    // We already checked that we're dealing with only one partition, so this is safe
    int partition = partitionLeaderMap.keySet().iterator().next();

    String oldLeader = partitionLeaderMap.get(partition);
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
        kafkaBrokerCounter(reporter, "initial partition ownership", newLeader, 1);
      } else {
        kafkaBrokerCounter(reporter, "observed partition leadership loss", oldLeader, 1);
        kafkaBrokerCounter(reporter, "observed partition leadership gain", newLeader, 1);
      }
    }

    // Other producer metrics, as configured
    kafkaMetricsToReportAsMrCounters.forEach(metricName -> {
      if (producerMetrics.containsKey(metricName)) {
        try {
          Double value = producerMetrics.get(metricName);
          long longValue = Math.round(value);
          kafkaBrokerCounter(reporter, metricName, newLeader, longValue);
          kafkaBrokerCounter(reporter, metricName, "all brokers", longValue);
        } catch (Exception e) {
          LOGGER.warn("Failed to report kafka metric: " + metricName + " as MR counters because of exception: "
              + e.getMessage());
        }
      }
    });

    // So that we also have a log for each time we incremented a MR counter
    printKafkaProducerMetrics(producerMetrics);
  }

  private void kafkaBrokerCounter(Reporter reporter, String metric, String broker, long value) {
    String counterName = String.format(KAFKA_PRODUCER_METRIC_FOR_BROKER, metric, broker);
    reporter.incrCounter(COUNTER_GROUP_KAFKA_BROKER, counterName, value);
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
  static class DuplicateKeyPrinter {
    private static int MAX_NUM_OF_LOG = 10;

    private boolean isDupKeyAllowed;

    private String topic;
    private Schema  keySchema;
    private VeniceKafkaSerializer keySerializer;
    private GenericDatumWriter writer;

    private int numOfDupKey = 0;

    DuplicateKeyPrinter(JobConf jobConf) {
      this.topic = jobConf.get(TOPIC_PROP);
      this.isDupKeyAllowed = jobConf.getBoolean(ALLOW_DUPLICATE_KEY, false);

      AbstractVeniceMapper mapper = jobConf.getBoolean(VSON_PUSH, false) ?
          new VeniceVsonMapper() : new VeniceAvroMapper();
      mapper.configure(jobConf);

      this.keySchema = Schema.parse(mapper.getKeySchemaStr());

      if (mapper.getKeySerializer() == null) {
        throw new VeniceException("key serializer can not be null.");
      }

      this.keySerializer = mapper.getKeySerializer();
      this.writer = new GenericDatumWriter(keySchema);
    }

    void handleDuplicateKeys(byte[] keyBytes, byte[] valueBytes, Iterator<BytesWritable> values, Reporter reporter) {
      if (values.hasNext() && numOfDupKey <= MAX_NUM_OF_LOG) {
        boolean shouldPrint = true; //in case there are lots of duplicate keys with the same value, only print once

        while (values.hasNext()) {
          if (Arrays.equals(values.next().copyBytes(), valueBytes)) {
            reporter.incrCounter(COUNTER_GROUP_DATA_QUALITY, DUPLICATE_KEY_WITH_IDENTICAL_VALUE, 1);

            if (shouldPrint) {
              shouldPrint = false;
              LOGGER.warn(printDuplicateKey(keyBytes));
            }
          } else {
            reporter.incrCounter(COUNTER_GROUP_DATA_QUALITY, DUPLICATE_KEY_WITH_DISTINCT_VALUE, 1);

            if (!isDupKeyAllowed) {
              throw new VeniceException(printDuplicateKey(keyBytes));
            } else {
              LOGGER.warn(printDuplicateKey(keyBytes));
            }
          }
        }
      }
    }

    private String printDuplicateKey(byte[] keyBytes) {
      Object keyRecord = keySerializer.deserialize(topic, keyBytes);
      ByteArrayOutputStream output = new ByteArrayOutputStream();

      try {
        Encoder jsonEncoder = AvroCompatibilityHelper.newJsonEncoder(keySchema, output, false);
        writer.write(keyRecord, jsonEncoder);
        jsonEncoder.flush();
        output.flush();

        numOfDupKey ++;
        return String.format("There are multiple records for key:\n%s", new String(output.toByteArray()));
      } catch (IOException exception) {
        throw new VeniceException(exception);
      }
    }
  }
}
