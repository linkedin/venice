package com.linkedin.venice.spark.datawriter.jobs;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.KAFKA_PRODUCER_REQUEST_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.KAFKA_PRODUCER_RETRIES_CONFIG;
import static com.linkedin.venice.ConfigKeys.PARTITIONER_CLASS;
import static com.linkedin.venice.ConfigKeys.PUSH_JOB_GUID_LEAST_SIGNIFICANT_BITS;
import static com.linkedin.venice.ConfigKeys.PUSH_JOB_GUID_MOST_SIGNIFICANT_BITS;
import static com.linkedin.venice.spark.SparkConstants.DEFAULT_SCHEMA;
import static com.linkedin.venice.spark.SparkConstants.DEFAULT_SCHEMA_WITH_PARTITION;
import static com.linkedin.venice.spark.SparkConstants.DEFAULT_SPARK_CLUSTER;
import static com.linkedin.venice.spark.SparkConstants.KEY_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.PARTITION_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.SPARK_CASE_SENSITIVE_CONFIG;
import static com.linkedin.venice.spark.SparkConstants.SPARK_CLUSTER_CONFIG;
import static com.linkedin.venice.spark.SparkConstants.SPARK_DATA_WRITER_CONF_PREFIX;
import static com.linkedin.venice.spark.SparkConstants.SPARK_LEADER_CONFIG;
import static com.linkedin.venice.spark.SparkConstants.SPARK_SESSION_CONF_PREFIX;
import static com.linkedin.venice.spark.SparkConstants.VALUE_COLUMN_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ALLOW_DUPLICATE_KEY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.BATCH_NUM_BYTES_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.COMPRESSION_METRIC_COLLECTION_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.COMPRESSION_STRATEGY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DERIVED_SCHEMA_ID_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ENABLE_WRITE_COMPUTE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_SOURCE_COMPRESSION_STRATEGY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_SOURCE_TOPIC_CHUNKING_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_SECURITY_PROTOCOL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PARTITION_COUNT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_ENABLE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_POLICY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_START_TIMESTAMP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.RMD_SCHEMA_DIR;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_CONFIGURATOR_CLASS_CONFIG;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_STORE_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_TRUST_STORE_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.STORAGE_QUOTA_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TELEMETRY_MESSAGE_INTERVAL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TOPIC_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_SCHEMA_ID_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ZSTD_COMPRESSION_LEVEL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ZSTD_DICTIONARY_CREATION_REQUIRED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ZSTD_DICTIONARY_CREATION_SUCCESS;

import com.github.luben.zstd.Zstd;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.hadoop.PushJobSetting;
import com.linkedin.venice.hadoop.input.kafka.ttl.TTLResolutionPolicy;
import com.linkedin.venice.hadoop.ssl.TempFileSSLConfigurator;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import com.linkedin.venice.jobs.DataWriterComputeJob;
import com.linkedin.venice.spark.datawriter.partition.PartitionSorter;
import com.linkedin.venice.spark.datawriter.partition.VeniceSparkPartitioner;
import com.linkedin.venice.spark.datawriter.recordprocessor.SparkInputRecordProcessorFactory;
import com.linkedin.venice.spark.datawriter.task.DataWriterAccumulators;
import com.linkedin.venice.spark.datawriter.task.SparkDataWriterTaskTracker;
import com.linkedin.venice.spark.datawriter.writer.SparkPartitionWriterFactory;
import com.linkedin.venice.spark.utils.SparkPartitionUtils;
import com.linkedin.venice.spark.utils.SparkScalaUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.AccumulatorV2;


/**
 * The implementation of {@link DataWriterComputeJob} for Spark engine.
 */
public abstract class AbstractDataWriterSparkJob extends DataWriterComputeJob {
  private static final Logger LOGGER = LogManager.getLogger(AbstractDataWriterSparkJob.class);

  private VeniceProperties props;
  private PushJobSetting pushJobSetting;

  private String jobGroupId;
  private SparkSession sparkSession;
  private Dataset<Row> dataFrame;
  private DataWriterAccumulators accumulatorsForDataWriterJob;
  private SparkDataWriterTaskTracker taskTracker;

  @Override
  public void configure(VeniceProperties props, PushJobSetting pushJobSetting) {
    this.props = props;
    this.pushJobSetting = pushJobSetting;
    setupDefaultSparkSessionForDataWriterJob(pushJobSetting, props);
    setupSparkDataWriterJobFlow(pushJobSetting);
  }

  private void setupSparkDataWriterJobFlow(PushJobSetting pushJobSetting) {
    ExpressionEncoder<Row> rowEncoder = RowEncoder.apply(DEFAULT_SCHEMA);
    ExpressionEncoder<Row> rowEncoderWithPartition = RowEncoder.apply(DEFAULT_SCHEMA_WITH_PARTITION);
    int numOutputPartitions = pushJobSetting.partitionCount;

    // Load data from input path
    Dataset<Row> dataFrameForDataWriterJob = getInputDataFrame();
    Objects.requireNonNull(dataFrameForDataWriterJob, "The input data frame cannot be null");

    Properties jobProps = new Properties();
    sparkSession.conf().getAll().foreach(entry -> jobProps.setProperty(entry._1, entry._2));
    JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
    Broadcast<Properties> broadcastProperties = sparkContext.broadcast(jobProps);
    accumulatorsForDataWriterJob = new DataWriterAccumulators(sparkSession);
    taskTracker = new SparkDataWriterTaskTracker(accumulatorsForDataWriterJob);

    // Validate the schema of the input data
    validateDataFrameSchema(dataFrameForDataWriterJob);

    // Convert all rows to byte[], byte[] pairs (compressed if compression is enabled)
    // We could have worked with "map", but because of spraying all PartitionWriters, we need to use "flatMap"
    dataFrameForDataWriterJob = dataFrameForDataWriterJob
        .flatMap(new SparkInputRecordProcessorFactory(broadcastProperties, accumulatorsForDataWriterJob), rowEncoder);

    // TODO: Add map-side combiner to reduce the data size before shuffling

    // Partition the data using the custom partitioner and sort the data within that partition
    dataFrameForDataWriterJob = SparkPartitionUtils.repartitionAndSortWithinPartitions(
        dataFrameForDataWriterJob,
        new VeniceSparkPartitioner(broadcastProperties, numOutputPartitions),
        new PartitionSorter());

    // Add a partition column to all rows based on the custom partitioner
    dataFrameForDataWriterJob =
        dataFrameForDataWriterJob.withColumn(PARTITION_COLUMN_NAME, functions.spark_partition_id());

    // Write the data to PubSub
    dataFrameForDataWriterJob = dataFrameForDataWriterJob.mapPartitions(
        new SparkPartitionWriterFactory(broadcastProperties, accumulatorsForDataWriterJob),
        rowEncoderWithPartition);

    this.dataFrame = dataFrameForDataWriterJob;
  }

  /**
   * Common configuration for all the Mapreduce Jobs run as part of VPJ
   *
   * @param config
   */
  private void setupCommonSparkConf(VeniceProperties props, RuntimeConfig config, PushJobSetting pushJobSetting) {
    if (pushJobSetting.enableSSL) {
      config.set(
          SSL_CONFIGURATOR_CLASS_CONFIG,
          props.getString(SSL_CONFIGURATOR_CLASS_CONFIG, TempFileSSLConfigurator.class.getName()));
      config.set(SSL_KEY_STORE_PROPERTY_NAME, props.getString(SSL_KEY_STORE_PROPERTY_NAME));
      config.set(SSL_TRUST_STORE_PROPERTY_NAME, props.getString(SSL_TRUST_STORE_PROPERTY_NAME));
      config.set(SSL_KEY_PASSWORD_PROPERTY_NAME, props.getString(SSL_KEY_PASSWORD_PROPERTY_NAME));
    }

    /** compression related common configs */
    config
        .set(COMPRESSION_METRIC_COLLECTION_ENABLED, String.valueOf(pushJobSetting.compressionMetricCollectionEnabled));
    config.set(ZSTD_DICTIONARY_CREATION_REQUIRED, String.valueOf(pushJobSetting.isZstdDictCreationRequired));

    config.set(SPARK_CASE_SENSITIVE_CONFIG, String.valueOf(true));
  }

  private void setupDefaultSparkSessionForDataWriterJob(PushJobSetting pushJobSetting, VeniceProperties props) {
    this.jobGroupId = pushJobSetting.jobId + ":venice_push_job-" + pushJobSetting.topic;

    SparkConf sparkConf = new SparkConf();
    SparkSession.Builder sparkSessionBuilder = SparkSession.builder().appName(jobGroupId).config(sparkConf);
    if (sparkConf.get(SPARK_LEADER_CONFIG, null) == null) {
      sparkSessionBuilder.master(props.getString(SPARK_CLUSTER_CONFIG, DEFAULT_SPARK_CLUSTER));
    }

    for (String key: props.keySet()) {
      if (key.toLowerCase().startsWith(SPARK_SESSION_CONF_PREFIX)) {
        String overrideKey = key.substring(SPARK_SESSION_CONF_PREFIX.length());
        sparkSessionBuilder.config(overrideKey, props.getString(key));
      }
    }

    sparkSession = sparkSessionBuilder.getOrCreate();
    SparkContext sparkContext = sparkSession.sparkContext();

    // Set job group to make the job be killable programmatically
    sparkContext.setJobGroup(jobGroupId, "VenicePushJob Data Writer for topic: " + pushJobSetting.topic, true);

    // Some configs to be able to identify the jobs from Spark UI
    sparkContext.setCallSite(jobGroupId);

    RuntimeConfig jobConf = sparkSession.conf();
    setupCommonSparkConf(props, jobConf, pushJobSetting);
    jobConf.set(BATCH_NUM_BYTES_PROP, pushJobSetting.batchNumBytes);
    jobConf.set(TOPIC_PROP, pushJobSetting.topic);
    jobConf.set(KAFKA_BOOTSTRAP_SERVERS, pushJobSetting.kafkaUrl);
    jobConf.set(PARTITIONER_CLASS, pushJobSetting.partitionerClass);
    // flatten partitionerParams since RuntimeConfig class does not support set an object
    if (pushJobSetting.partitionerParams != null) {
      pushJobSetting.partitionerParams.forEach((key, value) -> jobConf.set(key, value));
    }
    jobConf.set(PARTITION_COUNT, pushJobSetting.partitionCount);
    if (pushJobSetting.sslToKafka) {
      jobConf.set(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, KAFKA_SECURITY_PROTOCOL);
      props.keySet().stream().filter(key -> key.toLowerCase().startsWith(SSL_PREFIX)).forEach(key -> {
        jobConf.set(key, props.getString(key));
      });
    }
    jobConf.set(ALLOW_DUPLICATE_KEY, pushJobSetting.isDuplicateKeyAllowed);
    jobConf.set(VeniceWriter.ENABLE_CHUNKING, pushJobSetting.chunkingEnabled);
    jobConf.set(VeniceWriter.ENABLE_RMD_CHUNKING, pushJobSetting.rmdChunkingEnabled);
    jobConf.set(VeniceWriter.MAX_RECORD_SIZE_BYTES, pushJobSetting.maxRecordSizeBytes);

    jobConf.set(STORAGE_QUOTA_PROP, pushJobSetting.storeStorageQuota);

    if (pushJobSetting.isSourceKafka) {
      // Use some fake value schema id here since it won't be used
      jobConf.set(VALUE_SCHEMA_ID_PROP, -1);
      /**
       * Kafka input topic could be inferred from the store name, but absent from the original properties.
       * So here will set it up from {@link #pushJobSetting}.
       */
      jobConf.set(KAFKA_INPUT_TOPIC, pushJobSetting.kafkaInputTopic);
      jobConf.set(KAFKA_INPUT_BROKER_URL, pushJobSetting.kafkaInputBrokerUrl);
      jobConf.set(REPUSH_TTL_ENABLE, pushJobSetting.repushTTLEnabled);
      jobConf.set(REPUSH_TTL_START_TIMESTAMP, pushJobSetting.repushTTLStartTimeMs);
      if (pushJobSetting.repushTTLEnabled) {
        // Currently, we only support one policy. Thus, we don't allow overriding it.
        jobConf.set(REPUSH_TTL_POLICY, TTLResolutionPolicy.RT_WRITE_ONLY.getValue());
        jobConf.set(RMD_SCHEMA_DIR, pushJobSetting.rmdSchemaDir);
      }
      // Pass the compression strategy of source version to repush MR job
      jobConf.set(
          KAFKA_INPUT_SOURCE_COMPRESSION_STRATEGY,
          pushJobSetting.sourceKafkaInputVersionInfo.getCompressionStrategy().name());
      jobConf.set(
          KAFKA_INPUT_SOURCE_TOPIC_CHUNKING_ENABLED,
          pushJobSetting.sourceKafkaInputVersionInfo.isChunkingEnabled());
    } else {
      jobConf.set(VALUE_SCHEMA_ID_PROP, pushJobSetting.valueSchemaId);
      jobConf.set(DERIVED_SCHEMA_ID_PROP, pushJobSetting.derivedSchemaId);
    }
    jobConf.set(ENABLE_WRITE_COMPUTE, pushJobSetting.enableWriteCompute);

    if (!props.containsKey(KAFKA_PRODUCER_REQUEST_TIMEOUT_MS)) {
      // If the push job plug-in doesn't specify the request timeout config, default will be infinite
      jobConf.set(KAFKA_PRODUCER_REQUEST_TIMEOUT_MS, Integer.MAX_VALUE);
    }
    if (!props.containsKey(KAFKA_PRODUCER_RETRIES_CONFIG)) {
      // If the push job plug-in doesn't specify the retries config, default will be infinite
      jobConf.set(KAFKA_PRODUCER_RETRIES_CONFIG, Integer.MAX_VALUE);
    }
    if (!props.containsKey(KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS)) {
      // If the push job plug-in doesn't specify the delivery timeout config, default will be infinite
      jobConf.set(KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS, Integer.MAX_VALUE);
    }

    jobConf.set(TELEMETRY_MESSAGE_INTERVAL, props.getString(TELEMETRY_MESSAGE_INTERVAL, "10000"));
    jobConf.set(EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED, pushJobSetting.extendedSchemaValidityCheckEnabled);

    // Compression related
    // Note that COMPRESSION_STRATEGY is from topic creation response as it might be different from the store config
    // (eg: for inc push)
    jobConf.set(
        COMPRESSION_STRATEGY,
        pushJobSetting.topicCompressionStrategy != null
            ? pushJobSetting.topicCompressionStrategy.name()
            : CompressionStrategy.NO_OP.name());
    jobConf.set(
        ZSTD_COMPRESSION_LEVEL,
        props.getString(ZSTD_COMPRESSION_LEVEL, String.valueOf(Zstd.maxCompressionLevel())));
    jobConf.set(ZSTD_DICTIONARY_CREATION_SUCCESS, pushJobSetting.isZstdDictCreationSuccess);

    // We generate a random UUID once, and the tasks of the compute job can use this to build the same producerGUID
    // deterministically.
    UUID producerGuid = UUID.randomUUID();
    jobConf.set(PUSH_JOB_GUID_MOST_SIGNIFICANT_BITS, producerGuid.getMostSignificantBits());
    jobConf.set(PUSH_JOB_GUID_LEAST_SIGNIFICANT_BITS, producerGuid.getLeastSignificantBits());

    /**
     * Override the configs following the rules:
     * <ul>
     *   <li>Pass-through the properties whose names start with the prefixes defined in {@link PASS_THROUGH_CONFIG_PREFIXES}.</li>
     *   <li>Override the properties that are specified with the {@link SPARK_DATA_WRITER_CONF_PREFIX} prefix.</li>
     * </ul>
     **/
    for (String configKey: props.keySet()) {
      String lowerCaseConfigKey = configKey.toLowerCase();
      if (lowerCaseConfigKey.startsWith(SPARK_DATA_WRITER_CONF_PREFIX)) {
        String overrideKey = configKey.substring(SPARK_DATA_WRITER_CONF_PREFIX.length());
        jobConf.set(overrideKey, props.getString(configKey));
      }
      for (String prefix: PASS_THROUGH_CONFIG_PREFIXES) {
        if (lowerCaseConfigKey.startsWith(prefix)) {
          jobConf.set(configKey, props.getString(configKey));
          break;
        }
      }
    }
  }

  protected SparkSession getSparkSession() {
    return sparkSession;
  }

  /**
   * Get the data frame based on the user's input data. The schema of the {@link Row} has the following constraints:
   * <ul>
   *   <li>Must contain a field "key" with the schema: {@link DataTypes#BinaryType}. This is the key of the record represented in serialized Avro.</li>
   *   <li>Must contain a field "value" with the schema: {@link DataTypes#BinaryType}. This is the value of the record represented in serialized Avro.</li>
   *   <li>Must not contain fields with names beginning with "_". These are reserved for internal use.</li>
   *   <li>Can contain fields that do not violate the above constraints</li>
   * </ul>
   * @see {@link #validateDataFrameSchema(StructType)}
   *
   * @return The data frame based on the user's input data
   */
  protected abstract Dataset<Row> getUserInputDataFrame();

  private Dataset<Row> getInputDataFrame() {
    if (pushJobSetting.isSourceKafka) {
      throw new VeniceUnsupportedOperationException("Spark push job for repush workloads");
    } else {
      return getUserInputDataFrame();
    }
  }

  // Set configs for both SparkSession (data processing) and DataFrameReader (input format)
  protected void setInputConf(SparkSession session, DataFrameReader dataFrameReader, String key, String value) {
    session.conf().set(key, value);
    dataFrameReader.option(key, value);
  }

  @Override
  public DataWriterTaskTracker getTaskTracker() {
    return taskTracker;
  }

  // This is a part of the public API. Do not remove.
  @SuppressWarnings("unused")
  protected VeniceProperties getJobProperties() {
    return props;
  }

  // This is a part of the public API. Do not remove.
  @Override
  public PushJobSetting getPushJobSetting() {
    return pushJobSetting;
  }

  @Override
  protected void runComputeJob() {
    LOGGER.info("Triggering Spark job for data writer");
    try {
      // For VPJ, we don't care about the output from the DAG. ".count()" is an action that will trigger execution of
      // the DAG to completion and will not copy all the rows to the driver to be more memory efficient.
      dataFrame.count();
    } finally {
      // No matter what, always log the final accumulator values
      logAccumulatorValues();
    }
  }

  @Override
  public void kill() {
    super.kill();
    if (sparkSession == null) {
      return;
    }

    SparkContext sparkContext = sparkSession.sparkContext();
    if (!sparkContext.isStopped()) {
      sparkContext.cancelJobGroup(jobGroupId);
    }
  }

  @Override
  public void close() throws IOException {
    // We don't close the SparkSession to help with reusability across multiple Spark jobs, and it will eventually be
    // closed by SparkContext's ShutdownHook
  }

  private void logAccumulatorValues() {
    LOGGER.info("Accumulator values for data writer job:");
    logAccumulatorValue(accumulatorsForDataWriterJob.outputRecordCounter);
    logAccumulatorValue(accumulatorsForDataWriterJob.emptyRecordCounter);
    logAccumulatorValue(accumulatorsForDataWriterJob.totalKeySizeCounter);
    logAccumulatorValue(accumulatorsForDataWriterJob.uncompressedValueSizeCounter);
    logAccumulatorValue(accumulatorsForDataWriterJob.compressedValueSizeCounter);
    logAccumulatorValue(accumulatorsForDataWriterJob.gzipCompressedValueSizeCounter);
    logAccumulatorValue(accumulatorsForDataWriterJob.zstdCompressedValueSizeCounter);
    logAccumulatorValue(accumulatorsForDataWriterJob.sprayAllPartitionsTriggeredCount);
    logAccumulatorValue(accumulatorsForDataWriterJob.partitionWriterCloseCounter);
    logAccumulatorValue(accumulatorsForDataWriterJob.repushTtlFilteredRecordCounter);
    logAccumulatorValue(accumulatorsForDataWriterJob.writeAclAuthorizationFailureCounter);
    logAccumulatorValue(accumulatorsForDataWriterJob.recordTooLargeFailureCounter);
    logAccumulatorValue(accumulatorsForDataWriterJob.duplicateKeyWithIdenticalValueCounter);
    logAccumulatorValue(accumulatorsForDataWriterJob.duplicateKeyWithDistinctValueCounter);
  }

  private void logAccumulatorValue(AccumulatorV2<?, ?> accumulator) {
    LOGGER.info("  {}: {}", accumulator.name().get(), accumulator.value());
  }

  private void validateDataFrameSchema(Dataset<Row> dataFrameForDataWriterJob) {
    StructType dataSchema = dataFrameForDataWriterJob.schema();
    if (!validateDataFrameSchema(dataSchema)) {
      String errorMessage =
          String.format("The provided input data schema is not supported. Provided schema: %s.", dataSchema);
      throw new VeniceException(errorMessage);
    }
  }

  private boolean validateDataFrameSchema(StructType dataSchema) {
    StructField[] fields = dataSchema.fields();

    if (fields.length < 2) {
      LOGGER.error("The provided input data schema does not have enough fields");
      return false;
    }

    int keyFieldIndex = SparkScalaUtils.getFieldIndex(dataSchema, KEY_COLUMN_NAME);

    if (keyFieldIndex == -1) {
      LOGGER.error("The provided input data schema does not have a {} field", KEY_COLUMN_NAME);
      return false;
    }

    if (!fields[keyFieldIndex].dataType().equals(DataTypes.BinaryType)) {
      LOGGER.error("The provided input key field's schema must be {}", DataTypes.BinaryType);
      return false;
    }

    int valueFieldIndex = SparkScalaUtils.getFieldIndex(dataSchema, VALUE_COLUMN_NAME);

    if (valueFieldIndex == -1) {
      LOGGER.error("The provided input data schema does not have a {} field", VALUE_COLUMN_NAME);
      return false;
    }

    if (!fields[valueFieldIndex].dataType().equals(DataTypes.BinaryType)) {
      LOGGER.error("The provided input value field's schema must be {}", DataTypes.BinaryType);
      return false;
    }

    for (StructField field: fields) {
      if (field.name().startsWith("_")) {
        LOGGER.error("The provided input must not have fields that start with an underscore");
        return false;
      }
    }

    return true;
  }
}
