package com.linkedin.venice.spark.datawriter.jobs;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.KAFKA_PRODUCER_REQUEST_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.KAFKA_PRODUCER_RETRIES_CONFIG;
import static com.linkedin.venice.ConfigKeys.PARTITIONER_CLASS;
import static com.linkedin.venice.ConfigKeys.PUSH_JOB_GUID_LEAST_SIGNIFICANT_BITS;
import static com.linkedin.venice.ConfigKeys.PUSH_JOB_GUID_MOST_SIGNIFICANT_BITS;
import static com.linkedin.venice.ConfigKeys.PUSH_JOB_VIEW_CONFIGS;
import static com.linkedin.venice.guid.GuidUtils.DEFAULT_GUID_GENERATOR_IMPLEMENTATION;
import static com.linkedin.venice.guid.GuidUtils.GUID_GENERATOR_IMPLEMENTATION;
import static com.linkedin.venice.spark.SparkConstants.CHUNKED_KEY_SUFFIX_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.DEFAULT_SCHEMA;
import static com.linkedin.venice.spark.SparkConstants.DEFAULT_SCHEMA_WITH_PARTITION;
import static com.linkedin.venice.spark.SparkConstants.DEFAULT_SCHEMA_WITH_SCHEMA_ID;
import static com.linkedin.venice.spark.SparkConstants.DEFAULT_SPARK_CLUSTER;
import static com.linkedin.venice.spark.SparkConstants.KEY_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.MESSAGE_TYPE;
import static com.linkedin.venice.spark.SparkConstants.MESSAGE_TYPE_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.OFFSET;
import static com.linkedin.venice.spark.SparkConstants.OFFSET_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.PARTITION_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.RAW_PUBSUB_INPUT_TABLE_SCHEMA;
import static com.linkedin.venice.spark.SparkConstants.REPLICATION_METADATA_PAYLOAD;
import static com.linkedin.venice.spark.SparkConstants.RMD_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.RMD_VERSION_ID_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.SCHEMA_ID_COLUMN_NAME;
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
import static com.linkedin.venice.vpj.VenicePushJobConstants.PARTITION_COUNT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_ENABLE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_POLICY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_START_TIMESTAMP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.RMD_SCHEMA_DIR;
import static com.linkedin.venice.vpj.VenicePushJobConstants.RMD_SCHEMA_ID_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.RMD_SCHEMA_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_CONFIGURATOR_CLASS_CONFIG;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_PASSWORD_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_KEY_STORE_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_TRUST_STORE_PROPERTY_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.STORAGE_QUOTA_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TELEMETRY_MESSAGE_INTERVAL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TOPIC_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_SCHEMA_DIR;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_SCHEMA_ID_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ZSTD_COMPRESSION_LEVEL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ZSTD_DICTIONARY_CREATION_REQUIRED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ZSTD_DICTIONARY_CREATION_SUCCESS;

import com.github.luben.zstd.Zstd;
import com.google.common.collect.Iterators;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.PushJobSetting;
import com.linkedin.venice.hadoop.exceptions.VeniceInvalidInputException;
import com.linkedin.venice.hadoop.input.kafka.ttl.TTLResolutionPolicy;
import com.linkedin.venice.hadoop.ssl.TempFileSSLConfigurator;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import com.linkedin.venice.jobs.DataWriterComputeJob;
import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.spark.chunk.SparkChunkAssembler;
import com.linkedin.venice.spark.datawriter.compression.SparkCompressionReEncoder;
import com.linkedin.venice.spark.datawriter.partition.PartitionSorter;
import com.linkedin.venice.spark.datawriter.partition.VeniceSparkPartitioner;
import com.linkedin.venice.spark.datawriter.recordprocessor.SparkInputRecordProcessorFactory;
import com.linkedin.venice.spark.datawriter.recordprocessor.SparkLogicalTimestampProcessor;
import com.linkedin.venice.spark.datawriter.task.DataWriterAccumulators;
import com.linkedin.venice.spark.datawriter.task.SparkDataWriterTaskTracker;
import com.linkedin.venice.spark.datawriter.writer.SparkPartitionWriterFactory;
import com.linkedin.venice.spark.input.kafka.ttl.SparkKafkaInputTTLFilter;
import com.linkedin.venice.spark.utils.RmdPushUtils;
import com.linkedin.venice.spark.utils.SparkPartitionUtils;
import com.linkedin.venice.spark.utils.SparkScalaUtils;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;


/**
 * The implementation of {@link DataWriterComputeJob} for Spark engine.
 */
public abstract class AbstractDataWriterSparkJob extends DataWriterComputeJob {
  private static final Logger LOGGER = LogManager.getLogger(AbstractDataWriterSparkJob.class);

  private VeniceProperties props;
  private PushJobSetting pushJobSetting;
  private String jobGroupId;
  private SparkSession sparkSession;
  private DataWriterAccumulators accumulatorsForDataWriterJob;
  private SparkDataWriterTaskTracker taskTracker;

  @Override
  public void configure(VeniceProperties props, PushJobSetting pushJobSetting) {
    this.props = props;
    this.pushJobSetting = pushJobSetting;
    setupDefaultSparkSessionForDataWriterJob(pushJobSetting, props);

    Properties jobProps = new Properties();
    sparkSession.conf().getAll().foreach(entry -> jobProps.setProperty(entry._1, entry._2));
    accumulatorsForDataWriterJob = new DataWriterAccumulators(sparkSession);
    taskTracker = new SparkDataWriterTaskTracker(accumulatorsForDataWriterJob);
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
      pushJobSetting.partitionerParams.forEach(jobConf::set);
    }
    jobConf.set(PARTITION_COUNT, pushJobSetting.partitionCount);
    if (pushJobSetting.sslToKafka) {
      jobConf.set(ConfigKeys.PUBSUB_SECURITY_PROTOCOL, PubSubSecurityProtocol.SSL.name());
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
      jobConf.set(RMD_SCHEMA_ID_PROP, pushJobSetting.rmdSchemaId);
    }
    jobConf.set(ENABLE_WRITE_COMPUTE, pushJobSetting.enableWriteCompute);
    if (pushJobSetting.replicationMetadataSchemaString != null) {
      jobConf.set(RMD_SCHEMA_PROP, pushJobSetting.replicationMetadataSchemaString);
    }

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

    if (pushJobSetting.isSortedIngestionEnabled) {
      // We generate a random UUID once, and the tasks of the compute job can use this to build the same producerGUID
      // deterministically.
      UUID producerGuid = UUID.randomUUID();
      jobConf.set(PUSH_JOB_GUID_MOST_SIGNIFICANT_BITS, producerGuid.getMostSignificantBits());
      jobConf.set(PUSH_JOB_GUID_LEAST_SIGNIFICANT_BITS, producerGuid.getLeastSignificantBits());
    } else {
      // Use unique GUID for every speculative producers when the config `isSortedIngestionEnabled` is false
      // This prevents log compaction of control message which triggers the following during rebalance or store
      // migration
      // UNREGISTERED_PRODUCER data detected for producer
      jobConf.set(GUID_GENERATOR_IMPLEMENTATION, DEFAULT_GUID_GENERATOR_IMPLEMENTATION);
    }

    if (pushJobSetting.materializedViewConfigFlatMap != null) {
      jobConf.set(PUSH_JOB_VIEW_CONFIGS, pushJobSetting.materializedViewConfigFlatMap);
      jobConf.set(VALUE_SCHEMA_DIR, pushJobSetting.valueSchemaDir);
      jobConf.set(RMD_SCHEMA_DIR, pushJobSetting.rmdSchemaDir);
    }

    DataWriterComputeJob.populateWithPassThroughConfigs(
        props,
        jobConf::set,
        PASS_THROUGH_CONFIG_PREFIXES,
        SPARK_DATA_WRITER_CONF_PREFIX);
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
   * @see {@link #validateDataFrame(Dataset)}
   *
   * @return The data frame based on the user's input data
   */
  protected abstract Dataset<Row> getUserInputDataFrame();

  /*
   * Validates the presence of RMD field and then ensures the input schema is valid for the rest of the pipeline to run.
   * In case of input RMD being logical timestamp, we adapt the timestamp to RMD in # getUserInputDataFrame().
   * Otherwise, check for subset check with the RMD schema associated with the store for the given input value schema.
   */
  protected void validateRmdSchema(PushJobSetting pushJobSetting) {
    if (RmdPushUtils.rmdFieldPresent(pushJobSetting)) {
      Schema inputRmdSchema = RmdPushUtils.getInputRmdSchema(pushJobSetting);
      if ((!RmdPushUtils.containsLogicalTimestamp(pushJobSetting) && !AvroSchemaUtils.compareSchema(
          inputRmdSchema,
          AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(pushJobSetting.replicationMetadataSchemaString)))) {
        throw new VeniceException(
            "Input rmd schema does not match the server side RMD schema. Input rmd schema: " + inputRmdSchema
                + " , server side schema: " + pushJobSetting.replicationMetadataSchemaString);
      }
    }
  }

  private Dataset<Row> getInputDataFrame() {
    if (pushJobSetting.isSourceKafka) {
      Dataset<Row> rawKafkaInput = getKafkaInputDataFrame();

      // Apply TTL filter first on RAW_PUBSUB_INPUT_TABLE_SCHEMA (if enabled)
      Dataset<Row> filteredInput = applyTTLFilter(rawKafkaInput);

      // Only apply explicit compaction if chunking is DISABLED.
      // If chunking is enabled, applyChunkAssembly will handle both assembly and deduplication.
      Dataset<Row> processedInput;
      if (!pushJobSetting.sourceKafkaInputVersionInfo.isChunkingEnabled()) {
        LOGGER.info("Applying compaction to non-chunked Kafka input.");
        processedInput = applyCompaction(filteredInput);
      } else {
        LOGGER.info("Skipping explicit compaction as chunking is enabled. Deduplication will happen during assembly.");
        processedInput = filteredInput;
      }

      // If chunking is enabled, keep offset, message_type, and chunked_key_suffix for chunk assembly
      // Otherwise, just select the basic columns
      if (pushJobSetting.sourceKafkaInputVersionInfo.isChunkingEnabled()) {
        LOGGER.info("Chunking is enabled - selecting columns for chunk assembly");
        return processedInput.selectExpr(
            KEY_COLUMN_NAME,
            VALUE_COLUMN_NAME,
            "CAST(" + REPLICATION_METADATA_PAYLOAD + " AS BINARY) as " + RMD_COLUMN_NAME,
            SCHEMA_ID_COLUMN_NAME + " as " + SCHEMA_ID_COLUMN_NAME,
            RMD_VERSION_ID_COLUMN_NAME + " as " + RMD_VERSION_ID_COLUMN_NAME,
            OFFSET + " as " + OFFSET_COLUMN_NAME,
            MESSAGE_TYPE + " as " + MESSAGE_TYPE_COLUMN_NAME,
            CHUNKED_KEY_SUFFIX_COLUMN_NAME + " as " + CHUNKED_KEY_SUFFIX_COLUMN_NAME);
      } else {
        // Non-chunked: don't need offset/message_type or schema IDs
        return processedInput.selectExpr(
            KEY_COLUMN_NAME,
            VALUE_COLUMN_NAME,
            "CAST(" + REPLICATION_METADATA_PAYLOAD + " AS BINARY) as " + RMD_COLUMN_NAME);
      }
    } else {
      return getUserInputDataFrame();
    }
  }

  /**
   * Get the input DataFrame from Kafka/PubSub source for repush workloads.
   * This method reads from a Venice version topic (Kafka) and returns a DataFrame
   * with the RAW_PUBSUB_INPUT_TABLE_SCHEMA.
   *
   * @return DataFrame containing the Kafka input data
   */
  protected abstract Dataset<Row> getKafkaInputDataFrame();

  /**
   * Apply TTL filtering to the Kafka input dataframe.
   * This method filters out records that are older than the configured TTL threshold.
   * The input dataframe must have the RAW_PUBSUB_INPUT_TABLE_SCHEMA
   * (region, partition, offset, message_type, schema_id, key, value, rmd_version_id, rmd_payload).
   *
   * @param dataFrame Input dataframe with RAW_PUBSUB_INPUT_TABLE_SCHEMA
   * @return Filtered dataframe (with stale records removed if TTL filtering is enabled)
   */
  protected Dataset<Row> applyTTLFilter(Dataset<Row> dataFrame) {
    if (!pushJobSetting.repushTTLEnabled) {
      LOGGER.info("TTL filtering is not enabled for this repush job");
      return dataFrame;
    }

    boolean isChunkingEnabled = pushJobSetting.sourceKafkaInputVersionInfo.isChunkingEnabled();
    LOGGER.info(
        "Applying TTL filtering with start timestamp: {} (chunking enabled: {})",
        pushJobSetting.repushTTLStartTimeMs,
        isChunkingEnabled);

    try (JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext())) {
      // Create properties for TTL filter
      Properties filterProps = new Properties();
      this.sparkSession.conf().getAll().foreach(entry -> filterProps.setProperty(entry._1, entry._2));

      // Broadcast the filter configuration
      Broadcast<Properties> broadcastFilterProps = sparkContext.broadcast(filterProps);

      // Get schema for the encoder
      StructType schema = dataFrame.schema();
      ExpressionEncoder<Row> encoder = RowEncoder.apply(schema);

      final LongAccumulator ttlFilteredAcc = accumulatorsForDataWriterJob.repushTtlFilteredRecordCounter;

      // Apply filter using mapPartitions for efficiency (one filter instance per partition)
      dataFrame = dataFrame.mapPartitions((MapPartitionsFunction<Row, Row>) iterator -> {
        SparkKafkaInputTTLFilter ttlFilter =
            new SparkKafkaInputTTLFilter(new VeniceProperties(broadcastFilterProps.value()));
        try {
          // Filter rows in this partition
          return Iterators.filter(iterator, row -> {
            int schemaId = row.getInt(4); // schema_id is at index 4 in RAW_PUBSUB_INPUT_TABLE_SCHEMA

            // If chunking enabled, skip chunk/manifest records - they will be assembled first
            if (isChunkingEnabled && (schemaId == AvroProtocolDefinition.CHUNK.getCurrentProtocolVersion()
                || schemaId == AvroProtocolDefinition.CHUNKED_VALUE_MANIFEST.getCurrentProtocolVersion())) {
              return true; // Keep chunk/manifest records for assembly
            }

            // shouldFilter returns true if record should be removed
            // We negate to keep records that should NOT be filtered
            boolean shouldRemove = ttlFilter.shouldFilter(row);

            if (shouldRemove) {
              // Increment counter for filtered records
              ttlFilteredAcc.add(1);
            }

            return !shouldRemove; // Keep if NOT filtered
          });
        } catch (Exception e) {
          LOGGER.error("Error during TTL filtering", e);
          throw new VeniceException("TTL filtering failed", e);
        } finally {
          ttlFilter.close();
        }
      }, encoder);

      LOGGER.info("TTL filtering applied successfully");
      return dataFrame;

    } catch (Exception e) {
      LOGGER.error("Failed to apply TTL filter", e);
      throw new VeniceException("Failed to apply TTL filter", e);
    }
  }

  /**
   * Apply compaction to the Kafka input dataframe.
   * For each key, keep only the record with the highest offset.
   *
   * @param dataFrame Input dataframe with RAW_PUBSUB_INPUT_TABLE_SCHEMA
   * @return Compacted dataframe with duplicate keys removed
   */
  protected Dataset<Row> applyCompaction(Dataset<Row> dataFrame) {
    if (!pushJobSetting.isSourceKafka) {
      // Compaction only applies to Kafka input (repush)
      return dataFrame;
    }

    LOGGER.info("Applying compaction to Kafka input. Input schema: {}", dataFrame.schema());

    ExpressionEncoder<Row> encoder = RowEncoder.apply(RAW_PUBSUB_INPUT_TABLE_SCHEMA);

    // Extract accumulators to local variables to avoid serialization issues
    final LongAccumulator totalDupKeyAcc = accumulatorsForDataWriterJob.totalDuplicateKeyCounter;
    final LongAccumulator dupKeyDistinctValueAcc = accumulatorsForDataWriterJob.duplicateKeyWithDistinctValueCounter;
    final LongAccumulator dupKeyIdenticalValueAcc = accumulatorsForDataWriterJob.duplicateKeyWithIdenticalValueCounter;

    dataFrame = dataFrame
        // Group by key
        .groupByKey((MapFunction<Row, byte[]>) row -> row.getAs(KEY_COLUMN_NAME), Encoders.BINARY())
        // For each key group, keep only the latest record (highest offset)
        .flatMapGroups((FlatMapGroupsFunction<byte[], Row, Row>) (keyBytes, rowsIterator) -> {
          List<Row> rowsList = new ArrayList<>();
          rowsIterator.forEachRemaining(rowsList::add);

          if (rowsList.isEmpty()) {
            return Collections.emptyIterator();
          }

          // Track duplicate keys
          if (rowsList.size() > 1) {
            totalDupKeyAcc.add(1);

            // Check if values are identical or distinct
            boolean hasDistinctValues = false;
            byte[] firstValue = rowsList.get(0).getAs(VALUE_COLUMN_NAME);
            for (int i = 1; i < rowsList.size(); i++) {
              byte[] currentValue = rowsList.get(i).getAs(VALUE_COLUMN_NAME);
              if (!java.util.Arrays.equals(firstValue, currentValue)) {
                hasDistinctValues = true;
                break;
              }
            }

            if (hasDistinctValues) {
              dupKeyDistinctValueAcc.add(1);
            } else {
              dupKeyIdenticalValueAcc.add(1);
            }
          }

          // Sort by offset DESC and keep the first (latest) record
          Row latestRecord =
              rowsList.stream().max(Comparator.comparingLong(r -> (long) r.getAs(OFFSET_COLUMN_NAME))).orElse(null);

          if (latestRecord == null) {
            return Collections.emptyIterator();
          }

          return Collections.singletonList(latestRecord).iterator();
        }, encoder);

    LOGGER.info("Compaction completed. Output schema: {}", dataFrame.schema());
    return dataFrame;
  }

  /**
   * Apply chunk assembly if chunking is enabled.
   * Groups records by key, sorts by offset DESC, and assembles chunks into complete values/RMDs.
   * If TTL filtering is enabled, the assembler also filters assembled records post-assembly.
   *
   * @param dataFrame Input with SCHEMA_FOR_CHUNK_ASSEMBLY (7 columns)
   * @return DataFrame with DEFAULT_SCHEMA_WITH_SCHEMA_ID (5 columns - assembled records)
   */
  protected Dataset<Row> applyChunkAssembly(Dataset<Row> dataFrame) {
    boolean isRmdChunkingEnabled = pushJobSetting.sourceKafkaInputVersionInfo.isRmdChunkingEnabled();
    boolean isTTLEnabled = pushJobSetting.repushTTLEnabled;

    LOGGER.info("Chunk assembly starting (TTL filtering: {}). Input schema: {}", isTTLEnabled, dataFrame.schema());

    // Prepare TTL filter properties if enabled
    VeniceProperties filterProps = null;
    if (isTTLEnabled) {
      Properties props = new Properties();
      this.sparkSession.conf().getAll().foreach(entry -> props.setProperty(entry._1, entry._2));
      filterProps = new VeniceProperties(props);
    }
    final VeniceProperties broadcastFilterProps = filterProps;

    ExpressionEncoder<Row> encoder = RowEncoder.apply(DEFAULT_SCHEMA_WITH_SCHEMA_ID);

    final LongAccumulator emptyRecordAcc = accumulatorsForDataWriterJob.emptyRecordCounter;

    dataFrame = dataFrame
        // Group by key
        .groupByKey((MapFunction<Row, byte[]>) row -> row.getAs(KEY_COLUMN_NAME), Encoders.BINARY())
        // For each key group, sort by offset DESC and assemble
        .flatMapGroups((FlatMapGroupsFunction<byte[], Row, Row>) (keyBytes, rowsIterator) -> {
          // Collect rows and sort by offset DESC (highest first)
          List<Row> rowsList = new ArrayList<>();
          rowsIterator.forEachRemaining(rowsList::add);

          if (rowsList.isEmpty()) {
            return Collections.emptyIterator();
          }

          // Sort by offset DESC
          rowsList.sort((r1, r2) -> {
            long offset1 = r1.getAs(OFFSET_COLUMN_NAME);
            long offset2 = r2.getAs(OFFSET_COLUMN_NAME);
            return Long.compare(offset2, offset1);
          });

          // Assemble chunks (and apply TTL filtering if enabled)
          SparkChunkAssembler assembler =
              new SparkChunkAssembler(isRmdChunkingEnabled, isTTLEnabled, broadcastFilterProps);
          Row assembled = assembler.assembleChunks(keyBytes, rowsList.iterator());

          if (assembled == null) {
            // Latest record is DELETE, chunks incomplete, or filtered by TTL
            emptyRecordAcc.add(1);
            return Collections.emptyIterator();
          }

          return Collections.singletonList(assembled).iterator();
        }, encoder);

    LOGGER.info("Chunk assembly completed. Output schema: {}", dataFrame.schema());
    return dataFrame;
  }

  /**
   * Apply compression re-encoding if the source and destination compression strategies are different.
   * This is only applicable for repush workloads (isSourceKafka = true).
   *
   * @param dataFrame Input dataframe
   * @return Dataframe with values re-compressed if needed
   */
  protected Dataset<Row> applyCompressionReEncoding(Dataset<Row> dataFrame) {
    if (!pushJobSetting.isSourceKafka) {
      return dataFrame;
    }

    CompressionStrategy sourceStrategy = pushJobSetting.sourceVersionCompressionStrategy;
    CompressionStrategy destStrategy = pushJobSetting.topicCompressionStrategy;
    byte[] sourceDict = pushJobSetting.sourceDictionary;
    byte[] destDict = pushJobSetting.topicDictionary;
    boolean metricEnabled = pushJobSetting.compressionMetricCollectionEnabled;
    DataWriterAccumulators accumulators = accumulatorsForDataWriterJob;

    // Optimization: if strategies and dictionaries are the same and metrics are disabled, skip the map stage
    if (sourceStrategy == destStrategy && java.util.Arrays.equals(sourceDict, destDict)) {
      LOGGER.info("Source and destination compression are identical ({}). Skipping re-encoding stage.", sourceStrategy);
      return dataFrame;
    }

    LOGGER.info(
        "Applying compression handling: {} -> {} (metrics enabled: {})",
        sourceStrategy,
        destStrategy,
        metricEnabled);
    ExpressionEncoder<Row> encoder = RowEncoder.apply(dataFrame.schema());
    int valueIdx = dataFrame.schema().fieldIndex(VALUE_COLUMN_NAME);
    StructType schema = dataFrame.schema();

    return dataFrame.mapPartitions((MapPartitionsFunction<Row, Row>) iterator -> {
      SparkCompressionReEncoder reencoder = new SparkCompressionReEncoder(
          sourceStrategy,
          destStrategy,
          sourceDict,
          destDict,
          schema,
          valueIdx,
          metricEnabled,
          accumulators);
      return Iterators.transform(iterator, row -> {
        try {
          return reencoder.reEncode(row);
        } catch (IOException e) {
          throw new VeniceException("Failed to re-encode compression", e);
        }
      });
    }, encoder);
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

  @VisibleForTesting
  protected DataWriterAccumulators getAccumulatorsForDataWriterJob() {
    return accumulatorsForDataWriterJob;
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
  public void runComputeJob() {
    // Load data from input path
    Dataset<Row> dataFrame = getInputDataFrame();
    validateDataFrame(dataFrame);
    validateRmdSchema(pushJobSetting);

    ExpressionEncoder<Row> rowEncoder = RowEncoder.apply(DEFAULT_SCHEMA);
    ExpressionEncoder<Row> rowEncoderWithPartition = RowEncoder.apply(DEFAULT_SCHEMA_WITH_PARTITION);
    int numOutputPartitions = pushJobSetting.partitionCount;

    Properties jobProps = new Properties();
    this.sparkSession.conf().getAll().foreach(entry -> jobProps.setProperty(entry._1, entry._2));
    JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
    Broadcast<Properties> broadcastProperties = sparkContext.broadcast(jobProps);

    LOGGER.info("Triggering Spark job for data writer");
    try {
      if (pushJobSetting.isSourceKafka) {
        // Apply chunk assembly if chunking is enabled
        if (pushJobSetting.sourceKafkaInputVersionInfo.isChunkingEnabled()) {
          LOGGER.info(
              "Applying chunk assembly (RMD chunking: {})",
              pushJobSetting.sourceKafkaInputVersionInfo.isRmdChunkingEnabled());
          dataFrame = applyChunkAssembly(dataFrame);
        }

        // Apply compression re-encoding if needed (source vs destination compression)
        dataFrame = applyCompressionReEncoding(dataFrame);

        // Drop schema ID columns (current behavior - no writer changes)
        LOGGER.info("Dropping schema ID columns before writing");
        dataFrame = dataFrame.drop(SCHEMA_ID_COLUMN_NAME, RMD_VERSION_ID_COLUMN_NAME);
      } else {
        // For HDFS input, convert all rows to byte[], byte[] pairs (compressed if compression is enabled)
        dataFrame = dataFrame
            .map(
                new SparkLogicalTimestampProcessor(
                    RmdPushUtils.containsLogicalTimestamp(pushJobSetting),
                    pushJobSetting.replicationMetadataSchemaString),
                rowEncoder)
            .flatMap(
                new SparkInputRecordProcessorFactory(broadcastProperties, accumulatorsForDataWriterJob),
                rowEncoder);
      }

      // TODO: Add map-side combiner to reduce the data size before shuffling

      // Partition the data using the custom partitioner and sort the data within that partition
      dataFrame = SparkPartitionUtils.repartitionAndSortWithinPartitions(
          dataFrame,
          new VeniceSparkPartitioner(broadcastProperties, numOutputPartitions),
          new PartitionSorter());

      // Add a partition column to all rows based on the custom partitioner
      dataFrame = dataFrame.withColumn(PARTITION_COLUMN_NAME, functions.spark_partition_id());

      // Write the data to PubSub
      dataFrame = dataFrame.mapPartitions(
          createPartitionWriterFactory(broadcastProperties, accumulatorsForDataWriterJob),
          rowEncoderWithPartition);

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

  /**
   * Creates the partition writer factory. Can be overridden for testing purposes.
   * @param broadcastProperties the broadcast job properties
   * @param accumulators the data writer accumulators
   * @return the partition writer factory
   */
  @VisibleForTesting
  protected MapPartitionsFunction<Row, Row> createPartitionWriterFactory(
      Broadcast<Properties> broadcastProperties,
      DataWriterAccumulators accumulators) {
    return new SparkPartitionWriterFactory(broadcastProperties, accumulators);
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
    logAccumulatorValue(accumulatorsForDataWriterJob.totalDuplicateKeyCounter);
    logAccumulatorValue(accumulatorsForDataWriterJob.writeAclAuthorizationFailureCounter);
    logAccumulatorValue(accumulatorsForDataWriterJob.recordTooLargeFailureCounter);
    logAccumulatorValue(accumulatorsForDataWriterJob.duplicateKeyWithIdenticalValueCounter);
    logAccumulatorValue(accumulatorsForDataWriterJob.duplicateKeyWithDistinctValueCounter);
    logAccumulatorValue(accumulatorsForDataWriterJob.largestUncompressedValueSize);
  }

  private void logAccumulatorValue(AccumulatorV2<?, ?> accumulator) {
    LOGGER.info("  {}: {}", accumulator.name().get(), accumulator.value());
  }

  @VisibleForTesting
  void validateDataFrame(Dataset<Row> dataFrameForDataWriterJob) {
    if (dataFrameForDataWriterJob == null) {
      throw new VeniceInvalidInputException("The input data frame cannot be null");
    }

    StructType dataSchema = dataFrameForDataWriterJob.schema();
    StructField[] fields = dataSchema.fields();

    if (fields.length < 3) {
      String errorMessage =
          String.format("The provided input data does not have enough fields. Provided schema: %s.", dataSchema);
      throw new VeniceInvalidInputException(errorMessage);
    }

    validateDataFrameFieldAndTypes(fields, dataSchema, KEY_COLUMN_NAME, DataTypes.BinaryType);
    validateDataFrameFieldAndTypes(fields, dataSchema, VALUE_COLUMN_NAME, DataTypes.BinaryType);

    validateDataFrameFieldAndTypes(fields, dataSchema, RMD_COLUMN_NAME, DataTypes.BinaryType);

    // For KIF repush, the DataFrame may contain Venice internal columns (defined in SparkConstants)
    // needed for chunk assembly. These are consumed by applyChunkAssembly() and dropped in runComputeJob().
    Set<String> allowedInternalColumns = new HashSet<>();
    PushJobSetting setting = getPushJobSetting();
    if (setting != null && setting.isSourceKafka) {
      allowedInternalColumns.addAll(
          Arrays.asList(
              SCHEMA_ID_COLUMN_NAME,
              RMD_VERSION_ID_COLUMN_NAME,
              OFFSET_COLUMN_NAME,
              MESSAGE_TYPE_COLUMN_NAME,
              CHUNKED_KEY_SUFFIX_COLUMN_NAME));
    }

    for (StructField field: fields) {
      if (field.name().startsWith("_") && !allowedInternalColumns.contains(field.name())) {
        String errorMessage = String
            .format("The provided input must not have fields that start with an underscore. Got: %s", field.name());
        throw new VeniceInvalidInputException(errorMessage);
      }
    }
  }

  @VisibleForTesting
  void validateDataFrameFieldAndTypes(
      StructField[] fields,
      StructType dataSchema,
      String fieldName,
      DataType allowedType) {
    int fieldIndex = SparkScalaUtils.getFieldIndex(dataSchema, fieldName);

    if (fieldIndex == -1) {
      String errorMessage = String.format(
          "The provided input data frame does not have a %s field. Provided schema: %s.",
          fieldName,
          dataSchema);
      throw new VeniceInvalidInputException(errorMessage);
    }

    StructField field = fields[fieldIndex];
    DataType fieldType = field.dataType();

    if (!allowedType.equals(fieldType)) {
      String errorMessage =
          String.format("The provided input %s schema must be %s. Got: %s.", fieldName, allowedType, fieldType);
      throw new VeniceInvalidInputException(errorMessage);
    }
  }
}
