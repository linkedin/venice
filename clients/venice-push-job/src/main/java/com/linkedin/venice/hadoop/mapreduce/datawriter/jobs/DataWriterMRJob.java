package com.linkedin.venice.hadoop.mapreduce.datawriter.jobs;

import static com.linkedin.venice.CommonConfigKeys.SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.KAFKA_PRODUCER_REQUEST_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.KAFKA_PRODUCER_RETRIES_CONFIG;
import static com.linkedin.venice.ConfigKeys.PARTITIONER_CLASS;
import static com.linkedin.venice.ConfigKeys.PUSH_JOB_GUID_LEAST_SIGNIFICANT_BITS;
import static com.linkedin.venice.ConfigKeys.PUSH_JOB_GUID_MOST_SIGNIFICANT_BITS;
import static com.linkedin.venice.VeniceConstants.DEFAULT_SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ALLOW_DUPLICATE_KEY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.BATCH_NUM_BYTES_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.COMPRESSION_STRATEGY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DERIVED_SCHEMA_ID_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ENABLE_WRITE_COMPUTE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ETL_VALUE_SCHEMA_TRANSFORMATION;
import static com.linkedin.venice.vpj.VenicePushJobConstants.EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.FILE_KEY_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.FILE_VALUE_SCHEMA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.GENERATE_PARTIAL_UPDATE_RECORD_FROM_INPUT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_SOURCE_COMPRESSION_STRATEGY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_SOURCE_TOPIC_CHUNKING_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_SECURITY_PROTOCOL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.MAP_REDUCE_PARTITIONER_CLASS_CONFIG;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PARTITION_COUNT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REDUCER_SPECULATIVE_EXECUTION_ENABLE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_ENABLE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_POLICY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_START_TIMESTAMP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.RMD_SCHEMA_DIR;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SCHEMA_STRING_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.STORAGE_QUOTA_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SYSTEM_SCHEMA_CLUSTER_D2_SERVICE_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SYSTEM_SCHEMA_CLUSTER_D2_ZK_HOST;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SYSTEM_SCHEMA_READER_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TELEMETRY_MESSAGE_INTERVAL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TOPIC_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.UPDATE_SCHEMA_STRING_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_SCHEMA_DIR;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_SCHEMA_ID_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VSON_PUSH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ZSTD_COMPRESSION_LEVEL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ZSTD_DICTIONARY_CREATION_SUCCESS;

import com.github.luben.zstd.Zstd;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.JobClientWrapper;
import com.linkedin.venice.hadoop.PushJobSetting;
import com.linkedin.venice.hadoop.VsonSequenceFileInputFormat;
import com.linkedin.venice.hadoop.input.kafka.KafkaInputFormat;
import com.linkedin.venice.hadoop.input.kafka.KafkaInputFormatCombiner;
import com.linkedin.venice.hadoop.input.kafka.KafkaInputKeyComparator;
import com.linkedin.venice.hadoop.input.kafka.KafkaInputMRPartitioner;
import com.linkedin.venice.hadoop.input.kafka.KafkaInputValueGroupingComparator;
import com.linkedin.venice.hadoop.input.kafka.VeniceKafkaInputMapper;
import com.linkedin.venice.hadoop.input.kafka.VeniceKafkaInputReducer;
import com.linkedin.venice.hadoop.input.kafka.ttl.TTLResolutionPolicy;
import com.linkedin.venice.hadoop.mapreduce.common.JobUtils;
import com.linkedin.venice.hadoop.mapreduce.datawriter.map.VeniceAvroMapper;
import com.linkedin.venice.hadoop.mapreduce.datawriter.map.VeniceVsonMapper;
import com.linkedin.venice.hadoop.mapreduce.datawriter.partition.VeniceMRPartitioner;
import com.linkedin.venice.hadoop.mapreduce.datawriter.reduce.VeniceReducer;
import com.linkedin.venice.hadoop.mapreduce.datawriter.task.CounterBackedMapReduceDataWriterTaskTracker;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import com.linkedin.venice.jobs.DataWriterComputeJob;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.IOException;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The implementation of {@link DataWriterComputeJob} for MapReduce engine.
 */
public class DataWriterMRJob extends DataWriterComputeJob {
  private static final Logger LOGGER = LogManager.getLogger(DataWriterMRJob.class);
  public static final String HADOOP_PREFIX = "hadoop-conf.";

  private VeniceProperties vpjProperties;
  private PushJobSetting pushJobSetting;
  private JobConf jobConf;

  private RunningJob runningJob;
  private CounterBackedMapReduceDataWriterTaskTracker taskTracker;
  private JobClientWrapper jobClientWrapper;

  @Override
  public void configure(VeniceProperties props, PushJobSetting pushJobSetting) {
    this.jobConf = new JobConf();
    this.vpjProperties = props;
    this.pushJobSetting = pushJobSetting;
    setupMRConf(jobConf, pushJobSetting, props);
  }

  void setupMRConf(JobConf jobConf, PushJobSetting pushJobSetting, VeniceProperties props) {
    setupDefaultJobConf(jobConf, pushJobSetting, props);
    setupInputFormatConf(jobConf, pushJobSetting);
    setupReducerConf(jobConf, pushJobSetting);
  }

  private void setupDefaultJobConf(JobConf conf, PushJobSetting pushJobSetting, VeniceProperties props) {
    JobUtils.setupCommonJobConf(
        props,
        conf,
        pushJobSetting.jobId + ":venice_push_job-" + pushJobSetting.topic,
        pushJobSetting);
    conf.set(BATCH_NUM_BYTES_PROP, Integer.toString(pushJobSetting.batchNumBytes));
    conf.set(TOPIC_PROP, pushJobSetting.topic);
    conf.set(KAFKA_BOOTSTRAP_SERVERS, pushJobSetting.kafkaUrl);
    conf.set(PARTITIONER_CLASS, pushJobSetting.partitionerClass);
    // flatten partitionerParams since JobConf class does not support set an object
    if (pushJobSetting.partitionerParams != null) {
      pushJobSetting.partitionerParams.forEach(conf::set);
    }
    if (pushJobSetting.sslToKafka) {
      conf.set(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, KAFKA_SECURITY_PROTOCOL);
      props.keySet().stream().filter(key -> key.toLowerCase().startsWith(SSL_PREFIX)).forEach(key -> {
        conf.set(key, props.getString(key));
      });
    }
    conf.setBoolean(ALLOW_DUPLICATE_KEY, pushJobSetting.isDuplicateKeyAllowed);
    conf.setBoolean(VeniceWriter.ENABLE_CHUNKING, pushJobSetting.chunkingEnabled);
    conf.setBoolean(VeniceWriter.ENABLE_RMD_CHUNKING, pushJobSetting.rmdChunkingEnabled);
    conf.setInt(VeniceWriter.MAX_RECORD_SIZE_BYTES, pushJobSetting.maxRecordSizeBytes);

    conf.set(STORAGE_QUOTA_PROP, Long.toString(pushJobSetting.storeStorageQuota));

    if (pushJobSetting.isSourceKafka) {
      // Use some fake value schema id here since it won't be used
      conf.setInt(VALUE_SCHEMA_ID_PROP, -1);
      /**
       * Kafka input topic could be inferred from the store name, but absent from the original properties.
       * So here will set it up from {@link #pushJobSetting}.
       */
      conf.set(KAFKA_INPUT_TOPIC, pushJobSetting.kafkaInputTopic);
      conf.set(KAFKA_INPUT_BROKER_URL, pushJobSetting.kafkaInputBrokerUrl);
      conf.setBoolean(REPUSH_TTL_ENABLE, pushJobSetting.repushTTLEnabled);
      conf.setLong(REPUSH_TTL_START_TIMESTAMP, pushJobSetting.repushTTLStartTimeMs);
      if (pushJobSetting.repushTTLEnabled) {
        // Currently, we only support one policy. Thus, we don't allow overriding it.
        conf.setInt(REPUSH_TTL_POLICY, TTLResolutionPolicy.RT_WRITE_ONLY.getValue());
        conf.set(RMD_SCHEMA_DIR, pushJobSetting.rmdSchemaDir);
        conf.set(VALUE_SCHEMA_DIR, pushJobSetting.valueSchemaDir);
      }
      // Pass the compression strategy of source version to repush MR job
      conf.set(KAFKA_INPUT_SOURCE_COMPRESSION_STRATEGY, pushJobSetting.sourceVersionCompressionStrategy.name());
      conf.set(
          KAFKA_INPUT_SOURCE_TOPIC_CHUNKING_ENABLED,
          Boolean.toString(pushJobSetting.sourceVersionChunkingEnabled));

      conf.setBoolean(SYSTEM_SCHEMA_READER_ENABLED, pushJobSetting.isSystemSchemaReaderEnabled);
      if (pushJobSetting.isSystemSchemaReaderEnabled) {
        conf.set(SYSTEM_SCHEMA_CLUSTER_D2_SERVICE_NAME, pushJobSetting.systemSchemaClusterD2ServiceName);
        conf.set(SYSTEM_SCHEMA_CLUSTER_D2_ZK_HOST, pushJobSetting.systemSchemaClusterD2ZKHost);
        conf.set(SSL_FACTORY_CLASS_NAME, props.getString(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME));
      }
    } else {
      conf.setInt(VALUE_SCHEMA_ID_PROP, pushJobSetting.valueSchemaId);
      conf.setInt(DERIVED_SCHEMA_ID_PROP, pushJobSetting.derivedSchemaId);
    }
    conf.setBoolean(ENABLE_WRITE_COMPUTE, pushJobSetting.enableWriteCompute);

    if (!props.containsKey(KAFKA_PRODUCER_REQUEST_TIMEOUT_MS)) {
      // If the push job plug-in doesn't specify the request timeout config, default will be infinite
      conf.set(KAFKA_PRODUCER_REQUEST_TIMEOUT_MS, Integer.toString(Integer.MAX_VALUE));
    }
    if (!props.containsKey(KAFKA_PRODUCER_RETRIES_CONFIG)) {
      // If the push job plug-in doesn't specify the retries config, default will be infinite
      conf.set(KAFKA_PRODUCER_RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    }
    if (!props.containsKey(KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS)) {
      // If the push job plug-in doesn't specify the delivery timeout config, default will be infinite
      conf.set(KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS, Integer.toString(Integer.MAX_VALUE));
    }

    conf.set(TELEMETRY_MESSAGE_INTERVAL, props.getString(TELEMETRY_MESSAGE_INTERVAL, "10000"));
    conf.setBoolean(EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED, pushJobSetting.extendedSchemaValidityCheckEnabled);

    // Compression related
    // Note that COMPRESSION_STRATEGY is from topic creation response as it might be different from the store config
    // (eg: for inc push)
    conf.set(
        COMPRESSION_STRATEGY,
        pushJobSetting.topicCompressionStrategy != null
            ? pushJobSetting.topicCompressionStrategy.toString()
            : CompressionStrategy.NO_OP.toString());
    conf.set(
        ZSTD_COMPRESSION_LEVEL,
        props.getString(ZSTD_COMPRESSION_LEVEL, String.valueOf(Zstd.maxCompressionLevel())));
    conf.setBoolean(ZSTD_DICTIONARY_CREATION_SUCCESS, pushJobSetting.isZstdDictCreationSuccess);

    // We generate a random UUID once, and the tasks of the compute job can use this to build the same producerGUID
    // deterministically.
    UUID producerGuid = UUID.randomUUID();
    conf.setLong(PUSH_JOB_GUID_MOST_SIGNIFICANT_BITS, producerGuid.getMostSignificantBits());
    conf.setLong(PUSH_JOB_GUID_LEAST_SIGNIFICANT_BITS, producerGuid.getLeastSignificantBits());

    /**
     * Override the configs following the rules:
     * <ul>
     *   <li>Pass-through the properties whose names start with the prefixes defined in {@link PASS_THROUGH_CONFIG_PREFIXES}.</li>
     *   <li>Override the properties that are specified with the {@link HADOOP_PREFIX} prefix.</li>
     * </ul>
     **/
    for (String configKey: props.keySet()) {
      String lowerCaseConfigKey = configKey.toLowerCase();
      if (lowerCaseConfigKey.startsWith(HADOOP_PREFIX)) {
        String overrideKey = configKey.substring(HADOOP_PREFIX.length());
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

  protected void setupInputFormatConf(JobConf jobConf, PushJobSetting pushJobSetting) {
    if (pushJobSetting.isSourceKafka) {
      Schema keySchemaFromController = pushJobSetting.storeKeySchema;
      String keySchemaString = AvroCompatibilityHelper.toParsingForm(keySchemaFromController);
      jobConf.set(KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP, keySchemaString);
      jobConf.setInputFormat(KafkaInputFormat.class);
      jobConf.setMapperClass(VeniceKafkaInputMapper.class);
      if (pushJobSetting.kafkaInputCombinerEnabled) {
        jobConf.setCombinerClass(KafkaInputFormatCombiner.class);
      }
    } else {
      // TODO:The job is using path-filter to check the consistency of avro file schema,
      // but doesn't specify the path filter for the input directory of map-reduce job.
      // We need to revisit it if any failure because of this happens.
      FileInputFormat.setInputPaths(jobConf, new Path(pushJobSetting.inputURI));

      /**
       * Include all the files in the input directory no matter whether the file is with '.avro' suffix or not
       * to keep the logic consistent with {@link #checkAvroSchemaConsistency(FileStatus[])}.
       */
      jobConf.set(AvroInputFormat.IGNORE_FILES_WITHOUT_EXTENSION_KEY, "false");

      jobConf.set(KEY_FIELD_PROP, pushJobSetting.keyField);
      jobConf.set(VALUE_FIELD_PROP, pushJobSetting.valueField);
      if (pushJobSetting.etlValueSchemaTransformation != null) {
        jobConf.set(ETL_VALUE_SCHEMA_TRANSFORMATION, pushJobSetting.etlValueSchemaTransformation.name());
      }

      if (pushJobSetting.isAvro) {
        jobConf.set(SCHEMA_STRING_PROP, pushJobSetting.inputDataSchemaString);
        jobConf.set(AvroJob.INPUT_SCHEMA, pushJobSetting.inputDataSchemaString);
        if (pushJobSetting.generatePartialUpdateRecordFromInput) {
          jobConf.setBoolean(GENERATE_PARTIAL_UPDATE_RECORD_FROM_INPUT, true);
          jobConf.set(UPDATE_SCHEMA_STRING_PROP, pushJobSetting.valueSchemaString);
        }
        jobConf.setClass("avro.serialization.data.model", GenericData.class, GenericData.class);
        jobConf.setInputFormat(AvroInputFormat.class);
        jobConf.setMapperClass(VeniceAvroMapper.class);
        jobConf.setBoolean(VSON_PUSH, false);
      } else {
        jobConf.setInputFormat(VsonSequenceFileInputFormat.class);
        jobConf.setMapperClass(VeniceVsonMapper.class);
        jobConf.setBoolean(VSON_PUSH, true);
        jobConf.set(FILE_KEY_SCHEMA, pushJobSetting.vsonInputKeySchemaString);
        jobConf.set(FILE_VALUE_SCHEMA, pushJobSetting.vsonInputValueSchemaString);
      }
    }
  }

  private void setupReducerConf(JobConf jobConf, PushJobSetting pushJobSetting) {
    if (pushJobSetting.isSourceKafka) {
      jobConf.setOutputKeyComparatorClass(KafkaInputKeyComparator.class);
      jobConf.setOutputValueGroupingComparator(KafkaInputValueGroupingComparator.class);
      jobConf.setCombinerKeyGroupingComparator(KafkaInputValueGroupingComparator.class);
      jobConf.setPartitionerClass(KafkaInputMRPartitioner.class);
    } else {
      final Class<? extends Partitioner> partitionerClass;
      // Test related configs
      if (vpjProperties.containsKey(MAP_REDUCE_PARTITIONER_CLASS_CONFIG)) {
        partitionerClass = ReflectUtils.loadClass(vpjProperties.getString(MAP_REDUCE_PARTITIONER_CLASS_CONFIG));
      } else {
        partitionerClass = VeniceMRPartitioner.class;
      }
      jobConf.setPartitionerClass(partitionerClass);
    }
    jobConf.setReduceSpeculativeExecution(vpjProperties.getBoolean(REDUCER_SPECULATIVE_EXECUTION_ENABLE, false));
    int partitionCount = pushJobSetting.partitionCount;
    jobConf.setInt(PARTITION_COUNT, partitionCount);
    jobConf.setNumReduceTasks(partitionCount);
    jobConf.setMapOutputKeyClass(BytesWritable.class);
    jobConf.setMapOutputValueClass(BytesWritable.class);
    if (pushJobSetting.isSourceKafka) {
      jobConf.setReducerClass(VeniceKafkaInputReducer.class);
    } else {
      jobConf.setReducerClass(VeniceReducer.class);
    }
  }

  // Visible for testing
  public void setJobClientWrapper(JobClientWrapper jobClientWrapper) {
    this.jobClientWrapper = jobClientWrapper;
  }

  @Override
  public DataWriterTaskTracker getTaskTracker() {
    if (runningJob == null) {
      return null;
    }

    try {
      taskTracker = new CounterBackedMapReduceDataWriterTaskTracker(runningJob.getCounters());
    } catch (IOException e) {
      throw new VeniceException("Unable to get Counters of the running job", e);
    }
    return taskTracker;
  }

  @Override
  protected void runComputeJob() {
    LOGGER.info("Triggering MR job for data writer");
    try {
      runningJob = JobUtils.runJobWithConfig(jobConf, jobClientWrapper);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public PushJobSetting getPushJobSetting() {
    return pushJobSetting;
  }

  @Override
  public void kill() {
    if (runningJob == null) {
      LOGGER.warn("No op to kill a null running job");
      return;
    }
    try {
      if (runningJob.isComplete()) {
        LOGGER.warn(
            "No op to kill a completed job with name {} and ID {}",
            runningJob.getJobName(),
            runningJob.getID().getId());
        return;
      }
      super.kill();
      runningJob.killJob();
    } catch (Exception ex) {
      // Will try to kill Venice Offline Push Job no matter whether map-reduce job kill throws exception or not.
      LOGGER.info(
          "Received exception while killing map-reduce job with name {} and ID {}",
          runningJob.getJobName(),
          runningJob.getID().getId(),
          ex);
    }
  }

  @Override
  public void close() throws IOException {
    if (runningJob != null && !runningJob.isComplete()) {
      runningJob.killJob();
    }
  }
}
