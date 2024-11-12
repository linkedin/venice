package com.linkedin.venice.hadoop;

import static com.linkedin.venice.CommonConfigKeys.SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.KAFKA_PRODUCER_REQUEST_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.KAFKA_PRODUCER_RETRIES_CONFIG;
import static com.linkedin.venice.ConfigKeys.MULTI_REGION;
import static com.linkedin.venice.ConfigKeys.VENICE_PARTITIONERS;
import static com.linkedin.venice.VeniceConstants.DEFAULT_SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.status.BatchJobHeartbeatConfigs.HEARTBEAT_ENABLED_CONFIG;
import static com.linkedin.venice.utils.AvroSupersetSchemaUtils.validateSubsetValueSchema;
import static com.linkedin.venice.utils.ByteUtils.generateHumanReadableByteCountString;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ALLOW_DUPLICATE_KEY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.BATCH_NUM_BYTES_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.COMPRESSION_DICTIONARY_SAMPLE_SIZE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.COMPRESSION_DICTIONARY_SIZE_LIMIT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.COMPRESSION_METRIC_COLLECTION_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.COMPRESSION_STRATEGY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.CONTROLLER_REQUEST_RETRY_ATTEMPTS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.D2_ZK_HOSTS_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DATA_WRITER_COMPUTE_JOB_CLASS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_BATCH_BYTES_SIZE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_COMPRESSION_DICTIONARY_SAMPLE_SIZE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_COMPRESSION_METRIC_COLLECTION_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_JOB_STATUS_IN_UNKNOWN_STATE_TIMEOUT_MS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_POLL_STATUS_INTERVAL_MS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_RE_PUSH_REWIND_IN_SECONDS_OVERRIDE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_SSL_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_USE_MAPPER_TO_BUILD_DICTIONARY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFER_VERSION_SWAP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ENABLE_SSL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ENABLE_WRITE_COMPUTE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.ETL_VALUE_SCHEMA_TRANSFORMATION;
import static com.linkedin.venice.vpj.VenicePushJobConstants.EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.HADOOP_TMP_DIR;
import static com.linkedin.venice.vpj.VenicePushJobConstants.HADOOP_VALIDATE_SCHEMA_AND_BUILD_DICT_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INPUT_PATH_LAST_MODIFIED_TIME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INPUT_PATH_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.JOB_EXEC_ID;
import static com.linkedin.venice.vpj.VenicePushJobConstants.JOB_EXEC_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.JOB_SERVER_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.JOB_STATUS_IN_UNKNOWN_STATE_TIMEOUT_MS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_COMBINER_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_COMPRESSION_BUILD_NEW_DICT_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_FABRIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.LEGACY_AVRO_KEY_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.LEGACY_AVRO_VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.NON_CRITICAL_EXCEPTION;
import static com.linkedin.venice.vpj.VenicePushJobConstants.NOT_SET;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PARENT_CONTROLLER_REGION_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PARTITION_COUNT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PATH_FILTER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PERMISSION_700;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PERMISSION_777;
import static com.linkedin.venice.vpj.VenicePushJobConstants.POLL_JOB_STATUS_INTERVAL_MS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.POLL_STATUS_RETRY_ATTEMPTS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_JOB_STATUS_UPLOAD_ENABLE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_TO_SEPARATE_REALTIME_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_ENABLE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_SECONDS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REPUSH_TTL_START_TIMESTAMP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REWIND_EPOCH_TIME_BUFFER_IN_SECONDS_OVERRIDE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REWIND_EPOCH_TIME_IN_SECONDS_OVERRIDE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.REWIND_TIME_IN_SECONDS_OVERRIDE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SEND_CONTROL_MESSAGES_DIRECTLY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SORTED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_ETL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_GRID_FABRIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SOURCE_KAFKA;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SUPPRESS_END_OF_PUSH_MESSAGE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SYSTEM_SCHEMA_READER_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGETED_REGION_PUSH_LIST;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TEMP_DIR_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.UNCREATED_VERSION_NUMBER;
import static com.linkedin.venice.vpj.VenicePushJobConstants.USE_MAPPER_TO_BUILD_DICTIONARY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALIDATE_SCHEMA_AND_BUILD_DICTIONARY_MAPPER_OUTPUT_FILE_EXTENSION;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALIDATE_SCHEMA_AND_BUILD_DICTIONARY_MAPPER_OUTPUT_FILE_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALIDATE_SCHEMA_AND_BUILD_DICT_MAPPER_OUTPUT_DIRECTORY;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_FIELD_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_DISCOVER_URL_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_STORE_NAME_PROP;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.PushJobCheckpoints;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.ZstdWithDictCompressor;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerClientFactory;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ControllerClientFactory;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.RepushInfo;
import com.linkedin.venice.controllerapi.RepushInfoResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.etl.ETLValueSchemaTransformation;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceResourceAccessException;
import com.linkedin.venice.hadoop.input.kafka.KafkaInputDictTrainer;
import com.linkedin.venice.hadoop.mapreduce.common.JobUtils;
import com.linkedin.venice.hadoop.mapreduce.counter.MRJobCounterHelper;
import com.linkedin.venice.hadoop.mapreduce.datawriter.jobs.DataWriterMRJob;
import com.linkedin.venice.hadoop.mapreduce.engine.DefaultJobClientWrapper;
import com.linkedin.venice.hadoop.output.avro.ValidateSchemaAndBuildDictMapperOutput;
import com.linkedin.venice.hadoop.schema.HDFSSchemaSource;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import com.linkedin.venice.hadoop.utils.HadoopUtils;
import com.linkedin.venice.hadoop.utils.VPJSSLUtils;
import com.linkedin.venice.hadoop.validation.NoOpValidator;
import com.linkedin.venice.hadoop.validation.Validator;
import com.linkedin.venice.heartbeat.DefaultPushJobHeartbeatSenderFactory;
import com.linkedin.venice.heartbeat.NoOpPushJobHeartbeatSender;
import com.linkedin.venice.heartbeat.NoOpPushJobHeartbeatSenderFactory;
import com.linkedin.venice.heartbeat.PushJobHeartbeatSender;
import com.linkedin.venice.heartbeat.PushJobHeartbeatSenderFactory;
import com.linkedin.venice.jobs.ComputeJob;
import com.linkedin.venice.jobs.DataWriterComputeJob;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.schema.writecompute.WriteComputeOperation;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.status.PushJobDetailsStatus;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobDetailsStatusTuple;
import com.linkedin.venice.utils.AvroSupersetSchemaUtils;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.DictionaryUtils;
import com.linkedin.venice.utils.EncodingUtils;
import com.linkedin.venice.utils.LatencyUtils;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.RegionUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroJob;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class sets up the Hadoop job used to push data to Venice.
 * The job reads the input data off HDFS. It supports 2 kinds of
 * input -- Avro / Binary Json (Vson).
 */
public class VenicePushJob implements AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(VenicePushJob.class);

  // Immutable state
  private final VeniceProperties props;
  private final String jobId;

  /**
   * The temp directory structure for VPJ will be: <br/>
   * |____{@code $sharedTmpDir} (777 permissions) - shared temp space for all VPJ executions <br/>
   * | |____{@code $jobTmpDir} (700 permissions) - temp space for the current execution ({@code $job.execution.id}_{@literal unique-suffix}) <br/>
   * | | |____veniceMapperOutput (700 permissions) <br/>
   * | | |____rmd_schemas (700 permissions) <br/>
   * | | |____value_schemas (700 permissions) <br/>
   * | | |____...features_added_in_the_future (700 permissions) <br/>
   *  <br/>
   * Common directory under which all the different push jobs create their job specific directories.
   * The value of {@code sharedTmpDir} is obtained by the following steps:
   * <ol>
   *   <li>If {@code tmp.dir.prefix} is configured, that is used.</li>
   *   <li>Otherwise, if {@code hadoop.tmp.dir} is specified in HDFS configs, then {@code ${hadoop.tmp.dir}/venice-push-job} is used.</li>
   *   <li>Otherwise, {@code /venice-push-job} is used.</li>
   * </ol>
   *   {@code ${hadoop.tmp.dir}/venice-push-job} if {@code ${hadoop.tmp.dir}}
   * is set. Otherwise, the path is set to {@code /tmp/venice-push-job}.
   */
  private final Path sharedTmpDir;

  /**
   * Job specific directory: Unique directory for this VPJ to store intermediate data. The value of {@code jobTmpDir} is
   * {@code ${sharedTmpDir}/${job.execution.id}_{unique-suffix}}.
   */
  private final Path jobTmpDir;

  // Lazy state
  private final Lazy<Properties> sslProperties;
  private final Lazy<ByteBuffer> emptyPushZstdDictionary;
  private VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter;
  /** TODO: refactor to use {@link Lazy} */

  // Mutable state
  private ControllerClient controllerClient;
  private ControllerClient kmeSchemaSystemStoreControllerClient;
  private ControllerClient livenessHeartbeatStoreControllerClient;
  private RunningJob runningJob;

  private DataWriterComputeJob dataWriterComputeJob = null;

  // Job config for schema validation and Compression dictionary creation (if needed)
  protected final JobConf validateSchemaAndBuildDictJobConf = new JobConf();

  private InputDataInfoProvider inputDataInfoProvider;
  private ValidateSchemaAndBuildDictMapperOutputReader validateSchemaAndBuildDictMapperOutputReader;
  // Total input data size, which is used to talk to controller to decide whether we have enough quota or not
  private InputDataInfoProvider.InputDataInfo inputDataInfo;

  private Properties veniceWriterProperties;
  private JobClientWrapper jobClientWrapper;
  private SentPushJobDetailsTracker sentPushJobDetailsTracker;
  private ValidateSchemaAndBuildDictMapperOutput validateSchemaAndBuildDictMapperOutput;
  private Path validateSchemaAndBuildDictMapperOutputDirectory;
  private final PushJobSetting pushJobSetting;

  private final PushJobDetails pushJobDetails;
  private final InternalAvroSpecificSerializer<PushJobDetails> pushJobDetailsSerializer =
      AvroProtocolDefinition.PUSH_JOB_DETAILS.getSerializer();

  private InputStorageQuotaTracker inputStorageQuotaTracker;
  private final PushJobHeartbeatSenderFactory pushJobHeartbeatSenderFactory;
  private PushJobHeartbeatSender pushJobHeartbeatSender = null;
  private boolean pushJobStatusUploadDisabledHasBeenLogged = false;

  /**
   * @param jobId  id of the job
   * @param vanillaProps  Property bag for the job
   */
  public VenicePushJob(String jobId, Properties vanillaProps) {
    this.jobId = jobId;
    this.props = getVenicePropsFromVanillaProps(Objects.requireNonNull(vanillaProps, "VPJ props cannot be null"));
    LOGGER.info("Constructing {}: {}", VenicePushJob.class.getSimpleName(), props.toString(true));
    this.sslProperties = Lazy.of(() -> {
      try {
        return VPJSSLUtils.getSslProperties(this.props);
      } catch (IOException e) {
        throw new VeniceException("Could not get user credential");
      }
    });
    this.pushJobSetting = getPushJobSetting(props);
    LOGGER.info("Going to use controller URL: {}  to discover cluster.", pushJobSetting.veniceControllerUrl);
    // Optional configs:
    this.pushJobDetails = new PushJobDetails();
    if (pushJobSetting.livenessHeartbeatEnabled) {
      LOGGER.info("Push job heartbeat is enabled.");
      this.pushJobHeartbeatSenderFactory = new DefaultPushJobHeartbeatSenderFactory();
    } else {
      LOGGER.info("Push job heartbeat is NOT enabled.");
      this.pushJobHeartbeatSenderFactory = new NoOpPushJobHeartbeatSenderFactory();
    }
    emptyPushZstdDictionary =
        Lazy.of(() -> ByteBuffer.wrap(ZstdWithDictCompressor.buildDictionaryOnSyntheticAvroData()));
    sharedTmpDir = new Path(pushJobSetting.sharedTmpDir);
    jobTmpDir = new Path(pushJobSetting.jobTmpDir);
  }

  // This is a part of the public API. There is value in exposing this to users of VenicePushJob for reporting purposes
  public PushJobSetting getPushJobSetting() {
    return this.pushJobSetting;
  }

  // This is a part of the public API. There is value in exposing this to users of VenicePushJob for reporting purposes
  public VeniceProperties getJobProperties() {
    return this.props;
  }

  private VeniceProperties getVenicePropsFromVanillaProps(Properties vanillaProps) {
    handleLegacyConfig(vanillaProps, LEGACY_AVRO_KEY_FIELD_PROP, KEY_FIELD_PROP, "key field");
    handleLegacyConfig(vanillaProps, LEGACY_AVRO_VALUE_FIELD_PROP, VALUE_FIELD_PROP, "value field");
    return new VeniceProperties(vanillaProps);
  }

  private void handleLegacyConfig(
      Properties vanillaProps,
      String legacyConfigProp,
      String newConfigProp,
      String configDescription) {
    String legacyConfig = vanillaProps.getProperty(legacyConfigProp);
    if (legacyConfig != null) {
      String newConfig = vanillaProps.getProperty(newConfigProp);
      if (newConfig == null) {
        vanillaProps.setProperty(newConfigProp, legacyConfig);
      } else if (!newConfig.equals(legacyConfig)) {
        throw new VeniceException(
            "Duplicate " + configDescription + " config found! Both " + legacyConfigProp + " and " + newConfigProp
                + " are set, but with different values! Use only: " + newConfigProp);
      }
    }
  }

  private PushJobSetting getPushJobSetting(VeniceProperties props) {
    PushJobSetting pushJobSettingToReturn = new PushJobSetting();
    pushJobSettingToReturn.jobStartTimeMs = System.currentTimeMillis();
    pushJobSettingToReturn.jobId = jobId;
    pushJobSettingToReturn.jobExecutionId = props.getString(JOB_EXEC_ID, "unknown_exec_id");
    pushJobSettingToReturn.jobServerName = props.getString(JOB_SERVER_NAME, "unknown_job_server");
    pushJobSettingToReturn.veniceControllerUrl = props.getString(VENICE_DISCOVER_URL_PROP);
    pushJobSettingToReturn.enableSSL = props.getBoolean(ENABLE_SSL, DEFAULT_SSL_ENABLED);
    if (pushJobSettingToReturn.enableSSL) {
      VPJSSLUtils.validateSslProperties(props);
    }
    String hadoopTempDir = new Configuration().get(HADOOP_TMP_DIR, "/tmp");
    pushJobSettingToReturn.sharedTmpDir = props.getString(TEMP_DIR_PREFIX, hadoopTempDir + "/venice-push-job");
    LOGGER.info("Using {} as shared temp directory", pushJobSettingToReturn.sharedTmpDir);
    pushJobSettingToReturn.jobTmpDir = pushJobSettingToReturn.sharedTmpDir + "/"
        + Utils.escapeFilePathComponent(Utils.getUniqueString(pushJobSettingToReturn.jobExecutionId));
    LOGGER.info("Using {} as this job's temp directory", pushJobSettingToReturn.sharedTmpDir);
    pushJobSettingToReturn.vpjEntryClass = this.getClass();
    if (props.containsKey(SOURCE_GRID_FABRIC)) {
      pushJobSettingToReturn.sourceGridFabric = props.getString(SOURCE_GRID_FABRIC);
    }
    pushJobSettingToReturn.batchNumBytes = props.getInt(BATCH_NUM_BYTES_PROP, DEFAULT_BATCH_BYTES_SIZE);
    pushJobSettingToReturn.isIncrementalPush = props.getBoolean(INCREMENTAL_PUSH, false);
    pushJobSettingToReturn.isDuplicateKeyAllowed = props.getBoolean(ALLOW_DUPLICATE_KEY, false);
    pushJobSettingToReturn.enablePushJobStatusUpload = props.getBoolean(PUSH_JOB_STATUS_UPLOAD_ENABLE, false);
    pushJobSettingToReturn.controllerRetries = props.getInt(CONTROLLER_REQUEST_RETRY_ATTEMPTS, 1);
    pushJobSettingToReturn.controllerStatusPollRetries = props.getInt(POLL_STATUS_RETRY_ATTEMPTS, 15);
    pushJobSettingToReturn.pollJobStatusIntervalMs =
        props.getLong(POLL_JOB_STATUS_INTERVAL_MS, DEFAULT_POLL_STATUS_INTERVAL_MS);
    pushJobSettingToReturn.jobStatusInUnknownStateTimeoutMs =
        props.getLong(JOB_STATUS_IN_UNKNOWN_STATE_TIMEOUT_MS, DEFAULT_JOB_STATUS_IN_UNKNOWN_STATE_TIMEOUT_MS);
    pushJobSettingToReturn.sendControlMessagesDirectly = props.getBoolean(SEND_CONTROL_MESSAGES_DIRECTLY, false);
    pushJobSettingToReturn.enableWriteCompute = props.getBoolean(ENABLE_WRITE_COMPUTE, false);
    pushJobSettingToReturn.pushToSeparateRealtimeTopicEnabled =
        props.getBoolean(PUSH_TO_SEPARATE_REALTIME_TOPIC, false);
    pushJobSettingToReturn.isSourceETL = props.getBoolean(SOURCE_ETL, false);
    pushJobSettingToReturn.isSourceKafka = props.getBoolean(SOURCE_KAFKA, false);
    pushJobSettingToReturn.kafkaInputCombinerEnabled = props.getBoolean(KAFKA_INPUT_COMBINER_ENABLED, false);
    pushJobSettingToReturn.kafkaInputBuildNewDictEnabled =
        props.getBoolean(KAFKA_INPUT_COMPRESSION_BUILD_NEW_DICT_ENABLED, true);
    pushJobSettingToReturn.suppressEndOfPushMessage = props.getBoolean(SUPPRESS_END_OF_PUSH_MESSAGE, false);
    pushJobSettingToReturn.deferVersionSwap = props.getBoolean(DEFER_VERSION_SWAP, false);
    pushJobSettingToReturn.repushTTLEnabled = props.getBoolean(REPUSH_TTL_ENABLE, false);

    if (pushJobSettingToReturn.repushTTLEnabled && !pushJobSettingToReturn.isSourceKafka) {
      throw new VeniceException("Repush with TTL is only supported while using Kafka Input Format");
    }

    pushJobSettingToReturn.repushTTLStartTimeMs = -1;
    if (pushJobSettingToReturn.repushTTLEnabled) {
      long repushTtlSeconds = props.getLong(REPUSH_TTL_SECONDS, -1);
      long repushTtlStartTimestamp = props.getLong(REPUSH_TTL_START_TIMESTAMP, -1);

      if (repushTtlSeconds >= 0 && repushTtlStartTimestamp >= 0) {
        String message =
            "Both " + REPUSH_TTL_SECONDS + " and " + REPUSH_TTL_START_TIMESTAMP + " are set. Please set only one.";
        throw new VeniceException(message);
      }

      if (repushTtlSeconds >= 0) {
        pushJobSettingToReturn.repushTTLStartTimeMs =
            pushJobSettingToReturn.jobStartTimeMs - (repushTtlSeconds * Time.MS_PER_SECOND);
      } else if (repushTtlStartTimestamp >= 0) {
        pushJobSettingToReturn.repushTTLStartTimeMs = repushTtlStartTimestamp;
      }
    }

    pushJobSettingToReturn.isTargetedRegionPushEnabled = props.getBoolean(TARGETED_REGION_PUSH_ENABLED, false);
    pushJobSettingToReturn.isSystemSchemaReaderEnabled = props.getBoolean(SYSTEM_SCHEMA_READER_ENABLED, false);
    if (pushJobSettingToReturn.isIncrementalPush && pushJobSettingToReturn.isTargetedRegionPushEnabled) {
      throw new VeniceException("Incremental push is not supported while using targeted region push mode");
    }
    if (props.containsKey(TARGETED_REGION_PUSH_LIST)) {
      if (pushJobSettingToReturn.isTargetedRegionPushEnabled) {
        pushJobSettingToReturn.targetedRegions = props.getString(TARGETED_REGION_PUSH_LIST);
      } else {
        throw new VeniceException("Targeted region push list is only supported when targeted region push is enabled");
      }
    }

    final String D2_PREFIX = "d2://";
    if (pushJobSettingToReturn.veniceControllerUrl.startsWith(D2_PREFIX)) {
      pushJobSettingToReturn.d2Routing = true;
      pushJobSettingToReturn.controllerD2ServiceName =
          pushJobSettingToReturn.veniceControllerUrl.substring(D2_PREFIX.length());
      pushJobSettingToReturn.multiRegion = props.getBoolean(MULTI_REGION);
      if (pushJobSettingToReturn.multiRegion) {
        String parentControllerRegionName = props.getString(PARENT_CONTROLLER_REGION_NAME);
        pushJobSettingToReturn.parentControllerRegionD2ZkHosts =
            props.getString(D2_ZK_HOSTS_PREFIX + parentControllerRegionName);
      } else {
        pushJobSettingToReturn.childControllerRegionD2ZkHosts =
            props.getString(D2_ZK_HOSTS_PREFIX + pushJobSettingToReturn.sourceGridFabric);
      }
    } else {
      pushJobSettingToReturn.d2Routing = false;
      pushJobSettingToReturn.controllerD2ServiceName = null;
      pushJobSettingToReturn.multiRegion = props.getBoolean(MULTI_REGION, false);
      pushJobSettingToReturn.parentControllerRegionD2ZkHosts = null;
      pushJobSettingToReturn.childControllerRegionD2ZkHosts = null;
    }

    pushJobSettingToReturn.livenessHeartbeatEnabled = props.getBoolean(HEARTBEAT_ENABLED_CONFIG.getConfigName(), false);
    pushJobSettingToReturn.livenessHeartbeatStoreName = AvroProtocolDefinition.BATCH_JOB_HEARTBEAT.getSystemStoreName();
    if (pushJobSettingToReturn.isSourceKafka) {
      /**
       * The topic could contain duplicate records since the topic could belong to a hybrid store
       * or the speculation execution could be executed for the batch store as well.
       */
      pushJobSettingToReturn.isDuplicateKeyAllowed = true;

      if (pushJobSettingToReturn.isIncrementalPush) {
        throw new VeniceException("Incremental push is not supported while using Kafka Input Format");
      }
      if (pushJobSettingToReturn.isSourceETL) {
        throw new VeniceException("Source ETL is not supported while using Kafka Input Format");
      }
    }

    pushJobSettingToReturn.inputURI = pushJobSettingToReturn.isSourceKafka ? "" : getInputURI(props);
    pushJobSettingToReturn.storeName = props.getString(VENICE_STORE_NAME_PROP);
    pushJobSettingToReturn.rewindTimeInSecondsOverride = props.getLong(REWIND_TIME_IN_SECONDS_OVERRIDE, NOT_SET);

    // If we didn't specify a rewind time
    if (pushJobSettingToReturn.rewindTimeInSecondsOverride == NOT_SET) {
      // But we did specify a rewind time epoch timestamp
      long rewindTimestamp = props.getLong(REWIND_EPOCH_TIME_IN_SECONDS_OVERRIDE, NOT_SET);
      if (rewindTimestamp != NOT_SET) {
        long nowInSeconds = pushJobSettingToReturn.jobStartTimeMs / 1000;
        // So long as that rewind time isn't in the future
        if (rewindTimestamp > nowInSeconds) {
          throw new VeniceException(
              String.format(
                  "Provided '%d' for %s. %s cannot be a timestamp in the future!!",
                  rewindTimestamp,
                  REWIND_EPOCH_TIME_IN_SECONDS_OVERRIDE,
                  REWIND_EPOCH_TIME_IN_SECONDS_OVERRIDE));
        }
        // Set the rewindTimeInSecondsOverride to be the time that is now - the provided timestamp so that we rewind
        // from start of push to the provided timestamp with some extra buffer time since things aren't perfectly
        // instantaneous
        long bufferTime = props.getLong(REWIND_EPOCH_TIME_BUFFER_IN_SECONDS_OVERRIDE, 60);
        pushJobSettingToReturn.rewindTimeInSecondsOverride = (nowInSeconds - rewindTimestamp) + bufferTime;
        // In order for this config to make sense to the user, the remote rewind policy needs to be validated to be
        // REWIND_FROM_SOP
        pushJobSettingToReturn.validateRemoteReplayPolicy = BufferReplayPolicy.REWIND_FROM_SOP;
      }
    }

    pushJobSettingToReturn.extendedSchemaValidityCheckEnabled =
        props.getBoolean(EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED, DEFAULT_EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED);

    if (pushJobSettingToReturn.isSourceKafka) {
      // KIF uses a different code-path to build a dictionary, and we also don't need schema validations for KIF
      pushJobSettingToReturn.useMapperToBuildDict = false;
      pushJobSettingToReturn.compressionMetricCollectionEnabled = false;
    } else {
      pushJobSettingToReturn.useMapperToBuildDict =
          props.getBoolean(USE_MAPPER_TO_BUILD_DICTIONARY, DEFAULT_USE_MAPPER_TO_BUILD_DICTIONARY);
      pushJobSettingToReturn.compressionMetricCollectionEnabled =
          props.getBoolean(COMPRESSION_METRIC_COLLECTION_ENABLED, DEFAULT_COMPRESSION_METRIC_COLLECTION_ENABLED);
    }

    // Compute-engine abstraction related configs
    String dataWriterComputeJobClass = props.getString(DATA_WRITER_COMPUTE_JOB_CLASS, (String) null);

    // Currently, only MR mode supports KIF. This is temporary.
    if (dataWriterComputeJobClass == null || pushJobSettingToReturn.isSourceKafka) {
      pushJobSettingToReturn.dataWriterComputeJobClass = DataWriterMRJob.class;
    } else {
      Class objectClass = ReflectUtils.loadClass(dataWriterComputeJobClass);
      Validate.isAssignableFrom(DataWriterComputeJob.class, objectClass);
      pushJobSettingToReturn.dataWriterComputeJobClass = objectClass;
    }

    return pushJobSettingToReturn;
  }

  /**
   * This method gets the name of the topic with the current version for the given store. It handles below 5 cases:
   *
   * 1. User-provided topic name is null and discovered topic name is good --> use discovered topic name.
   * 2. User-provided topic name is null and discovered topic name is bad --> throw runtime exception.
   * 3. User-provided topic name is not null and discovered name is bad --> use user-provided topic name.
   * 4. User-provided topic name is not null, discovered name is good, and these 2 names mismatch --> throw runtime exception.
   * 5. User-provided topic name is not null, discovered name is good, and these 2 names match --> use either name since
   *    they are the same topic name.
   *
   * @param userProvidedStoreName store name provided by user
   * @param properties properties
   * @return Topic name
   */
  String getSourceTopicNameForKafkaInput(final String userProvidedStoreName, final VeniceProperties properties) {
    final Optional<String> userProvidedTopicNameOptional =
        Optional.ofNullable(properties.getString(KAFKA_INPUT_TOPIC, () -> null));

    // This mode of passing the topic name to VPJ is going to be deprecated.
    if (userProvidedTopicNameOptional.isPresent()) {
      return getUserProvidedTopicName(userProvidedStoreName, userProvidedTopicNameOptional.get());
    }
    // If VPJ has fabric name available use that to find the child colo version otherwise
    // use the largest version among the child colo to use as KIF input topic.
    final Optional<String> userProvidedFabricNameOptional =
        Optional.ofNullable(properties.getString(KAFKA_INPUT_FABRIC, () -> null));

    pushJobSetting.repushInfoResponse = ControllerClient.retryableRequest(
        controllerClient,
        pushJobSetting.controllerRetries,
        c -> c.getRepushInfo(userProvidedStoreName, userProvidedFabricNameOptional));
    if (pushJobSetting.repushInfoResponse.isError()) {
      throw new VeniceException(
          "Could not get repush info for store " + userProvidedStoreName + " with error: "
              + pushJobSetting.repushInfoResponse.getError());
    }
    int version = pushJobSetting.repushInfoResponse.getRepushInfo().getVersion().getNumber();
    pushJobSetting.repushSourceVersion = version;
    return Version.composeKafkaTopic(userProvidedStoreName, version);
  }

  private String getUserProvidedTopicName(final String userProvidedStoreName, String userProvidedTopicName) {
    String derivedStoreName = Version.parseStoreFromKafkaTopicName(userProvidedTopicName);
    if (!Objects.equals(derivedStoreName, userProvidedStoreName)) {
      throw new IllegalArgumentException(
          String.format(
              "Store user-provided name mismatch with the derived store name. "
                  + "Got user-provided store name %s and derived store name %s",
              userProvidedStoreName,
              derivedStoreName));
    }

    LOGGER.info("userProvidedStoreName: {}", userProvidedStoreName);
    StoreResponse storeResponse = getStoreResponse(userProvidedStoreName, true);
    Map<String, Integer> coloToCurrentVersions = getCurrentStoreVersions(storeResponse);
    if (new HashSet<>(coloToCurrentVersions.values()).size() > 1) {
      LOGGER.info(
          "Got current topic version mismatch across multiple colos {}. Use user-provided topic name: {}",
          coloToCurrentVersions,
          userProvidedTopicName);
      return userProvidedTopicName;
    }
    Integer detectedCurrentTopicVersion = null;
    for (Integer topicVersion: coloToCurrentVersions.values()) {
      detectedCurrentTopicVersion = topicVersion;
    }
    String derivedTopicName = Version.composeKafkaTopic(userProvidedStoreName, detectedCurrentTopicVersion);
    if (!Objects.equals(derivedTopicName, userProvidedTopicName)) {
      throw new IllegalStateException(
          String.format(
              "Mismatch between user-provided topic name and auto discovered "
                  + "topic name. They are %s and %s respectively",
              userProvidedTopicName,
              derivedTopicName));
    }
    return derivedTopicName;
  }

  // Visible for testing
  protected void setControllerClient(ControllerClient controllerClient) {
    this.controllerClient = controllerClient;
  }

  // Visible for testing
  // Note: The same jobClientWrapper object is used for all MR jobs currently,
  // so when this object is mocked for testing, every MR run is mocked.
  public void setJobClientWrapper(JobClientWrapper jobClientWrapper) {
    this.jobClientWrapper = jobClientWrapper;
  }

  // Visible for testing
  protected void setInputDataInfoProvider(InputDataInfoProvider inputDataInfoProvider) {
    this.inputDataInfoProvider = inputDataInfoProvider;
  }

  // Visible for testing
  protected void setVeniceWriter(VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter) {
    this.veniceWriter = veniceWriter;
  }

  // Visible for testing
  public void setSentPushJobDetailsTracker(SentPushJobDetailsTracker sentPushJobDetailsTracker) {
    this.sentPushJobDetailsTracker = sentPushJobDetailsTracker;
  }

  // Visible for testing
  protected void setValidateSchemaAndBuildDictMapperOutputReader(
      ValidateSchemaAndBuildDictMapperOutputReader validateSchemaAndBuildDictMapperOutputReader) {
    this.validateSchemaAndBuildDictMapperOutputReader = validateSchemaAndBuildDictMapperOutputReader;
  }

  /**
   * Extensions of this class are allowed to extend this function and return a DataWriterComputeJob that will be used to
   * execute the job
   */
  private DataWriterComputeJob constructDataWriterComputeJob() {
    Class<? extends DataWriterComputeJob> computeJobClass = getPushJobSetting().dataWriterComputeJobClass;
    LOGGER.info("Using '{}' for data writer job", computeJobClass);
    DataWriterComputeJob computeJob = ReflectUtils.callConstructor(computeJobClass, new Class[0], new Object[0]);

    // For testing temporarily
    if (computeJob instanceof DataWriterMRJob) {
      DataWriterMRJob dataWriterMRJob = (DataWriterMRJob) computeJob;

      // This is only used for testing.
      if (jobClientWrapper != null && !(jobClientWrapper instanceof DefaultJobClientWrapper)) {
        dataWriterMRJob.setJobClientWrapper(jobClientWrapper);
      }
    }

    return computeJob;
  }

  DataWriterComputeJob getDataWriterComputeJob() {
    if (dataWriterComputeJob != null) {
      return dataWriterComputeJob;
    }

    return constructDataWriterComputeJob();
  }

  /**
   * @throws VeniceException
   */
  public void run() {
    try {
      Optional<SSLFactory> sslFactory = VPJSSLUtils.createSSLFactory(
          pushJobSetting.enableSSL,
          props.getString(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME),
          this.sslProperties);
      initControllerClient(pushJobSetting.storeName, sslFactory);
      pushJobSetting.clusterName = controllerClient.getClusterName();
      LOGGER.info(
          "The store {} is discovered in Venice cluster {}",
          pushJobSetting.storeName,
          pushJobSetting.clusterName);

      if (pushJobSetting.isSourceKafka) {
        initKIFRepushDetails();
      }

      initPushJobDetails();
      logGreeting();
      sendPushJobDetailsToController();
      HadoopUtils.createDirectoryWithPermission(sharedTmpDir, PERMISSION_777);
      HadoopUtils.createDirectoryWithPermission(jobTmpDir, PERMISSION_700);
      validateKafkaMessageEnvelopeSchema(pushJobSetting);
      validateRemoteHybridSettings(pushJobSetting);
      validateStoreSettingAndPopulate(controllerClient, pushJobSetting);
      inputStorageQuotaTracker = new InputStorageQuotaTracker(pushJobSetting.storeStorageQuota);

      if (pushJobSetting.isSourceETL) {
        MultiSchemaResponse allValueSchemaResponses = controllerClient.getAllValueSchema(pushJobSetting.storeName);
        MultiSchemaResponse.Schema[] allValueSchemas = allValueSchemaResponses.getSchemas();
        Schema lastValueSchema = Schema.parse(allValueSchemas[allValueSchemas.length - 1].getSchemaStr());

        pushJobSetting.etlValueSchemaTransformation = ETLValueSchemaTransformation.fromSchema(lastValueSchema);
      } else {
        pushJobSetting.etlValueSchemaTransformation = ETLValueSchemaTransformation.NONE;
      }

      // For now, assume input has records
      pushJobSetting.compressionMetricCollectionEnabled =
          evaluateCompressionMetricCollectionEnabled(pushJobSetting, true);
      pushJobSetting.isZstdDictCreationRequired = shouldBuildZstdCompressionDictionary(pushJobSetting, true);

      inputDataInfo = getInputDataInfoProvider().validateInputAndGetInfo(pushJobSetting.inputURI);
      pushJobSetting.inputHasRecords = inputDataInfo.hasRecords();
      pushJobSetting.inputFileDataSizeInBytes = inputDataInfo.getInputFileDataSizeInBytes();

      // Now that we know about the input data, we can get the actual value of these configs
      pushJobSetting.compressionMetricCollectionEnabled =
          evaluateCompressionMetricCollectionEnabled(pushJobSetting, inputDataInfo.hasRecords());
      pushJobSetting.isZstdDictCreationRequired =
          shouldBuildZstdCompressionDictionary(pushJobSetting, inputDataInfo.hasRecords());

      /**
       * If the data source is from some existing Kafka topic, no need to validate the input.
       */
      if (!pushJobSetting.isSourceKafka) {
        if (pushJobSetting.isAvro) {
          validateInputDataSchema(pushJobSetting.inputDataSchemaString);
        } else {
          LOGGER.info("Skip validating file schema since it is not Avro.");
        }

        validateKeySchema(pushJobSetting);
        validateValueSchema(controllerClient, pushJobSetting, pushJobSetting.isSchemaAutoRegisterFromPushJobEnabled);

        if (pushJobSetting.useMapperToBuildDict) {
          validateSchemaAndBuildDictMapperOutputDirectory = new Path(jobTmpDir, "veniceMapperOutput");
          HadoopUtils.createDirectoryWithPermission(validateSchemaAndBuildDictMapperOutputDirectory, PERMISSION_700);

          /**
           * 1. validate whether the remaining file's schema are consistent with the first file
           * 2. calculate {@link inputFileDataSize} during step 1
           * 3. Build dictionary (if dictionary compression is enabled for this store version or compressionMetricCollectionEnabled)
           */
          validateSchemaAndBuildDict(
              validateSchemaAndBuildDictJobConf,
              pushJobSetting,
              props,
              jobId,
              pushJobSetting.inputURI);
          sendPushJobDetailsToController();
        }
      }

      Optional<ByteBuffer> optionalCompressionDictionary = getCompressionDictionary();
      String pushId =
          pushJobSetting.jobStartTimeMs + "_" + props.getString(JOB_EXEC_URL, "failed_to_obtain_execution_url");
      if (pushJobSetting.isSourceKafka) {
        pushId = Version.generateRePushId(pushId);
        if (pushJobSetting.sourceKafkaInputVersionInfo.getHybridStoreConfig() != null
            && pushJobSetting.rewindTimeInSecondsOverride == NOT_SET) {
          pushJobSetting.rewindTimeInSecondsOverride = DEFAULT_RE_PUSH_REWIND_IN_SECONDS_OVERRIDE;
          LOGGER.info("Overriding re-push rewind time in seconds to: {}", pushJobSetting.rewindTimeInSecondsOverride);
        }
        if (pushJobSetting.repushTTLEnabled) {
          // Build the full path for HDFSRmdSchemaSource:
          // RMD schemas: <job_temp_dir>/rmd_schemas
          // Value schemas: <job_temp_dir>/value_schemas
          Path rmdSchemaDir = new Path(jobTmpDir, "rmd_schemas");
          HadoopUtils.createDirectoryWithPermission(rmdSchemaDir, PERMISSION_700);
          Path valueSchemaDir = new Path(jobTmpDir, "value_schemas");
          HadoopUtils.createDirectoryWithPermission(valueSchemaDir, PERMISSION_700);
          try (HDFSSchemaSource schemaSource =
              new HDFSSchemaSource(valueSchemaDir, rmdSchemaDir, pushJobSetting.storeName)) {
            schemaSource.saveSchemasOnDisk(controllerClient);
            pushJobSetting.rmdSchemaDir = schemaSource.getRmdSchemaPath();
            pushJobSetting.valueSchemaDir = schemaSource.getValueSchemaPath();
          }
        }
      }
      // Create new store version, topic and fetch Kafka url from backend
      createNewStoreVersion(
          pushJobSetting,
          inputDataInfo.getInputFileDataSizeInBytes(),
          controllerClient,
          pushId,
          props,
          optionalCompressionDictionary);
      updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.NEW_VERSION_CREATED);
      // Update and send push job details with new info to the controller
      pushJobDetails.pushId = pushId;
      pushJobDetails.partitionCount = pushJobSetting.partitionCount;
      pushJobDetails.valueCompressionStrategy = pushJobSetting.topicCompressionStrategy != null
          ? pushJobSetting.topicCompressionStrategy.getValue()
          : CompressionStrategy.NO_OP.getValue();
      pushJobDetails.chunkingEnabled = pushJobSetting.chunkingEnabled;
      pushJobDetails.overallStatus.add(getPushJobDetailsStatusTuple(PushJobDetailsStatus.TOPIC_CREATED.getValue()));
      pushJobHeartbeatSender = createPushJobHeartbeatSender(pushJobSetting.enableSSL);
      pushJobHeartbeatSender.start(pushJobSetting.storeName, pushJobSetting.version);
      sendPushJobDetailsToController();
      // Log Venice data push job related info
      logPushJobProperties(pushJobSetting, pushJobSetting.inputURI, inputDataInfo.getInputFileDataSizeInBytes());

      if (pushJobSetting.isIncrementalPush) {
        /**
         * N.B.: For now, we always send control messages directly for incremental pushes, regardless of
         * {@link pushJobSetting.sendControlMessagesDirectly}, because the controller does not yet support
         * sending these types of CM. If/when we add support for that in the controller, then we'll be able
         * to completely stop using the {@link VeniceWriter} from this class.
         */
        pushJobSetting.incrementalPushVersion =
            pushJobSetting.jobStartTimeMs + "_" + pushJobSetting.jobServerName + "_" + pushJobSetting.jobExecutionId;
        LOGGER.info("Incremental Push Version: {}", pushJobSetting.incrementalPushVersion);
        getVeniceWriter(pushJobSetting)
            .broadcastStartOfIncrementalPush(pushJobSetting.incrementalPushVersion, new HashMap<>());
        runJobAndUpdateStatus();
        getVeniceWriter(pushJobSetting)
            .broadcastEndOfIncrementalPush(pushJobSetting.incrementalPushVersion, Collections.emptyMap());
      } else {
        if (pushJobSetting.sendControlMessagesDirectly) {
          getVeniceWriter(pushJobSetting).broadcastStartOfPush(
              SORTED,
              pushJobSetting.isChunkingEnabled,
              pushJobSetting.topicCompressionStrategy,
              optionalCompressionDictionary,
              Collections.emptyMap());
        } else {
          /**
           * No-op, as it was already sent as part of the call to
           * {@link createNewStoreVersion(PushJobSetting, long, ControllerClient, String, VeniceProperties)}
           */
        }
        runJobAndUpdateStatus();

        if (!pushJobSetting.suppressEndOfPushMessage) {
          if (pushJobSetting.sendControlMessagesDirectly) {
            getVeniceWriter(pushJobSetting).broadcastEndOfPush(Collections.emptyMap());
          } else {
            controllerClient.writeEndOfPush(pushJobSetting.storeName, pushJobSetting.version);
          }
        }
      }
      // Close VeniceWriter before polling job status since polling job status could
      // trigger job deletion
      closeVeniceWriter();
      // Update and send push job details with new info
      updatePushJobDetailsWithDataWriterTracker();
      pushJobDetails.overallStatus
          .add(getPushJobDetailsStatusTuple(PushJobDetailsStatus.DATA_WRITER_COMPLETED.getValue()));
      sendPushJobDetailsToController();
      // Waiting for Venice Backend to complete consumption
      updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.START_JOB_STATUS_POLLING);

      // Poll for job status unless we've suppressed sending EOP, in which case, don't wait up
      if (!pushJobSetting.suppressEndOfPushMessage) {
        pollStatusUntilComplete(
            pushJobSetting.incrementalPushVersion,
            controllerClient,
            pushJobSetting,
            pushJobSetting.targetedRegions,
            pushJobSetting.isTargetedRegionPushEnabled);
      }

      updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED);
      // Do not mark completed yet as for target region push it will be marked inside postValidationConsumption
      if (!pushJobSetting.isTargetedRegionPushEnabled) {
        pushJobDetails.overallStatus.add(getPushJobDetailsStatusTuple(PushJobDetailsStatus.COMPLETED.getValue()));
      }
      pushJobDetails.jobDurationInMs = LatencyUtils.getElapsedTimeFromMsToMs(pushJobSetting.jobStartTimeMs);
      sendPushJobDetailsToController();

      // only kick off the validation and post-validation flow when everything has to be done in a single VPJ
      if (!pushJobSetting.isTargetedRegionPushEnabled) {
        return;
      }

      /**
       * Post validation + consumption
       */
      Set<String> candidateRegions = getRegionsForPostValidationConsumption();
      if (candidateRegions.isEmpty()) {
        LOGGER.info("No region that needs post-validation consumption identified. Finish the job now.");
        return;
      }
      postPushValidation();
      postValidationConsumption(candidateRegions);
    } catch (Throwable e) {
      LOGGER.error("Failed to run job.", e);
      // Make sure all the logic before killing the failed push jobs is captured in the following block
      try {
        if (e instanceof VeniceResourceAccessException) {
          updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.WRITE_ACL_FAILED);
        }
        pushJobDetails.overallStatus.add(getPushJobDetailsStatusTuple(PushJobDetailsStatus.ERROR.getValue()));
        pushJobDetails.failureDetails = e.toString();
        pushJobDetails.jobDurationInMs = LatencyUtils.getElapsedTimeFromMsToMs(pushJobSetting.jobStartTimeMs);
        sendPushJobDetailsToController();
        closeVeniceWriter();
      } catch (Exception ex) {
        LOGGER.error(
            "Error before killing the failed push job; still issue the kill job command to clean up states in backend",
            ex);
      } finally {
        try {
          killJob(pushJobSetting, controllerClient);
          LOGGER.info("Successfully killed the failed push job.");
        } catch (Exception ex) {
          LOGGER.info("Failed to stop and cleanup the job. New pushes might be blocked.", ex);
        }
      }
      throwVeniceException(e);
    } finally {
      Utils.closeQuietlyWithErrorLogged(inputDataInfoProvider);
      if (pushJobHeartbeatSender != null) {
        pushJobHeartbeatSender.stop();
        pushJobHeartbeatSender = null;
      }
      inputDataInfoProvider = null;
      if (pushJobSetting.rmdSchemaDir != null) {
        HadoopUtils.cleanUpHDFSPath(pushJobSetting.rmdSchemaDir, true);
      }
    }
  }

  /**
   * Get the set of regions that haven't been pushed yet after targeted region push.
   * @return a set of regions that haven't been pushed yet.
   */
  private Set<String> getRegionsForPostValidationConsumption() {
    Set<String> targetedRegions = RegionUtils.parseRegionsFilterList(pushJobSetting.targetedRegions);
    Set<String> candidateRegions =
        new HashSet<>(pushJobSetting.storeResponse.getStore().getColoToCurrentVersions().keySet());
    candidateRegions.removeAll(targetedRegions);
    return candidateRegions;
  }

  /**
   * Start a group of validation job to verify result from the previous targeted region push is healthy.
   */
  void postPushValidation() {
    if (pushJobSetting.kafkaSourceRegion == null) {
      throw new VeniceException("Post-validation consumption halted due to no available source region found");
    }

    // TODO: Add parallelism support when we have more validators. It's premature to do so now.
    Validator noOpValidator = new NoOpValidator();
    noOpValidator.validate();
  }

  /**
   * Kick off the post-validation consumption on the rest of regions that haven't been pushed yet.
   * @param candidateRegions
   */
  void postValidationConsumption(Set<String> candidateRegions) {
    // get the latest version of the store for data recovery
    Version sourceVersion = getStoreVersion(pushJobSetting.storeName, pushJobSetting.version);
    LOGGER
        .info("Starting to push to rest of the regions {} after successful push to targeted region.", candidateRegions);

    if (sourceVersion.getHybridStoreConfig() != null) {
      // perform data recovery for HYBRID stores
      pushJobSetting.isTargetedRegionPushEnabled = false;
      pushJobSetting.targetedRegions = null;
      // set up repush
      pushJobSetting.isSourceKafka = true;
      pushJobSetting.kafkaInputBrokerUrl = pushJobSetting.kafkaUrl;
      pushJobSetting.kafkaInputTopic = pushJobSetting.topic;
      this.run();
    } else {
      // perform data recovery for BATCH stores
      for (String region: candidateRegions) {
        LOGGER.info("Pushing from {} to {}", pushJobSetting.kafkaSourceRegion, region);
        ControllerResponse response = controllerClient.dataRecovery(
            pushJobSetting.kafkaSourceRegion,
            region,
            pushJobSetting.storeName,
            sourceVersion.getNumber(),
            true,
            true,
            Optional.of(sourceVersion));
        if (response.isError()) {
          throw new VeniceException("Can't push data for region " + region + ". " + response.getError());
        }
      }
      // Essentially, the remaining regions are "targeted" regions now
      pollStatusUntilComplete(
          null,
          controllerClient,
          pushJobSetting,
          RegionUtils.composeRegionList(candidateRegions),
          false);
    }
    pushJobDetails.overallStatus.add(getPushJobDetailsStatusTuple(PushJobDetailsStatus.COMPLETED.getValue()));
    sendPushJobDetailsToController();
  }

  private PushJobHeartbeatSender createPushJobHeartbeatSender(final boolean sslEnabled) {
    try {
      return pushJobHeartbeatSenderFactory.createHeartbeatSender(
          pushJobSetting.kafkaUrl,
          props,
          livenessHeartbeatStoreControllerClient,
          sslEnabled ? Optional.of(this.sslProperties.get()) : Optional.empty());
    } catch (Exception e) {
      LOGGER.warn("Failed to create a push job heartbeat sender. Use the no-op push job heartbeat sender.", e);
      pushJobDetails.sendLivenessHeartbeatFailureDetails = e.getMessage();
      return new NoOpPushJobHeartbeatSender();
    }
  }

  private void updatePushJobDetailsWithLivenessHeartbeatException() {
    if (pushJobHeartbeatSender == null || this.pushJobDetails == null) {
      return;
    }
    if (pushJobDetails.sendLivenessHeartbeatFailureDetails == null) {
      pushJobHeartbeatSender.getFirstSendHeartbeatException()
          .ifPresent(
              firstSendHeartbeatException -> pushJobDetails.sendLivenessHeartbeatFailureDetails =
                  firstSendHeartbeatException.getMessage());
    }
  }

  private void validateInputDataSchema(String inputDataSchemaString) {
    try {
      AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(inputDataSchemaString);
    } catch (Exception e) {
      if (pushJobSetting.extendedSchemaValidityCheckEnabled) {
        LOGGER.error(
            "The schema of the input data failed strict Avro schema validation. Verify if the schema is a valid Avro schema.",
            e);
        updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.EXTENDED_INPUT_DATA_SCHEMA_VALIDATION_FAILED);
        throw new VeniceException(e);
      }

      LOGGER.info("The schema of the input data failed strict Avro schema validation. Trying loose schema validation.");
      try {
        AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(inputDataSchemaString);
      } catch (Exception looseValidationException) {
        LOGGER.error(
            "The schema of the input data failed loose Avro schema validation. Verify if the schema is a valid Avro schema.",
            looseValidationException);
        updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.INPUT_DATA_SCHEMA_VALIDATION_FAILED);
        throw new VeniceException(looseValidationException);
      }
    }
  }

  void runJobAndUpdateStatus() {
    try {
      updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.START_DATA_WRITER_JOB);
      LOGGER.info("Configuring data writer job");
      dataWriterComputeJob = getDataWriterComputeJob();
      dataWriterComputeJob.configure(props, pushJobSetting);
      LOGGER.info("Triggering data writer job");
      dataWriterComputeJob.runJob();
      if (dataWriterComputeJob.getStatus() != ComputeJob.Status.SUCCEEDED) {
        if (!pushJobSetting.isSourceKafka) {
          try {
            checkLastModificationTimeAndLog();
          } catch (IOException e) {
            LOGGER.warn("Failed to check last modification time of input file", e);
          }
        }
        Throwable t = dataWriterComputeJob.getFailureReason();
        if (t == null) {
          throw new VeniceException(
              "Data writer job failed unexpectedly with status: " + dataWriterComputeJob.getStatus());
        } else {
          throwVeniceException(t);
        }
      } else {
        String errorMessage = updatePushJobDetailsWithJobDetails(dataWriterComputeJob.getTaskTracker());
        if (errorMessage != null) {
          throw new VeniceException(errorMessage);
        }
      }
      updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.DATA_WRITER_JOB_COMPLETED);
    } finally {
      Utils.closeQuietlyWithErrorLogged(dataWriterComputeJob);
    }
  }

  private void runValidateSchemaAndBuildDictJobAndUpdateStatus(JobConf conf) throws Exception {
    updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.START_VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB);
    runningJob = runJobWithConfig(conf);
    validateCountersAfterValidateSchemaAndBuildDict();
    getValidateSchemaAndBuildDictMapperOutput(runningJob.getID().toString());
    updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB_COMPLETED);
  }

  protected static String getValidateSchemaAndBuildDictionaryOutputFileNameNoExtension(String mrJobId) {
    return VALIDATE_SCHEMA_AND_BUILD_DICTIONARY_MAPPER_OUTPUT_FILE_PREFIX + mrJobId;
  }

  protected static String getValidateSchemaAndBuildDictionaryOutputFileName(String mrJobId) {
    return getValidateSchemaAndBuildDictionaryOutputFileNameNoExtension(mrJobId)
        + VALIDATE_SCHEMA_AND_BUILD_DICTIONARY_MAPPER_OUTPUT_FILE_EXTENSION;
  }

  private void getValidateSchemaAndBuildDictMapperOutput(String mrJobId) throws Exception {
    Path outputDir = validateSchemaAndBuildDictMapperOutputDirectory;
    String outputAvroFile = getValidateSchemaAndBuildDictionaryOutputFileName(mrJobId);
    try (ValidateSchemaAndBuildDictMapperOutputReader outputReader =
        getValidateSchemaAndBuildDictMapperOutputReader(outputDir, outputAvroFile)) {
      validateSchemaAndBuildDictMapperOutput = outputReader.getOutput();
    }
    inputDataInfo.setInputFileDataSizeInBytes(validateSchemaAndBuildDictMapperOutput.getInputFileDataSize());
  }

  private void checkLastModificationTimeAndLog() throws IOException {
    checkLastModificationTimeAndLog(false);
  }

  private void checkLastModificationTimeAndLog(boolean throwExceptionOnDataSetChange) throws IOException {
    long lastModificationTime = getInputDataInfoProvider().getInputLastModificationTime(pushJobSetting.inputURI);
    if (lastModificationTime > inputDataInfo.getInputModificationTime()) {
      updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.DATASET_CHANGED);
      String error = "Dataset changed during the push job. Please check above logs to see if the change "
          + "caused the MapReduce failure and rerun the job without dataset change.";
      LOGGER.error(error);
      if (throwExceptionOnDataSetChange) {
        throw new VeniceException(error);
      }
    }
  }

  /**
   * This functions decides whether Zstd compression dictionary needs to be trained or not,
   * based on the type of push, configs and whether there are any input records or not, or
   * whether {@link PushJobSetting#compressionMetricCollectionEnabled} is enabled or not.
   */
  protected static boolean shouldBuildZstdCompressionDictionary(
      PushJobSetting pushJobSetting,
      boolean inputFileHasRecords) {
    if (pushJobSetting.isIncrementalPush) {
      LOGGER.info("No compression dictionary will be generated as the push type is incremental push");
      return false;
    }

    if (pushJobSetting.compressionMetricCollectionEnabled
        || pushJobSetting.storeCompressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
      if (!inputFileHasRecords) {
        if (pushJobSetting.storeCompressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
          LOGGER.info(
              "Compression strategy is {} with no input records. A dictionary will be generated from synthetic data or current version data for hybrid stores",
              pushJobSetting.storeCompressionStrategy);
        } else {
          LOGGER.info("No compression dictionary will be generated as there are no records");
          return false;
        }
      }

      LOGGER.info(
          "Compression dictionary will be generated with the compression strategy {} "
              + "and compressionMetricCollectionEnabled is {}",
          pushJobSetting.storeCompressionStrategy,
          (pushJobSetting.compressionMetricCollectionEnabled ? "Enabled" : "Disabled"));
      return true;
    } else if (!inputFileHasRecords) {
      LOGGER.info("No compression dictionary will be generated as there are no records");
    }

    LOGGER.info(
        "No Compression dictionary will be generated with the compression strategy"
            + " {} and compressionMetricCollectionEnabled is disabled",
        pushJobSetting.storeCompressionStrategy);
    return false;
  }

  /**
   * This functions evaluates the config {@link PushJobSetting#compressionMetricCollectionEnabled}
   * based on the input data and other configs as an initial filter to disable this config for cases
   * where we won't be able to collect this information or where it doesn't make sense to collect this
   * information. eg: When there are no data or for Incremental push.
   */
  protected static boolean evaluateCompressionMetricCollectionEnabled(
      PushJobSetting pushJobSetting,
      boolean inputFileHasRecords) {
    if (!pushJobSetting.compressionMetricCollectionEnabled) {
      // if the config is not enabled, just return false
      return false;
    }

    if (!inputFileHasRecords) {
      LOGGER.info("No compression related metrics will be generated as there are no records");
      return false;
    }

    if (pushJobSetting.isIncrementalPush) {
      LOGGER.info("No compression related metrics will be generated as the push type is incremental push");
      return false;
    }

    return true;
  }

  /**
   * Validate whether the Job ran successfully to validate schema and build dictionary:
   * - No error counters are increased
   * - Number of records processed == Num files + 1 (one extra to build dictionary)
   *
   * @throws IOException
   */
  private void validateCountersAfterValidateSchemaAndBuildDict() throws IOException {
    if (inputDataInfo.hasRecords()) {
      Counters counters = runningJob.getCounters();
      final long dataModifiedDuringPushJobCount =
          MRJobCounterHelper.getMapperErrorDataModifiedDuringPushJobCount(counters);
      if (dataModifiedDuringPushJobCount != 0) {
        updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.DATASET_CHANGED);
        String err =
            "Error while validating schema and building dictionary: Because Dataset changed during the push job. Rerun the job without dataset change";
        LOGGER.error(err);
        throw new VeniceException(err);
      }

      final long readInvalidInputIdxCount = MRJobCounterHelper.getMapperInvalidInputIdxCount(counters);
      if (readInvalidInputIdxCount != 0) {
        checkLastModificationTimeAndLog(true);
        updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.INVALID_INPUT_FILE);
        String err = "Error while validating schema and building dictionary: Input file Idx is invalid, "
            + "MR job counter is not reliable to point out the reason";
        LOGGER.error(err);
        throw new VeniceException(err);
      }

      final long invalidInputFileCount = MRJobCounterHelper.getMapperInvalidInputFileCount(counters);
      if (invalidInputFileCount != 0) {
        updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.INVALID_INPUT_FILE);
        String err = "Error while validating schema: Input directory should not have sub directory";
        LOGGER.error(err);
        throw new VeniceException(err);
      }

      final long schemaInconsistencyFailureCount =
          MRJobCounterHelper.getMapperSchemaInconsistencyFailureCount(counters);
      if (schemaInconsistencyFailureCount != 0) {
        updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.INPUT_DATA_SCHEMA_VALIDATION_FAILED);
        String err = "Error while validating schema: Inconsistent file schema found";
        LOGGER.error(err);
        throw new VeniceException(err);
      }

      final long zstdDictCreationFailureCount = MRJobCounterHelper.getMapperZstdDictTrainFailureCount(counters);
      final long zstdDictCreationSuccessCount = MRJobCounterHelper.getMapperZstdDictTrainSuccessCount(counters);
      final long zstdDictCreationSkippedCount = MRJobCounterHelper.getMapperZstdDictTrainSkippedCount(counters);
      pushJobSetting.isZstdDictCreationSuccess = (zstdDictCreationSuccessCount == 1);
      boolean isZstdDictCreationFailure = (zstdDictCreationFailureCount == 1);
      boolean isZstdDictCreationSkipped = (zstdDictCreationSkippedCount == 1);

      final long recordsSuccessfullyProcessedCount =
          MRJobCounterHelper.getMapperNumRecordsSuccessfullyProcessedCount(counters);
      if (recordsSuccessfullyProcessedCount == inputDataInfo.getNumInputFiles() + 1) {
        if (pushJobSetting.isZstdDictCreationRequired) {
          if (!pushJobSetting.isZstdDictCreationSuccess) {
            checkLastModificationTimeAndLog(true);
            updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.INVALID_INPUT_FILE);
            String err = "Error while validating schema: MR job counter is not reliable to point out the exact reason";
            LOGGER.error(err);
            throw new VeniceException(err);
          }
        }
      } else if (recordsSuccessfullyProcessedCount == inputDataInfo.getNumInputFiles()) {
        if (isZstdDictCreationFailure || isZstdDictCreationSkipped) {
          String err = isZstdDictCreationFailure
              ? "Training ZSTD compression dictionary failed: The content might not be suitable for creating dictionary."
              : "Training ZSTD compression dictionary skipped: The sample size is too small.";
          if (pushJobSetting.storeCompressionStrategy != CompressionStrategy.ZSTD_WITH_DICT) {
            // Tried creating dictionary due to compressionMetricCollectionEnabled
            LOGGER.warn(
                err + " But as this job's configured compression strategy don't need dictionary, the job is not stopped");
          } else {
            updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.ZSTD_DICTIONARY_CREATION_FAILED);
            LOGGER.error(err);
            throw new VeniceException(err);
          }
        } else {
          checkLastModificationTimeAndLog(true);
          String err = "Error while validating schema: MR job counter is not reliable to point out the reason";
          LOGGER.error(err);
          throw new VeniceException(err);
        }
      } else {
        checkLastModificationTimeAndLog(true);
        updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.INVALID_INPUT_FILE);
        String err = "Error while validating schema: MR job counter is not reliable to point out the exact reason";
        LOGGER.error(err);
        throw new VeniceException(err);
      }
    }
  }

  // Visible for testing
  void setKmeSchemaSystemStoreControllerClient(ControllerClient controllerClient) {
    this.kmeSchemaSystemStoreControllerClient = controllerClient;
  }

  private RunningJob runJobWithConfig(JobConf jobConf) throws IOException {
    if (jobClientWrapper == null) {
      jobClientWrapper = new DefaultJobClientWrapper();
    }
    try {
      return jobClientWrapper.runJobWithConfig(jobConf);
    } catch (Exception e) {
      if (!pushJobSetting.isSourceKafka) {
        checkLastModificationTimeAndLog();
      }
      throw e;
    }
  }

  protected InputDataInfoProvider constructInputDataInfoProvider() {
    InputDataInfoProvider dataInfoProvider;
    if (pushJobSetting.isSourceKafka) {
      dataInfoProvider = new KafkaInputDataInfoProvider();
    } else {
      dataInfoProvider = new DefaultInputDataInfoProvider(pushJobSetting, props);
    }

    return dataInfoProvider;
  }

  protected InputDataInfoProvider getInputDataInfoProvider() {
    if (inputDataInfoProvider == null) {
      inputDataInfoProvider = constructInputDataInfoProvider();
    }
    return inputDataInfoProvider;
  }

  protected ValidateSchemaAndBuildDictMapperOutputReader getValidateSchemaAndBuildDictMapperOutputReader(
      Path outputDir,
      String fileName) throws Exception {
    if (validateSchemaAndBuildDictMapperOutputReader == null) {
      validateSchemaAndBuildDictMapperOutputReader =
          new ValidateSchemaAndBuildDictMapperOutputReader(outputDir, fileName);
    }
    return validateSchemaAndBuildDictMapperOutputReader;
  }

  /**
   * Create a new instance of controller client and set it to the controller client field if the controller client field
   * has null value. If the controller client field is not null, it could mean:
   *    1. The controller client field has already been initialized
   *    2. A mock controller client is provided
   *
   * @param storeName
   * @param sslFactory
   */
  private void initControllerClient(String storeName, Optional<SSLFactory> sslFactory) {
    final String controllerD2ZkHost;
    if (pushJobSetting.multiRegion) {
      // In multi region mode, push jobs will communicate with parent controller
      controllerD2ZkHost = pushJobSetting.parentControllerRegionD2ZkHosts;
    } else {
      // In single region mode, push jobs will communicate with child controller
      controllerD2ZkHost = pushJobSetting.childControllerRegionD2ZkHosts;
    }

    if (controllerClient == null) {
      controllerClient = getControllerClient(
          storeName,
          pushJobSetting.d2Routing,
          pushJobSetting.controllerD2ServiceName,
          controllerD2ZkHost,
          sslFactory,
          pushJobSetting.controllerRetries);
    } else {
      LOGGER.info("Controller client has already been initialized");
    }

    if (kmeSchemaSystemStoreControllerClient == null) {
      kmeSchemaSystemStoreControllerClient = getControllerClient(
          AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName(),
          pushJobSetting.d2Routing,
          pushJobSetting.controllerD2ServiceName,
          controllerD2ZkHost,
          sslFactory,
          pushJobSetting.controllerRetries);
    } else {
      LOGGER.info("System store controller client has already been initialized");
    }

    if (pushJobSetting.livenessHeartbeatEnabled) {
      String heartbeatStoreName = pushJobSetting.livenessHeartbeatStoreName;
      this.livenessHeartbeatStoreControllerClient = getControllerClient(
          heartbeatStoreName,
          pushJobSetting.d2Routing,
          pushJobSetting.controllerD2ServiceName,
          controllerD2ZkHost,
          sslFactory,
          pushJobSetting.controllerRetries);
    } else {
      this.livenessHeartbeatStoreControllerClient = null;
    }
  }

  protected void initKIFRepushDetails() {
    pushJobSetting.kafkaInputTopic = getSourceTopicNameForKafkaInput(pushJobSetting.storeName, props);
    if (pushJobSetting.repushInfoResponse == null) {
      pushJobSetting.kafkaInputBrokerUrl = props.getString(KAFKA_INPUT_BROKER_URL);
    } else {
      RepushInfo repushInfo = pushJobSetting.repushInfoResponse.getRepushInfo();
      pushJobSetting.kafkaInputBrokerUrl = repushInfo.getKafkaBrokerUrl();
      pushJobSetting.systemSchemaClusterD2ServiceName = repushInfo.getSystemSchemaClusterD2ServiceName();
      pushJobSetting.systemSchemaClusterD2ZKHost = repushInfo.getSystemSchemaClusterD2ZkHost();
    }
    if (pushJobSetting.isSystemSchemaReaderEnabled
        && (StringUtils.isEmpty(pushJobSetting.systemSchemaClusterD2ServiceName)
            || StringUtils.isEmpty(pushJobSetting.systemSchemaClusterD2ZKHost))) {
      throw new VeniceException("D2 service name and zk host must be provided when system schema reader is enabled");
    }
  }

  private ControllerClient getControllerClient(
      String storeName,
      boolean useD2ControllerClient,
      String controllerD2ServiceName,
      String d2ZkHosts,
      Optional<SSLFactory> sslFactory,
      int retryAttempts) {
    if (useD2ControllerClient) {
      return D2ControllerClientFactory.discoverAndConstructControllerClient(
          storeName,
          controllerD2ServiceName,
          d2ZkHosts,
          sslFactory,
          retryAttempts);
    } else {
      return ControllerClientFactory.discoverAndConstructControllerClient(
          storeName,
          pushJobSetting.veniceControllerUrl,
          sslFactory,
          retryAttempts);
    }
  }

  private Optional<ByteBuffer> getCompressionDictionary() throws VeniceException {
    if (!pushJobSetting.isZstdDictCreationRequired) {
      return Optional.empty();
    }

    ByteBuffer compressionDictionary;
    try {
      compressionDictionary = fetchOrBuildCompressionDictionary();
    } catch (Exception e) {
      LOGGER.warn("Failed to fetch or build compression dictionary", e);
      compressionDictionary = null;
    }

    if (compressionDictionary != null && compressionDictionary.remaining() > 0) {
      pushJobSetting.isZstdDictCreationSuccess = true;
      return Optional.of(compressionDictionary);
    }

    if (pushJobSetting.storeCompressionStrategy != CompressionStrategy.ZSTD_WITH_DICT) {
      LOGGER.info(
          "No dictionary fetched. But since the compression strategy is not {}, it is not required",
          CompressionStrategy.ZSTD_WITH_DICT);
      pushJobSetting.isZstdDictCreationSuccess = false;
      return Optional.empty();
    }

    LOGGER.info("No dictionary fetched. Creating a default dictionary since compression strategy is ZSTD_WITH_DICT");
    pushJobSetting.isZstdDictCreationSuccess = true;
    return Optional.of(emptyPushZstdDictionary.get());
  }

  private ByteBuffer fetchOrBuildCompressionDictionary() throws VeniceException {
    // Prepare the param builder, which can be used by different scenarios.
    KafkaInputDictTrainer.ParamBuilder paramBuilder = new KafkaInputDictTrainer.ParamBuilder()
        .setKeySchema(AvroCompatibilityHelper.toParsingForm(pushJobSetting.storeKeySchema))
        .setSslProperties(pushJobSetting.enableSSL ? sslProperties.get() : new Properties())
        .setCompressionDictSize(
            props.getInt(
                COMPRESSION_DICTIONARY_SIZE_LIMIT,
                VeniceWriter.DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES))
        .setDictSampleSize(
            props.getInt(COMPRESSION_DICTIONARY_SAMPLE_SIZE, DEFAULT_COMPRESSION_DICTIONARY_SAMPLE_SIZE));
    if (pushJobSetting.isSourceKafka) {
      paramBuilder.setSourceVersionChunkingEnabled(pushJobSetting.sourceKafkaInputVersionInfo.isChunkingEnabled());
      // Currently, KIF repush will always build a dict in Azkaban Job driver if necessary.
      if (pushJobSetting.storeCompressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
        if (pushJobSetting.kafkaInputBuildNewDictEnabled) {
          LOGGER.info("Rebuild a new Zstd dictionary from the input topic: {}", pushJobSetting.kafkaInputTopic);
          paramBuilder.setKafkaInputBroker(pushJobSetting.kafkaInputBrokerUrl)
              .setTopicName(pushJobSetting.kafkaInputTopic)
              .setSourceVersionCompressionStrategy(pushJobSetting.sourceKafkaInputVersionInfo.getCompressionStrategy());
          KafkaInputDictTrainer dictTrainer = new KafkaInputDictTrainer(paramBuilder.build());
          return ByteBuffer.wrap(dictTrainer.trainDict());
        } else {
          LOGGER.info("Reading Zstd dictionary from input topic: {}", pushJobSetting.kafkaInputTopic);
          // set up ssl properties and kafka consumer properties
          Properties kafkaConsumerProperties = new Properties();
          if (pushJobSetting.enableSSL) {
            kafkaConsumerProperties.putAll(this.sslProperties.get());
          }
          kafkaConsumerProperties.setProperty(KAFKA_BOOTSTRAP_SERVERS, pushJobSetting.kafkaInputBrokerUrl);
          return DictionaryUtils
              .readDictionaryFromKafka(pushJobSetting.kafkaInputTopic, new VeniceProperties(kafkaConsumerProperties));
        }
      }
      LOGGER.info(
          "No dictionary will be fetched for repush workloads with compression strategy: {}",
          CompressionStrategy.ZSTD_WITH_DICT);
      return null;
    }

    if (pushJobSetting.storeCompressionStrategy == CompressionStrategy.ZSTD_WITH_DICT && !inputDataInfo.hasRecords()) {
      LOGGER.info("Compression strategy is {} with no input records", pushJobSetting.storeCompressionStrategy);

      if (pushJobSetting.hybridStoreConfig == null) {
        return null;
      }

      /**
       * Special handling for empty push with ZSTD_WITH_DICT. This compression strategy needs a dictionary even if
       * there is no input data, so we try to generate a dictionary from the current version. Note that it won't work
       * for the very first version, and the following versions will work.
       */
      LOGGER.info("Since this is a hybrid store, attempting to generate dictionary from current version data");
      String storeName = getPushJobSetting().storeName;

      // Get the latest version
      RepushInfoResponse repushInfoResponse = ControllerClient.retryableRequest(
          controllerClient,
          pushJobSetting.controllerRetries,
          c -> c.getRepushInfo(storeName, Optional.empty()));

      if (repushInfoResponse.isError()) {
        LOGGER.warn("Could not get repush info for store {} with error: {}", storeName, repushInfoResponse.getError());
        return null;
      }

      int sourceVersion = repushInfoResponse.getRepushInfo().getVersion().getNumber();
      String sourceTopicName = Version.composeKafkaTopic(storeName, sourceVersion);
      String sourceKafkaUrl = repushInfoResponse.getRepushInfo().getKafkaBrokerUrl();
      LOGGER.info(
          "Rebuild a new Zstd dictionary from the source topic: {} in Kafka: {}",
          sourceTopicName,
          sourceKafkaUrl);
      paramBuilder.setKafkaInputBroker(repushInfoResponse.getRepushInfo().getKafkaBrokerUrl())
          .setTopicName(sourceTopicName)
          .setSourceVersionCompressionStrategy(
              repushInfoResponse.getRepushInfo().getVersion().getCompressionStrategy());
      KafkaInputDictTrainer dictTrainer = new KafkaInputDictTrainer(paramBuilder.build());
      return ByteBuffer.wrap(dictTrainer.trainDict());
    }

    if (!pushJobSetting.useMapperToBuildDict) {
      return ByteBuffer.wrap(getInputDataInfoProvider().trainZstdDictionary());
    } else {
      // In case of pushJobSetting.useMapperToBuildDict job, the dictionary will already have been generated
      if (pushJobSetting.isZstdDictCreationSuccess) {
        LOGGER.info(
            "Retrieving the Zstd dictionary trained by {}",
            ValidateSchemaAndBuildDictMapper.class.getSimpleName());
        return validateSchemaAndBuildDictMapperOutput.getZstdDictionary();
      }
    }

    return null;
  }

  private void throwVeniceException(Throwable e) throws VeniceException {
    if (!(e instanceof VeniceException)) {
      e = new VeniceException("Exception or error caught during VenicePushJob: " + e.getMessage(), e);
    }
    throw (VeniceException) e;
  }

  /**
   * Get input path from the properties;
   * Check whether there is sub-directory in the input directory
   *
   * @param props
   * @return input URI
   * @throws Exception
   */
  protected String getInputURI(VeniceProperties props) {
    Configuration conf = new Configuration();
    String uri = props.getString(INPUT_PATH_PROP);
    Path uriPath = new Path(uri);
    try {
      FileSystem fs = uriPath.getFileSystem(conf);
      Path sourcePath = getLatestPathOfInputDirectory(uri, fs);
      return sourcePath.toString();
    } catch (IOException e) {
      throw new VeniceException("Exception caught when getting input path", e);
    }
  }

  private void initPushJobDetails() {
    pushJobDetails.clusterName = pushJobSetting.clusterName;
    pushJobDetails.overallStatus = new ArrayList<>();
    pushJobDetails.overallStatus.add(getPushJobDetailsStatusTuple(PushJobDetailsStatus.STARTED.getValue()));
    pushJobDetails.pushId = "";
    pushJobDetails.partitionCount = -1;
    pushJobDetails.valueCompressionStrategy = CompressionStrategy.NO_OP.getValue();
    pushJobDetails.chunkingEnabled = false;
    pushJobDetails.jobDurationInMs = -1;
    pushJobDetails.totalNumberOfRecords = -1;
    pushJobDetails.totalKeyBytes = -1;
    pushJobDetails.totalRawValueBytes = -1;
    pushJobDetails.totalCompressedValueBytes = -1;
    pushJobDetails.failureDetails = "";
    pushJobDetails.pushJobLatestCheckpoint = PushJobCheckpoints.INITIALIZE_PUSH_JOB.getValue();
    pushJobDetails.pushJobConfigs = Collections.singletonMap(
        HEARTBEAT_ENABLED_CONFIG.getConfigName(),
        String.valueOf(pushJobSetting.livenessHeartbeatEnabled));
  }

  private void updatePushJobDetailsWithCheckpoint(PushJobCheckpoints checkpoint) {
    pushJobDetails.pushJobLatestCheckpoint = checkpoint.getValue();
  }

  private void updatePushJobDetailsWithDataWriterTracker() {
    if (dataWriterComputeJob == null) {
      LOGGER.info("No running job found. Skip updating push job details.");
      return;
    }
    DataWriterTaskTracker taskTracker = dataWriterComputeJob.getTaskTracker();
    if (taskTracker == null) {
      LOGGER.info("Found a running job, but it didn't provide a task tracker.");
      return;
    }

    try {
      pushJobDetails.totalNumberOfRecords = taskTracker.getOutputRecordsCount();
      pushJobDetails.totalKeyBytes = taskTracker.getTotalKeySize();
      // size of uncompressed value
      pushJobDetails.totalRawValueBytes = taskTracker.getTotalUncompressedValueSize();
      // size of the final stored data in SN (can be compressed using NO_OP/GZIP/ZSTD_WITH_DICT)
      pushJobDetails.totalCompressedValueBytes = taskTracker.getTotalValueSize();
      // size of the Gzip compressed data
      pushJobDetails.totalGzipCompressedValueBytes = taskTracker.getTotalGzipCompressedValueSize();
      // size of the Zstd with Dict compressed data
      pushJobDetails.totalZstdWithDictCompressedValueBytes = taskTracker.getTotalZstdCompressedValueSize();
      LOGGER.info(
          "Data writer job summary: " + "\n\tTotal number of records: {}" + "\n\tSize of keys: {}"
              + "\n\tSize of uncompressed values: {}" + "\n\tConfigured value compression strategy: {}"
              + "\n\tSize of compressed values: {}" + "\n\tFinal data size stored in Venice: {}"
              + "\n\tCompression Metrics collection: {}",
          pushJobDetails.totalNumberOfRecords,
          ByteUtils.generateHumanReadableByteCountString(pushJobDetails.totalKeyBytes),
          ByteUtils.generateHumanReadableByteCountString(pushJobDetails.totalRawValueBytes),
          pushJobSetting.topicCompressionStrategy,
          ByteUtils.generateHumanReadableByteCountString(pushJobDetails.totalCompressedValueBytes),
          ByteUtils.generateHumanReadableByteCountString(
              pushJobDetails.totalKeyBytes + pushJobDetails.totalCompressedValueBytes),
          pushJobSetting.compressionMetricCollectionEnabled ? "Enabled" : "Disabled");
      if (pushJobSetting.compressionMetricCollectionEnabled) {
        LOGGER.info(
            "\tData size if compressed using Gzip: {}",
            ByteUtils.generateHumanReadableByteCountString(
                pushJobDetails.totalKeyBytes + pushJobDetails.totalGzipCompressedValueBytes));
        if (pushJobSetting.isZstdDictCreationSuccess) {
          LOGGER.info(
              "\tData size if compressed using Zstd with Dictionary: {}",
              ByteUtils.generateHumanReadableByteCountString(
                  pushJobDetails.totalKeyBytes + pushJobDetails.totalZstdWithDictCompressedValueBytes));
        } else {
          LOGGER.info("\tZstd Dictionary creation Failed");
        }
      }
    } catch (Exception e) {
      LOGGER.warn(
          "Exception caught while updating push job details with map reduce counters. {}",
          NON_CRITICAL_EXCEPTION,
          e);
    }
  }

  /**
   * Configs should only be attached to the last event for a store version due to the size of these configs. i.e. should
   * only be attached when the overall status is a terminal state.
   */
  private void updatePushJobDetailsWithConfigs() {
    try {
      int lastStatus = pushJobDetails.overallStatus.get(pushJobDetails.overallStatus.size() - 1).status;
      if (PushJobDetailsStatus.isTerminal(lastStatus)) {
        Map<CharSequence, CharSequence> pushJobConfigs = new HashMap<>();
        for (String key: props.keySet()) {
          pushJobConfigs.put(key, props.getString(key));
        }
        if (!pushJobConfigs.containsKey(HEARTBEAT_ENABLED_CONFIG.getConfigName())) {
          pushJobConfigs
              .put(HEARTBEAT_ENABLED_CONFIG.getConfigName(), String.valueOf(pushJobSetting.livenessHeartbeatEnabled));
        }
        pushJobDetails.pushJobConfigs = pushJobConfigs;
        // TODO find a way to get meaningful producer configs to populate the producerConfigs map here.
        // Currently most of the easily accessible VeniceWriter configs are not interesting and contains sensitive
        // information such as passwords which doesn't seem appropriate to propagate them to push job details.
        pushJobDetails.producerConfigs = new HashMap<>();
      }
    } catch (Exception e) {
      LOGGER.warn("Exception caught while updating push job details with configs. {}", NON_CRITICAL_EXCEPTION, e);
    }
  }

  /**
   * Best effort attempt to get more details on reasons behind Spark job failure by looking at Spark accumulators
   *
   * @return Error message if there is any error detected in the reporter counter and {@code null} otherwise
   */
  String updatePushJobDetailsWithJobDetails(DataWriterTaskTracker dataWriterTaskTracker) {
    // Quota exceeded
    final long totalInputDataSizeInBytes =
        dataWriterTaskTracker.getTotalKeySize() + dataWriterTaskTracker.getTotalValueSize();
    if (inputStorageQuotaTracker.exceedQuota(totalInputDataSizeInBytes)) {
      updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.QUOTA_EXCEEDED);
      Long storeQuota = inputStorageQuotaTracker.getStoreStorageQuota();
      String errorMessage = String.format(
          "Storage quota exceeded. Store quota %s, Input data size %s."
              + " Please request at least %s additional quota.",
          generateHumanReadableByteCountString(storeQuota),
          generateHumanReadableByteCountString(totalInputDataSizeInBytes),
          generateHumanReadableByteCountString(totalInputDataSizeInBytes - storeQuota));
      return errorMessage;
    }
    // Write ACL failed
    final long writeAclFailureCount = dataWriterTaskTracker.getWriteAclAuthorizationFailureCount();
    if (writeAclFailureCount > 0) {
      updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.WRITE_ACL_FAILED);
      String errorMessage = "Insufficient ACLs to write to the store";
      return errorMessage;
    }
    // Duplicate keys
    if (!pushJobSetting.isDuplicateKeyAllowed) {
      final long duplicateKeyWithDistinctValueCount = dataWriterTaskTracker.getDuplicateKeyWithDistinctValueCount();
      if (duplicateKeyWithDistinctValueCount > 0) {
        updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.DUP_KEY_WITH_DIFF_VALUE);
        String errorMessage = String.format(
            "Input data has at least %d keys that appear more than once but have different values",
            duplicateKeyWithDistinctValueCount);
        return errorMessage;
      }
    }
    // Record too large
    final long recordTooLargeFailureCount = dataWriterTaskTracker.getRecordTooLargeFailureCount();
    if (recordTooLargeFailureCount > 0) {
      updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.RECORD_TOO_LARGE_FAILED);

      // maxSizeForUserPayloadPerMessageInBytes < 1MB (Kafka record limit); errors appear when chunking is not enabled
      // maxRecordSizeBytes can be much larger and should take effect only once chunking is enabled
      final int recordSizeLimit = (pushJobDetails.chunkingEnabled)
          ? getVeniceWriter(pushJobSetting).getMaxRecordSizeBytes()
          : getVeniceWriter(pushJobSetting).getMaxSizeForUserPayloadPerMessageInBytes();
      final String errorMessage = String.format(
          "Input data has at least %d records that exceed the maximum record limit of %s",
          recordTooLargeFailureCount,
          generateHumanReadableByteCountString(recordSizeLimit));
      return errorMessage;
    }
    return null;
  }

  /** Transform per colo {@link ExecutionStatus} to per colo {@link PushJobDetailsStatus} */
  protected static PushJobDetailsStatus getPerColoPushJobDetailsStatusFromExecutionStatus(
      ExecutionStatus executionStatus) {
    switch (executionStatus.getRootStatus()) {
      case NOT_CREATED:
      case NEW:
      case NOT_STARTED:
        return PushJobDetailsStatus.NOT_CREATED;
      case STARTED:
      case PROGRESS:
      case CATCH_UP_BASE_TOPIC_OFFSET_LAG:
        return PushJobDetailsStatus.STARTED;
      case END_OF_PUSH_RECEIVED:
        return PushJobDetailsStatus.END_OF_PUSH_RECEIVED;
      case TOPIC_SWITCH_RECEIVED:
        return PushJobDetailsStatus.DATA_WRITER_COMPLETED;
      case START_OF_INCREMENTAL_PUSH_RECEIVED:
        return PushJobDetailsStatus.START_OF_INCREMENTAL_PUSH_RECEIVED;
      case END_OF_INCREMENTAL_PUSH_RECEIVED:
        return PushJobDetailsStatus.END_OF_INCREMENTAL_PUSH_RECEIVED;
      case COMPLETED:
      case DATA_RECOVERY_COMPLETED:
        return PushJobDetailsStatus.COMPLETED;
      case ERROR:
        return PushJobDetailsStatus.ERROR;
      default:
        return PushJobDetailsStatus.UNKNOWN;
    }
  }

  private void updatePushJobDetailsWithColoStatus(Map<String, String> coloSpecificInfo, Set<String> completedColos) {
    try {
      if (pushJobDetails.coloStatus == null) {
        pushJobDetails.coloStatus = new HashMap<>();
      }
      coloSpecificInfo.entrySet()
          .stream()
          // Don't bother updating the completed colo's status
          .filter(coloEntry -> !completedColos.contains(coloEntry.getKey()))
          .forEach(coloEntry -> {
            ExecutionStatus executionStatus = ExecutionStatus.valueOf(coloEntry.getValue());
            int pushJobDetailsStatus = getPerColoPushJobDetailsStatusFromExecutionStatus(executionStatus).getValue();
            if (!pushJobDetails.coloStatus.containsKey(coloEntry.getKey())) {
              List<PushJobDetailsStatusTuple> newList = new ArrayList<>();
              newList.add(getPushJobDetailsStatusTuple(pushJobDetailsStatus));
              pushJobDetails.coloStatus.put(coloEntry.getKey(), newList);
            } else {
              List<PushJobDetailsStatusTuple> statuses = pushJobDetails.coloStatus.get(coloEntry.getKey());
              if (statuses.get(statuses.size() - 1).status != pushJobDetailsStatus) {
                // Only add the pushJobDetailsStatus if there is a change
                statuses.add(getPushJobDetailsStatusTuple(pushJobDetailsStatus));
              }
            }
          });

    } catch (Exception e) {
      LOGGER.warn("Exception caught while updating push job details with colo status. {}", NON_CRITICAL_EXCEPTION, e);
    }
  }

  private PushJobDetailsStatusTuple getPushJobDetailsStatusTuple(int status) {
    PushJobDetailsStatusTuple tuple = new PushJobDetailsStatusTuple();
    tuple.status = status;
    tuple.timestamp = System.currentTimeMillis();
    return tuple;
  }

  private void sendPushJobDetailsToController() {
    if (!pushJobSetting.enablePushJobStatusUpload) {
      if (!pushJobStatusUploadDisabledHasBeenLogged) {
        pushJobStatusUploadDisabledHasBeenLogged = true;
        LOGGER.warn("Unable to send push job details for monitoring purpose. Feature is disabled");
      }
      return;
    } else if (pushJobDetails == null) {
      LOGGER.warn("Unable to send push job details for monitoring purpose. The payload was not populated properly");
      return;
    }

    // update push job details with more info if needed
    updatePushJobDetailsWithConfigs();
    updatePushJobDetailsWithLivenessHeartbeatException();

    // send push job details to controller
    try {
      pushJobDetails.reportTimestamp = System.currentTimeMillis();
      int version = pushJobSetting.version <= 0 ? UNCREATED_VERSION_NUMBER : pushJobSetting.version;
      ControllerResponse response = controllerClient.sendPushJobDetails(
          pushJobSetting.storeName,
          version,
          pushJobDetailsSerializer.serialize(null, pushJobDetails));
      getSentPushJobDetailsTracker().record(pushJobSetting.storeName, version, pushJobDetails);

      if (response.isError()) {
        LOGGER.warn("Failed to send push job details. {} Details: {}", NON_CRITICAL_EXCEPTION, response.getError());
      }
    } catch (Exception e) {
      LOGGER.error("Exception caught while sending push job details. {}", NON_CRITICAL_EXCEPTION, e);
    }
  }

  private SentPushJobDetailsTracker getSentPushJobDetailsTracker() {
    if (sentPushJobDetailsTracker == null) {
      sentPushJobDetailsTracker = new NoOpSentPushJobDetailsTracker();
    }
    return sentPushJobDetailsTracker;
  }

  private void logGreeting() {
    LOGGER.info(
        "Running VenicePushJob: " + jobId + Utils.NEW_LINE_CHAR + "  _    _           _                   "
            + Utils.NEW_LINE_CHAR + " | |  | |         | |                  " + Utils.NEW_LINE_CHAR
            + " | |__| | __ _  __| | ___   ___  _ __  " + Utils.NEW_LINE_CHAR
            + " |  __  |/ _` |/ _` |/ _ \\ / _ \\| '_ \\ " + Utils.NEW_LINE_CHAR
            + " | |  | | (_| | (_| | (_) | (_) | |_) |   " + Utils.NEW_LINE_CHAR
            + " |_|  |_|\\__,_|\\__,_|\\___/ \\___/| .__/" + Utils.NEW_LINE_CHAR
            + "                _______         | |     " + Utils.NEW_LINE_CHAR
            + "               |__   __|        |_|     " + Utils.NEW_LINE_CHAR
            + "                  | | ___               " + Utils.NEW_LINE_CHAR
            + "                  | |/ _ \\             " + Utils.NEW_LINE_CHAR
            + "     __      __   | | (_) |             " + Utils.NEW_LINE_CHAR
            + "     \\ \\    / /   |_|\\___/           " + Utils.NEW_LINE_CHAR
            + "      \\ \\  / /__ _ __  _  ___ ___     " + Utils.NEW_LINE_CHAR
            + "       \\ \\/ / _ | '_ \\| |/ __/ _ \\  " + Utils.NEW_LINE_CHAR
            + "        \\  |  __| | | | | (_|  __/     " + Utils.NEW_LINE_CHAR
            + "         \\/ \\___|_| |_|_|\\___\\___|  " + Utils.NEW_LINE_CHAR
            + "      ___        _     _                " + Utils.NEW_LINE_CHAR
            + "     |  _ \\     (_)   | |              " + Utils.NEW_LINE_CHAR
            + "     | |_) |_ __ _  __| | __ _  ___     " + Utils.NEW_LINE_CHAR
            + "     |  _ <| '__| |/ _` |/ _` |/ _ \\   " + Utils.NEW_LINE_CHAR
            + "     | |_) | |  | | (_| | (_| |  __/    " + Utils.NEW_LINE_CHAR
            + "     |____/|_|  |_|\\__,_|\\__, |\\___| " + Utils.NEW_LINE_CHAR
            + "                          __/ |         " + Utils.NEW_LINE_CHAR
            + "                         |___/          " + Utils.NEW_LINE_CHAR);
  }

  /**
   * This method will validate the key schema in the input file against the one registered in Venice.
   */
  void validateKeySchema(PushJobSetting setting) {
    Schema serverSchema = pushJobSetting.storeKeySchema;
    Schema clientSchema = pushJobSetting.keySchema;
    String canonicalizedServerSchema = AvroCompatibilityHelper.toParsingForm(serverSchema);
    String canonicalizedClientSchema = AvroCompatibilityHelper.toParsingForm(clientSchema);
    if (!canonicalizedServerSchema.equals(canonicalizedClientSchema)) {
      String briefErrorMessage = "Key schema mis-match for store " + setting.storeName;
      LOGGER.error(
          "{}\n\t\tschema defined in HDFS: \t{}\n\t\tschema defined in Venice: \t{}",
          briefErrorMessage,
          pushJobSetting.keySchemaString,
          serverSchema.toString());
      throw new VeniceException(briefErrorMessage);
    }
  }

  protected void validateRemoteHybridSettings() {
    validateRemoteHybridSettings(pushJobSetting);
  }

  protected void validateRemoteHybridSettings(PushJobSetting setting) {
    if (setting.validateRemoteReplayPolicy != null) {
      StoreResponse response = getStoreResponse(setting.storeName);
      HybridStoreConfig hybridStoreConfig = response.getStore().getHybridStoreConfig();
      if (!setting.validateRemoteReplayPolicy.equals(hybridStoreConfig.getBufferReplayPolicy())) {
        throw new VeniceException(
            String.format(
                "Remote rewind policy is {} but push settings require a policy of {}. "
                    + "Please adjust hybrid settings or push job configuration!",
                hybridStoreConfig.getBufferReplayPolicy(),
                setting.validateRemoteReplayPolicy));
      }
    }
  }

  private void validateKafkaMessageEnvelopeSchema(PushJobSetting setting) {
    SchemaResponse response = ControllerClient.retryableRequest(
        kmeSchemaSystemStoreControllerClient,
        setting.controllerRetries,
        c -> c.getValueSchema(
            AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName(),
            AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion()));

    if (response.isError()) {
      throw new VeniceException(
          "KME protocol is upgraded in the push job but not in the Venice backend; Please contact Venice team. Error : "
              + response.getError());
    }
  }

  private Schema getKeySchemaFromController(ControllerClient controllerClient, int retries, String storeName) {
    SchemaResponse keySchemaResponse =
        ControllerClient.retryableRequest(controllerClient, retries, c -> c.getKeySchema(storeName));
    if (keySchemaResponse.isError()) {
      throw new VeniceException("Got an error in keySchemaResponse: " + keySchemaResponse);
    } else if (keySchemaResponse.getSchemaStr() == null) {
      // TODO: Fix the server-side request handling. This should not happen. We should get a 404 instead.
      throw new VeniceException("Got a null schema in keySchemaResponse: " + keySchemaResponse);
    }
    return AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(keySchemaResponse.getSchemaStr());
  }

  /***
   * This method will talk to controller to validate value schema.
   */
  void validateValueSchema(
      ControllerClient controllerClient,
      PushJobSetting setting,
      boolean schemaAutoRegisterFromPushJobEnabled) {
    LOGGER.info("Validating value schema: {} for store: {}", pushJobSetting.valueSchemaString, setting.storeName);

    SchemaResponse getValueSchemaIdResponse;
    if (setting.enableWriteCompute) {
      if (!isUpdateSchema(pushJobSetting.valueSchemaString)) {
        MultiSchemaResponse multiSchemaResponse = ControllerClient.retryableRequest(
            controllerClient,
            setting.controllerRetries,
            c -> c.getAllValueAndDerivedSchema(setting.storeName));
        StoreResponse storeResponse = ControllerClient
            .retryableRequest(controllerClient, setting.controllerRetries, c -> c.getStore(setting.storeName));

        if (storeResponse.isError()) {
          throw new VeniceException("Store does not exist: " + setting.storeName);
        }
        if (multiSchemaResponse.isError()) {
          throw new VeniceException("Unable to retrieve schemas for store: " + setting.storeName);
        }
        /**
         * For now, we will issue a separated call to controller to retrieve superset schema ID to identify the latest
         * update schema for partial update enabled store. In the future, we should include superset schema ID in the
         * MultiSchemaResponse for different purpose.
         */
        int supersetSchemaId = storeResponse.getStore().getLatestSuperSetValueSchemaId();
        MultiSchemaResponse.Schema supersetSchema =
            AvroSupersetSchemaUtils.getSupersetSchemaFromSchemaResponse(multiSchemaResponse, supersetSchemaId);
        if (supersetSchema == null) {
          throw new VeniceException("Superset schema not found for store: " + setting.storeName);
        }
        if (!validateSubsetValueSchema(pushJobSetting.valueSchema, supersetSchema.getSchemaStr())) {
          throw new VeniceException(
              "Input value schema is not subset of superset schema. Input value schema: " + pushJobSetting.valueSchema
                  + " , superset schema: " + supersetSchema.getSchemaStr());
        }
        // With new input format, we will need to use the latest update schema to generate partial update record.
        MultiSchemaResponse.Schema latestUpdateSchema =
            AvroSupersetSchemaUtils.getLatestUpdateSchemaFromSchemaResponse(multiSchemaResponse, supersetSchemaId);
        if (latestUpdateSchema == null) {
          throw new VeniceException("Latest update schema not found for store: " + setting.storeName);
        }
        // Create a dummy schema response to make sure existing check logics went through.
        getValueSchemaIdResponse = new SchemaResponse();
        getValueSchemaIdResponse.setSchemaStr(latestUpdateSchema.getSchemaStr());
        getValueSchemaIdResponse.setId(latestUpdateSchema.getId());
        getValueSchemaIdResponse.setDerivedSchemaId(latestUpdateSchema.getDerivedSchemaId());
        // Update the schema to be the update schema of the superset schema.
        LOGGER.info(
            "Detect partial update input format as: {}, using latest update schema: {}",
            pushJobSetting.valueSchemaString,
            latestUpdateSchema.getSchemaStr());
        pushJobSetting.valueSchemaString = latestUpdateSchema.getSchemaStr();
        pushJobSetting.generatePartialUpdateRecordFromInput = true;
      } else {
        getValueSchemaIdResponse = ControllerClient.retryableRequest(
            controllerClient,
            setting.controllerRetries,
            c -> c.getValueOrDerivedSchemaId(setting.storeName, pushJobSetting.valueSchemaString));
      }
    } else {
      getValueSchemaIdResponse = ControllerClient.retryableRequest(
          controllerClient,
          setting.controllerRetries,
          c -> c.getValueSchemaID(setting.storeName, pushJobSetting.valueSchemaString));
    }
    if (getValueSchemaIdResponse.isError() && !schemaAutoRegisterFromPushJobEnabled) {
      MultiSchemaResponse response = controllerClient.getAllValueSchema(setting.storeName);
      if (response.isError()) {
        LOGGER.error("Failed to fetch all value schemas, so they will not be printed. " + response.getError());
      } else {
        LOGGER.info("All currently registered value schemas:");
        for (MultiSchemaResponse.Schema schema: response.getSchemas()) {
          LOGGER.info("Schema {}: {}", schema.getId(), schema.getSchemaStr());
        }
      }
      throw new VeniceException(
          "Failed to validate value schema for store: " + setting.storeName + "\nError from the server: "
              + getValueSchemaIdResponse.getError() + "\nSchema for the data file: "
              + pushJobSetting.valueSchemaString);
    }

    if (getValueSchemaIdResponse.isError() && schemaAutoRegisterFromPushJobEnabled) {
      LOGGER
          .info("Auto registering value schema: {} for store: {}", pushJobSetting.valueSchemaString, setting.storeName);
      SchemaResponse addValueSchemaResponse = ControllerClient.retryableRequest(
          controllerClient,
          setting.controllerRetries,
          c -> c.addValueSchema(setting.storeName, pushJobSetting.valueSchemaString));
      if (addValueSchemaResponse.isError()) {
        throw new VeniceException(
            "Failed to auto-register value schema for store: " + setting.storeName + "\nError from the server: "
                + addValueSchemaResponse.getError() + "\nSchema for the data file: "
                + pushJobSetting.valueSchemaString);
      }
      // Add value schema successfully
      setSchemaIdPropInPushJobSetting(pushJobSetting, addValueSchemaResponse, setting.enableWriteCompute);
    } else {
      // Get value schema ID successfully
      setSchemaIdPropInPushJobSetting(pushJobSetting, getValueSchemaIdResponse, setting.enableWriteCompute);
    }
    LOGGER.info(
        "Got schema id: {} for value schema: {} of store: {}",
        pushJobSetting.valueSchemaId,
        pushJobSetting.valueSchemaString,
        setting.storeName);
  }

  // Visible for testing
  boolean isUpdateSchema(String schemaString) {
    return schemaString.contains(WriteComputeOperation.NO_OP_ON_FIELD.getName());
  }

  private void setSchemaIdPropInPushJobSetting(
      PushJobSetting pushJobSetting,
      SchemaResponse valueSchemaResponse,
      boolean enableWriteCompute) {
    pushJobSetting.valueSchemaId = valueSchemaResponse.getId();
    if (enableWriteCompute) {
      pushJobSetting.derivedSchemaId = valueSchemaResponse.getDerivedSchemaId();
    }
  }

  /**
   * Validate the store settings against the Push job settings and populate additional information, e.g. keySchema, if needed.
   * @param controllerClient
   * @param jobSetting
   * @return
   */
  private void validateStoreSettingAndPopulate(ControllerClient controllerClient, PushJobSetting jobSetting) {
    StoreResponse storeResponse = getStoreResponse(jobSetting.storeName);
    jobSetting.storeStorageQuota = storeResponse.getStore().getStorageQuotaInByte();

    // Do not enable for deferred swap or hybrid store
    if (pushJobSetting.deferVersionSwap || storeResponse.getStore().getHybridStoreConfig() != null) {
      LOGGER.warn(
          "target region is not available for {} as it hybrid or deferred version swap enabled.",
          jobSetting.storeName);
      jobSetting.isTargetedRegionPushEnabled = false;
    }

    if (jobSetting.isTargetedRegionPushEnabled && jobSetting.targetedRegions == null) {
      // only override the targeted regions if it is not set and it is a single region push
      // use source grid fabric as target region to reduce data hop, else use default NR source
      if (!StringUtils.isEmpty(jobSetting.sourceGridFabric)) {
        jobSetting.targetedRegions = jobSetting.sourceGridFabric;
      } else {
        jobSetting.targetedRegions = storeResponse.getStore().getNativeReplicationSourceFabric();
      }
      if (StringUtils.isEmpty(jobSetting.targetedRegions)) {
        throw new VeniceException(
            "The store either does not have native replication mode enabled or set up default source fabric.");
      }
    }

    HybridStoreConfig hybridStoreConfig = storeResponse.getStore().getHybridStoreConfig();
    if (jobSetting.repushTTLEnabled) {
      if (hybridStoreConfig == null) {
        throw new VeniceException("Repush TTL is only supported for real-time only store.");
      } else {
        if (jobSetting.repushTTLStartTimeMs <= 0) {
          long storeRewindTimeInSeconds = hybridStoreConfig.getRewindTimeInSeconds();
          jobSetting.repushTTLStartTimeMs =
              pushJobSetting.jobStartTimeMs - (storeRewindTimeInSeconds * Time.MS_PER_SECOND);
        }

        LOGGER.info("Will evict records older than epoch time: {} ms", jobSetting.repushTTLStartTimeMs);
      }
    }

    if (jobSetting.enableWriteCompute && !jobSetting.isStoreWriteComputeEnabled) {
      throw new VeniceException("Store does not have write compute enabled.");
    }

    if (jobSetting.enableWriteCompute && (!jobSetting.isStoreIncrementalPushEnabled || !jobSetting.isIncrementalPush)) {
      throw new VeniceException("Write compute is only available for incremental push jobs.");
    }

    if (jobSetting.enableWriteCompute && jobSetting.isStoreWriteComputeEnabled) {
      /*
        If write compute is enabled, we would perform a topic switch from the controller and have the
        controller be in charge of broadcasting start and end messages. We will disable
        sendControlMessagesDirectly to prevent races between the messages sent by the VenicePushJob and
        by the controller for topic switch.
       */
      jobSetting.sendControlMessagesDirectly = false;
    }

    jobSetting.storeKeySchema =
        getKeySchemaFromController(controllerClient, jobSetting.controllerRetries, jobSetting.storeName);

    if (jobSetting.isSourceKafka) {
      int sourceVersionNumber = Version.parseVersionFromKafkaTopicName(jobSetting.kafkaInputTopic);
      Optional<Version> sourceVersion = storeResponse.getStore().getVersion(sourceVersionNumber);

      if (!sourceVersion.isPresent()) {
        if (jobSetting.repushInfoResponse != null
            && jobSetting.repushInfoResponse.getRepushInfo().getVersion().getNumber() == sourceVersionNumber) {
          LOGGER.warn("Could not find version {} in parent colo, fetching from child colo.", sourceVersionNumber);
          sourceVersion = Optional.of(jobSetting.repushInfoResponse.getRepushInfo().getVersion());
        } else {
          throw new VeniceException(
              "Could not find version " + sourceVersionNumber + ", please provide input fabric to repush.");
        }
      }
      jobSetting.sourceKafkaInputVersionInfo = sourceVersion.get();
      jobSetting.sourceVersionCompressionStrategy = jobSetting.sourceKafkaInputVersionInfo.getCompressionStrategy();
      jobSetting.sourceVersionChunkingEnabled = jobSetting.sourceKafkaInputVersionInfo.isChunkingEnabled();
      // Skip quota check
      jobSetting.storeStorageQuota = Store.UNLIMITED_STORAGE_QUOTA;
      if (sourceVersion.get().isChunkingEnabled() && !storeResponse.getStore().isChunkingEnabled()) {
        throw new VeniceException("Source version has chunking enabled while chunking is disabled in store config.");
      }
    }
  }

  private Map<String, Integer> getCurrentStoreVersions(StoreResponse storeResponse) {
    Map<String, Integer> coloToCurrentVersionMap = storeResponse.getStore().getColoToCurrentVersions();
    if (coloToCurrentVersionMap == null || coloToCurrentVersionMap.isEmpty()) {
      // Single-colo setup without Parent Cluster
      return Collections.singletonMap("unknown_single_colo", storeResponse.getStore().getCurrentVersion());
    }
    return Collections.unmodifiableMap(coloToCurrentVersionMap);
  }

  private Version.PushType getPushType(PushJobSetting pushJobSetting) {
    return pushJobSetting.isIncrementalPush ? Version.PushType.INCREMENTAL : Version.PushType.BATCH;
  }

  /**
   * This method will talk to parent controller to create new store version, which will create new topic for the version as well.
   */
  void createNewStoreVersion(
      PushJobSetting setting,
      long inputFileDataSize,
      ControllerClient controllerClient,
      String pushId,
      VeniceProperties props,
      Optional<ByteBuffer> optionalCompressionDictionary) {
    Version.PushType pushType = getPushType(setting);
    boolean askControllerToSendControlMessage = !setting.sendControlMessagesDirectly;
    final String partitioners = props.getString(VENICE_PARTITIONERS, DefaultVenicePartitioner.class.getName());

    Optional<String> dictionary;
    if (askControllerToSendControlMessage) {
      dictionary =
          optionalCompressionDictionary.map(ByteUtils::extractByteArray).map(EncodingUtils::base64EncodeToString);
    } else {
      dictionary = Optional.empty();
    }

    boolean writeComputeEnabled = false;

    if (setting.isStoreWriteComputeEnabled && setting.enableWriteCompute) {
      writeComputeEnabled = true;
    }

    // If WriteCompute is enabled, request for intermediate topic
    final boolean finalWriteComputeEnabled = writeComputeEnabled;
    VersionCreationResponse versionCreationResponse = ControllerClient.retryableRequest(
        controllerClient,
        setting.controllerRetries,
        c -> c.requestTopicForWrites(
            setting.storeName,
            inputFileDataSize,
            pushType,
            pushId,
            askControllerToSendControlMessage,
            SORTED,
            finalWriteComputeEnabled,
            Optional.of(partitioners),
            dictionary,
            Optional.ofNullable(setting.sourceGridFabric),
            setting.livenessHeartbeatEnabled,
            setting.rewindTimeInSecondsOverride,
            setting.deferVersionSwap,
            setting.targetedRegions,
            pushJobSetting.repushSourceVersion,
            setting.pushToSeparateRealtimeTopicEnabled));
    if (versionCreationResponse.isError()) {
      if (ErrorType.CONCURRENT_BATCH_PUSH.equals(versionCreationResponse.getErrorType())) {
        LOGGER.error("Unable to run this job since another batch push is running. See the error message for details.");
        updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.CONCURRENT_BATCH_PUSH);
      }
      throw new VeniceException(
          "Failed to create new store version with urls: " + setting.veniceControllerUrl + ", error: "
              + versionCreationResponse.getError());
    } else if (versionCreationResponse.getVersion() == 0) {
      // TODO: Fix the server-side request handling. This should not happen. We should get a 404 instead.
      throw new VeniceException("Got version 0 from: " + versionCreationResponse);
    } else {
      LOGGER.info("Push target version response: {}", versionCreationResponse);
    }

    setting.topic = versionCreationResponse.getKafkaTopic();
    setting.version = versionCreationResponse.getVersion();
    setting.kafkaUrl = versionCreationResponse.getKafkaBootstrapServers();
    setting.partitionCount = versionCreationResponse.getPartitions();
    setting.sslToKafka = versionCreationResponse.isEnableSSL();
    setting.topicCompressionStrategy = versionCreationResponse.getCompressionStrategy();
    setting.partitionerClass = versionCreationResponse.getPartitionerClass();
    setting.partitionerParams = versionCreationResponse.getPartitionerParams();

    setting.chunkingEnabled = setting.isChunkingEnabled && !Version.isRealTimeTopic(setting.topic);
    setting.rmdChunkingEnabled = setting.chunkingEnabled && setting.isRmdChunkingEnabled;
    setting.kafkaSourceRegion = versionCreationResponse.getKafkaSourceRegion();

    if (setting.isSourceKafka) {
      /**
       * Check whether the new version setup is compatible with the source version, and we will check the following configs:
       * 1. Chunking && RMD Chunking.
       * 2. Compression Strategy.
       * 3. Partition Count.
       * 4. Partitioner Config.
       * 5. Incremental Push policy
       * Since right now, the messages from the source topic will be passed through to the new version topic without
       * reformatting, we need to make sure the pass-through messages won't violate the new version config.
       *
       * TODO: maybe we should fail fast before creating a new version.
       */
      Version newVersion = getStoreVersion(setting.storeName, setting.version);
      Version sourceVersion = setting.sourceKafkaInputVersionInfo;

      // Chunked source version cannot be repushed if new version is not chunking enabled.
      if (sourceVersion.isChunkingEnabled() && !newVersion.isChunkingEnabled()) {
        throw new VeniceException(
            "Chunking config mismatch between the source and the new version of store " + setting.storeName
                + ". Source version: " + sourceVersion.getNumber() + " is using: " + sourceVersion.isChunkingEnabled()
                + ", new version: " + newVersion.getNumber() + " is using: " + newVersion.isChunkingEnabled());
      }
      if (sourceVersion.isRmdChunkingEnabled() && !newVersion.isRmdChunkingEnabled()) {
        throw new VeniceException(
            "RMD Chunking config mismatch between the source and the new version of store " + setting.storeName
                + ". Source version: " + sourceVersion.getNumber() + " is using: "
                + sourceVersion.isRmdChunkingEnabled() + ", new version: " + newVersion.getNumber() + " is using: "
                + newVersion.isRmdChunkingEnabled());
      }

      if (sourceVersion.isActiveActiveReplicationEnabled() && newVersion.isActiveActiveReplicationEnabled()
          && sourceVersion.getRmdVersionId() != newVersion.getRmdVersionId()) {
        throw new VeniceException(
            "Replication Metadata Version Id config mismatch between the source version and the new version is "
                + "not supported by Kafka Input Format, source version: " + sourceVersion.getNumber()
                + " is using RMD ID: " + sourceVersion.getRmdVersionId() + ", new version: " + newVersion.getNumber()
                + " is using RMD ID: " + newVersion.getRmdVersionId());
      }
    }
  }

  synchronized VeniceWriter<KafkaKey, byte[], byte[]> getVeniceWriter(PushJobSetting pushJobSetting) {
    if (veniceWriter == null) {
      VeniceWriterFactory veniceWriterFactory = new VeniceWriterFactory(getVeniceWriterProperties(pushJobSetting));
      Properties partitionerProperties = new Properties();
      partitionerProperties.putAll(pushJobSetting.partitionerParams);
      VenicePartitioner partitioner = PartitionUtils
          .getVenicePartitioner(pushJobSetting.partitionerClass, new VeniceProperties(partitionerProperties));

      VeniceWriterOptions vwOptions =
          new VeniceWriterOptions.Builder(pushJobSetting.topic).setUseKafkaKeySerializer(true)
              .setPartitioner(partitioner)
              .setPartitionCount(pushJobSetting.partitionCount)
              .setMaxRecordSizeBytes(pushJobSetting.maxRecordSizeBytes)
              .build();
      VeniceWriter<KafkaKey, byte[], byte[]> newVeniceWriter = veniceWriterFactory.createVeniceWriter(vwOptions);
      LOGGER.info("Created VeniceWriter: {}", newVeniceWriter);
      veniceWriter = newVeniceWriter;
    }
    return veniceWriter;
  }

  private synchronized Properties getVeniceWriterProperties(PushJobSetting pushJobSetting) {
    if (veniceWriterProperties == null) {
      veniceWriterProperties = createVeniceWriterProperties(pushJobSetting.kafkaUrl, pushJobSetting.sslToKafka);
    }
    return veniceWriterProperties;
  }

  private Properties createVeniceWriterProperties(String kafkaUrl, boolean sslToKafka) {
    Properties veniceWriterProperties = new Properties();
    veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, kafkaUrl);
    veniceWriterProperties.put(VeniceWriter.MAX_ELAPSED_TIME_FOR_SEGMENT_IN_MS, -1);
    if (props.containsKey(VeniceWriter.CLOSE_TIMEOUT_MS)) { /* Writer uses default if not specified */
      veniceWriterProperties.put(VeniceWriter.CLOSE_TIMEOUT_MS, props.getInt(VeniceWriter.CLOSE_TIMEOUT_MS));
    }
    if (sslToKafka) {
      veniceWriterProperties.putAll(sslProperties.get());
    }
    veniceWriterProperties.setProperty(
        KAFKA_PRODUCER_REQUEST_TIMEOUT_MS,
        props.getString(KAFKA_PRODUCER_REQUEST_TIMEOUT_MS, Integer.toString(Integer.MAX_VALUE)));
    veniceWriterProperties.setProperty(
        KAFKA_PRODUCER_RETRIES_CONFIG,
        props.getString(KAFKA_PRODUCER_RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE)));
    veniceWriterProperties.setProperty(
        KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS,
        props.getString(KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS, Integer.toString(Integer.MAX_VALUE)));
    return veniceWriterProperties;
  }

  private synchronized void closeVeniceWriter() {
    if (veniceWriter != null) {
      veniceWriter.close();
      veniceWriter = null;
    }
  }

  static ExecutionStatus getExecutionStatusFromControllerResponse(JobStatusQueryResponse response) {
    ExecutionStatus status;
    try {
      status = ExecutionStatus.valueOf(response.getStatus());
    } catch (IllegalArgumentException e) {
      StringBuilder errorMsg = new StringBuilder().append("Invalid ExecutionStatus returned from backend. status: ")
          .append(response.getStatus());
      if (response.getOptionalExtraDetails().isPresent()) {
        errorMsg.append(", extra details: ").append(response.getOptionalExtraDetails().get());
      }
      LOGGER.error(errorMsg.toString());
      throw new VeniceException(errorMsg.toString(), e);
    }
    return status;
  }

  /**
   * High level, we want to poll the consumption job status until it errors or is complete. This is more complicated
   * because we might be dealing with multiple destination clusters and we might not be able to reach all of them. We
   * are using a semantic of "poll until all accessible datacenters report success".
   * <p>
   * If any datacenter report an explicit error status, we throw an exception and fail the job. However, datacenters
   * with COMPLETED status will be serving new data.
   *
   * @param incrementalPushVersion, incremental push version
   * @param controllerClient,       controller client to send query to poll status
   * @param pushJobSetting,         push job setting
   * @param targetedRegions         if specified, only poll the status of the specified regions, otherwise it can be
   *                                null
   * @param isTargetedRegionPush
   */
  void pollStatusUntilComplete(
      String incrementalPushVersion,
      ControllerClient controllerClient,
      PushJobSetting pushJobSetting,
      String targetedRegions,
      boolean isTargetedRegionPush) {
    // Set of datacenters that have reported a completed status at least once.
    Set<String> completedDatacenters = new HashSet<>();
    // Datacenter-specific details. Stored in memory to avoid printing repetitive details.
    Map<String, String> previousExtraDetails = new HashMap<>();
    // Overall job details. Stored in memory to avoid printing repetitive details.
    String previousOverallDetails = null;
    // Perform a poll first in case the job has already finished before taking breaks between polls.
    long nextPollingTime = 0;
    /**
     * The start time when some data centers enter unknown state;
     * if 0, it means no data center is in unknown state.
     *
     * Once enter unknown state, it's allowed to stay in unknown state for
     * no more than {@link DEFAULT_JOB_STATUS_IN_UNKNOWN_STATE_TIMEOUT_MS}.
     */
    long unknownStateStartTimeMs = 0;
    long pollStartTimeMs = System.currentTimeMillis();

    String topicToMonitor = getTopicToMonitor(pushJobSetting);

    List<ExecutionStatus> successfulStatuses =
        Arrays.asList(ExecutionStatus.COMPLETED, ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED);

    for (;;) {
      long currentTime = System.currentTimeMillis();
      if (currentTime < nextPollingTime) {
        if (!Utils.sleep(nextPollingTime - currentTime)) {
          throw new VeniceException("Job status polling was interrupted!");
        }
      }
      nextPollingTime = currentTime + pushJobSetting.pollJobStatusIntervalMs;

      JobStatusQueryResponse response = ControllerClient.retryableRequest(
          controllerClient,
          pushJobSetting.controllerStatusPollRetries,
          client -> client
              .queryOverallJobStatus(topicToMonitor, Optional.ofNullable(incrementalPushVersion), targetedRegions));

      if (response.isError()) {
        // status could not be queried which could be due to a communication error.
        throw new VeniceException(
            "Failed to connect to: " + pushJobSetting.veniceControllerUrl + " to query job status, after "
                + pushJobSetting.controllerStatusPollRetries + " attempts. Error: " + response.getError());
      }

      previousOverallDetails = printJobStatus(response, previousOverallDetails, previousExtraDetails);
      ExecutionStatus overallStatus = getExecutionStatusFromControllerResponse(response);
      Map<String, String> regionSpecificInfo = response.getExtraInfo();
      // Note that it's intended to update the push job details before updating the completed datacenter set.
      updatePushJobDetailsWithColoStatus(regionSpecificInfo, completedDatacenters);
      regionSpecificInfo.forEach((region, regionStatus) -> {
        ExecutionStatus datacenterStatus = ExecutionStatus.valueOf(regionStatus);
        if (datacenterStatus.isTerminal() && !datacenterStatus.isError()) {
          completedDatacenters.add(region);
        }
      });
      if (overallStatus.isTerminal()) {
        if (completedDatacenters.size() != regionSpecificInfo.size() || !successfulStatuses.contains(overallStatus)) {
          // 1) For regular push, one or more DC could have an UNKNOWN status and never successfully reported a
          // completed status before,
          // but if the majority of datacenters have completed, we give up on the unreachable datacenter
          // and start truncating the data topic.
          // 2) For targeted region push, not all targeted regions have completed.

          StringBuilder errorMsg = new StringBuilder().append("Push job error reported by controller: ")
              .append(pushJobSetting.veniceControllerUrl)
              .append("\ncontroller response: ")
              .append(response);
          if (overallStatus.isDVCIngestionError()) {
            this.pushJobDetails.pushJobLatestCheckpoint =
                PushJobCheckpoints.valueOf(overallStatus.toString()).getValue();
          }
          throw new VeniceException(errorMsg.toString());
        }

        // Every known datacenter have successfully reported a completed status at least once.
        if (isTargetedRegionPush) {
          LOGGER.info("Successfully pushed {} to targeted region {}", pushJobSetting.topic, targetedRegions);
        } else {
          LOGGER.info("Successfully pushed {} to all the regions", pushJobSetting.topic);
        }
        return;
      }
      long bootstrapToOnlineTimeoutInHours =
          VenicePushJob.this.pushJobSetting.storeResponse.getStore().getBootstrapToOnlineTimeoutInHours();
      long durationMs = LatencyUtils.getElapsedTimeFromMsToMs(pollStartTimeMs);
      if (durationMs > TimeUnit.HOURS.toMillis(bootstrapToOnlineTimeoutInHours)) {
        throw new VeniceException(
            "Failing push-job for store " + VenicePushJob.this.pushJobSetting.storeResponse.getName()
                + " which is still running after " + TimeUnit.MILLISECONDS.toHours(durationMs) + " hours.");
      }
      if (!overallStatus.equals(ExecutionStatus.UNKNOWN)) {
        unknownStateStartTimeMs = 0;
      } else if (unknownStateStartTimeMs == 0) {
        unknownStateStartTimeMs = System.currentTimeMillis();
      } else if (LatencyUtils
          .getElapsedTimeFromMsToMs(unknownStateStartTimeMs) < pushJobSetting.jobStatusInUnknownStateTimeoutMs) {
        double elapsedMinutes =
            ((double) LatencyUtils.getElapsedTimeFromMsToMs(unknownStateStartTimeMs)) / Time.MS_PER_MINUTE;
        LOGGER.warn("Some data centers are still in unknown state after waiting for {} minutes", elapsedMinutes);
      } else {
        long timeoutMinutes = pushJobSetting.jobStatusInUnknownStateTimeoutMs / Time.MS_PER_MINUTE;
        throw new VeniceException(
            "After waiting for " + timeoutMinutes + " minutes; push job is still in unknown state.");
      }

      // Only send the push job details after all error checks have passed and job is not completed yet.
      sendPushJobDetailsToController();
    }
  }

  private String printJobStatus(
      JobStatusQueryResponse response,
      String previousOverallDetails,
      Map<String, String> previousExtraDetails) {
    String newOverallDetails = previousOverallDetails;
    Map<String, String> datacenterSpecificInfo = response.getExtraInfo();
    if (datacenterSpecificInfo != null && !datacenterSpecificInfo.isEmpty()) {
      LOGGER.info("Specific status: {}", datacenterSpecificInfo);
    }

    Optional<String> details = response.getOptionalStatusDetails();
    if (details.isPresent() && detailsAreDifferent(previousOverallDetails, details.get())) {
      if (!details.get().isEmpty()) {
        LOGGER.info("\t\tNew overall details: {}", details.get());
      }
      newOverallDetails = details.get();
    }

    Optional<Map<String, String>> extraDetails = response.getOptionalExtraDetails();
    if (extraDetails.isPresent()) {
      // Non-upgraded controllers will not provide these details, in which case, this will be null.
      extraDetails.get().forEach((region, currentDetails) -> {
        String previous = previousExtraDetails.get(region);

        if (detailsAreDifferent(previous, currentDetails)) {
          LOGGER.info("\t\tNew specific details for {}: {}", region, currentDetails);
          previousExtraDetails.put(region, currentDetails);
        }
      });
    }
    return newOverallDetails;
  }

  /**
   * @return true if the details are different
   */
  private boolean detailsAreDifferent(String previous, String current) {
    // Criteria for printing the current details:
    boolean detailsPresentWhenPreviouslyAbsent = (previous == null && current != null);
    boolean detailsDifferentFromPreviously = (previous != null && !previous.equals(current));
    return detailsPresentWhenPreviouslyAbsent || detailsDifferentFromPreviously;
  }

  /**
   * Invoke a mapper only MR to do the below tasks:
   * 1. Schema validation (whether the schema in all files is same as the first file which is
   * already validated with the store schema)
   * 2. Build dictionary for compression (if enabled)
   * @throws IOException
   */
  private void validateSchemaAndBuildDict(
      JobConf conf,
      PushJobSetting pushJobSetting,
      VeniceProperties props,
      String id,
      String inputDirectory) throws Exception {
    setupMRConfToValidateSchemaAndBuildDict(conf, pushJobSetting, props, id, inputDirectory);
    runValidateSchemaAndBuildDictJobAndUpdateStatus(conf);
  }

  /**
   * Set up MR config to validate Schema and Build Dictionary
   * @param conf MR Job Configuration
   * @param id Job Id
   */
  private void setupMRConfToValidateSchemaAndBuildDict(
      JobConf conf,
      PushJobSetting pushJobSetting,
      VeniceProperties props,
      String id,
      String inputDirectory) {
    setupDefaultJobConfToValidateSchemaAndBuildDict(conf, pushJobSetting, props, id);
    setupInputFormatConfToValidateSchemaAndBuildDict(conf, pushJobSetting, inputDirectory);
  }

  /**
   * Default config includes the details related to jobids, output formats, compression configs, ssl configs, etc.
   *
   * @param conf
   * @param id
   */
  private void setupDefaultJobConfToValidateSchemaAndBuildDict(
      JobConf conf,
      PushJobSetting pushJobSetting,
      VeniceProperties props,
      String id) {
    JobUtils.setupCommonJobConf(
        props,
        conf,
        id + ":venice_push_job_validate_schema_and_build_dict-" + pushJobSetting.storeName,
        pushJobSetting);
    conf.set(VENICE_STORE_NAME_PROP, pushJobSetting.storeName);
    if (pushJobSetting.etlValueSchemaTransformation != null) {
      conf.set(ETL_VALUE_SCHEMA_TRANSFORMATION, pushJobSetting.etlValueSchemaTransformation.name());
    }
    conf.setBoolean(INCREMENTAL_PUSH, pushJobSetting.isIncrementalPush);
    conf.set(INPUT_PATH_LAST_MODIFIED_TIME, Long.toString(inputDataInfo.getInputModificationTime()));

    /** Compression related config */
    conf.setInt(
        COMPRESSION_DICTIONARY_SIZE_LIMIT,
        props.getInt(
            COMPRESSION_DICTIONARY_SIZE_LIMIT,
            VeniceWriter.DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES));
    conf.setInt(
        COMPRESSION_DICTIONARY_SAMPLE_SIZE,
        props.getInt(COMPRESSION_DICTIONARY_SAMPLE_SIZE, DEFAULT_COMPRESSION_DICTIONARY_SAMPLE_SIZE));
    // USE_MAPPER_TO_BUILD_DICTIONARY is still needed to be passed here for validateInputAndGetInfo
    conf.setBoolean(USE_MAPPER_TO_BUILD_DICTIONARY, pushJobSetting.useMapperToBuildDict);
    conf.set(COMPRESSION_STRATEGY, VenicePushJob.this.pushJobSetting.storeCompressionStrategy.toString());
    conf.set(
        VALIDATE_SCHEMA_AND_BUILD_DICT_MAPPER_OUTPUT_DIRECTORY,
        validateSchemaAndBuildDictMapperOutputDirectory.toUri().getPath());

    /** adding below for {@link AbstractDataWriterTask.configure(EngineTaskConfigProvider)} to not crash: Doesn't affect this flow */
    conf.setBoolean(VeniceWriter.ENABLE_CHUNKING, false);

    /** Allow overriding properties if their names start with {@link HADOOP_VALIDATE_SCHEMA_AND_BUILD_DICT_PREFIX} */
    for (String key: props.keySet()) {
      String lowerCase = key.toLowerCase();
      if (lowerCase.startsWith(HADOOP_VALIDATE_SCHEMA_AND_BUILD_DICT_PREFIX)) {
        String overrideKey = key.substring(HADOOP_VALIDATE_SCHEMA_AND_BUILD_DICT_PREFIX.length());
        conf.set(overrideKey, props.getString(key));
      }
    }

    conf.setInt(PARTITION_COUNT, 0);
    conf.setNumReduceTasks(0);
  }

  protected void setupInputFormatConfToValidateSchemaAndBuildDict(
      JobConf conf,
      PushJobSetting pushJobSetting,
      String inputDirectory) {
    conf.set(INPUT_PATH_PROP, inputDirectory);

    conf.setInputFormat(VeniceFileInputFormat.class);
    conf.setMapperClass(ValidateSchemaAndBuildDictMapper.class);

    AvroJob.setOutputSchema(conf, ValidateSchemaAndBuildDictMapperOutput.getClassSchema());
    conf.setOutputFormat(ValidateSchemaAndBuildDictOutputFormat.class);

    /** key/value fields to be used in {@link DefaultInputDataInfoProvider#validateInputAndGetInfo(String)} in the mapper
     * These values were populated to schemaInfo in the same function but in driver */
    conf.set(KEY_FIELD_PROP, pushJobSetting.keyField);
    conf.set(VALUE_FIELD_PROP, pushJobSetting.valueField);
  }

  /**
   * Query the controller to retrieve a specific version
   * @param storeName
   * @param version
   * @return
   */
  private Version getStoreVersion(String storeName, int version) {
    StoreResponse storeResponse = getStoreResponse(storeName, true);
    Optional<Version> newVersion = storeResponse.getStore().getVersion(version);
    if (!newVersion.isPresent()) {
      throw new VeniceException(
          "Couldn't fetch the newly created version: " + version + " for store: " + storeName + " with urls: "
              + pushJobSetting.veniceControllerUrl);
    }

    return newVersion.get();
  }

  private StoreResponse getStoreResponse(String storeName) {
    return getStoreResponse(storeName, false);
  }

  /**
   * Get the previously cached {@link StoreResponse} if available, otherwise query the controller.
   * It's unlikely that a store configuration would change during a VenicePushJob so when we need to access the configuration
   * of a store, it's recommended to access them from the cached {@link StoreResponse} to avoid unnecessary controller
   * calls unless you need to access up-to-date {@link Version} from the controller.
   * @param storeName, the store name
   * @param refresh, if true, query the controller to get the latest store response otherwise return the cached one.
   * @return the cached {@link StoreResponse} or the newly fetched {@link StoreResponse} when refresh is true.
   */
  private StoreResponse getStoreResponse(String storeName, boolean refresh) {
    // If this is the first time getting the StoreResponse, fetch it despite the "refresh" argument
    if (pushJobSetting.storeResponse == null) {
      refresh = true;
    }
    if (refresh) {
      StoreResponse storeResponse = ControllerClient
          .retryableRequest(controllerClient, pushJobSetting.controllerRetries, c -> c.getStore(storeName));
      if (storeResponse.isError()) {
        throw new VeniceException("Can't get store info. " + storeResponse.getError());
      }
      pushJobSetting.storeResponse = storeResponse;
      pushJobSetting.isSchemaAutoRegisterFromPushJobEnabled =
          storeResponse.getStore().isSchemaAutoRegisterFromPushJobEnabled();
      pushJobSetting.isChunkingEnabled = storeResponse.getStore().isChunkingEnabled();
      pushJobSetting.isRmdChunkingEnabled = storeResponse.getStore().isRmdChunkingEnabled();
      pushJobSetting.storeCompressionStrategy = storeResponse.getStore().getCompressionStrategy();
      pushJobSetting.isStoreWriteComputeEnabled = storeResponse.getStore().isWriteComputationEnabled();
      pushJobSetting.isStoreIncrementalPushEnabled = storeResponse.getStore().isIncrementalPushEnabled();
      pushJobSetting.hybridStoreConfig = storeResponse.getStore().getHybridStoreConfig();
      pushJobSetting.maxRecordSizeBytes = storeResponse.getStore().getMaxRecordSizeBytes();
      final boolean isRepush = pushJobSetting.isSourceKafka || pushJobSetting.isSourceETL;
      if (isRepush && pushJobSetting.maxRecordSizeBytes != VeniceWriter.UNLIMITED_MAX_RECORD_SIZE) {
        pushJobSetting.maxRecordSizeBytes = VeniceWriter.UNLIMITED_MAX_RECORD_SIZE; // safer to allow on repush
        final String repushJobType = (pushJobSetting.isSourceKafka) ? "Kafka" : "ETL";
        LOGGER.info("Setting max record size to unlimited for {} repush job", repushJobType);
      }
    }
    return pushJobSetting.storeResponse;
  }

  private void logPushJobProperties(PushJobSetting pushJobSetting, String inputDirectory, long inputFileDataSize) {
    LOGGER.info(pushJobPropertiesToString(pushJobSetting, inputDirectory, inputFileDataSize));
  }

  private String pushJobPropertiesToString(
      PushJobSetting pushJobSetting,
      String inputDirectory,
      final long inputFileDataSize) {
    List<String> propKeyValuePairs = new ArrayList<>();
    propKeyValuePairs.add("Job ID: " + this.jobId);
    propKeyValuePairs.add("Kafka URL: " + pushJobSetting.kafkaUrl);
    propKeyValuePairs.add("Kafka Topic: " + pushJobSetting.topic);
    propKeyValuePairs.add("Kafka topic partition count: " + pushJobSetting.partitionCount);
    propKeyValuePairs.add("Kafka Queue Bytes: " + pushJobSetting.batchNumBytes);
    propKeyValuePairs.add("Input Directory: " + inputDirectory);
    propKeyValuePairs.add("Venice Store Name: " + pushJobSetting.storeName);
    propKeyValuePairs.add("Venice Cluster Name: " + pushJobSetting.clusterName);
    propKeyValuePairs.add("Venice URL: " + pushJobSetting.veniceControllerUrl);
    if (pushJobSetting.inputDataSchemaString != null) {
      propKeyValuePairs.add("File Schema: " + pushJobSetting.inputDataSchemaString);
      propKeyValuePairs.add("Avro key schema: " + pushJobSetting.keySchemaString);
      propKeyValuePairs.add("Avro value schema: " + pushJobSetting.valueSchemaString);
    }
    propKeyValuePairs.add(
        "Total input data file size: " + ((double) inputFileDataSize / 1024 / 1024)
            + " MB. This could be the size of compressed data if the underlying filesystem compresses it");
    propKeyValuePairs.add("Max Venice Record Size: " + pushJobSetting.maxRecordSizeBytes);
    propKeyValuePairs.add("Is Chunking Enabled: " + pushJobSetting.chunkingEnabled);
    propKeyValuePairs.add("Is Replication Metadata Chunking Enabled: " + pushJobSetting.rmdChunkingEnabled);
    propKeyValuePairs.add("Is incremental push: " + pushJobSetting.isIncrementalPush);
    propKeyValuePairs.add("Is duplicated key allowed: " + pushJobSetting.isDuplicateKeyAllowed);
    propKeyValuePairs.add("Is source ETL data: " + pushJobSetting.isSourceETL);
    propKeyValuePairs.add("ETL value schema transformation : " + pushJobSetting.etlValueSchemaTransformation);
    propKeyValuePairs.add("Is Kafka Input Format: " + pushJobSetting.isSourceKafka);
    if (pushJobSetting.isSourceKafka) {
      propKeyValuePairs.add("Kafka Input broker urls: " + pushJobSetting.kafkaInputBrokerUrl);
      propKeyValuePairs.add("Kafka Input topic name: " + pushJobSetting.kafkaInputTopic);
    }
    return String.join(Utils.NEW_LINE_CHAR, propKeyValuePairs);
  }

  /**
   * A cancel method for graceful cancellation of the running Job to be invoked as a result of user actions.
   *
   * @throws Exception
   */
  public void cancel() {
    killJob(pushJobSetting, controllerClient);
    if (StringUtils.isEmpty(pushJobSetting.topic)) {
      pushJobDetails.overallStatus.add(getPushJobDetailsStatusTuple(PushJobDetailsStatus.ERROR.getValue()));
    } else {
      pushJobDetails.overallStatus.add(getPushJobDetailsStatusTuple(PushJobDetailsStatus.KILLED.getValue()));
    }
    pushJobDetails.jobDurationInMs = LatencyUtils.getElapsedTimeFromMsToMs(pushJobSetting.jobStartTimeMs);
    sendPushJobDetailsToController();
  }

  private void killJob(PushJobSetting pushJobSetting, ControllerClient controllerClient) {
    // Attempting to kill job. There's a race condition, but meh. Better kill when you know it's running
    killComputeJob();
    if (!pushJobSetting.isIncrementalPush) {
      final int maxRetryAttempt = 10;
      int currentRetryAttempt = 0;
      while (currentRetryAttempt < maxRetryAttempt) {
        if (!StringUtils.isEmpty(pushJobSetting.topic)) {
          break;
        }
        Utils.sleep(Duration.ofMillis(10).toMillis());
        currentRetryAttempt++;
      }
      if (StringUtils.isEmpty(pushJobSetting.topic)) {
        LOGGER.error("Could not find a store version to delete for store: {}", pushJobSetting.storeName);
      } else {
        ControllerClient.retryableRequest(
            controllerClient,
            pushJobSetting.controllerRetries,
            c -> c.killOfflinePushJob(pushJobSetting.topic));
        LOGGER.info("Offline push job has been killed, topic: {}", pushJobSetting.topic);
      }
    }
  }

  private void killComputeJob() {
    killMRJob();
    killDataWriterJob();
  }

  private void killMRJob() {
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
      runningJob.killJob();
    } catch (Exception ex) {
      // Will try to kill Venice Offline Push Job no matter whether map-reduce job kill throws an exception or not.
      LOGGER.info(
          "Received exception while killing map-reduce job with name {} and ID {}",
          runningJob.getJobName(),
          runningJob.getID().getId(),
          ex);
    }
  }

  private void killDataWriterJob() {
    if (dataWriterComputeJob == null) {
      LOGGER.warn("No op to kill a null data writer job");
      return;
    }
    try {
      ComputeJob.Status jobStatus = dataWriterComputeJob.getStatus();
      if (jobStatus.isTerminal()) {
        LOGGER.warn("No op to kill a compute job in a terminal state: {}", jobStatus);
        return;
      }
      dataWriterComputeJob.kill();
    } catch (Exception ex) {
      // Will try to kill Venice Offline Push Job no matter whether the compute job kill throws an exception or not.
      LOGGER.info("Received exception while killing data writer job", ex);
    }
  }

  // Visible for testing
  static Path getLatestPathOfInputDirectory(String inputDirectory, FileSystem fs) throws IOException {
    String[] split = inputDirectory.split("#LATEST");

    String resolvedPath = split[0];
    for (int i = 1; i < split.length; i++) {
      resolvedPath = getLatestPath(new Path(resolvedPath), fs).toString() + split[i];
    }

    if (inputDirectory.endsWith("#LATEST")) {
      return getLatestPath(new Path(resolvedPath), fs);
    }

    return new Path(resolvedPath);
  }

  // Visible for testing
  public String getKafkaUrl() {
    return pushJobSetting.kafkaUrl;
  }

  // Visible for testing
  public String getIncrementalPushVersion() {
    return pushJobSetting.incrementalPushVersion;
  }

  // Visible for testing
  public String getTopicToMonitor() {
    if (pushJobSetting == null || pushJobSetting.topic == null) {
      throw new VeniceException("The push job is not initialized yet");
    }
    return getTopicToMonitor(this.pushJobSetting);
  }

  private String getTopicToMonitor(PushJobSetting pushJobSetting) {
    return Version.isRealTimeTopic(pushJobSetting.topic)
        ? Version.composeKafkaTopic(pushJobSetting.storeName, pushJobSetting.version)
        : pushJobSetting.topic;
  }

  private static Path getLatestPath(Path path, FileSystem fs) throws IOException {
    FileStatus[] statuses = fs.listStatus(path, PATH_FILTER);

    if (statuses.length != 0) {
      Arrays.sort(statuses);
      for (int i = statuses.length - 1; i >= 0; i--) {
        if (statuses[i].isDirectory()) {
          return statuses[i].getPath();
        }
      }
    }
    return path;
  }

  @Override
  public void close() {
    closeVeniceWriter();
    Utils.closeQuietlyWithErrorLogged(dataWriterComputeJob);
    Utils.closeQuietlyWithErrorLogged(controllerClient);
    Utils.closeQuietlyWithErrorLogged(kmeSchemaSystemStoreControllerClient);
    Utils.closeQuietlyWithErrorLogged(livenessHeartbeatStoreControllerClient);
    try {
      jobTmpDir.getFileSystem(new Configuration()).delete(jobTmpDir, true);
    } catch (IOException e) {
      LOGGER.warn("Failed to delete temp directory: {}", jobTmpDir);
    }
  }

  public static void main(String[] args) {

    if (args.length != 1) {
      Utils.exit("USAGE: java -jar venice-push-job-all.jar <VPJ_config_file_path>");
    }
    Properties properties = new Properties();
    try {
      properties.load(new FileReader(args[0]));
    } catch (IOException e) {
      e.printStackTrace();
      Utils.exit("Unable to read config file");
    }

    runPushJob("Venice Push Job", properties);
    Utils.exit("Venice Push Job Completed");
  }

  private static void runPushJob(String jobId, Properties props) {
    try (VenicePushJob job = new VenicePushJob(jobId, props)) {
      job.run();
    }
  }

  // used only for testing
  void setDataWriterComputeJob(DataWriterComputeJob dataWriterComputeJob) {
    this.dataWriterComputeJob = dataWriterComputeJob;
  }

  void setInputStorageQuotaTracker(InputStorageQuotaTracker inputStorageQuotaTracker) {
    this.inputStorageQuotaTracker = inputStorageQuotaTracker;
  }

  PushJobDetails getPushJobDetails() {
    return pushJobDetails;
  }
}
