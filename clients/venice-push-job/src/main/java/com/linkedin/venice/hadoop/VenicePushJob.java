package com.linkedin.venice.hadoop;

import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
import static com.linkedin.venice.CommonConfigKeys.SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.ConfigKeys.AMPLIFICATION_FACTOR;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.PARTITIONER_CLASS;
import static com.linkedin.venice.VeniceConstants.DEFAULT_SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.status.BatchJobHeartbeatConfigs.HEARTBEAT_ENABLED_CONFIG;
import static com.linkedin.venice.status.BatchJobHeartbeatConfigs.HEARTBEAT_STORE_NAME_CONFIG;
import static com.linkedin.venice.utils.ByteUtils.generateHumanReadableByteCountString;
import static org.apache.hadoop.mapreduce.MRJobConfig.MAPREDUCE_JOB_CLASSLOADER;
import static org.apache.hadoop.mapreduce.MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY;
import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

import com.github.luben.zstd.Zstd;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.RepushInfoResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.etl.ETLValueSchemaTransformation;
import com.linkedin.venice.exceptions.ErrorType;
import com.linkedin.venice.exceptions.TopicAuthorizationVeniceException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.heartbeat.DefaultPushJobHeartbeatSenderFactory;
import com.linkedin.venice.hadoop.heartbeat.NoOpPushJobHeartbeatSender;
import com.linkedin.venice.hadoop.heartbeat.NoOpPushJobHeartbeatSenderFactory;
import com.linkedin.venice.hadoop.heartbeat.PushJobHeartbeatSender;
import com.linkedin.venice.hadoop.heartbeat.PushJobHeartbeatSenderFactory;
import com.linkedin.venice.hadoop.input.kafka.KafkaInputFormat;
import com.linkedin.venice.hadoop.input.kafka.KafkaInputFormatCombiner;
import com.linkedin.venice.hadoop.input.kafka.KafkaInputRecordReader;
import com.linkedin.venice.hadoop.input.kafka.VeniceKafkaInputMapper;
import com.linkedin.venice.hadoop.input.kafka.VeniceKafkaInputReducer;
import com.linkedin.venice.hadoop.input.kafka.ttl.TTLResolutionPolicy;
import com.linkedin.venice.hadoop.pbnj.PostBulkLoadAnalysisMapper;
import com.linkedin.venice.hadoop.ssl.SSLConfigurator;
import com.linkedin.venice.hadoop.ssl.TempFileSSLConfigurator;
import com.linkedin.venice.hadoop.ssl.UserCredentialsFactory;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.schema.AvroSchemaParseUtils;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.status.PushJobDetailsStatus;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobDetailsStatusTuple;
import com.linkedin.venice.utils.DictionaryUtils;
import com.linkedin.venice.utils.EncodingUtils;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.ApacheKafkaProducer;
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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class sets up the Hadoop job used to push data to Venice.
 * The job reads the input data off HDFS. It supports 2 kinds of
 * input -- Avro / Binary Json (Vson).
 */
public class VenicePushJob implements AutoCloseable {
  // Avro input configs
  public static final String LEGACY_AVRO_KEY_FIELD_PROP = "avro.key.field";
  public static final String LEGACY_AVRO_VALUE_FIELD_PROP = "avro.value.field";

  public static final String KEY_FIELD_PROP = "key.field";
  public static final String VALUE_FIELD_PROP = "value.field";
  public static final String SCHEMA_STRING_PROP = "schema";
  public static final String KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP = "kafka.source.key.schema";
  public static final String EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED = "extended.schema.validity.check.enabled";
  public static final Boolean DEFAULT_EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED = true;

  // Vson input configs
  // Vson files store key/value schema on file header. key / value fields are optional
  // and should be specified only when key / value schema is the partial of the files.
  public static final String FILE_KEY_SCHEMA = "key.schema";
  public static final String FILE_VALUE_SCHEMA = "value.schema";
  public static final String INCREMENTAL_PUSH = "incremental.push";

  // veniceReducer will not fail fast and override the previous key if this is true and duplicate keys incur.
  public static final String ALLOW_DUPLICATE_KEY = "allow.duplicate.key";

  public static final String KAFKA_PRODUCER_REQUEST_TIMEOUT_MS =
      ApacheKafkaProducer.PROPERTIES_KAFKA_PREFIX + ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG; // kafka.request.timeout.ms
  public static final String KAFKA_PRODUCER_RETRIES_CONFIG =
      ApacheKafkaProducer.PROPERTIES_KAFKA_PREFIX + ProducerConfig.RETRIES_CONFIG; // kafka.retries
  public static final String KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS =
      ApacheKafkaProducer.PROPERTIES_KAFKA_PREFIX + ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
  public static final String POLL_STATUS_RETRY_ATTEMPTS = "poll.status.retry.attempts";
  public static final String CONTROLLER_REQUEST_RETRY_ATTEMPTS = "controller.request.retry.attempts";
  public static final String POLL_JOB_STATUS_INTERVAL_MS = "poll.job.status.interval.ms";
  public static final String JOB_STATUS_IN_UNKNOWN_STATE_TIMEOUT_MS = "job.status.in.unknown.state.timeout.ms";
  public static final String SEND_CONTROL_MESSAGES_DIRECTLY = "send.control.messages.directly";
  public static final String SOURCE_ETL = "source.etl";
  public static final String ETL_VALUE_SCHEMA_TRANSFORMATION = "etl.value.schema.transformation";

  /**
   *  Enabling/Disabling the feature to collect extra metrics wrt compression ratios
   *  as some stores might disable this feature. Even if disabled, the code flow will
   *  follow the new code of using a mapper to validate schema, etc.
   */
  public static final String COMPRESSION_METRIC_COLLECTION_ENABLED = "compression.metric.collection.enabled";
  public static final boolean DEFAULT_COMPRESSION_METRIC_COLLECTION_ENABLED = false;

  /**
   * Temporary flag to enable/disable the code changes until the flow of using mapper
   * to validate schema and build dictionary is stable.
   */
  public static final String USE_MAPPER_TO_BUILD_DICTIONARY = "use.mapper.to.build.dictionary";
  public static final boolean DEFAULT_USE_MAPPER_TO_BUILD_DICTIONARY = false;

  /**
   * Configs used to enable Kafka Input.
   */
  public static final String SOURCE_KAFKA = "source.kafka";
  /**
   * TODO: consider to automatically discover the source topic for the specified store.
   * We need to be careful in the following scenarios:
   * 1. Not all the prod colos are using the same current version if the previous push experiences a partial failure.
   * 2. We might want to re-push from a backup version, which should be unlikely.
   */
  public static final String KAFKA_INPUT_TOPIC = "kafka.input.topic";
  public static final String KAFKA_INPUT_FABRIC = "kafka.input.fabric";
  public static final String KAFKA_INPUT_BROKER_URL = "kafka.input.broker.url";
  // Optional
  public static final String KAFKA_INPUT_MAX_RECORDS_PER_MAPPER = "kafka.input.max.records.per.mapper";
  public static final String KAFKA_INPUT_COMBINER_ENABLED = "kafka.input.combiner.enabled";
  /**
   * Optional.
   * If we want to use a different rewind time from the default store-level rewind time config for Kafka Input re-push,
   * the following property needs to specified explicitly.
   *
   * This property comes to play when the default rewind time configured in store-level is too short or too long.
   * 1. If the default rewind time config is too short (for example 0 or several mins), it could cause data gap with
   * re-push since the push job itself could take several hours, and we would like to make sure the re-pushed version
   * will contain the same dataset as the source version.
   * 2. If the default rewind time config is too long (such as 28 days), it will be a big waste to rewind so much time
   * since the time gap between the source version and the re-push version should be comparable to the re-push time
   * if the whole ingestion pipeline is not lagging.
   *
   * There are some challenges to automatically detect the right rewind time for re-push because of the following reasons:
   * 1. For Venice Aggregate use case, some colo could be lagging behind other prod colos, so if the re-push source is
   * from a fast colo, too short rewind time could cause a data gap in the slower colos. Ideally, it is good to use
   * the slowest colo as the re-push source.
   * 2. For Venice non-Aggregate use case, the ingestion pipeline will include the following several phases:
   * 2.1 Customer's Kafka aggregation and mirroring pipeline to replicate the same data to all prod colos.
   * 2.2 Venice Ingestion pipeline to consume the local real-time topic.
   * We have visibility to 2.2, but not 2.1, so we may need to work with customer to understand how 2.1 can be measured
   * or use a long enough rewind time to mitigate all the potential issues.
   *
   * Make this property available in generic since it should be useful for ETL+VPJ use case as well.
   */
  public static final String REWIND_TIME_IN_SECONDS_OVERRIDE = "rewind.time.in.seconds.override";

  /**
   * A time stamp specified to rewind to before replaying data.  This config is ignored if rewind.time.in.seconds.override
   * is provided.  This config at time of push will be leveraged to fill in the rewind.time.in.seconds.override by taking
   * System.currentTime - rewind.epoch.time.in.seconds.override and storing the result in rewind.time.in.seconds.override.
   * With this in mind, a push policy of REWIND_FROM_SOP should be used in order to get a behavior that makes sense to a user.
   * A timestamp that is in the future is not valid and will result in an exception.
   */
  public static final String REWIND_EPOCH_TIME_IN_SECONDS_OVERRIDE = "rewind.epoch.time.in.seconds.override";

  /**
   * This config is a boolean which suppresses submitting the end of push message after data has been sent and does
   * not poll for the status of the job to complete.  Using this flag means that a user must manually mark the job success
   * or failed.
   */
  public static final String SUPPRESS_END_OF_PUSH_MESSAGE = "suppress.end.of.push.message";

  public static final String DEFER_VERSION_SWAP = "defer.version.swap";

  /**
   * Relates to the above argument.  An overridable amount of buffer to be applied to the epoch (as the rewind isn't
   * perfectly instantaneous).  Defaults to 1 minute.
   */
  public static final String REWIND_EPOCH_TIME_BUFFER_IN_SECONDS_OVERRIDE =
      "rewind.epoch.time.buffer.in.seconds.override";

  /**
   * In single-colo mode, this can be either a controller or router.
   * In multi-colo mode, it must be a parent controller.
   */
  public static final String VENICE_URL_PROP = "venice.urls";
  /**
   * This new url field is meant to replace {@link #VENICE_URL_PROP} to avoid Venice users accidentally override
   * this system property.
   * Once the new prop: {@link #VENICE_DISCOVER_URL_PROP} is fully adopted, we will deprecate {@link #VENICE_URL_PROP}.
   */
  public static final String VENICE_DISCOVER_URL_PROP = "venice.discover.urls";

  public static final String SOURCE_GRID_FABRIC = "source.grid.fabric";

  public static final String ENABLE_WRITE_COMPUTE = "venice.write.compute.enable";
  public static final String ENABLE_PUSH = "venice.push.enable";
  public static final String ENABLE_SSL = "venice.ssl.enable";
  public static final String VENICE_CLUSTER_NAME_PROP = "cluster.name";
  public static final String VENICE_STORE_NAME_PROP = "venice.store.name";
  public static final String INPUT_PATH_PROP = "input.path";
  public static final String INPUT_PATH_LAST_MODIFIED_TIME = "input.path.last.modified.time";
  public static final String BATCH_NUM_BYTES_PROP = "batch.num.bytes";
  /**
   * Specifies a list of partitioners venice supported.
   * It contains a string of concatenated partitioner class names separated by comma.
   */
  public static final String VENICE_PARTITIONERS_PROP = "venice.partitioners";

  public static final String VALUE_SCHEMA_ID_PROP = "value.schema.id";
  public static final String DERIVED_SCHEMA_ID_PROP = "derived.schema.id";
  public static final String TOPIC_PROP = "venice.kafka.topic";
  protected static final String HADOOP_PREFIX = "hadoop-conf.";
  protected static final String HADOOP_VALIDATE_SCHEMA_AND_BUILD_DICT_PREFIX = "hadoop-dict-build-conf.";
  protected static final String SSL_PREFIX = "ssl";

  // PBNJ-related configs are all optional
  public static final String PBNJ_ENABLE = "pbnj.enable";
  public static final String PBNJ_FAIL_FAST = "pbnj.fail.fast";
  public static final String PBNJ_ASYNC = "pbnj.async";
  public static final String PBNJ_ROUTER_URL_PROP = "pbnj.router.urls";
  public static final String PBNJ_SAMPLING_RATIO_PROP = "pbnj.sampling.ratio";

  public static final String STORAGE_QUOTA_PROP = "storage.quota";
  public static final String STORAGE_ENGINE_OVERHEAD_RATIO = "storage_engine_overhead_ratio";
  public static final String VSON_PUSH = "vson.push";
  public static final String KAFKA_SECURITY_PROTOCOL = "SSL";
  public static final String COMPRESSION_STRATEGY = "compression.strategy";
  public static final String SSL_CONFIGURATOR_CLASS_CONFIG = "ssl.configurator.class";
  public static final String SSL_KEY_STORE_PROPERTY_NAME = "ssl.key.store.property.name";
  public static final String SSL_TRUST_STORE_PROPERTY_NAME = "ssl.trust.store.property.name";
  public static final String SSL_KEY_STORE_PASSWORD_PROPERTY_NAME = "ssl.key.store.password.property.name";
  public static final String SSL_KEY_PASSWORD_PROPERTY_NAME = "ssl.key.password.property.name";
  public static final String JOB_EXEC_URL = "job.execution.url";
  public static final String JOB_EXEC_ID = "job.execution.id";
  public static final String JOB_SERVER_NAME = "job.server.name";

  /**
   * Config to enable the service that uploads push job statuses to the controller using
   * {@code ControllerClient.uploadPushJobStatus()}, the job status is then packaged and sent to dedicated Kafka channel.
   */
  public static final String PUSH_JOB_STATUS_UPLOAD_ENABLE = "push.job.status.upload.enable";
  public static final String REDUCER_SPECULATIVE_EXECUTION_ENABLE = "reducer.speculative.execution.enable";

  /**
   * The interval of number of messages upon which certain info is printed in the reducer logs.
   */
  public static final String TELEMETRY_MESSAGE_INTERVAL = "telemetry.message.interval";

  /**
   * Config to control the Compression Level for ZSTD Dictionary Compression.
   */
  public static final String ZSTD_COMPRESSION_LEVEL = "zstd.compression.level";
  public static final int DEFAULT_BATCH_BYTES_SIZE = 1000000;
  public static final boolean SORTED = true;
  /**
   * The rewind override when performing re-push to prevent data loss; if the store has higher rewind config setting than
   * 1 days, adopt the store config instead; otherwise, override the rewind config to 1 day if push job config doesn't
   * try to override it.
   */
  public static final long DEFAULT_RE_PUSH_REWIND_IN_SECONDS_OVERRIDE = Time.SECONDS_PER_DAY;
  /**
   * Config to control the TTL behaviors in repush.
   */
  public static final String REPUSH_TTL_IN_HOURS = "repush.ttl.hours";
  public static final String REPUSH_TTL_POLICY = "repush.ttl.policy";

  public static final int NOT_SET = -1;
  private static final Logger LOGGER = LogManager.getLogger(VenicePushJob.class);

  /**
   * Since the job is calculating the raw data file size, which is not accurate because of compression,
   * key/value schema and backend storage overhead, we are applying this factor to provide a more
   * reasonable estimation.
   */
  /**
   * TODO: for map-reduce job, we could come up with more accurate estimation.
   */
  public static final long INPUT_DATA_SIZE_FACTOR = 2;
  protected static final boolean DEFAULT_IS_DUPLICATED_KEY_ALLOWED = false;
  /**
   * Placeholder for version number that is yet to be created.
   */
  private static final int UNCREATED_VERSION_NUMBER = -1;
  private static final long DEFAULT_POLL_STATUS_INTERVAL_MS = 5 * Time.MS_PER_MINUTE;

  /**
   * The default total time we wait before failing a job if the job status stays in UNKNOWN state.
   */
  private static final long DEFAULT_JOB_STATUS_IN_UNKNOWN_STATE_TIMEOUT_MS = 30 * Time.MS_PER_MINUTE;
  private static final String NON_CRITICAL_EXCEPTION = "This exception does not fail the push job. ";

  // Immutable state
  protected final VeniceProperties props;
  private final String jobId;
  private final String clusterName;

  // Lazy state
  private final Lazy<Properties> sslProperties;
  private VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter;
  /** TODO: refactor to use {@link Lazy} */

  // Mutable state
  private ControllerClient controllerClient;
  private ControllerClient systemKMEStoreControllerClient;
  private ControllerClient clusterDiscoveryControllerClient;
  private ControllerClient livenessHeartbeatStoreControllerClient;
  private RunningJob runningJob;
  // Job config for schema validation and Compression dictionary creation (if needed)
  protected JobConf validateSchemaAndBuildDictJobConf = new JobConf();
  // Job config for regular push job
  protected JobConf jobConf = new JobConf();
  // Job config for pbnj
  protected JobConf pbnjJobConf = new JobConf();
  protected InputDataInfoProvider inputDataInfoProvider;
  // Total input data size, which is used to talk to controller to decide whether we have enough quota or not
  private long inputFileDataSize;
  private String inputDirectory;
  private boolean inputFileHasRecords;
  private long inputModificationTime;
  private long inputNumFiles;
  private long jobStartTimeMs;
  private Properties veniceWriterProperties;
  private JobClientWrapper jobClientWrapper;
  private SentPushJobDetailsTracker sentPushJobDetailsTracker;
  private Class<? extends Partitioner> mapRedPartitionerClass = VeniceMRPartitioner.class;
  private PushJobSchemaInfo pushJobSchemaInfo;

  protected static class PushJobSetting {
    boolean enablePush;
    boolean enableSsl;
    String sslFactoryClassName;
    String veniceControllerUrl;
    String veniceRouterUrl;
    String storeName;
    String sourceGridFabric;
    int batchNumBytes;
    boolean enablePBNJ;
    boolean pbnjFailFast;
    boolean pbnjAsync;
    double pbnjSamplingRatio;
    boolean isIncrementalPush;
    Optional<String> incrementalPushVersion = Optional.empty();
    boolean isDuplicateKeyAllowed;
    boolean enablePushJobStatusUpload;
    boolean enableReducerSpeculativeExecution;
    int controllerRetries;
    int controllerStatusPollRetries;
    long pollJobStatusIntervalMs;
    long jobStatusInUnknownStateTimeoutMs;
    boolean sendControlMessagesDirectly;
    boolean isSourceETL;
    boolean enableWriteCompute;
    ETLValueSchemaTransformation etlValueSchemaTransformation;
    boolean isSourceKafka;
    String kafkaInputBrokerUrl;
    String kafkaInputTopic;
    RepushInfoResponse repushInfoResponse;
    long rewindTimeInSecondsOverride;
    boolean kafkaInputCombinerEnabled;
    BufferReplayPolicy validateRemoteReplayPolicy;
    boolean suppressEndOfPushMessage;
    boolean deferVersionSwap;
    boolean extendedSchemaValidityCheckEnabled;
    boolean compressionMetricCollectionEnabled;
    // temporary flag to host the code to use mapper to validate schema and build dictionary
    boolean useMapperToBuildDict;
    // specify ttl time to drop stale records. Only works for repush
    long repushTtlInHours;
  }

  protected PushJobSetting pushJobSetting;

  protected static class VersionTopicInfo {
    // Kafka topic for new data push
    String topic;
    /** Version part of the store-version / topic name */
    int version;
    // Kafka topic partition count
    int partitionCount;
    // Kafka url will get from Venice backend for store push
    String kafkaUrl;
    boolean sslToKafka;
    boolean daVinciPushStatusStoreEnabled;
    CompressionStrategy compressionStrategy;
    String partitionerClass;
    Map<String, String> partitionerParams;
    int amplificationFactor;
    boolean chunkingEnabled;
  }

  private VersionTopicInfo kafkaTopicInfo;

  private final PushJobDetails pushJobDetails;
  private final InternalAvroSpecificSerializer<PushJobDetails> pushJobDetailsSerializer =
      AvroProtocolDefinition.PUSH_JOB_DETAILS.getSerializer();

  protected static class StoreSetting {
    boolean isChunkingEnabled;
    long storeStorageQuota;
    boolean isSchemaAutoRegisterFromPushJobEnabled;
    CompressionStrategy compressionStrategy;
    boolean isLeaderFollowerModelEnabled;
    boolean isWriteComputeEnabled;
    boolean isIncrementalPushEnabled;
    Version sourceKafkaInputVersionInfo;
    Version sourceKafkaOutputVersionInfo;

  }

  protected StoreSetting storeSetting;
  private InputStorageQuotaTracker inputStorageQuotaTracker;
  private PushJobHeartbeatSenderFactory pushJobHeartbeatSenderFactory;
  private final boolean jobLivenessHeartbeatEnabled;
  private boolean pushJobStatusUploadDisabledHasBeenLogged = false;

  /**
   * Different successful checkpoints and known error scenarios of the VPJ flow.
   * 1. The enums are not sequential
   * 2. Non-negative enums are successful checkpoints
   * 3. Negative enums are error scenarios (Can be user or system errors)
   */
  public enum PushJobCheckpoints {
    INITIALIZE_PUSH_JOB(0), NEW_VERSION_CREATED(1), START_MAP_REDUCE_JOB(2), MAP_REDUCE_JOB_COMPLETED(3),
    START_JOB_STATUS_POLLING(4), JOB_STATUS_POLLING_COMPLETED(5), START_VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB(6),
    VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB_COMPLETED(7), QUOTA_EXCEEDED(-1), WRITE_ACL_FAILED(-2),
    DUP_KEY_WITH_DIFF_VALUE(-3), FILE_SCHEMA_VALIDATION_FAILED(-4), EXTENDED_FILE_SCHEMA_VALIDATION_FAILED(-5),
    RECORD_TOO_LARGE_FAILED(-6), CONCURRENT_BATCH_PUSH(-7), DATASET_CHANGED(-8), INVALID_INPUT_FILE(-9),
    ZSTD_DICTIONARY_CREATION_FAILED(-10);

    private final int value;

    PushJobCheckpoints(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

  // Visible for testing
  public VenicePushJob(String jobId, Properties vanillaProps, ControllerClient controllerClient) {
    this(jobId, vanillaProps, controllerClient, null);
  }

  // Visible for testing
  public VenicePushJob(
      String jobId,
      Properties vanillaProps,
      ControllerClient controllerClient,
      ControllerClient clusterDiscoveryControllerClient) {
    this.controllerClient = controllerClient;
    this.clusterDiscoveryControllerClient = clusterDiscoveryControllerClient;
    this.jobId = jobId;
    this.props = getVenicePropsFromVanillaProps(vanillaProps);
    this.sslProperties = Lazy.of(() -> {
      try {
        return getSslProperties(this.props);
      } catch (IOException e) {
        throw new VeniceException("Could not get user credential");
      }
    });
    LOGGER.info("Constructing {}: {}", VenicePushJob.class.getSimpleName(), props.toString(true));
    String veniceControllerUrl = getVeniceControllerUrl(props);
    initControllerClient(
        props.getString(VENICE_STORE_NAME_PROP),
        getVeniceControllerUrl(props),
        createSSlFactory(
            props.getBoolean(ENABLE_SSL, false),
            props.getString(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME)),
        props.getInt(CONTROLLER_REQUEST_RETRY_ATTEMPTS, 1));
    this.pushJobSetting = getPushJobSetting(veniceControllerUrl, props);
    LOGGER.info("Going to use controller URL: {}  to discover cluster.", veniceControllerUrl);
    this.clusterName = discoverCluster(props.getString(VENICE_STORE_NAME_PROP));
    LOGGER
        .info("The store {} is discovered in Venice cluster {}", props.getString(VENICE_STORE_NAME_PROP), clusterName);
    // Optional configs:
    this.pushJobDetails = new PushJobDetails();
    boolean jobLivenessHeartbeatEnabled;
    if (props.getBoolean(HEARTBEAT_ENABLED_CONFIG.getConfigName(), false)) {
      LOGGER.info("Push job heartbeat is enabled.");
      try {
        this.pushJobHeartbeatSenderFactory = new DefaultPushJobHeartbeatSenderFactory();
        this.livenessHeartbeatStoreControllerClient = createLivenessHeartbeatControllerClient(props);
        jobLivenessHeartbeatEnabled = true;

      } catch (Exception e) {
        LOGGER.warn(
            "Initializing the liveness heartbeat sender or its controller client failed. Hence the feature is disabled.",
            e);
        this.pushJobDetails.sendLivenessHeartbeatFailureDetails = e.getMessage();
        this.pushJobHeartbeatSenderFactory = new NoOpPushJobHeartbeatSenderFactory();
        this.livenessHeartbeatStoreControllerClient = null;
        jobLivenessHeartbeatEnabled = false;
      }
    } else {
      LOGGER.info("Push job heartbeat is NOT enabled.");
      this.pushJobHeartbeatSenderFactory = new NoOpPushJobHeartbeatSenderFactory();
      this.livenessHeartbeatStoreControllerClient = null;
      jobLivenessHeartbeatEnabled = false;
    }
    this.jobLivenessHeartbeatEnabled = jobLivenessHeartbeatEnabled;
  }

  private ControllerClient createLivenessHeartbeatControllerClient(VeniceProperties properties) {
    String heartbeatStoreName = properties.getString(HEARTBEAT_STORE_NAME_CONFIG.getConfigName()); // Required config
                                                                                                   // value
    Optional<SSLFactory> sslFactory = createSSlFactory(
        properties.getBoolean(ENABLE_SSL, false),
        properties.getString(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME));
    String veniceControllerUrl = getVeniceControllerUrl(properties);
    String heartbeatStoreClusterName = discoverCluster(heartbeatStoreName);
    ControllerClient heartbeatStoreControllerClient =
        ControllerClient.constructClusterControllerClient(heartbeatStoreClusterName, veniceControllerUrl, sslFactory);
    LOGGER.info(
        "Created controller client for the liveness heartbeat store {} in {} cluster",
        heartbeatStoreName,
        heartbeatStoreClusterName);
    return heartbeatStoreControllerClient;
  }

  /**
   * @param jobId  id of the job
   * @param vanillaProps  Property bag for the job
   */
  public VenicePushJob(String jobId, Properties vanillaProps) {
    this(jobId, vanillaProps, null);
  }

  // Visible for testing
  PushJobSetting getPushJobSetting() {
    return this.pushJobSetting;
  }

  private VeniceProperties getVenicePropsFromVanillaProps(Properties vanillaProps) {
    if (vanillaProps.containsKey(LEGACY_AVRO_KEY_FIELD_PROP)) {
      if (vanillaProps.containsKey(KEY_FIELD_PROP)
          && !vanillaProps.getProperty(KEY_FIELD_PROP).equals(vanillaProps.getProperty(LEGACY_AVRO_KEY_FIELD_PROP))) {
        throw new VeniceException("Duplicate key filed found in config. Both avro.key.field and key.field are set up.");
      }
      vanillaProps.setProperty(KEY_FIELD_PROP, vanillaProps.getProperty(LEGACY_AVRO_KEY_FIELD_PROP));
    }
    if (vanillaProps.containsKey(LEGACY_AVRO_VALUE_FIELD_PROP)) {
      if (vanillaProps.containsKey(VALUE_FIELD_PROP) && !vanillaProps.getProperty(VALUE_FIELD_PROP)
          .equals(vanillaProps.getProperty(LEGACY_AVRO_VALUE_FIELD_PROP))) {
        throw new VeniceException(
            "Duplicate value filed found in config. Both avro.value.field and value.field are set up.");
      }
      vanillaProps.setProperty(VALUE_FIELD_PROP, vanillaProps.getProperty(LEGACY_AVRO_VALUE_FIELD_PROP));
    }
    String[] requiredSSLPropertiesNames = new String[] { SSL_KEY_PASSWORD_PROPERTY_NAME,
        SSL_KEY_STORE_PASSWORD_PROPERTY_NAME, SSL_KEY_STORE_PROPERTY_NAME, SSL_TRUST_STORE_PROPERTY_NAME };
    for (String sslPropertyName: requiredSSLPropertiesNames) {
      if (!vanillaProps.containsKey(sslPropertyName)) {
        throw new VeniceException("Miss the require ssl property name: " + sslPropertyName);
      }
    }
    return new VeniceProperties(vanillaProps);
  }

  private PushJobSetting getPushJobSetting(String veniceControllerUrl, VeniceProperties props) {
    PushJobSetting pushJobSettingToReturn = new PushJobSetting();
    pushJobSettingToReturn.veniceControllerUrl = veniceControllerUrl;
    pushJobSettingToReturn.enablePush = props.getBoolean(ENABLE_PUSH, true);
    /**
     * TODO: after controller SSL support is rolled out everywhere, change the default behavior for ssl enabled to true;
     * Besides, change the venice controller urls list for all push job to use the new port
     */
    pushJobSettingToReturn.enableSsl = props.getBoolean(ENABLE_SSL, false);
    pushJobSettingToReturn.sslFactoryClassName =
        props.getString(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
    if (props.containsKey(SOURCE_GRID_FABRIC)) {
      pushJobSettingToReturn.sourceGridFabric = props.getString(SOURCE_GRID_FABRIC);
    }
    pushJobSettingToReturn.batchNumBytes = props.getInt(BATCH_NUM_BYTES_PROP, DEFAULT_BATCH_BYTES_SIZE);
    pushJobSettingToReturn.enablePBNJ = props.getBoolean(PBNJ_ENABLE, false);
    pushJobSettingToReturn.pbnjFailFast = props.getBoolean(PBNJ_FAIL_FAST, false);
    pushJobSettingToReturn.pbnjAsync = props.getBoolean(PBNJ_ASYNC, false);
    pushJobSettingToReturn.pbnjSamplingRatio = props.getDouble(PBNJ_SAMPLING_RATIO_PROP, 1.0);
    pushJobSettingToReturn.isIncrementalPush = props.getBoolean(INCREMENTAL_PUSH, false);
    pushJobSettingToReturn.isDuplicateKeyAllowed = props.getBoolean(ALLOW_DUPLICATE_KEY, false);
    pushJobSettingToReturn.enablePushJobStatusUpload = props.getBoolean(PUSH_JOB_STATUS_UPLOAD_ENABLE, false);
    pushJobSettingToReturn.enableReducerSpeculativeExecution =
        props.getBoolean(REDUCER_SPECULATIVE_EXECUTION_ENABLE, false);
    pushJobSettingToReturn.controllerRetries = props.getInt(CONTROLLER_REQUEST_RETRY_ATTEMPTS, 1);
    pushJobSettingToReturn.controllerStatusPollRetries = props.getInt(POLL_STATUS_RETRY_ATTEMPTS, 15);
    pushJobSettingToReturn.pollJobStatusIntervalMs =
        props.getLong(POLL_JOB_STATUS_INTERVAL_MS, DEFAULT_POLL_STATUS_INTERVAL_MS);
    pushJobSettingToReturn.jobStatusInUnknownStateTimeoutMs =
        props.getLong(JOB_STATUS_IN_UNKNOWN_STATE_TIMEOUT_MS, DEFAULT_JOB_STATUS_IN_UNKNOWN_STATE_TIMEOUT_MS);
    pushJobSettingToReturn.sendControlMessagesDirectly = props.getBoolean(SEND_CONTROL_MESSAGES_DIRECTLY, false);
    pushJobSettingToReturn.enableWriteCompute = props.getBoolean(ENABLE_WRITE_COMPUTE, false);
    pushJobSettingToReturn.isSourceETL = props.getBoolean(SOURCE_ETL, false);
    pushJobSettingToReturn.isSourceKafka = props.getBoolean(SOURCE_KAFKA, false);
    pushJobSettingToReturn.kafkaInputCombinerEnabled = props.getBoolean(KAFKA_INPUT_COMBINER_ENABLED, false);
    pushJobSettingToReturn.suppressEndOfPushMessage = props.getBoolean(SUPPRESS_END_OF_PUSH_MESSAGE, false);
    pushJobSettingToReturn.deferVersionSwap = props.getBoolean(DEFER_VERSION_SWAP, false);
    pushJobSettingToReturn.repushTtlInHours = props.getLong(REPUSH_TTL_IN_HOURS, NOT_SET);

    if (pushJobSettingToReturn.repushTtlInHours != NOT_SET && !pushJobSettingToReturn.isSourceKafka) {
      throw new VeniceException("Repush with TTL is only supported while using Kafka Input Format");
    }

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
      if (pushJobSettingToReturn.enablePBNJ) {
        throw new VeniceException("PBNJ is not supported while using Kafka Input Format");
      }
      pushJobSettingToReturn.kafkaInputTopic =
          getSourceTopicNameForKafkaInput(props.getString(VENICE_STORE_NAME_PROP), props, pushJobSettingToReturn);
      pushJobSettingToReturn.kafkaInputBrokerUrl = pushJobSettingToReturn.repushInfoResponse == null
          ? props.getString(KAFKA_INPUT_BROKER_URL)
          : pushJobSettingToReturn.repushInfoResponse.getRepushInfo().getKafkaBrokerUrl();
    }
    pushJobSettingToReturn.storeName = props.getString(VENICE_STORE_NAME_PROP);
    pushJobSettingToReturn.rewindTimeInSecondsOverride = props.getLong(REWIND_TIME_IN_SECONDS_OVERRIDE, NOT_SET);

    // If we didn't specify a rewind time
    if (pushJobSettingToReturn.rewindTimeInSecondsOverride == NOT_SET) {
      // But we did specify a rewind time epoch timestamp
      long rewindTimestamp = props.getLong(REWIND_EPOCH_TIME_IN_SECONDS_OVERRIDE, NOT_SET);
      if (rewindTimestamp != NOT_SET) {
        long nowInSeconds = System.currentTimeMillis() / 1000;
        // So long as that rewind time isn't in the future
        if (rewindTimestamp > nowInSeconds) {
          throw new VeniceException(
              String.format(
                  "Provided {} for {}. {} cannot be a timestamp in the future!! ",
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

    if (pushJobSettingToReturn.enablePBNJ) {
      // If PBNJ is enabled, then the router URL config is mandatory
      pushJobSettingToReturn.veniceRouterUrl = props.getString(PBNJ_ROUTER_URL_PROP);
    } else {
      pushJobSettingToReturn.veniceRouterUrl = null;
    }

    if (!pushJobSettingToReturn.enablePush && !pushJobSettingToReturn.enablePBNJ) {
      throw new VeniceException(
          "At least one of the following config properties must be true: " + ENABLE_PUSH + " or " + PBNJ_ENABLE);
    }

    pushJobSettingToReturn.extendedSchemaValidityCheckEnabled =
        props.getBoolean(EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED, DEFAULT_EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED);
    pushJobSettingToReturn.compressionMetricCollectionEnabled =
        props.getBoolean(COMPRESSION_METRIC_COLLECTION_ENABLED, DEFAULT_COMPRESSION_METRIC_COLLECTION_ENABLED);
    pushJobSettingToReturn.useMapperToBuildDict =
        props.getBoolean(USE_MAPPER_TO_BUILD_DICTIONARY, DEFAULT_USE_MAPPER_TO_BUILD_DICTIONARY);
    return pushJobSettingToReturn;
  }

  private String getVeniceControllerUrl(VeniceProperties props) {
    String veniceControllerUrl = null;
    if (!props.containsKey(VENICE_URL_PROP) && !props.containsKey(VENICE_DISCOVER_URL_PROP)) {
      throw new VeniceException(
          "At least one of the following config properties needs to be present: " + VENICE_URL_PROP + " or "
              + VENICE_DISCOVER_URL_PROP);
    }
    if (props.containsKey(VENICE_URL_PROP)) {
      veniceControllerUrl = props.getString(VENICE_URL_PROP);
    }
    if (props.containsKey(VENICE_DISCOVER_URL_PROP)) {
      /**
       * {@link VENICE_DISCOVER_URL_PROP} has higher priority than {@link VENICE_URL_PROP}.
       */
      veniceControllerUrl = props.getString(VENICE_DISCOVER_URL_PROP);
    }
    return veniceControllerUrl;
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
   * @param pushJobSettingUnderConstruction some partially defined {@link PushJobSetting} which this function will
   *                                        contribute to further setting up.
   *                                        TODO: mutating input params is not the cleanest pattern, consider refactoring
   * @return Topic name
   */
  private String getSourceTopicNameForKafkaInput(
      final String userProvidedStoreName,
      final VeniceProperties properties,
      final PushJobSetting pushJobSettingUnderConstruction) {
    if (controllerClient == null) {
      initControllerClient(
          pushJobSettingUnderConstruction.storeName,
          getVeniceControllerUrl(properties),
          createSSlFactory(
              properties.getBoolean(ENABLE_SSL, false),
              properties.getString(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME)),
          pushJobSettingUnderConstruction.controllerRetries);
    }
    final Optional<String> userProvidedTopicNameOptional =
        Optional.ofNullable(properties.getString(KAFKA_INPUT_TOPIC, () -> null));

    // This mode of passing the topic name to VPJ is going to be deprecated.
    if (userProvidedTopicNameOptional.isPresent()) {
      return getUserProvidedTopicName(
          userProvidedStoreName,
          userProvidedTopicNameOptional.get(),
          pushJobSettingUnderConstruction.controllerRetries);
    }
    // If VPJ has fabric name available use that to find the child colo version otherwise
    // use the largest version among the child colo to use as KIF input topic.
    final Optional<String> userProvidedFabricNameOptional =
        Optional.ofNullable(properties.getString(KAFKA_INPUT_FABRIC, () -> null));

    pushJobSettingUnderConstruction.repushInfoResponse = ControllerClient.retryableRequest(
        controllerClient,
        pushJobSettingUnderConstruction.controllerRetries,
        c -> c.getRepushInfo(userProvidedStoreName, userProvidedFabricNameOptional));
    if (pushJobSettingUnderConstruction.repushInfoResponse.isError()) {
      throw new VeniceException("Could not get repush info for store " + userProvidedStoreName);
    }
    int version = pushJobSettingUnderConstruction.repushInfoResponse.getRepushInfo().getVersion().getNumber();
    return Version.composeKafkaTopic(userProvidedStoreName, version);
  }

  private String getUserProvidedTopicName(
      final String userProvidedStoreName,
      String userProvidedTopicName,
      int retryAttempts) {
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
    StoreResponse storeResponse =
        ControllerClient.retryableRequest(controllerClient, retryAttempts, c -> c.getStore(userProvidedStoreName));
    if (storeResponse.isError()) {
      throw new VeniceException(
          String.format(
              "Fail to get store information for store %s with error %s",
              userProvidedStoreName,
              storeResponse.getError()));
    }
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
  protected void setClusterDiscoveryControllerClient(ControllerClient clusterDiscoveryControllerClient) {
    this.clusterDiscoveryControllerClient = clusterDiscoveryControllerClient;
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
  protected void setSentPushJobDetailsTracker(SentPushJobDetailsTracker sentPushJobDetailsTracker) {
    this.sentPushJobDetailsTracker = sentPushJobDetailsTracker;
  }

  // Visible for testing
  protected void setMapRedPartitionerClass(Class<? extends Partitioner> mapRedPartitionerClass) {
    this.mapRedPartitionerClass = mapRedPartitionerClass;
  }

  /**
   * @throws VeniceException
   */
  public void run() {
    PushJobHeartbeatSender pushJobHeartbeatSender = null;
    try {
      initPushJobDetails();
      jobStartTimeMs = System.currentTimeMillis();
      logGreeting();
      final boolean sslEnabled = props.getBoolean(ENABLE_SSL, false);
      Optional<SSLFactory> sslFactory =
          createSSlFactory(sslEnabled, props.getString(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME));
      initControllerClient(
          pushJobSetting.storeName,
          pushJobSetting.veniceControllerUrl,
          sslFactory,
          pushJobSetting.controllerRetries);
      sendPushJobDetailsToController();
      validateKafkaMessageEnvelopeSchema(pushJobSetting);
      validateRemoteHybridSettings(pushJobSetting);
      pushJobHeartbeatSender = createPushJobHeartbeatSender(sslEnabled);
      inputDirectory = getInputURI(props);
      storeSetting = getSettingsFromController(controllerClient, pushJobSetting);
      inputStorageQuotaTracker = new InputStorageQuotaTracker(storeSetting.storeStorageQuota);

      if (pushJobSetting.repushTtlInHours != NOT_SET && storeSetting.isChunkingEnabled) {
        throw new VeniceException("Repush TTL is not supported when the store has chunking enabled.");
      }

      if (pushJobSetting.isSourceETL) {
        MultiSchemaResponse allValueSchemaResponses = controllerClient.getAllValueSchema(pushJobSetting.storeName);
        MultiSchemaResponse.Schema[] allValueSchemas = allValueSchemaResponses.getSchemas();
        Schema lastValueSchema = Schema.parse(allValueSchemas[allValueSchemas.length - 1].getSchemaStr());

        pushJobSetting.etlValueSchemaTransformation = ETLValueSchemaTransformation.fromSchema(lastValueSchema);
      } else {
        pushJobSetting.etlValueSchemaTransformation = ETLValueSchemaTransformation.NONE;
      }

      /**
       * If the data source is from some existing Kafka topic, no need to validate the input.
       */
      if (!pushJobSetting.isSourceKafka) {
        // Check data size
        InputDataInfoProvider.InputDataInfo inputInfo =
            getInputDataInfoProvider().validateInputAndGetInfo(inputDirectory);
        // Get input schema
        pushJobSchemaInfo = inputInfo.getSchemaInfo();
        if (pushJobSchemaInfo.isAvro()) {
          validateFileSchema(pushJobSchemaInfo.getFileSchemaString());
        } else {
          LOGGER.info("Skip validating file schema since it is not Avro.");
        }
        inputFileDataSize = inputInfo.getInputFileDataSizeInBytes();
        inputFileHasRecords = inputInfo.hasRecords();
        inputModificationTime = inputInfo.getInputModificationTime();
        inputNumFiles = inputInfo.getNumInputFiles();

        if (!inputFileHasRecords && storeSetting.compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
          throw new VeniceException("Empty push with ZSTD dictionary Compression is not allowed");
        }

        // validate the key/value of the first file with the store by speaking to the controller
        validateKeySchema(controllerClient, pushJobSetting, pushJobSchemaInfo);
        validateValueSchema(
            controllerClient,
            pushJobSetting,
            pushJobSchemaInfo,
            storeSetting.isSchemaAutoRegisterFromPushJobEnabled);

        if (pushJobSetting.useMapperToBuildDict) {
          /**
           * 1. validate whether the remaining file's schema are consistent with the first file
           * 2. Build dictionary (if dictionary compression is enabled for this store version or compressionMetricCollectionEnabled)
           */
          validateSchemaAndBuildDict(
              validateSchemaAndBuildDictJobConf,
              pushJobSetting,
              pushJobSchemaInfo,
              storeSetting,
              props,
              jobId,
              inputDirectory);
          sendPushJobDetailsToController();
        }
      } else {
        /**
         * Using a default number: 1GB here.
         * No need to specify the accurate input size since the re-push mustn't be the first version, so the partition
         * count calculation won't be affected by this random number.
         */
        inputFileDataSize = 1024 * 1024 * 1024L;
        /**
         * This is used to ensure the {@link #verifyCountersWithZeroValues()} function won't assume the reducer count
         * should be 0.
         */
        inputFileHasRecords = true;
      }

      if (!pushJobSetting.enablePush) {
        LOGGER.info("Skipping push job, since {} is set to false.", ENABLE_PUSH);
      } else {
        Optional<ByteBuffer> optionalCompressionDictionary = getCompressionDictionary();
        long pushStartTimeMs = System.currentTimeMillis();
        String pushId = pushStartTimeMs + "_" + props.getString(JOB_EXEC_URL, "failed_to_obtain_execution_url");
        if (pushJobSetting.isSourceKafka) {
          pushId = Version.generateRePushId(pushId);
          if (storeSetting.sourceKafkaInputVersionInfo.getHybridStoreConfig() != null
              && pushJobSetting.rewindTimeInSecondsOverride == NOT_SET) {
            pushJobSetting.rewindTimeInSecondsOverride = DEFAULT_RE_PUSH_REWIND_IN_SECONDS_OVERRIDE;
            LOGGER.info("Overriding re-push rewind time in seconds to: {}", DEFAULT_RE_PUSH_REWIND_IN_SECONDS_OVERRIDE);
          }
        }
        // Create new store version, topic and fetch Kafka url from backend
        createNewStoreVersion(
            pushJobSetting,
            inputFileDataSize,
            controllerClient,
            pushId,
            props,
            optionalCompressionDictionary);
        updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.NEW_VERSION_CREATED);
        // Update and send push job details with new info to the controller
        pushJobDetails.pushId = pushId;
        pushJobDetails.partitionCount = kafkaTopicInfo.partitionCount;
        pushJobDetails.valueCompressionStrategy = kafkaTopicInfo.compressionStrategy.getValue();
        pushJobDetails.chunkingEnabled = kafkaTopicInfo.chunkingEnabled;
        pushJobDetails.overallStatus.add(getPushJobDetailsStatusTuple(PushJobDetailsStatus.TOPIC_CREATED.getValue()));
        pushJobHeartbeatSender.start(pushJobSetting.storeName, kafkaTopicInfo.version);
        sendPushJobDetailsToController();
        // Log Venice data push job related info
        logPushJobProperties(
            kafkaTopicInfo,
            pushJobSetting,
            pushJobSchemaInfo,
            clusterName,
            inputDirectory,
            inputFileDataSize);

        // Setup the hadoop job
        // If reducer phase is enabled, each reducer will sort all the messages inside one single
        // topic partition.
        setupMRConf(
            jobConf,
            kafkaTopicInfo,
            pushJobSetting,
            pushJobSchemaInfo,
            storeSetting,
            props,
            jobId,
            inputDirectory);

        if (pushJobSetting.isIncrementalPush) {
          /**
           * N.B.: For now, we always send control messages directly for incremental pushes, regardless of
           * {@link pushJobSetting.sendControlMessagesDirectly}, because the controller does not yet support
           * sending these types of CM. If/when we add support for that in the controller, then we'll be able
           * to completely stop using the {@link VeniceWriter} from this class.
           */
          pushJobSetting.incrementalPushVersion = Optional.of(
              System.currentTimeMillis() + "_" + props.getString(JOB_SERVER_NAME, "unknown_job_server") + "_"
                  + props.getString(JOB_EXEC_ID, "unknown_exec_id"));
          LOGGER.info("Incremental Push Version: {}", pushJobSetting.incrementalPushVersion.get());
          getVeniceWriter(kafkaTopicInfo)
              .broadcastStartOfIncrementalPush(pushJobSetting.incrementalPushVersion.get(), new HashMap<>());
          runJobAndUpdateStatus();
          getVeniceWriter(kafkaTopicInfo)
              .broadcastEndOfIncrementalPush(pushJobSetting.incrementalPushVersion.get(), Collections.emptyMap());
        } else {
          if (pushJobSetting.sendControlMessagesDirectly) {
            getVeniceWriter(kafkaTopicInfo).broadcastStartOfPush(
                SORTED,
                storeSetting.isChunkingEnabled,
                kafkaTopicInfo.compressionStrategy,
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
              getVeniceWriter(kafkaTopicInfo).broadcastEndOfPush(Collections.emptyMap());
            } else {
              controllerClient.writeEndOfPush(pushJobSetting.storeName, kafkaTopicInfo.version);
            }
          }

        }
        // Close VeniceWriter before polling job status since polling job status could
        // trigger job deletion
        closeVeniceWriter();
        // Update and send push job details with new info
        updatePushJobDetailsWithMRCounters();
        pushJobDetails.overallStatus.add(getPushJobDetailsStatusTuple(PushJobDetailsStatus.WRITE_COMPLETED.getValue()));
        sendPushJobDetailsToController();
        // Waiting for Venice Backend to complete consumption
        updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.START_JOB_STATUS_POLLING);

        // Poll for job status unless we've suppressed sending EOP, in which case, don't wait up
        if (!pushJobSetting.suppressEndOfPushMessage) {
          pollStatusUntilComplete(
              pushJobSetting.incrementalPushVersion,
              controllerClient,
              pushJobSetting,
              kafkaTopicInfo);
        }

        updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED);
        pushJobDetails.overallStatus.add(getPushJobDetailsStatusTuple(PushJobDetailsStatus.COMPLETED.getValue()));
        pushJobDetails.jobDurationInMs = System.currentTimeMillis() - jobStartTimeMs;
        updatePushJobDetailsWithConfigs();
        updatePushJobDetailsWithLivenessHeartbeatException(pushJobHeartbeatSender);
        sendPushJobDetailsToController();
      }

      if (pushJobSetting.enablePBNJ) {
        LOGGER.info("Post-Bulkload Analysis Job is about to run.");
        setupPBNJConf(
            pbnjJobConf,
            kafkaTopicInfo,
            pushJobSetting,
            pushJobSchemaInfo,
            storeSetting,
            props,
            jobId,
            inputDirectory);
        runningJob = runJobWithConfig(pbnjJobConf);
      }
    } catch (Throwable e) {
      LOGGER.error("Failed to run job.", e);
      // Make sure all the logic before killing the failed push jobs is captured in the following block
      try {
        if (e instanceof TopicAuthorizationVeniceException) {
          updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.WRITE_ACL_FAILED);
        }
        pushJobDetails.overallStatus.add(getPushJobDetailsStatusTuple(PushJobDetailsStatus.ERROR.getValue()));
        pushJobDetails.failureDetails = e.toString();
        pushJobDetails.jobDurationInMs = System.currentTimeMillis() - jobStartTimeMs;
        updatePushJobDetailsWithConfigs();
        updatePushJobDetailsWithLivenessHeartbeatException(pushJobHeartbeatSender);
        sendPushJobDetailsToController();
        closeVeniceWriter();
      } catch (Exception ex) {
        LOGGER.error(
            "Error before killing the failed push job; still issue the kill job command to clean up states in backend",
            ex);
      } finally {
        try {
          killJobAndCleanup(pushJobSetting, controllerClient, kafkaTopicInfo);
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
      }
      inputDataInfoProvider = null;
    }
  }

  private PushJobHeartbeatSender createPushJobHeartbeatSender(final boolean sslEnabled) {
    try {
      return pushJobHeartbeatSenderFactory.createHeartbeatSender(
          props,
          livenessHeartbeatStoreControllerClient,
          sslEnabled ? Optional.of(this.sslProperties.get()) : Optional.empty());
    } catch (Exception e) {
      LOGGER.warn("Failed to create a push job heartbeat sender. Use the no-op push job heartbeat sender.", e);
      pushJobDetails.sendLivenessHeartbeatFailureDetails = e.getMessage();
      return new NoOpPushJobHeartbeatSender();
    }
  }

  private void updatePushJobDetailsWithLivenessHeartbeatException(PushJobHeartbeatSender pushJobHeartbeatSender) {
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

  private void validateFileSchema(String fileSchemaString) {
    try {
      AvroSchemaParseUtils.parseSchemaFromJSONStrictValidation(fileSchemaString);
    } catch (Exception e) {
      if (pushJobSetting.extendedSchemaValidityCheckEnabled) {
        LOGGER.error(
            "The schema of the input data failed strict Avro schema validation. Verify if the schema is a valid Avro schema.");
        updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.EXTENDED_FILE_SCHEMA_VALIDATION_FAILED);
        throw new VeniceException(e);
      }

      LOGGER.info("The schema of the input data failed strict Avro schema validation. Trying loose schema validation.");
      try {
        AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(fileSchemaString);
      } catch (Exception looseValidationException) {
        LOGGER.error(
            "The schema of the input data failed loose Avro schema validation. Verify if the schema is a valid Avro schema.");
        updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.FILE_SCHEMA_VALIDATION_FAILED);
        throw new VeniceException(looseValidationException);
      }
    }
  }

  private void runJobAndUpdateStatus() throws IOException {
    updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.START_MAP_REDUCE_JOB);
    runningJob = runJobWithConfig(jobConf);
    validateCountersAfterPush();
    Optional<ErrorMessage> errorMessage = updatePushJobDetailsWithMRDetails();
    if (errorMessage.isPresent()) {
      throw new VeniceException(errorMessage.get().getErrorMessage());
    }
    updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.MAP_REDUCE_JOB_COMPLETED);
  }

  private void validateCountersAfterPush() throws IOException {
    final long reducerClosedCount = MRJobCounterHelper.getReducerClosedCount(runningJob.getCounters());
    if (inputFileHasRecords) {
      long totalPutOrDeleteRecordsCount = 0;
      if (pushJobSetting.isSourceKafka) {
        totalPutOrDeleteRecordsCount = MRJobCounterHelper.getTotalPutOrDeleteRecordsCount(runningJob.getCounters());
        LOGGER.info(
            "Source kafka input topic : {} has {} records",
            pushJobSetting.kafkaInputTopic,
            totalPutOrDeleteRecordsCount);
      }
      if (reducerClosedCount < kafkaTopicInfo.partitionCount) {
        /**
         * No reducer tasks gets created if there is no data record present in source kafka topic in Kafka Input Format mode.
         * This is possible if the current version is created because of an empty push. Let's check to make sure this is
         * indeed the case and don't fail the job and instead let the job continue. This will basically be equivalent of
         * another empty push which will create a new version.
         */
        if (pushJobSetting.isSourceKafka && totalPutOrDeleteRecordsCount == 0) {
          return;
        }
        if (MRJobCounterHelper.getMapperSprayAllPartitionsTriggeredCount(runningJob.getCounters()) == 0) {
          /**
           * Right now, only the mapper with task id: 0 will spray all the partitions to make sure each reducer will
           * be instantiated.
           * In some situation, it is possible the mapper with task id: 0 won't receive any records, so this
           * {@link AbstractVeniceMapper#maybeSprayAllPartitions} won't be invoked, so it is not guaranteed that
           * each reducer will be invoked, and the closing event of the reducers won't be tracked by {@link Reporter},
           * which will only be passed via {@link org.apache.hadoop.mapred.Reducer#reduce}.
           *
           * It will require a lot of efforts to make sure {@link AbstractVeniceMapper#maybeSprayAllPartitions} will be
           * invoked in all the scenarios, so right now, we choose this approach:
           * When {@link AbstractVeniceMapper#maybeSprayAllPartitions} is not invoked, VPJ won't fail if the reducer
           * close count is smaller than the partition count since we couldn't differentiate whether it is a real issue
           * or not.
           *
           * If there is a need to make it work for all the cases, and here are the potential proposals:
           * 1. Invoke {@link AbstractVeniceMapper#maybeSprayAllPartitions} in every mapper.
           * 2. Fake some input record to make sure the first mapper would always receive some message.
           * 3. Examine the VT to find out how many partitions contain messages from batch push.
           */
          LOGGER.warn(
              "'AbstractVeniceMapper#maybeSprayAllPartitions' is not invoked, so we couldn't"
                  + " decide whether the push job finished successfully or not purely based on the reducer job"
                  + " closed count ({}) < the partition count ({})",
              reducerClosedCount,
              kafkaTopicInfo.partitionCount);
          return;
        }

        throw new VeniceException(
            String.format(
                "MR job counter is not reliable since the reducer job closed count (%d) < the partition count (%d), "
                    + "while the input file data size is %d byte(s)",
                reducerClosedCount,
                kafkaTopicInfo.partitionCount,
                inputFileDataSize));
      }
    } else {
      verifyCountersWithZeroValues();
    }
  }

  private void runValidateSchemaAndBuildDictJobAndUpdateStatus(JobConf jobConf) throws IOException {
    updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.START_VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB);
    runningJob = runJobWithConfig(jobConf);
    validateCountersAfterValidateSchemaAndBuildDict();
    updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB_COMPLETED);
  }

  private void checkLastModificationTimeAndLog() throws IOException {
    checkLastModificationTimeAndLog(false);
  }

  private void checkLastModificationTimeAndLog(boolean throwException) throws IOException {
    long lastModificationTime = getInputDataInfoProvider().getInputLastModificationTime(inputDirectory);
    if (lastModificationTime > inputModificationTime) {
      updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.DATASET_CHANGED);
      String error = "Dataset changed during the push job. Please check above logs to see if the change "
          + "caused the MapReduce failure and rerun the job without dataset change.";
      LOGGER.error(error);
      if (throwException) {
        throw new VeniceException(error);
      }
    }
  }

  protected static boolean shouldBuildDictionary(PushJobSetting pushJobSetting, StoreSetting storeSetting) {
    if (pushJobSetting.compressionMetricCollectionEnabled
        || storeSetting.compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
      if (pushJobSetting.isIncrementalPush) {
        LOGGER.info("No compression dictionary will be generated as it is incremental push");
        return false;
      }
      LOGGER.info(
          "Compression dictionary will be generated with the strategy {} and compressionMetricCollectionEnabled is {}",
          storeSetting.compressionStrategy,
          (pushJobSetting.compressionMetricCollectionEnabled ? "Enabled" : "Disabled"));
      return true;
    }

    LOGGER.info(
        "No Compression dictionary will be generated with the strategy {} and compressionMetricCollectionEnabled is {}",
        storeSetting.compressionStrategy,
        (pushJobSetting.compressionMetricCollectionEnabled ? "Enabled" : "Disabled"));
    return false;
  }

  /**
   * Validate whether the Job ran successfully to validate schema and build dictionary:
   * - No error counters are increased
   * - Number of records processed == Num files + 1 (one extra to build dictionary)
   *
   * @throws IOException
   */
  private void validateCountersAfterValidateSchemaAndBuildDict() throws IOException {
    if (inputFileHasRecords) {
      Counters counters = runningJob.getCounters();
      final long dataModifiedDuringPushJob = MRJobCounterHelper.getMapperErrorDataModifiedDuringPushJobCount(counters);
      if (dataModifiedDuringPushJob != 0) {
        updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.DATASET_CHANGED);
        String err =
            "Error while validating schema and building dictionary: Because Dataset changed during the push job. Rerun the job without dataset change";
        LOGGER.error(err);
        throw new VeniceException(err);
      }

      final long readInvalidInputIdx = MRJobCounterHelper.getMapperInvalidInputIdxCount(counters);
      if (readInvalidInputIdx != 0) {
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

      final long schemaInconsistencyFailure = MRJobCounterHelper.getMapperSchemaInconsistencyFailureCount(counters);
      if (schemaInconsistencyFailure != 0) {
        updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.FILE_SCHEMA_VALIDATION_FAILED);
        String err = "Error while validating schema: Inconsistent file schema found";
        LOGGER.error(err);
        throw new VeniceException(err);
      }

      final long zstdDictTrainFailure = MRJobCounterHelper.getMapperZstdDictTrainFailureCount(counters);
      final long zstdDictTrainSuccess = MRJobCounterHelper.getMapperZstdDictTrainSuccessCount(counters);
      final long numRecordsProcessed = MRJobCounterHelper.getMapperNumRecordsSuccessfullyProcessedCount(counters);
      if (numRecordsProcessed == inputNumFiles + 1) {
        if (shouldBuildDictionary(pushJobSetting, storeSetting)) {
          if (zstdDictTrainSuccess != 1) {
            checkLastModificationTimeAndLog(true);
            updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.INVALID_INPUT_FILE);
            String err = "Error while validating schema: MR job counter is not reliable to point out the exact reason";
            LOGGER.error(err);
            throw new VeniceException(err);
          }
        }
      } else if (numRecordsProcessed == inputNumFiles) {
        if (zstdDictTrainFailure == 1) {
          if (storeSetting.compressionStrategy != CompressionStrategy.ZSTD_WITH_DICT) {
            // Tried creating dictionary due to compressionMetricCollectionEnabled
            LOGGER.warn(
                "Training ZStd dictionary failed: Maybe the sample size is too small or the content is not "
                    + "suitable for creating dictionary. But as this job's configured compression type don't "
                    + "need dictionary, the job is not stopped");
          } else {
            updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.ZSTD_DICTIONARY_CREATION_FAILED);
            String err = "Training ZStd dictionary failed: Maybe the sample size is too small or the content is "
                + "not suitable for creating dictionary.";
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
  void setSystemKMEStoreControllerClient(ControllerClient controllerClient) {
    this.systemKMEStoreControllerClient = controllerClient;
  }

  private void verifyCountersWithZeroValues() throws IOException {
    final Counters counters = runningJob.getCounters();
    final long reducerClosedCount = MRJobCounterHelper.getReducerClosedCount(counters);
    if (reducerClosedCount != 0) {
      throw new VeniceException("Expect 0 reducer closed. Got count: " + reducerClosedCount);
    }
    final long outputRecordsCount = MRJobCounterHelper.getOutputRecordsCount(counters);
    if (outputRecordsCount != 0) {
      throw new VeniceException("Expect 0 output record. Got count: " + outputRecordsCount);
    }
    final long writeAclAuthorizationFailureCount = MRJobCounterHelper.getWriteAclAuthorizationFailureCount(counters);
    if (writeAclAuthorizationFailureCount != 0) {
      throw new VeniceException("Expect 0 ACL authorization failure. Got count: " + writeAclAuthorizationFailureCount);
    }
    final long duplicateKeyWithDistinctCount = MRJobCounterHelper.getDuplicateKeyWithDistinctCount(counters);
    if (duplicateKeyWithDistinctCount != 0) {
      throw new VeniceException(
          "Expect 0 duplicated key with distinct value. Got count: " + duplicateKeyWithDistinctCount);
    }
    final long totalKeySize = MRJobCounterHelper.getTotalKeySize(counters);
    if (totalKeySize != 0) {
      throw new VeniceException("Expect 0 byte for total key size. Got count: " + totalKeySize);
    }
    final long totalValueSize = MRJobCounterHelper.getTotalValueSize(counters);
    if (totalValueSize != 0) {
      throw new VeniceException("Expect 0 byte for total value size. Got count: " + totalValueSize);
    }
  }

  private Optional<SSLFactory> createSSlFactory(final boolean enableSsl, final String sslFactoryClassName) {
    Optional<SSLFactory> sslFactory = Optional.empty();
    if (enableSsl) {
      LOGGER.info("Controller ACL is enabled.");
      Properties sslProps = sslProperties.get();
      sslFactory = Optional.of(SslUtils.getSSLFactory(sslProps, sslFactoryClassName));
    }
    return sslFactory;
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

  protected InputDataInfoProvider getInputDataInfoProvider() {
    if (inputDataInfoProvider == null) {
      inputDataInfoProvider = new DefaultInputDataInfoProvider(storeSetting, pushJobSetting, props);
    }
    return inputDataInfoProvider;
  }

  /**
   * Create a new instance of controller client and set it to the controller client field if the controller client field
   * has null value. If the controller client field is not null, it could mean:
   *    1. The controller client field has already been initialized
   *    2. A mock controller client is provided
   *
   * @param storeName
   * @param sslFactory
   * @param retryAttempts
   */
  private void initControllerClient(
      String storeName,
      String veniceControllerUrl,
      Optional<SSLFactory> sslFactory,
      int retryAttempts) {
    if (controllerClient == null) {
      controllerClient = ControllerClient
          .discoverAndConstructControllerClient(storeName, veniceControllerUrl, sslFactory, retryAttempts);
    } else {
      LOGGER.info("Controller client has already been initialized");
    }
    if (systemKMEStoreControllerClient == null) {
      systemKMEStoreControllerClient = ControllerClient.discoverAndConstructControllerClient(
          AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName(),
          veniceControllerUrl,
          sslFactory,
          retryAttempts);
    } else {
      LOGGER.info("System store controller client has already been initialized");
    }
  }

  private Optional<ByteBuffer> getCompressionDictionary() {
    ByteBuffer compressionDictionary = null;

    if (!pushJobSetting.isIncrementalPush && storeSetting.compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
      if (pushJobSetting.isSourceKafka) {
        LOGGER.info("Reading Zstd dictionary from input topic");
        // set up ssl properties and kafka consumer properties
        Properties kafkaConsumerProperties = new Properties();
        kafkaConsumerProperties.putAll(this.sslProperties.get());
        kafkaConsumerProperties
            .setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, props.getString(KAFKA_INPUT_BROKER_URL));
        compressionDictionary = DictionaryUtils
            .readDictionaryFromKafka(pushJobSetting.kafkaInputTopic, new VeniceProperties(kafkaConsumerProperties));
      } else {
        LOGGER.info("Training Zstd dictionary");
        if (!pushJobSetting.useMapperToBuildDict) {
          compressionDictionary = ByteBuffer.wrap(getInputDataInfoProvider().getZstdDictTrainSamples());
        } else {
          compressionDictionary = ByteBuffer.wrap("TODO".getBytes());
        }
      }
      LOGGER.info("Zstd dictionary size = {} bytes", compressionDictionary.limit());
    } else {
      LOGGER.info("No compression dictionary is generated with the strategy {}", storeSetting.compressionStrategy);
    }
    return Optional.ofNullable(compressionDictionary);
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
  protected String getInputURI(VeniceProperties props) throws Exception {
    if (pushJobSetting.isSourceKafka) {
      return "";
    }
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    String uri = props.getString(INPUT_PATH_PROP);
    Path sourcePath = getLatestPathOfInputDirectory(uri, fs);
    return sourcePath.toString();
  }

  private void initPushJobDetails() {
    pushJobDetails.clusterName = this.clusterName;
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
    pushJobDetails.pushJobConfigs =
        Collections.singletonMap(HEARTBEAT_ENABLED_CONFIG.getConfigName(), String.valueOf(jobLivenessHeartbeatEnabled));
  }

  private void updatePushJobDetailsWithCheckpoint(PushJobCheckpoints checkpoint) {
    pushJobDetails.pushJobLatestCheckpoint = checkpoint.getValue();
  }

  private void updatePushJobDetailsWithMRCounters() {
    if (runningJob == null) {
      LOGGER.info("No running job to update push job details with MR counters");
      return;
    }
    try {
      pushJobDetails.totalNumberOfRecords = MRJobCounterHelper.getOutputRecordsCount(runningJob.getCounters());
      pushJobDetails.totalKeyBytes = MRJobCounterHelper.getTotalKeySize(runningJob.getCounters());
      pushJobDetails.totalRawValueBytes = MRJobCounterHelper.getTotalUncompressedValueSize(runningJob.getCounters());
      pushJobDetails.totalCompressedValueBytes = MRJobCounterHelper.getTotalValueSize(runningJob.getCounters());
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
          pushJobConfigs.put(HEARTBEAT_ENABLED_CONFIG.getConfigName(), String.valueOf(jobLivenessHeartbeatEnabled));
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
   * Best effort attempt to get more details on reasons behind MR failure by looking at MR counters
   *
   * @return Error message if there is any error detected in the reporter counter and empty optional otherwise
   */
  private Optional<ErrorMessage> updatePushJobDetailsWithMRDetails() throws IOException {
    // Quota exceeded
    final long totalInputDataSizeInBytes = MRJobCounterHelper.getTotalKeySize(runningJob.getCounters())
        + MRJobCounterHelper.getTotalValueSize(runningJob.getCounters());
    if (inputStorageQuotaTracker.exceedQuota(totalInputDataSizeInBytes)) {
      updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.QUOTA_EXCEEDED);
      Long storeQuota = inputStorageQuotaTracker.getStoreStorageQuota();
      String errorMessage = String.format(
          "Storage quota exceeded. Store quota %s, Input data size %s."
              + " Please request at least %s additional quota.",
          generateHumanReadableByteCountString(storeQuota),
          generateHumanReadableByteCountString(totalInputDataSizeInBytes),
          generateHumanReadableByteCountString(totalInputDataSizeInBytes - storeQuota));
      return Optional.of(new ErrorMessage(errorMessage));
    }
    // Write ACL failed
    final long writeAclFailureCount = MRJobCounterHelper.getWriteAclAuthorizationFailureCount(runningJob.getCounters());
    if (writeAclFailureCount > 0) {
      updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.WRITE_ACL_FAILED);
      String errorMessage = "Insufficient ACLs to write to the store";
      return Optional.of(new ErrorMessage(errorMessage));
    }
    // Duplicate keys
    if (!pushJobSetting.isDuplicateKeyAllowed) {
      final long duplicateKeyWithDistinctValueCount =
          MRJobCounterHelper.getDuplicateKeyWithDistinctCount(runningJob.getCounters());
      if (duplicateKeyWithDistinctValueCount > 0) {
        updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.DUP_KEY_WITH_DIFF_VALUE);
        String errorMessage = String.format(
            "Input data has at least %d keys that appear more than once but have different values",
            duplicateKeyWithDistinctValueCount);
        return Optional.of(new ErrorMessage(errorMessage));
      }
    }
    // Record too large
    final long recordTooLargeFailureCount = MRJobCounterHelper.getRecordTooLargeFailureCount(runningJob.getCounters());
    if (recordTooLargeFailureCount > 0) {
      updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.RECORD_TOO_LARGE_FAILED);
      String errorMessage = String.format(
          "Input data has at least %d records that exceed the maximum record limit of %s",
          recordTooLargeFailureCount,
          generateHumanReadableByteCountString(
              getVeniceWriter(kafkaTopicInfo).getMaxSizeForUserPayloadPerMessageInBytes()));
      return Optional.of(new ErrorMessage(errorMessage));
    }
    return Optional.empty();
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
            int status = PushJobDetailsStatus.valueOf(coloEntry.getValue()).getValue();
            if (!pushJobDetails.coloStatus.containsKey(coloEntry.getKey())) {
              List<PushJobDetailsStatusTuple> newList = new ArrayList<>();
              newList.add(getPushJobDetailsStatusTuple(status));
              pushJobDetails.coloStatus.put(coloEntry.getKey(), newList);
            } else {
              List<PushJobDetailsStatusTuple> statuses = pushJobDetails.coloStatus.get(coloEntry.getKey());
              if (statuses.get(statuses.size() - 1).status != status) {
                // Only add the status if there is a change
                statuses.add(getPushJobDetailsStatusTuple(status));
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
    try {
      pushJobDetails.reportTimestamp = System.currentTimeMillis();
      int version = kafkaTopicInfo == null ? UNCREATED_VERSION_NUMBER : kafkaTopicInfo.version;
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
   * This method will talk to parent controller to validate key schema.
   */
  private void validateKeySchema(
      ControllerClient controllerClient,
      PushJobSetting setting,
      PushJobSchemaInfo pushJobSchemaInfo) {
    Schema serverSchema = getKeySchemaFromController(controllerClient, setting.controllerRetries, setting.storeName);
    Schema clientSchema = Schema.parse(pushJobSchemaInfo.getKeySchemaString());
    String canonicalizedServerSchema = AvroCompatibilityHelper.toParsingForm(serverSchema);
    String canonicalizedClientSchema = AvroCompatibilityHelper.toParsingForm(clientSchema);
    if (!canonicalizedServerSchema.equals(canonicalizedClientSchema)) {
      String briefErrorMessage = "Key schema mis-match for store " + setting.storeName;
      LOGGER.error(
          "{}\n\t\tController URLs: {}\n\t\tschema defined in HDFS: \t{}\n\t\tschema defined in Venice: \t{}",
          briefErrorMessage,
          controllerClient.getControllerDiscoveryUrls(),
          pushJobSchemaInfo.getKeySchemaString(),
          serverSchema.toString());
      throw new VeniceException(briefErrorMessage);
    }
  }

  protected void validateRemoteHybridSettings() {
    validateRemoteHybridSettings(pushJobSetting);
  }

  protected void validateRemoteHybridSettings(PushJobSetting setting) {
    if (setting.validateRemoteReplayPolicy != null) {
      StoreResponse response = ControllerClient
          .retryableRequest(controllerClient, setting.controllerRetries, c -> c.getStore(setting.storeName));
      if (response.isError()) {
        throw new VeniceException(
            "Failed to get store information to validate push settings! Error: " + response.getError());
      }
      HybridStoreConfig hybridStoreConfig = response.getStore().getHybridStoreConfig();
      if (!setting.validateRemoteReplayPolicy.equals(hybridStoreConfig.getBufferReplayPolicy())) {
        throw new VeniceException(
            String.format(
                "Remote rewind policy is {} but push settings require a policy of {}.  "
                    + "Please adjust hybrid settings or push job configuration!",
                hybridStoreConfig.getBufferReplayPolicy(),
                setting.validateRemoteReplayPolicy));
      }
    }
  }

  private void validateKafkaMessageEnvelopeSchema(PushJobSetting setting) {
    SchemaResponse response = ControllerClient.retryableRequest(
        systemKMEStoreControllerClient,
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
  private void validateValueSchema(
      ControllerClient controllerClient,
      PushJobSetting setting,
      PushJobSchemaInfo pushJobSchemaInfo,
      boolean schemaAutoRegisterFromPushJobEnabled) {
    LOGGER
        .info("Validating value schema: {} for store: {}", pushJobSchemaInfo.getValueSchemaString(), setting.storeName);

    SchemaResponse getValueSchemaIdResponse;

    if (setting.enableWriteCompute) {
      getValueSchemaIdResponse = ControllerClient.retryableRequest(
          controllerClient,
          setting.controllerRetries,
          c -> c.getValueOrDerivedSchemaId(setting.storeName, pushJobSchemaInfo.getValueSchemaString()));
    } else {
      getValueSchemaIdResponse = ControllerClient.retryableRequest(
          controllerClient,
          setting.controllerRetries,
          c -> c.getValueSchemaID(setting.storeName, pushJobSchemaInfo.getValueSchemaString()));
    }
    if (getValueSchemaIdResponse.isError() && !schemaAutoRegisterFromPushJobEnabled) {
      throw new VeniceException(
          "Failed to validate value schema for store: " + setting.storeName + "\nError from the server: "
              + getValueSchemaIdResponse.getError() + "\nSchema for the data file: "
              + pushJobSchemaInfo.getValueSchemaString());
    }

    if (getValueSchemaIdResponse.isError() && schemaAutoRegisterFromPushJobEnabled) {
      LOGGER.info(
          "Auto registering value schema: {} for store: {}",
          pushJobSchemaInfo.getValueSchemaString(),
          setting.storeName);
      SchemaResponse addValueSchemaResponse = ControllerClient.retryableRequest(
          controllerClient,
          setting.controllerRetries,
          c -> c.addValueSchema(setting.storeName, pushJobSchemaInfo.getValueSchemaString()));
      if (addValueSchemaResponse.isError()) {
        throw new VeniceException(
            "Failed to auto-register value schema for store: " + setting.storeName + "\nError from the server: "
                + addValueSchemaResponse.getError() + "\nSchema for the data file: "
                + pushJobSchemaInfo.getValueSchemaString());
      }
      // Add value schema successfully
      setSchemaIdPropInSchemaInfo(pushJobSchemaInfo, addValueSchemaResponse, setting.enableWriteCompute);

    } else {
      // Get value schema ID successfully
      setSchemaIdPropInSchemaInfo(pushJobSchemaInfo, getValueSchemaIdResponse, setting.enableWriteCompute);
    }
    LOGGER.info(
        "Got schema id: {} for value schema: {} of store: {}",
        pushJobSchemaInfo.getValueSchemaId(),
        pushJobSchemaInfo.getValueSchemaString(),
        setting.storeName);
  }

  private void setSchemaIdPropInSchemaInfo(
      PushJobSchemaInfo pushJobSchemaInfo,
      SchemaResponse valueSchemaResponse,
      boolean enableWriteCompute) {
    pushJobSchemaInfo.setValueSchemaId(valueSchemaResponse.getId());
    if (enableWriteCompute) {
      pushJobSchemaInfo.setDerivedSchemaId(valueSchemaResponse.getDerivedSchemaId());
    }
  }

  private StoreSetting getSettingsFromController(ControllerClient controllerClient, PushJobSetting setting) {
    StoreSetting storeSetting = new StoreSetting();
    StoreResponse storeResponse = ControllerClient
        .retryableRequest(controllerClient, setting.controllerRetries, c -> c.getStore(setting.storeName));

    if (storeResponse.isError()) {
      throw new VeniceException("Can't get store info. " + storeResponse.getError());
    }
    storeSetting.storeStorageQuota = storeResponse.getStore().getStorageQuotaInByte();
    storeSetting.isSchemaAutoRegisterFromPushJobEnabled =
        storeResponse.getStore().isSchemaAutoRegisterFromPushJobEnabled();
    storeSetting.isChunkingEnabled = storeResponse.getStore().isChunkingEnabled();
    storeSetting.compressionStrategy = storeResponse.getStore().getCompressionStrategy();
    storeSetting.isWriteComputeEnabled = storeResponse.getStore().isWriteComputationEnabled();
    storeSetting.isLeaderFollowerModelEnabled = storeResponse.getStore().isLeaderFollowerModelEnabled();
    storeSetting.isIncrementalPushEnabled = storeResponse.getStore().isIncrementalPushEnabled();

    if (setting.enableWriteCompute && !storeSetting.isWriteComputeEnabled) {
      throw new VeniceException("Store does not have write compute enabled.");
    }

    if (setting.enableWriteCompute && (!storeSetting.isIncrementalPushEnabled || !setting.isIncrementalPush)) {
      throw new VeniceException("Write compute is only available for incremental push jobs.");
    }

    if (setting.enableWriteCompute && storeSetting.isWriteComputeEnabled) {
      /*
        If write compute is enabled, we would perform a topic switch from the controller and have the
        controller be in charge of broadcasting start and end messages. We will disable
        sendControlMessagesDirectly to prevent races between the messages sent by the VenicePushJob and
        by the controller for topic switch.
       */
      setting.sendControlMessagesDirectly = false;
      if (!storeSetting.isLeaderFollowerModelEnabled) {
        throw new VeniceException("Leader follower mode needs to be enabled for write compute.");
      }
    }

    if (setting.isSourceKafka) {
      int sourceVersionNumber = Version.parseVersionFromKafkaTopicName(pushJobSetting.kafkaInputTopic);
      Optional<Version> sourceVersion = storeResponse.getStore().getVersion(sourceVersionNumber);

      if (!sourceVersion.isPresent()) {
        if (pushJobSetting.repushInfoResponse != null
            && pushJobSetting.repushInfoResponse.getRepushInfo().getVersion().getNumber() == sourceVersionNumber) {
          LOGGER.warn("Could not find version {} in parent colo, fetching from child colo.", sourceVersionNumber);
          sourceVersion = Optional.of(pushJobSetting.repushInfoResponse.getRepushInfo().getVersion());
        } else {
          throw new VeniceException(
              "Could not find version " + sourceVersionNumber + ", please provide input fabric to repush.");
        }
      }
      if (storeSetting.isWriteComputeEnabled && sourceVersion.get().isActiveActiveReplicationEnabled()) {
        throw new VeniceException("KIF repush is not available for for write compute active/active store.");
      }
      storeSetting.sourceKafkaInputVersionInfo = sourceVersion.get();
      // Skip quota check
      storeSetting.storeStorageQuota = Store.UNLIMITED_STORAGE_QUOTA;

      storeSetting.isChunkingEnabled = sourceVersion.get().isChunkingEnabled();

      /**
       * If the source topic is using compression algorithm, we will keep the compression in the new topic. There are two cases:
       * 1. The source topic uses ZSTD with dictionary, we will copy the dictionary from source topic to new topic and pass-through.
       * 2. Other compression algos like Gzip, we will pass-through the compressed msgs to new topic.
       * For both cases, VPJ won't compress it again.
       */
      if (storeSetting.sourceKafkaInputVersionInfo.getCompressionStrategy()
          .equals(CompressionStrategy.ZSTD_WITH_DICT)) {
        storeSetting.compressionStrategy = CompressionStrategy.ZSTD_WITH_DICT;
      } else {
        storeSetting.compressionStrategy = CompressionStrategy.NO_OP;
      }
    }
    return storeSetting;
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
  private void createNewStoreVersion(
      PushJobSetting setting,
      long inputFileDataSize,
      ControllerClient controllerClient,
      String pushId,
      VeniceProperties props,
      Optional<ByteBuffer> optionalCompressionDictionary) {
    Version.PushType pushType = getPushType(setting);
    boolean askControllerToSendControlMessage = !pushJobSetting.sendControlMessagesDirectly;
    Optional<String> partitioners;
    if (props.containsKey(VENICE_PARTITIONERS_PROP)) {
      partitioners = Optional.of(props.getString(VENICE_PARTITIONERS_PROP));
    } else {
      partitioners = Optional.of(DefaultVenicePartitioner.class.getName());
    }

    Optional<String> dictionary;
    if (askControllerToSendControlMessage) {
      dictionary = optionalCompressionDictionary.map(ByteBuffer::array).map(EncodingUtils::base64EncodeToString);
    } else {
      dictionary = Optional.empty();
    }

    boolean writeComputeEnabled = false;

    if (storeSetting.isWriteComputeEnabled && setting.enableWriteCompute) {
      writeComputeEnabled = true;
    }

    // If WriteCompute is enabled, request for intermediate topic
    final boolean finalWriteComputeEnabled = writeComputeEnabled;
    kafkaTopicInfo = new VersionTopicInfo();
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
            partitioners,
            dictionary,
            Optional.ofNullable(setting.sourceGridFabric),
            jobLivenessHeartbeatEnabled,
            setting.rewindTimeInSecondsOverride,
            setting.deferVersionSwap));
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
      LOGGER.info(versionCreationResponse.toString());
    }
    kafkaTopicInfo.topic = versionCreationResponse.getKafkaTopic();
    kafkaTopicInfo.version = versionCreationResponse.getVersion();
    kafkaTopicInfo.kafkaUrl = versionCreationResponse.getKafkaBootstrapServers();
    kafkaTopicInfo.partitionCount = versionCreationResponse.getPartitions();
    kafkaTopicInfo.sslToKafka = versionCreationResponse.isEnableSSL();
    kafkaTopicInfo.compressionStrategy = versionCreationResponse.getCompressionStrategy();
    kafkaTopicInfo.partitionerClass = versionCreationResponse.getPartitionerClass();
    kafkaTopicInfo.partitionerParams = versionCreationResponse.getPartitionerParams();
    kafkaTopicInfo.amplificationFactor = versionCreationResponse.getAmplificationFactor();
    kafkaTopicInfo.daVinciPushStatusStoreEnabled = versionCreationResponse.isDaVinciPushStatusStoreEnabled();
    kafkaTopicInfo.chunkingEnabled = storeSetting.isChunkingEnabled && !Version.isRealTimeTopic(kafkaTopicInfo.topic);

    if (pushJobSetting.isSourceKafka) {
      /**
       * Check whether the new version setup is compatible with the source version, and we will check the following configs:
       * 1. Chunking.
       * 2. Compression Strategy.
       * 3. Partition Count.
       * 4. Partitioner Config.
       * 5. Incremental Push policy
       * Since right now, the messages from the source topic will be passed through to the new version topic without
       * reformatting, we need to make sure the pass-through messages won't violate the new version config.
       *
       * TODO: maybe we should fail fast before creating a new version.
       */
      StoreResponse storeResponse = ControllerClient
          .retryableRequest(controllerClient, setting.controllerRetries, c -> c.getStore(setting.storeName));
      if (storeResponse.isError()) {
        throw new VeniceException(
            "Failed to retrieve store response with urls: " + setting.veniceControllerUrl + ", error: "
                + storeResponse.getError());
      }
      int newVersionNum = kafkaTopicInfo.version;
      Optional<Version> newVersionOptional = storeResponse.getStore().getVersion(newVersionNum);
      if (!newVersionOptional.isPresent()) {
        throw new VeniceException(
            "Couldn't fetch the newly created version: " + newVersionNum + " for store: " + setting.storeName
                + " with urls: " + setting.veniceControllerUrl);
      }
      Version newVersion = newVersionOptional.get();
      storeSetting.sourceKafkaOutputVersionInfo = newVersion;
      Version sourceVersion = storeSetting.sourceKafkaInputVersionInfo;

      if (sourceVersion.getCompressionStrategy() != newVersion.getCompressionStrategy()) {
        throw new VeniceException(
            "Compression strategy mismatch between the source version and the new version is "
                + "not supported by Kafka Input right now, source version: " + sourceVersion.getNumber() + " is using: "
                + sourceVersion.getCompressionStrategy() + ", new version: " + newVersion.getNumber() + " is using: "
                + newVersion.getCompressionStrategy());
      }

      // Chunked source version cannot be repushed if new version is not chunking enabled.
      if (sourceVersion.isChunkingEnabled() && !newVersion.isChunkingEnabled()) {
        throw new VeniceException(
            "Chunking config mismatch between the source and the new version of store "
                + storeResponse.getStore().getName() + ". Source version: " + sourceVersion.getNumber() + " is using: "
                + sourceVersion.isChunkingEnabled() + ", new version: " + newVersion.getNumber() + " is using: "
                + newVersion.isChunkingEnabled());
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

  private synchronized VeniceWriter<KafkaKey, byte[], byte[]> getVeniceWriter(VersionTopicInfo versionTopicInfo) {
    if (veniceWriter == null) {
      // Initialize VeniceWriter
      VeniceWriterFactory veniceWriterFactory = new VeniceWriterFactory(getVeniceWriterProperties(versionTopicInfo));
      Properties partitionerProperties = new Properties();
      partitionerProperties.putAll(versionTopicInfo.partitionerParams);
      VenicePartitioner venicePartitioner = PartitionUtils.getVenicePartitioner(
          versionTopicInfo.partitionerClass,
          versionTopicInfo.amplificationFactor,
          new VeniceProperties(partitionerProperties));
      VeniceWriterOptions vwOptions =
          new VeniceWriterOptions.Builder(versionTopicInfo.topic).setUseKafkaKeySerializer(true)
              .setPartitioner(venicePartitioner)
              .setPartitionCount(
                  Optional.of(
                      Version.isVersionTopic(versionTopicInfo.topic)
                          ? versionTopicInfo.partitionCount * versionTopicInfo.amplificationFactor
                          : versionTopicInfo.partitionCount))
              .build();
      VeniceWriter<KafkaKey, byte[], byte[]> newVeniceWriter = veniceWriterFactory.createVeniceWriter(vwOptions);
      LOGGER.info("Created VeniceWriter: {}", newVeniceWriter);
      veniceWriter = newVeniceWriter;
    }
    return veniceWriter;
  }

  private synchronized Properties getVeniceWriterProperties(VersionTopicInfo versionTopicInfo) {
    if (veniceWriterProperties == null) {
      veniceWriterProperties = createVeniceWriterProperties(versionTopicInfo.kafkaUrl, versionTopicInfo.sslToKafka);
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
      veniceWriterProperties.putAll(this.sslProperties.get());
    }
    if (props.containsKey(KAFKA_PRODUCER_REQUEST_TIMEOUT_MS)) {
      veniceWriterProperties
          .setProperty(KAFKA_PRODUCER_REQUEST_TIMEOUT_MS, props.getString(KAFKA_PRODUCER_REQUEST_TIMEOUT_MS));
    } else {
      veniceWriterProperties.setProperty(KAFKA_PRODUCER_REQUEST_TIMEOUT_MS, Integer.toString(Integer.MAX_VALUE));
    }
    if (props.containsKey(KAFKA_PRODUCER_RETRIES_CONFIG)) {
      veniceWriterProperties.setProperty(KAFKA_PRODUCER_RETRIES_CONFIG, props.getString(KAFKA_PRODUCER_RETRIES_CONFIG));
    } else {
      veniceWriterProperties.setProperty(KAFKA_PRODUCER_RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    }
    if (props.containsKey(KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS)) {
      veniceWriterProperties
          .setProperty(KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS, props.getString(KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS));
    } else {
      veniceWriterProperties.setProperty(KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS, Integer.toString(Integer.MAX_VALUE));
    }
    return veniceWriterProperties;
  }

  /**
   * Build ssl properties based on the hadoop token file.
   */
  private Properties getSslProperties(VeniceProperties allProperties) throws IOException {
    Properties newSslProperties = new Properties();
    // SSL_ENABLED is needed in SSLFactory
    newSslProperties.setProperty(SSL_ENABLED, "true");
    newSslProperties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, KAFKA_SECURITY_PROTOCOL);
    allProperties.keySet()
        .stream()
        .filter(key -> key.toLowerCase().startsWith(SSL_PREFIX))
        .forEach(key -> newSslProperties.setProperty(key, allProperties.getString(key)));
    SSLConfigurator sslConfigurator = SSLConfigurator.getSSLConfigurator(
        allProperties.getString(SSL_CONFIGURATOR_CLASS_CONFIG, TempFileSSLConfigurator.class.getName()));

    Properties sslWriterProperties =
        sslConfigurator.setupSSLConfig(newSslProperties, UserCredentialsFactory.getUserCredentialsFromTokenFile());
    newSslProperties.putAll(sslWriterProperties);
    return newSslProperties;
  }

  private synchronized void closeVeniceWriter() {
    if (veniceWriter != null) {
      veniceWriter.close();
      veniceWriter = null;
    }
  }

  /**
   * High level, we want to poll the consumption job status until it errors or is complete.  This is more complicated
   * because we might be dealing with multiple destination clusters and we might not be able to reach all of them. We
   * are using a semantic of "poll until all accessible datacenters report success".
   *
   * If any datacenter report an explicit error status, we throw an exception and fail the job. However, datacenters
   * with COMPLETED status will be serving new data.
   */
  private void pollStatusUntilComplete(
      Optional<String> incrementalPushVersion,
      ControllerClient controllerClient,
      PushJobSetting pushJobSetting,
      VersionTopicInfo versionTopicInfo) {
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

    String topicToMonitor = getTopicToMonitor(versionTopicInfo, pushJobSetting);

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
          client -> client.queryOverallJobStatus(topicToMonitor, incrementalPushVersion));
      if (response.isError()) {
        // status could not be queried which could be due to a communication error.
        throw new VeniceException(
            "Failed to connect to: " + pushJobSetting.veniceControllerUrl + " to query job status, after "
                + pushJobSetting.controllerStatusPollRetries + " attempts.");
      }

      previousOverallDetails = printJobStatus(response, previousOverallDetails, previousExtraDetails);
      ExecutionStatus overallStatus = ExecutionStatus.valueOf(response.getStatus());
      Map<String, String> regionSpecificInfo = response.getExtraInfo();
      // Note that it's intended to update the push job details before updating the completed datacenter set.
      updatePushJobDetailsWithColoStatus(regionSpecificInfo, completedDatacenters);
      regionSpecificInfo.forEach((region, regionStatus) -> {
        ExecutionStatus datacenterStatus = ExecutionStatus.valueOf(regionStatus);
        if (datacenterStatus.isTerminal() && !datacenterStatus.equals(ExecutionStatus.ERROR)) {
          completedDatacenters.add(region);
        }
      });

      if (overallStatus.isTerminal()) {
        if (completedDatacenters.size() != regionSpecificInfo.size() || !successfulStatuses.contains(overallStatus)) {
          // One or more DC could have an UNKNOWN status and never successfully reported a completed status before,
          // but if the majority of datacenters have completed, we give up on the unreachable datacenter
          // and start truncating the data topic.
          throw new VeniceException(
              "Push job error reported by controller: " + pushJobSetting.veniceControllerUrl + "\ncontroller response: "
                  + response.toString());
        }

        // Every known datacenter have successfully reported a completed status at least once.
        LOGGER.info("Successfully pushed {}", versionTopicInfo.topic);
        return;
      }

      if (!overallStatus.equals(ExecutionStatus.UNKNOWN)) {
        unknownStateStartTimeMs = 0;
      } else if (unknownStateStartTimeMs == 0) {
        unknownStateStartTimeMs = System.currentTimeMillis();
      } else if (System.currentTimeMillis() < unknownStateStartTimeMs
          + pushJobSetting.jobStatusInUnknownStateTimeoutMs) {
        double elapsedMinutes = (double) (System.currentTimeMillis() - unknownStateStartTimeMs) / Time.MS_PER_MINUTE;
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
      LOGGER.info("\t\tNew overall details: {}", details.get());
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

  protected void setupMRConf(
      JobConf jobConf,
      VersionTopicInfo versionTopicInfo,
      PushJobSetting pushJobSetting,
      PushJobSchemaInfo pushJobSchemaInfo,
      StoreSetting storeSetting,
      VeniceProperties props,
      String id,
      String inputDirectory) {
    setupDefaultJobConf(jobConf, versionTopicInfo, pushJobSetting, pushJobSchemaInfo, storeSetting, props, id);
    setupInputFormatConf(jobConf, pushJobSchemaInfo, inputDirectory);
    setupReducerConf(jobConf, pushJobSetting, versionTopicInfo);
  }

  private void setupPBNJConf(
      JobConf jobConf,
      VersionTopicInfo versionTopicInfo,
      PushJobSetting pushJobSetting,
      PushJobSchemaInfo pushJobSchemaInfo,
      StoreSetting storeSetting,
      VeniceProperties props,
      String id,
      String inputDirectory) {
    if (!pushJobSchemaInfo.isAvro()) {
      throw new VeniceException("PBNJ only supports Avro input format");
    }

    setupMRConf(jobConf, versionTopicInfo, pushJobSetting, pushJobSchemaInfo, storeSetting, props, id, inputDirectory);
    jobConf.set(VENICE_STORE_NAME_PROP, pushJobSetting.storeName);
    jobConf.set(PBNJ_ROUTER_URL_PROP, pushJobSetting.veniceRouterUrl);
    jobConf.set(PBNJ_FAIL_FAST, Boolean.toString(pushJobSetting.pbnjFailFast));
    jobConf.set(PBNJ_ASYNC, Boolean.toString(pushJobSetting.pbnjAsync));
    jobConf.set(PBNJ_SAMPLING_RATIO_PROP, Double.toString(pushJobSetting.pbnjSamplingRatio));
    jobConf.set(STORAGE_QUOTA_PROP, Long.toString(Store.UNLIMITED_STORAGE_QUOTA));
    jobConf.setMapperClass(PostBulkLoadAnalysisMapper.class);
  }

  /**
   * Common configuration for all the Mapreduce Jobs run as part of VPJ
   * @param conf
   * @param jobName
   */
  private void setupCommonJobConf(JobConf conf, PushJobSchemaInfo pushJobSchemaInfo, String jobName) {
    if (System.getenv(HADOOP_TOKEN_FILE_LOCATION) != null) {
      conf.set(MAPREDUCE_JOB_CREDENTIALS_BINARY, System.getenv(HADOOP_TOKEN_FILE_LOCATION));
    }
    conf.setJobName(jobName);
    conf.setJarByClass(this.getClass());

    conf.set(
        SSL_CONFIGURATOR_CLASS_CONFIG,
        props.getString(SSL_CONFIGURATOR_CLASS_CONFIG, TempFileSSLConfigurator.class.getName()));
    conf.set(SSL_KEY_STORE_PROPERTY_NAME, props.getString(SSL_KEY_STORE_PROPERTY_NAME));
    conf.set(SSL_TRUST_STORE_PROPERTY_NAME, props.getString(SSL_TRUST_STORE_PROPERTY_NAME));
    conf.set(SSL_KEY_PASSWORD_PROPERTY_NAME, props.getString(SSL_KEY_PASSWORD_PROPERTY_NAME));

    // Hadoop2 dev cluster provides a newer version of an avro dependency.
    // Set mapreduce.job.classloader to true to force the use of the older avro dependency.
    conf.setBoolean(MAPREDUCE_JOB_CLASSLOADER, true);
    LOGGER.info("{}: {}", MAPREDUCE_JOB_CLASSLOADER, conf.get(MAPREDUCE_JOB_CLASSLOADER));

    /** Not writing anything to the output for key and value and so the format is Null:
     * Can be overwritten later for specific settings */
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(NullWritable.class);
    conf.setOutputFormat(NullOutputFormat.class);
  }

  protected void setupDefaultJobConf(
      JobConf conf,
      VersionTopicInfo versionTopicInfo,
      PushJobSetting pushJobSetting,
      PushJobSchemaInfo pushJobSchemaInfo,
      StoreSetting storeSetting,
      VeniceProperties props,
      String id) {
    setupCommonJobConf(conf, pushJobSchemaInfo, id + ":venice_push_job-" + versionTopicInfo.topic);
    conf.set(BATCH_NUM_BYTES_PROP, Integer.toString(pushJobSetting.batchNumBytes));
    conf.set(TOPIC_PROP, versionTopicInfo.topic);
    // We need the two configs with bootstrap servers since VeniceWriterFactory requires kafka.bootstrap.servers while
    // the Kafka consumer requires bootstrap.servers.
    conf.set(KAFKA_BOOTSTRAP_SERVERS, versionTopicInfo.kafkaUrl);
    conf.set(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, versionTopicInfo.kafkaUrl);
    conf.set(COMPRESSION_STRATEGY, versionTopicInfo.compressionStrategy.toString());
    conf.set(PARTITIONER_CLASS, versionTopicInfo.partitionerClass);
    // flatten partitionerParams since JobConf class does not support set an object
    versionTopicInfo.partitionerParams.forEach(conf::set);
    conf.setInt(AMPLIFICATION_FACTOR, versionTopicInfo.amplificationFactor);
    if (versionTopicInfo.sslToKafka) {
      conf.set(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, KAFKA_SECURITY_PROTOCOL);
      props.keySet().stream().filter(key -> key.toLowerCase().startsWith(SSL_PREFIX)).forEach(key -> {
        conf.set(key, props.getString(key));
      });
    }
    conf.setBoolean(ALLOW_DUPLICATE_KEY, pushJobSetting.isDuplicateKeyAllowed);
    conf.setBoolean(VeniceWriter.ENABLE_CHUNKING, kafkaTopicInfo.chunkingEnabled);

    conf.set(STORAGE_QUOTA_PROP, Long.toString(storeSetting.storeStorageQuota));

    /** Allow overriding properties if their names start with {@link HADOOP_PREFIX}.
     *  Allow overriding properties if their names start with {@link VeniceWriter.VENICE_WRITER_CONFIG_PREFIX}
     *  Allow overriding properties if their names start with {@link ApacheKafkaProducer.PROPERTIES_KAFKA_PREFIX}
     *  Allow overriding properties if their names start with {@link KafkaInputRecordReader.KAFKA_INPUT_RECORD_READER_KAFKA_CONFIG_PREFIX}
     **/
    List<String> passThroughPrefixList = Arrays.asList(
        VeniceWriter.VENICE_WRITER_CONFIG_PREFIX,
        ApacheKafkaProducer.PROPERTIES_KAFKA_PREFIX,
        KafkaInputRecordReader.KIF_RECORD_READER_KAFKA_CONFIG_PREFIX);
    int passThroughPrefixListSize = passThroughPrefixList.size();
    if (passThroughPrefixListSize > 1) {
      /**
       * The following logic will make sure there are no two prefixes, which will be fully overlapped.
       * The algo is slightly inefficient, but it should be good enough for VPJ.
       */
      for (int i = 0; i < passThroughPrefixListSize; ++i) {
        for (int j = 0; j < passThroughPrefixListSize; ++j) {
          if (i != j) {
            String prefixI = passThroughPrefixList.get(i);
            String prefixJ = passThroughPrefixList.get(j);
            if (prefixI.startsWith(prefixJ)) {
              throw new VeniceException("Prefix: " + prefixJ + " shouldn't be a prefix of another prefix: " + prefixI);
            }
          }
        }
      }
    }

    for (String key: props.keySet()) {
      String lowerCase = key.toLowerCase();
      if (lowerCase.startsWith(HADOOP_PREFIX)) {
        String overrideKey = key.substring(HADOOP_PREFIX.length());
        conf.set(overrideKey, props.getString(key));
      }
      for (String prefix: passThroughPrefixList) {
        if (lowerCase.startsWith(prefix)) {
          conf.set(key, props.getString(key));
          break;
        }
      }
    }

    if (pushJobSetting.isSourceKafka) {
      // Use some fake value schema id here since it won't be used
      conf.setInt(VALUE_SCHEMA_ID_PROP, -1);
      /**
       * Kafka input topic could be inferred from the store name, but absent from the original properties.
       * So here will set it up from {@link #pushJobSetting}.
       */
      conf.set(KAFKA_INPUT_TOPIC, pushJobSetting.kafkaInputTopic);
      conf.set(KAFKA_INPUT_BROKER_URL, pushJobSetting.kafkaInputBrokerUrl);
      conf.setLong(REPUSH_TTL_IN_HOURS, pushJobSetting.repushTtlInHours);
      conf.setInt(REPUSH_TTL_POLICY, TTLResolutionPolicy.RT_WRITE_ONLY.getValue()); // only support one policy thus not
                                                                                    // allow any value passed in.
    } else {
      conf.setInt(VALUE_SCHEMA_ID_PROP, pushJobSchemaInfo.getValueSchemaId());
      conf.setInt(DERIVED_SCHEMA_ID_PROP, pushJobSchemaInfo.getDerivedSchemaId());
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

    conf.set(
        ZSTD_COMPRESSION_LEVEL,
        props.getString(ZSTD_COMPRESSION_LEVEL, String.valueOf(Zstd.maxCompressionLevel())));
    conf.set(ETL_VALUE_SCHEMA_TRANSFORMATION, pushJobSetting.etlValueSchemaTransformation.name());
    conf.setBoolean(EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED, pushJobSetting.extendedSchemaValidityCheckEnabled);
  }

  protected void setupInputFormatConf(JobConf jobConf, PushJobSchemaInfo pushJobSchemaInfo, String inputDirectory) {
    if (pushJobSetting.isSourceKafka) {
      Schema keySchemaFromController = getKeySchemaFromController(controllerClient, 3, pushJobSetting.storeName);
      String keySchemaString = AvroCompatibilityHelper.toParsingForm(keySchemaFromController);
      jobConf.set(KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP, keySchemaString);
      jobConf.setInputFormat(KafkaInputFormat.class);
      jobConf.setMapperClass(VeniceKafkaInputMapper.class);
      if (pushJobSetting.kafkaInputCombinerEnabled) {
        jobConf.setCombinerClass(KafkaInputFormatCombiner.class);
      }
    } else {
      // TODO:The job is using path-filter to check the consistency of avro file schema ,
      // but doesn't specify the path filter for the input directory of map-reduce job.
      // We need to revisit it if any failure because of this happens.
      FileInputFormat.setInputPaths(jobConf, new Path(inputDirectory));

      /**
       * Include all the files in the input directory no matter whether the file is with '.avro' suffix or not
       * to keep the logic consistent with {@link #checkAvroSchemaConsistency(FileStatus[])}.
       */
      jobConf.set(AvroInputFormat.IGNORE_FILES_WITHOUT_EXTENSION_KEY, "false");

      jobConf.set(KEY_FIELD_PROP, pushJobSchemaInfo.getKeyField());
      jobConf.set(VALUE_FIELD_PROP, pushJobSchemaInfo.getValueField());

      if (pushJobSchemaInfo.isAvro()) {
        jobConf.set(SCHEMA_STRING_PROP, pushJobSchemaInfo.getFileSchemaString());
        jobConf.set(AvroJob.INPUT_SCHEMA, pushJobSchemaInfo.getFileSchemaString());
        jobConf.setClass("avro.serialization.data.model", GenericData.class, GenericData.class);
        jobConf.setInputFormat(AvroInputFormat.class);
        jobConf.setMapperClass(VeniceAvroMapper.class);
        jobConf.setBoolean(VSON_PUSH, false);
      } else {
        jobConf.setInputFormat(VsonSequenceFileInputFormat.class);
        jobConf.setMapperClass(VeniceVsonMapper.class);
        jobConf.setBoolean(VSON_PUSH, true);
        jobConf.set(FILE_KEY_SCHEMA, pushJobSchemaInfo.getVsonFileKeySchema());
        jobConf.set(FILE_VALUE_SCHEMA, pushJobSchemaInfo.getVsonFileValueSchema());
      }
    }
  }

  private void setupReducerConf(JobConf jobConf, PushJobSetting pushJobSetting, VersionTopicInfo versionTopicInfo) {
    jobConf.setPartitionerClass(this.mapRedPartitionerClass);
    jobConf.setReduceSpeculativeExecution(pushJobSetting.enableReducerSpeculativeExecution);
    jobConf.setNumReduceTasks(versionTopicInfo.partitionCount * versionTopicInfo.amplificationFactor);
    jobConf.setMapOutputKeyClass(BytesWritable.class);
    jobConf.setMapOutputValueClass(BytesWritable.class);
    if (pushJobSetting.isSourceKafka) {
      jobConf.setReducerClass(VeniceKafkaInputReducer.class);
    } else {
      jobConf.setReducerClass(VeniceReducer.class);
    }
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
      PushJobSchemaInfo pushJobSchemaInfo,
      StoreSetting storeSetting,
      VeniceProperties props,
      String id,
      String inputDirectory) throws IOException {
    setupMRConfToValidateSchemaAndBuildDict(
        conf,
        pushJobSetting,
        pushJobSchemaInfo,
        storeSetting,
        props,
        id,
        inputDirectory);
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
      PushJobSchemaInfo pushJobSchemaInfo,
      StoreSetting storeSetting,
      VeniceProperties props,
      String id,
      String inputDirectory) {
    setupDefaultJobConfToValidateSchemaAndBuildDict(conf, pushJobSetting, pushJobSchemaInfo, storeSetting, props, id);
    setupInputFormatConfToValidateSchemaAndBuildDict(conf, pushJobSchemaInfo, inputDirectory);
  }

  /**
   * Default config includes the details related to jobids, output formats, compression configs, ssl configs, etc.
   * @param conf
   * @param id
   */
  private void setupDefaultJobConfToValidateSchemaAndBuildDict(
      JobConf conf,
      PushJobSetting pushJobSetting,
      PushJobSchemaInfo pushJobSchemaInfo,
      StoreSetting storeSetting,
      VeniceProperties props,
      String id) {
    setupCommonJobConf(
        conf,
        pushJobSchemaInfo,
        id + ":venice_push_job_validate_schema_and_build_dict-" + pushJobSetting.storeName);

    conf.set(VENICE_STORE_NAME_PROP, pushJobSetting.storeName);
    conf.set(ETL_VALUE_SCHEMA_TRANSFORMATION, pushJobSetting.etlValueSchemaTransformation.name());
    conf.setBoolean(INCREMENTAL_PUSH, pushJobSetting.isIncrementalPush);
    conf.setLong(INPUT_PATH_LAST_MODIFIED_TIME, inputModificationTime);

    /** Compression related config */
    conf.setInt(
        DefaultInputDataInfoProvider.COMPRESSION_DICTIONARY_SIZE_LIMIT,
        props.getInt(
            DefaultInputDataInfoProvider.COMPRESSION_DICTIONARY_SIZE_LIMIT,
            VeniceWriter.DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES));
    conf.setInt(
        DefaultInputDataInfoProvider.COMPRESSION_DICTIONARY_SAMPLE_SIZE,
        props.getInt(
            DefaultInputDataInfoProvider.COMPRESSION_DICTIONARY_SAMPLE_SIZE,
            DefaultInputDataInfoProvider.DEFAULT_COMPRESSION_DICTIONARY_SAMPLE_SIZE));
    conf.set(COMPRESSION_STRATEGY, storeSetting.compressionStrategy.toString());
    conf.setBoolean(COMPRESSION_METRIC_COLLECTION_ENABLED, pushJobSetting.compressionMetricCollectionEnabled);
    conf.setBoolean(USE_MAPPER_TO_BUILD_DICTIONARY, pushJobSetting.useMapperToBuildDict);

    /** adding below for AbstractMapReduceTask.configure() to not crash: Doesn't affect this flow */
    conf.setBoolean(VeniceWriter.ENABLE_CHUNKING, false);

    /** Allow overriding properties if their names start with {@link HADOOP_VALIDATE_SCHEMA_AND_BUILD_DICT_PREFIX} */
    for (String key: props.keySet()) {
      String lowerCase = key.toLowerCase();
      if (lowerCase.startsWith(HADOOP_VALIDATE_SCHEMA_AND_BUILD_DICT_PREFIX)) {
        String overrideKey = key.substring(HADOOP_VALIDATE_SCHEMA_AND_BUILD_DICT_PREFIX.length());
        conf.set(overrideKey, props.getString(key));
      }
    }

    conf.setNumReduceTasks(0);
  }

  protected void setupInputFormatConfToValidateSchemaAndBuildDict(
      JobConf conf,
      PushJobSchemaInfo pushJobSchemaInfo,
      String inputDirectory) {
    conf.set(INPUT_PATH_PROP, inputDirectory);

    conf.setInputFormat(VeniceFileInputFormat.class);
    conf.setMapperClass(ValidateSchemaAndBuildDictMapper.class);

    /** key/value fields to be used in {@link DefaultInputDataInfoProvider#validateInputAndGetInfo(String)} in the mapper
     * These values were populated to schemaInfo in the same function but in driver */
    conf.set(KEY_FIELD_PROP, pushJobSchemaInfo.getKeyField());
    conf.set(VALUE_FIELD_PROP, pushJobSchemaInfo.getValueField());
  }

  private void logPushJobProperties(
      VersionTopicInfo versionTopicInfo,
      PushJobSetting pushJobSetting,
      PushJobSchemaInfo pushJobSchemaInfo,
      String clusterName,
      String inputDirectory,
      long inputFileDataSize) {
    LOGGER.info(
        pushJobPropertiesToString(
            versionTopicInfo,
            pushJobSetting,
            pushJobSchemaInfo,
            clusterName,
            inputDirectory,
            inputFileDataSize));
  }

  private String pushJobPropertiesToString(
      VersionTopicInfo versionTopicInfo,
      PushJobSetting pushJobSetting,
      PushJobSchemaInfo pushJobSchemaInfo,
      String clusterName,
      String inputDirectory,
      final long inputFileDataSize) {
    List<String> propKeyValuePairs = new ArrayList<>();
    propKeyValuePairs.add("Job ID: " + this.jobId);
    propKeyValuePairs.add("Kafka URL: " + versionTopicInfo.kafkaUrl);
    propKeyValuePairs.add("Kafka Topic: " + versionTopicInfo.topic);
    propKeyValuePairs.add("Kafka topic partition count: " + versionTopicInfo.partitionCount);
    propKeyValuePairs.add("Kafka Queue Bytes: " + pushJobSetting.batchNumBytes);
    propKeyValuePairs.add("Input Directory: " + inputDirectory);
    propKeyValuePairs.add("Venice Store Name: " + pushJobSetting.storeName);
    propKeyValuePairs.add("Venice Cluster Name: " + clusterName);
    propKeyValuePairs.add("Venice URL: " + pushJobSetting.veniceControllerUrl);
    if (pushJobSchemaInfo != null) {
      propKeyValuePairs.add("File Schema: " + pushJobSchemaInfo.getFileSchemaString());
      propKeyValuePairs.add("Avro key schema: " + pushJobSchemaInfo.getKeySchemaString());
      propKeyValuePairs.add("Avro value schema: " + pushJobSchemaInfo.getValueSchemaString());
    }
    propKeyValuePairs.add(
        "Total input data file size: " + ((double) inputFileDataSize / 1024 / 1024) + " MB, estimated with a factor of "
            + INPUT_DATA_SIZE_FACTOR);
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
    killJobAndCleanup(pushJobSetting, controllerClient, kafkaTopicInfo);
    if (kafkaTopicInfo != null && StringUtils.isEmpty(kafkaTopicInfo.topic)) {
      pushJobDetails.overallStatus.add(getPushJobDetailsStatusTuple(PushJobDetailsStatus.ERROR.getValue()));
    } else {
      pushJobDetails.overallStatus.add(getPushJobDetailsStatusTuple(PushJobDetailsStatus.KILLED.getValue()));
    }
    pushJobDetails.jobDurationInMs = System.currentTimeMillis() - jobStartTimeMs;
    updatePushJobDetailsWithConfigs();
    sendPushJobDetailsToController();
  }

  private void killJobAndCleanup(
      PushJobSetting pushJobSetting,
      ControllerClient controllerClient,
      VersionTopicInfo versionTopicInfo) {
    // Attempting to kill job. There's a race condition, but meh. Better kill when you know it's running
    killJob();
    if (!pushJobSetting.isIncrementalPush && versionTopicInfo != null) {
      final int maxRetryAttempt = 10;
      int currentRetryAttempt = 0;
      while (currentRetryAttempt < maxRetryAttempt) {
        if (!StringUtils.isEmpty(versionTopicInfo.topic)) {
          break;
        }
        Utils.sleep(Duration.ofMillis(10).toMillis());
        currentRetryAttempt++;
      }
      if (StringUtils.isEmpty(versionTopicInfo.topic)) {
        LOGGER.error("Could not find a store version to delete for store: {}", pushJobSetting.storeName);
      } else {
        ControllerClient.retryableRequest(
            controllerClient,
            pushJobSetting.controllerRetries,
            c -> c.killOfflinePushJob(versionTopicInfo.topic));
        LOGGER.info("Offline push job has been killed, topic: {}", versionTopicInfo.topic);
      }
    }
    close();
  }

  private void killJob() {
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
      // Will try to kill Venice Offline Push Job no matter whether map-reduce job kill throws exception or not.
      LOGGER.info(
          "Received exception while killing map-reduce job with name {} and ID {}",
          runningJob.getJobName(),
          runningJob.getID().getId(),
          ex);
    }
  }

  /**
   * Define as "protected" function instead of "private" because it's needed in some test cases.
   */
  protected static Path getLatestPathOfInputDirectory(String inputDirectory, FileSystem fs) throws IOException {
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

  private String discoverCluster(String storeName) {
    LOGGER.info("Discover cluster for store: {}", storeName);
    ControllerResponse clusterDiscoveryResponse = ControllerClient.retryableRequest(
        clusterDiscoveryControllerClient == null ? controllerClient : clusterDiscoveryControllerClient,
        pushJobSetting.controllerRetries,
        c -> c.discoverCluster(storeName));
    if (clusterDiscoveryResponse.isError()) {
      throw new VeniceException("Failed to discover cluster, error :" + clusterDiscoveryResponse.getError());
    } else {
      return clusterDiscoveryResponse.getCluster();
    }
  }

  public String getKafkaTopic() {
    return kafkaTopicInfo.topic;
  }

  public String getKafkaUrl() {
    return kafkaTopicInfo.kafkaUrl;
  }

  public String getInputDirectory() {
    return inputDirectory;
  }

  public long getInputFileDataSize() {
    return inputFileDataSize;
  }

  public Optional<String> getIncrementalPushVersion() {
    return pushJobSetting.incrementalPushVersion;
  }

  public PushJobSchemaInfo getVeniceSchemaInfo() {
    return pushJobSchemaInfo;
  }

  public String getTopicToMonitor() {
    if (kafkaTopicInfo == null || pushJobSetting == null) {
      throw new VeniceException("The push job is not initialized yet");
    }
    return getTopicToMonitor(this.kafkaTopicInfo, this.pushJobSetting);
  }

  private String getTopicToMonitor(VersionTopicInfo versionTopicInfo, PushJobSetting jobSetting) {
    return Version.isRealTimeTopic(versionTopicInfo.topic)
        ? Version.composeKafkaTopic(jobSetting.storeName, versionTopicInfo.version)
        : versionTopicInfo.topic;
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

  /**
   * ignore hdfs files with prefix "_" and "."
   */
  public static final PathFilter PATH_FILTER = p -> !p.getName().startsWith("_") && !p.getName().startsWith(".");

  @Override
  public void close() {
    closeClients();
  }

  private void closeClients() {
    closeVeniceWriter();
    Utils.closeQuietlyWithErrorLogged(controllerClient);
    Utils.closeQuietlyWithErrorLogged(systemKMEStoreControllerClient);
    Utils.closeQuietlyWithErrorLogged(clusterDiscoveryControllerClient);
    Utils.closeQuietlyWithErrorLogged(livenessHeartbeatStoreControllerClient);
  }

  private static class ErrorMessage {
    private final String errorMessage;

    ErrorMessage(String errorMessage) {
      this.errorMessage = errorMessage;
    }

    String getErrorMessage() {
      return errorMessage;
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

  public static void runPushJob(String jobId, Properties props) {
    try (VenicePushJob job = new VenicePushJob(jobId, props)) {
      job.run();
    }
  }
}
