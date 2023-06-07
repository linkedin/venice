package com.linkedin.venice.hadoop;

import static com.linkedin.venice.CommonConfigKeys.SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.ConfigKeys.AMPLIFICATION_FACTOR;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_CONFIG_PREFIX;
import static com.linkedin.venice.ConfigKeys.KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.KAFKA_PRODUCER_REQUEST_TIMEOUT_MS;
import static com.linkedin.venice.ConfigKeys.KAFKA_PRODUCER_RETRIES_CONFIG;
import static com.linkedin.venice.ConfigKeys.PARTITIONER_CLASS;
import static com.linkedin.venice.ConfigKeys.VENICE_PARTITIONERS;
import static com.linkedin.venice.VeniceConstants.DEFAULT_SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.status.BatchJobHeartbeatConfigs.HEARTBEAT_ENABLED_CONFIG;
import static com.linkedin.venice.utils.ByteUtils.generateHumanReadableByteCountString;
import static com.linkedin.venice.utils.Utils.getUniqueString;
import static org.apache.hadoop.mapreduce.MRJobConfig.MAPREDUCE_JOB_CLASSLOADER;
import static org.apache.hadoop.mapreduce.MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY;
import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

import com.github.luben.zstd.Zstd;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerClientFactory;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ControllerClientFactory;
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
import com.linkedin.venice.hadoop.input.kafka.KafkaInputDictTrainer;
import com.linkedin.venice.hadoop.input.kafka.KafkaInputFormat;
import com.linkedin.venice.hadoop.input.kafka.KafkaInputFormatCombiner;
import com.linkedin.venice.hadoop.input.kafka.KafkaInputKeyComparator;
import com.linkedin.venice.hadoop.input.kafka.KafkaInputMRPartitioner;
import com.linkedin.venice.hadoop.input.kafka.KafkaInputRecordReader;
import com.linkedin.venice.hadoop.input.kafka.KafkaInputValueGroupingComparator;
import com.linkedin.venice.hadoop.input.kafka.VeniceKafkaInputMapper;
import com.linkedin.venice.hadoop.input.kafka.VeniceKafkaInputReducer;
import com.linkedin.venice.hadoop.input.kafka.ttl.TTLResolutionPolicy;
import com.linkedin.venice.hadoop.output.avro.ValidateSchemaAndBuildDictMapperOutput;
import com.linkedin.venice.hadoop.schema.HDFSRmdSchemaSource;
import com.linkedin.venice.hadoop.ssl.TempFileSSLConfigurator;
import com.linkedin.venice.hadoop.utils.HadoopUtils;
import com.linkedin.venice.hadoop.utils.VPJSSLUtils;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreInfo;
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
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.kafka.clients.CommonClientConfigs;
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
  public static final String DEFAULT_KEY_FIELD_PROP = "key";
  public static final String DEFAULT_VALUE_FIELD_PROP = "value";
  public static final boolean DEFAULT_SSL_ENABLED = false;
  public static final String SCHEMA_STRING_PROP = "schema";
  public static final String KAFKA_SOURCE_KEY_SCHEMA_STRING_PROP = "kafka.source.key.schema";
  public static final String EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED = "extended.schema.validity.check.enabled";
  public static final boolean DEFAULT_EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED = true;

  // Vson input configs
  // Vson files store key/value schema on file header. key / value fields are optional
  // and should be specified only when key / value schema is the partial of the files.
  public static final String FILE_KEY_SCHEMA = "key.schema";
  public static final String FILE_VALUE_SCHEMA = "value.schema";
  public static final String INCREMENTAL_PUSH = "incremental.push";

  // veniceReducer will not fail fast and override the previous key if this is true and duplicate keys incur.
  public static final String ALLOW_DUPLICATE_KEY = "allow.duplicate.key";
  public static final String POLL_STATUS_RETRY_ATTEMPTS = "poll.status.retry.attempts";
  public static final String CONTROLLER_REQUEST_RETRY_ATTEMPTS = "controller.request.retry.attempts";
  public static final String POLL_JOB_STATUS_INTERVAL_MS = "poll.job.status.interval.ms";
  public static final String JOB_STATUS_IN_UNKNOWN_STATE_TIMEOUT_MS = "job.status.in.unknown.state.timeout.ms";
  public static final String SEND_CONTROL_MESSAGES_DIRECTLY = "send.control.messages.directly";
  public static final String SOURCE_ETL = "source.etl";
  public static final String ETL_VALUE_SCHEMA_TRANSFORMATION = "etl.value.schema.transformation";

  /**
   *  Config to enable/disable the feature to collect extra metrics wrt compression.
   *  Enabling this collects metrics for all compression strategies regardless of
   *  the configured compression strategy. This means: zstd dictionary will be
   *  created even if {@link CompressionStrategy#ZSTD_WITH_DICT} is not the configured
   *  store compression strategy (refer {@link #shouldBuildZstdCompressionDictionary})
   *  <br><br>
   *
   *  This config also gets evaluated in {@link #evaluateCompressionMetricCollectionEnabled}
   *  <br><br>
   *
   *  Enabling this feature force enables {@link #USE_MAPPER_TO_BUILD_DICTIONARY}.
   */
  public static final String COMPRESSION_METRIC_COLLECTION_ENABLED = "compression.metric.collection.enabled";
  public static final boolean DEFAULT_COMPRESSION_METRIC_COLLECTION_ENABLED = false;

  /**
   * Config to enable/disable using mapper to do the below which are currently done in VPJ driver <br>
   * 1. validate schema, <br>
   * 2. collect the input data size <br>
   * 3. build dictionary (if needed: refer {@link #shouldBuildZstdCompressionDictionary}) <br><br>
   *
   * This new mapper was added because the sample collection for Zstd dictionary is currently
   * in-memory and to help play around with the sample size and also to support future enhancements
   * if needed. <br><br>
   *
   * Currently, this will be force enabled if {@link #COMPRESSION_METRIC_COLLECTION_ENABLED} is
   * enabled and the plan is to make this enabled by default and clean up remaining code once
   * this becomes stable to make the flow similar for all cases. But needs to be discussed further
   * on "using a mapper when not really needed (if no dictionary needed or the sample size is small)"
   * vs "having 2 different flows to manage/test".
   */
  public static final String USE_MAPPER_TO_BUILD_DICTIONARY = "use.mapper.to.build.dictionary";
  public static final boolean DEFAULT_USE_MAPPER_TO_BUILD_DICTIONARY = false;

  /**
   * Configs to pass to {@link AbstractVeniceMapper} based on the input configs and Dictionary
   * training status
   */
  public static final String ZSTD_DICTIONARY_CREATION_REQUIRED = "zstd.dictionary.creation.required";
  public static final String ZSTD_DICTIONARY_CREATION_SUCCESS = "zstd.dictionary.creation.success";

  /**
   * Location and key to store the output of {@link ValidateSchemaAndBuildDictMapper} and retrieve it back
   * when USE_MAPPER_TO_BUILD_DICTIONARY is enabled
   */
  // used to send the dir details from VPJ driver to mapper
  public static final String VALIDATE_SCHEMA_AND_BUILD_DICT_MAPPER_OUTPUT_DIRECTORY =
      "validate.schema.and.build.dict.mapper.output.directory";

  // used to get parent directory input from Users
  public static final String MAPPER_OUTPUT_DIRECTORY = "mapper.output.directory";

  // static names used to construct the directory and file name
  protected static final String VALIDATE_SCHEMA_AND_BUILD_DICTIONARY_MAPPER_OUTPUT_PARENT_DIR_DEFAULT =
      "/tmp/veniceMapperOutput";
  private static final String VALIDATE_SCHEMA_AND_BUILD_DICTIONARY_MAPPER_OUTPUT_FILE_PREFIX = "mapper-output-";
  private static final String VALIDATE_SCHEMA_AND_BUILD_DICTIONARY_MAPPER_OUTPUT_FILE_EXTENSION = ".avro";

  // keys inside the avro file
  public static final String KEY_ZSTD_COMPRESSION_DICTIONARY = "zstdDictionary";
  public static final String KEY_INPUT_FILE_DATA_SIZE = "inputFileDataSize";

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
  // Whether to build a new dict in the repushed version or not while the original version has already enabled dict
  // compression.
  public static final String KAFKA_INPUT_COMPRESSION_BUILD_NEW_DICT_ENABLED =
      "kafka.input.compression.build.new.dict.enabled";

  public static final String KAFKA_INPUT_SOURCE_TOPIC_CHUNKING_ENABLED = "kafka.input.source.topic.chunking.enabled";
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
   * A time stamp specified to rewind to before replaying data. This config is ignored if rewind.time.in.seconds.override
   * is provided. This config at time of push will be leveraged to fill in the rewind.time.in.seconds.override by taking
   * System.currentTime - rewind.epoch.time.in.seconds.override and storing the result in rewind.time.in.seconds.override.
   * With this in mind, a push policy of REWIND_FROM_SOP should be used in order to get a behavior that makes sense to a user.
   * A timestamp that is in the future is not valid and will result in an exception.
   */
  public static final String REWIND_EPOCH_TIME_IN_SECONDS_OVERRIDE = "rewind.epoch.time.in.seconds.override";

  /**
   * This config is a boolean which suppresses submitting the end of push message after data has been sent and does
   * not poll for the status of the job to complete. Using this flag means that a user must manually mark the job success
   * or failed.
   */
  public static final String SUPPRESS_END_OF_PUSH_MESSAGE = "suppress.end.of.push.message";

  /**
   * This config is a boolean which waits for an external signal to trigger version swap after buffer replay is complete.
   */
  public static final String DEFER_VERSION_SWAP = "defer.version.swap";

  /**
   * This config specifies the prefix for d2 zk hosts config. Configs of type {@literal <prefix>.<regionName>} are
   * expected to be defined.
   */
  public static final String D2_ZK_HOSTS_PREFIX = "d2.zk.hosts.";

  /**
   * This config specifies the region identifier where parent controller is running
   */
  public static final String PARENT_CONTROLLER_REGION_NAME = "parent.controller.region.name";

  /**
   * Relates to the above argument. An overridable amount of buffer to be applied to the epoch (as the rewind isn't
   * perfectly instantaneous). Defaults to 1 minute.
   */
  public static final String REWIND_EPOCH_TIME_BUFFER_IN_SECONDS_OVERRIDE =
      "rewind.epoch.time.buffer.in.seconds.override";

  /**
   * This config specifies if Venice is deployed in a multi-region mode
   */
  public static final String MULTI_REGION = "multi.region";

  /**
   * In single-region mode, this must be a comma-separated list of child controller URLs or {@literal d2://<d2ServiceNameForChildController>}
   * In multi-region mode, it must be a comma-separated list of parent controller URLs or {@literal d2://<d2ServiceNameForParentController>}
   */
  public static final String VENICE_DISCOVER_URL_PROP = "venice.discover.urls";

  /**
   * An identifier of the data center which is used to determine the Kafka URL and child controllers that push jobs communicate with
   */
  public static final String SOURCE_GRID_FABRIC = "source.grid.fabric";

  public static final String ENABLE_WRITE_COMPUTE = "venice.write.compute.enable";
  public static final String ENABLE_PUSH = "venice.push.enable";
  public static final String ENABLE_SSL = "venice.ssl.enable";
  public static final String VENICE_STORE_NAME_PROP = "venice.store.name";
  public static final String INPUT_PATH_PROP = "input.path";
  public static final String INPUT_PATH_LAST_MODIFIED_TIME = "input.path.last.modified.time";
  public static final String BATCH_NUM_BYTES_PROP = "batch.num.bytes";

  public static final String VALUE_SCHEMA_ID_PROP = "value.schema.id";
  public static final String DERIVED_SCHEMA_ID_PROP = "derived.schema.id";
  public static final String TOPIC_PROP = "venice.kafka.topic";
  protected static final String HADOOP_PREFIX = "hadoop-conf.";
  protected static final String HADOOP_VALIDATE_SCHEMA_AND_BUILD_DICT_PREFIX = "hadoop-dict-build-conf.";
  public static final String SSL_PREFIX = "ssl";

  public static final String STORAGE_QUOTA_PROP = "storage.quota";
  public static final String STORAGE_ENGINE_OVERHEAD_RATIO = "storage_engine_overhead_ratio";
  @Deprecated
  public static final String VSON_PUSH = "vson.push";
  public static final String KAFKA_SECURITY_PROTOCOL = "SSL";
  public static final String COMPRESSION_STRATEGY = "compression.strategy";
  public static final String KAFKA_INPUT_SOURCE_COMPRESSION_STRATEGY = "kafka.input.source.compression.strategy";
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
  public static final String REPUSH_TTL_ENABLE = "repush.ttl.enable";
  public static final String REPUSH_TTL_IN_SECONDS = "repush.ttl.seconds";
  public static final String REPUSH_TTL_POLICY = "repush.ttl.policy";
  public static final String RMD_SCHEMA_DIR = "rmd.schema.dir";
  private static final String TEMP_DIR_PREFIX = "/tmp/veniceRmdSchemas/";
  public static final int NOT_SET = -1;
  private static final Logger LOGGER = LogManager.getLogger(VenicePushJob.class);

  /**
   * Config to enable single targeted region push mode in VPJ.
   * In this mode, the VPJ will only push data to a single region.
   * The single region is decided by the store config in {@link StoreInfo#getNativeReplicationSourceFabric()}}.
   * For multiple targeted regions push, may use the advanced mode. See {@link #TARGETED_REGION_PUSH_LIST}.
   */
  public static final String TARGETED_REGION_PUSH_ENABLED = "targeted.region.push.enabled";

  /**
   * This is experimental config to specify a list of regions used for targeted region push in VPJ.
   * {@link #TARGETED_REGION_PUSH_ENABLED} has to be enabled to use this config.
   * In this mode, the VPJ will only push data to the provided regions.
   * The input is comma separated list of region names, e.g. "dc-0,dc-1,dc-2".
   * For single targeted region push, see {@link #TARGETED_REGION_PUSH_ENABLED}.
   */
  public static final String TARGETED_REGION_PUSH_LIST = "targeted.region.push.list";

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

  // Lazy state
  private final Lazy<Properties> sslProperties;
  private VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter;
  /** TODO: refactor to use {@link Lazy} */

  // Mutable state
  private ControllerClient controllerClient;
  private ControllerClient kmeSchemaSystemStoreControllerClient;
  private ControllerClient livenessHeartbeatStoreControllerClient;
  private RunningJob runningJob;
  // Job config for schema validation and Compression dictionary creation (if needed)
  protected JobConf validateSchemaAndBuildDictJobConf = new JobConf();
  // Job config for regular push job
  protected JobConf jobConf = new JobConf();
  protected InputDataInfoProvider inputDataInfoProvider;
  private ValidateSchemaAndBuildDictMapperOutputReader validateSchemaAndBuildDictMapperOutputReader;
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
  private ValidateSchemaAndBuildDictMapperOutput validateSchemaAndBuildDictMapperOutput;
  private String validateSchemaAndBuildDictMapperOutputDirectory;
  private boolean isZstdDictCreationRequired = false;
  private boolean isZstdDictCreationSuccess = false;

  protected static class PushJobSetting {
    boolean enablePush;
    String veniceControllerUrl;
    String storeName;
    String clusterName;
    String sourceGridFabric;
    int batchNumBytes;
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
    boolean kafkaInputBuildNewDictEnabled;
    BufferReplayPolicy validateRemoteReplayPolicy;
    boolean suppressEndOfPushMessage;
    boolean deferVersionSwap;
    boolean extendedSchemaValidityCheckEnabled;
    /** Refer {@link #COMPRESSION_METRIC_COLLECTION_ENABLED} **/
    boolean compressionMetricCollectionEnabled;
    /** Refer {@link #USE_MAPPER_TO_BUILD_DICTIONARY} **/
    boolean useMapperToBuildDict;
    String useMapperToBuildDictOutputPath;
    boolean repushTTLEnabled;
    // specify ttl time to drop stale records.
    long repushTTLInSeconds;
    // HDFS directory to cache RMD schemas
    String rmdSchemaDir;
    String controllerD2ServiceName;
    String parentControllerRegionD2ZkHosts;
    String childControllerRegionD2ZkHosts;
    boolean livenessHeartbeatEnabled;
    String livenessHeartbeatStoreName;
    boolean multiRegion;
    boolean d2Routing;
    String targetedRegions;
    boolean isTargetedRegionPushEnabled;
  }

  protected PushJobSetting pushJobSetting;

  protected static class TopicInfo {
    // Kafka topic for new data push
    String topic;
    /** Version part of the store-version / topic name */
    int version;
    // Kafka topic partition count
    int partitionCount;
    // Kafka url will get from Venice backend for store push
    String kafkaUrl;
    boolean sslToKafka;
    CompressionStrategy compressionStrategy;
    String partitionerClass;
    Map<String, String> partitionerParams;
    int amplificationFactor;
    boolean chunkingEnabled;
    boolean rmdChunkingEnabled;
  }

  private TopicInfo kafkaTopicInfo;

  private final PushJobDetails pushJobDetails;
  private final InternalAvroSpecificSerializer<PushJobDetails> pushJobDetailsSerializer =
      AvroProtocolDefinition.PUSH_JOB_DETAILS.getSerializer();

  protected static class StoreSetting {
    boolean isChunkingEnabled;
    boolean isRmdChunkingEnabled;
    long storeStorageQuota;
    boolean isSchemaAutoRegisterFromPushJobEnabled;
    CompressionStrategy compressionStrategy;
    boolean isWriteComputeEnabled;
    boolean isIncrementalPushEnabled;
    Version sourceKafkaInputVersionInfo;
    long storeRewindTimeInSeconds;
    Schema keySchema;
    HybridStoreConfig hybridStoreConfig;
    StoreResponse storeResponse;
  }

  protected StoreSetting storeSetting;
  private InputStorageQuotaTracker inputStorageQuotaTracker;
  private PushJobHeartbeatSenderFactory pushJobHeartbeatSenderFactory;
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

  /**
   * @param jobId  id of the job
   * @param vanillaProps  Property bag for the job
   */
  public VenicePushJob(String jobId, Properties vanillaProps) {
    this.jobId = jobId;
    this.props = getVenicePropsFromVanillaProps(Objects.requireNonNull(vanillaProps, "VPJ props cannot be null"));
    if (isSslEnabled()) {
      VPJSSLUtils.validateSslProperties(vanillaProps);
    }
    this.sslProperties = Lazy.of(() -> {
      try {
        return VPJSSLUtils.getSslProperties(this.props);
      } catch (IOException e) {
        throw new VeniceException("Could not get user credential");
      }
    });
    LOGGER.info("Constructing {}: {}", VenicePushJob.class.getSimpleName(), props.toString(true));
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
  }

  // Visible for testing
  PushJobSetting getPushJobSetting() {
    return this.pushJobSetting;
  }

  // Visible for testing
  VeniceProperties getVeniceProperties() {
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

  private boolean isSslEnabled() {
    return props.getBoolean(ENABLE_SSL, DEFAULT_SSL_ENABLED);
  }

  private PushJobSetting getPushJobSetting(VeniceProperties props) {
    PushJobSetting pushJobSettingToReturn = new PushJobSetting();
    pushJobSettingToReturn.veniceControllerUrl = props.getString(VENICE_DISCOVER_URL_PROP);
    pushJobSettingToReturn.enablePush = props.getBoolean(ENABLE_PUSH, true);
    if (props.containsKey(SOURCE_GRID_FABRIC)) {
      pushJobSettingToReturn.sourceGridFabric = props.getString(SOURCE_GRID_FABRIC);
    }
    pushJobSettingToReturn.batchNumBytes = props.getInt(BATCH_NUM_BYTES_PROP, DEFAULT_BATCH_BYTES_SIZE);
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
    pushJobSettingToReturn.kafkaInputBuildNewDictEnabled =
        props.getBoolean(KAFKA_INPUT_COMPRESSION_BUILD_NEW_DICT_ENABLED, true);
    pushJobSettingToReturn.suppressEndOfPushMessage = props.getBoolean(SUPPRESS_END_OF_PUSH_MESSAGE, false);
    pushJobSettingToReturn.deferVersionSwap = props.getBoolean(DEFER_VERSION_SWAP, false);
    pushJobSettingToReturn.repushTTLEnabled = props.getBoolean(REPUSH_TTL_ENABLE, false);
    pushJobSettingToReturn.repushTTLInSeconds = NOT_SET;
    pushJobSettingToReturn.isTargetedRegionPushEnabled = props.getBoolean(TARGETED_REGION_PUSH_ENABLED, false);
    if (props.containsKey(TARGETED_REGION_PUSH_LIST)) {
      if (pushJobSettingToReturn.isTargetedRegionPushEnabled) {
        pushJobSettingToReturn.targetedRegions = props.getString(TARGETED_REGION_PUSH_LIST);
      } else {
        throw new VeniceException("Targeted region push list is only supported when targeted region push is enabled");
      }
    }

    if (pushJobSettingToReturn.repushTTLEnabled && !pushJobSettingToReturn.isSourceKafka) {
      throw new VeniceException("Repush with TTL is only supported while using Kafka Input Format");
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

    pushJobSettingToReturn.extendedSchemaValidityCheckEnabled =
        props.getBoolean(EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED, DEFAULT_EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED);
    pushJobSettingToReturn.compressionMetricCollectionEnabled =
        props.getBoolean(COMPRESSION_METRIC_COLLECTION_ENABLED, DEFAULT_COMPRESSION_METRIC_COLLECTION_ENABLED);
    pushJobSettingToReturn.useMapperToBuildDict =
        props.getBoolean(USE_MAPPER_TO_BUILD_DICTIONARY, DEFAULT_USE_MAPPER_TO_BUILD_DICTIONARY);
    if (pushJobSettingToReturn.compressionMetricCollectionEnabled && !pushJobSettingToReturn.useMapperToBuildDict) {
      // TODO the idea is to only have compressionMetricCollectionEnabled as a config and remove useMapperToBuildDict
      // feature flag after its stable.
      LOGGER.warn(
          "Force enabling \"{}\" to support \"{}\"",
          USE_MAPPER_TO_BUILD_DICTIONARY,
          COMPRESSION_METRIC_COLLECTION_ENABLED);
      pushJobSettingToReturn.useMapperToBuildDict = true;
    }
    if (pushJobSettingToReturn.useMapperToBuildDict) {
      pushJobSettingToReturn.useMapperToBuildDictOutputPath = props
          .getString(MAPPER_OUTPUT_DIRECTORY, VALIDATE_SCHEMA_AND_BUILD_DICTIONARY_MAPPER_OUTPUT_PARENT_DIR_DEFAULT);
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
  private String getSourceTopicNameForKafkaInput(
      final String userProvidedStoreName,
      final VeniceProperties properties) {
    final Optional<String> userProvidedTopicNameOptional =
        Optional.ofNullable(properties.getString(KAFKA_INPUT_TOPIC, () -> null));

    // This mode of passing the topic name to VPJ is going to be deprecated.
    if (userProvidedTopicNameOptional.isPresent()) {
      return getUserProvidedTopicName(
          userProvidedStoreName,
          userProvidedTopicNameOptional.get(),
          pushJobSetting.controllerRetries);
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

  // Visible for testing
  protected void setValidateSchemaAndBuildDictMapperOutputReader(
      ValidateSchemaAndBuildDictMapperOutputReader validateSchemaAndBuildDictMapperOutputReader) throws Exception {
    this.validateSchemaAndBuildDictMapperOutputReader = validateSchemaAndBuildDictMapperOutputReader;
  }

  /**
   * @throws VeniceException
   */
  public void run() {
    PushJobHeartbeatSender pushJobHeartbeatSender = null;
    try {
      Optional<SSLFactory> sslFactory = VPJSSLUtils.createSSLFactory(
          isSslEnabled(),
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
      jobStartTimeMs = System.currentTimeMillis();
      logGreeting();
      sendPushJobDetailsToController();
      validateKafkaMessageEnvelopeSchema(pushJobSetting);
      validateRemoteHybridSettings(pushJobSetting);
      inputDirectory = getInputURI(props);
      storeSetting = getSettingsFromController(controllerClient, pushJobSetting);
      inputStorageQuotaTracker = new InputStorageQuotaTracker(storeSetting.storeStorageQuota);

      if (pushJobSetting.repushTTLEnabled && storeSetting.isWriteComputeEnabled) {
        throw new VeniceException("Repush TTL is not supported when the store has write compute enabled.");
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

        validateKeySchema(controllerClient, pushJobSetting, pushJobSchemaInfo, storeSetting);
        validateValueSchema(
            controllerClient,
            pushJobSetting,
            pushJobSchemaInfo,
            storeSetting.isSchemaAutoRegisterFromPushJobEnabled);

        pushJobSetting.compressionMetricCollectionEnabled =
            evaluateCompressionMetricCollectionEnabled(pushJobSetting, inputFileHasRecords);
        isZstdDictCreationRequired =
            shouldBuildZstdCompressionDictionary(pushJobSetting, storeSetting, inputFileHasRecords);
        if (pushJobSetting.useMapperToBuildDict) {
          /**
           * 1. validate whether the remaining file's schema are consistent with the first file
           * 2. calculate {@link inputFileDataSize} during step 1
           * 3. Build dictionary (if dictionary compression is enabled for this store version or compressionMetricCollectionEnabled)
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
            LOGGER.info("Overriding re-push rewind time in seconds to: {}", pushJobSetting.rewindTimeInSecondsOverride);
          }
          if (pushJobSetting.repushTTLEnabled) {
            pushJobSetting.repushTTLInSeconds = storeSetting.storeRewindTimeInSeconds;
            // make the base directory TEMP_DIR_PREFIX with 777 permissions
            Path baseSchemaDir = new Path(TEMP_DIR_PREFIX);
            FileSystem fs = FileSystem.get(new Configuration());
            if (!fs.exists(baseSchemaDir)) {
              fs.mkdirs(baseSchemaDir);
              fs.setPermission(baseSchemaDir, new FsPermission("777"));
            }

            // build the full path for HDFSRmdSchemaSource: the schema path will be suffixed
            // by the store name and time like: <TEMP_DIR_PREFIX>/<store_name>/<timestamp>
            StringBuilder schemaDirBuilder = new StringBuilder();
            schemaDirBuilder.append(TEMP_DIR_PREFIX)
                .append(pushJobSetting.storeName)
                .append("/")
                .append(System.currentTimeMillis());
            try (HDFSRmdSchemaSource rmdSchemaSource =
                new HDFSRmdSchemaSource(schemaDirBuilder.toString(), pushJobSetting.storeName)) {
              rmdSchemaSource.loadRmdSchemasOnDisk(controllerClient);
              pushJobSetting.rmdSchemaDir = rmdSchemaSource.getPath();
            }
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
        pushJobDetails.valueCompressionStrategy = kafkaTopicInfo.compressionStrategy != null
            ? kafkaTopicInfo.compressionStrategy.getValue()
            : CompressionStrategy.NO_OP.getValue();
        pushJobDetails.chunkingEnabled = kafkaTopicInfo.chunkingEnabled;
        pushJobDetails.overallStatus.add(getPushJobDetailsStatusTuple(PushJobDetailsStatus.TOPIC_CREATED.getValue()));
        pushJobHeartbeatSender = createPushJobHeartbeatSender(isSslEnabled());
        pushJobHeartbeatSender.start(pushJobSetting.storeName, kafkaTopicInfo.version);
        sendPushJobDetailsToController();
        // Log Venice data push job related info
        logPushJobProperties(kafkaTopicInfo, pushJobSetting, pushJobSchemaInfo, inputDirectory, inputFileDataSize);

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
      if (pushJobSetting.rmdSchemaDir != null) {
        HadoopUtils.cleanUpHDFSPath(pushJobSetting.rmdSchemaDir, true);
      }
    }
  }

  private PushJobHeartbeatSender createPushJobHeartbeatSender(final boolean sslEnabled) {
    try {
      return pushJobHeartbeatSenderFactory.createHeartbeatSender(
          kafkaTopicInfo.kafkaUrl,
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

  void runJobAndUpdateStatus() throws IOException {
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
        if (pushJobSetting.repushTTLEnabled) {
          LOGGER.info(
              "Repush with ttl filtered out {} records",
              MRJobCounterHelper.getRepushTtlFilterCount(runningJob.getCounters()));
        }
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

  private void runValidateSchemaAndBuildDictJobAndUpdateStatus(JobConf conf) throws Exception {
    updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.START_VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB);
    runningJob = runJobWithConfig(conf);
    validateCountersAfterValidateSchemaAndBuildDict();
    getValidateSchemaAndBuildDictMapperOutput(runningJob.getID().toString());
    updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.VALIDATE_SCHEMA_AND_BUILD_DICT_MAP_JOB_COMPLETED);
  }

  /**
   * Creating the output file for {@link ValidateSchemaAndBuildDictMapper} to persist data
   * to be read from VPJ driver.
   *
   * Output Directory: {$hadoopTmpDir}/{$storeName}-{$JOB_EXEC_ID}-{$randomUniqueString}
   * File name: mapper-output-{$MRJobID}.avro
   *
   * Why JOB_EXEC_ID and randomUniqueString: This gives uniqueness to the name of the directory whose
   * permission will be restricted to the current user who started the VPJ only. This helps with 2 issues.
   * 1. There could be instances where multiple headless accounts are writing to a single Venice store.
   *    It shouldn't happen in regular cases - but is very likely in case of migrations (technical or organizational)
   *    => unless we have unique directory for each job, the multiple accounts will have access issues of the directory.
   *
   * 2. Multiple push jobs can be started in parallel, but only 1 will continue beyond
   *    {@link ControllerClient#requestTopicForWrites} as this method will throw CONCURRENT_BATCH_PUSH error
   *    if there is another push job in progress. As {@link ValidateSchemaAndBuildDictMapper} runs before this method,
   *    it is prone to concurrent push jobs and thus race conditions. Having unique directories per execution will help here.
   *
   * Why can't use MRJobID to achieve randomness: MR's jobID gets populated only after {@link FileOutputFormat#checkOutputSpecs},
   * which needs {@link FileOutputFormat#setOutputPath} to be set already. so currently unable to use the ID.
   *
   * TODO: should try exploring using conf.get("hadoop.tmp.dir") or similar configs to get default
   *     tmp directory in different HDFS environments rather than hardcoding it to
   *     VALIDATE_SCHEMA_AND_BUILD_DICTIONARY_MAPPER_OUTPUT_PARENT_DIR_DEFAULT.
   */
  protected static String getValidateSchemaAndBuildDictionaryOutputDir(
      String parentOutputDir,
      String storeName,
      String jobExecId) {
    return parentOutputDir + "/" + storeName + "-" + jobExecId + "-" + getUniqueString();
  }

  protected static String getValidateSchemaAndBuildDictionaryOutputFileNameNoExtension(String mrJobId) {
    return VALIDATE_SCHEMA_AND_BUILD_DICTIONARY_MAPPER_OUTPUT_FILE_PREFIX + mrJobId;
  }

  protected static String getValidateSchemaAndBuildDictionaryOutputFileName(String mrJobId) {
    return getValidateSchemaAndBuildDictionaryOutputFileNameNoExtension(mrJobId)
        + VALIDATE_SCHEMA_AND_BUILD_DICTIONARY_MAPPER_OUTPUT_FILE_EXTENSION;
  }

  private void getValidateSchemaAndBuildDictMapperOutput(String mrJobId) throws Exception {
    String outputDir = validateSchemaAndBuildDictMapperOutputDirectory;
    String outputAvroFile = getValidateSchemaAndBuildDictionaryOutputFileName(mrJobId);
    try (ValidateSchemaAndBuildDictMapperOutputReader outputReader =
        getValidateSchemaAndBuildDictMapperOutputReader(outputDir, outputAvroFile)) {
      validateSchemaAndBuildDictMapperOutput = outputReader.getOutput();
    }
    inputFileDataSize = validateSchemaAndBuildDictMapperOutput.getInputFileDataSize() * INPUT_DATA_SIZE_FACTOR;
  }

  private void checkLastModificationTimeAndLog() throws IOException {
    checkLastModificationTimeAndLog(false);
  }

  private void checkLastModificationTimeAndLog(boolean throwExceptionOnDataSetChange) throws IOException {
    long lastModificationTime = getInputDataInfoProvider().getInputLastModificationTime(inputDirectory);
    if (lastModificationTime > inputModificationTime) {
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
      StoreSetting storeSetting,
      boolean inputFileHasRecords) {
    if (pushJobSetting.isSourceKafka) {
      /**
       * Currently, KIF repush will use a different code path for dict buid.
       * If later, we add the support to build the dict in a MR job, we need to revist this logic.
       */
      return false;
    }
    if (!inputFileHasRecords) {
      LOGGER.info("No compression dictionary will be generated as there are no records");
      return false;
    }

    if (pushJobSetting.compressionMetricCollectionEnabled
        || storeSetting.compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
      if (pushJobSetting.isIncrementalPush) {
        LOGGER.info("No compression dictionary will be generated as the push type is incremental push");
        return false;
      }
      LOGGER.info(
          "Compression dictionary will be generated with the compression strategy {} "
              + "and compressionMetricCollectionEnabled is {}",
          storeSetting.compressionStrategy,
          (pushJobSetting.compressionMetricCollectionEnabled ? "Enabled" : "Disabled"));
      return true;
    }

    LOGGER.info(
        "No Compression dictionary will be generated with the compression strategy"
            + " {} and compressionMetricCollectionEnabled is disabled",
        storeSetting.compressionStrategy);
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

    if (pushJobSetting.isSourceKafka) {
      // repush from kafka: This is already checked before calling this function.
      // This is a defensive check.
      LOGGER.info("No compression related metrics will be generated as the push type is repush");
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
    if (inputFileHasRecords) {
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
        updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.FILE_SCHEMA_VALIDATION_FAILED);
        String err = "Error while validating schema: Inconsistent file schema found";
        LOGGER.error(err);
        throw new VeniceException(err);
      }

      final long zstdDictCreationFailureCount = MRJobCounterHelper.getMapperZstdDictTrainFailureCount(counters);
      final long zstdDictCreationSuccessCount = MRJobCounterHelper.getMapperZstdDictTrainSuccessCount(counters);
      final long zstdDictCreationSkippedCount = MRJobCounterHelper.getMapperZstdDictTrainSkippedCount(counters);
      isZstdDictCreationSuccess = (zstdDictCreationSuccessCount == 1);
      boolean isZstdDictCreationFailure = (zstdDictCreationFailureCount == 1);
      boolean isZstdDictCreationSkipped = (zstdDictCreationSkippedCount == 1);

      final long recordsSuccessfullyProcessedCount =
          MRJobCounterHelper.getMapperNumRecordsSuccessfullyProcessedCount(counters);
      if (recordsSuccessfullyProcessedCount == inputNumFiles + 1) {
        if (isZstdDictCreationRequired) {
          if (!isZstdDictCreationSuccess) {
            checkLastModificationTimeAndLog(true);
            updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.INVALID_INPUT_FILE);
            String err = "Error while validating schema: MR job counter is not reliable to point out the exact reason";
            LOGGER.error(err);
            throw new VeniceException(err);
          }
        }
      } else if (recordsSuccessfullyProcessedCount == inputNumFiles) {
        if (isZstdDictCreationFailure || isZstdDictCreationSkipped) {
          String err = isZstdDictCreationFailure
              ? "Training ZSTD compression dictionary failed: The content might not be suitable for creating dictionary."
              : "Training ZSTD compression dictionary skipped: The sample size is too small.";
          if (storeSetting.compressionStrategy != CompressionStrategy.ZSTD_WITH_DICT) {
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

  protected ValidateSchemaAndBuildDictMapperOutputReader getValidateSchemaAndBuildDictMapperOutputReader(
      String outputDir,
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
    pushJobSetting.kafkaInputBrokerUrl = pushJobSetting.repushInfoResponse == null
        ? props.getString(KAFKA_INPUT_BROKER_URL)
        : pushJobSetting.repushInfoResponse.getRepushInfo().getKafkaBrokerUrl();
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
    ByteBuffer compressionDictionary = null;

    // Prepare the param builder, which can be used by different scenarios.
    KafkaInputDictTrainer.ParamBuilder paramBuilder = new KafkaInputDictTrainer.ParamBuilder()
        .setKeySchema(AvroCompatibilityHelper.toParsingForm(storeSetting.keySchema))
        .setSslProperties(isSslEnabled() ? sslProperties.get() : new Properties())
        .setCompressionDictSize(
            props.getInt(
                DefaultInputDataInfoProvider.COMPRESSION_DICTIONARY_SIZE_LIMIT,
                VeniceWriter.DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES))
        .setDictSampleSize(
            props.getInt(
                DefaultInputDataInfoProvider.COMPRESSION_DICTIONARY_SAMPLE_SIZE,
                DefaultInputDataInfoProvider.DEFAULT_COMPRESSION_DICTIONARY_SAMPLE_SIZE));
    if (pushJobSetting.isSourceKafka) {
      /**
       * Currently KIF repush will always build a dict in Azkaban Job driver if necessary.
       */
      boolean rebuildDict = pushJobSetting.kafkaInputBuildNewDictEnabled;
      paramBuilder.setSourceVersionChunkingEnabled(storeSetting.sourceKafkaInputVersionInfo.isChunkingEnabled());
      // Repush
      if (storeSetting.compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
        if (rebuildDict) {
          LOGGER.info("Rebuild a new Zstd dictionary from the input topic: {}", pushJobSetting.kafkaInputTopic);
          paramBuilder.setKafkaInputBroker(pushJobSetting.kafkaInputBrokerUrl)
              .setTopicName(pushJobSetting.kafkaInputTopic)
              .setSourceVersionCompressionStrategy(storeSetting.sourceKafkaInputVersionInfo.getCompressionStrategy());
          KafkaInputDictTrainer dictTrainer = new KafkaInputDictTrainer(paramBuilder.build());
          compressionDictionary = ByteBuffer.wrap(dictTrainer.trainDict());
        } else {
          LOGGER.info("Reading Zstd dictionary from input topic: {}", pushJobSetting.kafkaInputTopic);
          // set up ssl properties and kafka consumer properties
          Properties kafkaConsumerProperties = new Properties();
          if (isSslEnabled()) {
            kafkaConsumerProperties.putAll(this.sslProperties.get());
          }
          kafkaConsumerProperties.setProperty(KAFKA_BOOTSTRAP_SERVERS, pushJobSetting.kafkaInputBrokerUrl);
          compressionDictionary = DictionaryUtils
              .readDictionaryFromKafka(pushJobSetting.kafkaInputTopic, new VeniceProperties(kafkaConsumerProperties));
        }
      }

      return Optional.ofNullable(compressionDictionary);
    } else {
      /**
       * Special handling for an empty push to a hybrid store.
       * Push Job will try to train a dict based on the records of the current version, and it won't work
       * for the very first version, and the following versions will work.
       */
      if (storeSetting.compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT && !inputFileHasRecords
          && storeSetting.hybridStoreConfig != null) {
        String storeName = getPushJobSetting().storeName;
        try {
          // Get the latest version
          RepushInfoResponse repushInfoResponse = ControllerClient.retryableRequest(
              controllerClient,
              pushJobSetting.controllerRetries,
              c -> c.getRepushInfo(storeName, Optional.empty()));
          if (repushInfoResponse.isError()) {
            throw new VeniceException(
                "Could not get repush info for store " + storeName + " with error: " + repushInfoResponse.getError());
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
          compressionDictionary = ByteBuffer.wrap(dictTrainer.trainDict());

          return Optional.of(compressionDictionary);
        } catch (Exception e) {
          LOGGER.warn(
              "Encountered an exception when trying to build a dict from an existing version for an empty push to a hybrid store: "
                  + storeName + ", so the push job will use a default dict built in the Controller",
              e);
          return Optional.empty();
        }
      }

      if (isZstdDictCreationRequired) {
        if (!pushJobSetting.useMapperToBuildDict) {
          LOGGER.info("Training Zstd dictionary");
          compressionDictionary = ByteBuffer.wrap(getInputDataInfoProvider().getZstdDictTrainSamples());
          isZstdDictCreationSuccess = true;
        } else {
          if (isZstdDictCreationSuccess) {
            LOGGER.info(
                "Retrieving the Zstd dictionary trained by {}",
                ValidateSchemaAndBuildDictMapper.class.getSimpleName());
            compressionDictionary = validateSchemaAndBuildDictMapperOutput.getZstdDictionary();
          } else {
            if (storeSetting.compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
              // This should not happen
              String err = "Dictionary creation failed for the configured ZSTD compression type";
              LOGGER.error(err);
              throw new VeniceException(err);
            } // else case: Dictionary creation failed, but it was not needed for the push job to succeed
          }
        }
      }
      if (compressionDictionary != null) {
        LOGGER.info("Zstd dictionary size = {} bytes", compressionDictionary.limit());
      } else {
        LOGGER.info(
            "No Compression dictionary is generated with the compression strategy {} "
                + "and compressionMetricCollectionEnabled is {}",
            storeSetting.compressionStrategy,
            (pushJobSetting.compressionMetricCollectionEnabled ? "Enabled" : "Disabled"));
      }

      return Optional.ofNullable(compressionDictionary);
    }
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

  private void updatePushJobDetailsWithMRCounters() {
    if (runningJob == null) {
      LOGGER.info("No running job to update push job details with MR counters");
      return;
    }
    try {
      pushJobDetails.totalNumberOfRecords = MRJobCounterHelper.getOutputRecordsCount(runningJob.getCounters());
      pushJobDetails.totalKeyBytes = MRJobCounterHelper.getTotalKeySize(runningJob.getCounters());
      // size of uncompressed value
      pushJobDetails.totalRawValueBytes = MRJobCounterHelper.getTotalUncompressedValueSize(runningJob.getCounters());
      // size of the final stored data in SN (can be compressed using NO_OP/GZIP/ZSTD_WITH_DICT)
      pushJobDetails.totalCompressedValueBytes = MRJobCounterHelper.getTotalValueSize(runningJob.getCounters());
      // size of the Gzip compressed data
      pushJobDetails.totalGzipCompressedValueBytes =
          MRJobCounterHelper.getTotalGzipCompressedValueSize(runningJob.getCounters());
      // size of the Zstd with Dict compressed data
      pushJobDetails.totalZstdWithDictCompressedValueBytes =
          MRJobCounterHelper.getTotalZstdWithDictCompressedValueSize(runningJob.getCounters());
      LOGGER.info(
          "pushJobDetails MR Counters: " + "\n\tTotal number of records: {} " + "\n\tSize of keys: {} Bytes "
              + "\n\tsize of uncompressed value: {} Bytes " + "\n\tConfigured value Compression Strategy: {} "
              + "\n\tFinal data size stored in Venice based on this compression strategy: {} Bytes "
              + "\n\tCompression Metrics collection is: {} ",
          pushJobDetails.totalNumberOfRecords,
          pushJobDetails.totalKeyBytes,
          pushJobDetails.totalRawValueBytes,
          CompressionStrategy.valueOf(pushJobDetails.valueCompressionStrategy).name(),
          pushJobDetails.totalCompressedValueBytes,
          pushJobSetting.compressionMetricCollectionEnabled ? "Enabled" : "Disabled");
      if (pushJobSetting.compressionMetricCollectionEnabled) {
        LOGGER.info("\tData size if compressed using Gzip: {} Bytes ", pushJobDetails.totalGzipCompressedValueBytes);
        if (isZstdDictCreationSuccess) {
          LOGGER.info(
              "\tData size if compressed using Zstd with Dictionary: {} Bytes",
              pushJobDetails.totalZstdWithDictCompressedValueBytes);
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
   * This method will validate the key schema in the input file against the one registered in Venice.
   */
  void validateKeySchema(
      ControllerClient controllerClient,
      PushJobSetting setting,
      PushJobSchemaInfo pushJobSchemaInfo,
      StoreSetting storeSetting) {
    Schema serverSchema = storeSetting.keySchema;
    Schema clientSchema = AvroCompatibilityHelper.parse(pushJobSchemaInfo.getKeySchemaString());
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
      MultiSchemaResponse response = controllerClient.getAllValueSchema(setting.storeName);
      if (response.isError()) {
        LOGGER.error("Failed to fetch all value schemas, so they will not be printed.");
      } else {
        LOGGER.info("All currently registered value schemas:");
        for (MultiSchemaResponse.Schema schema: response.getSchemas()) {
          LOGGER.info("Schema {}: {}", schema.getId(), schema.getSchemaStr());
        }
      }
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
    storeSetting.storeResponse = storeResponse;
    storeSetting.storeStorageQuota = storeResponse.getStore().getStorageQuotaInByte();
    storeSetting.isSchemaAutoRegisterFromPushJobEnabled =
        storeResponse.getStore().isSchemaAutoRegisterFromPushJobEnabled();
    storeSetting.isChunkingEnabled = storeResponse.getStore().isChunkingEnabled();
    storeSetting.isRmdChunkingEnabled = storeResponse.getStore().isRmdChunkingEnabled();
    storeSetting.compressionStrategy = storeResponse.getStore().getCompressionStrategy();
    storeSetting.isWriteComputeEnabled = storeResponse.getStore().isWriteComputationEnabled();
    storeSetting.isIncrementalPushEnabled = storeResponse.getStore().isIncrementalPushEnabled();
    storeSetting.storeRewindTimeInSeconds = DEFAULT_RE_PUSH_REWIND_IN_SECONDS_OVERRIDE;
    if (setting.isTargetedRegionPushEnabled && setting.targetedRegions == null) {
      // only override the targeted regions if it is not set and it is a single region push
      setting.targetedRegions = storeResponse.getStore().getNativeReplicationSourceFabric();
      if (StringUtils.isEmpty(setting.targetedRegions)) {
        throw new VeniceException(
            "The store either does not have native replication mode enabled or set up default source fabric.");
      }
      if (setting.isIncrementalPush) {
        throw new VeniceException("Incremental push is not supported while using targeted region push mode");
      }
    }

    HybridStoreConfig hybridStoreConfig = storeResponse.getStore().getHybridStoreConfig();
    storeSetting.hybridStoreConfig = hybridStoreConfig;
    if (setting.repushTTLEnabled) {
      if (hybridStoreConfig == null) {
        throw new VeniceException("Repush TTL is only supported for real-time only store.");
      } else {
        storeSetting.storeRewindTimeInSeconds = hybridStoreConfig.getRewindTimeInSeconds();
      }
    }

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
    }

    storeSetting.keySchema = getKeySchemaFromController(controllerClient, setting.controllerRetries, setting.storeName);

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
      storeSetting.sourceKafkaInputVersionInfo = sourceVersion.get();
      // Skip quota check
      storeSetting.storeStorageQuota = Store.UNLIMITED_STORAGE_QUOTA;
      if (sourceVersion.get().isChunkingEnabled() && !storeResponse.getStore().isChunkingEnabled()) {
        throw new VeniceException("Source version has chunking enabled while chunking is disabled in store config.");
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
  void createNewStoreVersion(
      PushJobSetting setting,
      long inputFileDataSize,
      ControllerClient controllerClient,
      String pushId,
      VeniceProperties props,
      Optional<ByteBuffer> optionalCompressionDictionary) {
    Version.PushType pushType = getPushType(setting);
    boolean askControllerToSendControlMessage = !pushJobSetting.sendControlMessagesDirectly;
    final String partitioners = props.getString(VENICE_PARTITIONERS, DefaultVenicePartitioner.class.getName());

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
    kafkaTopicInfo = new TopicInfo();
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
            setting.targetedRegions));
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
    kafkaTopicInfo.chunkingEnabled = storeSetting.isChunkingEnabled && !Version.isRealTimeTopic(kafkaTopicInfo.topic);
    kafkaTopicInfo.rmdChunkingEnabled = kafkaTopicInfo.chunkingEnabled && storeSetting.isRmdChunkingEnabled;

    if (pushJobSetting.isSourceKafka) {
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
      Version sourceVersion = storeSetting.sourceKafkaInputVersionInfo;

      // Chunked source version cannot be repushed if new version is not chunking enabled.
      if (sourceVersion.isChunkingEnabled() && !newVersion.isChunkingEnabled()) {
        throw new VeniceException(
            "Chunking config mismatch between the source and the new version of store "
                + storeResponse.getStore().getName() + ". Source version: " + sourceVersion.getNumber() + " is using: "
                + sourceVersion.isChunkingEnabled() + ", new version: " + newVersion.getNumber() + " is using: "
                + newVersion.isChunkingEnabled());
      }
      if (sourceVersion.isRmdChunkingEnabled() && !newVersion.isRmdChunkingEnabled()) {
        throw new VeniceException(
            "RMD Chunking config mismatch between the source and the new version of store "
                + storeResponse.getStore().getName() + ". Source version: " + sourceVersion.getNumber() + " is using: "
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

  private synchronized VeniceWriter<KafkaKey, byte[], byte[]> getVeniceWriter(TopicInfo topicInfo) {
    if (veniceWriter == null) {
      // Initialize VeniceWriter
      VeniceWriterFactory veniceWriterFactory = new VeniceWriterFactory(getVeniceWriterProperties(topicInfo));
      Properties partitionerProperties = new Properties();
      partitionerProperties.putAll(topicInfo.partitionerParams);
      VenicePartitioner venicePartitioner = PartitionUtils.getVenicePartitioner(
          topicInfo.partitionerClass,
          topicInfo.amplificationFactor,
          new VeniceProperties(partitionerProperties));
      VeniceWriterOptions vwOptions = new VeniceWriterOptions.Builder(topicInfo.topic).setUseKafkaKeySerializer(true)
          .setPartitioner(venicePartitioner)
          .setPartitionCount(
              Version.isVersionTopic(topicInfo.topic)
                  ? topicInfo.partitionCount * topicInfo.amplificationFactor
                  : topicInfo.partitionCount)
          .build();
      VeniceWriter<KafkaKey, byte[], byte[]> newVeniceWriter = veniceWriterFactory.createVeniceWriter(vwOptions);
      LOGGER.info("Created VeniceWriter: {}", newVeniceWriter);
      veniceWriter = newVeniceWriter;
    }
    return veniceWriter;
  }

  private synchronized Properties getVeniceWriterProperties(TopicInfo topicInfo) {
    if (veniceWriterProperties == null) {
      veniceWriterProperties = createVeniceWriterProperties(topicInfo.kafkaUrl, topicInfo.sslToKafka);
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

  private synchronized void closeVeniceWriter() {
    if (veniceWriter != null) {
      veniceWriter.close();
      veniceWriter = null;
    }
  }

  /**
   * High level, we want to poll the consumption job status until it errors or is complete. This is more complicated
   * because we might be dealing with multiple destination clusters and we might not be able to reach all of them. We
   * are using a semantic of "poll until all accessible datacenters report success".
   *
   * If any datacenter report an explicit error status, we throw an exception and fail the job. However, datacenters
   * with COMPLETED status will be serving new data.
   */
  void pollStatusUntilComplete(
      Optional<String> incrementalPushVersion,
      ControllerClient controllerClient,
      PushJobSetting pushJobSetting,
      TopicInfo topicInfo) {
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

    String topicToMonitor = getTopicToMonitor(topicInfo, pushJobSetting);

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
              .queryOverallJobStatus(topicToMonitor, incrementalPushVersion, pushJobSetting.targetedRegions));

      if (response.isError()) {
        // status could not be queried which could be due to a communication error.
        throw new VeniceException(
            "Failed to connect to: " + pushJobSetting.veniceControllerUrl + " to query job status, after "
                + pushJobSetting.controllerStatusPollRetries + " attempts. Error: " + response.getError());
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
          // 1) For regular push, one or more DC could have an UNKNOWN status and never successfully reported a
          // completed status before,
          // but if the majority of datacenters have completed, we give up on the unreachable datacenter
          // and start truncating the data topic.
          // 2) For targeted region push, not all targeted regions have completed.

          StringBuilder errorMsg = new StringBuilder().append("Push job error reported by controller: ")
              .append(pushJobSetting.veniceControllerUrl)
              .append("\ncontroller response: ")
              .append(response);

          throw new VeniceException(errorMsg.toString());
        }

        // Every known datacenter have successfully reported a completed status at least once.
        LOGGER.info("Successfully pushed {}", topicInfo.topic);
        return;
      }
      long bootstrapToOnlineTimeoutInHours = storeSetting.storeResponse.getStore().getBootstrapToOnlineTimeoutInHours();
      long durationMs = System.currentTimeMillis() - pollStartTimeMs;
      if (durationMs > TimeUnit.HOURS.toMillis(bootstrapToOnlineTimeoutInHours)) {
        throw new VeniceException(
            "Failing push-job for store " + storeSetting.storeResponse.getName() + " which is still running after "
                + TimeUnit.MILLISECONDS.toHours(durationMs) + " hours.");
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

  void setupMRConf(
      JobConf jobConf,
      TopicInfo topicInfo,
      PushJobSetting pushJobSetting,
      PushJobSchemaInfo pushJobSchemaInfo,
      StoreSetting storeSetting,
      VeniceProperties props,
      String id,
      String inputDirectory) {
    setupDefaultJobConf(jobConf, topicInfo, pushJobSetting, pushJobSchemaInfo, storeSetting, props, id);
    setupInputFormatConf(jobConf, pushJobSchemaInfo, inputDirectory);
    setupReducerConf(jobConf, pushJobSetting, topicInfo);
  }

  /**
   * Common configuration for all the Mapreduce Jobs run as part of VPJ
   *
   * @param conf
   * @param jobName
   */
  private void setupCommonJobConf(JobConf conf, String jobName) {
    if (System.getenv(HADOOP_TOKEN_FILE_LOCATION) != null) {
      conf.set(MAPREDUCE_JOB_CREDENTIALS_BINARY, System.getenv(HADOOP_TOKEN_FILE_LOCATION));
    }
    conf.setJobName(jobName);
    conf.setJarByClass(this.getClass());
    if (isSslEnabled()) {
      conf.set(
          SSL_CONFIGURATOR_CLASS_CONFIG,
          props.getString(SSL_CONFIGURATOR_CLASS_CONFIG, TempFileSSLConfigurator.class.getName()));
      conf.set(SSL_KEY_STORE_PROPERTY_NAME, props.getString(SSL_KEY_STORE_PROPERTY_NAME));
      conf.set(SSL_TRUST_STORE_PROPERTY_NAME, props.getString(SSL_TRUST_STORE_PROPERTY_NAME));
      conf.set(SSL_KEY_PASSWORD_PROPERTY_NAME, props.getString(SSL_KEY_PASSWORD_PROPERTY_NAME));
    }

    // Hadoop2 dev cluster provides a newer version of an avro dependency.
    // Set mapreduce.job.classloader to true to force the use of the older avro dependency.
    conf.setBoolean(MAPREDUCE_JOB_CLASSLOADER, true);
    LOGGER.info("{}: {}", MAPREDUCE_JOB_CLASSLOADER, conf.get(MAPREDUCE_JOB_CLASSLOADER));

    /** Not writing anything to the output for key and value and so the format is Null:
     * Can be overwritten later for specific settings */
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(NullWritable.class);
    conf.setOutputFormat(NullOutputFormat.class);

    /** compression related common configs */
    conf.setBoolean(COMPRESSION_METRIC_COLLECTION_ENABLED, pushJobSetting.compressionMetricCollectionEnabled);
    conf.setBoolean(ZSTD_DICTIONARY_CREATION_REQUIRED, isZstdDictCreationRequired);
  }

  protected void setupDefaultJobConf(
      JobConf conf,
      TopicInfo topicInfo,
      PushJobSetting pushJobSetting,
      PushJobSchemaInfo pushJobSchemaInfo,
      StoreSetting storeSetting,
      VeniceProperties props,
      String id) {
    setupCommonJobConf(conf, id + ":venice_push_job-" + topicInfo.topic);
    conf.set(BATCH_NUM_BYTES_PROP, Integer.toString(pushJobSetting.batchNumBytes));
    conf.set(TOPIC_PROP, topicInfo.topic);
    // We need the two configs with bootstrap servers since VeniceWriterFactory requires kafka.bootstrap.servers while
    // the Kafka consumer requires bootstrap.servers.
    conf.set(KAFKA_BOOTSTRAP_SERVERS, topicInfo.kafkaUrl);
    conf.set(KAFKA_BOOTSTRAP_SERVERS, topicInfo.kafkaUrl);
    conf.set(PARTITIONER_CLASS, topicInfo.partitionerClass);
    // flatten partitionerParams since JobConf class does not support set an object
    if (topicInfo.partitionerParams != null) {
      topicInfo.partitionerParams.forEach(conf::set);
    }
    conf.setInt(AMPLIFICATION_FACTOR, topicInfo.amplificationFactor);
    if (topicInfo.sslToKafka) {
      conf.set(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, KAFKA_SECURITY_PROTOCOL);
      props.keySet().stream().filter(key -> key.toLowerCase().startsWith(SSL_PREFIX)).forEach(key -> {
        conf.set(key, props.getString(key));
      });
    }
    conf.setBoolean(ALLOW_DUPLICATE_KEY, pushJobSetting.isDuplicateKeyAllowed);
    conf.setBoolean(VeniceWriter.ENABLE_CHUNKING, kafkaTopicInfo.chunkingEnabled);
    conf.setBoolean(VeniceWriter.ENABLE_RMD_CHUNKING, kafkaTopicInfo.rmdChunkingEnabled);

    conf.set(STORAGE_QUOTA_PROP, Long.toString(storeSetting.storeStorageQuota));

    if (pushJobSetting.isSourceKafka) {
      // Use some fake value schema id here since it won't be used
      conf.setInt(VALUE_SCHEMA_ID_PROP, -1);
      /**
       * Kafka input topic could be inferred from the store name, but absent from the original properties.
       * So here will set it up from {@link #pushJobSetting}.
       */
      conf.set(KAFKA_INPUT_TOPIC, pushJobSetting.kafkaInputTopic);
      conf.set(KAFKA_INPUT_BROKER_URL, pushJobSetting.kafkaInputBrokerUrl);
      conf.setLong(REPUSH_TTL_IN_SECONDS, pushJobSetting.repushTTLInSeconds);
      if (pushJobSetting.repushTTLEnabled) {
        conf.setInt(REPUSH_TTL_POLICY, TTLResolutionPolicy.RT_WRITE_ONLY.getValue()); // only support one policy
        // thus not allow any value passed
        // in.
        conf.set(RMD_SCHEMA_DIR, pushJobSetting.rmdSchemaDir);
      }
      // Pass the compression strategy of source version to repush MR job
      conf.set(
          KAFKA_INPUT_SOURCE_COMPRESSION_STRATEGY,
          storeSetting.sourceKafkaInputVersionInfo.getCompressionStrategy().name());
      conf.set(
          KAFKA_INPUT_SOURCE_TOPIC_CHUNKING_ENABLED,
          Boolean.toString(storeSetting.sourceKafkaInputVersionInfo.isChunkingEnabled()));

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
    conf.set(ETL_VALUE_SCHEMA_TRANSFORMATION, pushJobSetting.etlValueSchemaTransformation.name());
    conf.setBoolean(EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED, pushJobSetting.extendedSchemaValidityCheckEnabled);

    // Compression related
    // Note that COMPRESSION_STRATEGY is from topic creation response as it might be different from the store config
    // (eg: for inc push)
    conf.set(
        COMPRESSION_STRATEGY,
        topicInfo.compressionStrategy != null
            ? topicInfo.compressionStrategy.toString()
            : CompressionStrategy.NO_OP.toString());
    conf.set(
        ZSTD_COMPRESSION_LEVEL,
        props.getString(ZSTD_COMPRESSION_LEVEL, String.valueOf(Zstd.maxCompressionLevel())));
    conf.setBoolean(ZSTD_DICTIONARY_CREATION_SUCCESS, isZstdDictCreationSuccess);

    /** Allow overriding properties if their names start with {@link HADOOP_PREFIX}.
     *  Allow overriding properties if their names start with {@link VeniceWriter.VENICE_WRITER_CONFIG_PREFIX}
     *  Allow overriding properties if their names start with {@link ApacheKafkaProducerConfig.KAFKA_CONFIG_PREFIX}
     *  Allow overriding properties if their names start with {@link KafkaInputRecordReader.KIF_RECORD_READER_KAFKA_CONFIG_PREFIX}
     **/
    List<String> passThroughPrefixList = Arrays.asList(
        VeniceWriter.VENICE_WRITER_CONFIG_PREFIX,
        KAFKA_CONFIG_PREFIX,
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
  }

  protected void setupInputFormatConf(JobConf jobConf, PushJobSchemaInfo pushJobSchemaInfo, String inputDirectory) {
    if (pushJobSetting.isSourceKafka) {
      Schema keySchemaFromController = storeSetting.keySchema;
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

  private void setupReducerConf(JobConf jobConf, PushJobSetting pushJobSetting, TopicInfo topicInfo) {
    if (pushJobSetting.isSourceKafka) {
      jobConf.setOutputKeyComparatorClass(KafkaInputKeyComparator.class);
      jobConf.setOutputValueGroupingComparator(KafkaInputValueGroupingComparator.class);
      jobConf.setCombinerKeyGroupingComparator(KafkaInputValueGroupingComparator.class);
      jobConf.setPartitionerClass(KafkaInputMRPartitioner.class);
    } else {
      jobConf.setPartitionerClass(this.mapRedPartitionerClass);
    }
    jobConf.setReduceSpeculativeExecution(pushJobSetting.enableReducerSpeculativeExecution);
    jobConf.setNumReduceTasks(topicInfo.partitionCount * topicInfo.amplificationFactor);
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
      String inputDirectory) throws Exception {
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
    setupDefaultJobConfToValidateSchemaAndBuildDict(conf, pushJobSetting, storeSetting, props, id);
    setupInputFormatConfToValidateSchemaAndBuildDict(conf, pushJobSchemaInfo, inputDirectory);
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
      StoreSetting storeSetting,
      VeniceProperties props,
      String id) {
    setupCommonJobConf(conf, id + ":venice_push_job_validate_schema_and_build_dict-" + pushJobSetting.storeName);
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
    // USE_MAPPER_TO_BUILD_DICTIONARY is still needed to be passed here for validateInputAndGetInfo
    conf.setBoolean(USE_MAPPER_TO_BUILD_DICTIONARY, pushJobSetting.useMapperToBuildDict);
    conf.set(MAPPER_OUTPUT_DIRECTORY, pushJobSetting.useMapperToBuildDictOutputPath);
    conf.set(COMPRESSION_STRATEGY, storeSetting.compressionStrategy.toString());
    validateSchemaAndBuildDictMapperOutputDirectory = getValidateSchemaAndBuildDictionaryOutputDir(
        pushJobSetting.useMapperToBuildDictOutputPath,
        pushJobSetting.storeName,
        props.getString(JOB_EXEC_ID, ""));
    conf.set(VALIDATE_SCHEMA_AND_BUILD_DICT_MAPPER_OUTPUT_DIRECTORY, validateSchemaAndBuildDictMapperOutputDirectory);

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

    AvroJob.setOutputSchema(conf, ValidateSchemaAndBuildDictMapperOutput.getClassSchema());
    conf.setOutputFormat(ValidateSchemaAndBuildDictOutputFormat.class);

    /** key/value fields to be used in {@link DefaultInputDataInfoProvider#validateInputAndGetInfo(String)} in the mapper
     * These values were populated to schemaInfo in the same function but in driver */
    conf.set(KEY_FIELD_PROP, pushJobSchemaInfo.getKeyField());
    conf.set(VALUE_FIELD_PROP, pushJobSchemaInfo.getValueField());
  }

  private void logPushJobProperties(
      TopicInfo topicInfo,
      PushJobSetting pushJobSetting,
      PushJobSchemaInfo pushJobSchemaInfo,
      String inputDirectory,
      long inputFileDataSize) {
    LOGGER.info(
        pushJobPropertiesToString(topicInfo, pushJobSetting, pushJobSchemaInfo, inputDirectory, inputFileDataSize));
  }

  private String pushJobPropertiesToString(
      TopicInfo topicInfo,
      PushJobSetting pushJobSetting,
      PushJobSchemaInfo pushJobSchemaInfo,
      String inputDirectory,
      final long inputFileDataSize) {
    List<String> propKeyValuePairs = new ArrayList<>();
    propKeyValuePairs.add("Job ID: " + this.jobId);
    propKeyValuePairs.add("Kafka URL: " + topicInfo.kafkaUrl);
    propKeyValuePairs.add("Kafka Topic: " + topicInfo.topic);
    propKeyValuePairs.add("Kafka topic partition count: " + topicInfo.partitionCount);
    propKeyValuePairs.add("Kafka Queue Bytes: " + pushJobSetting.batchNumBytes);
    propKeyValuePairs.add("Input Directory: " + inputDirectory);
    propKeyValuePairs.add("Venice Store Name: " + pushJobSetting.storeName);
    propKeyValuePairs.add("Venice Cluster Name: " + pushJobSetting.clusterName);
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
      TopicInfo topicInfo) {
    // Attempting to kill job. There's a race condition, but meh. Better kill when you know it's running
    killJob();
    if (!pushJobSetting.isIncrementalPush && topicInfo != null) {
      final int maxRetryAttempt = 10;
      int currentRetryAttempt = 0;
      while (currentRetryAttempt < maxRetryAttempt) {
        if (!StringUtils.isEmpty(topicInfo.topic)) {
          break;
        }
        Utils.sleep(Duration.ofMillis(10).toMillis());
        currentRetryAttempt++;
      }
      if (StringUtils.isEmpty(topicInfo.topic)) {
        LOGGER.error("Could not find a store version to delete for store: {}", pushJobSetting.storeName);
      } else {
        ControllerClient.retryableRequest(
            controllerClient,
            pushJobSetting.controllerRetries,
            c -> c.killOfflinePushJob(topicInfo.topic));
        LOGGER.info("Offline push job has been killed, topic: {}", topicInfo.topic);
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

  public String getKafkaTopic() {
    return kafkaTopicInfo.topic;
  }

  public String getKafkaUrl() {
    return kafkaTopicInfo.kafkaUrl;
  }

  public String getInputDirectory() {
    return inputDirectory;
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

  private String getTopicToMonitor(TopicInfo topicInfo, PushJobSetting jobSetting) {
    return Version.isRealTimeTopic(topicInfo.topic)
        ? Version.composeKafkaTopic(jobSetting.storeName, topicInfo.version)
        : topicInfo.topic;
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
    closeVeniceWriter();
    Utils.closeQuietlyWithErrorLogged(controllerClient);
    Utils.closeQuietlyWithErrorLogged(kmeSchemaSystemStoreControllerClient);
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
