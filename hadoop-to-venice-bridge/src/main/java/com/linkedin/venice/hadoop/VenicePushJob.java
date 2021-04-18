package com.linkedin.venice.hadoop;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StorageEngineOverheadRatioResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.etl.ETLValueSchemaTransformation;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.heartbeat.DefaultPushJobHeartbeatSenderFactory;
import com.linkedin.venice.hadoop.heartbeat.NoOpPushJobHeartbeatSenderFactory;
import com.linkedin.venice.hadoop.heartbeat.PushJobHeartbeatSender;
import com.linkedin.venice.hadoop.heartbeat.PushJobHeartbeatSenderFactory;
import com.linkedin.venice.hadoop.input.kafka.KafkaInputFormat;
import com.linkedin.venice.hadoop.input.kafka.VeniceKafkaInputMapper;
import com.linkedin.venice.hadoop.input.kafka.VeniceKafkaInputReducer;
import com.linkedin.venice.hadoop.pbnj.PostBulkLoadAnalysisMapper;
import com.linkedin.venice.hadoop.ssl.SSLConfigurator;
import com.linkedin.venice.hadoop.ssl.TempFileSSLConfigurator;
import com.linkedin.venice.hadoop.ssl.UserCredentialsFactory;
import com.linkedin.venice.hadoop.utils.AvroSchemaParseUtils;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushstatushelper.PushStatusStoreRecordDeleter;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.status.PushJobDetailsStatus;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobDetailsStatusTuple;
import com.linkedin.venice.utils.EncodingUtils;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.ApacheKafkaProducer;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictTrainer;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.commons.io.IOUtils;
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
import org.apache.log4j.Logger;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.linkedin.venice.CommonConfigKeys.*;
import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.VeniceConstants.*;
import static com.linkedin.venice.status.BatchJobHeartbeatConfigs.*;
import static org.apache.hadoop.mapreduce.MRJobConfig.*;
import static org.apache.hadoop.security.UserGroupInformation.*;

/**
 * This class sets up the Hadoop job used to push data to Venice.
 * The job reads the input data off HDFS. It supports 2 kinds of
 * input -- Avro / Binary Json (Vson).
 */
public class VenicePushJob implements AutoCloseable, Cloneable {
  //Avro input configs
  public static final String LEGACY_AVRO_KEY_FIELD_PROP = "avro.key.field";
  public static final String LEGACY_AVRO_VALUE_FIELD_PROP = "avro.value.field";

  public static final String KEY_FIELD_PROP = "key.field";
  public static final String VALUE_FIELD_PROP = "value.field";
  public static final String SCHEMA_STRING_PROP = "schema";
  public static final String EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED = "extended.schema.validity.check.enabled";
  public static final Boolean DEFAULT_EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED = true;

  //Vson input configs
  //Vson files store key/value schema on file header. key / value fields are optional
  //and should be specified only when key / value schema is the partial of the files.
  public final static String FILE_KEY_SCHEMA = "key.schema";
  public final static String FILE_VALUE_SCHEMA = "value.schema";
  public final static String INCREMENTAL_PUSH = "incremental.push";

  //veniceReducer will not fail fast and override the previous key if this is true and duplicate keys incur.
  public final static String ALLOW_DUPLICATE_KEY = "allow.duplicate.key";

  public final static String KAFKA_PRODUCER_REQUEST_TIMEOUT_MS
      = ApacheKafkaProducer.PROPERTIES_KAFKA_PREFIX + "request.timeout.ms"; // kafka.request.timeout.ms
  public final static String KAFKA_PRODUCER_RETRIES_CONFIG
      = ApacheKafkaProducer.PROPERTIES_KAFKA_PREFIX + "retries"; //kafka.retries
  public final static String POLL_STATUS_RETRY_ATTEMPTS = "poll.status.retry.attempts";
  public final static String CONTROLLER_REQUEST_RETRY_ATTEMPTS = "controller.request.retry.attempts";
  public final static String POLL_JOB_STATUS_INTERVAL_MS = "poll.job.status.interval.ms";
  public final static String JOB_STATUS_IN_UNKNOWN_STATE_TIMEOUT_MS = "job.status.in.unknown.state.timeout.ms";
  public final static String SEND_CONTROL_MESSAGES_DIRECTLY = "send.control.messages.directly";
  public final static String SOURCE_ETL = "source.etl";
  public final static String ETL_VALUE_SCHEMA_TRANSFORMATION = "etl.value.schema.transformation";

  /**
   * Configs used to enable Kafka Input.
   */
  public final static String SOURCE_KAFKA = "source.kafka";
  /**
   * TODO: consider to automatically discover the source topic for the specified store.
   * We need to be careful in the following scenarios:
   * 1. Not all the prod colos are using the same current version if the previous push experiences a partial failure.
   * 2. We might want to re-push from a backup version, which should be unlikely.
   */
  public static final String KAFKA_INPUT_TOPIC = "kafka.input.topic";
  public static final String KAFKA_INPUT_BROKER_URL = "kafka.input.broker.url";
  // Optional
  public static final String KAFKA_INPUT_MAX_RECORDS_PER_MAPPER = "kafka.input.max.records.per.mapper";

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

  public static final String BATCH_STARTING_FABRIC = "batch.starting.fabric";

  public static final String ENABLE_WRITE_COMPUTE = "venice.write.compute.enable";
  public static final String ENABLE_PUSH = "venice.push.enable";
  public static final String ENABLE_SSL = "venice.ssl.enable";
  public static final String VENICE_CLUSTER_NAME_PROP = "cluster.name";
  public static final String VENICE_STORE_NAME_PROP = "venice.store.name";
  public static final String INPUT_PATH_PROP = "input.path";
  public static final String BATCH_NUM_BYTES_PROP = "batch.num.bytes";
  /**
   * Specifies a list of partitioners venice supported.
   * It contains a string of concatenated partitioner class names separated by comma.
   */
  public static final String VENICE_PARTITIONERS_PROP = "venice.partitioners";

  public static final String VALUE_SCHEMA_ID_PROP = "value.schema.id";
  public static final String DERIVED_SCHEMA_ID_PROP = "derived.schema.id";
  public static final String KAFKA_URL_PROP = "venice.kafka.url";
  public static final String TOPIC_PROP = "venice.kafka.topic";
  protected static final String HADOOP_PREFIX = "hadoop-conf.";
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
  public static final String SSL_KEY_PASSWORD_PROPERTY_NAME= "ssl.key.password.property.name";
  public static final String JOB_EXEC_URL = "job.execution.url";

  /**
   * Config to enable the service that uploads push job statuses to the controller using
   * {@code ControllerClient.uploadPushJobStatus()}, the job status is then packaged and sent to dedicated Kafka channel.
   */
  public static final String PUSH_JOB_STATUS_UPLOAD_ENABLE = "push.job.status.upload.enable";
  public static final String REDUCER_SPECULATIVE_EXECUTION_ENABLE = "reducer.speculative.execution.enable";

  /**
   * Config that controls the minimum logging interval for the reducers to log their status such as internal states,
   * metrics and progress.
   */
  public static final String REDUCER_MINIMUM_LOGGING_INTERVAL_MS = "reducer.minimum.logging.interval.ms";

  /**
   * The interval of number of messages upon which some telemetry code is executed, including logging certain info
   * in the reducer logs, as well as updating the counters specified in {@value KAFKA_METRICS_TO_REPORT_AS_MR_COUNTERS}.
   */
  public static final String TELEMETRY_MESSAGE_INTERVAL = "telemetry.message.interval";

  /**
   * The Kafka producer metric names included in this list will be reported as MapReduce counters by grouping
   * them according to which broker they interacted with. This means the semantic of these counters may not be
   * the same as that of the metrics they stem from. For example, the average queue time representing how log a
   * record stays stuck in the Kafka producer's buffer would not represent the average queue time anymore, since
   * the counter will sum up that value multiple times.
   *
   * The point of having these per-broker aggregates is to identify whether Kafka brokers are performing unevenly.
   */
  public static final String KAFKA_METRICS_TO_REPORT_AS_MR_COUNTERS = "kafka.metrics.to.report.as.mr.counters";

  /**
   * Config to control the Compression Level for ZSTD Dictionary Compression.
   */
  public static final String ZSTD_COMPRESSION_LEVEL = "zstd.compression.level";
  public static final int DEFAULT_BATCH_BYTES_SIZE = 1000000;
  public static final boolean SORTED = true;
  private static final Logger LOGGER = Logger.getLogger(VenicePushJob.class);

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

  private String inputDirectory;
  // Immutable state
  protected final VeniceProperties props;
  private final String jobId;
  private ControllerClient controllerClient;
  private String clusterName;
  private RunningJob runningJob;
  // Job config for regular push job
  protected JobConf jobConf = new JobConf();
  // Job config for pbnj
  protected JobConf pbnjJobConf = new JobConf();
  protected InputDataInfoProvider inputDataInfoProvider;

  // Total input data size, which is used to talk to controller to decide whether we have enough quota or not
  private long inputFileDataSize;
  private boolean inputFileHasRecords;
  private long jobStartTimeMs;
  private Properties veniceWriterProperties;
  private Properties sslProperties;
  private VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter; // Lazily initialized
  private JobClientWrapper jobClientWrapper;
  // A controller client that is used to discover cluster
  private ControllerClient clusterDiscoveryControllerClient;
  private SentPushJobDetailsTracker sentPushJobDetailsTracker;
  private Class<? extends Partitioner> mapRedPartitionerClass = VeniceMRPartitioner.class;

  protected static class SchemaInfo {
    boolean isAvro = true;
    int valueSchemaId; // Value schema id retrieved from backend for valueSchemaString
    int derivedSchemaId = -1;
    String keyField;
    String valueField;
    String fileSchemaString;
    String keySchemaString;
    String valueSchemaString;
    String vsonFileKeySchema;
    String vsonFileValueSchema;
  }
  private SchemaInfo schemaInfo;

  protected static class PushJobSetting {
    boolean enablePush;
    boolean enableSsl;
    String sslFactoryClassName;
    String veniceControllerUrl;
    String veniceRouterUrl;
    String storeName;
    String batchStartingFabric;
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
    long minimumReducerLoggingIntervalInMs;
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
  }

  private VersionTopicInfo kafkaTopicInfo;

  private PushJobDetails pushJobDetails;
  private final InternalAvroSpecificSerializer<PushJobDetails> pushJobDetailsSerializer =
      AvroProtocolDefinition.PUSH_JOB_DETAILS.getSerializer();

  protected static class StoreSetting {
    boolean isChunkingEnabled;
    long storeStorageQuota;
    double storageEngineOverheadRatio;
    boolean isSchemaAutoRegisterFromPushJobEnabled;
    CompressionStrategy compressionStrategy;
    boolean isLeaderFollowerModelEnabled;
    boolean isWriteComputeEnabled;
    boolean isIncrementalPushEnabled;
    Version sourceKafkaInputVersionInfo;
  }

  protected StoreSetting storeSetting;
  private InputStorageQuotaTracker inputStorageQuotaTracker;
  private PushJobHeartbeatSenderFactory pushJobHeartbeatSenderFactory;
  private final boolean jobHeartbeatEnabled;

  protected static class ZstdConfig {
    ZstdDictTrainer zstdDictTrainer;
    int maxBytesPerFile;
    int dictSize;
    int sampleSize;
  }

  public enum PushJobCheckpoints {
    INITIALIZE_PUSH_JOB(0),
    NEW_VERSION_CREATED(1),
    START_MAP_REDUCE_JOB(2),
    MAP_REDUCE_JOB_COMPLETED(3),
    START_JOB_STATUS_POLLING(4),
    JOB_STATUS_POLLING_COMPLETED(5),
    QUOTA_EXCEEDED(-1),
    WRITE_ACL_FAILED(-2),
    DUP_KEY_WITH_DIFF_VALUE(-3),
    FILE_SCHEMA_VALIDATION_FAILED(-4),
    EXTENDED_FILE_SCHEMA_VALIDATION_FAILED(-5);

    private final int value;

    PushJobCheckpoints(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

  // Visible for testing
  VenicePushJob(String jobId, Properties vanillaProps, ControllerClient controllerClient) {
    this(jobId, vanillaProps, controllerClient, null);
  }

  // Visible for testing
  VenicePushJob(String jobId, Properties vanillaProps, ControllerClient controllerClient, ControllerClient clusterDiscoveryControllerClient) {
    this.controllerClient = controllerClient;
    this.clusterDiscoveryControllerClient = clusterDiscoveryControllerClient;
    this.jobId = jobId;
    props = getVenicePropsFromVanillaProps(vanillaProps);
    LOGGER.info("Constructing " + VenicePushJob.class.getSimpleName() + ": " + props.toString(true));
    String veniceControllerUrl = getVeniceControllerUrl(props);
    LOGGER.info("Get Venice controller URL: " + veniceControllerUrl);
    this.clusterName = getClusterName(veniceControllerUrl, props);
    LOGGER.info("Get Venice cluster name: " + clusterName);
    // Optional configs:
    pushJobSetting = getPushJobSetting(veniceControllerUrl, this.clusterName, props);
    jobHeartbeatEnabled = props.getBoolean(HEARTBEAT_ENABLED_CONFIG.getConfigName(), false);
    if (jobHeartbeatEnabled) {
      LOGGER.info("Push job heartbeat is enabled.");
      pushJobHeartbeatSenderFactory = new DefaultPushJobHeartbeatSenderFactory();
    } else {
      LOGGER.info("Push job heartbeat is NOT enabled.");
      pushJobHeartbeatSenderFactory = new NoOpPushJobHeartbeatSenderFactory();
    }
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
      if (vanillaProps.containsKey(KEY_FIELD_PROP) &&
          !vanillaProps.getProperty(KEY_FIELD_PROP).equals(vanillaProps.getProperty(LEGACY_AVRO_KEY_FIELD_PROP))) {
        throw new VeniceException("Duplicate key filed found in config. Both avro.key.field and key.field are set up.");
      }
      vanillaProps.setProperty(KEY_FIELD_PROP, vanillaProps.getProperty(LEGACY_AVRO_KEY_FIELD_PROP));
    }
    if (vanillaProps.containsKey(LEGACY_AVRO_VALUE_FIELD_PROP)) {
      if (vanillaProps.containsKey(VALUE_FIELD_PROP) &&
          !vanillaProps.getProperty(VALUE_FIELD_PROP).equals(vanillaProps.getProperty(LEGACY_AVRO_VALUE_FIELD_PROP))) {
        throw new VeniceException("Duplicate value filed found in config. Both avro.value.field and value.field are set up.");
      }
      vanillaProps.setProperty(VALUE_FIELD_PROP, vanillaProps.getProperty(LEGACY_AVRO_VALUE_FIELD_PROP));
    }
    String[] requiredSSLPropertiesNames = new String[]{SSL_KEY_PASSWORD_PROPERTY_NAME,SSL_KEY_STORE_PASSWORD_PROPERTY_NAME, SSL_KEY_STORE_PROPERTY_NAME,SSL_TRUST_STORE_PROPERTY_NAME};
    for (String sslPropertyName : requiredSSLPropertiesNames) {
      if (!vanillaProps.containsKey(sslPropertyName)) {
        throw new VeniceException("Miss the require ssl property name: "+sslPropertyName);
      }
    }
    return new VeniceProperties(vanillaProps);
  }

  private PushJobSetting getPushJobSetting(String veniceControllerUrl, String clusterName, VeniceProperties props) {
    PushJobSetting pushJobSetting = new PushJobSetting();
    pushJobSetting.veniceControllerUrl = veniceControllerUrl;
    pushJobSetting.enablePush = props.getBoolean(ENABLE_PUSH, true);
    /**
     * TODO: after controller SSL support is rolled out everywhere, change the default behavior for ssl enabled to true;
     * Besides, change the venice controller urls list for all push job to use the new port
     */
    pushJobSetting.enableSsl = props.getBoolean(ENABLE_SSL, false);
    pushJobSetting.sslFactoryClassName = props.getString(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
    if (props.containsKey(BATCH_STARTING_FABRIC)) {
      pushJobSetting.batchStartingFabric = props.getString(BATCH_STARTING_FABRIC);
    }
    pushJobSetting.batchNumBytes = props.getInt(BATCH_NUM_BYTES_PROP, DEFAULT_BATCH_BYTES_SIZE);
    pushJobSetting.enablePBNJ = props.getBoolean(PBNJ_ENABLE, false);
    pushJobSetting.pbnjFailFast = props.getBoolean(PBNJ_FAIL_FAST, false);
    pushJobSetting.pbnjAsync = props.getBoolean(PBNJ_ASYNC, false);
    pushJobSetting.pbnjSamplingRatio = props.getDouble(PBNJ_SAMPLING_RATIO_PROP, 1.0);
    pushJobSetting.isIncrementalPush = props.getBoolean(INCREMENTAL_PUSH, false);
    pushJobSetting.isDuplicateKeyAllowed = props.getBoolean(ALLOW_DUPLICATE_KEY, false);
    pushJobSetting.enablePushJobStatusUpload = props.getBoolean(PUSH_JOB_STATUS_UPLOAD_ENABLE, false);
    pushJobSetting.enableReducerSpeculativeExecution = props.getBoolean(REDUCER_SPECULATIVE_EXECUTION_ENABLE, false);
    pushJobSetting.minimumReducerLoggingIntervalInMs = props.getLong(REDUCER_MINIMUM_LOGGING_INTERVAL_MS, TimeUnit.MINUTES.toMillis(1));
    pushJobSetting.controllerRetries = props.getInt(CONTROLLER_REQUEST_RETRY_ATTEMPTS, 1);
    pushJobSetting.controllerStatusPollRetries = props.getInt(POLL_STATUS_RETRY_ATTEMPTS, 15);
    pushJobSetting.pollJobStatusIntervalMs = props.getLong(POLL_JOB_STATUS_INTERVAL_MS, DEFAULT_POLL_STATUS_INTERVAL_MS);
    pushJobSetting.jobStatusInUnknownStateTimeoutMs = props.getLong(JOB_STATUS_IN_UNKNOWN_STATE_TIMEOUT_MS, DEFAULT_JOB_STATUS_IN_UNKNOWN_STATE_TIMEOUT_MS);
    pushJobSetting.sendControlMessagesDirectly = props.getBoolean(SEND_CONTROL_MESSAGES_DIRECTLY, false);
    pushJobSetting.enableWriteCompute = props.getBoolean(ENABLE_WRITE_COMPUTE, false);
    pushJobSetting.isSourceETL = props.getBoolean(SOURCE_ETL, false);
    pushJobSetting.isSourceKafka = props.getBoolean(SOURCE_KAFKA, false);

    if (pushJobSetting.isSourceKafka) {
      /**
       * The topic could contain duplicate records since the topic could belong to a hybrid store
       * or the speculation execution could be executed for the batch store as well.
       */
      pushJobSetting.isDuplicateKeyAllowed = true;

      if (pushJobSetting.isIncrementalPush) {
        throw new VeniceException("Incremental push is not supported while using Kafka Input");
      }
      if (pushJobSetting.isSourceETL) {
        throw new VeniceException("Source ETL is not supported while using Kafka Input");
      }
      if (pushJobSetting.enablePBNJ) {
        throw new VeniceException("PBNJ is not supported while using Kafka Input");
      }
      pushJobSetting.kafkaInputBrokerUrl = props.getString(KAFKA_INPUT_BROKER_URL);
      pushJobSetting.kafkaInputTopic = getSourceTopicNameForKafkaInput(props.getString(VENICE_STORE_NAME_PROP), clusterName, props);
      pushJobSetting.storeName = props.getString(VENICE_STORE_NAME_PROP);

    } else {
      pushJobSetting.storeName = props.getString(VENICE_STORE_NAME_PROP);
    }

    if (pushJobSetting.enablePBNJ) {
      // If PBNJ is enabled, then the router URL config is mandatory
      pushJobSetting.veniceRouterUrl = props.getString(PBNJ_ROUTER_URL_PROP);
    } else {
      pushJobSetting.veniceRouterUrl = null;
    }

    if (!pushJobSetting.enablePush && !pushJobSetting.enablePBNJ) {
      throw new VeniceException("At least one of the following config properties must be true: " + ENABLE_PUSH + " or " + PBNJ_ENABLE);
    }
    return pushJobSetting;
  }

  private String getVeniceControllerUrl(VeniceProperties props) {
    String veniceControllerUrl = null;
    if (!props.containsKey(VENICE_URL_PROP) && !props.containsKey(VENICE_DISCOVER_URL_PROP)) {
      throw new VeniceException("At least one of the following config properties needs to be present: "
              + VENICE_URL_PROP + " or " + VENICE_DISCOVER_URL_PROP);
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

  private String getClusterName(String veniceControllerUrl, VeniceProperties props) {
    return discoverCluster(
            props.getString(VENICE_STORE_NAME_PROP),
            veniceControllerUrl,
            createSSlFactory(
                    props.getBoolean(ENABLE_SSL, false),
                    props.getString(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME)
    ));
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
          final String clusterName,
          final VeniceProperties properties
  ) {
    final Optional<String> userProvidedTopicNameOptional = Optional.ofNullable(properties.getString(KAFKA_INPUT_TOPIC, () -> null));
    if (userProvidedTopicNameOptional.isPresent()) {
      String derivedStoreName = Version.parseStoreFromKafkaTopicName(userProvidedTopicNameOptional.get());
      if (!Objects.equals(derivedStoreName, userProvidedStoreName)) {
        throw new IllegalArgumentException(String.format("Store user-provided name mismatch with the derived store name. " +
            "Got user-provided store name %s and derived store name %s", userProvidedStoreName, derivedStoreName));
      }
    }
    if (controllerClient == null) {
      initControllerClient(createSSlFactory(
              properties.getBoolean(ENABLE_SSL, false),
              properties.getString(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME)
              ),
              getVeniceControllerUrl(properties),
              clusterName
      );
    }
    LOGGER.info("userProvidedStoreName: " + userProvidedStoreName);
    StoreResponse storeResponse = ControllerClient.retryableRequest(controllerClient, 3, c -> c.getStore(userProvidedStoreName));
    if (storeResponse.isError()) {
      throw new VeniceException(
              String.format("Fail to get store information for store %s with error %s", userProvidedStoreName, storeResponse.getError()));
    }
    Map<String, Integer> coloToCurrentVersions = getCurrentStoreVersions(storeResponse);
    if (new HashSet<>(coloToCurrentVersions.values()).size() > 1) {
      if (userProvidedTopicNameOptional.isPresent()) {
        LOGGER.info(String.format("Got current topic version mismatch across multiple colos %s. Use user-provided topic " +
            "name: %s", coloToCurrentVersions, userProvidedTopicNameOptional.get()));
        return userProvidedTopicNameOptional.get();

      } else {
        throw new VeniceException(String.format("Got current topic version mismatch across multiple colos %s but " +
            "no user-provided topic name.", coloToCurrentVersions));
      }
    }
    Integer detectedCurrentTopicVersion = null;
    for (Integer topicVersion : coloToCurrentVersions.values()) {
      detectedCurrentTopicVersion = topicVersion;
    }
    String derivedTopicName = Version.composeKafkaTopic(userProvidedStoreName, detectedCurrentTopicVersion);
    if (userProvidedTopicNameOptional.isPresent() && !Objects.equals(derivedTopicName, userProvidedTopicNameOptional.get())) {
      throw new IllegalStateException(String.format("Mismatch between user-provided topic name and auto discovered " +
              "topic name. They are %s and %s respectively", userProvidedTopicNameOptional.get(), derivedTopicName));
    }
    return derivedTopicName;
  }

  // Visible for testing
  protected void setControllerClient(ControllerClient controllerClient) {
    this.controllerClient = controllerClient;
  }

  // Visible for testing
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

  // Visible for testing
  protected void setPushJobHeartbeatSenderFactory(PushJobHeartbeatSenderFactory pushJobHeartbeatSenderFactory) {
    this.pushJobHeartbeatSenderFactory = pushJobHeartbeatSenderFactory;
  }

  /**
   * @throws VeniceException
   */
  public void run() {
    PushJobHeartbeatSender pushJobHeartbeatSender =
            pushJobHeartbeatSenderFactory.createHeartbeatSender(props, Optional.empty());
    try {
      initPushJobDetails();
      jobStartTimeMs = System.currentTimeMillis();
      logGreeting();
      Optional<SSLFactory> sslFactory = createSSlFactory(
              props.getBoolean(ENABLE_SSL, false),
              props.getString(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME)
      );
      initControllerClient(sslFactory, pushJobSetting.veniceControllerUrl, clusterName);
      sendPushJobDetailsToController();
      inputDirectory = getInputURI(props);
      storeSetting = getSettingsFromController(controllerClient, pushJobSetting);
      inputStorageQuotaTracker = new InputStorageQuotaTracker(storeSetting.storeStorageQuota, storeSetting.storageEngineOverheadRatio);

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
        // TODO: do we actually need this information?
        InputDataInfoProvider.InputDataInfo inputInfo =
            getInputDataInfoProvider().validateInputAndGetInfo(inputDirectory);
        // Get input schema
        schemaInfo = inputInfo.getSchemaInfo();
        if (schemaInfo.isAvro) {
          validateFileSchema(schemaInfo.fileSchemaString);
        } else {
          LOGGER.info("Skip validating file schema since it is not Avro.");
        }
        inputFileDataSize = inputInfo.getInputFileDataSizeInBytes();
        inputFileHasRecords = inputInfo.hasRecords();
        validateKeySchema(controllerClient, pushJobSetting, schemaInfo);
        validateValueSchema(controllerClient, pushJobSetting, schemaInfo, storeSetting.isSchemaAutoRegisterFromPushJobEnabled);
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
        LOGGER.info("Skipping push job, since " + ENABLE_PUSH + " is set to false.");
      } else {
        Optional<ByteBuffer> optionalCompressionDictionary = getCompressionDictionary();
        long pushStartTimeMs = System.currentTimeMillis();
        String pushId = pushStartTimeMs + "_" + props.getString(JOB_EXEC_URL, "failed_to_obtain_execution_url");
        // Create new store version, topic and fetch Kafka url from backend
        createNewStoreVersion(pushJobSetting, inputFileDataSize, controllerClient, pushId, props, optionalCompressionDictionary);
        updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.NEW_VERSION_CREATED);
        // Update and send push job details with new info to the controller
        pushJobDetails.pushId = pushId;
        pushJobDetails.partitionCount = kafkaTopicInfo.partitionCount;
        pushJobDetails.valueCompressionStrategy = kafkaTopicInfo.compressionStrategy.getValue();
        pushJobDetails.chunkingEnabled = storeSetting.isChunkingEnabled;
        pushJobDetails.overallStatus.add(getPushJobDetailsStatusTuple(PushJobDetailsStatus.TOPIC_CREATED.getValue()));
        pushJobHeartbeatSender.start(pushJobSetting.storeName, kafkaTopicInfo.version);
        sendPushJobDetailsToController();
        // Log Venice data push job related info
        logPushJobProperties(kafkaTopicInfo, pushJobSetting, schemaInfo, clusterName, inputDirectory, inputFileDataSize);

        // Setup the hadoop job
        // If reducer phase is enabled, each reducer will sort all the messages inside one single
        // topic partition.
        setupMRConf(jobConf, kafkaTopicInfo, pushJobSetting, schemaInfo, storeSetting, props, jobId, inputDirectory);
        if (pushJobSetting.isIncrementalPush) {
          /**
           * N.B.: For now, we always send control messages directly for incremental pushes, regardless of
           * {@link pushJobSetting.sendControlMessagesDirectly}, because the controller does not yet support
           * sending these types of CM. If/when we add support for that in the controller, then we'll be able
           * to completely stop using the {@link VeniceWriter} from this class.
           */
          pushJobSetting.incrementalPushVersion = Optional.of(String.valueOf(System.currentTimeMillis()));
          getVeniceWriter(kafkaTopicInfo).broadcastStartOfIncrementalPush(pushJobSetting.incrementalPushVersion.get(), new HashMap<>());
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
                Collections.emptyMap()
            );
          } else {
            /**
             * No-op, as it was already sent as part of the call to
             * {@link createNewStoreVersion(PushJobSetting, long, ControllerClient, String, VeniceProperties)}
             */
          }
          runJobAndUpdateStatus();
          if (pushJobSetting.sendControlMessagesDirectly) {
            getVeniceWriter(kafkaTopicInfo).broadcastEndOfPush(Collections.emptyMap());
          } else {
            controllerClient.writeEndOfPush(pushJobSetting.storeName, kafkaTopicInfo.version);
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
        pollStatusUntilComplete(pushJobSetting.incrementalPushVersion, controllerClient, pushJobSetting, kafkaTopicInfo);
        updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.JOB_STATUS_POLLING_COMPLETED);
        pushJobDetails.overallStatus.add(getPushJobDetailsStatusTuple(PushJobDetailsStatus.COMPLETED.getValue()));
        pushJobDetails.jobDurationInMs = System.currentTimeMillis() - jobStartTimeMs;
        updatePushJobDetailsWithConfigs();
        sendPushJobDetailsToController();
        deleteOutdatedIncrementalPushStatusIfNeeded();
      }

      if (pushJobSetting.enablePBNJ) {
        LOGGER.info("Post-Bulkload Analysis Job is about to run.");
        setupPBNJConf(pbnjJobConf, kafkaTopicInfo, pushJobSetting, schemaInfo, storeSetting, props, jobId, inputDirectory);
        runningJob = runJobWithConfig(pbnjJobConf);
      }
    } catch (Throwable e) {
      LOGGER.error("Failed to run job.", e);
      // Make sure all the logic before killing the failed push jobs is captured in the following block
      try {
        pushJobDetails.overallStatus.add(getPushJobDetailsStatusTuple(PushJobDetailsStatus.ERROR.getValue()));
        pushJobDetails.failureDetails = e.toString();
        pushJobDetails.jobDurationInMs = System.currentTimeMillis() - jobStartTimeMs;
        updatePushJobDetailsWithConfigs();
        sendPushJobDetailsToController();
        closeVeniceWriter();
      } catch (Exception ex) {
        LOGGER.error("Error before killing the failed push job; still issue the kill job command to clean up states in backend", ex);
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
      IOUtils.closeQuietly(inputDataInfoProvider);
      pushJobHeartbeatSender.stop();
      inputDataInfoProvider = null;
    }
  }

  private void validateFileSchema(String fileSchemaString) {
    final boolean extendedSchemaValidityCheckEnabled = props.getBoolean(EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED, DEFAULT_EXTENDED_SCHEMA_VALIDITY_CHECK_ENABLED);
    boolean parseSchemaFailed = false;

    try {
      AvroSchemaParseUtils.parseSchemaFromJSONWithExtendedValidation(fileSchemaString);
    } catch (Exception e) {
      if (extendedSchemaValidityCheckEnabled) {
        updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.EXTENDED_FILE_SCHEMA_VALIDATION_FAILED);
        throw new VeniceException(e);
      }
      parseSchemaFailed = true;
    }

    if (parseSchemaFailed) {
      try {
        AvroSchemaParseUtils.parseSchemaFromJSONWithNoExtendedValidation(fileSchemaString);
      } catch (Exception e) {
        updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.FILE_SCHEMA_VALIDATION_FAILED);
        throw new VeniceException(e);
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
      if (reducerClosedCount < kafkaTopicInfo.partitionCount) {
        throw new VeniceException(String.format(
            "MR job counter is not reliable since the reducer job closed count (%d) < the partition count (%d), "
                + "while the input file data size is %d byte(s)",
            reducerClosedCount, kafkaTopicInfo.partitionCount, inputFileDataSize));
      }
    } else {
      verifyCountersWithZeroValues();
    }
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
      throw new VeniceException("Expect 0 duplicated key with distinct value. Got count: " + duplicateKeyWithDistinctCount);
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
      Properties sslProps = getSslProperties();
      sslFactory = Optional.of(SslUtils.getSSLFactory(sslProps, sslFactoryClassName));
    }
    return sslFactory;
  }

  private RunningJob runJobWithConfig(JobConf jobConf) throws IOException {
    if (jobClientWrapper == null) {
      jobClientWrapper = new DefaultJobClientWrapper();
    }
    return jobClientWrapper.runJobWithConfig(jobConf);
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
   * @param sslFactory
   */
  private void initControllerClient(Optional<SSLFactory> sslFactory, String veniceControllerUrl, String clusterName) {
    if (controllerClient == null) {
       controllerClient = new ControllerClient(clusterName, veniceControllerUrl, sslFactory);
    } else {
      LOGGER.warn("Controller client has already been initialized");
    }
  }

  private Optional<ByteBuffer> getCompressionDictionary() {
    if (pushJobSetting.isSourceKafka) {
      /**
       * Kafka Input is not supporting dict compression right now.
       */
      return Optional.empty();
    }
    ByteBuffer compressionDictionary = null;
    if (storeSetting.compressionStrategy == CompressionStrategy.ZSTD_WITH_DICT) {
      LOGGER.info("Training Zstd dictionary");
      compressionDictionary = ByteBuffer.wrap(getInputDataInfoProvider().getZstdDictTrainSamples());
      LOGGER.info("Zstd dictionary size = " + compressionDictionary.limit() + " bytes");
    } else {
      LOGGER.info("No compression dictionary is generated with the strategy " + storeSetting.compressionStrategy);
    }
    return Optional.ofNullable(compressionDictionary);
  }

  private void deleteOutdatedIncrementalPushStatusIfNeeded() {
    // delete outdated incremental push status
    // TODO(xnma): if VenicePushJob might hang forever, incremental push status wont get purged.
    // Design and implement a better scenario when Da Vinci have incremental push use cases.
    if (pushJobSetting.isIncrementalPush && kafkaTopicInfo.daVinciPushStatusStoreEnabled) {
      try (PushStatusStoreRecordDeleter pushStatusStoreDeleter =
          new PushStatusStoreRecordDeleter(new VeniceWriterFactory(getVeniceWriterProperties(kafkaTopicInfo)))) {
        pushStatusStoreDeleter.deletePushStatus(
            pushJobSetting.storeName,
            kafkaTopicInfo.version,
            pushJobSetting.incrementalPushVersion, kafkaTopicInfo.partitionCount
        );
      }
    }
  }

  private void throwVeniceException(Throwable e) throws VeniceException {
    if (!(e instanceof VeniceException)) {
      e = new VeniceException("Exception or error caught during Hadoop to Venice Bridge: " + e.getMessage(), e);
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
    pushJobDetails = new PushJobDetails();
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
      LOGGER.warn("Exception caught while updating push job details with map reduce counters. "
          + NON_CRITICAL_EXCEPTION, e);
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
        for (String key : props.keySet()) {
          pushJobConfigs.put(key, props.getString(key));
        }
        if (!pushJobConfigs.containsKey(HEARTBEAT_ENABLED_CONFIG.getConfigName())) {
          pushJobConfigs.put(HEARTBEAT_ENABLED_CONFIG.getConfigName(), String.valueOf(jobHeartbeatEnabled));
        }
        pushJobDetails.pushJobConfigs = pushJobConfigs;
        // TODO find a way to get meaningful producer configs to populate the producerConfigs map here.
        // Currently most of the easily accessible VeniceWriter configs are not interesting and contains sensitive
        // information such as passwords which doesn't seem appropriate to propagate them to push job details.
        pushJobDetails.producerConfigs = new HashMap<>();
      }
    } catch (Exception e) {
      LOGGER.warn("Exception caught while updating push job details with configs. " + NON_CRITICAL_EXCEPTION, e);
    }
  }

  /**
   * Best effort attempt to get more details on reasons behind MR failure by looking at MR counters
   *
   * @return Error message if there is any error detected in the reporter counter and empty optional otherwise
   */
  private Optional<ErrorMessage> updatePushJobDetailsWithMRDetails() throws IOException {
    // Quota exceeded
    final long totalInputDataSizeInBytes =
        MRJobCounterHelper.getTotalKeySize(runningJob.getCounters()) + MRJobCounterHelper.getTotalValueSize(runningJob.getCounters());
    if (inputStorageQuotaTracker.exceedQuota(totalInputDataSizeInBytes)) {
      updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.QUOTA_EXCEEDED);
      String errorMessage = String.format("Detect input file size quota exceeded. Job ID %s, store quota %d, "
              + "storage engine overhead ratio %.3f, total input data size in bytes %d",
          jobId, inputStorageQuotaTracker.getStoreStorageQuota(),
          inputStorageQuotaTracker.getStorageEngineOverheadRatio(), totalInputDataSizeInBytes);
      return Optional.of(new ErrorMessage(errorMessage));
    }
    // Write ACL failed
    final long writeAclFailureCount = MRJobCounterHelper.getWriteAclAuthorizationFailureCount(runningJob.getCounters());
    if (writeAclFailureCount > 0) {
      updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.WRITE_ACL_FAILED);
      String errorMessage = String.format("Detect write ACL failure. Job ID %s, counter value %d", jobId, writeAclFailureCount);
      return Optional.of(new ErrorMessage(errorMessage));
    }
    // Duplicate keys
    if (!pushJobSetting.isDuplicateKeyAllowed) {
      final long duplicateKeyWithDistinctValueCount = MRJobCounterHelper.getDuplicateKeyWithDistinctCount(runningJob.getCounters());
      if (duplicateKeyWithDistinctValueCount > 0) {
        updatePushJobDetailsWithCheckpoint(PushJobCheckpoints.DUP_KEY_WITH_DIFF_VALUE);
        String errorMessage = String.format("Detect duplicate key with distinct value. Job ID %s, counter value %d",
            jobId, duplicateKeyWithDistinctValueCount);
        return Optional.of(new ErrorMessage(errorMessage));
      }
    }
    return Optional.empty();
  }

  private void updatePushJobDetailsWithColoStatus(Map<String, String> coloSpecificInfo, Set<String> completedColos) {
    try {
      if (pushJobDetails.coloStatus == null) {
        pushJobDetails.coloStatus = new HashMap<>();
      }
      for (Map.Entry<String, String> coloEntry : coloSpecificInfo.entrySet()) {
        if (completedColos.contains(coloEntry.getKey()))
          // Don't bother updating the completed colo's status
          continue;
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
      }

    } catch (Exception e) {
      LOGGER.warn("Exception caught while updating push job details with colo status. " + NON_CRITICAL_EXCEPTION, e);
    }
  }

  private PushJobDetailsStatusTuple getPushJobDetailsStatusTuple(int status) {
    PushJobDetailsStatusTuple tuple = new PushJobDetailsStatusTuple();
    tuple.status = status;
    tuple.timestamp = System.currentTimeMillis();
    return tuple;
  }

  private void sendPushJobDetailsToController() {
    if (!pushJobSetting.enablePushJobStatusUpload || pushJobDetails == null) {
      String detailMessage = pushJobSetting.enablePushJobStatusUpload ?
          "The payload was not populated properly" : "Feature is disabled";
      LOGGER.warn("Unable to send push job details for monitoring purpose. " + detailMessage);
      return;
    }
    try {
      pushJobDetails.reportTimestamp = System.currentTimeMillis();
      int version = kafkaTopicInfo == null ? UNCREATED_VERSION_NUMBER : kafkaTopicInfo.version;
      ControllerResponse response = controllerClient.sendPushJobDetails(
          pushJobSetting.storeName,
          version,
          pushJobDetailsSerializer.serialize(null, pushJobDetails)
      );
      getSentPushJobDetailsTracker().record(pushJobSetting.storeName, version, pushJobDetails);

      if (response.isError()) {
        LOGGER.warn("Failed to send push job details. " + NON_CRITICAL_EXCEPTION + " Details: " + response.getError());
      }
    } catch (Exception e) {
      LOGGER.error("Exception caught while sending push job details. " + NON_CRITICAL_EXCEPTION, e);
    }
  }

  private SentPushJobDetailsTracker getSentPushJobDetailsTracker() {
    if (sentPushJobDetailsTracker == null) {
      sentPushJobDetailsTracker = new NoOpSentPushJobDetailsTracker();
    }
    return sentPushJobDetailsTracker;
  }

  private void logGreeting() {
    LOGGER.info("Running Hadoop to Venice Bridge: " + jobId + Utils.NEW_LINE_CHAR +
        "  _    _           _                   "        + Utils.NEW_LINE_CHAR +
        " | |  | |         | |                  "        + Utils.NEW_LINE_CHAR +
        " | |__| | __ _  __| | ___   ___  _ __  "        + Utils.NEW_LINE_CHAR +
        " |  __  |/ _` |/ _` |/ _ \\ / _ \\| '_ \\ "     + Utils.NEW_LINE_CHAR +
        " | |  | | (_| | (_| | (_) | (_) | |_) |   "     + Utils.NEW_LINE_CHAR +
        " |_|  |_|\\__,_|\\__,_|\\___/ \\___/| .__/"     + Utils.NEW_LINE_CHAR+
        "                _______         | |     "       + Utils.NEW_LINE_CHAR +
        "               |__   __|        |_|     "       + Utils.NEW_LINE_CHAR +
        "                  | | ___               "       + Utils.NEW_LINE_CHAR +
        "                  | |/ _ \\             "       + Utils.NEW_LINE_CHAR +
        "     __      __   | | (_) |             "       + Utils.NEW_LINE_CHAR +
        "     \\ \\    / /   |_|\\___/           "       + Utils.NEW_LINE_CHAR +
        "      \\ \\  / /__ _ __  _  ___ ___     "       + Utils.NEW_LINE_CHAR +
        "       \\ \\/ / _ | '_ \\| |/ __/ _ \\  "       + Utils.NEW_LINE_CHAR +
        "        \\  |  __| | | | | (_|  __/     "       + Utils.NEW_LINE_CHAR +
        "         \\/ \\___|_| |_|_|\\___\\___|  "       + Utils.NEW_LINE_CHAR +
        "      ___        _     _                "       + Utils.NEW_LINE_CHAR +
        "     |  _ \\     (_)   | |              "       + Utils.NEW_LINE_CHAR +
        "     | |_) |_ __ _  __| | __ _  ___     "       + Utils.NEW_LINE_CHAR +
        "     |  _ <| '__| |/ _` |/ _` |/ _ \\   "       + Utils.NEW_LINE_CHAR +
        "     | |_) | |  | | (_| | (_| |  __/    "       + Utils.NEW_LINE_CHAR +
        "     |____/|_|  |_|\\__,_|\\__, |\\___| "       + Utils.NEW_LINE_CHAR +
        "                          __/ |         "       + Utils.NEW_LINE_CHAR +
        "                         |___/          "       + Utils.NEW_LINE_CHAR);
  }

  /**
   * This method will talk to parent controller to validate key schema.
   */
  private void validateKeySchema(ControllerClient controllerClient, PushJobSetting setting, SchemaInfo schemaInfo) {
    SchemaResponse keySchemaResponse =
        ControllerClient.retryableRequest(controllerClient, setting.controllerRetries, c -> c.getKeySchema(setting.storeName));
    if (keySchemaResponse.isError()) {
      throw new VeniceException("Got an error in keySchemaResponse: " + keySchemaResponse.toString());
    } else if (null == keySchemaResponse.getSchemaStr()) {
      // TODO: Fix the server-side request handling. This should not happen. We should get a 404 instead.
      throw new VeniceException("Got a null schema in keySchemaResponse: " + keySchemaResponse.toString());
    }
    Schema serverSchema = Schema.parse(keySchemaResponse.getSchemaStr());
    Schema clientSchema = Schema.parse(schemaInfo.keySchemaString);
    String canonicalizedServerSchema = AvroCompatibilityHelper.toParsingForm(serverSchema);
    String canonicalizedClientSchema = AvroCompatibilityHelper.toParsingForm(clientSchema);
    if (!canonicalizedServerSchema.equals(canonicalizedClientSchema)) {
      String briefErrorMessage = "Key schema mis-match for store " + setting.storeName;
      LOGGER.error(briefErrorMessage +
          "\n\t\tController URLs: " + controllerClient.getControllerDiscoveryUrls() +
          "\n\t\tschema defined in HDFS: \t" + schemaInfo.keySchemaString +
          "\n\t\tschema defined in Venice: \t" + keySchemaResponse.getSchemaStr());
      throw new VeniceException(briefErrorMessage);
    }
  }

  /***
   * This method will talk to controller to validate value schema.
   */
  private void validateValueSchema(ControllerClient controllerClient, PushJobSetting setting, SchemaInfo schemaInfo,
      boolean schemaAutoRegisterFromPushJobEnabled) {
    LOGGER.info("Validating value schema: " + schemaInfo.valueSchemaString + " for store: " + setting.storeName);

    SchemaResponse getValueSchemaIdResponse;

    if (setting.enableWriteCompute) {
      getValueSchemaIdResponse = ControllerClient.retryableRequest(controllerClient, setting.controllerRetries, c ->
          c.getValueOrDerivedSchemaId(setting.storeName, schemaInfo.valueSchemaString));
    } else {
      getValueSchemaIdResponse = ControllerClient.retryableRequest(controllerClient, setting.controllerRetries, c ->
          c.getValueSchemaID(setting.storeName, schemaInfo.valueSchemaString));
    }
    if (getValueSchemaIdResponse.isError() && !schemaAutoRegisterFromPushJobEnabled) {
      throw new VeniceException("Failed to validate value schema for store: " + setting.storeName
          + "\nError from the server: " + getValueSchemaIdResponse.getError()
          + "\nSchema for the data file: " + schemaInfo.valueSchemaString
      );
    }

    if (getValueSchemaIdResponse.isError() && schemaAutoRegisterFromPushJobEnabled) {
      LOGGER.info("Auto registering value schema: " + schemaInfo.valueSchemaString + " for store: " + setting.storeName);
      SchemaResponse addValueSchemaResponse = ControllerClient.retryableRequest(controllerClient, setting.controllerRetries, c ->
          c.addValueSchema(setting.storeName, schemaInfo.valueSchemaString));
      if (addValueSchemaResponse.isError()) {
        throw new VeniceException("Failed to auto-register value schema for store: " + setting.storeName
            + "\nError from the server: " + addValueSchemaResponse.getError()
            + "\nSchema for the data file: " + schemaInfo.valueSchemaString
        );
      }
      // Add value schema successfully
      setSchemaIdPropInSchemaInfo(schemaInfo, addValueSchemaResponse, setting.enableWriteCompute);

    } else {
      // Get value schema ID successfully
      setSchemaIdPropInSchemaInfo(schemaInfo, getValueSchemaIdResponse, setting.enableWriteCompute);
    }
    LOGGER.info("Got schema id: " + schemaInfo.valueSchemaId + " for value schema: " + schemaInfo.valueSchemaString + " of store: " + setting.storeName);
  }

  private void setSchemaIdPropInSchemaInfo(SchemaInfo schemaInfo, SchemaResponse valueSchemaResponse, boolean enableWriteCompute) {
    schemaInfo.valueSchemaId = valueSchemaResponse.getId();
    if (enableWriteCompute) {
      schemaInfo.derivedSchemaId = valueSchemaResponse.getDerivedSchemaId();
    }
  }

  private StoreSetting getSettingsFromController(ControllerClient controllerClient, PushJobSetting setting) {
    StoreSetting storeSetting = new StoreSetting();
    StoreResponse storeResponse =
        ControllerClient.retryableRequest(controllerClient, setting.controllerRetries, c -> c.getStore(setting.storeName));

    if (storeResponse.isError()) {
      throw new VeniceException("Can't get store info. " + storeResponse.getError());
    }
    storeSetting.storeStorageQuota = storeResponse.getStore().getStorageQuotaInByte();
    storeSetting.isSchemaAutoRegisterFromPushJobEnabled = storeResponse.getStore().isSchemaAutoRegisterFromPushJobEnabled();
    StorageEngineOverheadRatioResponse storageEngineOverheadRatioResponse = ControllerClient.retryableRequest(
        controllerClient,
        setting.controllerRetries,
        c -> c.getStorageEngineOverheadRatio(setting.storeName)
    );
    if (storageEngineOverheadRatioResponse.isError()) {
      throw new VeniceException("Can't get storage engine overhead ratio. " +
          storageEngineOverheadRatioResponse.getError());
    }

    storeSetting.storageEngineOverheadRatio = storageEngineOverheadRatioResponse.getStorageEngineOverheadRatio();
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
        throw new VeniceException("Couldn't find the source version for Kafka Input: " + sourceVersionNumber +
            " from Controller for store: " + setting.storeName);
      }
      storeSetting.sourceKafkaInputVersionInfo = sourceVersion.get();
      if (storeSetting.sourceKafkaInputVersionInfo.isChunkingEnabled()) {
        /**
         * TODO: we need to extract the raw key bytes from the composite key including raw key and chunking suffix,
         * otherwise the partitioner couldn't guarantee that all the chunks belonging to the same large message will be
         * assigned to the same reducer/partition.
         */
        throw new VeniceException("Source version: " + sourceVersionNumber + " with chunking enabled is not supported right now");
      }
      if (storeSetting.sourceKafkaInputVersionInfo.getCompressionStrategy().equals(CompressionStrategy.ZSTD_WITH_DICT)) {
        /**
         * Dictionary Compression is not supported right now since Kafka Input is doing message pass-through,
         * and it won't copy the dictionary from the source version to the new version
         * during re-push.
         * TODO: We will need to extend the implementation to support compression or re-compression.
         */
        throw new VeniceException("Source version: " + sourceVersionNumber + " with ZSTD_WITH_DICT compression strategy is not supported right now");
      }
      // Skip quota check
      storeSetting.storeStorageQuota = Store.UNLIMITED_STORAGE_QUOTA;
      /**
       * Chunking should be disabled since Kafka Input is meant to do pass-through.
       */
      storeSetting.isChunkingEnabled = false;
      /**
       * If the source topic is using self-contained compression algorithm, such as Gzip, KafkaInput pass-through will produce
       * the compressed message to the new topic, so with the following logic, VPJ won't compress it again.
       */
      storeSetting.compressionStrategy = CompressionStrategy.NO_OP;
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
      Optional<ByteBuffer> optionalCompressionDictionary
  ) {
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

    //If WriteCompute is enabled, request for intermediate topic
    final boolean finalWriteComputeEnabled = writeComputeEnabled;
    kafkaTopicInfo = new VersionTopicInfo();
    VersionCreationResponse versionCreationResponse = ControllerClient.retryableRequest(
        controllerClient,
        setting.controllerRetries,
        c -> c.requestTopicForWrites(setting.storeName, inputFileDataSize, pushType, pushId,
            askControllerToSendControlMessage, SORTED, finalWriteComputeEnabled, partitioners, dictionary,
            Optional.ofNullable(setting.batchStartingFabric), jobHeartbeatEnabled)
    );
    if (versionCreationResponse.isError()) {
      throw new VeniceException("Failed to create new store version with urls: " + setting.veniceControllerUrl
          + ", error: " + versionCreationResponse.getError());
    } else if (versionCreationResponse.getVersion() == 0) {
      // TODO: Fix the server-side request handling. This should not happen. We should get a 404 instead.
      throw new VeniceException("Got version 0 from: " + versionCreationResponse.toString());
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

    if (pushJobSetting.isSourceKafka) {
      /**
       * Check whether the new version setup is compatible with the source version, and we will check the following configs:
       * 1. Chunking.
       * 2. Compression Strategy.
       * 3. Partition Count.
       * 4. Partitioner Config.
       * Since right now, the messages from the source topic will be passed through to the new version topic without
       * reformatting, we need to make sure the pass-through messages won't violate the new version config.
       *
       * TODO: maybe we should fail fast before creating a new version.
       */
      StoreResponse storeResponse = ControllerClient.retryableRequest(controllerClient, setting.controllerRetries,
          c -> c.getStore(setting.storeName));
      if (storeResponse.isError()) {
        throw new VeniceException("Failed to retrieve store response with urls: " + setting.veniceControllerUrl
            + ", error: " + storeResponse.getError());
      }
      int newVersionNum = kafkaTopicInfo.version;
      Optional<Version> newVersionOptional = storeResponse.getStore().getVersion(newVersionNum);
      if (!newVersionOptional.isPresent()) {
        throw new VeniceException("Couldn't fetch the newly created version: " + newVersionNum + " for store: " + setting.storeName
            + " with urls: " + setting.veniceControllerUrl);
      }
      Version newVersion = newVersionOptional.get();
      Version sourceVersion = storeSetting.sourceKafkaInputVersionInfo;

      if (sourceVersion.getCompressionStrategy() != newVersion.getCompressionStrategy()) {
        throw new VeniceException("Compression strategy mismatch between the source version and the new version is "
            + "not supported by Kafka Input right now, "
            + " source version: " + sourceVersion.getNumber() + " is using: " + sourceVersion.getCompressionStrategy()
            + ", new version: " + newVersion.getNumber() + " is using: " + newVersion.getCompressionStrategy());
      }
      if (sourceVersion.isChunkingEnabled() != newVersion.isChunkingEnabled()) {
        throw new VeniceException("Chunking config mismatch between the source version and the new version is "
            + "not supported by Kafka Input right now, "
            + " source version: " + sourceVersion.getNumber() + " is using: " + sourceVersion.isChunkingEnabled()
            + ", new version: " + newVersion.getNumber() + " is using: " + newVersion.isChunkingEnabled());
      }
    }
  }

  private synchronized VeniceWriter<KafkaKey, byte[], byte[]> getVeniceWriter(VersionTopicInfo versionTopicInfo) {
    if (veniceWriter == null) {
      // Initialize VeniceWriter
      VeniceWriterFactory veniceWriterFactory = new VeniceWriterFactory(getVeniceWriterProperties(versionTopicInfo));
      Properties partitionerProperties = new Properties();
      partitionerProperties.putAll(versionTopicInfo.partitionerParams);
      VenicePartitioner venicePartitioner =
          PartitionUtils.getVenicePartitioner(
              versionTopicInfo.partitionerClass,
              versionTopicInfo.amplificationFactor,
              new VeniceProperties(partitionerProperties));
      VeniceWriter<KafkaKey, byte[], byte[]> newVeniceWriter =
          veniceWriterFactory.createVeniceWriter(versionTopicInfo.topic, venicePartitioner);
      LOGGER.info("Created VeniceWriter: " + newVeniceWriter.toString());
      veniceWriter = newVeniceWriter;
    }
    return veniceWriter;
  }

  private synchronized Properties getVeniceWriterProperties(VersionTopicInfo versionTopicInfo) {
    if (null == veniceWriterProperties) {
      veniceWriterProperties = createVeniceWriterProperties(versionTopicInfo.kafkaUrl, versionTopicInfo.sslToKafka);
    }
    return veniceWriterProperties;
  }

  private Properties createVeniceWriterProperties(String kafkaUrl, boolean sslToKafka) {
    Properties veniceWriterProperties = new Properties();
    veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, kafkaUrl);
    veniceWriterProperties.put(VeniceWriter.MAX_ELAPSED_TIME_FOR_SEGMENT_IN_MS, -1);
    if (props.containsKey(VeniceWriter.CLOSE_TIMEOUT_MS)){ /* Writer uses default if not specified */
      veniceWriterProperties.put(VeniceWriter.CLOSE_TIMEOUT_MS, props.getInt(VeniceWriter.CLOSE_TIMEOUT_MS));
    }
    if (sslToKafka) {
      Properties sslProps = getSslProperties();
      veniceWriterProperties.putAll(sslProps);
    }
    if (props.containsKey(KAFKA_PRODUCER_REQUEST_TIMEOUT_MS)) {
      // Not a typo, we actually want to set delivery timeout!
      veniceWriterProperties.setProperty(KAFKA_DELIVERY_TIMEOUT_MS, props.getString(KAFKA_PRODUCER_REQUEST_TIMEOUT_MS));
    }
    if (props.containsKey(KAFKA_PRODUCER_RETRIES_CONFIG)) {
      veniceWriterProperties.setProperty(KAFKA_PRODUCER_RETRIES_CONFIG, props.getString(KAFKA_PRODUCER_RETRIES_CONFIG));
    }
    return veniceWriterProperties;
  }

  /**
   * Build ssl properties based on the hadoop token file.
   */
  private synchronized Properties getSslProperties() {
    if (null == sslProperties) {
      sslProperties = new Properties();
      // SSL_ENABLED is needed in SSLFactory
      sslProperties.setProperty(SSL_ENABLED, "true");
      sslProperties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, KAFKA_SECURITY_PROTOCOL);
      props.keySet().stream().filter(key -> key.toLowerCase().startsWith(SSL_PREFIX)).forEach(key -> {
        sslProperties.setProperty(key, props.getString(key));
      });
      try {
        SSLConfigurator sslConfigurator = SSLConfigurator.getSSLConfigurator(
            props.getString(SSL_CONFIGURATOR_CLASS_CONFIG, TempFileSSLConfigurator.class.getName()));
        Properties sslWriterProperties = sslConfigurator.setupSSLConfig(sslProperties,
            UserCredentialsFactory.getUserCredentialsFromTokenFile());
        sslProperties.putAll(sslWriterProperties);
      } catch (IOException e) {
        throw new VeniceException("Could not get user credential for kafka push job for topic" + kafkaTopicInfo.topic);
      }
    }
    return sslProperties;
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
    long pollTime = 0;
    /**
     * The start time when some data centers enter unknown state;
     * if 0, it means no data center is in unknown state.
     *
     * Once enter unknown state, it's allowed to stayed in unknown state for
     * no more than {@link DEFAULT_JOB_STATUS_IN_UNKNOWN_STATE_TIMEOUT_MS}.
     */
    long unknownStateStartTimeMs = 0;

    String topicToMonitor = Version.isRealTimeTopic(versionTopicInfo.topic) ?
            Version.composeKafkaTopic(pushJobSetting.storeName, versionTopicInfo.version) :
            versionTopicInfo.topic;

    List<ExecutionStatus> successfulStatuses = Arrays.asList(ExecutionStatus.COMPLETED, ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED);

    for (;;) {
      long currentTime = System.currentTimeMillis();
      if (pollTime > currentTime) {
        if (!Utils.sleep(pollTime - currentTime)) {
          throw new VeniceException("Job status polling was interrupted!");
        }
      }
      pollTime = currentTime + pushJobSetting.pollJobStatusIntervalMs;

      JobStatusQueryResponse response = ControllerClient.retryableRequest(
          controllerClient,
          pushJobSetting.controllerStatusPollRetries,
          client -> client.queryOverallJobStatus(topicToMonitor, incrementalPushVersion)
      );
      if (response.isError()) {
        // status could not be queried which could be due to a communication error.
        throw new VeniceException("Failed to connect to: " + pushJobSetting.veniceControllerUrl +
            " to query job status, after " + pushJobSetting.controllerStatusPollRetries + " attempts.");
      }

      previousOverallDetails = printJobStatus(response, previousOverallDetails, previousExtraDetails);
      ExecutionStatus overallStatus = ExecutionStatus.valueOf(response.getStatus());
      Map<String, String> datacenterSpecificInfo = response.getExtraInfo();
      // Note that it's intended to update the push job details before updating the completed datacenter set.
      updatePushJobDetailsWithColoStatus(datacenterSpecificInfo, completedDatacenters);
      for (String datacenter : datacenterSpecificInfo.keySet()) {
        ExecutionStatus datacenterStatus = ExecutionStatus.valueOf(datacenterSpecificInfo.get(datacenter));
        if (datacenterStatus.isTerminal() && !datacenterStatus.equals(ExecutionStatus.ERROR)) {
          completedDatacenters.add(datacenter);
        }
      }

      if (overallStatus.isTerminal()) {
        if (completedDatacenters.size() != datacenterSpecificInfo.size() || !successfulStatuses.contains(overallStatus)) {
          // One or more DC could have an UNKNOWN status and never successfully reported a completed status before,
          // but if the majority of datacenters have completed, we give up on the unreachable datacenter
          // and start truncating the data topic.
          throw new VeniceException("Push job error reported by controller: " + pushJobSetting.veniceControllerUrl
              + "\ncontroller response: " + response.toString());
        }

        // Every known datacenter have successfully reported a completed status at least once.
        LOGGER.info("Successfully pushed " + versionTopicInfo.topic);
        return;
      }

      if (overallStatus.equals(ExecutionStatus.UNKNOWN)) {
        if (unknownStateStartTimeMs > pushJobSetting.jobStatusInUnknownStateTimeoutMs) {
          long timeoutMinutes = pushJobSetting.jobStatusInUnknownStateTimeoutMs / Time.MS_PER_MINUTE;
          throw new VeniceException("After waiting for " + timeoutMinutes + " minutes; push job is still in unknown state.");
        }

        if (unknownStateStartTimeMs == 0) {
          unknownStateStartTimeMs = System.currentTimeMillis();
        } else {
          double elapsedMinutes = (double) unknownStateStartTimeMs / Time.MS_PER_SECOND;
          LOGGER.warn("Some data centers are still in unknown state after waiting for " + elapsedMinutes + " minutes.");
        }
      } else {
        unknownStateStartTimeMs = 0;
      }

      // Only send the push job details after all error checks have passed and job is not completed yet.
      sendPushJobDetailsToController();
    }
  }

  private String printJobStatus(JobStatusQueryResponse response, String previousOverallDetails,
      Map<String, String> previousExtraDetails) {
    String newOverallDetails = previousOverallDetails;
    String logMessage = "Specific status: ";
    Map<String, String> datacenterSpecificInfo = response.getExtraInfo();
    if (null != datacenterSpecificInfo && !datacenterSpecificInfo.isEmpty()) {
      logMessage += datacenterSpecificInfo;
    }
    LOGGER.info(logMessage);

    Optional<String> details = response.getOptionalStatusDetails();
    if (details.isPresent() && detailsAreDifferent(previousOverallDetails, details.get())) {
      LOGGER.info("\t\tNew overall details: " + details.get());
      newOverallDetails = details.get();
    }

    Optional<Map<String, String>> extraDetails = response.getOptionalExtraDetails();
    if (extraDetails.isPresent()) {
      // Non-upgraded controllers will not provide these details, in which case, this will be null.
      for (Map.Entry<String, String> entry: extraDetails.get().entrySet()) {
        String cluster = entry.getKey();
        String previous = previousExtraDetails.get(cluster);
        String current = entry.getValue();

        if (detailsAreDifferent(previous, current)) {
          LOGGER.info("\t\tNew specific details for " + cluster + ": " + current);
          previousExtraDetails.put(cluster, current);
        }
      }
    }
    return newOverallDetails;
  }

  /**
   * @return true if the details are different
   */
  private boolean detailsAreDifferent(String previous, String current) {
    // Criteria for printing the current details:
    boolean detailsPresentWhenPreviouslyAbsent = (null == previous && null != current);
    boolean detailsDifferentFromPreviously = (null != previous && !previous.equals(current));
    return detailsPresentWhenPreviouslyAbsent || detailsDifferentFromPreviously;
  }

  protected void setupMRConf(JobConf jobConf, VersionTopicInfo versionTopicInfo, PushJobSetting pushJobSetting, SchemaInfo schemaInfo, StoreSetting storeSetting, VeniceProperties props, String id, String inputDirectory) {
    setupDefaultJobConf(jobConf, versionTopicInfo, pushJobSetting, schemaInfo, storeSetting, props, id, inputDirectory);
    setupInputFormatConf(jobConf, schemaInfo, inputDirectory);
    setupReducerConf(jobConf, pushJobSetting, versionTopicInfo);
  }

  private void setupPBNJConf(JobConf jobConf, VersionTopicInfo versionTopicInfo, PushJobSetting pushJobSetting, SchemaInfo schemaInfo, StoreSetting storeSetting, VeniceProperties props, String id, String inputDirectory) {
    if (!schemaInfo.isAvro) {
      throw new VeniceException("PBNJ only supports Avro input format");
    }

    setupMRConf(jobConf, versionTopicInfo, pushJobSetting, schemaInfo, storeSetting, props, id, inputDirectory);
    jobConf.set(VENICE_STORE_NAME_PROP, pushJobSetting.storeName);
    jobConf.set(PBNJ_ROUTER_URL_PROP, pushJobSetting.veniceRouterUrl);
    jobConf.set(PBNJ_FAIL_FAST, Boolean.toString(pushJobSetting.pbnjFailFast));
    jobConf.set(PBNJ_ASYNC, Boolean.toString(pushJobSetting.pbnjAsync));
    jobConf.set(PBNJ_SAMPLING_RATIO_PROP, Double.toString(pushJobSetting.pbnjSamplingRatio));
    jobConf.set(STORAGE_QUOTA_PROP, Long.toString(Store.UNLIMITED_STORAGE_QUOTA));
    jobConf.setMapperClass(PostBulkLoadAnalysisMapper.class);
  }

  protected void setupDefaultJobConf(JobConf conf, VersionTopicInfo versionTopicInfo, PushJobSetting pushJobSetting, SchemaInfo schemaInfo, StoreSetting storeSetting, VeniceProperties props, String id, String inputDirectory) {
    conf.set(BATCH_NUM_BYTES_PROP, Integer.toString(pushJobSetting.batchNumBytes));
    conf.set(TOPIC_PROP, versionTopicInfo.topic);
    // We need the two configs with bootstrap servers since VeniceWriterFactory requires kafka.bootstrap.servers while
    // the Kafka consumer requires bootstrap.servers.
    conf.set(KAFKA_BOOTSTRAP_SERVERS, versionTopicInfo.kafkaUrl);
    conf.set(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, versionTopicInfo.kafkaUrl);
    conf.set(COMPRESSION_STRATEGY, versionTopicInfo.compressionStrategy.toString());
    conf.set(REDUCER_MINIMUM_LOGGING_INTERVAL_MS, Long.toString(pushJobSetting.minimumReducerLoggingIntervalInMs));
    conf.set(PARTITIONER_CLASS, versionTopicInfo.partitionerClass);
    // flatten partitionerParams since JobConf class does not support set an object
    for (Map.Entry<String, String> entry : versionTopicInfo.partitionerParams.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    conf.setInt(AMPLIFICATION_FACTOR, versionTopicInfo.amplificationFactor);
    if( versionTopicInfo.sslToKafka ){
      conf.set(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, KAFKA_SECURITY_PROTOCOL);
      props.keySet().stream().filter(key -> key.toLowerCase().startsWith(SSL_PREFIX)).forEach(key -> {
        conf.set(key, props.getString(key));
      });
    }
    conf.setBoolean(ALLOW_DUPLICATE_KEY, pushJobSetting.isDuplicateKeyAllowed);
    conf.setBoolean(VeniceWriter.ENABLE_CHUNKING, storeSetting.isChunkingEnabled);

    conf.set(STORAGE_QUOTA_PROP, Long.toString(storeSetting.storeStorageQuota));
    conf.set(STORAGE_ENGINE_OVERHEAD_RATIO, Double.toString(storeSetting.storageEngineOverheadRatio));

    /** Allow overriding properties if their names start with {@link HADOOP_PREFIX}.
     *  Allow overriding properties if their names start with {@link VeniceWriter.VENICE_WRITER_CONFIG_PREFIX}
     *  Allow overriding properties if their names start with {@link ApacheKafkaProducer.PROPERTIES_KAFKA_PREFIX}
     **/
    for (String key : props.keySet()) {
      String lowerCase = key.toLowerCase();
      if (lowerCase.startsWith(HADOOP_PREFIX)) {
        String overrideKey = key.substring(HADOOP_PREFIX.length());
        conf.set(overrideKey, props.getString(key));
      }
      /**
       * Pass Venice Writer related properties down to VeniceWriter instance in
       * {@link VeniceReducer}.
       */
      if (lowerCase.startsWith(VeniceWriter.VENICE_WRITER_CONFIG_PREFIX)) {
        conf.set(key, props.getString(key));
      }
      /**
       * Pass Kafka related properties down to VeniceWriter instance in {@link VeniceReducer}.
       */
      if (lowerCase.startsWith(ApacheKafkaProducer.PROPERTIES_KAFKA_PREFIX)) {
        conf.set(key, props.getString(key));
      }
    }

    if (pushJobSetting.isSourceKafka) {
      // Use some fake value schema id here since it won't be used
      conf.setInt(VALUE_SCHEMA_ID_PROP, -1);
    } else {
      conf.setInt(VALUE_SCHEMA_ID_PROP, schemaInfo.valueSchemaId);
      conf.setInt(DERIVED_SCHEMA_ID_PROP, schemaInfo.derivedSchemaId);
    }
    conf.setBoolean(ENABLE_WRITE_COMPUTE, pushJobSetting.enableWriteCompute);

    if (System.getenv(HADOOP_TOKEN_FILE_LOCATION) != null) {
      conf.set(MAPREDUCE_JOB_CREDENTIALS_BINARY, System.getenv(HADOOP_TOKEN_FILE_LOCATION));
    }

    conf.setJobName(id + ":" + "hadoop_to_venice_bridge" + "-" + versionTopicInfo.topic);
    conf.setJarByClass(this.getClass());

    //do we need these two configs?
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(NullWritable.class);

    // Setting up the Output format
    conf.setOutputFormat(NullOutputFormat.class);

    conf.set(SSL_CONFIGURATOR_CLASS_CONFIG, props.getString(SSL_CONFIGURATOR_CLASS_CONFIG, TempFileSSLConfigurator.class.getName()));
    conf.set(SSL_KEY_STORE_PROPERTY_NAME, props.getString(SSL_KEY_STORE_PROPERTY_NAME));
    conf.set(SSL_TRUST_STORE_PROPERTY_NAME, props.getString(SSL_TRUST_STORE_PROPERTY_NAME));
    conf.set(SSL_KEY_PASSWORD_PROPERTY_NAME, props.getString(SSL_KEY_PASSWORD_PROPERTY_NAME));

    if (props.containsKey(KAFKA_PRODUCER_REQUEST_TIMEOUT_MS)) {
      conf.set(KAFKA_PRODUCER_REQUEST_TIMEOUT_MS, props.getString(KAFKA_PRODUCER_REQUEST_TIMEOUT_MS));
    }
    if (props.containsKey(KAFKA_PRODUCER_RETRIES_CONFIG)) {
      conf.set(KAFKA_PRODUCER_RETRIES_CONFIG, props.getString(KAFKA_PRODUCER_RETRIES_CONFIG));
    }

    conf.set(TELEMETRY_MESSAGE_INTERVAL, props.getString(TELEMETRY_MESSAGE_INTERVAL, "10000"));
    conf.set(KAFKA_METRICS_TO_REPORT_AS_MR_COUNTERS, props.getString(KAFKA_METRICS_TO_REPORT_AS_MR_COUNTERS,
        "record-queue-time-avg,request-latency-avg,record-send-rate,byte-rate"));

    conf.set(ZSTD_COMPRESSION_LEVEL, props.getString(ZSTD_COMPRESSION_LEVEL, String.valueOf(Zstd.maxCompressionLevel())));
    conf.set(ETL_VALUE_SCHEMA_TRANSFORMATION, pushJobSetting.etlValueSchemaTransformation.name());
  }

  protected void setupInputFormatConf(JobConf jobConf, SchemaInfo schemaInfo, String inputDirectory) {
    // Hadoop2 dev cluster provides a newer version of an avro dependency.
    // Set mapreduce.job.classloader to true to force the use of the older avro dependency.
    jobConf.setBoolean(MAPREDUCE_JOB_CLASSLOADER, true);
    LOGGER.info("**************** " + MAPREDUCE_JOB_CLASSLOADER + ": " + jobConf.get(MAPREDUCE_JOB_CLASSLOADER));

    if (pushJobSetting.isSourceKafka) {
        jobConf.setInputFormat(KafkaInputFormat.class);
        jobConf.setMapperClass(VeniceKafkaInputMapper.class);
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

      jobConf.set(KEY_FIELD_PROP, schemaInfo.keyField);
      jobConf.set(VALUE_FIELD_PROP, schemaInfo.valueField);

      if (schemaInfo.isAvro) {
        jobConf.set(SCHEMA_STRING_PROP, schemaInfo.fileSchemaString);
        jobConf.set(AvroJob.INPUT_SCHEMA, schemaInfo.fileSchemaString);
        jobConf.setClass("avro.serialization.data.model", GenericData.class, GenericData.class);
        jobConf.setInputFormat(AvroInputFormat.class);
        jobConf.setMapperClass(VeniceAvroMapper.class);
        jobConf.setBoolean(VSON_PUSH, false);
      } else {
        jobConf.setInputFormat(VsonSequenceFileInputFormat.class);
        jobConf.setMapperClass(VeniceVsonMapper.class);
        jobConf.setBoolean(VSON_PUSH, true);
        jobConf.set(FILE_KEY_SCHEMA, schemaInfo.vsonFileKeySchema);
        jobConf.set(FILE_VALUE_SCHEMA, schemaInfo.vsonFileValueSchema);
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

  private void logPushJobProperties(
      VersionTopicInfo versionTopicInfo,
      PushJobSetting pushJobSetting,
      SchemaInfo schemaInfo,
      String clusterName,
      String inputDirectory,
      long inputFileDataSize
  ) {
    LOGGER.info(pushJobPropertiesToString(versionTopicInfo, pushJobSetting, schemaInfo, clusterName, inputDirectory, inputFileDataSize));
  }

  private String pushJobPropertiesToString(
      VersionTopicInfo versionTopicInfo,
      PushJobSetting pushJobSetting,
      SchemaInfo schemaInfo,
      String clusterName,
      String inputDirectory,
      final long inputFileDataSize
  ) {
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
    if (schemaInfo != null) {
      propKeyValuePairs.add("File Schema: " + schemaInfo.fileSchemaString);
      propKeyValuePairs.add("Avro key schema: " + schemaInfo.keySchemaString);
      propKeyValuePairs.add("Avro value schema: " + schemaInfo.valueSchemaString);
    }
    propKeyValuePairs.add("Total input data file size: " + ((double) inputFileDataSize / 1024 / 1024)
        + " MB, estimated with a factor of " + INPUT_DATA_SIZE_FACTOR);
    propKeyValuePairs.add("Is incremental push: " + pushJobSetting.isIncrementalPush);
    propKeyValuePairs.add("Is duplicated key allowed: " + pushJobSetting.isDuplicateKeyAllowed);
    propKeyValuePairs.add("Is source ETL data: " + pushJobSetting.isSourceETL);
    propKeyValuePairs.add("ETL value schema transformation : " + pushJobSetting.etlValueSchemaTransformation);
    propKeyValuePairs.add("Is Kafka Input: " + pushJobSetting.isSourceKafka);
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
    if (kafkaTopicInfo != null && Utils.isNullOrEmpty(kafkaTopicInfo.topic)) {
      pushJobDetails.overallStatus.add(getPushJobDetailsStatusTuple(PushJobDetailsStatus.ERROR.getValue()));
    } else {
      pushJobDetails.overallStatus.add(getPushJobDetailsStatusTuple(PushJobDetailsStatus.KILLED.getValue()));
    }
    pushJobDetails.jobDurationInMs = System.currentTimeMillis() - jobStartTimeMs;
    updatePushJobDetailsWithConfigs();
    sendPushJobDetailsToController();
  }

  private void killJobAndCleanup(PushJobSetting pushJobSetting, ControllerClient controllerClient, VersionTopicInfo versionTopicInfo) {
    // Attempting to kill job. There's a race condition, but meh. Better kill when you know it's running
    killJob();
    if (!pushJobSetting.isIncrementalPush && versionTopicInfo != null) {
      final int maxRetryAttempt = 10;
      int currentRetryAttempt = 0;
      while (currentRetryAttempt < maxRetryAttempt) {
        if (!Utils.isNullOrEmpty(versionTopicInfo.topic)) {
          break;
        }
        Utils.sleep(Duration.ofMillis(10).toMillis());
        currentRetryAttempt++;
      }
      if (Utils.isNullOrEmpty(versionTopicInfo.topic)) {
        LOGGER.error("Could not find a store version to delete for store: " + pushJobSetting.storeName);
      } else {
        ControllerClient.retryableRequest(
            controllerClient,
            pushJobSetting.controllerRetries,
            c -> c.killOfflinePushJob(versionTopicInfo.topic)
        );
        LOGGER.info("Offline push job has been killed, topic: " + versionTopicInfo.topic);
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
        LOGGER.warn(String.format("No op to kill a completed job with name %s and ID %d",
            runningJob.getJobName(), runningJob.getID().getId()));
        return;
      }
      runningJob.killJob();
    } catch (Exception ex) {
      // Will try to kill Venice Offline Push Job no matter whether map-reduce job kill throws exception or not.
      LOGGER.info(String.format("Received exception while killing map-reduce job with name %s and ID %d",
          runningJob.getJobName(), runningJob.getID().getId()), ex);
    }
  }

  /**
   * Define as "protected" function instead of "private" because it's needed in some test cases.
   */
  protected static Path getLatestPathOfInputDirectory(String inputDirectory, FileSystem fs) throws IOException{
    Path sourcePath;
    boolean computeLatestPath = false;
    if (inputDirectory.endsWith("#LATEST") || inputDirectory.endsWith("#LATEST/")) {
      int poundSign = inputDirectory.lastIndexOf('#');
      sourcePath = new Path(inputDirectory.substring(0, poundSign));
      computeLatestPath = true;
    } else {
      sourcePath = new Path(inputDirectory);
    }

    if(computeLatestPath) {
      sourcePath = getLatestPath(sourcePath, fs);
    }

    return sourcePath;
  }

  private String discoverCluster(String storeName, String veniceControllerUrl, Optional<SSLFactory> sslFactory) {
    LOGGER.info("Discover cluster for store:" + storeName);
    // TODO: Evaluate what's the proper way to add retries here...
    ControllerResponse clusterDiscoveryResponse;
    if (clusterDiscoveryControllerClient == null) {
      clusterDiscoveryResponse = ControllerClient.discoverCluster(veniceControllerUrl, storeName, sslFactory);
    } else {
      clusterDiscoveryResponse = clusterDiscoveryControllerClient.discoverCluster(storeName);
    }
    if (clusterDiscoveryResponse.isError()) {
      throw new VeniceException("Get error in clusterDiscoveryResponse:" + clusterDiscoveryResponse.getError());
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

  public String getFileSchemaString() {
    return schemaInfo.fileSchemaString;
  }

  public String getKeySchemaString() {
    return schemaInfo.keySchemaString;
  }

  public String getValueSchemaString() {
    return schemaInfo.valueSchemaString;
  }

  public long getInputFileDataSize() {
    return inputFileDataSize;
  }

  public Optional<String> getIncrementalPushVersion() {
    return pushJobSetting.incrementalPushVersion;
  }

  private static Path getLatestPath(Path path, FileSystem fs) throws IOException {
    FileStatus[] statuses = fs.listStatus(path, PATH_FILTER);

    if (statuses.length == 0) {
      return path;
    } else {
      Arrays.sort(statuses);
      return statuses[statuses.length - 1].getPath();
    }
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
    IOUtils.closeQuietly(controllerClient);
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
}
