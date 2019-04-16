package com.linkedin.venice.hadoop;

import azkaban.jobExecutor.AbstractJob;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StorageEngineOverheadRatioResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.controllerapi.routes.PushJobStatusUploadResponse;
import com.linkedin.venice.hadoop.pbnj.PostBulkLoadAnalysisMapper;
import com.linkedin.venice.hadoop.ssl.SSLConfigurator;
import com.linkedin.venice.hadoop.ssl.TempFileSSLConfigurator;
import com.linkedin.venice.hadoop.ssl.UserCredentialsFactory;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.schema.vson.VsonSchema;
import com.linkedin.venice.status.protocol.enums.PushJobStatus;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.writer.ApacheKafkaProducer;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.hadoop.exceptions.VeniceInconsistentSchemaException;
import com.linkedin.venice.hadoop.exceptions.VeniceSchemaFieldNotFoundException;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.text.DecimalFormat;
import java.util.Arrays;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.LinkedinAvroMigrationHelper;
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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.linkedin.venice.ConfigKeys.*;
import static org.apache.hadoop.mapreduce.MRJobConfig.MAPREDUCE_JOB_CLASSLOADER;
import static org.apache.hadoop.mapreduce.MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY;
import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

/**
 * This class sets up the Hadoop job used to push data to Venice.
 * The job reads the input data off HDFS. It supports 2 kinds of
 * input -- Avro / Binary Json (Vson).
 */
public class KafkaPushJob extends AbstractJob implements AutoCloseable, Cloneable {
  //Avro input configs
  public static final String LEGACY_AVRO_KEY_FIELD_PROP = "avro.key.field";
  public static final String LEGACY_AVRO_VALUE_FIELD_PROP = "avro.value.field";

  public static final String KEY_FIELD_PROP = "key.field";
  public static final String VALUE_FIELD_PROP = "value.field";
  public static final String SCHEMA_STRING_PROP = "schema";

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

  /**
   * In single-colo mode, this can be either a controller or router.
   * In multi-colo mode, it must be a parent controller.
   */
  public static final String VENICE_URL_PROP = "venice.urls";

  public static final String ENABLE_PUSH = "venice.push.enable";
  public static final String VENICE_CLUSTER_NAME_PROP = "cluster.name";
  public static final String VENICE_STORE_NAME_PROP = "venice.store.name";
  public static final String INPUT_PATH_PROP = "input.path";
  public static final String BATCH_NUM_BYTES_PROP = "batch.num.bytes";

  // Map-only job or a Map-Reduce job for data push, by default it is a map-reduce job.
  public static final String VENICE_MAP_ONLY = "venice.map.only";

  public static final String VALUE_SCHEMA_ID_PROP = "value.schema.id";
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

  public static final String AZK_JOB_EXEC_URL = "azkaban.link.attempt.url";

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

  private static Logger logger = Logger.getLogger(KafkaPushJob.class);

  public static int DEFAULT_BATCH_BYTES_SIZE = 1000000;

  /**
   * Since the job is calculating the raw data file size, which is not accurate because of compression,
   * key/value schema and backend storage overhead, we are applying this factor to provide a more
   * reasonable estimation.
   */
  /**
   * TODO: for map-reduce job, we could come up with more accurate estimation.
   */
  public static final long INPUT_DATA_SIZE_FACTOR = 2;

  private static final DecimalFormat PERCENT_FORMAT = new DecimalFormat("##.#%");

  /**
   * Placeholder for version number that is yet to be created.
   */
  private static final int UNCREATED_VERSION_NUMBER = -1;

  private String inputDirectory;
  // Immutable state
  private final VeniceProperties props;
  private final String id;

  private ControllerClient controllerClient;

  private String clusterName;

  private RunningJob runningJob;

  // Job config for regular push job
  protected JobConf jobConf = new JobConf();
  // Job config for pbnj
  protected JobConf pbnjJobConf = new JobConf();

  // Total input data size, which is used to talk to controller to decide whether we have enough quota or not
  private long inputFileDataSize;
  private long pushStartTime;
  private String pushId;

  private VeniceWriter<KafkaKey, byte[]> veniceWriter; // Lazily initialized

  protected class SchemaInfo {
    boolean isAvro = true;
    int valueSchemaId; // Value schema id retrieved from backend for valueSchemaString
    String keyField;
    String valueField;
    String fileSchemaString;
    String keySchemaString;
    String valueSchemaString;
    String vsonFileKeySchema;
    String vsonFileValueSchema;
  }
  private SchemaInfo schemaInfo;

  protected class PushJobSetting {
    boolean enablePush;
    String veniceControllerUrl;
    String veniceRouterUrl;
    String storeName;
    int batchNumBytes;
    // TODO: Map only code should be removed; it's incompatible with sorted keys
    boolean isMapOnly;
    boolean enablePBNJ;
    boolean pbnjFailFast;
    boolean pbnjAsync;
    double pbnjSamplingRatio;
    boolean isIncrementalPush;
    boolean isDuplicateKeyAllowed;
    boolean enablePushJobStatusUpload;
    boolean enableReducerSpeculativeExecution;
    long minimumReducerLoggingIntervalInMs;
    int controllerRetries;
    int controllerStatusPollRetries;
  }
  private PushJobSetting pushJobSetting;

  protected class VersionTopicInfo {
    // Kafka topic for new data push
    String topic;
    // Kafka topic partition count
    int partitionCount;
    // Kafka url will get from Venice backend for store push
    String kafkaUrl;
    boolean sslToKafka;
    CompressionStrategy compressionStrategy;
  }
  private VersionTopicInfo versionTopicInfo;

  protected class StoreSetting {
    boolean isChunkingEnabled;
    long storeStorageQuota;
    double storageEngineOverheadRatio;
  }

  /**
   * Do not change this method argument type
   * Constructor used by Azkaban for creating the job.
   * http://azkaban.github.io/azkaban/docs/latest/#hadoopjava-type
   * @param jobId  id of the job
   * @param vanillaProps  Property bag for the job
   * @throws Exception
   */
  public KafkaPushJob(String jobId, Properties vanillaProps) {
    super(jobId, logger);
    this.id = jobId;

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
    for(String sslPropertyName:requiredSSLPropertiesNames){
      if(!vanillaProps.containsKey(sslPropertyName)){
        throw new VeniceException("Miss the require ssl property name: "+sslPropertyName);
      }
    }

    this.props = new VeniceProperties(vanillaProps);
    logger.info("Constructing " + KafkaPushJob.class.getSimpleName() + ": " + props.toString(true));

    // Optional configs:
    pushJobSetting = new PushJobSetting();
    pushJobSetting.enablePush = props.getBoolean(ENABLE_PUSH, true);
    pushJobSetting.batchNumBytes = props.getInt(BATCH_NUM_BYTES_PROP, DEFAULT_BATCH_BYTES_SIZE);
    pushJobSetting.isMapOnly = props.getBoolean(VENICE_MAP_ONLY, false);
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
    pushJobSetting.controllerStatusPollRetries = props.getInt(POLL_STATUS_RETRY_ATTEMPTS, 3);

    if (pushJobSetting.enablePBNJ) {
      // If PBNJ is enabled, then the router URL config is mandatory
      pushJobSetting.veniceRouterUrl = props.getString(PBNJ_ROUTER_URL_PROP);
    } else {
      pushJobSetting.veniceRouterUrl = null;
    }

    // Mandatory configs:
    pushJobSetting.veniceControllerUrl = props.getString(VENICE_URL_PROP);
    pushJobSetting.storeName = props.getString(VENICE_STORE_NAME_PROP);


    if (!pushJobSetting.enablePush && !pushJobSetting.enablePBNJ) {
      throw new VeniceException("At least one of the following config properties must be true: " + ENABLE_PUSH + " or " + PBNJ_ENABLE);
    }

  }

  /**
   * Do not change this method argument type.
   * Used by Azkaban
   *
   * http://azkaban.github.io/azkaban/docs/latest/#hadoopjava-type
   *
   * The run method is invoked by Azkaban dynamically for running
   * the job.
   *
   * @throws VeniceException
   */
  @Override
  public void run() {
    try {
      logGreeting();

      this.inputDirectory = getInputURI(this.props);

      // Check data size
      // TODO: do we actually need this information?
      this.inputFileDataSize = calculateInputDataSize(this.inputDirectory);
      // Get input schema
      this.schemaInfo = getInputSchema(this.inputDirectory, this.props);

      // Discover the cluster based on the store name and re-initialized controller client.
      this.clusterName = discoverCluster(pushJobSetting);
      this.controllerClient = new ControllerClient(clusterName, pushJobSetting.veniceControllerUrl);
      validateKeySchema(controllerClient, pushJobSetting, schemaInfo);
      validateValueSchema(controllerClient, pushJobSetting, schemaInfo);
      StoreSetting storeSetting = getSettingsFromController(controllerClient, pushJobSetting);

      JobClient jc;

      if (pushJobSetting.enablePush) {
        this.pushStartTime = System.currentTimeMillis();
        this.pushId = pushStartTime + "_" + props.getString(AZK_JOB_EXEC_URL, "failed_to_obtain_azkaban_url");
        // Create new store version, topic and fetch Kafka url from backend
        versionTopicInfo = createNewStoreVersion(pushJobSetting, inputFileDataSize, controllerClient, pushId, props);
        // Log Venice data push job related info
        logPushJobProperties(versionTopicInfo, pushJobSetting, schemaInfo, this.clusterName, this.inputDirectory, this.inputFileDataSize);

        // Setup the hadoop job
        setupMRConf(jobConf, versionTopicInfo, pushJobSetting, schemaInfo, storeSetting, props, id, inputDirectory);
        // Whether the messages in one single topic partition is lexicographically sorted by key bytes.
        // If reducer phase is enabled, each reducer will sort all the messages inside one single
        // topic partition.
        jc = new JobClient(jobConf);
        Optional<String> incrementalPushVersion = Optional.empty();
        if (pushJobSetting.isIncrementalPush) {
          incrementalPushVersion = Optional.of(String.valueOf(System.currentTimeMillis()));
          getVeniceWriter(versionTopicInfo).broadcastStartOfIncrementalPush(incrementalPushVersion.get(), new HashMap<>());
          runningJob = jc.runJob(jobConf);
          getVeniceWriter(versionTopicInfo).broadcastEndOfIncrementalPush(incrementalPushVersion.get(), new HashMap<>());
        } else {
          getVeniceWriter(versionTopicInfo).broadcastStartOfPush(!pushJobSetting.isMapOnly, storeSetting.isChunkingEnabled, versionTopicInfo.compressionStrategy, new HashMap<>());
          // submit the job for execution and wait for completion
          runningJob = jc.runJob(jobConf);
          //TODO: send a failure END OF PUSH message if something went wrong
          getVeniceWriter(versionTopicInfo).broadcastEndOfPush(new HashMap<>());
        }
        // Close VeniceWriter before polling job status since polling job status could
        // trigger job deletion
        closeVeniceWriter();

        // Waiting for Venice Backend to complete consumption
        pollStatusUntilComplete(incrementalPushVersion, controllerClient, pushJobSetting, versionTopicInfo);
        checkAndUploadPushJobStatus(PushJobStatus.SUCCESS, "", pushJobSetting, versionTopicInfo, pushStartTime, pushId);
      } else {
        logger.info("Skipping push job, since " + ENABLE_PUSH + " is set to false.");
      }

      if (pushJobSetting.enablePBNJ) {
        logger.info("Post-Bulkload Analysis Job is about to run.");
        setupPBNJConf(pbnjJobConf, versionTopicInfo, pushJobSetting, schemaInfo, storeSetting, props, id, inputDirectory);
        jc = new JobClient(pbnjJobConf);
        runningJob = jc.runJob(pbnjJobConf);
      }
    } catch (Exception e) {
      logger.error("Failed to run job.", e);
      checkAndUploadPushJobStatus(PushJobStatus.ERROR, e.getMessage(), pushJobSetting, versionTopicInfo, pushStartTime, pushId);
      closeVeniceWriter();
      try {
        stopAndCleanup(pushJobSetting, controllerClient, versionTopicInfo);
      } catch (Exception ex) {
        logger.info("Failed to stop and cleanup the job", ex);
      }
      if (! (e instanceof VeniceException)) {
        e = new VeniceException("Exception caught during Hadoop to Venice Bridge!", e);
      }
      throw (VeniceException) e;
    }
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
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    String uri = props.getString(INPUT_PATH_PROP);
    Path sourcePath = getLatestPathOfInputDirectory(uri, fs);
    return sourcePath.toString();
  }

  /**
   * Calculate total data size of input directory
   * @param inputUri
   * @return total input data files size
   * @throws Exception
   */
  protected long calculateInputDataSize(String inputUri) throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path srcPath = new Path(inputUri);
    FileStatus[] fileStatuses = fs.listStatus(srcPath, PATH_FILTER);

    if (fileStatuses == null || fileStatuses.length == 0) {
      throw new RuntimeException("No data found at source path: " + srcPath);
    }

    // Since the job is calculating the raw data file size, which is not accurate because of compression, key/value schema and backend storage overhead,
    // we are applying this factor to provide a more reasonable estimation.
    return validateInputDir(fileStatuses) * INPUT_DATA_SIZE_FACTOR;
  }

  /**
   * 1. Check whether it's Vson input or Avro input
   * 2. Check schema consistency;
   * 3. Populate key schema, value schema;
   * @param inputUri
   * @param props push job properties
   * @return all schema related information
   * @throws Exception
   */
  protected SchemaInfo getInputSchema(String inputUri, VeniceProperties props) throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path srcPath = new Path(inputUri);
    FileStatus[] fileStatuses = fs.listStatus(srcPath, PATH_FILTER);

    SchemaInfo schemaInfo = new SchemaInfo();

    //try reading the file via sequence file reader. It indicates Vson input if it is succeeded.
    Map<String, String> fileMetadata = getMetadataFromSequenceFile(fs, fileStatuses[0].getPath());
    if (fileMetadata.containsKey(FILE_KEY_SCHEMA) && fileMetadata.containsKey(FILE_VALUE_SCHEMA)) {
      schemaInfo.isAvro = false;
      schemaInfo.vsonFileKeySchema = fileMetadata.get(FILE_KEY_SCHEMA);
      schemaInfo.vsonFileValueSchema = fileMetadata.get(FILE_VALUE_SCHEMA);
    }

    if (schemaInfo.isAvro) {
      logger.info("Detected Avro input format.");
      schemaInfo.keyField = props.getString(KEY_FIELD_PROP);
      schemaInfo.valueField = props.getString(VALUE_FIELD_PROP);

      Schema avroSchema = checkAvroSchemaConsistency(fs, fileStatuses);

      schemaInfo.fileSchemaString = avroSchema.toString();
      schemaInfo.keySchemaString = extractAvroSubSchema(avroSchema, schemaInfo.keyField).toString();
      schemaInfo.valueSchemaString = extractAvroSubSchema(avroSchema, schemaInfo.valueField).toString();
    } else {
      logger.info("Detected Vson input format, will convert to Avro automatically.");
      //key / value fields are optional for Vson input
      schemaInfo.keyField = props.getString(KEY_FIELD_PROP, "");
      schemaInfo.valueField = props.getString(VALUE_FIELD_PROP, "");

      Pair<VsonSchema, VsonSchema> vsonSchemaPair = checkVsonSchemaConsistency(fs, fileStatuses);

      VsonSchema vsonKeySchema = Utils.isNullOrEmpty(schemaInfo.keyField) ?
          vsonSchemaPair.getFirst() : vsonSchemaPair.getFirst().recordSubtype(schemaInfo.keyField);
      VsonSchema vsonValueSchema = Utils.isNullOrEmpty(schemaInfo.valueField) ?
          vsonSchemaPair.getSecond() : vsonSchemaPair.getSecond().recordSubtype(schemaInfo.valueField);

      schemaInfo.keySchemaString = VsonAvroSchemaAdapter.parse(vsonKeySchema.toString()).toString();
      schemaInfo.valueSchemaString = VsonAvroSchemaAdapter.parse(vsonValueSchema.toString()).toString();
    }
    return schemaInfo;
  }

  /**
   * Check push job status related fields and provide appropriate values if yet to be set due to error situations.
   * Upload these fields to the controller using the {@link ControllerClient}.
   * @param status the status of the push job.
   * @param message the corresponding message to the push job status.
   */
  private void checkAndUploadPushJobStatus(PushJobStatus status, String message, PushJobSetting pushJobSetting, VersionTopicInfo versionTopicInfo, long pushStartTime, String pushId) {
    if (pushJobSetting.enablePushJobStatusUpload) {
      int version = versionTopicInfo.topic == null ? UNCREATED_VERSION_NUMBER : Version.parseVersionFromKafkaTopicName(versionTopicInfo.topic);
      long duration = pushStartTime == 0 ? 0 : System.currentTimeMillis() - pushStartTime;
      String verifiedPushId = pushId == null ? "" : pushId;
      try {
        PushJobStatusUploadResponse response = controllerClient.retryableRequest(pushJobSetting.controllerRetries, c ->
            c.uploadPushJobStatus(pushJobSetting.storeName, version, status, duration, verifiedPushId, message));
        if (response.isError()) {
          logger.warn("Failed to upload push job status with error: " + response.getError());
        }
      } catch (Exception e) {
        logger.warn("Exception thrown while uploading push job status", e);
      }
    }
  }

  private void logGreeting() {
    logger.info("Running Hadoop to Venice Bridge: " + id + "\n" +
        "  _    _           _                   \n" +
        " | |  | |         | |                  \n" +
        " | |__| | __ _  __| | ___   ___  _ __  \n" +
        " |  __  |/ _` |/ _` |/ _ \\ / _ \\| '_ \\ \n" +
        " | |  | | (_| | (_| | (_) | (_) | |_) |\n" +
        " |_|  |_|\\__,_|\\__,_|\\___/ \\___/| .__/ \n" +
        "                _______         | |    \n" +
        "               |__   __|        |_|    \n" +
        "                  | | ___                 \n" +
        "                  | |/ _ \\                \n" +
        "     __      __   | | (_) |               \n" +
        "     \\ \\    / /   |_|\\___/              \n" +
        "      \\ \\  / /__ _ __  _  ___ ___      \n" +
        "       \\ \\/ / _ | '_ \\| |/ __/ _ \\     \n" +
        "        \\  |  __| | | | | (_|  __/     \n" +
        "         \\/ \\___|_| |_|_|\\___\\___|\n" +
        "      ___        _     _\n" +
        "     |  _ \\     (_)   | |              \n" +
        "     | |_) |_ __ _  __| | __ _  ___    \n" +
        "     |  _ <| '__| |/ _` |/ _` |/ _ \\   \n" +
        "     | |_) | |  | | (_| | (_| |  __/   \n" +
        "     |____/|_|  |_|\\__,_|\\__, |\\___|   \n" +
        "                          __/ |        \n" +
        "                         |___/         \n");
  }

  /**
   * This method will talk to parent controller to validate key schema.
   */
  private void validateKeySchema(ControllerClient controllerClient, PushJobSetting setting, SchemaInfo schemaInfo) {
    SchemaResponse keySchemaResponse = controllerClient.retryableRequest(setting.controllerRetries, c ->
        c.getKeySchema(setting.storeName));
    if (keySchemaResponse.isError()) {
      throw new VeniceException("Got an error in keySchemaResponse: " + keySchemaResponse.toString());
    } else if (null == keySchemaResponse.getSchemaStr()) {
      // TODO: Fix the server-side request handling. This should not happen. We should get a 404 instead.
      throw new VeniceException("Got a null schema in keySchemaResponse: " + keySchemaResponse.toString());
    }
    Schema serverSchema = Schema.parse(keySchemaResponse.getSchemaStr());
    Schema clientSchema = Schema.parse(schemaInfo.keySchemaString);
    String canonicalizedServerSchema = LinkedinAvroMigrationHelper.toParsingForm(serverSchema);
    String canonicalizedClientSchema = LinkedinAvroMigrationHelper.toParsingForm(clientSchema);
    if (!canonicalizedServerSchema.equals(canonicalizedClientSchema)) {
      String briefErrorMessage = "Key schema mis-match for store " + setting.storeName;
      logger.error(briefErrorMessage +
          "\n\t\tController URLs: " + controllerClient.getUrlsToFindMasterController() +
          "\n\t\tschema defined in HDFS: \t" + schemaInfo.keySchemaString +
          "\n\t\tschema defined in Venice: \t" + keySchemaResponse.getSchemaStr());
      throw new VeniceException(briefErrorMessage);
    }
  }

  /***
   * This method will talk to controller to validate value schema.
   */
  private void validateValueSchema(ControllerClient controllerClient, PushJobSetting setting, SchemaInfo schemaInfo) {
    SchemaResponse valueSchemaResponse = controllerClient.retryableRequest(setting.controllerRetries, c ->
        c.getValueSchemaID(setting.storeName, schemaInfo.valueSchemaString));
    if (valueSchemaResponse.isError()) {
      throw new VeniceException("Failed to validate value schema for store: " + setting.storeName
          + "\nError from the server: " + valueSchemaResponse.getError()
          + "\nSchema for the data file: " + schemaInfo.valueSchemaString
      );
    }
    schemaInfo.valueSchemaId = valueSchemaResponse.getId();
    logger.info("Got schema id: " + schemaInfo.valueSchemaId + " for value schema: " + schemaInfo.valueSchemaString + " of store: " + setting.storeName);
  }

  private StoreSetting getSettingsFromController(ControllerClient controllerClient, PushJobSetting setting) {
    StoreSetting storeSetting = new StoreSetting();
    StoreResponse storeResponse = controllerClient.retryableRequest(setting.controllerRetries, c -> c.getStore(setting.storeName));
    if (storeResponse.isError()) {
      throw new VeniceException("Can't get store info. " + storeResponse.getError());
    }
    storeSetting.storeStorageQuota = storeResponse.getStore().getStorageQuotaInByte();
    if (storeSetting.storeStorageQuota != Store.UNLIMITED_STORAGE_QUOTA && setting.isMapOnly) {
      throw new VeniceException("Can't run this mapper only job since storage quota is not unlimited. " + "Store: " + setting.storeName);
    }

    StorageEngineOverheadRatioResponse storageEngineOverheadRatioResponse =
        controllerClient.retryableRequest(setting.controllerRetries, c -> c.getStorageEngineOverheadRatio(setting.storeName));
    if (storageEngineOverheadRatioResponse.isError()) {
      throw new VeniceException("Can't get storage engine overhead ratio. " +
      storageEngineOverheadRatioResponse.getError());
    }

    storeSetting.storageEngineOverheadRatio = storageEngineOverheadRatioResponse.getStorageEngineOverheadRatio();

    storeSetting.isChunkingEnabled = storeResponse.getStore().isChunkingEnabled();
    return storeSetting;
  }

  /**
   * This method will talk to parent controller to create new store version, which will create new topic for the version as well.
   */
  private VersionTopicInfo createNewStoreVersion(PushJobSetting setting, long inputFileDataSize, ControllerClient controllerClient, String pushId, VeniceProperties props) {
    VersionTopicInfo versionTopicInfo = new VersionTopicInfo();
    ControllerApiConstants.PushType pushType = setting.isIncrementalPush ?
        ControllerApiConstants.PushType.INCREMENTAL : ControllerApiConstants.PushType.BATCH;
    VersionCreationResponse versionCreationResponse =
        controllerClient.retryableRequest(setting.controllerRetries, c ->
            c.requestTopicForWrites(setting.storeName, inputFileDataSize, pushType, pushId, false));
    if (versionCreationResponse.isError()) {
      throw new VeniceException("Failed to create new store version with urls: " + setting.veniceControllerUrl
          + ", error: " + versionCreationResponse.getError());
    } else if (versionCreationResponse.getVersion() == 0) {
      // TODO: Fix the server-side request handling. This should not happen. We should get a 404 instead.
      throw new VeniceException("Got version 0 from: " + versionCreationResponse.toString());
    } else {
      logger.info(versionCreationResponse.toString());
    }
    versionTopicInfo.topic = versionCreationResponse.getKafkaTopic();
    versionTopicInfo.kafkaUrl = versionCreationResponse.getKafkaBootstrapServers();
    versionTopicInfo.partitionCount = versionCreationResponse.getPartitions();
    versionTopicInfo.sslToKafka = versionCreationResponse.isEnableSSL();
    versionTopicInfo.compressionStrategy = versionCreationResponse.getCompressionStrategy();
    // Upload the properties to controller, as it's not in the critical path, so if it's failed, just log the error
    // but do not thrown the exception. No retries for this.
    ControllerResponse response =
        controllerClient.uploadPushProperties(setting.storeName, versionCreationResponse.getVersion(), props.toProperties());
    if (response.isError()) {
      logger.warn("Could not upload properties of this job to the controlelr. Error: " + response.getError());
    }

    return versionTopicInfo;
  }

  private synchronized VeniceWriter<KafkaKey, byte[]> getVeniceWriter(VersionTopicInfo versionTopicInfo) {
    if (null == this.veniceWriter) {
      // Initialize VeniceWriter
      Properties veniceWriterProperties = new Properties();
      veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, versionTopicInfo.kafkaUrl);
      if (props.containsKey(VeniceWriter.CLOSE_TIMEOUT_MS)){ /* Writer uses default if not specified */
        veniceWriterProperties.put(VeniceWriter.CLOSE_TIMEOUT_MS, props.getInt(VeniceWriter.CLOSE_TIMEOUT_MS));
      }
      if (versionTopicInfo.sslToKafka) {
        veniceWriterProperties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, KAFKA_SECURITY_PROTOCOL);
        props.keySet().stream().filter(key -> key.toLowerCase().startsWith(SSL_PREFIX)).forEach(key -> {
          veniceWriterProperties.setProperty(key, props.getString(key));
        });
        try {
          SSLConfigurator sslConfigurator = SSLConfigurator.getSSLConfigurator(
              props.getString(SSL_CONFIGURATOR_CLASS_CONFIG, TempFileSSLConfigurator.class.getName()));
          Properties sslWriterProperties = sslConfigurator.setupSSLConfig(veniceWriterProperties,
              UserCredentialsFactory.getUserCredentialsFromTokenFile());
          veniceWriterProperties.putAll(sslWriterProperties);
          // Get the certs from Azkaban executor's file system.
        } catch (IOException e) {
          throw new VeniceException("Could not get user credential for kafka push job for topic" + versionTopicInfo.topic);
        }
      }
      if (props.containsKey(KAFKA_PRODUCER_REQUEST_TIMEOUT_MS)) {
        veniceWriterProperties.setProperty(KAFKA_PRODUCER_REQUEST_TIMEOUT_MS, props.getString(KAFKA_PRODUCER_REQUEST_TIMEOUT_MS));
      }
      if (props.containsKey(KAFKA_PRODUCER_RETRIES_CONFIG)) {
        veniceWriterProperties.setProperty(KAFKA_PRODUCER_RETRIES_CONFIG, props.getString(KAFKA_PRODUCER_RETRIES_CONFIG));
      }
      VeniceWriterFactory veniceWriterFactory = new VeniceWriterFactory(veniceWriterProperties);

      VeniceWriter<KafkaKey, byte[]> newVeniceWriter = veniceWriterFactory.getVeniceWriter(versionTopicInfo.topic);
      logger.info("Created VeniceWriter: " + newVeniceWriter.toString());
      this.veniceWriter = newVeniceWriter;
    }
    return this.veniceWriter;
  }

  private synchronized void closeVeniceWriter() {
    if (null != this.veniceWriter) {
      this.veniceWriter.close();
      this.veniceWriter = null;
    }
  }

  /**
   * TODO: This function needs to be refactored. Its flow is way too convoluted ): - FGV
   *
   * High level, we want to poll the consumption job status until it errors or is complete.  This is more complicated
   * because we might be dealing with multiple destination clusters and we might not be able to reach all of them. We
   * are using a semantic of "poll until all accessible datacenters report success".
   *
   * If a datacenter is not accessible, we ignore it.  As long as at least one datacenter is able to report success
   * we are willing to mark the job as successful.  If any datacenters report an explicit error status, we throw an
   * exception and fail the job.
   */
  private void pollStatusUntilComplete(Optional<String> incrementalPushVersion, ControllerClient controllerClient, PushJobSetting pushJobSetting, VersionTopicInfo versionTopicInfo){
    ExecutionStatus currentStatus = ExecutionStatus.NOT_CREATED;
    boolean keepChecking = true;

    /** Datacenter-specific details. Stored in memory to avoid printing repetitive details. */
    Map<String, String> previousExtraDetails = new HashMap<>();
    /** Overall job details. Stored in memory to avoid printing repetitive details. */
    String previousOverallDetails = null;

    while (keepChecking){
      Utils.sleep(5000); /* TODO better polling policy */
      keepChecking = false;
      switch (currentStatus) {
        case COMPLETED: /* batch jobs done */
        case END_OF_INCREMENTAL_PUSH_RECEIVED: /* incremental push jobs done */
        case ARCHIVED: /* This shouldn't happen, but presumably wont change on further polls */
          continue;
        case ERROR: /* failure to query (treat as connection issue, so we retry */
        case NOT_CREATED:
        case NEW:
        case STARTED:
        case START_OF_INCREMENTAL_PUSH_RECEIVED:
        case END_OF_PUSH_RECEIVED:
        case PROGRESS:
          JobStatusQueryResponse response = controllerClient.retryableRequest(pushJobSetting.controllerStatusPollRetries, c ->
              c.queryJobStatus(versionTopicInfo.topic, incrementalPushVersion));
          /**
           * Note: {@link JobStatusQueryResponse#isError()} means the status could not be queried.
           * This may be due to a communication error.
           */
          if (response.isError()){
            throw new RuntimeException("Failed to connect to: " + pushJobSetting.veniceControllerUrl + " to query job status, after " + pushJobSetting.controllerStatusPollRetries + " attempts.");
          } else {
            printJobStatus(response, previousOverallDetails, previousExtraDetails);
            ExecutionStatus status = ExecutionStatus.valueOf(response.getStatus());
            /* Status of ERROR means that the job status was queried, and the job is in an error status */
            if (status.equals(ExecutionStatus.ERROR)){
              throw new RuntimeException("Push job triggered error for: " + pushJobSetting.veniceControllerUrl);
            }
            currentStatus = status;
            keepChecking = true;
          }
          break;
        default:
          throw new VeniceException("Unexpected status returned by job status poll: " + pushJobSetting.veniceControllerUrl);
      }
    }
    /* At this point either reported success, or had connection issues */
    if (currentStatus.equals(ExecutionStatus.COMPLETED) || currentStatus.equals(ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED)){
      logger.info("Successfully pushed");
    } else {
      throw new VeniceException("Push failed with execution status: " + currentStatus.toString());
    }
  }

  private void printJobStatus(JobStatusQueryResponse response, String previousOverallDetails, Map<String, String> previousExtraDetails) {
    String fullStatus = response.getStatus();
    ExecutionStatus status = ExecutionStatus.valueOf(fullStatus);
    long messagesConsumed = response.getMessagesConsumed();
    long messagesAvailable = response.getMessagesAvailable();
    String logMessage = "Overall status: " + fullStatus;
    Map<String, String> extraInfo = response.getExtraInfo();
    if (null != extraInfo && !extraInfo.isEmpty()) {
      logMessage += ". Specific status: " + extraInfo;
    }
    logMessage += ". Consumed ";
    if (status != ExecutionStatus.COMPLETED) {
      double fractionOfMessagesConsumed = (double) messagesConsumed / (double) messagesAvailable;
      logMessage += "~" + PERCENT_FORMAT.format(fractionOfMessagesConsumed) + " of ";
    }
    logMessage += Utils.makeLargeNumberPretty(messagesAvailable) + " total records.";
    logger.info(logMessage);

    Optional<String> details = response.getOptionalStatusDetails();
    if (details.isPresent() && detailsAreDifferent(previousOverallDetails, details.get())) {
      logger.info("\t\tNew overall details: " + details.get());
      previousOverallDetails = details.get();
    }

    Optional<Map<String, String>> extraDetails = response.getOptionalExtraDetails();
    if (extraDetails.isPresent()) {
      // Non-upgraded controllers will not provide these details, in which case, this will be null.
      for (Map.Entry<String, String> entry: extraDetails.get().entrySet()) {
        String cluster = entry.getKey();
        String previous = previousExtraDetails.get(cluster);
        String current = entry.getValue();

        if (detailsAreDifferent(previous, current)) {
          logger.info("\t\tNew specific details for " + cluster + ": " + current);
          previousExtraDetails.put(cluster, current);
        }
      }
    }
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
    conf.set(KAFKA_BOOTSTRAP_SERVERS, versionTopicInfo.kafkaUrl);
    conf.set(COMPRESSION_STRATEGY, versionTopicInfo.compressionStrategy.toString());
    conf.set(REDUCER_MINIMUM_LOGGING_INTERVAL_MS, Long.toString(pushJobSetting.minimumReducerLoggingIntervalInMs));
    if( versionTopicInfo.sslToKafka ){
      conf.set(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, KAFKA_SECURITY_PROTOCOL);
      props.keySet().stream().filter(key -> key.toLowerCase().startsWith(SSL_PREFIX)).forEach(key -> {
        conf.set(key, props.getString(key));
      });
    }
    conf.setBoolean(VENICE_MAP_ONLY, pushJobSetting.isMapOnly);

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

    conf.setInt(VALUE_SCHEMA_ID_PROP, schemaInfo.valueSchemaId);

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
    conf.set(SSL_KEY_PASSWORD_PROPERTY_NAME, props.getString(SSL_KEY_PASSWORD_PROPERTY_NAME));

    if (props.containsKey(KAFKA_PRODUCER_REQUEST_TIMEOUT_MS)) {
      conf.set(KAFKA_PRODUCER_REQUEST_TIMEOUT_MS, props.getString(KAFKA_PRODUCER_REQUEST_TIMEOUT_MS));
    }
    if (props.containsKey(KAFKA_PRODUCER_RETRIES_CONFIG)) {
      conf.set(KAFKA_PRODUCER_RETRIES_CONFIG, props.getString(KAFKA_PRODUCER_RETRIES_CONFIG));
    }
  }

  protected void setupInputFormatConf(JobConf jobConf, SchemaInfo schemaInfo, String inputDirectory) {
    // Hadoop2 dev cluster provides a newer version of an avro dependency.
    // Set mapreduce.job.classloader to true to force the use of the older avro dependency.
    jobConf.setBoolean(MAPREDUCE_JOB_CLASSLOADER, true);
    logger.info("**************** " + MAPREDUCE_JOB_CLASSLOADER + ": " + jobConf.get(MAPREDUCE_JOB_CLASSLOADER));

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

  private void setupReducerConf(JobConf jobConf, PushJobSetting pushJobSetting, VersionTopicInfo versionTopicInfo) {
    if (pushJobSetting.isMapOnly) {
      jobConf.setMapSpeculativeExecution(false);
      jobConf.setNumReduceTasks(0);
    } else {
      jobConf.setPartitionerClass(VeniceMRPartitioner.class);
      jobConf.setMapOutputKeyClass(BytesWritable.class);
      jobConf.setMapOutputValueClass(BytesWritable.class);
      jobConf.setReducerClass(VeniceReducer.class);
      jobConf.setReduceSpeculativeExecution(pushJobSetting.enableReducerSpeculativeExecution);
      jobConf.setNumReduceTasks(versionTopicInfo.partitionCount);
    }
  }

  private void logPushJobProperties(VersionTopicInfo versionTopicInfo, PushJobSetting pushJobSetting, SchemaInfo schemaInfo, String clusterName, String inputDirectory, long inputFileDataSize) {
    logger.info("Kafka URL: " + versionTopicInfo.kafkaUrl);
    logger.info("Kafka Topic: " + versionTopicInfo.topic);
    logger.info("Kafka topic partition count: " + versionTopicInfo.partitionCount);
    logger.info("Kafka Queue Bytes: " + pushJobSetting.batchNumBytes);
    logger.info("Input Directory: " + inputDirectory);
    logger.info("Venice Store Name: " + pushJobSetting.storeName);
    logger.info("Venice Cluster Name: " + clusterName);
    logger.info("Venice URL: " + pushJobSetting.veniceControllerUrl);
    logger.info("File Schema: " + schemaInfo.fileSchemaString);
    logger.info("Avro key schema: " + schemaInfo.keySchemaString);
    logger.info("Avro value schema: " + schemaInfo.valueSchemaString);
    logger.info("Total input data file size: " + ((double) inputFileDataSize / 1024 / 1024) + " MB");
    logger.info("Is incremental push: " + pushJobSetting.isIncrementalPush);
    logger.info("Is duplicated key allowed" + pushJobSetting.isDuplicateKeyAllowed);
  }
  /**
   * Do not change this method argument type.
   * Used by Azkaban
   * http://azkaban.github.io/azkaban/docs/latest/#hadoopjava-type
   *
   * The cancel method is invoked dynamically by Azkaban for graceful
   * cancelling of the Job.
   *
   * @throws Exception
   */
  @Override
  public void cancel() {
    stopAndCleanup(pushJobSetting, controllerClient, versionTopicInfo);
    checkAndUploadPushJobStatus(PushJobStatus.KILLED, "", pushJobSetting, versionTopicInfo, pushStartTime, pushId);
  }

  private void stopAndCleanup(PushJobSetting pushJobSetting, ControllerClient controllerClient, VersionTopicInfo versionTopicInfo) {
    // Attempting to kill job. There's a race condition, but meh. Better kill when you know it's running
    try {
      if (null != runningJob && !runningJob.isComplete()) {
        runningJob.killJob();
      }
    } catch (Exception ex) {
      // Will try to kill Venice Offline Push Job no matter whether map-reduce job kill throws exception or not.
      logger.info("Received exception while killing map-reduce job", ex);
    }
    if (! Utils.isNullOrEmpty(versionTopicInfo.topic) && !pushJobSetting.isIncrementalPush) {
      controllerClient.retryableRequest(pushJobSetting.controllerRetries, c -> c.killOfflinePushJob(versionTopicInfo.topic));
      logger.info("Offline push job has been killed, topic: " + versionTopicInfo.topic);
    }
    close();
  }

  protected Schema extractAvroSubSchema(Schema origin, String fieldName) {
    Schema.Field field = origin.getField(fieldName);

    if (field == null) {
      throw new VeniceSchemaFieldNotFoundException(fieldName, "Could not find field: " + fieldName + " from " + origin.toString());
    }

    return field.schema();
  }

  private Schema getAvroFileHeader(FileSystem fs, Path path) throws IOException {
    logger.debug("path:" + path.toUri().getPath());

    GenericDatumReader<Object> avroReader = new GenericDatumReader<Object>();
    DataFileStream<Object> avroDataFileStream = null;
    Schema schema;
    try (InputStream hdfsInputStream = fs.open(path)) {
      avroDataFileStream = new DataFileStream<>(hdfsInputStream, avroReader);
      schema = avroDataFileStream.getSchema();
    } finally {
      if (avroDataFileStream != null) {
        avroDataFileStream.close();
      }
    }

    return schema;
  }

  private Pair<VsonSchema, VsonSchema> getVsonFileHeader(FileSystem fs, Path path) {
    Map<String, String> fileMetadata = getMetadataFromSequenceFile(fs, path);
    if (!fileMetadata.containsKey(FILE_KEY_SCHEMA) || !fileMetadata.containsKey(FILE_VALUE_SCHEMA)) {
      throw new VeniceException("Can't find Vson schema from file: " + path.getName());
    }

    return new Pair<>(VsonSchema.parse(fileMetadata.get(FILE_KEY_SCHEMA)),
        VsonSchema.parse(fileMetadata.get(FILE_VALUE_SCHEMA)));
  }

  //Vson-based file store key / value schema string as separated properties in file header
  private Pair<VsonSchema, VsonSchema> checkVsonSchemaConsistency(FileSystem fs, FileStatus[] fileStatusList) {
    Pair<VsonSchema, VsonSchema> vsonSchema = null;
    for (FileStatus status : fileStatusList) {
      Pair<VsonSchema, VsonSchema> newSchema = getVsonFileHeader(fs, status.getPath());
      if (vsonSchema == null) {
        vsonSchema = newSchema;
      } else {
        if (!vsonSchema.getFirst().equals(newSchema.getFirst()) || !vsonSchema.getSecond().equals(newSchema.getSecond())) {
          throw new VeniceInconsistentSchemaException(String.format("Inconsistent file Vson schema found. File: %s.\n Expected key schema: %s.\n"
                          + "Expected value schema: %s.\n File key schema: %s.\n File value schema: %s.", status.getPath().getName(),
                  vsonSchema.getFirst().toString(), vsonSchema.getSecond().toString(), newSchema.getFirst().toString(), newSchema.getSecond().toString()));
        }
      }
    }

    return vsonSchema;
  }

  //Avro-based file composes key and value schema as a whole
  private Schema checkAvroSchemaConsistency(FileSystem fs, FileStatus[] fileStatusList) throws IOException{
    Schema avroSchema = null;
    for (FileStatus status : fileStatusList) {
      Schema newSchema = getAvroFileHeader(fs, status.getPath());
      if (avroSchema == null) {
        avroSchema = newSchema;
      } else {
        if (!avroSchema.equals(newSchema)) {
          throw new VeniceInconsistentSchemaException(String.format("Inconsistent file Avro schema found. File: %s.\n"
              + "Expected file schema: %s.\n Real File schema: %s.", status.getPath().getName(),
              avroSchema.toString(), newSchema.toString()));
        }
      }
    }
    return avroSchema;
  }

  private long validateInputDir(FileStatus[] fileStatuses) {
    long inputFileDataSize = 0;
    for (FileStatus status : fileStatuses) {
      if (status.isDirectory()) {
        // Map-reduce job will fail if the input directory has sub-directory and 'recursive' is not specified.
        throw new VeniceException("Input directory: " + status.getPath().getParent().getName() +
            " should not have sub directory: " + status.getPath().getName());
      }

      inputFileDataSize += status.getLen();
    }
    return inputFileDataSize;
  }

  private Map<String, String> getMetadataFromSequenceFile(FileSystem fs, Path path) {
    TreeMap<String, String> metadataMap = new TreeMap<>();
    try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, new Configuration())) {
      reader.getMetadata().getMetadata().forEach((key, value) -> metadataMap.put(key.toString(), value.toString()));
    } catch (IOException e) {
      logger.info("Path: " + path.getName() + " is not a sequence file.");
    }
    return metadataMap;
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

  private String discoverCluster(PushJobSetting setting) {
    logger.info("Discover cluster for store:" + setting.storeName);
    // TODO: Evaluate what's the proper way to add retries here...
    ControllerResponse clusterDiscoveryResponse = ControllerClient.discoverCluster(setting.veniceControllerUrl, setting.storeName);
    if (clusterDiscoveryResponse.isError()) {
      throw new VeniceException("Get error in clusterDiscoveryResponse:" + clusterDiscoveryResponse.getError());
    } else {
      String clusterName = clusterDiscoveryResponse.getCluster();
      logger.info("Found cluster: " + clusterName + " for store: " + setting.storeName);
      return clusterName;
    }
  }

  public String getKafkaTopic() {
    return versionTopicInfo.topic;
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
    closeVeniceWriter();
    IOUtils.closeQuietly(controllerClient);
  }
}