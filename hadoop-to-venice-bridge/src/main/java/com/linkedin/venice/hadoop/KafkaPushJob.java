package com.linkedin.venice.hadoop;

import azkaban.jobExecutor.AbstractJob;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StorageEngineOverheadRatioResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.hadoop.pbnj.PostBulkLoadAnalysisMapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.hadoop.exceptions.VeniceInconsistentSchemaException;
import com.linkedin.venice.hadoop.exceptions.VeniceSchemaFieldNotFoundException;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.Utils;
import java.util.Arrays;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.TreeMap;
import javafx.util.Pair;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
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
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static org.apache.hadoop.mapreduce.MRJobConfig.MAPREDUCE_JOB_CLASSLOADER;
import static org.apache.hadoop.mapreduce.MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY;
import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

/**
 * This class sets up the Hadoop job used to push data to Venice.
 * The job reads the input data off HDFS. It supports 2 kinds of
 * input -- Avro / Binary Json (Vson).
 */
public class KafkaPushJob extends AbstractJob {
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

  private static final String HADOOP_PREFIX = "hadoop-conf.";

  // PBNJ-related configs are all optional
  public static final String PBNJ_ENABLE = "pbnj.enable";
  public static final String PBNJ_FAIL_FAST = "pbnj.fail.fast";
  public static final String PBNJ_ASYNC = "pbnj.async";
  public static final String PBNJ_ROUTER_URL_PROP = "pbnj.router.urls";
  public static final String PBNJ_SAMPLING_RATIO_PROP = "pbnj.sampling.ratio";

  public static final String STORAGE_QUOTA_PROP = "storage.quota";
  public static final String STORAGE_ENGINE_OVERHEAD_RATIO = "storage_engine_overhead_ratio";

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
  public static final int INPUT_DATA_SIZE_FACTOR = 2;

  // Immutable state
  private final VeniceProperties props;
  private final String id;
  private final boolean enablePush;
  private final String veniceControllerUrl;
  private final String veniceRouterUrl;
  private final String clusterName;
  private final String storeName;
  private final int batchNumBytes;
  private final boolean isMapOnly;
  private final boolean enablePBNJ;
  private final boolean pbnjFailFast;
  private final boolean pbnjAsync;
  private final double pbnjSamplingRatio;

  private final ControllerClient controllerClient;

  boolean isAvro = true;

  private String keyField;
  private String valueField;
  // Mutable state
  // Kafka url will get from Venice backend for store push
  private String kafkaUrl;
  private String inputDirectory;
  private FileSystem fs;
  private RunningJob runningJob;
  private String fileSchemaString;
  private String keySchemaString;
  private String valueSchemaString;
  // Value schema id retrieved from backend for valueSchemaString
  private int valueSchemaId;
  // Kafka topic for new data push
  private String topic;
  // Kafka topic partition count
  private int partitionCount;
  // Total input data size, which is used to talk to controller to decide whether we have enough quota or not
  private long inputFileDataSize;
  private long storeStorageQuota;
  private double storageEngineOverheadRatio;
  private VeniceWriter<KafkaKey, byte[]> veniceWriter; // Lazily initialized

  // This main method is not called by azkaban, this is only for testing purposes.
  //TODO: the main method is out-of-dated. We should remove
  public static void main(String[] args) throws Exception {
    OptionParser parser = new OptionParser();
    OptionSpec<String> veniceUrlOpt =
        parser.accepts("venice-url", "REQUIRED: comma-delimited venice URLs")
            .withRequiredArg()
            .describedAs("venice-url")
            .ofType(String.class);
    OptionSpec<String> inputPathOpt =
        parser.accepts("input-path", "REQUIRED: Input path")
            .withRequiredArg()
            .describedAs("input-path")
            .ofType(String.class);
    OptionSpec<String> clusterNameOpt =
        parser.accepts("cluster-name", "REQUIRED: cluster name")
            .withRequiredArg()
            .describedAs("cluster-name")
            .ofType(String.class);
    OptionSpec<String> storeNameOpt =
        parser.accepts("store-name", "REQUIRED: store name")
            .withRequiredArg()
            .describedAs("store-name")
            .ofType(String.class);
    OptionSpec<String> keyFieldOpt =
        parser.accepts("key-field", "REQUIRED: key field")
            .withRequiredArg()
            .describedAs("key-field")
            .ofType(String.class);
    OptionSpec<String> valueFieldOpt =
        parser.accepts("value-field", "REQUIRED: value field")
            .withRequiredArg()
            .describedAs("value-field")
            .ofType(String.class);
    OptionSpec<String> queueBytesOpt =
        parser.accepts("batch-num-bytes", "Optional: max number of bytes that should be batched in a flush to kafka")
            .withRequiredArg()
            .describedAs("batch-num-bytes")
            .ofType(String.class)
            .defaultsTo(Integer.toString(1000000));

    OptionSet options = parser.parse(args);
    validateExpectedArguments(new OptionSpec[] {inputPathOpt,  keyFieldOpt, valueFieldOpt, clusterNameOpt,
        veniceUrlOpt, storeNameOpt}, options, parser);

    Properties props = new Properties();
    props.put(VENICE_CLUSTER_NAME_PROP, options.valueOf(clusterNameOpt));
    props.put(VENICE_URL_PROP, options.valueOf(veniceUrlOpt));
    props.put(VENICE_STORE_NAME_PROP, options.valueOf(storeNameOpt));
    props.put(INPUT_PATH_PROP, options.valueOf(inputPathOpt));
    props.put(KEY_FIELD_PROP, options.valueOf(keyFieldOpt));
    props.put(VALUE_FIELD_PROP, options.valueOf(valueFieldOpt));
    // Optional ones
    props.put(BATCH_NUM_BYTES_PROP, options.valueOf(queueBytesOpt));

    KafkaPushJob job = new KafkaPushJob("Console", props);
    job.run();
  }

  private static void validateExpectedArguments(OptionSpec[] expectedOptions, OptionSet options, OptionParser parser)  throws IOException {
    for (int i = 0; i < expectedOptions.length; i++) {
      OptionSpec opt = expectedOptions[i];
      if (!options.has(opt)) {
        abortOnMissingArgument(opt, parser);
      }
    }
  }

  private static void abortOnMissingArgument(OptionSpec missingArgument, OptionParser parser)  throws IOException {
    logger.error("Missing required argument \"" + missingArgument + "\"");
    parser.printHelpOn(System.out);
    throw new VeniceException("Missing required argument \"" + missingArgument + "\"");
  }

  /**
   * Do not change this method argument type
   * Constructor used by Azkaban for creating the job.
   * http://azkaban.github.io/azkaban/docs/latest/#hadoopjava-type
   * @param jobId  id of the job
   * @param vanillaProps  Property bag for the job
   * @throws Exception
   */
  public KafkaPushJob(String jobId, Properties vanillaProps) throws Exception {
    super(jobId, logger);
    this.id = jobId;

    if (vanillaProps.contains(LEGACY_AVRO_KEY_FIELD_PROP)) {
      if (vanillaProps.contains(KEY_FIELD_PROP)) {
        throw new VeniceException("Duplicate key filed found in config. Both avro.key.field and key.field are set up.");
      }
      vanillaProps.setProperty(KEY_FIELD_PROP, vanillaProps.getProperty(LEGACY_AVRO_KEY_FIELD_PROP));
    }
    if (vanillaProps.contains(LEGACY_AVRO_VALUE_FIELD_PROP)) {
      if (vanillaProps.contains(VALUE_FIELD_PROP)) {
        throw new VeniceException("Duplicate value filed found in config. Both avro.value.field and value.field are set up.");
      }
      vanillaProps.setProperty(VALUE_FIELD_PROP, vanillaProps.getProperty(LEGACY_AVRO_VALUE_FIELD_PROP));
    }

    this.props = new VeniceProperties(vanillaProps);
    logger.info("Constructing " + KafkaPushJob.class.getSimpleName() + ": " + props.toString(true));

    // Optional configs:
    this.enablePush = props.getBoolean(ENABLE_PUSH, true);
    this.batchNumBytes = props.getInt(BATCH_NUM_BYTES_PROP, DEFAULT_BATCH_BYTES_SIZE);
    this.isMapOnly = props.getBoolean(VENICE_MAP_ONLY, false);
    this.enablePBNJ = props.getBoolean(PBNJ_ENABLE, false);
    this.pbnjFailFast = props.getBoolean(PBNJ_FAIL_FAST, false);
    this.pbnjAsync = props.getBoolean(PBNJ_ASYNC, false);
    this.pbnjSamplingRatio = props.getDouble(PBNJ_SAMPLING_RATIO_PROP, 1.0);

    if (enablePBNJ) {
      // If PBNJ is enabled, then the router URL config is mandatory
      this.veniceRouterUrl = props.getString(PBNJ_ROUTER_URL_PROP);
    } else {
      this.veniceRouterUrl = null;
    }

    // Mandatory configs:
    this.clusterName = props.getString(VENICE_CLUSTER_NAME_PROP);
    this.veniceControllerUrl = props.getString(VENICE_URL_PROP);
    this.storeName = props.getString(VENICE_STORE_NAME_PROP);
    this.inputDirectory = props.getString(INPUT_PATH_PROP);


    if (!enablePush && !enablePBNJ) {
      throw new VeniceException("At least one of the following config properties must be true: " + ENABLE_PUSH + " or " + PBNJ_ENABLE);
    }

    this.controllerClient = new ControllerClient(this.clusterName, this.veniceControllerUrl);
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

      // Create FileSystem handle, which will be shared in multiple functions
      Configuration conf = new Configuration();
      fs = FileSystem.get(conf);
      // This will try to extract the source folder if the input path has 'latest' anchor.
      Path sourcePath = getLatestPathOfInputDirectory(inputDirectory, fs);
      inputDirectory = sourcePath.toString();

      // Check Avro file schema consistency, data size
      inspectHdfsSource(sourcePath);

      validateKeySchema();
      validateValueSchema();
      checkStoreStorageQuota();

      JobClient jc;

      if (enablePush) {
        // Create new store version, topic and fetch Kafka url from backend
        createNewStoreVersion();
        // Log Venice data push job related info
        logPushJobProperties();

        // Setup the hadoop job
        JobConf pushJobConf = setupMRConf();
        // Whether the messages in one single topic partition is lexicographically sorted by key bytes.
        // If reducer phase is enabled, each reducer will sort all the messages inside one single
        // topic partition.
        boolean sortedMessageInTopicPartition = !isMapOnly;
        jc = new JobClient(pushJobConf);
        getVeniceWriter().broadcastStartOfPush(sortedMessageInTopicPartition, new HashMap<>());
        // submit the job for execution and wait for completion
        runningJob = jc.runJob(pushJobConf);
        //TODO: send a failure END OF PUSH message if something went wrong
        getVeniceWriter().broadcastEndOfPush(new HashMap<>());
        // Close VeniceWriter before polling job status since polling job status could
        // trigger job deletion
        closeVeniceWriter();

        // Waiting for Venice Backend to complete consumption
        pollStatusUntilComplete();
      } else {
        logger.info("Skipping push job, since " + ENABLE_PUSH + " is set to false.");
      }

      if (enablePBNJ) {
        logger.info("Post-Bulkload Analysis Job is about to run.");
        JobConf pbnjJobConf = setupPBNJConf();
        jc = new JobClient(pbnjJobConf);
        runningJob = jc.runJob(pbnjJobConf);
      }
    } catch (Exception e) {
      closeVeniceWriter();
      try {
        cancel();
      } catch (Exception ex) {
        logger.info("Failed to cancel job", ex);
      }
      if (! (e instanceof VeniceException)) {
        e = new VeniceException("Exception caught during Hadoop to Venice Bridge!", e);
      }
      throw (VeniceException) e;
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
  private void validateKeySchema() {
    SchemaResponse keySchemaResponse = controllerClient.getKeySchema(storeName);
    if (keySchemaResponse.isError()) {
      throw new VeniceException("Got an error in keySchemaResponse: " + keySchemaResponse.toString());
    } else if (null == keySchemaResponse.getSchemaStr()) {
      // TODO: Fix the server-side request handling. This should not happen. We should get a 404 instead.
      throw new VeniceException("Got a null schema in keySchemaResponse: " + keySchemaResponse.toString());
    }
    SchemaEntry serverSchema = new SchemaEntry(keySchemaResponse.getId(), keySchemaResponse.getSchemaStr());
    SchemaEntry clientSchema = new SchemaEntry(1, keySchemaString);
    if (! serverSchema.equals(clientSchema)) {
      String briefErrorMessage = "Key schema mis-match for store " + storeName;
      logger.error(briefErrorMessage +
          "\n\t\tController URLs: " + veniceControllerUrl +
          "\n\t\tschema defined in HDFS: \t" + keySchemaString +
          "\n\t\tschema defined in Venice: \t" + keySchemaResponse.getSchemaStr());
      throw new VeniceException(briefErrorMessage);
    }
  }

  /***
   * This method will talk to controller to validate value schema.
   */
  private void validateValueSchema() {
    SchemaResponse valueSchemaResponse = controllerClient.getValueSchemaID(storeName, valueSchemaString);
    if (valueSchemaResponse.isError()) {
      throw new VeniceException("Fail to validate value schema: " + valueSchemaString
          + " for store: " + storeName
          + ", error: " + valueSchemaResponse.getError());
    }
    valueSchemaId = valueSchemaResponse.getId();
    logger.info("Got schema id: " + valueSchemaId + " for value schema: " + valueSchemaString + " of store: " + storeName);
  }

  private void checkStoreStorageQuota() {
    StoreResponse storeResponse = controllerClient.getStore(storeName);
    if (storeResponse.isError()) {
      throw new VeniceException("Can't get store info. " + storeResponse.getError());
    }

    storeStorageQuota = storeResponse.getStore().getStorageQuotaInByte();
    if (storeStorageQuota != Store.UNLIMITED_STORAGE_QUOTA && isMapOnly) {
      throw new VeniceException("Can't run this mapper only job since storage quota is not unlimited. " + "Store: " + storeName);
    }

    StorageEngineOverheadRatioResponse storageEngineOverheadRatioResponse =
        controllerClient.getStorageEngineOverheadRatio(storeName);
    if (storageEngineOverheadRatioResponse.isError()) {
      throw new VeniceException("Can't get storage engine overhead ratio. " +
      storageEngineOverheadRatioResponse.getError());
    }

    storageEngineOverheadRatio = storageEngineOverheadRatioResponse.getStorageEngineOverheadRatio();
  }

  /**
   * This method will talk to parent controller to create new store version, which will create new topic for the version as well.
   */
  private void createNewStoreVersion() {
    VersionCreationResponse versionCreationResponse = controllerClient.createNewStoreVersion(storeName, inputFileDataSize);
    if (versionCreationResponse.isError()) {
      throw new VeniceException("Failed to create new store version with urls: " + veniceControllerUrl
          + ", error: " + versionCreationResponse.getError());
    } else if (versionCreationResponse.getVersion() == 0) {
      // TODO: Fix the server-side request handling. This should not happen. We should get a 404 instead.
      throw new VeniceException("Got version 0 from: " + versionCreationResponse.toString());
    } else {
      logger.info(versionCreationResponse.toString());
    }
    topic = versionCreationResponse.getKafkaTopic();
    kafkaUrl = versionCreationResponse.getKafkaBootstrapServers();
    partitionCount = versionCreationResponse.getPartitions();
  }

  private synchronized VeniceWriter<KafkaKey, byte[]> getVeniceWriter() {
    if (null == this.veniceWriter) {
      // Initialize VeniceWriter
      Properties veniceWriterProperties = new Properties();
      veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, kafkaUrl);
      if (props.containsKey(VeniceWriter.CLOSE_TIMEOUT_MS)){ /* Writer uses default if not specified */
        veniceWriterProperties.put(VeniceWriter.CLOSE_TIMEOUT_MS, props.getInt(VeniceWriter.CLOSE_TIMEOUT_MS));
      }
      VeniceWriter<KafkaKey, byte[]> newVeniceWriter = new VeniceWriter<>(
          new VeniceProperties(veniceWriterProperties),
          topic,
          new KafkaKeySerializer(),
          new DefaultSerializer());
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
  private void pollStatusUntilComplete(){
    ExecutionStatus currentStatus = ExecutionStatus.NOT_CREATED;
    boolean keepChecking = true;
    while (keepChecking){
      Utils.sleep(5000); /* TODO better polling policy */
      keepChecking = false;
      switch (currentStatus) {
        /* Note that we're storing a status of ERROR if the response object .isError() returns true.
           If .isError() is false and the response object's status is ERROR we throw an exception since the job has failed
           Otherwise we store the response object's status.
           This is a potentially confusing overloaded use of the ERROR status, since it means something different
             in the response object compared to the stored perDatacenterStatus.
         */
        case COMPLETED: /* jobs done */
        case ARCHIVED: /* This shouldn't happen, but presumably wont change on further polls */
          continue;
        case ERROR: /* failure to query (treat as connection issue, so we retry */
        case NOT_CREATED:
        case NEW:
        case STARTED:
        case END_OF_PUSH_RECEIVED:
        case PROGRESS:
          /* This could take 90 seconds if connection is down.  30 second timeout with 3 attempts */
          JobStatusQueryResponse response = ControllerClient.queryJobStatusWithRetry(veniceControllerUrl, clusterName, topic, 3);
          /*  Note: .isError means the status could not be queried.  This may be due to a communication error, or
              a caught exception.
           */
          if (response.isError()){
            if (currentStatus.equals(ExecutionStatus.ERROR)){ // give up if we are already errored.  For connection issues means 3 minutes of issues
              throw new RuntimeException("Failed to connect to: " + veniceControllerUrl + " to query job status.");
            } else {
              logger.error("Error querying job status from: " + veniceControllerUrl + ", error message: " + response.getError());
              currentStatus = ExecutionStatus.ERROR; /* Note: overloading usage of ERROR */
            }
          } else {
            ExecutionStatus status = ExecutionStatus.valueOf(response.getStatus());
            long messagesConsumed = response.getMessagesConsumed();
            long messagesAvailable = response.getMessagesAvailable();
            logger.info("Consumed " + messagesConsumed + " out of " + messagesAvailable + " records.  Status: " + status);
            Map<String, String> extraInfo = response.getExtraInfo();
            if (null != extraInfo && !extraInfo.isEmpty()) {
              logger.info("Extra info: " + extraInfo);
            }
            /* Status of ERROR means that the job status was queried, and the job is in an error status */
            if (status.equals(ExecutionStatus.ERROR)){
              throw new RuntimeException("Push job triggered error for: " + veniceControllerUrl);
            }
            currentStatus = status;
            keepChecking = true;
          }
          break;
        default:
          throw new VeniceException("Unexpected status returned by job status poll: " + veniceControllerUrl);
      }

    }
      /* At this point either reported success, or had connection issues */
    if (currentStatus.equals(ExecutionStatus.COMPLETED)){
      logger.info("Successfully pushed");
    } else {
      throw new VeniceException("Push failed with execution status: " + currentStatus.toString());
    }
  }

  private JobConf setupMRConf() {
    return setupReducerConf(setupInputFormatConf(setupDefaultJobConf(), isAvro), isMapOnly);
  }

  private JobConf setupPBNJConf() {
    if (!isAvro) {
      throw new VeniceException("PBNJ only supports Avro input format");
    }

    JobConf conf = setupReducerConf(setupInputFormatConf(setupDefaultJobConf(), true), true);
    conf.set(VENICE_STORE_NAME_PROP, storeName);
    conf.set(PBNJ_ROUTER_URL_PROP, veniceRouterUrl);
    conf.set(PBNJ_FAIL_FAST, Boolean.toString(pbnjFailFast));
    conf.set(PBNJ_ASYNC, Boolean.toString(pbnjAsync));
    conf.set(PBNJ_SAMPLING_RATIO_PROP, Double.toString(pbnjSamplingRatio));
    conf.set(STORAGE_QUOTA_PROP, Long.toString(Store.UNLIMITED_STORAGE_QUOTA));
    conf.setMapperClass(PostBulkLoadAnalysisMapper.class);

    return conf;
  }

  private JobConf setupDefaultJobConf() {
    JobConf conf = new JobConf();

    conf.set(BATCH_NUM_BYTES_PROP, Integer.toString(batchNumBytes));
    conf.set(TOPIC_PROP, topic);
    conf.set(KAFKA_BOOTSTRAP_SERVERS, kafkaUrl);
    conf.setBoolean(VENICE_MAP_ONLY, isMapOnly);

    // Hadoop2 dev cluster provides a newer version of an avro dependency.
    // Set mapreduce.job.classloader to true to force the use of the older avro dependency.
    conf.setBoolean(MAPREDUCE_JOB_CLASSLOADER, true);
    logger.info("**************** " + MAPREDUCE_JOB_CLASSLOADER + ": " + conf.get(MAPREDUCE_JOB_CLASSLOADER));

    conf.set(STORAGE_QUOTA_PROP, Long.toString(storeStorageQuota));
    conf.set(STORAGE_ENGINE_OVERHEAD_RATIO, Double.toString(storageEngineOverheadRatio));

    /** Allow overriding properties if their names start with {@link HADOOP_PREFIX}. */
    for (String key : props.keySet()) {
      String lowerCase = key.toLowerCase();
      if (lowerCase.startsWith(HADOOP_PREFIX)) {
        String overrideKey = key.substring(HADOOP_PREFIX.length());
        conf.set(overrideKey, props.getString(key));
      }
    }

    conf.setInt(VALUE_SCHEMA_ID_PROP, valueSchemaId);

    if (System.getenv(HADOOP_TOKEN_FILE_LOCATION) != null) {
      conf.set(MAPREDUCE_JOB_CREDENTIALS_BINARY, System.getenv(HADOOP_TOKEN_FILE_LOCATION));
    }

    conf.setJobName(id + ":" + "hadoop_to_venice_bridge" + "-" + topic);
    conf.setJarByClass(this.getClass());


    // TODO:The job is using path-filter to check the consistency of avro file schema ,
    // but doesn't specify the path filter for the input directory of map-reduce job.
    // We need to revisit it if any failure because of this happens.
    FileInputFormat.setInputPaths(conf, new Path(inputDirectory));

    //do we need these two configs?
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(NullWritable.class);

    // Setting up the Output format
    conf.setOutputFormat(NullOutputFormat.class);

    return conf;
  }

  private JobConf setupInputFormatConf(JobConf jobConf, boolean isAvro) {
    jobConf.set(KEY_FIELD_PROP, keyField);
    jobConf.set(VALUE_FIELD_PROP, valueField);

    if (isAvro) {
      jobConf.set(SCHEMA_STRING_PROP, fileSchemaString);
      jobConf.set(AvroJob.INPUT_SCHEMA, fileSchemaString);
      jobConf.setClass("avro.serialization.data.model", GenericData.class, GenericData.class);
      jobConf.setInputFormat(AvroInputFormat.class);
      jobConf.setMapperClass(VeniceAvroMapper.class);
    } else {
      jobConf.setInputFormat(VsonSequenceFileInputFormat.class);
      jobConf.setMapperClass(VeniceVsonMapper.class);
    }

    return jobConf;
  }

  private JobConf setupReducerConf(JobConf jobConf, boolean isMapOnly) {
    if (isMapOnly) {
      jobConf.setMapSpeculativeExecution(false);
      jobConf.setNumReduceTasks(0);
    } else {
      jobConf.setPartitionerClass(VeniceMRPartitioner.class);

      jobConf.setMapOutputKeyClass(BytesWritable.class);
      jobConf.setMapOutputValueClass(BytesWritable.class);
      jobConf.setReducerClass(VeniceReducer.class);
      // TODO: Consider turning speculative execution on later.
      jobConf.setReduceSpeculativeExecution(false);
      jobConf.setNumReduceTasks(partitionCount);
    }

    return jobConf;
  }

  private void logPushJobProperties() {
    logger.info("Kafka URL: " + kafkaUrl);
    logger.info("Kafka Topic: " + topic);
    logger.info("Kafka topic partition count: " + partitionCount);
    logger.info("Kafka Queue Bytes: " + batchNumBytes);
    logger.info("Input Directory: " + inputDirectory);
    logger.info("Venice Store Name: " + storeName);
    logger.info("Venice Cluster Name: " + clusterName);
    logger.info("Venice URL: " + veniceControllerUrl);
    logger.info("File Schema: " + fileSchemaString);
    logger.info("Avro key schema: " + keySchemaString);
    logger.info("Avro value schema: " + valueSchemaString);
    logger.info("Total input data file size: " + ((double) inputFileDataSize / 1024 / 1024) + " MB");
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
  public void cancel() throws Exception {
    // Attempting to kill job. There's a race condition, but meh. Better kill when you know it's running
    try {
      if (null != runningJob && !runningJob.isComplete()) {
        runningJob.killJob();
      }
    } catch (Exception ex) {
      // Will try to kill Venice Offline Push Job no matter whether map-reduce job kill throws exception or not.
      logger.info("Received exception while killing map-reduce job", ex);
    }
    if (! Utils.isNullOrEmpty(topic)) {
      controllerClient.killOfflinePushJob(topic);
      logger.info("Offline push job has been killed, topic: " + topic);
    }
    closeVeniceWriter();
  }

  /**
   * Pulls out the schema from an avro file, ignoring directories and prefix "."
   * This function will do the following tasks as well:
   * 1. Check whether we have sub-directory in inputDirectory or not;
   * 2. Calculate total data size of input directory;
   * 3. Check whether it's Vson input or Avro input
   * 4. Check schema consistency;
   * 5. Populate key schema, value schema;
   */
  private void inspectHdfsSource(Path srcPath) throws IOException {
    inputFileDataSize = 0;
    FileStatus[] fileStatuses = fs.listStatus(srcPath, PATH_FILTER);

    if (fileStatuses == null || fileStatuses.length == 0) {
      throw new RuntimeException("No data found at source path: " + srcPath);
    }

    inputFileDataSize = validateInputDir(fileStatuses);
    // Since the job is calculating the raw data file size, which is not accurate because of compression, key/value schema and backend storage overhead,
    // we are applying this factor to provide a more reasonable estimation.
    inputFileDataSize *= INPUT_DATA_SIZE_FACTOR;

    //try reading the file via sequence file reader. It indicates Vson input if it is succeeded.
    Map<String, String> fileMetadata = getMetadataFromSequenceFile(fs, fileStatuses[0].getPath());
    if (fileMetadata.containsKey(FILE_KEY_SCHEMA) && fileMetadata.containsKey(FILE_VALUE_SCHEMA)) {
      isAvro = false;
    }

    if (isAvro) {
      logger.info("Detected Avro input format.");
      keyField = props.getString(KEY_FIELD_PROP);
      valueField = props.getString(VALUE_FIELD_PROP);

      Schema avroSchema = checkAvroSchemaConsistency(fileStatuses);

      fileSchemaString = avroSchema.toString();
      keySchemaString = extractSubSchema(avroSchema, this.keyField).toString();
      valueSchemaString = extractSubSchema(avroSchema, this.valueField).toString();
    } else {
      logger.info("Detected Vson input format, will convert to Avro automatically.");
      //key / value fields are optional for Vson input
      keyField = props.getString(KEY_FIELD_PROP, "");
      valueField = props.getString(VALUE_FIELD_PROP, "");

      Pair<Schema, Schema> vsonSchemaPair = checkVsonSchemaConsistency(fileStatuses);

      keySchemaString = extractSubSchema(vsonSchemaPair.getKey(), this.keyField).toString();
      valueSchemaString = extractSubSchema(vsonSchemaPair.getValue(), this.valueField).toString();
    }
  }

  private Schema extractSubSchema(Schema origin, String fieldName) {
    if (Utils.isNullOrEmpty(fieldName)) {
      return origin;
    }

    Schema.Field field = origin.getField(fieldName);
    if (field == null) {
      throw new VeniceSchemaFieldNotFoundException(fieldName, "Could not find field: " + fieldName + " from " + origin.toString());
    }

    return field.schema();
  }

  private Schema getAvroFileHeader(Path path) throws IOException {
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

  private Pair<Schema, Schema> getVsonFileHeader(Path path) {
    Map<String, String> fileMetadata = getMetadataFromSequenceFile(fs, path);
    if (!fileMetadata.containsKey(FILE_KEY_SCHEMA) || !fileMetadata.containsKey(FILE_VALUE_SCHEMA)) {
      throw new VeniceException("Can't find Vson schema from file: " + path.getName());
    }

    return new Pair<>(VsonAvroSchemaAdapter.parse(fileMetadata.get(FILE_KEY_SCHEMA)),
                      VsonAvroSchemaAdapter.parse(fileMetadata.get(FILE_VALUE_SCHEMA)));
  }

  //Vson-based file store key / value schema string as separated properties in file header
  private Pair<Schema, Schema> checkVsonSchemaConsistency(FileStatus[] fileStatusList) {
    Pair<Schema, Schema> vsonSchema = null;
    for (FileStatus status : fileStatusList) {
      Pair newSchema = getVsonFileHeader(status.getPath());
      if (vsonSchema == null) {
        vsonSchema = newSchema;
      } else {
        if (!vsonSchema.getKey().equals(newSchema.getKey()) || !vsonSchema.getValue().equals(newSchema.getValue())) {
          throw new VeniceInconsistentSchemaException(String.format("Inconsistent file Vson schema found. File: %s.\n Expected key schema: %s.\n"
                          + "Expected value schema: %s.\n File key schema: %s.\n File value schema: %s.", status.getPath().getName(),
                  vsonSchema.getKey().toString(), vsonSchema.getValue().toString(), newSchema.getKey().toString(), newSchema.getValue().toString()));
        }
      }
    }

    return vsonSchema;
  }

  //Avro-based file composes key and value schema as a whole
  private Schema checkAvroSchemaConsistency(FileStatus[] fileStatusList) throws IOException{
    Schema avroSchema = null;
    for (FileStatus status : fileStatusList) {
      Schema newSchema = getAvroFileHeader(status.getPath());
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

  public String getKafkaTopic() {
    return topic;
  }

  public String getInputDirectory() {
    return inputDirectory;
  }

  public String getFileSchemaString() {
    return fileSchemaString;
  }

  public String getKeySchemaString() {
    return keySchemaString;
  }

  public String getValueSchemaString() {
    return valueSchemaString;
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
}