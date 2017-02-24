package com.linkedin.venice.hadoop;

import azkaban.jobExecutor.AbstractJob;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.hadoop.pbnj.PostBulkloadAnalysisOutputFormat;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.NewStoreResponse;
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
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;
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
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static org.apache.hadoop.mapreduce.MRJobConfig.MAPREDUCE_JOB_CLASSLOADER;
import static org.apache.hadoop.mapreduce.MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY;
import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

/**
 * This class sets up the Hadoop job used to push data to Venice.
 * The job reads the input data off HDFS and pushes it to venice using Mapper tasks.
 * The job spawns only Mappers, and there are no Reducers.
 */
public class KafkaPushJob extends AbstractJob {
  // Job Properties
  public static final String AVRO_KEY_FIELD_PROP = "avro.key.field";
  public static final String AVRO_VALUE_FIELD_PROP = "avro.value.field";

  /**
   * In single-colo mode, this can be either a controller or router.
   * In multi-colo mode, it must be a parent controller.
   */
  public static final String VENICE_URL_PROP = "venice.urls";

  public static final String ENABLE_PUSH = "venice.push.enable";
  public static final String VENICE_CLUSTER_NAME_PROP = "cluster.name";
  public static final String VENICE_STORE_NAME_PROP = "venice.store.name";
  public static final String VENICE_STORE_OWNERS_PROP = "venice.store.owners";
  public static final String AUTO_CREATE_STORE_PROP = "auto.create.store";
  public static final String INPUT_PATH_PROP = "input.path";
  public static final String BATCH_NUM_BYTES_PROP = "batch.num.bytes";

  public static final String SCHEMA_STRING_PROP = "schema";
  public static final String VALUE_SCHEMA_ID_PROP = "value.schema.id";
  public static final String KAFKA_URL_PROP = "venice.kafka.url";
  public static final String TOPIC_PROP = "venice.kafka.topic";

  private static final String HADOOP_PREFIX = "hadoop-conf.";

  // PBNJ-related configs are all optional
  public static final String PBNJ_ENABLE = "pbnj.enable";
  public static final String PBNJ_FAIL_FAST = "pbnj.fail.fast";
  public static final String PBNJ_ASYNC = "pbnj.async";
  public static final String PBNJ_ROUTER_URL_PROP = "pbnj.router.urls";

  private static Logger logger = Logger.getLogger(KafkaPushJob.class);

  public static int DEFAULT_BATCH_BYTES_SIZE = 1000000;

  /**
   * Since the job is calculating the raw data file size, which is not accurate because of compression,
   * key/value schema and backend storage overhead, we are applying this factor to provide a more
   * reasonable estimation.
   */
  public static final int INPUT_DATA_SIZE_FACTOR = 2;

  // Immutable state
  private final VeniceProperties props;
  private final String id;
  private final boolean enablePush;
  private final String keyField;
  private final String valueField;
  private final String veniceControllerUrl;
  private final String veniceRouterUrl;
  private final String clusterName;
  private final String storeName;
  private final String storeOwners;
  private final int batchNumBytes;
  private final boolean autoCreateStoreIfNeeded;
  private final boolean enablePBNJ;
  private final boolean pbnjFailFast;
  private final boolean pbnjAsync;

  private final ControllerClient controllerClient;

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
  private Schema parsedFileSchema;
  // Kafka topic for new data push
  private String topic;
  // Total input data size, which is used to talk to controller to decide whether we have enough quota or not
  private long inputFileDataSize;
  private VeniceWriter<KafkaKey, byte[]> veniceWriter; // Lazily initialized

  // This main method is not called by azkaban, this is only for testing purposes.
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
    OptionSpec<String> storeOwnersOpt =
        parser.accepts("store-owners", "REQUIRED: store owners that should be a comma-separated list of email addresses")
            .withRequiredArg()
            .describedAs("store-owners")
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
    OptionSpecBuilder autoCreateOpt =
        parser.accepts("create-store", "Flag: auto create store if it does not exist");

    OptionSet options = parser.parse(args);
    validateExpectedArguments(new OptionSpec[] {inputPathOpt,  keyFieldOpt, valueFieldOpt, clusterNameOpt,
        veniceUrlOpt, storeNameOpt, storeOwnersOpt}, options, parser);

    Properties props = new Properties();
    props.put(VENICE_CLUSTER_NAME_PROP, options.valueOf(clusterNameOpt));
    props.put(VENICE_URL_PROP, options.valueOf(veniceUrlOpt));
    props.put(VENICE_STORE_NAME_PROP, options.valueOf(storeNameOpt));
    props.put(VENICE_STORE_OWNERS_PROP, options.valueOf(storeOwnersOpt));
    props.put(INPUT_PATH_PROP, options.valueOf(inputPathOpt));
    props.put(AVRO_KEY_FIELD_PROP, options.valueOf(keyFieldOpt));
    props.put(AVRO_VALUE_FIELD_PROP, options.valueOf(valueFieldOpt));
    // Optional ones
    props.put(BATCH_NUM_BYTES_PROP, options.valueOf(queueBytesOpt));

    boolean autoCreateStore = options.has(autoCreateOpt);
    if (autoCreateStore){
      props.put(AUTO_CREATE_STORE_PROP, "true");
    }

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
    this.props = new VeniceProperties(vanillaProps);
    logger.info("Constructing " + KafkaPushJob.class.getSimpleName() + ": " + props.toString(true));

    // Optional configs:
    this.enablePush = props.getBoolean(ENABLE_PUSH, true);
    this.autoCreateStoreIfNeeded = props.getBoolean(AUTO_CREATE_STORE_PROP, false);
    this.batchNumBytes = props.getInt(BATCH_NUM_BYTES_PROP, DEFAULT_BATCH_BYTES_SIZE);
    this.enablePBNJ = props.getBoolean(PBNJ_ENABLE, false);
    this.pbnjFailFast = props.getBoolean(PBNJ_FAIL_FAST, false);
    this.pbnjAsync = props.getBoolean(PBNJ_ASYNC, false);

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
    this.storeOwners = props.getString(VENICE_STORE_OWNERS_PROP);
    this.inputDirectory = props.getString(INPUT_PATH_PROP);
    this.keyField = props.getString(AVRO_KEY_FIELD_PROP);
    this.valueField = props.getString(AVRO_VALUE_FIELD_PROP);

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

      if (enablePush && autoCreateStoreIfNeeded) {
        createStoreIfNeeded();
        uploadValueSchema();
      }

      validateKeySchema();
      validateValueSchema();

      JobClient jc;

      if (enablePush) {
        // Create new store version, topic and fetch Kafka url from backend
        createNewStoreVersion();
        // Log Venice data push job related info
        logPushJobProperties();

        // Setup the hadoop job
        JobConf pushJobConf = setupPushJob(conf);
        jc = new JobClient(pushJobConf);

        getVeniceWriter().broadcastStartOfPush(new HashMap<>());
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
        JobConf pbnjJobConf = setupPBNJ(conf);
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
   * This method will talk to parent controller to create new store if necessary.
   */
  private void createStoreIfNeeded(){
    NewStoreResponse response = controllerClient.createNewStore(clusterName, storeName,
        "H2V-user", keySchemaString, valueSchemaString);
    if (response.isError()){
      throw new VeniceException("Error creating new store with urls: " + veniceControllerUrl + "  " + response.getError());
    }
  }

  /**
   * This method will talk to parent controller to validate key schema.
   */
  private void validateKeySchema() {
    SchemaResponse keySchemaResponse = controllerClient.getKeySchema(clusterName, storeName);
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
          "\n\t\texpected: \t" + keySchemaString +
          "\n\t\tactual: \t" + keySchemaResponse.getSchemaStr());
      throw new VeniceException(briefErrorMessage);
    }
  }

  /***
   * TODO: rip out this method when we don't need to auto create stores any more
   */
  private void uploadValueSchema() {
    SchemaResponse valueSchemaResponse = controllerClient.addValueSchema(clusterName, storeName, valueSchemaString);
    if (valueSchemaResponse.isError()) {
      throw new VeniceException("Fail to validate/create value schema: " + valueSchemaString
          + " for store: " + storeName
          + ", error: " + valueSchemaResponse.getError());
    }
  }

  /***
   * This method will talk to controller to validate value schema.
   */
  private void validateValueSchema() {
    SchemaResponse valueSchemaResponse = controllerClient.getValueSchemaID(clusterName, storeName, valueSchemaString);
    if (valueSchemaResponse.isError()) {
      throw new VeniceException("Fail to validate value schema: " + valueSchemaString
          + " for store: " + storeName
          + ", error: " + valueSchemaResponse.getError());
    }
    valueSchemaId = valueSchemaResponse.getId();
    logger.info("Got schema id: " + valueSchemaId + " for value schema: " + valueSchemaString + " of store: " + storeName);
  }

  /**
   * This method will talk to parent controller to create new store version, which will create new topic for the version as well.
   */
  private void createNewStoreVersion() {
    VersionCreationResponse versionCreationResponse = controllerClient.createNewStoreVersion(clusterName, storeName, inputFileDataSize);
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

  /**
   * Calls {@link #setupHadoopJob(Configuration, Class, String)} and adds some extra properties required
   * by the {@link VeniceWriter} used by the push job.
   */
  private JobConf setupPushJob(Configuration conf) throws IOException {
    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
    conf.set(BATCH_NUM_BYTES_PROP, Integer.toString(batchNumBytes));
    conf.set(TOPIC_PROP, topic);
    conf.set(KAFKA_BOOTSTRAP_SERVERS, kafkaUrl);
    return setupHadoopJob(conf, AvroKafkaOutputFormat.class, "VenicePushJob");
  }

  /**
   * Calls {@link #setupHadoopJob(Configuration, Class, String)} and adds some extra properties required
   * by the {@link com.linkedin.venice.hadoop.pbnj.PostBulkloadAnalysisRecordWriter}.
   */
  private JobConf setupPBNJ(Configuration conf) throws IOException {
    conf.set(VENICE_STORE_NAME_PROP, storeName);
    conf.set(PBNJ_ROUTER_URL_PROP, veniceRouterUrl);
    conf.set(PBNJ_FAIL_FAST, Boolean.toString(pbnjFailFast));
    conf.set(PBNJ_ASYNC, Boolean.toString(pbnjAsync));
    return setupHadoopJob(conf, PostBulkloadAnalysisOutputFormat.class, "VenicePBNJ");
  }

  /**
   * Set up a Hadoop {@link JobConf} used to trigger a job.
   *
   * Should not be called directly. Instead, use:
   *
   * @see #setupPushJob(Configuration)
   * @see #setupPBNJ(Configuration)
   */
  private JobConf setupHadoopJob(Configuration conf,
                                 Class<? extends OutputFormat> outputFormatClass,
                                 String jobName) throws IOException {
    // Hadoop2 dev cluster provides a newer version of an avro dependency.
    // Set mapreduce.job.classloader to true to force the use of the older avro dependency.
    conf.setBoolean(MAPREDUCE_JOB_CLASSLOADER, true);
    logger.info("**************** " + MAPREDUCE_JOB_CLASSLOADER + ": " + conf.get(MAPREDUCE_JOB_CLASSLOADER));

    /** Allow overriding properties if their names start with {@link HADOOP_PREFIX}. */
    for (String key : props.keySet()) {
      String lowerCase = key.toLowerCase();
      if (lowerCase.startsWith(HADOOP_PREFIX)) {
        String overrideKey = key.substring(HADOOP_PREFIX.length());
        conf.set(overrideKey, props.getString(key));
      }
    }

    // Following properties are for Avro schema validation
    conf.set(SCHEMA_STRING_PROP, fileSchemaString);
    conf.set(AVRO_KEY_FIELD_PROP, keyField);
    conf.set(AVRO_VALUE_FIELD_PROP, valueField);
    conf.set(VALUE_SCHEMA_ID_PROP, Integer.toString(valueSchemaId));

    // Set data serialization model. Default is ReflectData.class.
    // The default causes reading records as SpecificRecords. This fails in reading records of type fixes, eg. Guid.
    conf.setClass("avro.serialization.data.model", GenericData.class, GenericData.class);

    if (System.getenv(HADOOP_TOKEN_FILE_LOCATION) != null) {
      conf.set(MAPREDUCE_JOB_CREDENTIALS_BINARY, System.getenv(HADOOP_TOKEN_FILE_LOCATION));
    }

    // Create job.
    JobConf job = new JobConf(conf);

    // Set the job name and the main jar.
    job.setJobName(id + ":" + jobName + "-" + topic);
    job.setJarByClass(this.getClass());

    // Setting up the Input format for the mapper
    job.setInputFormat(AvroInputFormat.class);
    AvroJob.setMapperClass(job, KafkaOutputMapper.class);
    // TODO:The job is using path-filter to check the consistency of avro file schema ,
    // but doesn't specify the path filter for the input directory of map-reduce job.
    // We need to revisit it if any failure because of this happens.
    FileInputFormat.setInputPaths(job, new Path(inputDirectory));
    AvroJob.setInputSchema(job, parsedFileSchema);

    // TODO: Consider turning speculative execution on later.
    job.setMapSpeculativeExecution(false);

    // Setting up the Output format for the mapper
    job.setOutputFormat(outputFormatClass);
    job.setNumReduceTasks(0);
    return job;
  }

  private void logPushJobProperties() {
    logger.info("Kafka URL: " + kafkaUrl);
    logger.info("Kafka Topic: " + topic);
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
      controllerClient.killOfflinePushJob(clusterName, topic);
      logger.info("Offline push job has been killed, topic: " + topic);
    }
    closeVeniceWriter();
  }

  /**
   * Pulls out the schema from an avro file, ignoring directories and prefix "."
   * This function will do the following tasks as well:
   * 1. Check whether we have sub-directory in inputDirectory or not;
   * 2. Calculate total data size of input directory;
   * 3. Check schema consistency;
   * 4. Populate key schema, value schema;
   */
  private void inspectHdfsSource(Path srcPath) throws IOException {
    parsedFileSchema = null;
    inputFileDataSize = 0;
    FileStatus[] fileStatus = fs.listStatus(srcPath, PATH_FILTER);

    if (fileStatus == null || fileStatus.length == 0) {
      throw new RuntimeException("No data found at source path: " + srcPath);
    }

    for (FileStatus status : fileStatus) {
      logger.info("Processing file " + status.getPath());
      if (status.isDirectory()) {
        // Map-reduce job will fail if the input directory has sub-directory and 'recursive' is not specified.
        throw new VeniceException("Input directory: " + srcPath.getName() +
                " should not have sub directory: " + status.getPath().getName());
      }

      Path path = status.getPath();
      Schema newSchema = getAvroFileHeader(path);
      if (newSchema == null) {
        logger.error("Was not able to grab file schema for " + path);
        throw new RuntimeException("Invalid Avro file: " + path.getName());
      } else {
        if (parsedFileSchema == null) {
          parsedFileSchema = newSchema;
        } else {
          // Need to check whether schema is consistent or not
          if (!parsedFileSchema.equals(newSchema)) {
            throw new VeniceInconsistentSchemaException("Avro schema is not consistent in the input directory: schema 1: " +
                    parsedFileSchema.toString() + ", schema 2: " + newSchema.toString());
          }
        }
      }
      inputFileDataSize += status.getLen();
    }
    // Since the job is calculating the raw data file size, which is not accurate because of compression, key/value schema and backend storage overhead,
    // we are applying this factor to provide a more reasonable estimation.
    inputFileDataSize *= INPUT_DATA_SIZE_FACTOR;

    fileSchemaString = parsedFileSchema.toString();
    Schema.Field keyField = parsedFileSchema.getField(this.keyField);
    if (keyField == null) {
      throw new VeniceSchemaFieldNotFoundException(this.keyField, "The configured key field: " + this.keyField + " is not found in the input data");
    }
    keySchemaString = keyField.schema().toString();
    Schema.Field valueField = parsedFileSchema.getField(this.valueField);
    if (valueField == null) {
      throw new VeniceSchemaFieldNotFoundException(this.valueField, "The configured value field: " + this.valueField + " is not found in the input data");
    }
    valueSchemaString = valueField.schema().toString();
  }

  private Schema getAvroFileHeader(Path path) throws IOException {
    logger.debug("path:" + path.toUri().getPath());

    GenericDatumReader<Object> avroReader = new GenericDatumReader<Object>();
    DataFileStream<Object> avroDataFileStream = null;
    Schema schema = null;
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