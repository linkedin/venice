package com.linkedin.venice.hadoop;

import com.linkedin.venice.client.VeniceWriter;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.hadoop.exceptions.VeniceInconsistentSchemaException;
import com.linkedin.venice.hadoop.exceptions.VeniceSchemaFieldNotFoundException;
import com.linkedin.venice.hadoop.exceptions.VeniceStoreCreationException;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.Arrays;

import com.linkedin.venice.exceptions.VeniceException;
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
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Iterator;
import java.util.Properties;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;


/**
 * This class sets up the Hadoop job used to push data to Kafka. The job reads the input data off HDFS and pushes it to
 * venice using Mapper tasks. The job spawns only Mappers, and there are no Reducers.
 */
public class KafkaPushJob {

  public static final String AVRO_KEY_FIELD_PROP = "avro.key.field";
  public static final String AVRO_VALUE_FIELD_PROP = "avro.value.field";
  public static final String VENICE_ROUTER_URL_PROP = "venice.router.urls";
  public static final String VENICE_CLUSTER_NAME_PROP = "cluster.name";
  public static final String VENICE_STORE_NAME_PROP = "venice.store.name";
  public static final String VENICE_STORE_OWNERS_PROP = "venice.store.owners";

  public static final String SCHEMA_STRING_PROP = "schema";
  public static final String SCHEMA_ID_PROP = "schema.id";
  public static final String KAFKA_URL_PROP = "kafka.url";
  public static final String TOPIC_PROP = "kafka.topic";
  public static final String INPUT_PATH_PROP = "input.path";
  public static final String BATCH_NUM_BYTES_PROP = "batch.num.bytes";
  public static final String PRODUCT_SPEC_JSON = "product-spec.json";

  private static Logger logger = Logger.getLogger(KafkaPushJob.class);
  private static final String HADOOP_PREFIX = "hadoop-conf.";

  public static int DEFAULT_BATCH_BYTES_SIZE = 1000000;

  // Since the job is calculating the raw data file size, which is not accurate because of compression, key/value schema and backend storage overhead,
  // we are applying this factor to provide a more reasonable estimation.
  public static final int INPUT_DATA_SIZE_FACTOR = 2;

  // Immutable state
  private final String id;
  private final String keyField;
  private final String valueField;
  private final String veniceRouterUrl;
  private final String clusterName;
  private final String storeName;
  private final String storeOwners;
  private final int batchNumBytes;
  // Whether the current job is for venice store push or just Kafka push
  private final boolean storePush;

  // Mutable state
  private String inputDirectory;
  private FileSystem fs;
  private JobConf job;
  private RunningJob runningJob;
  private String fileSchemaString;
  private String keySchemaString;
  private String valueSchemaString;
  private Schema parsedFileSchema;
  private Properties props;
  // We will override kafkaUrl and topic if it is a store push
  private String kafkaUrl;
  private String topic;
  // Total input data size, which is used to talk to controller to decide whether we have enough quota or not
  private long inputFileDataSize;

  // This main method is not called by azkaban, this is only for testing purposes.
  public static void main(String[] args) throws Exception {

    printExternalDependencies();

    OptionParser parser = new OptionParser();

    OptionSpec<String> kafkaUrlOpt =
        parser.accepts("kafka-url", "REQUIRED: Kafka URL")
            .withRequiredArg()
            .describedAs("kafka-url")
            .ofType(String.class);
    OptionSpec<String> veniceUrlOpt =
            parser.accepts("venice-router-url", "REQUIRED: comma-delimeted venice URLs")
                    .withRequiredArg()
                    .describedAs("venice-url")
                    .ofType(String.class);
    OptionSpec<String> inputPathOpt =
        parser.accepts("input-path", "REQUIRED: Input path")
            .withRequiredArg()
            .describedAs("path")
            .ofType(String.class);
    OptionSpec<String> topicOpt =
        parser.accepts("topic", "REQUIRED: topic") // is this really required?
            .withRequiredArg()
            .describedAs("topic")
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
              .describedAs("key")
              .ofType(String.class);
    OptionSpec<String> valueFieldOpt =
            parser.accepts("value-field", "REQUIRED: value field")
                    .withRequiredArg()
                    .describedAs("value")
                    .ofType(String.class);
    OptionSpec<String> queueBytesOpt =
        parser.accepts("batch-num-bytes", "Optional: max number of bytes that should be batched in a flush to kafka")
            .withRequiredArg()
            .describedAs("batch-num-bytes")
            .ofType(String.class)
            .defaultsTo(Integer.toString(1000000));

    OptionSet options = parser.parse(args);

    OptionSpec[] expectedOptions = {inputPathOpt,  keyFieldOpt, valueFieldOpt};
    validateExpectedArguments( expectedOptions, options, parser);

    // Either Kafka Url or VeniceUrl should be present
    boolean isKafkaUrlPresent = options.has(kafkaUrlOpt);
    boolean isVeniceUrlPresent = options.has(veniceUrlOpt);

    Properties props = new Properties();

    String kafkaUrl;
    String topicName;
    if(isKafkaUrlPresent && isVeniceUrlPresent) {
      String errorMessage = "Both Kafka and Venice Urls present, only one of them should be supplied. Kafka " +
              kafkaUrlOpt + " Venice " + veniceUrlOpt;
      logger.error(errorMessage);
      throw new VeniceException(errorMessage);
    } else if(isKafkaUrlPresent) {
      validateExpectedArguments( new OptionSpec[] { kafkaUrlOpt, topicOpt}, options, parser);
      kafkaUrl = options.valueOf(kafkaUrlOpt);
      topicName = options.valueOf(topicOpt);

      props.put(KAFKA_URL_PROP, kafkaUrl);
      props.put(TOPIC_PROP, topicName);

    } else if ( isVeniceUrlPresent) {
      validateExpectedArguments(new OptionSpec[] {clusterNameOpt, veniceUrlOpt, storeNameOpt, storeOwnersOpt}, options, parser);

      props.put(VENICE_CLUSTER_NAME_PROP, options.valueOf(clusterNameOpt));
      props.put(VENICE_ROUTER_URL_PROP, options.valueOf(veniceUrlOpt));
      props.put(VENICE_STORE_NAME_PROP, options.valueOf(storeNameOpt));
      props.put(VENICE_STORE_OWNERS_PROP, options.valueOf(storeOwnersOpt));
    }  else {
      String errorMessage = "At least one of the Options should be present " + kafkaUrlOpt + " Or " + veniceUrlOpt;
      logger.error(errorMessage);
      throw new VeniceException(errorMessage);
    }

    // Required options
    validateExpectedArguments(new OptionSpec[] {inputPathOpt, keyFieldOpt, valueFieldOpt}, options, parser);
    props.put(INPUT_PATH_PROP, options.valueOf(inputPathOpt));
    props.put(AVRO_KEY_FIELD_PROP, options.valueOf(keyFieldOpt));
    props.put(AVRO_VALUE_FIELD_PROP, options.valueOf(valueFieldOpt));

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
   * @param props  Property bag for the job
   * @throws Exception
   */
  public KafkaPushJob(String jobId, Properties props) throws Exception {
    this.props = props;
    this.id = jobId;
    this.clusterName = props.getProperty(VENICE_CLUSTER_NAME_PROP);
    this.veniceRouterUrl = props.getProperty(VENICE_ROUTER_URL_PROP);
    this.storeName = props.getProperty(VENICE_STORE_NAME_PROP);
    this.storeOwners = props.getProperty(VENICE_STORE_OWNERS_PROP);

    String kafkaUrl = props.getProperty(KAFKA_URL_PROP);
    String kafkaTopic = props.getProperty(TOPIC_PROP);

    boolean isKafkaUrlMissing = Utils.isNullOrEmpty(kafkaUrl);
    boolean isKafkaTopicMissing = Utils.isNullOrEmpty(kafkaTopic);

    if(isKafkaUrlMissing || isKafkaTopicMissing) {
      if (Utils.isNullOrEmpty(veniceRouterUrl)) {
        throw new VeniceException("At least one of the " + VENICE_ROUTER_URL_PROP + " or " + KAFKA_URL_PROP + " must be specified.");
      }
      if (Utils.isNullOrEmpty(storeName)) {
        throw new VeniceException(VENICE_STORE_NAME_PROP + " is required for Starting a push");
      }
      if (Utils.isNullOrEmpty(storeOwners)) {
        throw new VeniceException(VENICE_STORE_OWNERS_PROP + " is required for Starting a push");
      }
      this.storePush = true;
    } else {
      this.storePush = false;
      this.kafkaUrl = kafkaUrl;
      this.topic = kafkaTopic;
      logger.warn ("Running the job in debug mode, Controller will not be informed of a new push");
    }

    this.inputDirectory = props.getProperty(INPUT_PATH_PROP);
    this.keyField = props.getProperty(AVRO_KEY_FIELD_PROP);
    this.valueField = props.getProperty(AVRO_VALUE_FIELD_PROP);

    if (!props.containsKey(BATCH_NUM_BYTES_PROP)) {
      this.batchNumBytes = DEFAULT_BATCH_BYTES_SIZE;
    } else {
      this.batchNumBytes = Integer.parseInt(props.getProperty(BATCH_NUM_BYTES_PROP));
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
   * @throws Exception
   */
  public void run()
          throws Exception {
    logger.info("Running KafkaPushJob: " + id);

    // Create FileSystem handle, which will be shared in multiple functions
    Configuration conf = new Configuration();
    fs = FileSystem.get(conf);

    // This will try to extract the source folder if the input path has 'latest' anchor.
    Path sourcePath = getLatestPathOfInputDirectory();

    // Check Avro file schema consistency, data size
    inspectHdfsSource(sourcePath);

    // If the current job is for a store push, we need to pull Kafka Url/Topic from Venice Controller
    if (storePush) {
      populateKafkaInfoForNewStorePush();
    }
    // Log job properties
    logJobProperties();

    // Hadoop2 dev cluster provides a newer version of an avro dependency.
    // Set mapreduce.job.classloader to true to force the use of the older avro dependency.
    conf.setBoolean("mapreduce.job.classloader", true);
    logger.info("**************** mapreduce.job.classloader: " + conf.get("mapreduce.job.classloader"));

    // Setup the hadoop job
    this.job = setupHadoopJob(conf);
    JobClient jc = new JobClient(job);

    sendControlMessage(ControlMessageType.START_OF_PUSH);
    // submit the job for execution
    runningJob = jc.submitJob(job);
    logger.info("Job Tracking URL: " + runningJob.getTrackingURL());
    runningJob.waitForCompletion();

    if (!runningJob.isSuccessful()) {
      throw new RuntimeException("KafkaPushJob failed");
    }
    sendControlMessage(ControlMessageType.END_OF_PUSH);

    if (!storePush){
      logger.info("No controller provided, skipping poll of push status, job completed");
    } else {
      // Throws a VeniceException if the job completes with a failed status
      ControllerClient.pollJobStatusUntilFinished(Time.MS_PER_SECOND, 5*Time.MS_PER_MINUTE, veniceRouterUrl, clusterName, topic);
    }
  }

  private void populateKafkaInfoForNewStorePush() {
    if (!storePush) {
      return;
    }
    VersionCreationResponse response = ControllerClient.createStoreVersion(veniceRouterUrl, clusterName, storeName,
            storeOwners, inputFileDataSize, keySchemaString, valueSchemaString);
    if (response.isError()) {
      throw new VeniceStoreCreationException(storeName, "Received error from Venice Controller for new store push: " +
              response.getError());
    }
    kafkaUrl = response.getKafkaBootstrapServers();
    if (Utils.isNullOrEmpty(kafkaUrl)) {
      throw new VeniceStoreCreationException(storeName, "Venice Controller returned empty kafka url " + kafkaUrl);
    }
    topic = response.getKafkaTopic();
    if (Utils.isNullOrEmpty(topic)) {
      throw new VeniceStoreCreationException(storeName, "Venice Controller returned empty topic name " + topic);
    }
    logger.info("Used the Venice Controller to resolve to kafka url: " +
            kafkaUrl + " and topic: " + topic );
  }

  private void logJobProperties() {
    logger.info("Kafka URL: " + kafkaUrl);
    logger.info("Kafka Topic: " + topic);
    logger.info("Kafka Queue Bytes: " + batchNumBytes);
    logger.info("Input Directory: " + inputDirectory);
    logger.info("Venice Store Name: " + storeName);
    logger.info("Venice Cluster Name: " + clusterName);
    logger.info("Venice Router URL: " + veniceRouterUrl);
    logger.info("File Schema: " + fileSchemaString);
    logger.info("Avro key schema: " + keySchemaString);
    logger.info("Avro value schema: " + valueSchemaString);
    logger.info("Total input data file size: " + ((double)inputFileDataSize / 1024 / 1024) + " MB");
  }

  private void sendControlMessage(ControlMessageType controlMessageType) {
    Properties properties = new Properties();
    properties.put(KAFKA_BOOTSTRAP_SERVERS, kafkaUrl);

    VeniceProperties props = new VeniceProperties(properties);

    KafkaKeySerializer keySerializer = new KafkaKeySerializer();

    VeniceWriter<KafkaKey, byte[]> writer =
            new VeniceWriter<>(props, topic, keySerializer, new DefaultSerializer());

    writer.broadcastControlMessage(controlMessageType);
    writer.close();
    logger.info("Successfully sent " + controlMessageType.name() + " Control Message for topic '" + topic + "'.");
  }

  private static void printExternalDependencies() {
    URL specUrl = KafkaPushJob.class.getResource("/" + PRODUCT_SPEC_JSON);
    if (specUrl != null) {
      logger.info("Printing external dependencies: ");
      try {
        JsonNode node = new ObjectMapper().readTree(specUrl.openStream());
        ObjectNode externals = (ObjectNode) node.get("external");
        Iterator<String> fieldNames = externals.getFieldNames();
        while (fieldNames.hasNext()) {
          logger.info("   -" + externals.get(fieldNames.next()).getTextValue());
        }
      } catch (IOException e) {
        logger.error("Unable to parse product spec file: " + e);
      }
    } else {
      logger.error("Product spec json file not found.");
    }
  }

  // Set up the job
  private JobConf setupHadoopJob(Configuration conf) throws IOException {
    // Set maximum number of attempts per map task to 1 - avoid pushing data multiple times to venice in case the job gets into
    // the default queue and times out. Set the default value here so that it can be overridden with a user provided value below.
    conf.setInt("mapred.map.max.attempts", 1);

    // Allow overriding properties if their names start with HADOOP_PREFIX.
    for (String key : props.stringPropertyNames()) {
      String lowerCase = key.toLowerCase();
      if (lowerCase.startsWith(HADOOP_PREFIX)) {
        String overrideKey = key.substring(HADOOP_PREFIX.length());
        conf.set(overrideKey, props.getProperty(key));
      }
    }

    // These properties are actually used by the venice producer.
    conf.set(KAFKA_URL_PROP, kafkaUrl);
    conf.set(BATCH_NUM_BYTES_PROP, Integer.toString(batchNumBytes));
    conf.set(TOPIC_PROP, topic);

    // Following properties are for Avro schema validation
    conf.set(SCHEMA_STRING_PROP, fileSchemaString);
    conf.set(AVRO_KEY_FIELD_PROP, keyField);
    conf.set(AVRO_VALUE_FIELD_PROP, valueField);

    // Set data serialization model. Default is ReflectData.class.
    // The default causes reading records as SpecificRecords. This fails in reading records of type fixes, eg. Guid.
    conf.setClass("avro.serialization.data.model", GenericData.class, GenericData.class);

    // Turn speculative execution off.
    conf.setBoolean("mapred.map.tasks.speculative.execution", false);
    conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);

    if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
      conf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
    }

    // Push dependent jars to the distributed cache -- this will not be needed once the KafkaPushJob is packaged into a
    // fat jar along with its dependencies.
    if (props.containsKey("hdfs.classpath.dir")) {
      String classPathDir = props.getProperty("hdfs.classpath.dir");
      logger.info("Adding distributed cache: " + classPathDir);
      FileSystem fs = FileSystem.get(conf);
      if (fs != null) {
        FileStatus[] status = fs.listStatus(new Path(classPathDir));
        if (status != null) {
          for (int i = 0; i < status.length; ++i) {
            if (!status[i].isDir()) {
              Path path = new Path(classPathDir, status[i].getPath().getName());
              logger.info("Adding Jar to Distributed Cache Archive File:" + path);
              DistributedCache.addFileToClassPath(path, conf);
            }
          }
        } else {
          logger.info("hdfs.classpath.dir " + classPathDir + " is empty.");
        }
      } else {
        logger.info("hdfs.classpath.dir " + classPathDir + " filesystem doesn't exist");
      }
    }

    // Create job.
    JobConf job = new JobConf(conf);

    // Set the job name and the main jar.
    job.setJobName(id + ":kafkaPushJob-" + topic);
    job.setJarByClass(this.getClass());

    // Setting up the Input format for the mapper
    job.setInputFormat(AvroInputFormat.class);
    AvroJob.setMapperClass(job, KafkaOutputMapper.class);
    // TODO:The job is using path-filter to check the consistency of avro file schema ,
    // but doesn't specify the path filter for the input directory of map-reduce job.
    // We need to revisit it if any failure because of this happens.
    FileInputFormat.setInputPaths(job, new Path(inputDirectory));
    AvroJob.setInputSchema(job, parsedFileSchema);

    // Setting up the Output format for the mapper
    job.setOutputFormat(AvroKafkaOutputFormat.class);
    job.setNumReduceTasks(0);
    return job;
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
  public void cancel() throws Exception {
    // Attempting to kill job. There's a race condition, but meh. Better kill when you know it's running
    if (runningJob != null) {
      runningJob.killJob();
    }
    //TODO : Inform Venice Controller that the job is cancelled and it should be aborted.
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
      if (status.isDir()) {
        // Map-reduce job will fail if the input directory has sub-directory and 'recursive' is not specified.
        throw new RuntimeException("Input directory: " + srcPath.getName() +
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
      avroDataFileStream = new DataFileStream<Object>(hdfsInputStream, avroReader);
      schema = avroDataFileStream.getSchema();
    } finally {
      if (avroDataFileStream != null) {
        avroDataFileStream.close();
      }
    }

    return schema;
  }

  private Path getLatestPathOfInputDirectory() throws IOException{
    Path sourcePath = null;
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
      inputDirectory = sourcePath.toString();
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
  public static final PathFilter PATH_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return !path.getName().startsWith("_") && !path.getName().startsWith(".");
    }
  };
}
