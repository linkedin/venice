package com.linkedin.venice.hadoop;

import com.linkedin.venice.client.VeniceWriter;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.JobStatusQueryResponse;
import com.linkedin.venice.controllerapi.StoreCreationResponse;
import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.message.OperationType;
import com.linkedin.venice.serialization.DefaultSerializer;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.utils.Props;
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
import java.net.URI;
import java.net.URL;
import java.util.Iterator;
import java.util.Properties;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.clusterSpecificProperties;


/**
 * This class sets up the Hadoop job used to push data to Kafka. The job reads the input data off HDFS and pushes it to
 * venice using Mapper tasks. The job spawns only Mappers, and there are no Reducers.
 */
public class KafkaPushJob {

  public static final String AVRO_KEY_FIELD_PROP = "avro.key.field";
  public static final String AVRO_VALUE_FIELD_PROP = "avro.value.field";
  public static final String VENICE_CONTROLLER_URL_PROP = "venice.controller.url";
  public static final String VENICE_STORE_NAME_PROP = "venice.store.name";

  public static final String SCHEMA_STRING_PROP = "schema";
  public static final String SCHEMA_ID_PROP = "schema.id";
  public static final String KAFKA_URL_PROP = "kafka.url";
  public static final String TOPIC_PROP = "kafka.topic";
  public static final String INPUT_PATH_PROP = "input.path";
  public static final String NAME_NODE_PROP = "name.node";
  public static final String BATCH_NUM_BYTES_PROP = "batch.num.bytes";
  public static final String PRODUCT_SPEC_JSON = "product-spec.json";

  private static Logger logger = Logger.getLogger(KafkaPushJob.class);
  private static final String HADOOP_PREFIX = "hadoop-conf.";

  public static int DEFAULT_BATCH_BYTES_SIZE = 1000000;

  // Immutable state
  private final String id;
  private final String nameNode;
  private final String keyField;
  private final String valueField;
  private final String veniceControllerUrl;
  private final String storeName;
  private final String kafkaUrl;
  private final String topic;
  private final int batchNumBytes;

  // Mutable state
  private String inputDirectory;
  private FileSystem fs;
  private JobConf job;
  private RunningJob runningJob;
  private String fileSchemaString;
  private Schema parsedFileSchema;
  private Properties props;

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
            parser.accepts("venice-controller-url", "REQUIRED: venice URL")
                    .withRequiredArg()
                    .describedAs("venice-url")
                    .ofType(String.class);
    OptionSpec<String> inputPathOpt =
        parser.accepts("input-path", "REQUIRED: Input path")
            .withRequiredArg()
            .describedAs("path")
            .ofType(String.class);
    OptionSpec<String> nameNodeOpt =
        parser.accepts("name-node", "REQUIRED: Name node")
            .withRequiredArg()
            .describedAs("name-node")
            .ofType(String.class);
    OptionSpec<String> topicOpt =
        parser.accepts("topic", "REQUIRED: topic")
            .withRequiredArg()
            .describedAs("topic")
            .ofType(String.class);
    OptionSpec<String> storeNameOpt =
            parser.accepts("store-name", "REQUIRED: store name")
                    .withRequiredArg()
                    .describedAs("store-name")
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
      String veniceUrl = options.valueOf(veniceUrlOpt);
      String storeName = options.valueOf(storeNameOpt);

      props.put(VENICE_CONTROLLER_URL_PROP, veniceUrl);
      props.put(VENICE_STORE_NAME_PROP, storeName);
    }  else {
      String errorMessage = "At least one of the Options should be present " + kafkaUrlOpt + " Or " + veniceUrlOpt;
      logger.error(errorMessage);
      throw new VeniceException(errorMessage);
    }


    props.put(INPUT_PATH_PROP, options.valueOf(inputPathOpt));
    props.put(BATCH_NUM_BYTES_PROP, options.valueOf(queueBytesOpt));
    props.put(AVRO_KEY_FIELD_PROP, options.valueOf(keyFieldOpt));
    props.put(AVRO_VALUE_FIELD_PROP, options.valueOf(valueFieldOpt));

    String nameNodeValue = options.valueOf(nameNodeOpt);
    if(nameNodeValue != null && nameNodeValue.length() > 0) {
      props.put(NAME_NODE_PROP, nameNodeValue);
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

  public static StoreCreationResponse initiateNewStorePush(String veniceUrl, String storeName) throws Exception {
    // TODO : compute the store size properly and populate the owner correctly.
    final String DUMMY_OWNER = "dev";
    final int DUMMY_SIZE_MB = 100;

    StoreCreationResponse response = ControllerClient.createStoreVersion(veniceUrl, storeName,
            DUMMY_OWNER, DUMMY_SIZE_MB);
    String kafkaUrl = response.getKafkaBootstrapServers();
    if(Utils.isNullOrEmpty(kafkaUrl)) {
      throw new VeniceException("Returned empty kafka url " + kafkaUrl);
    }

    String kafkaTopic = response.getKafkaTopic();
    if(Utils.isNullOrEmpty(kafkaTopic)) {
      throw new VeniceException("Returned empty topic name " + kafkaTopic);
    }

    return response;
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
    this.veniceControllerUrl = props.getProperty(VENICE_CONTROLLER_URL_PROP);
    this.storeName = props.getProperty(VENICE_STORE_NAME_PROP);

    String kafkaUrl = props.getProperty(KAFKA_URL_PROP);
    String kafkaTopic = props.getProperty(TOPIC_PROP);

    boolean isKafkaUrlMissing = Utils.isNullOrEmpty(kafkaUrl);
    boolean isKafkaTopicMissing = Utils.isNullOrEmpty(kafkaTopic);

    if(isKafkaUrlMissing || isKafkaTopicMissing) {
      if (Utils.isNullOrEmpty(veniceControllerUrl)) {
        throw new VeniceException("At least one of the " + VENICE_CONTROLLER_URL_PROP + " or " + KAFKA_URL_PROP + " must be specified.");
      }
      if (Utils.isNullOrEmpty(storeName)) {
        throw new VeniceException(VENICE_STORE_NAME_PROP + " is required for Starting a push");
      }

      StoreCreationResponse response = initiateNewStorePush(this.veniceControllerUrl, this.storeName);
      kafkaUrl = response.getKafkaBootstrapServers();
      kafkaTopic = response.getKafkaTopic();
      logger.info("Used the Venice Controller " + veniceControllerUrl + " to resolve to kafka url " + kafkaUrl + " topic " + kafkaTopic );
    } else {
      logger.warn ("Running the job in debug mode, Controller will not be informed of a new push");
    }

    this.kafkaUrl = kafkaUrl;
    this.topic = kafkaTopic;

    this.inputDirectory = props.getProperty(INPUT_PATH_PROP);
    this.nameNode = props.getProperty(NAME_NODE_PROP);

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
    logger.info("Kafka URL: " + kafkaUrl);
    logger.info("Kafka Topic: " + topic);
    logger.info("Kafka Queue Bytes: " + batchNumBytes);
    logger.info("Input Directory: " + inputDirectory);
    logger.info("Name Node: " + nameNode);
    logger.info("Venice Store Name: " + storeName);
    logger.info("Venice Controller URL: " + veniceControllerUrl);

    // Check that input path exists in HDFS
    URI nameNodeUri = null;
    if(nameNode != null ) {
      nameNodeUri = new URI(nameNode);
    }
    // Before we run, we check the schema of the first file.
    Configuration conf = new Configuration();
    if(nameNodeUri != null) {
      fs = FileSystem.get(nameNodeUri, conf);
    } else {
      fs = FileSystem.getLocal(conf);
    }

    Path sourcePath;
    if (inputDirectory.endsWith("#LATEST") || inputDirectory.endsWith("#LATEST/")) {
      int poundSign = inputDirectory.lastIndexOf('#');
      sourcePath = new Path(inputDirectory.substring(0, poundSign));
      sourcePath = getLatestPath(sourcePath, fs);
      inputDirectory = sourcePath.toString();
      logger.info("Actual Input Directory: " + inputDirectory);
    } else {
      sourcePath = new Path(inputDirectory);
    }

    printExternalDependencies();

    // If overridden using config, register schema.
    fileSchemaString = getSchemaFromHDFSSource(sourcePath);
    parsedFileSchema = Schema.parse(fileSchemaString.toString());
    logger.info("File Schema: " + fileSchemaString);

    // Hadoop2 dev cluster provides a newer version of an avro dependency.
    // Set mapreduce.job.classloader to true to force the use of the older avro dependency.
    conf.setBoolean("mapreduce.job.classloader", true);
    logger.info("**************** mapreduce.job.classloader: " + conf.get("mapreduce.job.classloader"));


    // Setup the hadoop job
    this.job = setupHadoopJob(conf);
    JobClient jc = new JobClient(job);

    // TODO: Set the jobId to epoch for now. Figure out how to get unique jobId
    // for each of the pushes.
    long jobId = System.currentTimeMillis();
    sendControlMessage(OperationType.BEGIN_OF_PUSH, jobId);
    // submit the job for execution
    runningJob = jc.submitJob(job);
    logger.info("Job Tracking URL: " + runningJob.getTrackingURL());
    runningJob.waitForCompletion();

    if (!runningJob.isSuccessful()) {
      throw new RuntimeException("KafkaPushJob failed");
    }
    sendControlMessage(OperationType.END_OF_PUSH, jobId);

    if (Utils.isNullOrEmpty(veniceControllerUrl)){
      logger.info("No controller provided, skipping poll of push status, job completed");
    } else {
      // Throws a VeniceException if the job completes with a failed status
      ControllerClient.pollJobStatusUntilFinished(Time.MS_PER_SECOND, 5*Time.MS_PER_MINUTE, veniceControllerUrl, topic);
    }
  }

  private void sendControlMessage (OperationType opType, long jobId) {
    Props props = new Props()
            .with(KAFKA_BOOTSTRAP_SERVERS, kafkaUrl);

    KafkaKeySerializer keySerializer = new KafkaKeySerializer();

    VeniceWriter<KafkaKey, byte[]> writer =
            new VeniceWriter<>(props, topic, keySerializer, new DefaultSerializer());

    writer.writeControlMessage(opType, jobId);
    writer.close();
    logger.info("Successfully sent control message for topic " + topic + " Op " + opType);
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
   * Pulls out the schema from an avro file. The first avro file it finds, ignoring directories and prefix "."
   */
  private String getSchemaFromHDFSSource(Path srcPath) throws IOException {
    Schema newSchema = null;

    FileStatus[] fileStatus = fs.listStatus(srcPath, PATH_FILTER);

    if (fileStatus == null || fileStatus.length == 0) {
      throw new RuntimeException("No data found at source path: " + srcPath);
    }

    for (FileStatus status : fileStatus) {
      System.out.println("Processing file " + status.getPath());
      if (status.isDir()) {
        continue;
      }

      Path path = status.getPath();
      newSchema = getAvroFileHeader(fs, path);
      if (newSchema == null) {
        logger.error("Was not able to grab file schema for " + path);
        throw new RuntimeException("Invalid Avro file.");
      } else {
        break;
      }
    }

    return newSchema.toString();
  }

  private Schema getAvroFileHeader(FileSystem fs, Path path) throws IOException {
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
