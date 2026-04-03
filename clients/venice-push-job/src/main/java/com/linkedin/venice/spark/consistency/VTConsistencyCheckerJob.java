package com.linkedin.venice.spark.consistency;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.spark.SparkConstants.SPARK_CLUSTER_CONFIG;
import static com.linkedin.venice.spark.SparkConstants.SPARK_SESSION_CONF_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_CONFIGURATOR_CLASS_CONFIG;

import com.linkedin.venice.acl.VeniceComponent;
import com.linkedin.venice.hadoop.utils.VPJSSLUtils;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerContext;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import com.linkedin.venice.vpj.pubsub.input.PubSubSplitIterator;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;


/**
 * Spark job that checks VT consistency between two DCs using the Lily Pad algorithm.
 *
 * <p>Parallelizes across VT partitions: each Spark task handles one partition, consuming
 * directly from both DCs' PubSub brokers and running the Lily Pad algorithm to find
 * keys where the two leaders disagreed despite having full information.
 *
 * <p>Output is written as Parquet. Each row is one detected inconsistency with
 * full forensic context (key hash, value hashes from both DCs, position vectors, high watermarks).
 *
 * <p>Example invocation:
 * <pre>
 *   java -cp venice-push-job-all.jar \
 *     com.linkedin.venice.spark.consistency.VTConsistencyCheckerJob \
 *     /path/to/vt-consistency.properties
 * </pre>
 *
 * <p>Required config keys:
 * <ul>
 *   <li>{@value #DC0_BROKER_URL} — PubSub broker address for DC-0</li>
 *   <li>{@value #DC1_BROKER_URL} — PubSub broker address for DC-1</li>
 *   <li>{@value #VERSION_TOPIC} — version topic name to scan (e.g. {@code my-store_v3})</li>
 *   <li>{@value #OUTPUT_PATH} — output path to write Parquet results</li>
 *   <li>{@value #NUMBER_OF_REGIONS} — total number of regions in the AA topology</li>
 * </ul>
 *
 * <p>Spark configs can be passed via the {@code venice.spark.session.conf.} prefix:
 * <pre>
 *   venice.spark.cluster=yarn
 *   venice.spark.session.conf.spark.executor.memory=20g
 *   venice.spark.session.conf.spark.executor.instances=20
 * </pre>
 */
public class VTConsistencyCheckerJob {
  private static final Logger LOGGER = LogManager.getLogger(VTConsistencyCheckerJob.class);
  private static final String DEFAULT_SPARK_CLUSTER = "local[*]";

  public static final String DC0_BROKER_URL = "dc0.broker.url";
  public static final String DC1_BROKER_URL = "dc1.broker.url";
  public static final String VERSION_TOPIC = "version.topic";
  public static final String OUTPUT_PATH = "output.path";
  public static final String NUMBER_OF_REGIONS = "number.of.regions";

  /**
   * Schema of each output row. One row per detected inconsistency.
   * Nullable fields are null when the key is absent in that DC (MISSING type) or on ERROR rows.
   */
  static final StructType OUTPUT_SCHEMA = new StructType(
      new StructField[] { new StructField("version_topic", DataTypes.StringType, false, Metadata.empty()),
          new StructField("vt_partition", DataTypes.IntegerType, false, Metadata.empty()),
          new StructField("type", DataTypes.StringType, false, Metadata.empty()),
          new StructField("key_hash", DataTypes.LongType, false, Metadata.empty()),
          new StructField("dc0_value_hash", DataTypes.IntegerType, true, Metadata.empty()),
          new StructField("dc1_value_hash", DataTypes.IntegerType, true, Metadata.empty()),
          new StructField("dc0_position_vector", DataTypes.StringType, true, Metadata.empty()),
          new StructField("dc1_position_vector", DataTypes.StringType, true, Metadata.empty()),
          new StructField("dc0_high_watermark", DataTypes.StringType, true, Metadata.empty()),
          new StructField("dc1_high_watermark", DataTypes.StringType, true, Metadata.empty()),
          new StructField("dc0_logical_ts", DataTypes.LongType, true, Metadata.empty()),
          new StructField("dc1_logical_ts", DataTypes.LongType, true, Metadata.empty()),
          new StructField("dc0_vt_position", DataTypes.StringType, true, Metadata.empty()),
          new StructField("dc1_vt_position", DataTypes.StringType, true, Metadata.empty()) });

  public static void main(String[] args) {
    if (args.length != 1) {
      Utils.exit(
          "USAGE: java -cp venice-push-job-all.jar "
              + "com.linkedin.venice.spark.consistency.VTConsistencyCheckerJob <config_file_path>");
    }
    Properties jobProps = new Properties();
    try (FileReader reader = new FileReader(args[0])) {
      jobProps.load(reader);
    } catch (IOException e) {
      e.printStackTrace();
      Utils.exit("Unable to read config file: " + args[0]);
    }
    validateRequiredProps(jobProps);
    run(jobProps);
  }

  /**
   * Entry point for both CLI and tests. Accepts a {@link Properties} object so tests can
   * configure the job programmatically without going through arg parsing.
   */
  public static void run(Properties jobProps) {
    String dc0BrokerUrl = jobProps.getProperty(DC0_BROKER_URL);
    String dc1BrokerUrl = jobProps.getProperty(DC1_BROKER_URL);
    String versionTopic = jobProps.getProperty(VERSION_TOPIC);
    String outputPath = jobProps.getProperty(OUTPUT_PATH);
    int numberOfRegions = Integer.parseInt(jobProps.getProperty(NUMBER_OF_REGIONS));

    PubSubTopicRepository topicRepository = new PubSubTopicRepository();
    VeniceProperties dc0Props = brokerProps(dc0BrokerUrl, jobProps);
    VeniceProperties dc1Props = brokerProps(dc1BrokerUrl, jobProps);
    PubSubTopic topic = topicRepository.getTopic(versionTopic);

    // Batch-fetch start/end positions for all partitions on the driver using TopicManager (with retry)
    PubSubPositionTypeRegistry positionTypeRegistry = PubSubPositionTypeRegistry.fromPropertiesOrDefault(dc0Props);
    Map<Integer, PubSubPartitionSplit> dc0Splits;
    Map<Integer, PubSubPartitionSplit> dc1Splits;
    int partitionCount;
    try (TopicManager dc0TopicManager = createTopicManager(dc0Props, topicRepository, positionTypeRegistry)) {
      partitionCount = dc0TopicManager.getPartitionCount(topic);
      dc0Splits = batchFetchSplits(dc0TopicManager, topic, topicRepository, partitionCount);
    }
    try (TopicManager dc1TopicManager = createTopicManager(dc1Props, topicRepository, positionTypeRegistry)) {
      dc1Splits = batchFetchSplits(dc1TopicManager, topic, topicRepository, partitionCount);
    }
    LOGGER.info(
        "Planned {} splits for topic {} from DC0={} and DC1={}",
        partitionCount,
        versionTopic,
        dc0BrokerUrl,
        dc1BrokerUrl);

    // Create SparkSession: venice.spark.cluster sets the master, venice.spark.session.conf.* passes through
    String appName = "VTConsistencyChecker-" + versionTopic;
    VeniceProperties veniceProps = new VeniceProperties(jobProps);
    SparkConf sparkConf = new SparkConf().set("spark.sql.caseSensitive", "true").set("spark.jobGroup.id", appName);
    SparkSession.Builder sparkSessionBuilder = SparkSession.builder()
        .appName(appName)
        .config(sparkConf)
        .master(veniceProps.getString(SPARK_CLUSTER_CONFIG, DEFAULT_SPARK_CLUSTER));
    for (String key: jobProps.stringPropertyNames()) {
      if (key.toLowerCase().startsWith(SPARK_SESSION_CONF_PREFIX)) {
        sparkSessionBuilder.config(key.substring(SPARK_SESSION_CONF_PREFIX.length()), jobProps.getProperty(key));
      }
    }
    SparkSession spark = sparkSessionBuilder.getOrCreate();

    try {
      List<Integer> partitions = IntStream.range(0, partitionCount).boxed().collect(Collectors.toList());

      LongAccumulator partitionsProcessed = spark.sparkContext().longAccumulator("partitionsProcessed");
      LongAccumulator partitionsWithErrors = spark.sparkContext().longAccumulator("partitionsWithErrors");

      Dataset<Row> inconsistencies = spark.createDataset(partitions, Encoders.INT())
          .flatMap(
              (FlatMapFunction<Integer, Row>) p -> findInconsistenciesForPartition(
                  dc0Splits.get(p),
                  dc1Splits.get(p),
                  jobProps,
                  numberOfRegions,
                  partitionsProcessed,
                  partitionsWithErrors),
              RowEncoder.apply(OUTPUT_SCHEMA));

      inconsistencies.write().mode(SaveMode.ErrorIfExists).parquet(outputPath);

      LOGGER.info(
          "VT consistency check complete. topic={} partitions={} processed={} errors={} output={}",
          versionTopic,
          partitionCount,
          partitionsProcessed.value(),
          partitionsWithErrors.value(),
          outputPath);

      if (partitionsWithErrors.value() > 0) {
        throw new RuntimeException(
            partitionsWithErrors.value() + " partition(s) failed during scan of topic " + versionTopic
                + ". Check executor logs for details.");
      }
    } finally {
      spark.stop();
    }
  }

  /**
   * Runs on a Spark executor. Builds snapshots from both DCs for a single partition,
   * runs the lily-pad algorithm, and returns output rows.
   */
  static Iterator<Row> findInconsistenciesForPartition(
      PubSubPartitionSplit dc0Split,
      PubSubPartitionSplit dc1Split,
      Properties jobProps,
      int numberOfRegions,
      LongAccumulator partitionsProcessed,
      LongAccumulator partitionsWithErrors) {
    int partition = dc0Split.getPartitionNumber();
    String versionTopic = dc0Split.getTopicName();
    try {
      VeniceProperties baseProps = setupSSLForExecutor(new VeniceProperties(jobProps));
      VeniceProperties dc0Props = brokerProps(jobProps.getProperty(DC0_BROKER_URL), baseProps.toProperties());
      VeniceProperties dc1Props = brokerProps(jobProps.getProperty(DC1_BROKER_URL), baseProps.toProperties());

      PubSubTopicRepository topicRepository = new PubSubTopicRepository();
      PubSubPositionTypeRegistry positionTypeRegistry = PubSubPositionTypeRegistry.fromPropertiesOrDefault(dc0Props);
      PubSubPositionDeserializer positionDeserializer = new PubSubPositionDeserializer(positionTypeRegistry);

      try (TopicManager dc0TopicManager = createTopicManager(dc0Props, topicRepository, positionTypeRegistry);
          TopicManager dc1TopicManager = createTopicManager(dc1Props, topicRepository, positionTypeRegistry)) {

        LilyPadUtils.Snapshot<ComparablePubSubPosition> dc0Snapshot;
        try (PubSubSplitIterator dc0Iterator = new PubSubSplitIterator(
            createConsumer(dc0Props, topicRepository, "dc0-checker-p" + partition),
            dc0Split,
            false)) {
          dc0Snapshot =
              LilyPadSnapshotBuilder.buildSnapshot(dc0Iterator, dc0TopicManager, positionDeserializer, numberOfRegions);
        }

        LilyPadUtils.Snapshot<ComparablePubSubPosition> dc1Snapshot;
        try (PubSubSplitIterator dc1Iterator = new PubSubSplitIterator(
            createConsumer(dc1Props, topicRepository, "dc1-checker-p" + partition),
            dc1Split,
            false)) {
          dc1Snapshot =
              LilyPadSnapshotBuilder.buildSnapshot(dc1Iterator, dc1TopicManager, positionDeserializer, numberOfRegions);
        }

        List<LilyPadUtils.Inconsistency<ComparablePubSubPosition>> found =
            LilyPadUtils.findInconsistencies(dc0Snapshot, dc1Snapshot);

        partitionsProcessed.add(1);
        return found.stream().map(inc -> toRow(inc, versionTopic, partition)).iterator();
      }
    } catch (Exception e) {
      partitionsWithErrors.add(1);
      LOGGER.error("Failed to process partition {} of topic {}", partition, versionTopic, e);
      Row errorRow = RowFactory
          .create(versionTopic, partition, "ERROR", 0L, null, null, null, null, null, null, null, null, null, null);
      return java.util.Collections.singletonList(errorRow).iterator();
    }
  }

  /** Converts one {@link LilyPadUtils.Inconsistency} to a Spark output row. */
  static Row toRow(LilyPadUtils.Inconsistency<ComparablePubSubPosition> inc, String versionTopic, int partition) {
    LilyPadUtils.KeyRecord<ComparablePubSubPosition> dc0 = inc.dc0Record;
    LilyPadUtils.KeyRecord<ComparablePubSubPosition> dc1 = inc.dc1Record;
    return RowFactory.create(
        versionTopic,
        partition,
        inc.type.name(),
        inc.keyHash,
        dc0 != null ? dc0.valueHash : null,
        dc1 != null ? dc1.valueHash : null,
        dc0 != null ? dc0.upstreamRTPosition.toString() : null,
        dc1 != null ? dc1.upstreamRTPosition.toString() : null,
        dc0 != null ? dc0.highWatermark.toString() : null,
        dc1 != null ? dc1.highWatermark.toString() : null,
        dc0 != null ? dc0.logicalTimestamp : null,
        dc1 != null ? dc1.logicalTimestamp : null,
        dc0 != null ? dc0.vtPosition.toString() : null,
        dc1 != null ? dc1.vtPosition.toString() : null);
  }

  /**
   * Batch-fetches beginning and end positions for all partitions of a topic using TopicManager
   * (with retry), then constructs one {@link PubSubPartitionSplit} per partition.
   */
  static Map<Integer, PubSubPartitionSplit> batchFetchSplits(
      TopicManager topicManager,
      PubSubTopic topic,
      PubSubTopicRepository topicRepository,
      int partitionCount) {
    Map<PubSubTopicPartition, PubSubPosition> startPositions = topicManager.getStartPositionsForTopicWithRetries(topic);
    Map<PubSubTopicPartition, PubSubPosition> endPositions = topicManager.getEndPositionsForTopicWithRetries(topic);

    Map<Integer, PubSubPartitionSplit> splits = new HashMap<>(partitionCount);
    for (int p = 0; p < partitionCount; p++) {
      PubSubTopicPartition tp = new PubSubTopicPartitionImpl(topic, p);
      PubSubPosition start = startPositions.get(tp);
      PubSubPosition end = endPositions.get(tp);
      long estimatedRecords = Math.max(0, topicManager.diffPosition(tp, end, start));
      splits.put(p, new PubSubPartitionSplit(topicRepository, tp, start, end, estimatedRecords, 0, 0));
    }
    return splits;
  }

  private static PubSubConsumerAdapter createConsumer(
      VeniceProperties props,
      PubSubTopicRepository topicRepository,
      String consumerName) {
    return PubSubClientsFactory.createConsumerFactory(props)
        .create(
            new PubSubConsumerAdapterContext.Builder().setPubSubBrokerAddress(props.getString(KAFKA_BOOTSTRAP_SERVERS))
                .setVeniceProperties(props)
                .setPubSubTopicRepository(topicRepository)
                .setPubSubMessageDeserializer(PubSubMessageDeserializer.createDefaultDeserializer())
                .setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry.fromPropertiesOrDefault(props))
                .setConsumerName(consumerName)
                .build());
  }

  private static TopicManager createTopicManager(
      VeniceProperties props,
      PubSubTopicRepository topicRepository,
      PubSubPositionTypeRegistry positionTypeRegistry) {
    String brokerUrl = props.getString(KAFKA_BOOTSTRAP_SERVERS);
    TopicManagerContext context = new TopicManagerContext.Builder().setPubSubPropertiesSupplier(k -> props)
        .setPubSubConsumerAdapterFactory(PubSubClientsFactory.createConsumerFactory(props))
        .setPubSubAdminAdapterFactory(PubSubClientsFactory.createAdminFactory(props))
        .setPubSubTopicRepository(topicRepository)
        .setPubSubPositionTypeRegistry(positionTypeRegistry)
        .setTopicMetadataFetcherConsumerPoolSize(1)
        .setTopicMetadataFetcherThreadPoolSize(1)
        .setVeniceComponent(VeniceComponent.UNSPECIFIED)
        .build();
    // The repository is a thin wrapper (ConcurrentHashMap) with no resources beyond the TopicManagers it holds.
    // The caller closes the returned TopicManager directly. Same pattern as PubSubSplitPlanner.
    return new TopicManagerRepository(context, brokerUrl).getLocalTopicManager();
  }

  private static VeniceProperties setupSSLForExecutor(VeniceProperties config) {
    if (!config.containsKey(SSL_CONFIGURATOR_CLASS_CONFIG)) {
      return config;
    }
    try {
      Properties sslProps = VPJSSLUtils.getSslProperties(config);
      Properties merged = config.toProperties();
      merged.putAll(sslProps);
      return new VeniceProperties(merged);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to setup SSL for executor-side consumer creation. "
              + "Ensure the Hadoop token file is accessible and SSL certificates are valid. "
              + "SSL configurator class: " + config.getString(SSL_CONFIGURATOR_CLASS_CONFIG),
          e);
    }
  }

  private static VeniceProperties brokerProps(String brokerUrl, Properties allProps) {
    Properties p = new Properties();
    p.putAll(allProps);
    p.setProperty(KAFKA_BOOTSTRAP_SERVERS, brokerUrl);
    return new VeniceProperties(p);
  }

  private static void validateRequiredProps(Properties props) {
    for (String required: new String[] { DC0_BROKER_URL, DC1_BROKER_URL, VERSION_TOPIC, OUTPUT_PATH,
        NUMBER_OF_REGIONS }) {
      if (!props.containsKey(required)) {
        Utils.exit("Missing required config: " + required);
      }
    }
  }
}
