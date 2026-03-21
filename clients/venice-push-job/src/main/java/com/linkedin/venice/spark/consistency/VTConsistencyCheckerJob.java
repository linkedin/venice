package com.linkedin.venice.spark.consistency;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_CONFIGURATOR_CLASS_CONFIG;

import com.linkedin.venice.hadoop.utils.VPJSSLUtils;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import com.linkedin.venice.vpj.pubsub.input.PubSubSplitIterator;
import java.time.Duration;
import java.util.ArrayList;
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
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;


/**
 * Spark job that checks VT consistency between two DCs using the lily-pad algorithm.
 *
 * <p>Parallelizes across VT partitions: each Spark task handles one partition, consuming
 * directly from both DCs' Kafka brokers and running {@link VTConsistencyChecker} to find
 * keys where the two leaders disagreed despite having full information.
 *
 * <p>Output is written to HDFS as Parquet. Each row is one detected inconsistency with
 * full forensic context (key, values from both DCs, offset vectors, high watermarks).
 *
 * <p>Required config keys:
 * <ul>
 *   <li>{@value #DC0_BROKER_URL} — Kafka broker address for DC-0</li>
 *   <li>{@value #DC1_BROKER_URL} — Kafka broker address for DC-1</li>
 *   <li>{@value #VERSION_TOPIC} — version topic name to scan (e.g. {@code my-store_v3})</li>
 *   <li>{@value #OUTPUT_PATH} — HDFS path to write Parquet output</li>
 * </ul>
 *
 * <p>Any additional {@code key=value} arguments are forwarded verbatim to both Kafka consumers,
 * so the caller is responsible for supplying any required SSL or auth properties.
 *
 * <p>Example CLI invocation:
 * <pre>
 *   spark-submit \
 *     --class com.linkedin.venice.spark.consistency.VTConsistencyCheckerJob \
 *     --master yarn \
 *     --deploy-mode cluster \
 *     --executor-memory 8g \
 *     --num-executors 20 \
 *     venice-push-job-*-all.jar \
 *     dc0.broker.url=dc0-kafka-broker:9092 \
 *     dc1.broker.url=dc1-kafka-broker:9092 \
 *     version.topic=my-store_v5 \
 *     output.path=hdfs:///user/mdamarap/vt-consistency/my-store_v5 \
 *     ssl.keystore.location=/path/to/keystore.jks \
 *     ssl.keystore.password=secret \
 *     ssl.key.password=secret \
 *     ssl.truststore.location=/path/to/truststore.jks \
 *     ssl.truststore.password=secret
 * </pre>
 *
 * <p>To inspect the output after the job completes:
 * <pre>
 *   spark-shell
 *   > spark.read.parquet("hdfs:///user/mdamarap/vt-consistency/my-store_v5").show(20, false)
 * </pre>
 */
public class VTConsistencyCheckerJob {
  private static final Logger LOGGER = LogManager.getLogger(VTConsistencyCheckerJob.class);

  public static final String DC0_BROKER_URL = "dc0.broker.url";
  public static final String DC1_BROKER_URL = "dc1.broker.url";
  public static final String VERSION_TOPIC = "version.topic";
  public static final String OUTPUT_PATH = "output.path";

  /**
   * Schema of each output row. One row per detected inconsistency.
   * Nullable fields are null when the key is absent in that DC (MISSING type).
   */
  static final StructType OUTPUT_SCHEMA = new StructType(
      new StructField[] { new StructField("version_topic", DataTypes.StringType, false, Metadata.empty()),
          new StructField("vt_partition", DataTypes.IntegerType, false, Metadata.empty()),
          new StructField("type", DataTypes.StringType, false, Metadata.empty()),
          new StructField("key", DataTypes.StringType, false, Metadata.empty()),
          new StructField("dc0_value", DataTypes.StringType, true, Metadata.empty()),
          new StructField("dc1_value", DataTypes.StringType, true, Metadata.empty()),
          new StructField("dc0_offset_vector", new ArrayType(DataTypes.LongType, false), true, Metadata.empty()),
          new StructField("dc1_offset_vector", new ArrayType(DataTypes.LongType, false), true, Metadata.empty()),
          new StructField("dc0_high_watermark", new ArrayType(DataTypes.LongType, false), true, Metadata.empty()),
          new StructField("dc1_high_watermark", new ArrayType(DataTypes.LongType, false), true, Metadata.empty()),
          new StructField("dc0_logical_ts", DataTypes.LongType, true, Metadata.empty()),
          new StructField("dc1_logical_ts", DataTypes.LongType, true, Metadata.empty()),
          new StructField("dc0_vt_position", DataTypes.StringType, true, Metadata.empty()),
          new StructField("dc1_vt_position", DataTypes.StringType, true, Metadata.empty()) });

  public static void main(String[] args) {
    Properties jobProps = parseArgs(args);
    run(jobProps);
    SparkSession.builder().getOrCreate().stop();
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

    // Batch-fetch start/end positions for all partitions on the driver (3 broker calls for DC0,
    // 2 for DC1) rather than 2 per-executor calls per partition per DC (4N calls total).
    PubSubTopicRepository topicRepository = new PubSubTopicRepository();
    VeniceProperties dc0Props = brokerProps(dc0BrokerUrl, jobProps);
    VeniceProperties dc1Props = brokerProps(dc1BrokerUrl, jobProps);
    PubSubTopic topic = topicRepository.getTopic(versionTopic);

    Map<Integer, PubSubPartitionSplit> dc0Splits;
    Map<Integer, PubSubPartitionSplit> dc1Splits;
    int partitionCount;
    try (PubSubConsumerAdapter dc0Consumer = createConsumer(dc0Props, topicRepository, "split-planner-dc0")) {
      partitionCount = dc0Consumer.partitionsFor(topic).size();
      dc0Splits = batchFetchSplits(dc0Consumer, topic, topicRepository, partitionCount);
    }
    try (PubSubConsumerAdapter dc1Consumer = createConsumer(dc1Props, topicRepository, "split-planner-dc1")) {
      dc1Splits = batchFetchSplits(dc1Consumer, topic, topicRepository, partitionCount);
    }
    LOGGER.info(
        "Planned {} splits for topic {} from DC0={} and DC1={}",
        partitionCount,
        versionTopic,
        dc0BrokerUrl,
        dc1BrokerUrl);

    String appName = "VTConsistencyChecker-" + versionTopic;
    SparkConf sparkConf = new SparkConf()
        // Venice keys are case-sensitive; prevent Spark from normalizing column names
        .set("spark.sql.caseSensitive", "true")
        // Groups all tasks under one cancellable unit in the Spark UI
        .set("spark.jobGroup.id", appName);
    SparkSession spark = SparkSession.builder().appName(appName).config(sparkConf).getOrCreate();

    // One integer per VT partition — Spark distributes these across workers
    List<Integer> partitions = IntStream.range(0, partitionCount).boxed().collect(Collectors.toList());

    // Accumulators: executor increments these; driver reads the totals after actions complete.
    LongAccumulator partitionsProcessed = spark.sparkContext().longAccumulator("partitionsProcessed");
    LongAccumulator partitionsWithErrors = spark.sparkContext().longAccumulator("partitionsWithErrors");

    Dataset<Row> inconsistencies = spark.createDataset(partitions, Encoders.INT())
        .flatMap(
            (FlatMapFunction<Integer, Row>) p -> findInconsistenciesForPartition(
                dc0Splits.get(p),
                dc1Splits.get(p),
                jobProps,
                partitionsProcessed,
                partitionsWithErrors),
            RowEncoder.apply(OUTPUT_SCHEMA));

    // Cache so the dataset is not recomputed for both write() and count()
    inconsistencies.cache();

    inconsistencies.write().mode(SaveMode.ErrorIfExists).parquet(outputPath);

    long total = inconsistencies.count();
    LOGGER.info(
        "VT consistency check complete. topic={} partitions={} processed={} errors={} totalInconsistencies={} output={}",
        versionTopic,
        partitionCount,
        partitionsProcessed.value(),
        partitionsWithErrors.value(),
        total,
        outputPath);

    inconsistencies.unpersist();

    if (partitionsWithErrors.value() > 0) {
      throw new RuntimeException(
          partitionsWithErrors.value() + " partition(s) failed during scan of topic " + versionTopic
              + ". Check ERROR rows in output or executor logs for details.");
    }
  }

  /**
   * Runs on a Spark executor. Receives pre-built splits (with positions fetched on the driver),
   * creates consumers, builds snapshots, runs the lily-pad algorithm, and returns output rows.
   */
  private static Iterator<Row> findInconsistenciesForPartition(
      PubSubPartitionSplit dc0Split,
      PubSubPartitionSplit dc1Split,
      Properties jobProps,
      LongAccumulator partitionsProcessed,
      LongAccumulator partitionsWithErrors) {
    int partition = dc0Split.getPartitionNumber();
    String versionTopic = dc0Split.getTopicName();
    try {
      VeniceProperties baseProps = setupSSLForExecutor(new VeniceProperties(jobProps));
      VeniceProperties dc0Props = brokerProps(jobProps.getProperty(DC0_BROKER_URL), baseProps.toProperties());
      VeniceProperties dc1Props = brokerProps(jobProps.getProperty(DC1_BROKER_URL), baseProps.toProperties());

      PubSubTopicRepository topicRepository = new PubSubTopicRepository();

      VTConsistencyChecker.Snapshot dc0Snapshot;
      try (PubSubSplitIterator dc0Iterator = new PubSubSplitIterator(
          createConsumer(dc0Props, topicRepository, "dc0-checker-p" + partition),
          dc0Split,
          false)) {
        dc0Snapshot = VTConsistencyChecker.buildSnapshot(dc0Iterator);
      }

      VTConsistencyChecker.Snapshot dc1Snapshot;
      try (PubSubSplitIterator dc1Iterator = new PubSubSplitIterator(
          createConsumer(dc1Props, topicRepository, "dc1-checker-p" + partition),
          dc1Split,
          false)) {
        dc1Snapshot = VTConsistencyChecker.buildSnapshot(dc1Iterator);
      }

      List<VTConsistencyChecker.Inconsistency> found =
          VTConsistencyChecker.findInconsistencies(dc0Snapshot, dc1Snapshot);

      partitionsProcessed.add(1);
      return found.stream().map(inc -> toRow(inc, versionTopic, partition)).iterator();
    } catch (Exception e) {
      partitionsWithErrors.add(1);
      // Emit a sentinel ERROR row so the failure is visible in the Parquet output and
      // queryable after the job completes, rather than silently dropping the partition.
      LOGGER.error("Failed to process partition {} of topic {}", partition, versionTopic, e);
      Row errorRow = RowFactory.create(
          versionTopic,
          partition,
          "ERROR",
          e.getMessage(),
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null);
      return java.util.Collections.singletonList(errorRow).iterator();
    }
  }

  /**
   * Batch-fetches beginning and end positions for all partitions of a topic in 2 broker calls,
   * then constructs one {@link PubSubPartitionSplit} per partition.
   */
  private static Map<Integer, PubSubPartitionSplit> batchFetchSplits(
      PubSubConsumerAdapter consumer,
      PubSubTopic topic,
      PubSubTopicRepository topicRepository,
      int partitionCount) {
    List<PubSubTopicPartition> allPartitions = new ArrayList<>(partitionCount);
    for (int p = 0; p < partitionCount; p++) {
      allPartitions.add(new PubSubTopicPartitionImpl(topic, p));
    }
    Duration timeout = Duration.ofSeconds(30);
    Map<PubSubTopicPartition, PubSubPosition> startPositions = consumer.beginningPositions(allPartitions, timeout);
    Map<PubSubTopicPartition, PubSubPosition> endPositions = consumer.endPositions(allPartitions, timeout);

    Map<Integer, PubSubPartitionSplit> splits = new HashMap<>(partitionCount);
    for (PubSubTopicPartition tp: allPartitions) {
      int p = tp.getPartitionNumber();
      PubSubPosition start = startPositions.get(tp);
      PubSubPosition end = endPositions.get(tp);
      long estimatedRecords = Math.max(0, consumer.positionDifference(tp, end, start));
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

  /** Converts one {@link VTConsistencyChecker.Inconsistency} to a Spark output row. */
  private static Row toRow(VTConsistencyChecker.Inconsistency inc, String versionTopic, int partition) {
    String dc0Value = inc.dc0Record != null ? inc.dc0Record.valueHash : null;
    String dc1Value = inc.dc1Record != null ? inc.dc1Record.valueHash : null;
    long[] dc0Ov = inc.dc0Record != null ? toLongArray(inc.dc0Record.upstreamRTOffset) : null;
    long[] dc1Ov = inc.dc1Record != null ? toLongArray(inc.dc1Record.upstreamRTOffset) : null;
    long[] dc0Hw = inc.dc0Record != null ? toLongArray(inc.dc0Record.highWatermark) : null;
    long[] dc1Hw = inc.dc1Record != null ? toLongArray(inc.dc1Record.highWatermark) : null;
    Long dc0Ts = inc.dc0Record != null ? inc.dc0Record.logicalTimestamp : null;
    Long dc1Ts = inc.dc1Record != null ? inc.dc1Record.logicalTimestamp : null;
    String dc0VtPosition = inc.dc0Record != null ? inc.dc0Record.vtPosition : null;
    String dc1VtPosition = inc.dc1Record != null ? inc.dc1Record.vtPosition : null;

    return RowFactory.create(
        versionTopic,
        partition,
        inc.type.name(),
        inc.key,
        dc0Value,
        dc1Value,
        dc0Ov,
        dc1Ov,
        dc0Hw,
        dc1Hw,
        dc0Ts,
        dc1Ts,
        dc0VtPosition,
        dc1VtPosition);
  }

  /**
   * If {@code SSL_CONFIGURATOR_CLASS_CONFIG} is present, reads SSL credentials from the
   * Hadoop token file (standard YARN distribution mechanism) and merges them into the
   * config. If the key is absent, returns the config unchanged — so local/test runs
   * that pass SSL props directly as CLI args continue to work without modification.
   */
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
    // Override the broker address so each DC talks to its own Kafka cluster.
    p.setProperty(KAFKA_BOOTSTRAP_SERVERS, brokerUrl);
    return new VeniceProperties(p);
  }

  private static long[] toLongArray(List<Long> list) {
    long[] arr = new long[list.size()];
    for (int i = 0; i < list.size(); i++) {
      arr[i] = list.get(i);
    }
    return arr;
  }

  /**
   * Parses {@code key=value} command-line arguments into a {@link Properties} object.
   * Example: {@code dc0.broker.url=dc0-kafka:9092 version.topic=my-store_v3}
   */
  static Properties parseArgs(String[] args) {
    Properties props = new Properties();
    for (String arg: args) {
      int idx = arg.indexOf('=');
      if (idx <= 0) {
        throw new IllegalArgumentException("Expected key=value argument, got: " + arg);
      }
      props.setProperty(arg.substring(0, idx), arg.substring(idx + 1));
    }
    for (String required: new String[] { DC0_BROKER_URL, DC1_BROKER_URL, VERSION_TOPIC, OUTPUT_PATH }) {
      if (!props.containsKey(required)) {
        throw new IllegalArgumentException("Missing required argument: " + required);
      }
    }
    return props;
  }
}
