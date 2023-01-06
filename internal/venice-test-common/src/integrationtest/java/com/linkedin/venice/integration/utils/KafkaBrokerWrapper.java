package com.linkedin.venice.integration.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.MockTime;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import kafka.metrics.KafkaMetricsReporter;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;


/**
 * This class contains a Kafka Broker, and provides facilities for cleaning up
 * its side effects when we're done using it.
 */
public class KafkaBrokerWrapper extends ProcessWrapper {
  // Class-level state and APIs
  public static final String SERVICE_NAME = "Kafka";
  private static final int OFFSET_TOPIC_PARTITIONS = 1;
  private static final short OFFSET_TOPIC_REPLICATION_FACTOR = 1;
  private static final boolean LOG_CLEANER_ENABLE = false;

  private final int sslPort;
  private static final Logger LOGGER = LogManager.getLogger(KafkaBrokerWrapper.class);

  /**
   * This is package private because the only way to call this should be from
   * {@link ServiceFactory#getKafkaBroker()}.
   *
   * @return a function which yields a {@link KafkaBrokerWrapper} instance
   */
  static StatefulServiceProvider<KafkaBrokerWrapper> generateService(
      ZkServerWrapper zkServerWrapper,
      Optional<MockTime> mockTime) {
    return (String serviceName, File dir) -> {
      int port = Utils.getFreePort();
      int sslPort = Utils.getFreePort();
      Map<String, Object> configMap = new HashMap<>();

      // Essential configs
      configMap.put(KafkaConfig.ZkConnectProp(), zkServerWrapper.getAddress());
      configMap.put(KafkaConfig.PortProp(), port);
      configMap.put(KafkaConfig.HostNameProp(), DEFAULT_HOST_NAME);
      configMap.put(KafkaConfig.LogDirProp(), dir.getAbsolutePath());
      configMap.put(KafkaConfig.AutoCreateTopicsEnableProp(), false);
      configMap.put(KafkaConfig.DeleteTopicEnableProp(), true);
      configMap.put(KafkaConfig.LogMessageTimestampTypeProp(), "LogAppendTime");
      configMap.put(KafkaConfig.LogMessageFormatVersionProp(), "0.10.1");

      // The configs below aim to reduce the overhead of the Kafka process:
      configMap.put(KafkaConfig.OffsetsTopicPartitionsProp(), OFFSET_TOPIC_PARTITIONS);
      configMap.put(KafkaConfig.OffsetsTopicReplicationFactorProp(), OFFSET_TOPIC_REPLICATION_FACTOR);
      configMap.put(KafkaConfig.LogCleanerEnableProp(), LOG_CLEANER_ENABLE);

      // Setup ssl related configs for kafka.
      Properties sslConfig = KafkaSSLUtils.getLocalKafkaBrokerSSlConfig(DEFAULT_HOST_NAME, port, sslPort);
      sslConfig.entrySet().stream().forEach(entry -> configMap.put((String) entry.getKey(), entry.getValue()));
      KafkaConfig kafkaConfig = new KafkaConfig(configMap, true);
      KafkaServer kafkaServer = instantiateNewKafkaServer(kafkaConfig, mockTime);
      LOGGER.info("KafkaBroker URL: {}:{}", kafkaServer.config().hostName(), kafkaServer.config().port());
      return new KafkaBrokerWrapper(kafkaConfig, kafkaServer, dir, zkServerWrapper, mockTime, sslPort);
    };
  }

  /**
   * This function encapsulates all of the Scala weirdness required to interop with the {@link KafkaServer}.
   */
  private static KafkaServer instantiateNewKafkaServer(KafkaConfig kafkaConfig, Optional<MockTime> mockTime) {
    // We cannot get a kafka.utils.Time out of an Optional<MockTime>, even though MockTime implements it.
    org.apache.kafka.common.utils.Time time =
        Optional.<org.apache.kafka.common.utils.Time>ofNullable(mockTime.orElse(null)).orElse(SystemTime.SYSTEM);
    int port = kafkaConfig.getInt(KafkaConfig.PortProp());
    // Scala's Some (i.e.: the non-empty Optional) needs to be instantiated via Some's object (i.e.: static companion
    // class)
    Option<String> threadNamePrefix = scala.Some$.MODULE$.apply("kafka-broker-port-" + port);
    // This needs to be a Scala List
    Seq<KafkaMetricsReporter> metricsReporterSeq = JavaConverters.asScalaBuffer(new ArrayList<>());
    return new KafkaServer(kafkaConfig, time, threadNamePrefix, metricsReporterSeq);
  }

  // Instance-level state and APIs

  private KafkaServer kafkaServer;
  private final ZkServerWrapper zkServerWrapper;
  private final KafkaConfig kafkaConfig;
  private final Optional<MockTime> mockTime;

  /**
   * The constructor is private because {@link #generateService(ZkServerWrapper, Optional)} should be the only
   * way to construct a {@link KafkaBrokerWrapper} instance.
   *
   * @param kafkaServer the Kafka instance to wrap
   * @param dataDirectory where Kafka keeps its log
   * @param zkServerWrapper the ZK which Kafka uses for its coordination
   */
  private KafkaBrokerWrapper(
      KafkaConfig kafkaConfig,
      KafkaServer kafkaServer,
      File dataDirectory,
      ZkServerWrapper zkServerWrapper,
      Optional<MockTime> mockTime,
      int sslPort) {
    super(SERVICE_NAME, dataDirectory);
    this.kafkaConfig = kafkaConfig;
    this.kafkaServer = kafkaServer;
    this.zkServerWrapper = zkServerWrapper;
    this.mockTime = mockTime;
    this.sslPort = sslPort;
  }

  /**
   * @see {@link ProcessWrapper#getHost()}
   */
  public String getHost() {
    return kafkaServer.config().hostName();
  }

  /**
   * @see {@link ProcessWrapper#getPort()}
   */
  public int getPort() {
    return kafkaServer.config().port();
  }

  public int getSslPort() {
    return sslPort;
  }

  public String getAddress() {
    return getHost() + ":" + getPort();
  }

  public String getSSLAddress() {
    return getHost() + ":" + getSslPort();
  }

  /**
   * @return the address of the ZK used by this Kafka instance
   */
  public String getZkAddress() {
    return zkServerWrapper.getAddress();
  }

  @Override
  protected void internalStart() {
    Properties properties = System.getProperties();
    if (properties.contains("kafka_mx4jenable")) {
      throw new VeniceException(
          "kafka_mx4jenable should not be set! kafka_mx4jenable = " + properties.getProperty("kafka_mx4jenable"));
    }
    kafkaServer.startup();
    LOGGER.info("Start kafka broker listen on port: {} and ssl port: {}", getPort(), getSslPort());
  }

  @Override
  protected void internalStop() {
    kafkaServer.shutdown();
    kafkaServer.awaitShutdown();
  }

  @Override
  protected void newProcess() {
    kafkaServer = instantiateNewKafkaServer(kafkaConfig, mockTime);
  }

  @Override
  public String toString() {
    return "KafkaBrokerWrapper{address: '" + getAddress() + "', sslAddress: '" + getSSLAddress() + "'}";
  }
}
