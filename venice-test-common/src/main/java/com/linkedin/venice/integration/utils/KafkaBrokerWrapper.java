package com.linkedin.venice.integration.utils;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import scala.None$;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

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

  /**
   * This is package private because the only way to call this should be from
   * {@link ServiceFactory#getKafkaBroker()}.
   *
   * @return a function which yields a {@link KafkaBrokerWrapper} instance
   */
  static StatefulServiceProvider<KafkaBrokerWrapper> generateService(ZkServerWrapper zkServerWrapper) {
    return (String serviceName, int port, File dir) -> {
      Map<String, Object> configMap = new HashMap<>();

      // Essential configs
      configMap.put(KafkaConfig.ZkConnectProp(), zkServerWrapper.getAddress());
      configMap.put(KafkaConfig.PortProp(), port);
      configMap.put(KafkaConfig.HostNameProp(), DEFAULT_HOST_NAME);
      configMap.put(KafkaConfig.LogDirProp(), dir.getAbsolutePath());

      // The configs below aim to reduce the overhead of the Kafka process:
      configMap.put(KafkaConfig.OffsetsTopicPartitionsProp(), OFFSET_TOPIC_PARTITIONS);
      configMap.put(KafkaConfig.OffsetsTopicReplicationFactorProp(), OFFSET_TOPIC_REPLICATION_FACTOR);
      configMap.put(KafkaConfig.LogCleanerEnableProp(), LOG_CLEANER_ENABLE);

      configMap.put(KafkaConfig.AutoCreateTopicsEnableProp(), false);
      configMap.put(KafkaConfig.DeleteTopicEnableProp(), true);

      KafkaConfig kafkaConfig = new KafkaConfig(configMap, true);
      // kafka.server.KafkaServerStartable kafkaServerStartable = new KafkaServerStartable(kafkaConfig);
      KafkaServer kafkaServer = new KafkaServer(kafkaConfig, SystemTime$.MODULE$, None$.empty());
      return new KafkaBrokerWrapper(kafkaConfig, kafkaServer, dir, zkServerWrapper);
    };
  }

  // Instance-level state and APIs

  private KafkaServer kafkaServer;
  private ZkServerWrapper zkServerWrapper;
  private final KafkaConfig kafkaConfig;

  /**
   * The constructor is private because {@link #generateService(ZkServerWrapper)} should be the only
   * way to construct a {@link KafkaBrokerWrapper} instance.
   *
   * @param kafkaServer the Kafka instance to wrap
   * @param dataDirectory where Kafka keeps its log
   * @param zkServerWrapper the ZK which Kafka uses for its coordination
   */
  private KafkaBrokerWrapper(KafkaConfig kafkaConfig, KafkaServer kafkaServer, File dataDirectory, ZkServerWrapper zkServerWrapper) {
    super(SERVICE_NAME, dataDirectory);
    this.kafkaConfig = kafkaConfig;
    this.kafkaServer = kafkaServer;
    this.zkServerWrapper = zkServerWrapper;
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

  /**
   * @return the address of the ZK used by this Kafka instance
   */
  public String getZkAddress() {
    return zkServerWrapper.getAddress();
  }

  @Override
  protected void internalStart() throws Exception {
    kafkaServer.startup();
  }

  @Override
  protected void internalStop() throws Exception {
    kafkaServer.shutdown();
    zkServerWrapper.close();
  }

  @Override
  protected void newProcess()
      throws Exception {
    zkServerWrapper = ServiceFactory.getZkServer();
    kafkaServer = new KafkaServer(kafkaConfig, SystemTime$.MODULE$, None$.empty());
  }
}
