package com.linkedin.venice.integration.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.ForkedJavaProcess;
import com.linkedin.venice.utils.PropertyBuilder;

import com.linkedin.mirrormaker.IdentityNewConsumerRebalanceListener;
import com.linkedin.mirrormaker.IdentityPartitioningMessageHandler;

import java.util.Optional;
import kafka.tools.MirrorMaker;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.Utils;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class MirrorMakerWrapper extends ProcessWrapper {
  public static final Logger logger = Logger.getLogger(MirrorMakerWrapper.class);
  public static final String SERVICE_NAME = "MirrorMaker";
  public static final String DEFAULT_TOPIC_WHITELIST = ".*";

  private final String topicWhitelist;
  private final String consumerConfigPath;
  private final String producerConfigPath;
  private Process mirrorMakerProcess;

  static StatefulServiceProvider<MirrorMakerWrapper> generateService(
      KafkaBrokerWrapper sourceKafka,
      KafkaBrokerWrapper targetKafka) {

    return generateService(sourceKafka, targetKafka, DEFAULT_TOPIC_WHITELIST);
  }

  static StatefulServiceProvider<MirrorMakerWrapper> generateService(
      KafkaBrokerWrapper sourceKafka,
      KafkaBrokerWrapper targetKafka,
      String whitelistForKMM) {

    return generateService(
        sourceKafka.getAddress(),
        targetKafka.getAddress(),
        targetKafka.getZkAddress(),
        whitelistForKMM,
        new Properties(),
        new Properties());
  }

  static StatefulServiceProvider<MirrorMakerWrapper> generateService(
      String sourceKafkaAddress,
      String targetKafkaAddress,
      String targetZkAddress,
      String topicWhitelist,
      Properties consumerProperties,
      Properties producerProperties) {

    return (serviceName, dataDirectory) -> {
      String consumerConfigPath = createConsumerConfig(dataDirectory, sourceKafkaAddress, consumerProperties);
      String producerConfigPath = createProducerConfig(dataDirectory, targetKafkaAddress, targetZkAddress, producerProperties);
      logger.info("MirrorMaker: source:" + sourceKafkaAddress + " dest:" + targetKafkaAddress);
      return new MirrorMakerWrapper(serviceName, dataDirectory, topicWhitelist, consumerConfigPath, producerConfigPath);
    };
  }

  public static String createConsumerConfig(File directory, String sourceKafkaAddress, Properties consumerProperties) {
    Properties properties = new PropertyBuilder()
        .put(consumerProperties)
        .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceKafkaAddress)
        .putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, com.linkedin.venice.utils.Utils.getUniqueString("mm"))
        .putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .putIfAbsent(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "0")
        .build()
        .toProperties();
    return IntegrationTestUtils.getConfigFile(directory, com.linkedin.venice.utils.Utils.getUniqueString(), properties).getAbsolutePath();
  }

  public static String createProducerConfig(File directory, String targetKafkaAddress, String targetZkAddress, Properties producerProperties) {
    Properties properties = new PropertyBuilder()
        .put(producerProperties)
        .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, targetKafkaAddress)
        .put("identityMirror.Version", "3")
        .put("identityMirror.TargetZookeeper.connect", targetZkAddress)
        .putIfAbsent("identityMirror.TargetReplicationFactor", "1")
        .build()
        .toProperties();
    return IntegrationTestUtils.getConfigFile(directory, com.linkedin.venice.utils.Utils.getUniqueString(), properties).getAbsolutePath();
  }

  MirrorMakerWrapper(
      String serviceName,
      File dataDirectory,
      String topicWhitelist,
      String consumerConfigPath,
      String producerConfigPath) throws Exception {
    super(serviceName, dataDirectory);
    logger.info("Source config: " + Utils.loadProps(consumerConfigPath));
    logger.info("Target config: " + Utils.loadProps(producerConfigPath));
    this.topicWhitelist = topicWhitelist;
    this.consumerConfigPath = consumerConfigPath;
    this.producerConfigPath = producerConfigPath;
  }

  @Override
  public boolean isRunning() {
    return super.isRunning() && mirrorMakerProcess.isAlive();
  }

  @Override
  protected void internalStart() throws Exception {
    mirrorMakerProcess = ForkedJavaProcess.exec(
        MirrorMaker.class,
        Arrays.asList(
            "--consumer.config", consumerConfigPath,
            "--producer.config", producerConfigPath,
            "--whitelist", topicWhitelist,
            "--offset.commit.interval.ms", "1000",
            "--message.handler", IdentityPartitioningMessageHandler.class.getName(),
            "--consumer.rebalance.listener", IdentityNewConsumerRebalanceListener.class.getName(),
            "--rebalance.listener.args", IdentityNewConsumerRebalanceListener.getConfigString(consumerConfigPath, producerConfigPath)
        ),
        Arrays.asList("-Xms64m", "-Xmx128m"),
        Optional.empty()
    );

    // It's tricky to find a good timeout here... if it's too small it'll never detect any start up failure,
    // while if it's too long then it needlessly extends test run-time even when everything's fine.
    // At 1 second, it doesn't actually detect every possible failure mode, but it's probably an okay compromise...
    if (mirrorMakerProcess.waitFor(1, TimeUnit.SECONDS)) {
      mirrorMakerProcess.destroy();
      throw new VeniceException("MirrorMaker exited unexpectedly with the code " + mirrorMakerProcess.exitValue());
    }
    logger.info("MirrorMaker is started!");
  }

  @Override
  protected void internalStop() {
    mirrorMakerProcess.destroy();
  }

  @Override
  public String getHost() {
    throw new UnsupportedOperationException("getHost() is not supported by " + getClass().getSimpleName());
  }

  @Override
  public int getPort() {
    throw new UnsupportedOperationException("getPort() is not supported by " + getClass().getSimpleName());
  }

  @Override
  protected void newProcess() {
    throw new UnsupportedOperationException("newProcess() is not supported by " + getClass().getSimpleName());
  }
}
