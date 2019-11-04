package com.linkedin.venice.integration.utils;

import com.linkedin.mirrormaker.IdentityOldConsumerRebalanceListener;
import com.linkedin.mirrormaker.IdentityPartitioningMessageHandler;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import kafka.api.OffsetRequest;
import kafka.tools.MirrorMaker;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.log4j.Logger;


public class MirrorMakerWrapper extends ProcessWrapper {
  public static final Logger LOGGER = Logger.getLogger(MirrorMakerWrapper.class);
  public static final String SERVICE_NAME = "MirrorMaker";
  private static final String DEFAULT_WHITELIST = ".*";

  Process mmProcess = null;
  final File consumerPropsFile;
  final File producerPropsFile;
  final String whitelist;

  static StatefulServiceProvider<MirrorMakerWrapper> generateService(KafkaBrokerWrapper sourceKafka, KafkaBrokerWrapper destinationKafka) {
    return generateService(sourceKafka.getZkAddress(), destinationKafka.getZkAddress(), destinationKafka.getAddress(), DEFAULT_WHITELIST, new Properties(), new Properties());
  }

  static StatefulServiceProvider<MirrorMakerWrapper> generateService(
      String sourceZkAdr,
      String destinationZkAdr,
      String destinationKafkaAdr,
      String whitelist,
      Properties consumerProps,
      Properties producerProps) {
    return (serviceName, port, dataDirectory) -> {
      // Consumer configs
      Properties combinedConsumerProps = new PropertyBuilder()
          .put(consumerProps)
          // .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceKafka.getAddress()) // Used by the "new consumer"
          .putIfAbsent("zookeeper.connect", sourceZkAdr) // Used by the "old consumer" and the IdentityOldConsumerRebalanceListener
          .putIfAbsent("group.id", TestUtils.getUniqueString("mm"))
          .putIfAbsent("security.protocol", SecurityProtocol.PLAINTEXT.name) // to simplify tests, though we may want to test SSL connectivity within integ tests later
          .putIfAbsent("auto.offset.reset", OffsetRequest.SmallestTimeString()) // "smallest"
          .build().toProperties();
      File consumerPropsFile = IntegrationTestUtils.getConfigFile(dataDirectory, "consumer.properties", combinedConsumerProps);

      // Producer configs
      Properties combinedProducerProps = new PropertyBuilder()
          .put(producerProps)
          .putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, destinationKafkaAdr)
          .putIfAbsent(ProducerConfig.LINGER_MS_CONFIG, "500")
          .putIfAbsent("identityMirror.Version", "2")
          .putIfAbsent("identityMirror.TargetZookeeper.connect", destinationZkAdr)
          .build().toProperties();
      File producerPropsFile = IntegrationTestUtils.getConfigFile(dataDirectory, "producer.properties", combinedProducerProps);

      return new MirrorMakerWrapper(serviceName, dataDirectory, consumerPropsFile, producerPropsFile, whitelist);
    };
  }

  MirrorMakerWrapper(String serviceName, File dataDirectory, File consumerPropsFile, File producerPropsFile, String whitelist) {
    super(serviceName, dataDirectory);
    this.consumerPropsFile = consumerPropsFile;
    this.producerPropsFile = producerPropsFile;
    this.whitelist = whitelist;
  }

  @Override
  public String getHost() {
    return "N/A";
  }

  @Override
  public int getPort() {
    return 0;
  }

  @Override
  protected void internalStart() throws Exception {
    /** This is a messed up format but that's what the {@link IdentityOldConsumerRebalanceListener} wants. */
    final String CONFIG_DELIMITER = "#";
    String rebalanceListenerArgs = "file" + CONFIG_DELIMITER + consumerPropsFile.getAbsolutePath() + CONFIG_DELIMITER + producerPropsFile.getAbsolutePath();

    LOGGER.info("About to fork MM process with the following source/target configs:" +
        "\nsource config:" + org.apache.kafka.common.utils.Utils.loadProps(consumerPropsFile.getAbsolutePath()).toString() +
        "\ntarget config:" + org.apache.kafka.common.utils.Utils.loadProps(producerPropsFile.getAbsolutePath()).toString());

    mmProcess = ForkedJavaProcess.exec(MirrorMaker.class,
        "--consumer.config", consumerPropsFile.getAbsolutePath(),
        "--producer.config", producerPropsFile.getAbsolutePath(),
        "--whitelist", whitelist,
        "--message.handler", IdentityPartitioningMessageHandler.class.getName(),
        "--consumer.rebalance.listener", IdentityOldConsumerRebalanceListener.class.getName(),
        "--rebalance.listener.args", rebalanceListenerArgs,
        "--num.streams", "2"
    );
    // TODO: Add other configs we usually run MM with.

    try {
      // It's tricky to find a good timeout here... if it's too small it'll never detect any start up failure,
      // while if it's too long then it needlessly extends test run-time even when everything's fine.
      // At 1 second, it doesn't actually detect every possible failure mode, but it's probably an okay compromise...
      mmProcess.waitFor(1000, TimeUnit.MILLISECONDS);
      int exitValue = mmProcess.exitValue();
      throw new VeniceException("MirrorMaker did not keep running after starting up, and exited with code: " + exitValue);
    } catch (IllegalThreadStateException e) {
      // Good, this means the process is still running
      LOGGER.info("MirrorMaker is started!");
    } catch (Exception e) {
      throw new VeniceException("Unexpected exception while trying to fork the MM process.", e);
    }
  }

  @Override
  protected void internalStop() throws Exception {
    mmProcess.destroy();
  }

  @Override
  protected void newProcess() throws Exception {
    throw new VeniceException("newProcess() is not supported for now in " + this.getClass().getSimpleName());
  }
}
