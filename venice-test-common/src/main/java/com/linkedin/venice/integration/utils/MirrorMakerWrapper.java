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
import kafka.tools.MirrorMaker;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.log4j.Logger;


public class MirrorMakerWrapper extends ProcessWrapper {
  public static final Logger LOGGER = Logger.getLogger(MirrorMakerWrapper.class);
  public static final String SERVICE_NAME = "MirrorMaker";

  Process mmProcess = null;
  final File consumerPropsFile;
  final File producerPropsFile;

  static StatefulServiceProvider<MirrorMakerWrapper> generateService(KafkaBrokerWrapper sourceKafka, KafkaBrokerWrapper destinationKafka) {
    return (serviceName, port, dataDirectory) -> {
      // Consumer configs
      Properties consumerProps = new PropertyBuilder()
          // .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceKafka.getAddress()) // Used by the "new consumer"
          .put("zookeeper.connect", sourceKafka.getZkAddress()) // Used by the "old consumer" and the IdentityOldConsumerRebalanceListener
          .put("group.id", TestUtils.getUniqueString("mm"))
          .put("security.protocol", SecurityProtocol.PLAINTEXT.name) // to simplify tests, though we may want to test SSL connectivity within integ tests later
          .build().toProperties();
      File consumerPropsFile = IntegrationTestUtils.getConfigFile(dataDirectory, "consumer.properties", consumerProps);

      // Producer configs
      Properties producerProps = new PropertyBuilder()
          .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, destinationKafka.getAddress())
          .put(ProducerConfig.LINGER_MS_CONFIG, "500")
          .put("identityMirror.Version", "2")
          .put("identityMirror.TargetZookeeper.connect", destinationKafka.getZkAddress())
          .build().toProperties();
      File producerPropsFile = IntegrationTestUtils.getConfigFile(dataDirectory, "producer.properties", producerProps);

      return new MirrorMakerWrapper(serviceName, dataDirectory, consumerPropsFile, producerPropsFile);
    };
  }

  MirrorMakerWrapper(String serviceName, File dataDirectory, File consumerPropsFile, File producerPropsFile) {
    super(serviceName, dataDirectory);
    this.consumerPropsFile = consumerPropsFile;
    this.producerPropsFile = producerPropsFile;
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
        "--whitelist", ".*",
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
