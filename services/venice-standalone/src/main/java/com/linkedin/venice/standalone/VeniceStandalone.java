package com.linkedin.venice.standalone;

import com.linkedin.venice.controller.VeniceController;
import com.linkedin.venice.router.RouterServer;
import com.linkedin.venice.server.VeniceServer;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import kafka.metrics.KafkaMetricsReporter;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;


public class VeniceStandalone {
  private static final Logger LOGGER = LogManager.getLogger(VeniceStandalone.class);

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      Utils.exit("USAGE: java -jar venice-standalone.jar <config_directory>");
      return;
    }
    try {

      if (isServiceEnabled("zookeeper")) {
        startZooKeeperServer(args);
      }

      if (isServiceEnabled("kafka")) {
        startKafkaServer(args);
      }

      if (isServiceEnabled("controller")) {
        VeniceController.run(
            new File(args[0], "cluster.properties").getAbsolutePath(),
            new File(args[0], "controller.properties").getAbsolutePath(),
            false);
      }

      if (isServiceEnabled("server")) {
        VeniceServer.run(new File(args[0]).getAbsolutePath(), false);
      }

      if (isServiceEnabled("router")) {
        RouterServer.run(new File(args[0], "router.properties").getAbsolutePath(), false);
      }

      Thread.sleep(Integer.MAX_VALUE);
    } catch (Throwable error) {
      LOGGER.error("Error starting Venice standalone", error);
      System.exit(-1);
      return;
    }

  }

  private static boolean isServiceEnabled(String service) {
    String services = System.getProperty("services", "*");
    if (services.equals("*") || services.toLowerCase().contains(service)) {
      return true;
    }
    return false;
  }

  private static void startZooKeeperServer(String[] args) throws Exception {
    File zookeeperConfig = new File(args[0], "zookeeper.properties");
    if (zookeeperConfig.isFile()) {
      Properties properties = new Properties();
      try (Reader reader = new FileReader(zookeeperConfig)) {
        properties.load(reader);
      }
      boolean enabled = Boolean.parseBoolean(properties.getProperty("enabled", "false"));
      if (enabled) {
        String dataDirectory = properties.getProperty("dataDir", "zookeeperData");
        int port = Integer.parseInt(properties.getProperty("port", "2181"));
        LOGGER.info("Starting ZooKeeper on port {}, dataDir is {}", port, dataDirectory);
        InstanceSpec spec = new InstanceSpec(new File(dataDirectory), port, -1, -1, false, 1);
        TestingServer zkServer = new TestingServer(spec, false);
        zkServer.start();
      }
    }
  }

  private static void startKafkaServer(String[] args) throws Exception {
    File kafkaConfigFile = new File(args[0], "kafka.properties");
    if (kafkaConfigFile.isFile()) {
      Properties properties = new Properties();
      try (Reader reader = new FileReader(kafkaConfigFile)) {
        properties.load(reader);
      }
      boolean enabled = Boolean.parseBoolean(properties.getProperty("enabled", "false"));
      if (enabled) {
        KafkaConfig kafkaConfig = new KafkaConfig(properties);
        int brokerId = kafkaConfig.brokerId();
        removeEphemeralBrokerIdIfPresent(brokerId);
        Option<String> threadNamePrefix = scala.Some$.MODULE$.apply("kafka-broker-port");
        // This needs to be a Scala List
        Seq<KafkaMetricsReporter> metricsReporterSeq = JavaConverters.asScalaBuffer(new ArrayList<>());
        KafkaServer kafkaServer = new KafkaServer(kafkaConfig, SystemTime.SYSTEM, threadNamePrefix, metricsReporterSeq);
        LOGGER.info("Starting Kafka with config {}", kafkaConfig);
        kafkaServer.startup();
      }
    }
  }

  private static void removeEphemeralBrokerIdIfPresent(int brokerId) {
    String path = "/brokers/ids/" + brokerId;
    CompletableFuture<?> connected = new CompletableFuture<>();
    try (ZooKeeper zooKeeper = new ZooKeeper("localhost:2181", 20000, new Watcher() {
      @Override
      public void process(WatchedEvent watchedEvent) {
        connected.complete(null);
      }
    });) {
      connected.get(10, TimeUnit.SECONDS);
      if (zooKeeper.exists(path, false) != null) {
        LOGGER.info("Deleting {} from ZK", path);
        zooKeeper.delete(path, -1);
      }
    } catch (Exception error) {
      // ignore
    }
  }
}
