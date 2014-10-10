package com.linkedin.venice.kafka;

import com.linkedin.venice.config.GlobalConfiguration;
import com.linkedin.venice.kafka.consumer.KafkaConsumerPartitionManager;
import com.linkedin.venice.kafka.consumer.SimpleKafkaConsumerTask;
import com.linkedin.venice.kafka.consumer.VeniceKafkaConsumerException;
import com.linkedin.venice.message.OperationType;
import com.linkedin.venice.message.VeniceMessage;
import com.linkedin.venice.storage.InMemoryStorageNode;
import com.linkedin.venice.storage.VeniceStorageException;
import com.linkedin.venice.storage.VeniceStorageManager;
import kafka.admin.AdminUtils;
import kafka.producer.KeyedMessage;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *  Class which tests the Kafka Consumption class.
 *
 *  Note: This class starts many embedded services:
 *   - Zookeeper
 *   - Kafka Server
 *   - Kafka Producer
 *   - Kafka Consumer
 *   - Venice Storage
 *
 */

public class TestKafkaConsumer {

  static final Logger logger = Logger.getLogger(TestKafkaConsumer.class.getName());

  static final String DEFAULT_KAFKA_LOG_DIR = "/tmp/testng-kafka-logs";
  static final String DEFAULT_ZK_LOG_DIR = "/tmp/testng-zookeeper-logs";

  static final int LOCALHOST_ZK_BROKER_PORT = 2626;
  static final int NUM_CONNECTIONS = 5000;
  static final int TICKTIME = 2000;

  static final String TEST_TOPIC = "testng-topic";
  static final String TEST_KEY = "test_key";

  static VeniceStorageManager storeManager;
  static KafkaServerStartable kafkaServer;
  static Producer<String, VeniceMessage> kafkaProducer;

  @BeforeClass
  private static void init() {

    clearLogs();

    try {

      // config file for testng
      GlobalConfiguration.initializeFromFile("./src/test/resources/test.properties");

      // start Zookeeper
      startZookeeper();
      Thread.sleep(2000);

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    // TODO: Understand how topic creation is done in the Kafka API
    // An absolutely awful and terrible "hack" which allows a topic to be created on this embedded ZK instance
    startUpServices();
    startKafkaConsumers(new InMemoryStorageNode(0));
    sendKafkaMessage("");
    tearDown();

    // The real startup procedure
    startUpServices();

  }

  private static void startUpServices() {

    Properties kafkaProperties = new Properties();

    try {

      // start Kakfa
      kafkaProperties.load(new FileInputStream("./src/test/resources/kafkatest.properties"));
      startKafkaServer(kafkaProperties);
      Thread.sleep(2000);

      // start the Kafka Producer
      startKafkaProducer(GlobalConfiguration.getKafkaBrokerUrl());

      // start the Venice Storage nodes
      startVeniceStorage();

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

  }

  /**
   *  Starts a local instance of ZooKeeper
   * */
  private static void startZookeeper() throws Exception {

    File dir = new File(DEFAULT_ZK_LOG_DIR);

    ZooKeeperServer server = new ZooKeeperServer(dir, dir, TICKTIME);
    server.setMaxSessionTimeout(1000000);
    NIOServerCnxn.Factory standaloneServerFactory =
        new NIOServerCnxn.Factory(new InetSocketAddress(LOCALHOST_ZK_BROKER_PORT), NUM_CONNECTIONS);

    standaloneServerFactory.startup(server);
    Thread.sleep(2000);

  }

  /**
   *  Starts a local instance of Kafka
   * */
  private static void startKafkaServer(Properties kafkaProps) {

    KafkaConfig config = new KafkaConfig(kafkaProps);

    // start kafka
    kafkaServer = new KafkaServerStartable(config);
    kafkaServer.startup();

  }

  /**
   *  Starts a Kafka producer service.
   *  Kakfa server must be active for the producer to be started properly.
   * */
  private static void startKafkaProducer(String brokerUrl) {

    Properties props = new Properties();
    props.put("metadata.broker.list", brokerUrl);
    props.put("key.serializer.class", "kafka.serializer.StringEncoder");
    props.put("serializer.class", "com.linkedin.venice.message.VeniceMessageSerializer");
    props.setProperty("partitioner.class", "com.linkedin.venice.kafka.partitioner.KafkaPartitioner");

    ProducerConfig config = new ProducerConfig(props);

    kafkaProducer = new Producer<String, VeniceMessage>(config);

  }

  /**
   *  Set up the nodes for Venice, such that they can be written to
   * */
  private static void startVeniceStorage() {

    KafkaConsumerPartitionManager.initialize(TEST_TOPIC,
        GlobalConfiguration.getBrokerList(), GlobalConfiguration.getKafkaBrokerPort());

    // initialize the storage engine, start n nodes and p partitions.
    storeManager = new VeniceStorageManager();

    try {

      // For testing, use 2 storage nodes
      for (int n = 0; n < GlobalConfiguration.getNumStorageNodes(); n++) {
        storeManager.registerNewNode(n);
      }

      // For testing, use 5 partitions
      for (int p = 0; p < GlobalConfiguration.getNumKafkaPartitions(); p++) {
        storeManager.registerNewPartition(p);
      }

    } catch (VeniceStorageException e) {
      Assert.fail(e.getMessage());
    } catch (VeniceKafkaConsumerException e) {
      Assert.fail(e.getMessage());
    }

  }

  /**
   *  Set up the Kafka consumer object to be tied to the given storage node
   * */
  private static void startKafkaConsumers(InMemoryStorageNode node) {

    try {

      KafkaConsumerPartitionManager manager = KafkaConsumerPartitionManager.getInstance();
      SimpleKafkaConsumerTask task = manager.getConsumerTask(node, 0);

      // launch each consumer task on a new thread
      ExecutorService executor = Executors.newFixedThreadPool(1);
      executor.submit(task);

    } catch (VeniceKafkaConsumerException e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

  }

  /**
   * Empties out the remaining logs in the Kafka and ZooKeeper directories
   * */
  private static void clearLogs() {

    try {

      File kafkaLogs = new File(DEFAULT_KAFKA_LOG_DIR);
      if (kafkaLogs.exists()) {
        FileUtils.deleteDirectory(kafkaLogs);
      }

      File zkLogs = new File(DEFAULT_ZK_LOG_DIR);
      if (zkLogs.exists()) {
        FileUtils.deleteDirectory(zkLogs);
      }

    } catch (IOException e) {
      Assert.fail("Encountered problem while deleting Kafka test logs.");
    }

  }

  /**
   *  Sends a Kafka message through a Kafka Producer.
   *  Kafka Producer must be active
   * */
  public static void sendKafkaMessage(String payload) {

    try {

        KeyedMessage<String, VeniceMessage> data = new KeyedMessage<String, VeniceMessage>(
            TEST_TOPIC, TEST_KEY, new VeniceMessage(OperationType.PUT, payload));

        kafkaProducer.send(data);

    } catch (Exception e) {

      logger.error(e.getMessage());
      e.printStackTrace();

    }

  }

  /**
   * Safely shutdown the services started in this class
   * */
  @AfterClass
  public static void tearDown() {

    kafkaProducer.close();
    kafkaServer.shutdown();

  }

  /**
   *  A basic test which send messages through Kafka, and consumes them
   * */
  @Test(enabled = true)
  public void testKafkaBasic() {

    InMemoryStorageNode node = new InMemoryStorageNode(0);

    try {

      node.addStoragePartition(0);

      startKafkaConsumers(node);
      Thread.sleep(2000); // at least 2 seconds is mandatory here!!

      ZkClient zkc = new ZkClient(GlobalConfiguration.getZookeeperURL(), 10000, 10000);
      Assert.assertTrue(AdminUtils.topicExists(zkc, TEST_TOPIC));

      sendKafkaMessage("test_message");
      Thread.sleep(1000);
      Assert.assertEquals(node.get(0, TEST_KEY), "test_message");

      sendKafkaMessage("test_message 2");
      Thread.sleep(1000);
      Assert.assertEquals(node.get(0, TEST_KEY), "test_message 2");

      sendKafkaMessage("test_message 3");
      Thread.sleep(1000);
      Assert.assertEquals(node.get(0, TEST_KEY), "test_message 3");

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

  }

}
