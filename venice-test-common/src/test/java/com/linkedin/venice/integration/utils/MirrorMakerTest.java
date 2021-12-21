package com.linkedin.venice.integration.utils;

import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.consumer.ApacheKafkaConsumer;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.ApacheKafkaProducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import kafka.utils.Whitelist;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.kafka.TopicManager.*;


public class MirrorMakerTest {
  private static final Logger LOGGER = Logger.getLogger(MirrorMakerTest.class);

  KafkaBrokerWrapper sourceKafka = null;
  KafkaBrokerWrapper destinationKafka = null;

  @BeforeClass(alwaysRun = true)
  void setUp() {
    sourceKafka = ServiceFactory.getKafkaBroker();
    destinationKafka = ServiceFactory.getKafkaBroker();
  }

  @AfterClass(alwaysRun = true)
  void cleanUp() {
    IOUtils.closeQuietly(sourceKafka);
    IOUtils.closeQuietly(destinationKafka);
  }

  /**
   * Unfortunately, MirrorMaker is a little flaky and sometimes fails. I have seen failures about 2% of the
   * time when running this test repeatedly, hence why I am adding the FlakyTestRetryAnalyzer. -FGV
   *
   * FlakyTestRetryAnalyzer class has been removed; we use "flaky" group to mark flaky test now.
   */
  @Test(timeOut = 30 * Time.MS_PER_SECOND)
  void testMirrorMakerProcessWrapper() throws ExecutionException, InterruptedException, IOException {
    LOGGER.info("Source Kafka: " + sourceKafka.getAddress());
    LOGGER.info("Source Kafka's ZK: " + sourceKafka.getZkAddress());
    LOGGER.info("Destination Kafka: " + destinationKafka.getAddress());
    LOGGER.info("Destination Kafka's ZK: " + destinationKafka.getZkAddress());
    String cliCommandParams = "--start-kafka-mirror-maker"
        + " --kafka-zk-url-source " + sourceKafka.getZkAddress()
        + " --kafka-zk-url-dest " + destinationKafka.getZkAddress()
        + " --kafka-bootstrap-servers-dest " + destinationKafka.getAddress()
        + " --kafka-topic-whitelist '.*'";
    String cliCommand = "java -jar venice-admin-tool/build/libs/venice-admin-tool-0.1.jar " + cliCommandParams;
    LOGGER.info("Manual MM params for admin-tool: \n" + cliCommand);

    LOGGER.info("Starting MM!");
    try (MirrorMakerWrapper mirrorMaker = ServiceFactory.getKafkaMirrorMaker(sourceKafka, destinationKafka);
        TopicManager topicManager =
            new TopicManager(DEFAULT_KAFKA_OPERATION_TIMEOUT_MS, 100, 0l, TestUtils.getVeniceConsumerFactory(sourceKafka))) {

      String topicName = Utils.getUniqueString("topic");
      topicManager.createTopic(topicName, 2, 1, false);

      Properties producerJavaProps = new Properties();
      producerJavaProps.setProperty(ApacheKafkaProducer.PROPERTIES_KAFKA_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
          sourceKafka.getAddress());
      ApacheKafkaProducer producer = null;
      try (ApacheKafkaConsumer consumer = new ApacheKafkaConsumer(getKafkaConsumerProperties(destinationKafka.getAddress()))) {
        producer = new ApacheKafkaProducer(new VeniceProperties(producerJavaProps));

        // Test pre-conditions
        consumer.subscribe(topicName, 0, OffsetRecord.LOWEST_OFFSET);

        LOGGER.info("About to consume message from destination cluster (should be empty).");
        ConsumerRecords consumerRecordsBeforeTest = consumer.poll(1 * Time.MS_PER_SECOND);
        Assert.assertTrue(consumerRecordsBeforeTest.isEmpty(),
            "The destination Kafka cluster should be empty at the beginning of the test!");

        LOGGER.info("About to produce message into source cluster.");
        producer.sendMessage(topicName, new KafkaKey(MessageType.CONTROL_MESSAGE, new byte[]{}), getValue(), 0, null).get();

        LOGGER.info("About to consume message from destination cluster (should contain something).");
        TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
          ApacheKafkaConsumer newConsumer = new ApacheKafkaConsumer(getKafkaConsumerProperties(destinationKafka.getAddress()));
          newConsumer.subscribe(topicName, 0, OffsetRecord.LOWEST_OFFSET);
          ConsumerRecords consumerRecords = newConsumer.poll(1 * Time.MS_PER_SECOND);
          Assert.assertFalse(consumerRecords.isEmpty(), "The destination Kafka cluster should NOT be empty!");
        });
      } finally {
        if (null != producer) {
          producer.close(5000);
        }
      }
    }
  }

  private KafkaMessageEnvelope getValue() {
    KafkaMessageEnvelope value = new KafkaMessageEnvelope();
    value.producerMetadata = new ProducerMetadata();
    value.producerMetadata.producerGUID = GuidUtils.getGUID(new VeniceProperties(new Properties()));
    value.producerMetadata.messageSequenceNumber = 0;
    value.producerMetadata.segmentNumber = 0;
    value.producerMetadata.messageTimestamp = 0;
    value.messageType = MessageType.CONTROL_MESSAGE.getValue();
    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = ControlMessageType.START_OF_PUSH.getValue();
    controlMessage.controlMessageUnion = ControlMessageType.START_OF_PUSH.getNewInstance();
    controlMessage.debugInfo = new HashMap<>();
    value.payloadUnion = controlMessage;
    return value;
  }

  /**
   * Copied from {@link KafkaStoreIngestionService}
   * @return Properties Kafka properties corresponding to the venice store.
   */
  private static Properties getKafkaConsumerProperties(String kafkaBootstrapServers) {
    Properties kafkaConsumerProperties = new Properties();
    kafkaConsumerProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    kafkaConsumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Venice is persisting offset in local offset db.
    kafkaConsumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    String groupId = Utils.getUniqueString("group-id");
    kafkaConsumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    /**
     * Temporarily we are going to use group_id as client_id as well since it is unique in cluster level.
     * With unique client_id, it will be easier for us to check Kafka consumer related metrics through JMX.
     * TODO: Kafka is throttling based on client_id, need to investigate whether we should use Kafka throttling or not.
     */
    kafkaConsumerProperties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, groupId);
    kafkaConsumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class);
    kafkaConsumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaValueSerializer.class);
    return kafkaConsumerProperties;
  }

  @Test
  public void whitelistRegexTest() {
    String topic1 = "topic1";
    String topic2 = "topic_2";

    String whitelistForTopic1 = topic1;
    String whitelistForTopic2 = topic2;
    String whitelistForBothTopics = topic1 + "|" + topic2;
    String whitelistForBothTopicsWithCSV = topic1 + "," + topic2;

    Assert.assertTrue(Pattern.matches(whitelistForTopic1, topic1));
    Assert.assertFalse(Pattern.matches(whitelistForTopic2, topic1));
    Assert.assertTrue(Pattern.matches(whitelistForTopic2, topic2));
    Assert.assertFalse(Pattern.matches(whitelistForTopic1, topic2));
    Assert.assertTrue(Pattern.matches(whitelistForBothTopics, topic1));
    Assert.assertTrue(Pattern.matches(whitelistForBothTopics, topic2));

    Whitelist kafkaWhiteListForTopic1 = new Whitelist(whitelistForTopic1);
    Whitelist kafkaWhiteListForTopic2 = new Whitelist(whitelistForTopic2);
    Whitelist kafkaWhiteListForBothTopics = new Whitelist(whitelistForBothTopics);
    Whitelist kafkaWhiteListForBothTopicsWithCSV = new Whitelist(whitelistForBothTopicsWithCSV);

    Assert.assertTrue(kafkaWhiteListForTopic1.isTopicAllowed(topic1, true));
    Assert.assertFalse(kafkaWhiteListForTopic2.isTopicAllowed(topic1, true));
    Assert.assertTrue(kafkaWhiteListForTopic2.isTopicAllowed(topic2, true));
    Assert.assertFalse(kafkaWhiteListForTopic1.isTopicAllowed(topic2, true));
    Assert.assertTrue(kafkaWhiteListForBothTopics.isTopicAllowed(topic1, true));
    Assert.assertTrue(kafkaWhiteListForBothTopics.isTopicAllowed(topic2, true));
    Assert.assertTrue(kafkaWhiteListForBothTopicsWithCSV.isTopicAllowed(topic1, true));
    Assert.assertTrue(kafkaWhiteListForBothTopicsWithCSV.isTopicAllowed(topic2, true));
  }
}
