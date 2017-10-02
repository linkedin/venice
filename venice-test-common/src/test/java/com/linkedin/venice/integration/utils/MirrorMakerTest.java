package com.linkedin.venice.integration.utils;

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
import com.linkedin.venice.utils.FlakyTestRetryAnalyzer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.ApacheKafkaProducer;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MirrorMakerTest {
  KafkaBrokerWrapper sourceKafka = null;
  KafkaBrokerWrapper destinationKafka = null;

  @BeforeClass
  void setUp() {
    sourceKafka = ServiceFactory.getKafkaBroker();
    destinationKafka = ServiceFactory.getKafkaBroker();
  }

  @AfterClass
  void cleanUp() {
    IOUtils.closeQuietly(sourceKafka);
    IOUtils.closeQuietly(destinationKafka);
  }

  /**
   * Unfortunately, MirrorMaker is a little flaky and sometimes fails. I have seen failures about 2% of the
   * time when running this test repeatedly, hence why I am adding the {@link FlakyTestRetryAnalyzer}. -FGV
   */
  //@Test(retryAnalyzer = FlakyTestRetryAnalyzer.class, timeOut = 30 * Time.MS_PER_SECOND)
  void testMirrorMakerProcessWrapper() throws ExecutionException, InterruptedException {
    MirrorMakerWrapper mirrorMaker = null;

    try {
      mirrorMaker = ServiceFactory.getKafkaMirrorMaker(sourceKafka, destinationKafka);

      TopicManager topicManager = new TopicManager(sourceKafka.getZkAddress());

      String topicName = TestUtils.getUniqueString("topic");
      topicManager.createTopic(topicName, 2, 1, false);

      Properties producerJavaProps = new Properties();
      producerJavaProps.setProperty(ApacheKafkaProducer.PROPERTIES_KAFKA_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
          sourceKafka.getAddress());
      ApacheKafkaProducer producer = new ApacheKafkaProducer(new VeniceProperties(producerJavaProps));
      ApacheKafkaConsumer consumer = new ApacheKafkaConsumer(getKafkaConsumerProperties(destinationKafka.getAddress()));

      // Test pre-conditions
      consumer.subscribe(topicName, 0, new OffsetRecord());
      ConsumerRecords consumerRecordsBeforeTest = consumer.poll(1 * Time.MS_PER_SECOND);
      Assert.assertTrue(consumerRecordsBeforeTest.isEmpty(),
          "The destination Kafka cluster should be empty at the beginning of the test!");

      producer.sendMessage(topicName, new KafkaKey(MessageType.CONTROL_MESSAGE, new byte[]{}), getValue(), 0, null).get();

      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        ConsumerRecords consumerRecords = consumer.poll(1 * Time.MS_PER_SECOND);
        Assert.assertFalse(consumerRecords.isEmpty(), "The destination Kafka cluster should NOT be empty!");
      });
    } finally {
      IOUtils.closeQuietly(mirrorMaker);
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
   * Copied from {@link com.linkedin.venice.kafka.consumer.KafkaStoreIngestionService}
   * @return Properties Kafka properties corresponding to the venice store.
   */
  private static Properties getKafkaConsumerProperties(String kafkaBootstrapServers) {
    Properties kafkaConsumerProperties = new Properties();
    kafkaConsumerProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    kafkaConsumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Venice is persisting offset in local offset db.
    kafkaConsumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    String groupId = TestUtils.getUniqueString("group-id");
    kafkaConsumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    /**
     * Temporarily we are going to use group_id as client_id as well since it is unique in cluster level.
     * With unique client_id, it will be easier for us to check Kafka consumer related metrics through JMX.
     * TODO: Kafka is throttling based on client_id, need to investigate whether we should use Kafka throttling or not.
     */
    kafkaConsumerProperties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, groupId);
    kafkaConsumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        KafkaKeySerializer.class.getName());
    kafkaConsumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        KafkaValueSerializer.class.getName());
    return kafkaConsumerProperties;
  }
}
