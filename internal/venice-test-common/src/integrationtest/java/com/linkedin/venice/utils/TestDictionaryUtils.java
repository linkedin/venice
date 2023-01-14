package com.linkedin.venice.utils;

import static com.linkedin.venice.kafka.TopicManager.DEFAULT_KAFKA_OPERATION_TIMEOUT_MS;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.ZkServerWrapper;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.writer.VeniceWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestDictionaryUtils {
  private static final long MIN_COMPACTION_LAG = 24 * Time.MS_PER_HOUR;

  /** Wait time for {@link #manager} operations, in seconds */
  private static final int WAIT_TIME = 10;
  private static final int PARTITION_COUNT = 1;
  private KafkaBrokerWrapper kafka;
  private ZkServerWrapper zkServer;
  private TopicManager manager;
  private MockTime mockTime;

  private String getTopic() {
    String callingFunction = Thread.currentThread().getStackTrace()[2].getMethodName();
    String topicName = Utils.getUniqueString(callingFunction);
    int replicas = 1;
    manager.createTopic(topicName, PARTITION_COUNT, replicas, false);
    TestUtils.waitForNonDeterministicAssertion(
        WAIT_TIME,
        TimeUnit.SECONDS,
        () -> Assert.assertTrue(manager.containsTopicAndAllPartitionsAreOnline(topicName)));
    return topicName;
  }

  private Properties getKafkaProperties() {
    Properties props = new Properties();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, manager.getKafkaBootstrapServers());
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class.getName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaValueSerializer.class.getName());
    props.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, kafka.getAddress());
    props.put(ConfigKeys.PARTITIONER_CLASS, DefaultVenicePartitioner.class.getName());
    return props;
  }

  @BeforeClass
  public void setUp() {
    zkServer = ServiceFactory.getZkServer();
    mockTime = new MockTime();
    kafka = ServiceFactory.getKafkaBroker(zkServer, Optional.of(mockTime));
    manager = new TopicManager(
        DEFAULT_KAFKA_OPERATION_TIMEOUT_MS,
        100,
        MIN_COMPACTION_LAG,
        IntegrationTestPushUtils.getVeniceConsumerFactory(kafka));
  }

  @AfterClass
  public void cleanUp() throws IOException {
    kafka.close();
    manager.close();
    zkServer.close();
  }

  @Test
  public void testGetDictionary() {
    String topic = getTopic();
    byte[] dictionaryToSend = "TEST_DICT".getBytes();
    Properties props = getKafkaProperties();

    try (VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter =
        TestUtils.getVeniceWriterFactory(props).createVeniceWriter(topic, PARTITION_COUNT)) {
      veniceWriter.broadcastStartOfPush(
          true,
          false,
          CompressionStrategy.ZSTD_WITH_DICT,
          Optional.of(ByteBuffer.wrap(dictionaryToSend)),
          null);
      veniceWriter.broadcastEndOfPush(null);
    }

    ByteBuffer dictionaryFromKafka = DictionaryUtils.readDictionaryFromKafka(topic, new VeniceProperties(props));
    Assert.assertEquals(dictionaryFromKafka.array(), dictionaryToSend);
  }

  @Test
  public void testGetDictionaryReturnsNullWhenNoDictionary() {
    String topic = getTopic();
    Properties props = getKafkaProperties();

    try (VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter =
        TestUtils.getVeniceWriterFactory(props).createVeniceWriter(topic, PARTITION_COUNT)) {
      veniceWriter.broadcastStartOfPush(true, false, CompressionStrategy.ZSTD_WITH_DICT, null);
      veniceWriter.broadcastEndOfPush(null);
    }

    ByteBuffer dictionaryFromKafka = DictionaryUtils.readDictionaryFromKafka(topic, new VeniceProperties(props));
    Assert.assertNull(dictionaryFromKafka);
  }

  @Test
  public void testGetDictionaryReturnsNullWhenNoSOP() {
    String topic = getTopic();
    Properties props = getKafkaProperties();

    try (VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter =
        TestUtils.getVeniceWriterFactory(props).createVeniceWriter(topic, PARTITION_COUNT)) {
      veniceWriter.put(new KafkaKey(MessageType.PUT, "blah".getBytes()), "blah".getBytes(), 1, null);
    }

    ByteBuffer dictionaryFromKafka = DictionaryUtils.readDictionaryFromKafka(topic, new VeniceProperties(props));
    Assert.assertNull(dictionaryFromKafka);
  }
}
