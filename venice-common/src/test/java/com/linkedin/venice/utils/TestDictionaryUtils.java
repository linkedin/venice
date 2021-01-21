package com.linkedin.venice.utils;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.integration.utils.KafkaBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.kafka.TopicManager.*;


public class TestDictionaryUtils {

  private static final long MIN_COMPACTION_LAG = 24 * Time.MS_PER_HOUR;

  /** Wait time for {@link #manager} operations, in seconds */
  private static final int WAIT_TIME = 10;
  private KafkaBrokerWrapper kafka;
  private TopicManager manager;
  private MockTime mockTime;

  private String getTopic() {
    String callingFunction = Thread.currentThread().getStackTrace()[2].getMethodName();
    String topicName = TestUtils.getUniqueString(callingFunction);
    int partitions = 1;
    int replicas = 1;
    manager.createTopic(topicName, partitions, replicas, false);
    TestUtils.waitForNonDeterministicAssertion(WAIT_TIME, TimeUnit.SECONDS,
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
  public void setup() {
    mockTime = new MockTime();
    kafka = ServiceFactory.getKafkaBroker(mockTime);
    manager = new TopicManager(DEFAULT_KAFKA_OPERATION_TIMEOUT_MS, 100, MIN_COMPACTION_LAG, TestUtils.getVeniceConsumerFactory(kafka));
  }

  @AfterClass
  public void teardown() throws IOException {
    kafka.close();
    manager.close();
  }

  @Test
  public void testGetDictionary() {
    String topic = getTopic();
    byte[] dictionaryToSend = "TEST_DICT".getBytes();
    Properties props = getKafkaProperties();

    try (VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter = TestUtils.getVeniceWriterFactory(props).createVeniceWriter(topic)) {
      veniceWriter.broadcastStartOfPush(true, false, CompressionStrategy.ZSTD_WITH_DICT, ByteBuffer.wrap(dictionaryToSend), null);
      veniceWriter.broadcastEndOfPush(null);
    }

    ByteBuffer dictionaryFromKafka = DictionaryUtils.readDictionaryFromKafka(topic, new VeniceProperties(props));
    Assert.assertEquals(dictionaryFromKafka.array(), dictionaryToSend);
  }

  @Test
  public void testGetDictionaryReturnsNullWhenNoDictionary() {
    String topic = getTopic();
    Properties props = getKafkaProperties();

    try (VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter = TestUtils.getVeniceWriterFactory(props).createVeniceWriter(topic)) {
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

    try (VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter = TestUtils.getVeniceWriterFactory(props).createVeniceWriter(topic)) {
      veniceWriter.put(new KafkaKey(MessageType.PUT, "blah".getBytes()), "blah".getBytes(), 1, null);
    }

    ByteBuffer dictionaryFromKafka = DictionaryUtils.readDictionaryFromKafka(topic, new VeniceProperties(props));
    Assert.assertNull(dictionaryFromKafka);
  }
}
