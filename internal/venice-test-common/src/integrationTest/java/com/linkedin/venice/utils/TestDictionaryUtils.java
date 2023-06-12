package com.linkedin.venice.utils;

import static com.linkedin.venice.kafka.TopicManager.DEFAULT_KAFKA_OPERATION_TIMEOUT_MS;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.integration.utils.PubSubBrokerConfigs;
import com.linkedin.venice.integration.utils.PubSubBrokerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestDictionaryUtils {
  private static final long MIN_COMPACTION_LAG = 24 * Time.MS_PER_HOUR;

  /** Wait time for {@link #manager} operations, in seconds */
  private static final int WAIT_TIME = 10;
  private static final int PARTITION_COUNT = 1;
  private PubSubBrokerWrapper kafka;
  private TopicManager manager;
  private TestMockTime mockTime;
  private final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  private String getTopic() {
    String callingFunction = Thread.currentThread().getStackTrace()[2].getMethodName();
    PubSubTopic pubSubTopic =
        pubSubTopicRepository.getTopic(Version.composeKafkaTopic(Utils.getUniqueString(callingFunction), 1));
    int replicas = 1;
    manager.createTopic(pubSubTopic, PARTITION_COUNT, replicas, false);
    TestUtils.waitForNonDeterministicAssertion(
        WAIT_TIME,
        TimeUnit.SECONDS,
        () -> Assert.assertTrue(manager.containsTopicAndAllPartitionsAreOnline(pubSubTopic)));
    return pubSubTopic.getName();
  }

  private Properties getKafkaProperties() {
    Properties props = new Properties();
    props.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, manager.getKafkaBootstrapServers());
    props.put(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, kafka.getAddress());
    props.put(ConfigKeys.PARTITIONER_CLASS, DefaultVenicePartitioner.class.getName());
    return props;
  }

  @BeforeClass
  public void setUp() {
    mockTime = new TestMockTime();
    kafka = ServiceFactory.getPubSubBroker(new PubSubBrokerConfigs.Builder().setMockTime(mockTime).build());
    manager =
        IntegrationTestPushUtils
            .getTopicManagerRepo(
                DEFAULT_KAFKA_OPERATION_TIMEOUT_MS,
                100,
                MIN_COMPACTION_LAG,
                kafka.getAddress(),
                pubSubTopicRepository)
            .getTopicManager();
  }

  @AfterClass
  public void cleanUp() throws IOException {
    kafka.close();
    manager.close();
  }

  @Test
  public void testGetDictionary() {
    String topic = getTopic();
    byte[] dictionaryToSend = "TEST_DICT".getBytes();
    Properties props = getKafkaProperties();

    try (VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter = TestUtils.getVeniceWriterFactory(props)
        .createVeniceWriter(
            new VeniceWriterOptions.Builder(topic).setUseKafkaKeySerializer(true)
                .setPartitionCount(PARTITION_COUNT)
                .build())) {
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

    try (VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter = TestUtils.getVeniceWriterFactory(props)
        .createVeniceWriter(
            new VeniceWriterOptions.Builder(topic).setUseKafkaKeySerializer(true)
                .setPartitionCount(PARTITION_COUNT)
                .build())) {
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

    try (VeniceWriter<KafkaKey, byte[], byte[]> veniceWriter = TestUtils.getVeniceWriterFactory(props)
        .createVeniceWriter(
            new VeniceWriterOptions.Builder(topic).setUseKafkaKeySerializer(true)
                .setPartitionCount(PARTITION_COUNT)
                .build())) {
      veniceWriter.put(new KafkaKey(MessageType.PUT, "blah".getBytes()), "blah".getBytes(), 1, null);
    }

    ByteBuffer dictionaryFromKafka = DictionaryUtils.readDictionaryFromKafka(topic, new VeniceProperties(props));
    Assert.assertNull(dictionaryFromKafka);
  }
}
