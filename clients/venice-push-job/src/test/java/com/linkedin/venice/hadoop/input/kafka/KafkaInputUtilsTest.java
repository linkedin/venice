package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_CONFIG_PREFIX;
import static com.linkedin.venice.ConfigKeys.PUBSUB_BROKER_ADDRESS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KIF_RECORD_READER_KAFKA_CONFIG_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_CONFIGURATOR_CLASS_CONFIG;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_REPUSH_SOURCE_PUBSUB_BROKER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.hadoop.ssl.SSLConfigurator;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.StartOfPush;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.Credentials;
import org.apache.kafka.clients.CommonClientConfigs;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class KafkaInputUtilsTest {
  private JobConf jobConf;

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    jobConf = new JobConf();
  }

  @Test
  public void testGetConsumerPropertiesWithoutSSL() {
    jobConf = new JobConf();
    jobConf.set(VENICE_REPUSH_SOURCE_PUBSUB_BROKER, "localhost:9092");

    VeniceProperties consumerProps = KafkaInputUtils.getConsumerProperties(jobConf);
    System.out.println("Consumer properties: " + consumerProps);

    assertEquals(
        consumerProps.getString(PUBSUB_BROKER_ADDRESS),
        "localhost:9092",
        "PubSub broker address should match the configured value");

    assertEquals(
        consumerProps.getLong(KAFKA_CONFIG_PREFIX + CommonClientConfigs.RECEIVE_BUFFER_CONFIG),
        4L * 1024 * 1024,
        "Receive buffer size should be set to 4MB");
  }

  @Test
  public void testPrefixedPropertiesAreClippedAndMerged() {
    jobConf.set(VENICE_REPUSH_SOURCE_PUBSUB_BROKER, "localhost:9095");
    jobConf.set(KIF_RECORD_READER_KAFKA_CONFIG_PREFIX + "some.kafka.prop", "value123");

    VeniceProperties consumerProps = KafkaInputUtils.getConsumerProperties(jobConf);

    assertEquals(
        consumerProps.getString("some.kafka.prop"),
        "value123",
        "Prefixed Kafka property should be merged correctly");
  }

  @Test
  public void testGetConsumerPropertiesWithSSLConfigurator() {
    jobConf.set(VENICE_REPUSH_SOURCE_PUBSUB_BROKER, "localhost:9093");
    jobConf.set(SSL_CONFIGURATOR_CLASS_CONFIG, DummySSLConfigurator.class.getName());
    jobConf.set(KIF_RECORD_READER_KAFKA_CONFIG_PREFIX + "some.kafka.prop", "value123");
    VeniceProperties consumerProps = KafkaInputUtils.getConsumerProperties(jobConf);
    assertEquals(consumerProps.getString("ssl.test.property"), "sslValue", "SSL property should be merged");
    assertEquals(consumerProps.getString(PUBSUB_BROKER_ADDRESS), "localhost:9093");
    assertEquals(
        consumerProps.getString("some.kafka.prop"),
        "value123",
        "Prefixed Kafka property should be merged correctly");
  }

  /**
   * Regression test for a broker-precedence bug found in review of PR #2975: getCompressor() only set
   * KAFKA_BOOTSTRAP_SERVERS on the properties handed to the dictionary consumer, so a stale/incorrect
   * pubsub.broker.address already present in the input properties (e.g. pointing at the destination
   * broker) would silently take precedence over the intended source broker (see
   * PubSubUtil#getPubSubBrokerAddress, which checks PUBSUB_BROKER_ADDRESS before falling back to
   * KAFKA_BOOTSTRAP_SERVERS). Verifies PUBSUB_BROKER_ADDRESS is explicitly overridden with the source
   * kafkaUrl, mirroring KafkaInputUtils#getConsumerProperties.
   */
  @Test
  public void testGetCompressorOverridesStalePubSubBrokerAddressForZstdWithDict() throws IOException {
    RecordingPubSubConsumerAdapterFactory.reset();

    Properties props = new Properties();
    props.setProperty(PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS, RecordingPubSubConsumerAdapterFactory.class.getName());
    // A stale/incorrect broker address already present on the input properties (e.g. left over from the
    // destination cluster config) must not win over the source kafkaUrl passed to getCompressor().
    props.setProperty(PUBSUB_BROKER_ADDRESS, "stale-destination-broker:9999");
    VeniceProperties veniceProperties = new VeniceProperties(props);

    CompressorFactory compressorFactory = new CompressorFactory();
    try {
      VeniceCompressor compressor = KafkaInputUtils.getCompressor(
          compressorFactory,
          CompressionStrategy.ZSTD_WITH_DICT,
          "correct-source-broker:9092",
          "test_store_v1",
          veniceProperties);
      assertNotNull(compressor);
      assertEquals(
          RecordingPubSubConsumerAdapterFactory.getObservedBrokerAddress(),
          "correct-source-broker:9092",
          "PUBSUB_BROKER_ADDRESS seen by the dictionary consumer factory should be the source kafkaUrl, "
              + "not the stale value already present in the input properties");
      assertEquals(
          RecordingPubSubConsumerAdapterFactory.getObservedBootstrapServers(),
          "correct-source-broker:9092",
          "KAFKA_BOOTSTRAP_SERVERS seen by the dictionary consumer factory should also be the source kafkaUrl");
    } finally {
      compressorFactory.close();
    }
  }

  /**
   * Dummy SSLConfigurator for simulating successful SSL config setup.
   */
  public static class DummySSLConfigurator implements SSLConfigurator {
    @Override
    public Properties setupSSLConfig(Properties properties, Credentials userCredentials) {
      Properties sslProps = new Properties();
      sslProps.setProperty("ssl.test.property", "sslValue");
      return sslProps;
    }
  }

  /**
   * Test double for {@link PubSubConsumerAdapterFactory} that records the PUBSUB_BROKER_ADDRESS and
   * KAFKA_BOOTSTRAP_SERVERS properties it's constructed with and returns a mock consumer that serves a
   * minimal StartOfPush control message, so getCompressor()'s ZSTD_WITH_DICT dictionary read succeeds
   * without needing a real Kafka broker.
   */
  public static class RecordingPubSubConsumerAdapterFactory
      extends PubSubConsumerAdapterFactory<PubSubConsumerAdapter> {
    private static final AtomicReference<String> OBSERVED_BROKER_ADDRESS = new AtomicReference<>();
    private static final AtomicReference<String> OBSERVED_BOOTSTRAP_SERVERS = new AtomicReference<>();

    static void reset() {
      OBSERVED_BROKER_ADDRESS.set(null);
      OBSERVED_BOOTSTRAP_SERVERS.set(null);
    }

    static String getObservedBrokerAddress() {
      return OBSERVED_BROKER_ADDRESS.get();
    }

    static String getObservedBootstrapServers() {
      return OBSERVED_BOOTSTRAP_SERVERS.get();
    }

    @Override
    public PubSubConsumerAdapter create(PubSubConsumerAdapterContext context) {
      VeniceProperties properties = context.getVeniceProperties();
      OBSERVED_BROKER_ADDRESS.set(properties.getString(PUBSUB_BROKER_ADDRESS));
      OBSERVED_BOOTSTRAP_SERVERS.set(properties.getString(KAFKA_BOOTSTRAP_SERVERS));

      PubSubConsumerAdapter consumer = mock(PubSubConsumerAdapter.class);
      when(consumer.getAssignment()).thenReturn(Collections.emptySet());
      AtomicReference<PubSubTopicPartition> subscribedPartition = new AtomicReference<>();
      doAnswer(invocation -> {
        subscribedPartition.set(invocation.getArgument(0));
        return null;
      }).when(consumer).subscribe(any(PubSubTopicPartition.class), any(PubSubPosition.class));
      doAnswer(invocation -> {
        PubSubTopicPartition topicPartition = subscribedPartition.get();
        return Collections
            .singletonMap(topicPartition, Collections.singletonList(createStartOfPushMessage(topicPartition)));
      }).when(consumer).poll(anyLong());
      return consumer;
    }

    private static DefaultPubSubMessage createStartOfPushMessage(PubSubTopicPartition topicPartition) {
      StartOfPush startOfPush = new StartOfPush();
      startOfPush.compressionStrategy = CompressionStrategy.ZSTD_WITH_DICT.getValue();
      startOfPush.compressionDictionary = ByteBuffer.wrap(new byte[] { 1, 2, 3, 4 });

      ControlMessage controlMessage = new ControlMessage();
      controlMessage.controlMessageType = ControlMessageType.START_OF_PUSH.getValue();
      controlMessage.controlMessageUnion = startOfPush;
      KafkaMessageEnvelope envelope =
          new KafkaMessageEnvelope(MessageType.CONTROL_MESSAGE.getValue(), null, controlMessage, null);
      return new ImmutablePubSubMessage(
          new KafkaKey(MessageType.CONTROL_MESSAGE, new byte[0]),
          envelope,
          topicPartition,
          ApacheKafkaOffsetPosition.of(0),
          0L,
          0);
    }

    @Override
    public String getName() {
      return RecordingPubSubConsumerAdapterFactory.class.getSimpleName();
    }

    @Override
    public void close() throws IOException {
    }
  }
}
