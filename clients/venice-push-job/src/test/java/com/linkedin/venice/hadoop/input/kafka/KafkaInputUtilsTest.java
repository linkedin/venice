package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_CONFIG_PREFIX;
import static com.linkedin.venice.ConfigKeys.PUBSUB_BROKER_ADDRESS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KIF_RECORD_READER_KAFKA_CONFIG_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUB_SUB_ENCRYPTION_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUB_SUB_ENCRYPTION_KEY_URN;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_CONFIGURATOR_CLASS_CONFIG;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_REPUSH_SOURCE_PUBSUB_BROKER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
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
import com.linkedin.venice.vpj.PubSubEncryptionUtilsTest;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.Credentials;
import org.apache.kafka.clients.CommonClientConfigs;
import org.testng.Assert;
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

  @Test(dataProvider = "encryptionConfigurations", dataProviderClass = PubSubEncryptionUtilsTest.class)
  public void testGetCompressorSelectsDictionaryReaderFromFlag(Boolean enabled, String keyUrn, boolean invalid)
      throws IOException {
    RecordingPubSubConsumerAdapterFactory.reset();

    Properties props = new Properties();
    props.setProperty(PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS, RecordingPubSubConsumerAdapterFactory.class.getName());
    if (enabled != null) {
      props.setProperty(PUB_SUB_ENCRYPTION_ENABLED, enabled.toString());
    }
    if (keyUrn != null) {
      props.setProperty(PUB_SUB_ENCRYPTION_KEY_URN, keyUrn);
    }
    VeniceProperties veniceProperties = new VeniceProperties(props);

    CompressorFactory compressorFactory = new CompressorFactory();
    try {
      if (invalid) {
        Assert.expectThrows(
            VeniceException.class,
            () -> KafkaInputUtils.getCompressor(
                compressorFactory,
                CompressionStrategy.ZSTD_WITH_DICT,
                "correct-source-broker:9092",
                "test_store_v1",
                veniceProperties));
        assertEquals(RecordingPubSubConsumerAdapterFactory.getCreateCount(), 0);
        assertEquals(RecordingPubSubConsumerAdapterFactory.getPollCount(), 0);
        return;
      }
      VeniceCompressor compressor = KafkaInputUtils.getCompressor(
          compressorFactory,
          CompressionStrategy.ZSTD_WITH_DICT,
          "correct-source-broker:9092",
          "test_store_v1",
          veniceProperties);
      assertNotNull(compressor);
      Function<String, String> observedLookup =
          RecordingPubSubConsumerAdapterFactory.getObservedEncryptionKeyUrnLookup();
      if (Boolean.TRUE.equals(enabled)) {
        assertNotNull(observedLookup);
        assertEquals(observedLookup.apply("test_store"), keyUrn.trim());
      } else {
        assertNull(observedLookup);
        assertNull(
            RecordingPubSubConsumerAdapterFactory.getObservedContext()
                .getVeniceProperties()
                .getString(PUB_SUB_ENCRYPTION_KEY_URN, (String) null));
      }
      assertEquals(RecordingPubSubConsumerAdapterFactory.getCreateCount(), 1);
      assertEquals(RecordingPubSubConsumerAdapterFactory.getPollCount(), 1);
    } finally {
      compressorFactory.close();
    }
  }

  @Test(dataProvider = "encryptionConfigurations", dataProviderClass = PubSubEncryptionUtilsTest.class)
  public void testReaderOverridesCannotChangeEncryptionConfig(Boolean enabled, String keyUrn, boolean invalid) {
    jobConf.set(VENICE_REPUSH_SOURCE_PUBSUB_BROKER, "source-broker");
    if (enabled != null) {
      jobConf.setBoolean(PUB_SUB_ENCRYPTION_ENABLED, enabled);
    }
    if (keyUrn != null) {
      jobConf.set(PUB_SUB_ENCRYPTION_KEY_URN, keyUrn);
    }
    jobConf.set(KIF_RECORD_READER_KAFKA_CONFIG_PREFIX + PUB_SUB_ENCRYPTION_ENABLED, "true");
    jobConf.set(KIF_RECORD_READER_KAFKA_CONFIG_PREFIX + PUB_SUB_ENCRYPTION_KEY_URN, "urn:li:stale");
    Properties overrides = new Properties();
    overrides.setProperty(PUB_SUB_ENCRYPTION_ENABLED, Boolean.toString(!Boolean.TRUE.equals(enabled)));
    overrides.setProperty(PUB_SUB_ENCRYPTION_KEY_URN, "urn:li:override");
    overrides.setProperty("unrelated", "retained");

    VeniceProperties actual = KafkaInputUtils.getConsumerProperties(jobConf, overrides);

    assertEquals(actual.getBoolean(PUB_SUB_ENCRYPTION_ENABLED), Boolean.TRUE.equals(enabled));
    assertEquals(
        actual.getString(PUB_SUB_ENCRYPTION_KEY_URN, (String) null),
        Boolean.TRUE.equals(enabled) ? keyUrn : null);
    assertEquals(actual.getString("unrelated"), "retained");
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

  @Test
  public void testDictionaryFixtureReturnsDefensiveCopies() {
    byte[] expected = RecordingPubSubConsumerAdapterFactory.getDictionaryToServe().clone();
    byte[] dictionary = RecordingPubSubConsumerAdapterFactory.getDictionaryToServe();

    dictionary[0]++;

    assertEquals(RecordingPubSubConsumerAdapterFactory.getDictionaryToServe(), expected);
  }

  /** Records consumer context and serves a synthetic dictionary SOP without a broker. */
  public static class RecordingPubSubConsumerAdapterFactory
      extends PubSubConsumerAdapterFactory<PubSubConsumerAdapter> {
    private static final byte[] DICTIONARY_TO_SERVE = { 1, 2, 3, 4 };
    private static final AtomicInteger CREATE_COUNT = new AtomicInteger();
    private static final AtomicInteger POLL_COUNT = new AtomicInteger();
    private static final AtomicReference<PubSubConsumerAdapterContext> OBSERVED_CONTEXT = new AtomicReference<>();
    private static final AtomicReference<String> OBSERVED_BROKER_ADDRESS = new AtomicReference<>();
    private static final AtomicReference<String> OBSERVED_BOOTSTRAP_SERVERS = new AtomicReference<>();
    private static final AtomicReference<Function<String, String>> OBSERVED_ENCRYPTION_KEY_URN_LOOKUP =
        new AtomicReference<>();

    public static void reset() {
      CREATE_COUNT.set(0);
      POLL_COUNT.set(0);
      OBSERVED_CONTEXT.set(null);
      OBSERVED_BROKER_ADDRESS.set(null);
      OBSERVED_BOOTSTRAP_SERVERS.set(null);
      OBSERVED_ENCRYPTION_KEY_URN_LOOKUP.set(null);
    }

    public static int getCreateCount() {
      return CREATE_COUNT.get();
    }

    public static byte[] getDictionaryToServe() {
      return DICTIONARY_TO_SERVE.clone();
    }

    public static int getPollCount() {
      return POLL_COUNT.get();
    }

    public static PubSubConsumerAdapterContext getObservedContext() {
      return OBSERVED_CONTEXT.get();
    }

    static String getObservedBrokerAddress() {
      return OBSERVED_BROKER_ADDRESS.get();
    }

    static String getObservedBootstrapServers() {
      return OBSERVED_BOOTSTRAP_SERVERS.get();
    }

    public static Function<String, String> getObservedEncryptionKeyUrnLookup() {
      return OBSERVED_ENCRYPTION_KEY_URN_LOOKUP.get();
    }

    @Override
    public PubSubConsumerAdapter create(PubSubConsumerAdapterContext context) {
      CREATE_COUNT.incrementAndGet();
      OBSERVED_CONTEXT.set(context);
      VeniceProperties properties = context.getVeniceProperties();
      OBSERVED_BROKER_ADDRESS.set(properties.getString(PUBSUB_BROKER_ADDRESS));
      OBSERVED_BOOTSTRAP_SERVERS.set(properties.getString(KAFKA_BOOTSTRAP_SERVERS));
      OBSERVED_ENCRYPTION_KEY_URN_LOOKUP.set(context.getPubSubEncryptionKeyUrnLookup());

      PubSubConsumerAdapter consumer = mock(PubSubConsumerAdapter.class);
      when(consumer.getAssignment()).thenReturn(Collections.emptySet());
      AtomicReference<PubSubTopicPartition> subscribedPartition = new AtomicReference<>();
      doAnswer(invocation -> {
        subscribedPartition.set(invocation.getArgument(0));
        return null;
      }).when(consumer).subscribe(any(PubSubTopicPartition.class), any(PubSubPosition.class));
      doAnswer(invocation -> {
        POLL_COUNT.incrementAndGet();
        PubSubTopicPartition topicPartition = subscribedPartition.get();
        return Collections
            .singletonMap(topicPartition, Collections.singletonList(createStartOfPushMessage(topicPartition)));
      }).when(consumer).poll(anyLong());
      return consumer;
    }

    private static DefaultPubSubMessage createStartOfPushMessage(PubSubTopicPartition topicPartition) {
      StartOfPush startOfPush = new StartOfPush();
      startOfPush.compressionStrategy = CompressionStrategy.ZSTD_WITH_DICT.getValue();
      startOfPush.compressionDictionary = ByteBuffer.wrap(getDictionaryToServe());

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
