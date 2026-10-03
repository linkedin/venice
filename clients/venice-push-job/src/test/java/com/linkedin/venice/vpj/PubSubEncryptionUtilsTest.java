package com.linkedin.venice.vpj;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_BROKER_ADDRESS;
import static com.linkedin.venice.ConfigKeys.PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUB_SUB_ENCRYPTION_KEY_URN;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.hadoop.input.kafka.KafkaInputUtilsTest.RecordingPubSubConsumerAdapterFactory;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.function.Function;
import org.testng.annotations.Test;


public class PubSubEncryptionUtilsTest {
  private static final String KEY_URN = "urn:li:kmsKeyLineage:test-key";

  @Test
  public void testLookupIgnoresStoreNameArgument() {
    Function<String, String> lookup = PubSubEncryptionUtils.getKeyUrnLookup(KEY_URN);

    assertEquals(lookup.apply("store-a"), KEY_URN);
    assertEquals(lookup.apply("store-b"), KEY_URN);
    assertEquals(lookup.apply(null), KEY_URN);
  }

  @Test
  public void testBlankUrnYieldsNullLookup() {
    assertNull(PubSubEncryptionUtils.getKeyUrnLookup(""));
    assertNull(PubSubEncryptionUtils.getKeyUrnLookup("   "));
  }

  @Test
  public void testUrnIsTrimmed() {
    assertEquals(PubSubEncryptionUtils.getKeyUrnLookup("  " + KEY_URN + "  ").apply("my-store"), KEY_URN);
  }

  @Test
  public void testPropertiesOverloadReadsThreadedUrn() {
    Properties properties = new Properties();
    properties.setProperty(PUB_SUB_ENCRYPTION_KEY_URN, KEY_URN);

    assertEquals(PubSubEncryptionUtils.getKeyUrnLookup(properties).apply("my-store"), KEY_URN);
  }

  @Test
  public void testPropertiesOverloadWithoutUrnYieldsNullLookup() {
    assertNull(PubSubEncryptionUtils.getKeyUrnLookup(new Properties()));
  }

  /**
   * Verifies readDictionaryFromKafka derives the encryption lookup from the same props it reads the topic with,
   * instead of requiring a caller-supplied lookup — see the method's javadoc for why every VPJ call site already
   * threads PUB_SUB_ENCRYPTION_KEY_URN through props, making a second caller-derived copy redundant. Reuses
   * KafkaInputUtilsTest's RecordingPubSubConsumerAdapterFactory test double rather than redefining one, same as
   * KafkaInputRecordReaderTest does for its own encryption-lookup regression test.
   */
  @Test
  public void testReadDictionaryFromKafkaDerivesLookupFromProps() {
    RecordingPubSubConsumerAdapterFactory.reset();

    Properties props = new Properties();
    props.setProperty(PUBSUB_BROKER_ADDRESS, "localhost:9092");
    props.setProperty(KAFKA_BOOTSTRAP_SERVERS, "localhost:9092");
    props.setProperty(PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS, RecordingPubSubConsumerAdapterFactory.class.getName());
    props.setProperty(PUB_SUB_ENCRYPTION_KEY_URN, KEY_URN);

    ByteBuffer dict = PubSubEncryptionUtils.readDictionaryFromKafka("test_store_v1", new VeniceProperties(props));

    assertEquals(dict.array(), RecordingPubSubConsumerAdapterFactory.DICTIONARY_TO_SERVE);
    Function<String, String> observedLookup = RecordingPubSubConsumerAdapterFactory.getObservedEncryptionKeyUrnLookup();
    assertEquals(observedLookup.apply("any-store"), KEY_URN);
  }

  /**
   * Companion to the test above: confirms non-encrypted (the common case) dictionary reads are unaffected — when
   * PUB_SUB_ENCRYPTION_KEY_URN isn't configured, the consumer context's lookup stays null, same as before.
   */
  @Test
  public void testReadDictionaryFromKafkaLeavesLookupNullWhenUrnNotConfigured() {
    RecordingPubSubConsumerAdapterFactory.reset();

    Properties props = new Properties();
    props.setProperty(PUBSUB_BROKER_ADDRESS, "localhost:9092");
    props.setProperty(KAFKA_BOOTSTRAP_SERVERS, "localhost:9092");
    props.setProperty(PUBSUB_CONSUMER_ADAPTER_FACTORY_CLASS, RecordingPubSubConsumerAdapterFactory.class.getName());

    ByteBuffer dict = PubSubEncryptionUtils.readDictionaryFromKafka("test_store_v1", new VeniceProperties(props));

    assertEquals(dict.array(), RecordingPubSubConsumerAdapterFactory.DICTIONARY_TO_SERVE);
    assertNull(RecordingPubSubConsumerAdapterFactory.getObservedEncryptionKeyUrnLookup());
  }
}
