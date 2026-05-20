package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.ConfigKeys.KAFKA_CONFIG_PREFIX;
import static com.linkedin.venice.ConfigKeys.PUBSUB_BROKER_ADDRESS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KIF_RECORD_READER_KAFKA_CONFIG_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.NEWER_KME_SCHEMAS_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_CONFIGURATOR_CLASS_CONFIG;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SYSTEM_SCHEMA_READER_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VENICE_REPUSH_SOURCE_PUBSUB_BROKER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.hadoop.ssl.SSLConfigurator;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
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

  @Test
  public void testBuildSchemaAwareDeserializerFallsBackWhenSystemSchemaReaderDisabled() {
    /*
     * Default conf has SYSTEM_SCHEMA_READER_ENABLED unset (false). The helper should log a
     * warning and return the jar-only default deserializer instead of throwing - the on-wire
     * vtp header bootstrap still applies for forward-compat reads.
     */
    PubSubMessageDeserializer deserializer = KafkaInputUtils.buildSchemaAwareDeserializer(VeniceProperties.empty());
    assertNotNull(deserializer);
    assertNotNull(deserializer.getValueSerializer());
  }

  @Test
  public void testBuildSchemaAwareDeserializerBuildsSchemaAwareWhenEnabled() {
    /*
     * When SYSTEM_SCHEMA_READER_ENABLED is true and the VPJ driver has broadcast the
     * newer.kme.schemas.* entries, the helper builds a KmeSchemaReader-backed deserializer.
     */
    Properties props = new Properties();
    props.setProperty(SYSTEM_SCHEMA_READER_ENABLED, "true");
    int currentVersion = AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion();
    props.setProperty(
        NEWER_KME_SCHEMAS_PREFIX + currentVersion,
        AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersionSchema().toString());

    PubSubMessageDeserializer deserializer = KafkaInputUtils.buildSchemaAwareDeserializer(new VeniceProperties(props));
    assertNotNull(deserializer);
    assertNotNull(deserializer.getValueSerializer());
  }

  @Test
  public void testBuildSchemaAwareOptimizedDeserializerFallsBackWhenSystemSchemaReaderDisabled() {
    /*
     * Same fallback semantics as the non-optimized variant: when SYSTEM_SCHEMA_READER_ENABLED
     * is unset, the optimized helper logs once per JVM and returns a jar-only OPTIMIZED
     * deserializer (reused decoders) for Spark-style hot read paths.
     */
    PubSubMessageDeserializer deserializer =
        KafkaInputUtils.buildSchemaAwareOptimizedDeserializer(VeniceProperties.empty());
    assertNotNull(deserializer);
    assertNotNull(deserializer.getValueSerializer());
  }

  @Test
  public void testBuildSchemaAwareOptimizedDeserializerBuildsSchemaAwareWhenEnabled() {
    /*
     * When the system flag is on and newer.kme.schemas.* entries are present, the optimized
     * variant builds a KmeSchemaReader-backed OptimizedKafkaValueSerializer for the Spark hot path.
     */
    Properties props = new Properties();
    props.setProperty(SYSTEM_SCHEMA_READER_ENABLED, "true");
    int currentVersion = AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersion();
    props.setProperty(
        NEWER_KME_SCHEMAS_PREFIX + currentVersion,
        AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getCurrentProtocolVersionSchema().toString());

    PubSubMessageDeserializer deserializer =
        KafkaInputUtils.buildSchemaAwareOptimizedDeserializer(new VeniceProperties(props));
    assertNotNull(deserializer);
    assertNotNull(deserializer.getValueSerializer());
  }

  @Test
  public void testBuildSchemaAwareDeserializerFallsBackWhenSystemSchemaReaderEnabledButBroadcastIsEmpty() {
    /*
     * Misconfig case: SYSTEM_SCHEMA_READER_ENABLED is true but the VPJ driver failed to
     * populate the newer.kme.schemas.* broadcast. The helper logs once per JVM and falls
     * back to the jar-only deserializer (previously this case silently built a useless
     * SchemaReader with an empty map).
     */
    Properties props = new Properties();
    props.setProperty(SYSTEM_SCHEMA_READER_ENABLED, "true");

    PubSubMessageDeserializer deserializer = KafkaInputUtils.buildSchemaAwareDeserializer(new VeniceProperties(props));
    assertNotNull(deserializer);
    assertNotNull(deserializer.getValueSerializer());
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
}
