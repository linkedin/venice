package com.linkedin.venice.hadoop.input.kafka;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_CONFIG_PREFIX;
import static com.linkedin.venice.ConfigKeys.PUBSUB_BROKER_ADDRESS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KAFKA_INPUT_BROKER_URL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.KIF_RECORD_READER_KAFKA_CONFIG_PREFIX;
import static com.linkedin.venice.vpj.VenicePushJobConstants.SSL_CONFIGURATOR_CLASS_CONFIG;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.hadoop.ssl.SSLConfigurator;
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
    jobConf.set(KAFKA_INPUT_BROKER_URL, "localhost:9092");

    VeniceProperties consumerProps = KafkaInputUtils.getConsumerProperties(jobConf);
    System.out.println("Consumer properties: " + consumerProps);

    assertEquals(
        consumerProps.getString(KAFKA_BOOTSTRAP_SERVERS),
        "localhost:9092",
        "Kafka bootstrap servers should match the configured value");

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
    jobConf.set(KAFKA_INPUT_BROKER_URL, "localhost:9095");
    jobConf.set(KIF_RECORD_READER_KAFKA_CONFIG_PREFIX + "some.kafka.prop", "value123");

    VeniceProperties consumerProps = KafkaInputUtils.getConsumerProperties(jobConf);

    assertEquals(
        consumerProps.getString("some.kafka.prop"),
        "value123",
        "Prefixed Kafka property should be merged correctly");
  }

  @Test
  public void testGetConsumerPropertiesWithSSLConfigurator() {
    jobConf.set(KAFKA_INPUT_BROKER_URL, "localhost:9093");
    jobConf.set(SSL_CONFIGURATOR_CLASS_CONFIG, DummySSLConfigurator.class.getName());
    jobConf.set(KIF_RECORD_READER_KAFKA_CONFIG_PREFIX + "some.kafka.prop", "value123");
    VeniceProperties consumerProps = KafkaInputUtils.getConsumerProperties(jobConf);
    assertEquals(consumerProps.getString("ssl.test.property"), "sslValue", "SSL property should be merged");
    assertEquals(consumerProps.getString(KAFKA_BOOTSTRAP_SERVERS), "localhost:9093");
    assertEquals(consumerProps.getString(PUBSUB_BROKER_ADDRESS), "localhost:9093");
    assertEquals(
        consumerProps.getString("some.kafka.prop"),
        "value123",
        "Prefixed Kafka property should be merged correctly");
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
