package com.linkedin.venice.pubsub.adapter.kafka.producer;

import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_KEY_SERIALIZER;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_VALUE_SERIALIZER;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.SSL_KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.SSL_TO_KAFKA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.testng.annotations.Test;


public class ApacheKafkaProducerConfigTest {
  private static final String KAFKA_BROKER_ADDR = "kafka.broker.com:8181";
  private static final String PRODUCER_NAME = "sender-store_v1";

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Required property: kafka.bootstrap.servers is missing.*")
  public void testConfiguratorThrowsAnExceptionWhenBrokerAddressIsMissing() {
    VeniceProperties veniceProperties = new VeniceProperties();
    new ApacheKafkaProducerConfig(veniceProperties, null, PRODUCER_NAME, true);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*requiredConfigKey: 'key.serializer', requiredConfigValue:.*")
  public void testValidateAndUpdatePropertiesShouldThrowAnErrorIfKeySerIsIncorrect() {
    Properties props = new Properties();
    props.put(KAFKA_KEY_SERIALIZER, Object.class.getName());
    new ApacheKafkaProducerConfig(new VeniceProperties(props), KAFKA_BROKER_ADDR, PRODUCER_NAME, true);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*requiredConfigKey: 'value.serializer', requiredConfigValue:.*")
  public void testValidateAndUpdatePropertiesShouldThrowAnErrorIfValSerIsIncorrect() {
    Properties props = new Properties();
    props.put(KAFKA_KEY_SERIALIZER, KafkaKeySerializer.class.getName());
    props.put(KAFKA_VALUE_SERIALIZER, Object.class.getName());
    new ApacheKafkaProducerConfig(new VeniceProperties(props), KAFKA_BROKER_ADDR, PRODUCER_NAME, true);
  }

  @Test
  public void testValidateOrPopulatePropCanFillMissingConfigs() {
    Properties props = new Properties();
    Properties resultantProps =
        new ApacheKafkaProducerConfig(new VeniceProperties(props), KAFKA_BROKER_ADDR, PRODUCER_NAME, true)
            .getProducerProperties();
    assertTrue(resultantProps.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
    assertTrue(resultantProps.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Failed to load the specified class: ThisIsBogusClass for key: key.serializer")
  public void testValidateClassPropFailsToLoadGarbageClass() {
    Properties props = new Properties();
    props.put(KAFKA_KEY_SERIALIZER, "ThisIsBogusClass");
    new ApacheKafkaProducerConfig(new VeniceProperties(props), KAFKA_BROKER_ADDR, PRODUCER_NAME, false);
  }

  @Test
  public void testGetBrokerAddress() {
    // broker address from props should be used
    Properties props = new Properties();
    props.put(KAFKA_BOOTSTRAP_SERVERS, KAFKA_BROKER_ADDR);
    ApacheKafkaProducerConfig producerConfig = new ApacheKafkaProducerConfig(props);
    assertNotNull(producerConfig);
    assertEquals(producerConfig.getBrokerAddress(), KAFKA_BROKER_ADDR);
    assertFalse(producerConfig.getProducerProperties().containsKey(ProducerConfig.CLIENT_ID_CONFIG));

    ApacheKafkaProducerConfig producerConfig1 =
        new ApacheKafkaProducerConfig(new VeniceProperties(props), "overridden.addr", PRODUCER_NAME, false);
    // broker address from props should be used
    assertEquals(producerConfig1.getBrokerAddress(), "overridden.addr");
    assertEquals(producerConfig1.getProducerProperties().getProperty(ProducerConfig.CLIENT_ID_CONFIG), PRODUCER_NAME);
  }

  @Test
  public void testGetBrokerAddressReturnsSslAddrIfKafkaSslIsEnabled() {
    // broker address from props should be used
    Properties props = new Properties();
    props.put(SSL_TO_KAFKA, true);
    props.put(KAFKA_BOOTSTRAP_SERVERS, KAFKA_BROKER_ADDR);
    props.put(SSL_KAFKA_BOOTSTRAP_SERVERS, "ssl.kafka.broker.com:8182");
    assertEquals(new ApacheKafkaProducerConfig(props).getBrokerAddress(), "ssl.kafka.broker.com:8182");
  }
}
