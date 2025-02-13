package com.linkedin.venice.pubsub.adapter.kafka.consumer;

import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.SSL_KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.SSL_TO_KAFKA_LEGACY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.testng.annotations.Test;


public class ApacheKafkaConsumerConfigTest {
  private static final String KAFKA_BROKER_ADDR = "kafka.broker.com:8181";
  private static final String SASL_JAAS_CONFIG =
      "org.apache.kafka.common.security.plain.PlainLoginModule required " + "username=\"foo\" password=\"bar\"\n";

  private static final String SASL_MECHANISM = "PLAIN";

  @Test
  public void testSaslConfiguration() {
    Properties props = new Properties();
    props.put(SSL_TO_KAFKA_LEGACY, true);
    props.put(KAFKA_BOOTSTRAP_SERVERS, KAFKA_BROKER_ADDR);
    props.put(SSL_KAFKA_BOOTSTRAP_SERVERS, "ssl.kafka.broker.com:8182");
    props.put("kafka.sasl.jaas.config", SASL_JAAS_CONFIG);
    props.put("kafka.sasl.mechanism", SASL_MECHANISM);
    props.put("kafka.security.protocol", "SASL_SSL");
    ApacheKafkaConsumerConfig consumerConfig = new ApacheKafkaConsumerConfig(new VeniceProperties(props), null);
    Properties consumerProps = consumerConfig.getConsumerProperties();
    assertEquals(SASL_JAAS_CONFIG, consumerProps.get("sasl.jaas.config"));
    assertEquals(SASL_MECHANISM, consumerProps.get("sasl.mechanism"));
    assertEquals("SASL_SSL", consumerProps.get("security.protocol"));
  }

  @Test
  public void testGetValidConsumerProperties() {
    Properties allProps = new Properties();
    allProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
    allProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "2000");
    // this is common config; there are no admin specific configs
    allProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    allProps.put("bogus.kafka.config", "bogusValue");

    Properties validProps = ApacheKafkaConsumerConfig.getValidConsumerProperties(allProps);
    assertEquals(validProps.size(), 2);
    assertEquals(validProps.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG), "localhost:9092");
    assertEquals(validProps.get(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG), "2000");
  }

  @Test
  public void testDefaultsValuesAreUsedIfConfigIsNotProvided() {
    Properties props = new Properties();
    ApacheKafkaConsumerConfig apacheKafkaConsumerConfig =
        new ApacheKafkaConsumerConfig(new VeniceProperties(props), "test");
    Properties consumerProps = apacheKafkaConsumerConfig.getConsumerProperties();
    assertNotNull(consumerProps);
    assertTrue(consumerProps.containsKey(ConsumerConfig.RECEIVE_BUFFER_CONFIG));
    assertEquals(
        consumerProps.get(ConsumerConfig.RECEIVE_BUFFER_CONFIG),
        ApacheKafkaConsumerConfig.DEFAULT_RECEIVE_BUFFER_SIZE);

    props.put(ApacheKafkaProducerConfig.KAFKA_CONFIG_PREFIX + ConsumerConfig.RECEIVE_BUFFER_CONFIG, "98765");
    apacheKafkaConsumerConfig = new ApacheKafkaConsumerConfig(new VeniceProperties(props), "test");
    consumerProps = apacheKafkaConsumerConfig.getConsumerProperties();
    assertNotNull(consumerProps);
    assertTrue(consumerProps.containsKey(ConsumerConfig.RECEIVE_BUFFER_CONFIG));
    assertEquals(consumerProps.get(ConsumerConfig.RECEIVE_BUFFER_CONFIG), "98765");
  }
}
