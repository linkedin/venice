package com.linkedin.venice.pubsub.adapter.kafka.admin;

import static com.linkedin.venice.pubsub.adapter.kafka.admin.ApacheKafkaAdminConfig.KAFKA_ADMIN_CONFIG_PREFIXES;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_CONFIG_PREFIX;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.pubsub.PubSubAdminAdapterContext;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.adapter.kafka.ApacheKafkaUtils;
import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.testng.annotations.Test;


public class ApacheKafkaAdminConfigTest {
  private static final String SASL_JAAS_CONFIG =
      "org.apache.kafka.common.security.plain.PlainLoginModule required " + "username=\"foo\" password=\"bar\"\n";

  private static final String SASL_MECHANISM = "PLAIN";
  private static final PubSubTopicRepository TOPIC_REPOSITORY = new PubSubTopicRepository();
  private static final PubSubPositionTypeRegistry TYPE_REGISTRY =
      PubSubPositionTypeRegistry.RESERVED_POSITION_TYPE_REGISTRY;

  @Test
  public void testSetupSaslInKafkaAdminPlaintext() {
    testSetupSaslInKafkaAdmin(PubSubSecurityProtocol.SASL_PLAINTEXT);
  }

  @Test
  public void testSetupSaslInKafkaAdminSSL() {
    testSetupSaslInKafkaAdmin(PubSubSecurityProtocol.SASL_SSL);
  }

  private void testSetupSaslInKafkaAdmin(PubSubSecurityProtocol securityProtocol) {
    Properties properties = new Properties();
    properties.put("cluster.name", "cluster");
    properties.put("zookeeper.address", "localhost:2181");
    properties.put("kafka.bootstrap.servers", "localhost:9092");
    properties.put("kafka.sasl.jaas.config", SASL_JAAS_CONFIG);
    properties.put("kafka.sasl.mechanism", SASL_MECHANISM);
    properties.put("kafka.security.protocol", securityProtocol.name());
    if (securityProtocol.name().contains("SSL")) {
      properties.put("kafka.ssl.keystore.location", "/etc/kafka/secrets/kafka.keystore.jks");
      properties.put("kafka.ssl.keystore.password", "keystore-pass");
      properties.put("kafka.ssl.keystore.type", "JKS");
      properties.put("kafka.ssl.key.password", "key-pass");
      properties.put("kafka.ssl.truststore.location", "/etc/kafka/secrets/kafka.truststore.jks");
      properties.put("kafka.ssl.truststore.password", "truststore-pass");
      properties.put("kafka.ssl.truststore.type", "JKS");
      properties.put("kafka.ssl.keymanager.algorithm", "SunX509");
      properties.put("kafka.ssl.trustmanager.algorithm", "SunX509");
      properties.put("kafka.ssl.secure.random.implementation", "SHA1PRNG");
    }
    VeniceProperties veniceProperties = new VeniceProperties(properties);
    ApacheKafkaAdminConfig serverConfig = new ApacheKafkaAdminConfig(
        new PubSubAdminAdapterContext.Builder().setVeniceProperties(veniceProperties)
            .setPubSubSecurityProtocol(securityProtocol)
            .setPubSubTopicRepository(TOPIC_REPOSITORY)
            .setPubSubPositionTypeRegistry(TYPE_REGISTRY)
            .setAdminClientName("testAdminClient")
            .build());
    Properties adminProperties = serverConfig.getAdminProperties();
    assertEquals(SASL_JAAS_CONFIG, adminProperties.get("sasl.jaas.config"));
    assertEquals(SASL_MECHANISM, adminProperties.get("sasl.mechanism"));
    assertEquals(securityProtocol.name(), adminProperties.get("security.protocol"));
  }

  @Test
  public void testGetValidAdminProperties() {
    Properties allProps = new Properties();
    allProps.put(KAFKA_CONFIG_PREFIX + ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");
    allProps.put(KAFKA_CONFIG_PREFIX + ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "2000");
    allProps.put(KAFKA_CONFIG_PREFIX + AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    allProps.put(KAFKA_CONFIG_PREFIX + "bogus.kafka.config", "bogusValue");

    Properties validProps = ApacheKafkaUtils.getValidKafkaClientProperties(
        new VeniceProperties(allProps),
        PubSubSecurityProtocol.PLAINTEXT,
        AdminClientConfig.configNames(),
        KAFKA_ADMIN_CONFIG_PREFIXES);
    assertEquals(validProps.size(), 2);
    assertEquals(validProps.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG), "localhost:9092");
  }
}
