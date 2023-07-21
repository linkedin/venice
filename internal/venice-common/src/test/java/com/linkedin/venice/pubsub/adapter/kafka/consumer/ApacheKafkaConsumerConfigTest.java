package com.linkedin.venice.pubsub.adapter.kafka.consumer;

import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.SSL_KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig.SSL_TO_KAFKA_LEGACY;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.CommonConfigKeys;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.pubsub.adapter.VeniceClusterConfig;
import com.linkedin.venice.pubsub.adapter.kafka.producer.ApacheKafkaProducerConfig;
import com.linkedin.venice.pubsub.api.PubSubClientsFactory;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ApacheKafkaConsumerConfigTest {
  private static final String KAFKA_BROKER_ADDR = "kafka.broker.com:8181";
  private static final String SASL_JAAS_CONFIG =
      "org.apache.kafka.common.security.plain.PlainLoginModule required " + "username=\"foo\" password=\"bar\"\n";

  private static final String SASL_MECHANISM = "PLAIN";

  @Test
  public void testConsumerConfig() {

    /**
     * The map contains the following info:
     *
     * <map>
     *   <entry key="0">
     *     <map>
     *       <entry key="name" value="dc0"/>
     *       <entry key="url" value="kafka.in.dc0.my.company.com:1234"/>
     *       <entry key="otherUrls" value="alt1.kafka.in.dc0.my.company.com:1234,alt2.kafka.in.dc0.my.company.com:1234"/>
     *       <entry key="securityProtocol" value="PLAINTEXT"/>
     *     </map>
     *   </entry>
     *   <entry key="1">
     *     <map>
     *       <entry key="name" value="dc1"/>
     *       <entry key="url" value="kafka.in.dc1.my.company.com:1234"/>
     *       <entry key="otherUrls" value="alt1.kafka.in.dc1.my.company.com:1234,alt2.kafka.in.dc1.my.company.com:1234"/>
     *       <entry key="securityProtocol" value="SSL"/>
     *     </map>
     *   </entry>
     * </map>
     */
    String dc0Name = "dc0";
    String dc1Name = "dc1";
    String dc0Alt1KafkaUrl = "alt1.kafka.in.dc0.my.company.com:1234";
    String dc0Alt2KafkaUrl = "alt2.kafka.in.dc0.my.company.com:1234";
    String dc1Alt1KafkaUrl = "alt1.kafka.in.dc1.my.company.com:1234";
    String dc1Alt2KafkaUrl = "alt2.kafka.in.dc1.my.company.com:1234";
    String dc0KafkaClusterUrl = "kafka.in.dc1.my.company.com:1234";
    String dc1KafkaClusterUrl = "kafka.in.dc1.my.company.com:1234";
    String dc1KafkaClusterSSLUrl = "kafka.in.dc1.my.company.com:1234";
    String dc1InvalidKafkaClusterUrl = "invalid.kafka.in.dc1.my.company.com:1234";
    Map<String, Map<String, String>> kafkaRegionClusterMap = new HashMap<>();
    Map<String, String> dc0ClusterMap = new HashMap<>();
    dc0ClusterMap.put(ConfigKeys.KAFKA_CLUSTER_MAP_KEY_NAME, dc0Name);
    dc0ClusterMap.put(ConfigKeys.KAFKA_CLUSTER_MAP_KEY_URL, dc0KafkaClusterUrl);
    dc0ClusterMap.put(ConfigKeys.KAFKA_CLUSTER_MAP_KEY_OTHER_URLS, dc0Alt1KafkaUrl + "," + dc0Alt2KafkaUrl);
    dc0ClusterMap.put(ConfigKeys.KAFKA_CLUSTER_MAP_SECURITY_PROTOCOL, "PLAINTEXT");
    kafkaRegionClusterMap.put("0", dc0ClusterMap);

    Map<String, String> dc1ClusterMap = new HashMap<>();
    dc1ClusterMap.put(ConfigKeys.KAFKA_CLUSTER_MAP_KEY_NAME, dc1Name);
    dc1ClusterMap.put(ConfigKeys.KAFKA_CLUSTER_MAP_KEY_URL, dc1KafkaClusterUrl);
    dc1ClusterMap.put(ConfigKeys.KAFKA_CLUSTER_MAP_KEY_OTHER_URLS, dc1Alt1KafkaUrl + "," + dc1Alt2KafkaUrl);
    dc1ClusterMap.put(ConfigKeys.KAFKA_CLUSTER_MAP_SECURITY_PROTOCOL, "SSL");
    kafkaRegionClusterMap.put("1", dc1ClusterMap);

    String kafkaClusterMapStr = VeniceClusterConfig.flattenKafkaClusterMapToStr(kafkaRegionClusterMap);
    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.PUB_SUB_KAFKA_CLUSTER_MAP_STRING, kafkaClusterMapStr);
    properties
        .setProperty(ConfigKeys.PUB_SUB_COMPONENTS_USAGE, PubSubClientsFactory.PUB_SUB_CLIENT_USAGE_FOR_CONTROLLER);
    properties.setProperty(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS, dc1InvalidKafkaClusterUrl);
    properties.setProperty(ConfigKeys.SSL_KAFKA_BOOTSTRAP_SERVERS, dc1KafkaClusterSSLUrl);
    properties.setProperty(ConfigKeys.PUB_SUB_BOOTSTRAP_SERVERS_TO_RESOLVE, dc1KafkaClusterUrl);

    ApacheKafkaConsumerConfig apacheKafkaConsumerConfig =
        new ApacheKafkaConsumerConfig(new VeniceProperties(properties), "consumerName");
    Properties consumerProperties = apacheKafkaConsumerConfig.getConsumerProperties();
    Assert.assertEquals(consumerProperties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), dc1KafkaClusterUrl);

    properties.setProperty(ConfigKeys.KAFKA_SECURITY_PROTOCOL, SecurityProtocol.SSL.name());
    properties.setProperty(CommonConfigKeys.SSL_KEYSTORE_LOCATION, "SSL_KEYSTORE_LOCATION");
    properties.setProperty(CommonConfigKeys.SSL_KEYSTORE_PASSWORD, "");
    properties.setProperty(CommonConfigKeys.SSL_KEYSTORE_TYPE, "");
    properties.setProperty(CommonConfigKeys.SSL_KEY_PASSWORD, "");
    properties.setProperty(CommonConfigKeys.SSL_TRUSTSTORE_LOCATION, "");
    properties.setProperty(CommonConfigKeys.SSL_TRUSTSTORE_PASSWORD, "");
    properties.setProperty(CommonConfigKeys.SSL_TRUSTSTORE_TYPE, "");
    properties.setProperty(CommonConfigKeys.SSL_KEYMANAGER_ALGORITHM, "");
    properties.setProperty(CommonConfigKeys.SSL_TRUSTMANAGER_ALGORITHM, "");
    properties.setProperty(CommonConfigKeys.SSL_SECURE_RANDOM_IMPLEMENTATION, "");
    properties.setProperty(CommonConfigKeys.SSL_NEEDS_CLIENT_CERT, "false");
    properties.setProperty(ConfigKeys.CONTROLLER_SSL_ENABLED, "true");
    apacheKafkaConsumerConfig = new ApacheKafkaConsumerConfig(new VeniceProperties(properties), "consumerName2");
    consumerProperties = apacheKafkaConsumerConfig.getConsumerProperties();
    Assert.assertEquals(consumerProperties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), dc1KafkaClusterSSLUrl);

    properties.setProperty(ConfigKeys.PUB_SUB_COMPONENTS_USAGE, PubSubClientsFactory.PUB_SUB_CLIENT_USAGE_FOR_SERVER);
    properties.setProperty(ConfigKeys.CLUSTER_NAME, dc0Name);
    properties.setProperty(ConfigKeys.ZOOKEEPER_ADDRESS, "zookeeper_address");
    properties.setProperty(ConfigKeys.PUB_SUB_BOOTSTRAP_SERVERS_TO_RESOLVE, dc1Alt2KafkaUrl);
    apacheKafkaConsumerConfig = new ApacheKafkaConsumerConfig(new VeniceProperties(properties), "consumerName3");
    consumerProperties = apacheKafkaConsumerConfig.getConsumerProperties();
    Assert.assertEquals(consumerProperties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), dc1KafkaClusterUrl);
  }

  @Test
  public void testSaslConfiguration() {
    Properties props = new Properties();
    props.put(SSL_TO_KAFKA_LEGACY, true);
    props.put(KAFKA_BOOTSTRAP_SERVERS, KAFKA_BROKER_ADDR);
    props.put(SSL_KAFKA_BOOTSTRAP_SERVERS, "ssl.kafka.broker.com:8182");
    props.put("kafka.sasl.jaas.config", SASL_JAAS_CONFIG);
    props.put("kafka.sasl.mechanism", SASL_MECHANISM);
    props.put("kafka.security.protocol", "SASL_SSL");
    ApacheKafkaProducerConfig apacheKafkaProducerConfig = new ApacheKafkaProducerConfig(props);
    Properties producerProperties = apacheKafkaProducerConfig.getProducerProperties();
    assertEquals(SASL_JAAS_CONFIG, producerProperties.get("sasl.jaas.config"));
    assertEquals(SASL_MECHANISM, producerProperties.get("sasl.mechanism"));
    assertEquals("SASL_SSL", producerProperties.get("security.protocol"));
  }
}
