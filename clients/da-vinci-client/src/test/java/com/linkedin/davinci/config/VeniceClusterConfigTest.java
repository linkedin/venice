package com.linkedin.davinci.config;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLUSTER_MAP_KEY_URL;
import static com.linkedin.venice.ConfigKeys.KAFKA_CLUSTER_MAP_SECURITY_PROTOCOL;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;

import com.linkedin.venice.pubsub.api.PubSubSecurityProtocol;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceClusterConfigTest {
  VeniceClusterConfig config;
  static Map<String, Map<String, String>> KAFKA_CLUSTER_MAP;
  static {
    KAFKA_CLUSTER_MAP = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      Map<String, String> entry = new HashMap<>();
      entry.put("name", "region_" + i);
      entry.put("url", "localhost:" + i);

      if (i % 2 == 0) {
        entry.put("securityProtocol", PubSubSecurityProtocol.PLAINTEXT.name());
      } else {
        entry.put("securityProtocol", PubSubSecurityProtocol.SSL.name());
      }
      KAFKA_CLUSTER_MAP.put(String.valueOf(i), entry);
    }

    // additional mapping for sep topics
    for (int i = 3; i < 6; i++) {
      int clusterId = i % 3;
      Map<String, String> entry = new HashMap<>();
      entry.put("name", "region_" + clusterId + "_sep");
      entry.put("url", "localhost:" + clusterId + "_sep");
      if (i % 2 == 0) {
        entry.put("securityProtocol", PubSubSecurityProtocol.PLAINTEXT.name());
      } else {
        entry.put("securityProtocol", PubSubSecurityProtocol.SSL.name());
      }
      KAFKA_CLUSTER_MAP.put(String.valueOf(i), entry);
    }
  }

  @BeforeMethod
  public void setUp() {
    Properties props = new Properties();
    props.setProperty(CLUSTER_NAME, "test_cluster");
    props.setProperty(ZOOKEEPER_ADDRESS, "fake_zk_addr");
    props.setProperty(KAFKA_BOOTSTRAP_SERVERS, "fake_kafka_addr");
    props.put("ssl.keystore.location", "/etc/kafka/secrets/kafka.keystore.jks");
    props.put("ssl.keystore.password", "keystore-pass");
    props.put("ssl.keystore.type", "JKS");
    props.put("ssl.key.password", "key-pass");
    props.put("ssl.truststore.location", "/etc/kafka/secrets/kafka.truststore.jks");
    props.put("ssl.truststore.password", "truststore-pass");
    props.put("ssl.truststore.type", "JKS");
    props.put("ssl.keymanager.algorithm", "SunX509");
    props.put("ssl.trustmanager.algorithm", "SunX509");
    props.put("ssl.secure.random.implementation", "SHA1PRNG");
    config = new VeniceClusterConfig(new VeniceProperties(props), KAFKA_CLUSTER_MAP);
  }

  @Test
  public void testGetEquivalentKafkaClusterIdForSepTopic() {
    Assert.assertEquals(config.getEquivalentKafkaClusterIdForSepTopic(0), 0);
    Assert.assertEquals(config.getEquivalentKafkaClusterIdForSepTopic(1), 1);
    Assert.assertEquals(config.getEquivalentKafkaClusterIdForSepTopic(2), 2);
    Assert.assertEquals(config.getEquivalentKafkaClusterIdForSepTopic(3), 0);
    Assert.assertEquals(config.getEquivalentKafkaClusterIdForSepTopic(4), 1);
    Assert.assertEquals(config.getEquivalentKafkaClusterIdForSepTopic(5), 2);
  }

  @Test
  public void testPubSubSecurityProtocol() {
    for (Map.Entry<String, Map<String, String>> entry: KAFKA_CLUSTER_MAP.entrySet()) {
      Map<String, String> clusterInfo = entry.getValue();
      String expectedSecurityProtocol = clusterInfo.get(KAFKA_CLUSTER_MAP_SECURITY_PROTOCOL);
      String url = clusterInfo.get(KAFKA_CLUSTER_MAP_KEY_URL);
      Assert.assertEquals(
          config.getPubSubSecurityProtocol(url),
          PubSubSecurityProtocol.valueOf(expectedSecurityProtocol));
    }
  }
}
