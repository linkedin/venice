package com.linkedin.davinci.config;

import static com.linkedin.venice.ConfigKeys.CLUSTER_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.ZOOKEEPER_ADDRESS;

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
      KAFKA_CLUSTER_MAP.put(String.valueOf(i), entry);
    }

    // additional mapping for sep topics
    for (int i = 3; i < 6; i++) {
      int clusterId = i % 3;
      Map<String, String> entry = new HashMap<>();
      entry.put("name", "region_" + clusterId + "_sep");
      entry.put("url", "localhost:" + clusterId + "_sep");
      KAFKA_CLUSTER_MAP.put(String.valueOf(i), entry);
    }
  }

  @BeforeMethod
  public void setUp() {
    Properties props = new Properties();
    props.setProperty(CLUSTER_NAME, "test_cluster");
    props.setProperty(ZOOKEEPER_ADDRESS, "fake_zk_addr");
    props.setProperty(KAFKA_BOOTSTRAP_SERVERS, "fake_kafka_addr");
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
}
