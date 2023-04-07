package com.linkedin.venice.controller;

import static org.testng.Assert.*;

import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.testng.annotations.Test;


public class VeniceControllerClusterConfigTest {
  private static final String SASL_JAAS_CONFIG =
      "org.apache.kafka.common.security.plain.PlainLoginModule required " + "username=\"foo\" password=\"bar\"\n";

  private static final String SASL_MECHANISM = "PLAIN";

  @Test
  public void testGetKafkaSaslConfig() {
    Properties properties = createBaseProperties();
    properties.put("sasl.jaas.config", SASL_JAAS_CONFIG);
    properties.put("sasl.mechanism", SASL_MECHANISM);
    properties.put("security.protocol", "SASL_PLAINTEXT");
    VeniceProperties veniceProperties = new VeniceProperties(properties);
    VeniceControllerClusterConfig clusterConfig = new VeniceControllerClusterConfig(veniceProperties);
    assertEquals(SASL_JAAS_CONFIG, clusterConfig.getKafkaSaslJaasConfig());
    assertEquals(SASL_MECHANISM, clusterConfig.getKafkaSaslMechanism());
    assertEquals("SASL_PLAINTEXT", clusterConfig.getKafkaSecurityProtocol());
  }

  private static Properties createBaseProperties() {
    Properties properties = new Properties();
    properties.put("cluster.name", "cluster");
    properties.put("controller.name", "name");
    properties.put("zookeeper.address", "localhost:2181");
    properties.put("default.replica.factor", "1");
    properties.put("default.partition.size", "1024");
    properties.put("default.partition.count", "1");
    properties.put("default.partition.max.count", "1");
    properties.put("cluster.to.d2", "venice-cluster0:venice-discovery");
    properties.put("child.cluster.allowlist", "");

    properties.put("kafka.bootstrap.servers", "localhost:9092");

    properties.put("ssl.keystore.location", "some/path");
    properties.put("ssl.keystore.password", "foo");
    properties.put("ssl.keystore.type", "JKS");
    properties.put("ssl.key.password", "foo");
    properties.put("ssl.truststore.location", "foo");
    properties.put("ssl.truststore.password", "foo");
    properties.put("ssl.truststore.type", "JKS");
    properties.put("ssl.keymanager.algorithm", "SunX509");
    properties.put("ssl.trustmanager.algorithm", "SunX509");
    properties.put("ssl.secure.random.implementation", "SHA1PRNG");
    return properties;
  }

  @Test
  public void testGetKafkaSaslConfigWithSSL() {
    Properties properties = createBaseProperties();
    properties.put("sasl.jaas.config", SASL_JAAS_CONFIG);
    properties.put("sasl.mechanism", SASL_MECHANISM);
    properties.put("security.protocol", "SASL_SSL");

    VeniceProperties veniceProperties = new VeniceProperties(properties);
    VeniceControllerClusterConfig clusterConfig = new VeniceControllerClusterConfig(veniceProperties);
    assertEquals(SASL_JAAS_CONFIG, clusterConfig.getKafkaSaslJaasConfig());
    assertEquals(SASL_MECHANISM, clusterConfig.getKafkaSaslMechanism());
    assertEquals("SASL_SSL", clusterConfig.getKafkaSecurityProtocol());
  }
}
