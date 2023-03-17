package com.linkedin.davinci.config;

import static org.testng.Assert.*;

import com.linkedin.venice.utils.VeniceProperties;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.testng.annotations.Test;


public class VeniceClusterConfigTest {
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
    VeniceClusterConfig clusterConfig = new VeniceClusterConfig(veniceProperties, Collections.emptyMap());
    assertEquals(SASL_JAAS_CONFIG, clusterConfig.getKafkaSaslJaasConfig());
    assertEquals(SASL_MECHANISM, clusterConfig.getKafkaSaslMechanism());
    assertEquals(SecurityProtocol.SASL_PLAINTEXT, clusterConfig.getKafkaSecurityProtocol(null));
  }

  private static Properties createBaseProperties() {
    Properties properties = new Properties();
    properties.put("cluster.name", "cluster");
    properties.put("zookeeper.address", "localhost:2181");
    properties.put("kafka.bootstrap.servers", "localhost:9092");
    return properties;
  }

  @Test
  public void testGetKafkaSaslConfigWithSSL() {
    Properties properties = createBaseProperties();
    properties.put("sasl.jaas.config", SASL_JAAS_CONFIG);
    properties.put("sasl.mechanism", SASL_MECHANISM);
    properties.put("security.protocol", "SASL_SSL");

    // as we are setting SASL_SSL these properties are required
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

    VeniceProperties veniceProperties = new VeniceProperties(properties);
    VeniceClusterConfig clusterConfig = new VeniceClusterConfig(veniceProperties, Collections.emptyMap());
    assertEquals(SASL_JAAS_CONFIG, clusterConfig.getKafkaSaslJaasConfig());
    assertEquals(SASL_MECHANISM, clusterConfig.getKafkaSaslMechanism());
    assertEquals(SecurityProtocol.SASL_SSL, clusterConfig.getKafkaSecurityProtocol(null));
  }

}
