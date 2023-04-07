package com.linkedin.venice.controller.kafka.consumer;

import static org.testng.Assert.*;

import com.linkedin.venice.controller.VeniceControllerConfig;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import java.util.Properties;
import org.testng.annotations.Test;


public class ControllerKafkaClientFactoryTest {
  private static final String SASL_JAAS_CONFIG =
      "org.apache.kafka.common.security.plain.PlainLoginModule required " + "username=\"foo\" password=\"bar\"\n";

  private static final String SASL_MECHANISM = "PLAIN";

  private static Properties createBaseProperties() {
    Properties properties = new Properties();
    properties.put("cluster.name", "cluster");
    properties.put("admin.port", "7777");
    properties.put("admin.secure.port", "7778");
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
  public void testSetupSecuritySaslPlainText() {
    Properties properties = createBaseProperties();
    properties.put("sasl.jaas.config", SASL_JAAS_CONFIG);
    properties.put("sasl.mechanism", SASL_MECHANISM);
    properties.put("security.protocol", "SASL_PLAINTEXT");
    VeniceProperties veniceProperties = new VeniceProperties(properties);
    VeniceControllerConfig serverConfig = new VeniceControllerConfig(veniceProperties);
    assertEquals(SASL_JAAS_CONFIG, serverConfig.getKafkaSaslJaasConfig());
    assertEquals(SASL_MECHANISM, serverConfig.getKafkaSaslMechanism());
    assertEquals("SASL_PLAINTEXT", serverConfig.getKafkaSecurityProtocol());
    ControllerKafkaClientFactory factory = new ControllerKafkaClientFactory(serverConfig, Optional.empty());
    Properties props = new Properties();
    factory.setupSecurity(props);

    assertEquals(SASL_JAAS_CONFIG, props.getProperty("sasl.jaas.config"));
    assertEquals(SASL_MECHANISM, props.getProperty("sasl.mechanism"));
    assertEquals("SASL_PLAINTEXT", props.getProperty("security.protocol"));
  }

  @Test
  public void testSetupSecurityNoSasl() {
    Properties properties = createBaseProperties();
    properties.put("sasl.jaas.config", SASL_JAAS_CONFIG);
    properties.put("sasl.mechanism", "");
    properties.put("security.protocol", "PLAINTEXT");
    VeniceProperties veniceProperties = new VeniceProperties(properties);
    VeniceControllerConfig serverConfig = new VeniceControllerConfig(veniceProperties);
    assertEquals(SASL_JAAS_CONFIG, serverConfig.getKafkaSaslJaasConfig());
    assertEquals("", serverConfig.getKafkaSaslMechanism());
    assertEquals("PLAINTEXT", serverConfig.getKafkaSecurityProtocol());
    ControllerKafkaClientFactory factory = new ControllerKafkaClientFactory(serverConfig, Optional.empty());
    Properties props = new Properties();
    factory.setupSecurity(props);

    // when we don't set sasl.mechanism, we should not set sasl.jaas.config
    assertNull(props.getProperty("sasl.jaas.config"));
    assertNull(props.getProperty("sasl.mechanism"));
    assertNull(props.getProperty("security.protocol"));
  }
}
