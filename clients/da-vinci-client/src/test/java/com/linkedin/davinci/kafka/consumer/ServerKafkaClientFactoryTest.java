package com.linkedin.davinci.kafka.consumer;

import static org.testng.Assert.*;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.testng.annotations.Test;


public class ServerKafkaClientFactoryTest {
  private static final String SASL_JAAS_CONFIG =
      "org.apache.kafka.common.security.plain.PlainLoginModule required " + "username=\"foo\" password=\"bar\"\n";

  private static final String SASL_MECHANISM = "PLAIN";

  @Test
  public void testSetupSecuritySaslPlainText() {
    Properties properties = new Properties();
    properties.put("cluster.name", "cluster");
    properties.put("zookeeper.address", "localhost:2181");
    properties.put("kafka.bootstrap.servers", "localhost:9092");
    properties.put("sasl.jaas.config", SASL_JAAS_CONFIG);
    properties.put("sasl.mechanism", SASL_MECHANISM);
    properties.put("security.protocol", "SASL_PLAINTEXT");
    VeniceProperties veniceProperties = new VeniceProperties(properties);
    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceProperties, Collections.emptyMap());
    assertEquals(SASL_JAAS_CONFIG, serverConfig.getKafkaSaslJaasConfig());
    assertEquals(SASL_MECHANISM, serverConfig.getKafkaSaslMechanism());
    assertEquals(SecurityProtocol.SASL_PLAINTEXT, serverConfig.getKafkaSecurityProtocol(null));
    ServerKafkaClientFactory factory = new ServerKafkaClientFactory(serverConfig, Optional.empty(), Optional.empty());
    Properties props = new Properties();
    factory.setupSecurity(props);

    assertEquals(SASL_JAAS_CONFIG, props.getProperty("sasl.jaas.config"));
    assertEquals(SASL_MECHANISM, props.getProperty("sasl.mechanism"));
    assertEquals("SASL_PLAINTEXT", props.getProperty("security.protocol"));
  }

  @Test
  public void testSetupSecurityNoSasl() {
    Properties properties = new Properties();
    properties.put("cluster.name", "cluster");
    properties.put("zookeeper.address", "localhost:2181");
    properties.put("kafka.bootstrap.servers", "localhost:9092");
    properties.put("sasl.jaas.config", SASL_JAAS_CONFIG);
    properties.put("sasl.mechanism", "");
    properties.put("security.protocol", "PLAINTEXT");
    VeniceProperties veniceProperties = new VeniceProperties(properties);
    VeniceServerConfig serverConfig = new VeniceServerConfig(veniceProperties, Collections.emptyMap());
    assertEquals(SASL_JAAS_CONFIG, serverConfig.getKafkaSaslJaasConfig());
    assertEquals("", serverConfig.getKafkaSaslMechanism());
    assertEquals(SecurityProtocol.PLAINTEXT, serverConfig.getKafkaSecurityProtocol(null));
    ServerKafkaClientFactory factory = new ServerKafkaClientFactory(serverConfig, Optional.empty(), Optional.empty());
    Properties props = new Properties();
    factory.setupSecurity(props);

    // when we don't set sasl.mechanism, we should not set sasl.jaas.config
    assertNull(props.getProperty("sasl.jaas.config"));
    assertNull(props.getProperty("sasl.mechanism"));
    assertEquals("PLAINTEXT", props.getProperty("security.protocol"));
  }
}
