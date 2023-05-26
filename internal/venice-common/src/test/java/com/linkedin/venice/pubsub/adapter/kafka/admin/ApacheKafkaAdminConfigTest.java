package com.linkedin.venice.pubsub.adapter.kafka.admin;

import static org.testng.Assert.*;

import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.testng.annotations.Test;


public class ApacheKafkaAdminConfigTest {
  private static final String SASL_JAAS_CONFIG =
      "org.apache.kafka.common.security.plain.PlainLoginModule required " + "username=\"foo\" password=\"bar\"\n";

  private static final String SASL_MECHANISM = "PLAIN";

  @Test
  public void testSetupSaslInKafkaAdminPlaintext() {
    testSetupSaslInKafkaAdmin(SecurityProtocol.SASL_PLAINTEXT);
  }

  @Test
  public void testSetupSaslInKafkaAdminSSL() {
    testSetupSaslInKafkaAdmin(SecurityProtocol.SASL_SSL);
  }

  private void testSetupSaslInKafkaAdmin(SecurityProtocol securityProtocol) {
    Properties properties = new Properties();
    properties.put("cluster.name", "cluster");
    properties.put("zookeeper.address", "localhost:2181");
    properties.put("kafka.bootstrap.servers", "localhost:9092");
    properties.put("kafka.sasl.jaas.config", SASL_JAAS_CONFIG);
    properties.put("kafka.sasl.mechanism", SASL_MECHANISM);
    properties.put("kafka.security.protocol", securityProtocol.name);
    if (securityProtocol.name.contains("SSL")) {
      properties.put("ssl.truststore.location", "-");
      properties.put("ssl.truststore.password", "");
      properties.put("ssl.truststore.type", "JKS");
      properties.put("ssl.keymanager.algorithm", SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM);
      properties.put("ssl.trustmanager.algorithm", SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM);
      properties.put("ssl.secure.random.implementation", "SHA1PRNG");
    }
    VeniceProperties veniceProperties = new VeniceProperties(properties);
    ApacheKafkaAdminConfig serverConfig = new ApacheKafkaAdminConfig(veniceProperties);
    Properties adminProperties = serverConfig.getAdminProperties();
    assertEquals(SASL_JAAS_CONFIG, adminProperties.get("sasl.jaas.config"));
    assertEquals(SASL_MECHANISM, adminProperties.get("sasl.mechanism"));
    assertEquals(securityProtocol.name, adminProperties.get("security.protocol"));
  }
}
