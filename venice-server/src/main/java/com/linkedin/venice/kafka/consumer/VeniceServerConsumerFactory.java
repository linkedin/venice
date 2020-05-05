package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.utils.KafkaSSLUtils;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;


public class VeniceServerConsumerFactory extends KafkaClientFactory {
  private final VeniceServerConfig serverConfig;

  public VeniceServerConsumerFactory(VeniceServerConfig serverConfig) {
    this.serverConfig = serverConfig;
  }

  public Properties setupSSL(Properties properties) {
    if (KafkaSSLUtils.isKafkaSSLProtocol(serverConfig.getKafkaSecurityProtocol())) {
      Optional<SSLConfig> sslConfig = serverConfig.getSslConfig();
      if (!sslConfig.isPresent()) {
        throw new VeniceException("SSLConfig should be present when Kafka SSL is enabled");
      }
      properties.putAll(sslConfig.get().getKafkaSSLConfig());
      properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, serverConfig.getKafkaSecurityProtocol());
    }
    properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, serverConfig.getKafkaBootstrapServers());
    return properties;
  }

  @Override
  protected String getKafkaAdminClass() {
    return serverConfig.getKafkaAdminClass();
  }

  @Override
  protected String getKafkaZkAddress() {
    return serverConfig.getKafkaZkAddress();
  }
}
