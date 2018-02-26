package com.linkedin.venice.controller.kafka.consumer;

import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.controller.VeniceControllerConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.VeniceConsumerFactory;
import com.linkedin.venice.utils.KafkaSSLUtils;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;


public class VeniceControllerConsumerFactory extends VeniceConsumerFactory {
  private final VeniceControllerConfig controllerConfig;
  public VeniceControllerConsumerFactory(VeniceControllerConfig controllerConfig){
    this.controllerConfig = controllerConfig;
  }

  public Properties setupSSL(Properties properties) {
    if (KafkaSSLUtils.isKafkaSSLProtocol(controllerConfig.getKafkaSecurityProtocol())) {
      Optional<SSLConfig> sslConfig = controllerConfig.getSslConfig();
      if (!sslConfig.isPresent()) {
        throw new VeniceException("SSLConfig should be present when Kafka SSL is enabled");
      }
      properties.putAll(sslConfig.get().getKafkaSSLConfig());
      properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, controllerConfig.getKafkaSecurityProtocol());
      properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, controllerConfig.getSslKafkaBootStrapServers());
    } else {
      properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, controllerConfig.getKafkaBootstrapServers());
    }
    return properties;
  }

}
