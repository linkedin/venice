package com.linkedin.venice.controller.kafka.consumer;

import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.controller.VeniceControllerConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;

import static com.linkedin.venice.ConfigKeys.*;


public class VeniceControllerConsumerFactory extends KafkaClientFactory {
  private final VeniceControllerConfig controllerConfig;
  public VeniceControllerConsumerFactory(VeniceControllerConfig controllerConfig, Optional<MetricsParameters> metricsParameters) {
    super(Optional.empty(), metricsParameters, controllerConfig.isAutoCloseIdleConsumersEnabled());
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

  @Override
  protected String getKafkaAdminClass() {
    return controllerConfig.getKafkaAdminClass();
  }

  @Override
  protected String getKafkaZkAddress() {
    return controllerConfig.getKafkaZkAddress();
  }

  @Override
  public String getKafkaBootstrapServers() {
    return controllerConfig.isSslToKafka() ? controllerConfig.getSslKafkaBootStrapServers() : controllerConfig.getKafkaBootstrapServers();
  }

  @Override
  protected KafkaClientFactory clone(String kafkaBootstrapServers, String kafkaZkAddress, Optional<MetricsParameters> metricsParameters) {
    VeniceProperties originalPros = this.controllerConfig.getProps();
    Properties clonedProperties = originalPros.toProperties();
    if (originalPros.getBoolean(SSL_TO_KAFKA, false)) {
      clonedProperties.setProperty(SSL_KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers);
    } else {
      clonedProperties.setProperty(KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers);
    }
    clonedProperties.setProperty(KAFKA_ZK_ADDRESS, kafkaZkAddress);
    return new VeniceControllerConsumerFactory(
        new VeniceControllerConfig(new VeniceProperties(clonedProperties)),
        metricsParameters
    );
  }
}
