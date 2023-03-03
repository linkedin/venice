package com.linkedin.venice.controller.kafka.consumer;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.SSL_KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.ConfigKeys.SSL_TO_KAFKA;

import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.controller.VeniceControllerConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;


/**
 * A factory used by the Venice controller to create Kafka clients, specifically Kafka consumer and Kafka admin client.
 */
public class ControllerKafkaClientFactory extends KafkaClientFactory {
  private final VeniceControllerConfig controllerConfig;

  public ControllerKafkaClientFactory(
      VeniceControllerConfig controllerConfig,
      Optional<MetricsParameters> metricsParameters) {
    super(Optional.empty(), metricsParameters);
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
      properties
          .setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, controllerConfig.getSslKafkaBootstrapServers());
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
  public String getWriteOnlyAdminClass() {
    return controllerConfig.getKafkaWriteOnlyClass();
  }

  @Override
  public String getReadOnlyAdminClass() {
    return controllerConfig.getKafkaReadOnlyClass();
  }

  @Override
  public String getKafkaBootstrapServers() {
    return controllerConfig.isSslToKafka()
        ? controllerConfig.getSslKafkaBootstrapServers()
        : controllerConfig.getKafkaBootstrapServers();
  }

  @Override
  protected KafkaClientFactory clone(String kafkaBootstrapServers, Optional<MetricsParameters> metricsParameters) {
    VeniceProperties originalPros = this.controllerConfig.getProps();
    Properties clonedProperties = originalPros.toProperties();
    if (originalPros.getBoolean(SSL_TO_KAFKA, false)) {
      clonedProperties.setProperty(SSL_KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers);
    } else {
      clonedProperties.setProperty(KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers);
    }
    return new ControllerKafkaClientFactory(
        new VeniceControllerConfig(new VeniceProperties(clonedProperties)),
        metricsParameters);
  }
}
