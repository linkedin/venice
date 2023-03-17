package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.ConfigConstants.DEFAULT_KAFKA_SSL_CONTEXT_PROVIDER_CLASS_NAME;
import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static org.apache.kafka.common.config.SslConfigs.SSL_CONTEXT_PROVIDER_CLASS_CONFIG;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.utils.KafkaSSLUtils;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A factory used by the Venice server (storage node) to create Kafka clients, specifically Kafka consumer and Kafka
 * admin client.
 */
public class ServerKafkaClientFactory extends KafkaClientFactory {
  private static final Logger LOGGER = LogManager.getLogger(ServerKafkaClientFactory.class);

  protected final VeniceServerConfig serverConfig;

  public ServerKafkaClientFactory(
      VeniceServerConfig serverConfig,
      Optional<SchemaReader> kafkaMessageEnvelopeSchemaReader,
      Optional<MetricsParameters> metricsParameters) {
    super(kafkaMessageEnvelopeSchemaReader, metricsParameters);
    this.serverConfig = serverConfig;
  }

  public Properties setupSecurity(Properties properties) {
    String kafkaBootstrapUrls = properties.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
    if (kafkaBootstrapUrls == null) {
      /** Override the bootstrap servers config if it's not set in the proposed properties. */
      kafkaBootstrapUrls = serverConfig.getKafkaBootstrapServers();
    }
    String resolvedKafkaUrl = serverConfig.getKafkaClusterUrlResolver().apply(kafkaBootstrapUrls);
    if (resolvedKafkaUrl != null) {
      kafkaBootstrapUrls = resolvedKafkaUrl;
    }
    properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapUrls);
    SecurityProtocol securityProtocol = serverConfig.getKafkaSecurityProtocol(kafkaBootstrapUrls);
    if (KafkaSSLUtils.isKafkaSSLProtocol(securityProtocol)) {
      Optional<SSLConfig> sslConfig = serverConfig.getSslConfig();
      if (!sslConfig.isPresent()) {
        throw new VeniceException("SSLConfig should be present when Kafka SSL is enabled");
      }
      properties.putAll(sslConfig.get().getKafkaSSLConfig());
      /**
       * Check whether openssl is enabled for the kafka consumers in ingestion service.
       */
      if (serverConfig.isKafkaOpenSSLEnabled()) {
        properties.setProperty(SSL_CONTEXT_PROVIDER_CLASS_CONFIG, DEFAULT_KAFKA_SSL_CONTEXT_PROVIDER_CLASS_NAME);
      }
    }
    properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name);

    if (!StringUtils.isBlank(serverConfig.getKafkaSaslMechanism())) {
      properties.setProperty(
          CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
          serverConfig.getKafkaSecurityProtocol(kafkaBootstrapUrls).name);
      properties.setProperty("sasl.mechanism", serverConfig.getKafkaSaslMechanism());
      properties.setProperty("sasl.jaas.config", serverConfig.getKafkaSaslJaasConfig());
    }
    LOGGER.info("Kafka client properties: {}", properties);
    return properties;
  }

  @Override
  protected String getKafkaAdminClass() {
    return serverConfig.getKafkaAdminClass();
  }

  @Override
  protected String getWriteOnlyAdminClass() {
    return serverConfig.getKafkaWriteOnlyClass();
  }

  @Override
  protected String getReadOnlyAdminClass() {
    return serverConfig.getKafkaReadOnlyClass();
  }

  @Override
  public String getKafkaBootstrapServers() {
    return serverConfig.getKafkaBootstrapServers();
  }

  @Override
  protected boolean isKafkaConsumerOffsetCollectionEnabled() {
    return serverConfig.isKafkaConsumerOffsetCollectionEnabled();
  }

  @Override
  protected KafkaClientFactory clone(String kafkaBootstrapServers, Optional<MetricsParameters> metricsParameters) {
    Properties clonedProperties = this.serverConfig.getClusterProperties().toProperties();
    clonedProperties.setProperty(KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers);
    return new ServerKafkaClientFactory(
        new VeniceServerConfig(new VeniceProperties(clonedProperties), serverConfig.getKafkaClusterMap()),
        kafkaMessageEnvelopeSchemaReader,
        metricsParameters);
  }
}
