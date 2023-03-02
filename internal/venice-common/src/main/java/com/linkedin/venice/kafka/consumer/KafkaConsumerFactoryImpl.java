package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.admin.KafkaAdminClient;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class KafkaConsumerFactoryImpl extends KafkaClientFactory {
  private static final Logger LOGGER = LogManager.getLogger(KafkaConsumerFactoryImpl.class);
  private final VeniceProperties veniceProperties;

  public KafkaConsumerFactoryImpl(VeniceProperties veniceProperties) {
    this.veniceProperties = veniceProperties;
  }

  @Override
  public Properties setupSSL(Properties properties) {
    properties.putAll(veniceProperties.toProperties());
    try {
      SSLConfig sslConfig = new SSLConfig(veniceProperties);
      properties.putAll(sslConfig.getKafkaSSLConfig());
      properties.setProperty(
          CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
          veniceProperties.getString(ConfigKeys.KAFKA_SECURITY_PROTOCOL));
    } catch (UndefinedPropertyException e) {
      LOGGER.warn("SSL properties are missing, Kafka consumer will not be able to consume if SSL is required.");
    }
    return properties;
  }

  @Override
  protected String getKafkaAdminClass() {
    return KafkaAdminClient.class.getName();
  }

  @Override
  protected String getWriteOnlyAdminClass() {
    return getKafkaAdminClass();
  }

  @Override
  protected String getReadOnlyAdminClass() {
    return getKafkaAdminClass();
  }

  @Override
  public String getKafkaBootstrapServers() {
    return veniceProperties.getString(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
  }

  @Override
  protected KafkaClientFactory clone(String kafkaBootstrapServers, Optional<MetricsParameters> metricsParameters) {
    Properties clonedProperties = this.veniceProperties.toProperties();
    clonedProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    return new KafkaConsumerFactoryImpl(new VeniceProperties(clonedProperties));
  }
}
