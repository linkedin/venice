package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.admin.KafkaAdminClient;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import java.util.Properties;


/**
 * A factory used by the Venice server (storage node) to create Kafka clients, specifically Kafka consumer and Kafka
 * admin client. Note that the Kafka admin client it creates is the Java admin client instead of the Scala admin client.
 */
public class ServerJavaKafkaClientFactory extends ServerKafkaClientFactory {
  public ServerJavaKafkaClientFactory(
      VeniceServerConfig serverConfig,
      Optional<SchemaReader> kafkaMessageEnvelopeSchemaReader,
      Optional<MetricsParameters> metricsParameters) {
    super(serverConfig, kafkaMessageEnvelopeSchemaReader, metricsParameters);
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
  protected KafkaClientFactory clone(String kafkaBootstrapServers, Optional<MetricsParameters> metricsParameters) {
    Properties clonedProperties = this.serverConfig.getClusterProperties().toProperties();
    clonedProperties.setProperty(KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers);
    return new ServerJavaKafkaClientFactory(
        new VeniceServerConfig(new VeniceProperties(clonedProperties), serverConfig.getKafkaClusterMap()),
        kafkaMessageEnvelopeSchemaReader,
        metricsParameters);
  }
}
