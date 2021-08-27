package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.kafka.KafkaClientFactory;
import com.linkedin.venice.kafka.admin.KafkaAdminClient;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Optional;
import java.util.Properties;

import static com.linkedin.venice.ConfigKeys.*;


public class VeniceServerConsumerJavaBasedFactory extends VeniceServerConsumerFactory {
  public VeniceServerConsumerJavaBasedFactory(VeniceServerConfig serverConfig, Optional<SchemaReader> kafkaMessageEnvelopeSchemaReader) {
    super(serverConfig, kafkaMessageEnvelopeSchemaReader);
  }

  @Override
  protected String getKafkaAdminClass() {
    return KafkaAdminClient.class.getName();
  }

  @Override
  protected KafkaClientFactory clone(String kafkaBootstrapServers, String kafkaZkAddress) {
    Properties clonedProperties = this.serverConfig.getClusterProperties().toProperties();
    clonedProperties.setProperty(KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers);
    clonedProperties.setProperty(KAFKA_ZK_ADDRESS, kafkaZkAddress);
    return new VeniceServerConsumerJavaBasedFactory(new VeniceServerConfig(new VeniceProperties(clonedProperties)), kafkaMessageEnvelopeSchemaReader);
  }
}
