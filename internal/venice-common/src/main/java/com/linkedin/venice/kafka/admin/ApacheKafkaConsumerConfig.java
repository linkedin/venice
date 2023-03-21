package com.linkedin.venice.kafka.admin;

import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;


public class ApacheKafkaConsumerConfig {
  private final Properties consumerProperties;

  public ApacheKafkaConsumerConfig(VeniceProperties veniceProperties, String brokerAddressToOverride) {
    this.consumerProperties = veniceProperties.toProperties();
    if (brokerAddressToOverride != null) {
      consumerProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerAddressToOverride);
    }
    // Copied from KafkaClientFactory
    consumerProperties.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
  }

  public Properties getConsumerProperties() {
    return consumerProperties;
  }
}
