package com.linkedin.venice.kafka.consumer;

import java.util.Properties;

public class VeniceConsumerFactory {
  public KafkaConsumerWrapper getConsumer(Properties props) {
    return new ApacheKafkaConsumer(props);
  }
}
