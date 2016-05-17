package com.linkedin.venice.kafka.consumer;

import java.util.Properties;

public class VeniceConsumerFactory {
  public VeniceConsumer getConsumer(Properties props) {
    return new ApacheKafkaConsumer(props);
  }
}
