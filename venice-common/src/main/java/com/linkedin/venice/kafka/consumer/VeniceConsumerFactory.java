package com.linkedin.venice.kafka.consumer;

import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public abstract class VeniceConsumerFactory {
  public KafkaConsumerWrapper getConsumer(Properties props) {
    return new ApacheKafkaConsumer(setupSSL(props));
  }

  public <K, V> KafkaConsumer<K, V> getKafkaConsumer(Properties properties){
    return new KafkaConsumer<>(setupSSL(properties));
  }

  /**
   * Setup essential ssl related configuration by putting all ssl properties of this factory into the given properties.
   */
  public abstract Properties setupSSL(Properties properties);
}
