package com.linkedin.venice.pubsub.adapter.kafka.producer;

import org.apache.kafka.clients.producer.ProducerConfig;


/**
 * A class containing shared kafka producer related configurations.
 */
public class SharedKafkaProducerConfig {

  // This helps override kafka config for shared producer separately than dedicated producer.
  public static final String SHARED_KAFKA_PRODUCER_CONFIG_PREFIX = "shared.producer.";
  public static final String SHARED_KAFKA_PRODUCER_BATCH_SIZE =
      SHARED_KAFKA_PRODUCER_CONFIG_PREFIX + ProducerConfig.BATCH_SIZE_CONFIG;
}
