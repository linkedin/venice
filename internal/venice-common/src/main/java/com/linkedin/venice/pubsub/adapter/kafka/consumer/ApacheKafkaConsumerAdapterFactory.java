package com.linkedin.venice.pubsub.adapter.kafka.consumer;

import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;


public class ApacheKafkaConsumerAdapterFactory extends PubSubConsumerAdapterFactory<PubSubConsumerAdapter> {
  private static final String NAME = "ApacheKafkaConsumerAdapter";

  /**
   * Constructor for ApacheKafkaConsumerAdapterFactory used mainly for reflective instantiation.
   */
  public ApacheKafkaConsumerAdapterFactory() {
    // no-op
  }

  @Override
  public ApacheKafkaConsumerAdapter create(
      VeniceProperties veniceProperties,
      boolean isKafkaConsumerOffsetCollectionEnabled,
      PubSubMessageDeserializer pubSubMessageDeserializer,
      String consumerName) {
    ApacheKafkaConsumerConfig apacheKafkaConsumerConfig = new ApacheKafkaConsumerConfig(veniceProperties, consumerName);
    return new ApacheKafkaConsumerAdapter(
        apacheKafkaConsumerConfig,
        pubSubMessageDeserializer,
        isKafkaConsumerOffsetCollectionEnabled);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void close() throws IOException {
  }
}
