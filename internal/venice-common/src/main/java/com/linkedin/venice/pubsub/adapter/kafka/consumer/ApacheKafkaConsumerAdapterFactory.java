package com.linkedin.venice.pubsub.adapter.kafka.consumer;

import com.linkedin.venice.pubsub.api.PubSubClientConfigs;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapterFactory;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class ApacheKafkaConsumerAdapterFactory implements PubSubConsumerAdapterFactory<PubSubConsumerAdapter> {
  private static final String NAME = "ApacheKafkaConsumerAdapter";

  @Override
  public ApacheKafkaConsumerAdapter create(
      VeniceProperties veniceProperties,
      PubSubClientConfigs pubSubClientConfigs,
      boolean isKafkaConsumerOffsetCollectionEnabled,
      String consumerName) {
    ApacheKafkaConsumerConfig apacheKafkaConsumerConfig = new ApacheKafkaConsumerConfig(veniceProperties, consumerName);
    return new ApacheKafkaConsumerAdapter(
        new KafkaConsumer<>(apacheKafkaConsumerConfig.getConsumerProperties()),
        veniceProperties,
        pubSubClientConfigs,
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
