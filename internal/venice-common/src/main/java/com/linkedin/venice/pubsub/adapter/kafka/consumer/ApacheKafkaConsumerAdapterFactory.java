package com.linkedin.venice.pubsub.adapter.kafka.consumer;

import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
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
  public ApacheKafkaConsumerAdapter create(PubSubConsumerAdapterContext context) {
    return new ApacheKafkaConsumerAdapter(new ApacheKafkaConsumerConfig(context));
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void close() throws IOException {
  }
}
