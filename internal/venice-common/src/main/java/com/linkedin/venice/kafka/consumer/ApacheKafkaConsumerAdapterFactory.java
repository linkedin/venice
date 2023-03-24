package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapterFactory;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.consumer.PubSubConsumer;
import com.linkedin.venice.pubsub.kafka.KafkaPubSubMessageDeserializer;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class ApacheKafkaConsumerAdapterFactory implements PubSubConsumerAdapterFactory<PubSubConsumer> {
  private static final String NAME = "ApacheKafkaConsumer";

  @Override
  public ApacheKafkaConsumer create(
      VeniceProperties veniceProperties,
      boolean isKafkaConsumerOffsetCollectionEnabled,
      PubSubMessageDeserializer pubSubMessageDeserializer,
      String consumerName) {
    ApacheKafkaConsumerConfig apacheKafkaConsumerConfig = new ApacheKafkaConsumerConfig(veniceProperties, consumerName);
    if (pubSubMessageDeserializer instanceof KafkaPubSubMessageDeserializer) {
      return new ApacheKafkaConsumer(
          new KafkaConsumer<>(apacheKafkaConsumerConfig.getConsumerProperties()),
          veniceProperties,
          isKafkaConsumerOffsetCollectionEnabled,
          (KafkaPubSubMessageDeserializer) pubSubMessageDeserializer);
    } else {
      throw new VeniceException("Only support " + KafkaPubSubMessageDeserializer.class);
    }
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void close() throws IOException {
  }
}
