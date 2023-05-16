package com.linkedin.davinci.consumer;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import java.util.Collection;
import org.apache.kafka.clients.consumer.Consumer;


public class VeniceAfterImageConsumerImpl<K, V> extends VeniceChangelogConsumerImpl<K, V> {
  public VeniceAfterImageConsumerImpl(
      ChangelogClientConfig changelogClientConfig,
      Consumer<KafkaKey, KafkaMessageEnvelope> kafkaConsumer) {
    super(changelogClientConfig, kafkaConsumer);
  }

  @Override
  public Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> poll(long timeoutInMs) {
    return internalPoll(timeoutInMs, "");
  }
}
