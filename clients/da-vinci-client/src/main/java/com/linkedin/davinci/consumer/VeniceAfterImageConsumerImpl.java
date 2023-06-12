package com.linkedin.davinci.consumer;

import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import java.util.Collection;


public class VeniceAfterImageConsumerImpl<K, V> extends VeniceChangelogConsumerImpl<K, V> {
  public VeniceAfterImageConsumerImpl(ChangelogClientConfig changelogClientConfig, PubSubConsumerAdapter consumer) {
    super(changelogClientConfig, consumer);
  }

  @Override
  public Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> poll(long timeoutInMs) {
    return internalPoll(timeoutInMs, "");
  }
}
