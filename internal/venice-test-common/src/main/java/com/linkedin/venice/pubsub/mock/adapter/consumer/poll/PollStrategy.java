package com.linkedin.venice.pubsub.mock.adapter.consumer.poll;

import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.mock.InMemoryPubSubBroker;
import com.linkedin.venice.pubsub.mock.InMemoryPubSubPosition;
import com.linkedin.venice.pubsub.mock.adapter.consumer.MockInMemoryConsumerAdapter;
import java.util.List;
import java.util.Map;


/**
 * This interface is used inside of the {@link MockInMemoryConsumerAdapter}
 * in order to mess around with the way messages are delivered to the consuming code.
 *
 * This is used in unit tests in order to control message deliver order, introduce duplicates, inject new
 * arbitrary messages, skip messages...
 */
public interface PollStrategy {
  Map<PubSubTopicPartition, List<DefaultPubSubMessage>> poll(
      InMemoryPubSubBroker broker,
      Map<PubSubTopicPartition, InMemoryPubSubPosition> offsets,
      long timeout);
}
