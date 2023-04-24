package com.linkedin.venice.unit.kafka.consumer;

import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.unit.kafka.InMemoryKafkaBroker;
import com.linkedin.venice.unit.kafka.MockInMemoryAdminAdapter;
import com.linkedin.venice.unit.kafka.consumer.poll.PollStrategy;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * A {@link PubSubConsumerAdapter} implementation which reads messages from the {@link InMemoryKafkaBroker}.
 *
 * Used in unit tests as a lightweight alternative to a full-fledged integration test. Can be configured
 * with various {@link PollStrategy} implementations in order to tweak the consuming behavior.
 *
 * When {@link MockInMemoryConsumer} is used to simulate the shared consumer behavior, there might be 2 different threads calling the methods
 * from this class. For example, consumer task thread from {@link com.linkedin.davinci.kafka.consumer.KafkaConsumerService} will
 * periodically call {@link MockInMemoryConsumer#poll(long)} and {@link com.linkedin.davinci.kafka.consumer.StoreIngestionTask} thread
 * is calling {@link MockInMemoryConsumer#resetOffset(PubSubTopicPartition)}, which may cause test failure.
 *
 * TODO: Remove synchronized keyword in this class when consumer operations in consumption task is event-driven.
 */
public class MockInMemoryConsumer implements PubSubConsumerAdapter {
  private final InMemoryKafkaBroker broker;
  private final Map<PubSubTopicPartition, Long> offsets = new HashMap<>();
  private final PollStrategy pollStrategy;
  private final PubSubConsumerAdapter delegate;
  private final Set<PubSubTopicPartition> pausedTopicPartitions = new HashSet<>();

  private MockInMemoryAdminAdapter adminAdapter;

  /**
   * @param delegate Can be used to pass a mock, in order to verify calls. Note: functions that return
   *                 do not return the result of the mock, but rather the results of the in-memory
   *                 consumer components.
   */
  public MockInMemoryConsumer(InMemoryKafkaBroker broker, PollStrategy pollStrategy, PubSubConsumerAdapter delegate) {
    this.broker = broker;
    this.pollStrategy = pollStrategy;
    this.delegate = delegate;
  }

  @Override
  public synchronized void subscribe(PubSubTopicPartition pubSubTopicPartition, long lastReadOffset) {
    pausedTopicPartitions.remove(pubSubTopicPartition);
    delegate.subscribe(pubSubTopicPartition, lastReadOffset);
    offsets.put(pubSubTopicPartition, lastReadOffset);
  }

  @Override
  public synchronized void unSubscribe(PubSubTopicPartition pubSubTopicPartition) {
    delegate.unSubscribe(pubSubTopicPartition);
    offsets.remove(pubSubTopicPartition);
    pausedTopicPartitions.remove(pubSubTopicPartition);
  }

  @Override
  public void batchUnsubscribe(Set<PubSubTopicPartition> pubSubTopicPartitionSet) {
    delegate.batchUnsubscribe(pubSubTopicPartitionSet);
    for (PubSubTopicPartition topicPartition: pubSubTopicPartitionSet) {
      offsets.remove(topicPartition);
      pausedTopicPartitions.remove(topicPartition);
    }
  }

  @Override
  public synchronized void resetOffset(PubSubTopicPartition pubSubTopicPartition) {
    if (!hasSubscription(pubSubTopicPartition)) {
      throw new UnsubscribedTopicPartitionException(pubSubTopicPartition);
    }
    delegate.resetOffset(pubSubTopicPartition);
    offsets.put(pubSubTopicPartition, OffsetRecord.LOWEST_OFFSET);
  }

  @Override
  public void close() {
    delegate.close();
    pausedTopicPartitions.clear();
    offsets.clear();
  }

  @Override
  public synchronized Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> poll(
      long timeout) {
    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> delegatePolledMessages =
        delegate.poll(timeout);
    if (delegatePolledMessages != null && !delegatePolledMessages.isEmpty()) {
      throw new IllegalArgumentException(
          "The MockInMemoryConsumer's delegate can only be used to verify calls, not to return arbitrary instances.");
    }

    Map<PubSubTopicPartition, Long> offsetsToPoll = new HashMap<>();
    for (Map.Entry<PubSubTopicPartition, Long> entry: offsets.entrySet()) {
      PubSubTopicPartition topicPartition = entry.getKey();
      Long offset = entry.getValue();
      if (!pausedTopicPartitions.contains(entry.getKey())) {
        offsetsToPoll.put(topicPartition, offset);
      }
    }

    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> pubSubMessages =
        pollStrategy.poll(broker, offsetsToPoll, timeout);
    for (Map.Entry<PubSubTopicPartition, Long> entry: offsetsToPoll.entrySet()) {
      PubSubTopicPartition topicPartition = entry.getKey();
      Long offsetToPoll = entry.getValue();
      if (offsets.containsKey(topicPartition)) {
        offsets.put(topicPartition, offsetToPoll);
      }
    }
    return pubSubMessages;
  }

  @Override
  public boolean hasAnySubscription() {
    return !offsets.isEmpty();
  }

  @Override
  public boolean hasSubscription(PubSubTopicPartition pubSubTopicPartition) {
    return offsets.containsKey(pubSubTopicPartition);
  }

  public Map<PubSubTopicPartition, Long> getOffsets() {
    return offsets;
  }

  @Override
  public synchronized void pause(PubSubTopicPartition pubSubTopicPartition) {
    pausedTopicPartitions.add(pubSubTopicPartition);
    delegate.pause(pubSubTopicPartition);
  }

  @Override
  public synchronized void resume(PubSubTopicPartition pubSubTopicPartition) {
    if (pausedTopicPartitions.contains(pubSubTopicPartition)) {
      pausedTopicPartitions.remove(pubSubTopicPartition);
    }
    delegate.resume(pubSubTopicPartition);
  }

  @Override
  public Set<PubSubTopicPartition> getAssignment() {
    return offsets.keySet();
  }

  @Override
  public Long offsetForTime(PubSubTopicPartition pubSubTopicPartition, long timestamp, Duration timeout) {
    return null;
  }

  @Override
  public Long offsetForTime(PubSubTopicPartition pubSubTopicPartition, long timestamp) {
    return null;
  }

  @Override
  public Long beginningOffset(PubSubTopicPartition partition, Duration timeout) {
    return 0L;
  }

  @Override
  public Map<PubSubTopicPartition, Long> endOffsets(Collection<PubSubTopicPartition> partitions, Duration timeout) {
    Map<PubSubTopicPartition, Long> retOffsets = new HashMap<>();
    for (PubSubTopicPartition pubSubTopicPartition: partitions) {
      retOffsets.put(pubSubTopicPartition, endOffset(pubSubTopicPartition));
    }
    return retOffsets;
  }

  @Override
  public Long endOffset(PubSubTopicPartition pubSubTopicPartition) {
    return broker
        .endOffsets(pubSubTopicPartition.getPubSubTopic().getName(), pubSubTopicPartition.getPartitionNumber());
  }

  @Override
  public List<PubSubTopicPartitionInfo> partitionsFor(PubSubTopic topic) {
    if (adminAdapter != null) {
      return adminAdapter.partitionsFor(topic);
    } else {
      throw new UnsupportedOperationException("In-memory admin adapter is not set");
    }
  }

  public void setMockInMemoryAdminAdapter(MockInMemoryAdminAdapter adminAdapter) {
    this.adminAdapter = adminAdapter;
  }
}
