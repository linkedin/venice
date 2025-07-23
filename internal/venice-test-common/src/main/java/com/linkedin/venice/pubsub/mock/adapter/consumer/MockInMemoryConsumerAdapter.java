package com.linkedin.venice.pubsub.mock.adapter.consumer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubUnsubscribedTopicPartitionException;
import com.linkedin.venice.pubsub.mock.InMemoryPubSubBroker;
import com.linkedin.venice.pubsub.mock.InMemoryPubSubPosition;
import com.linkedin.venice.pubsub.mock.adapter.admin.MockInMemoryAdminAdapter;
import com.linkedin.venice.pubsub.mock.adapter.consumer.poll.PollStrategy;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * A {@link PubSubConsumerAdapter} implementation which reads messages from the {@link InMemoryPubSubBroker}.
 *
 * Used in unit tests as a lightweight alternative to a full-fledged integration test. Can be configured
 * with various {@link PollStrategy} implementations in order to tweak the consuming behavior.
 *
 * When {@link MockInMemoryConsumerAdapter} is used to simulate the shared consumer behavior, there might be 2 different threads calling the methods
 * from this class. For example, consumer task thread from {@link com.linkedin.davinci.kafka.consumer.KafkaConsumerService} will
 * periodically call {@link MockInMemoryConsumerAdapter#poll(long)} and {@link com.linkedin.davinci.kafka.consumer.StoreIngestionTask} thread
 * is calling {@link MockInMemoryConsumerAdapter#resetOffset(PubSubTopicPartition)}, which may cause test failure.
 *
 */
public class MockInMemoryConsumerAdapter implements PubSubConsumerAdapter {
  private final InMemoryPubSubBroker broker;
  private final Map<PubSubTopicPartition, InMemoryPubSubPosition> offsets = new VeniceConcurrentHashMap<>();
  private final PollStrategy pollStrategy;
  private final PubSubConsumerAdapter delegate;
  private final Set<PubSubTopicPartition> pausedTopicPartitions = VeniceConcurrentHashMap.newKeySet();

  private MockInMemoryAdminAdapter adminAdapter;

  /**
   * @param delegate Can be used to pass a mock, in order to verify calls. Note: functions that return
   *                 do not return the result of the mock, but rather the results of the in-memory
   *                 consumer components.
   */
  public MockInMemoryConsumerAdapter(
      InMemoryPubSubBroker broker,
      PollStrategy pollStrategy,
      PubSubConsumerAdapter delegate) {
    this.broker = broker;
    this.pollStrategy = pollStrategy;
    this.delegate = delegate;
  }

  @Override
  public synchronized void subscribe(PubSubTopicPartition pubSubTopicPartition, long lastReadOffset) {
    subscribe(pubSubTopicPartition, InMemoryPubSubPosition.of(lastReadOffset));
  }

  @Override
  public synchronized void subscribe(PubSubTopicPartition pubSubTopicPartition, PubSubPosition lastReadPubSubPosition) {
    InMemoryPubSubPosition lastReadPosition;
    if (lastReadPubSubPosition == PubSubSymbolicPosition.EARLIEST) {
      lastReadPosition = InMemoryPubSubPosition.of(-1L);
    } else if (lastReadPubSubPosition == PubSubSymbolicPosition.LATEST) {
      lastReadPosition = (InMemoryPubSubPosition) endPosition(pubSubTopicPartition);
    } else if (lastReadPubSubPosition instanceof InMemoryPubSubPosition) {
      lastReadPosition = (InMemoryPubSubPosition) lastReadPubSubPosition;
    } else {
      throw new IllegalArgumentException("Unsupported PubSubPosition type: " + lastReadPubSubPosition.getClass());
    }

    pausedTopicPartitions.remove(pubSubTopicPartition);
    delegate.subscribe(pubSubTopicPartition, lastReadPosition);
    offsets.put(pubSubTopicPartition, lastReadPosition);
  }

  @Override
  public synchronized void unSubscribe(PubSubTopicPartition pubSubTopicPartition) {
    delegate.unSubscribe(pubSubTopicPartition);
    offsets.remove(pubSubTopicPartition);
    pausedTopicPartitions.remove(pubSubTopicPartition);
  }

  @Override
  public synchronized void batchUnsubscribe(Set<PubSubTopicPartition> pubSubTopicPartitionSet) {
    delegate.batchUnsubscribe(pubSubTopicPartitionSet);
    for (PubSubTopicPartition topicPartition: pubSubTopicPartitionSet) {
      offsets.remove(topicPartition);
      pausedTopicPartitions.remove(topicPartition);
    }
  }

  @Override
  public synchronized void resetOffset(PubSubTopicPartition pubSubTopicPartition) {
    if (!hasSubscription(pubSubTopicPartition)) {
      throw new PubSubUnsubscribedTopicPartitionException(pubSubTopicPartition);
    }
    delegate.resetOffset(pubSubTopicPartition);
    offsets.put(pubSubTopicPartition, InMemoryPubSubPosition.of(-1L));
  }

  @Override
  public synchronized void close() {
    delegate.close();
    pausedTopicPartitions.clear();
    offsets.clear();
  }

  @Override
  public synchronized Map<PubSubTopicPartition, List<DefaultPubSubMessage>> poll(long timeout) {
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> delegatePolledMessages = delegate.poll(timeout);
    if (delegatePolledMessages != null && !delegatePolledMessages.isEmpty()) {
      throw new IllegalArgumentException(
          "The MockInMemoryConsumer's delegate can only be used to verify calls, not to return arbitrary instances.");
    }

    Map<PubSubTopicPartition, InMemoryPubSubPosition> offsetsToPoll = new HashMap<>();
    for (Map.Entry<PubSubTopicPartition, InMemoryPubSubPosition> entry: offsets.entrySet()) {
      PubSubTopicPartition topicPartition = entry.getKey();
      InMemoryPubSubPosition offset = entry.getValue();
      if (!pausedTopicPartitions.contains(entry.getKey())) {
        offsetsToPoll.put(topicPartition, offset);
      }
    }

    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pubSubMessages =
        pollStrategy.poll(broker, offsetsToPoll, timeout);
    for (Map.Entry<PubSubTopicPartition, InMemoryPubSubPosition> entry: offsetsToPoll.entrySet()) {
      PubSubTopicPartition topicPartition = entry.getKey();
      InMemoryPubSubPosition offsetToPoll = entry.getValue();
      if (offsets.containsKey(topicPartition)) {
        offsets.put(topicPartition, offsetToPoll);
      }
    }
    return pubSubMessages;
  }

  @Override
  public synchronized boolean hasAnySubscription() {
    return !offsets.isEmpty();
  }

  @Override
  public synchronized boolean hasSubscription(PubSubTopicPartition pubSubTopicPartition) {
    return offsets.containsKey(pubSubTopicPartition);
  }

  public synchronized Map<PubSubTopicPartition, InMemoryPubSubPosition> getOffsets() {
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
  public synchronized Set<PubSubTopicPartition> getAssignment() {
    return offsets.keySet();
  }

  @Override
  public synchronized Long offsetForTime(PubSubTopicPartition pubSubTopicPartition, long timestamp, Duration timeout) {
    return null;
  }

  @Override
  public synchronized PubSubPosition getPositionByTimestamp(
      PubSubTopicPartition pubSubTopicPartition,
      long timestamp,
      Duration timeout) {
    return null;
  }

  @Override
  public synchronized Long offsetForTime(PubSubTopicPartition pubSubTopicPartition, long timestamp) {
    return null;
  }

  @Override
  public synchronized PubSubPosition getPositionByTimestamp(PubSubTopicPartition pubSubTopicPartition, long timestamp) {
    return null;
  }

  @Override
  public synchronized Long beginningOffset(PubSubTopicPartition partition, Duration timeout) {
    return 0L;
  }

  @Override
  public synchronized PubSubPosition beginningPosition(PubSubTopicPartition pubSubTopicPartition, Duration timeout) {
    return PubSubSymbolicPosition.EARLIEST;
  }

  @Override
  public synchronized Map<PubSubTopicPartition, Long> endOffsets(
      Collection<PubSubTopicPartition> partitions,
      Duration timeout) {
    Map<PubSubTopicPartition, Long> retOffsets = new HashMap<>();
    for (PubSubTopicPartition pubSubTopicPartition: partitions) {
      retOffsets.put(pubSubTopicPartition, endOffset(pubSubTopicPartition));
    }
    return retOffsets;
  }

  @Override
  public synchronized Map<PubSubTopicPartition, PubSubPosition> endPositions(
      Collection<PubSubTopicPartition> partitions,
      Duration timeout) {
    Map<PubSubTopicPartition, PubSubPosition> retPositions = new HashMap<>(partitions.size());
    for (PubSubTopicPartition pubSubTopicPartition: partitions) {
      retPositions.put(pubSubTopicPartition, endPosition(pubSubTopicPartition));
    }
    return retPositions;
  }

  @Override
  public synchronized Long endOffset(PubSubTopicPartition pubSubTopicPartition) {
    return broker
        .endOffsets(pubSubTopicPartition.getPubSubTopic().getName(), pubSubTopicPartition.getPartitionNumber());
  }

  @Override
  public synchronized PubSubPosition endPosition(PubSubTopicPartition pubSubTopicPartition) {
    return broker
        .endPosition(pubSubTopicPartition.getPubSubTopic().getName(), pubSubTopicPartition.getPartitionNumber());
  }

  @Override
  public synchronized List<PubSubTopicPartitionInfo> partitionsFor(PubSubTopic topic) {
    if (adminAdapter != null) {
      return adminAdapter.partitionsFor(topic);
    } else {
      throw new UnsupportedOperationException("In-memory admin adapter is not set");
    }
  }

  @Override
  public long comparePositions(PubSubTopicPartition partition, PubSubPosition position1, PubSubPosition position2) {
    return positionDifference(partition, position1, position2);
  }

  @Override
  public long positionDifference(PubSubTopicPartition partition, PubSubPosition position1, PubSubPosition position2) {
    if (position1 == null || position2 == null) {
      throw new IllegalArgumentException("Positions cannot be null");
    }

    PubSubPosition resolved1 = resolveSymbolicPosition(partition, position1);
    PubSubPosition resolved2 = resolveSymbolicPosition(partition, position2);

    // Case 1: Both resolved to concrete InMemoryPubSubPosition
    if (resolved1 instanceof InMemoryPubSubPosition && resolved2 instanceof InMemoryPubSubPosition) {
      long offset1 = ((InMemoryPubSubPosition) resolved1).getInternalOffset();
      long offset2 = ((InMemoryPubSubPosition) resolved2).getInternalOffset();
      return offset1 - offset2;
    }

    // Case 2: Both unresolved symbolic positions and equal
    if (resolved1 == resolved2
        && (resolved1 == PubSubSymbolicPosition.EARLIEST || resolved1 == PubSubSymbolicPosition.LATEST)) {
      return 0L;
    }

    // Case 3: One is EARLIEST, one is concrete
    if (resolved1 == PubSubSymbolicPosition.EARLIEST && resolved2 instanceof InMemoryPubSubPosition) {
      return -((InMemoryPubSubPosition) resolved2).getInternalOffset();
    }
    if (resolved2 == PubSubSymbolicPosition.EARLIEST && resolved1 instanceof InMemoryPubSubPosition) {
      return ((InMemoryPubSubPosition) resolved1).getInternalOffset();
    }

    // Case 4: One is LATEST, one is concrete
    if (resolved1 == PubSubSymbolicPosition.LATEST && resolved2 instanceof InMemoryPubSubPosition) {
      return Long.MAX_VALUE - ((InMemoryPubSubPosition) resolved2).getInternalOffset();
    }
    if (resolved2 == PubSubSymbolicPosition.LATEST && resolved1 instanceof InMemoryPubSubPosition) {
      return ((InMemoryPubSubPosition) resolved1).getInternalOffset() - Long.MAX_VALUE;
    }

    throw new IllegalArgumentException(
        "Unsupported position types: " + resolved1.getClass().getName() + " vs " + resolved2.getClass().getName());
  }

  private PubSubPosition resolveSymbolicPosition(PubSubTopicPartition partition, PubSubPosition position) {
    if (position == PubSubSymbolicPosition.EARLIEST) {
      return beginningPosition(partition, Duration.ofMillis(60000));
    } else if (position == PubSubSymbolicPosition.LATEST) {
      return endPosition(partition);
    }
    return position;
  }

  @Override
  public PubSubPosition decodePosition(PubSubTopicPartition partition, ByteBuffer buffer) {
    try {
      if (buffer.remaining() < Long.BYTES) {
        throw new VeniceException("Buffer too short to decode InMemoryPubSubPosition: " + buffer);
      }
      long offset = buffer.getLong();
      return InMemoryPubSubPosition.of(offset);
    } catch (Exception e) {
      throw new VeniceException("Failed to decode InMemoryPubSubPosition from buffer: " + buffer, e);
    }
  }

  public synchronized void setMockInMemoryAdminAdapter(MockInMemoryAdminAdapter adminAdapter) {
    this.adminAdapter = adminAdapter;
  }
}
