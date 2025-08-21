package com.linkedin.venice.pubsub.mock.adapter.consumer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.PubSubUtil;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
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
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


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
  private static final Logger LOGGER = LogManager.getLogger(MockInMemoryConsumerAdapter.class);

  private final InMemoryPubSubBroker broker;
  private final Map<PubSubTopicPartition, InMemoryPubSubPosition> lastReadPositions = new VeniceConcurrentHashMap<>();
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
    subscribe(pubSubTopicPartition, lastReadPubSubPosition, false);
  }

  @Override
  public synchronized void subscribe(
      @Nonnull PubSubTopicPartition pubSubTopicPartition,
      @Nonnull PubSubPosition position,
      boolean isInclusive) {
    LOGGER.info(
        "Requested subscription to topic-partition: {} with position: {} isInclusive: {}",
        pubSubTopicPartition,
        position,
        isInclusive);

    long seekOffset;

    if (PubSubSymbolicPosition.EARLIEST.equals(position)) {
      seekOffset = 0L; // start from first available record
    } else if (PubSubSymbolicPosition.LATEST.equals(position)) {
      PubSubPosition resolved = endPosition(pubSubTopicPartition);
      if (!(resolved instanceof InMemoryPubSubPosition)) {
        throw new IllegalStateException(
            "endPosition returned unsupported type: " + resolved.getClass().getSimpleName());
      }
      seekOffset = ((InMemoryPubSubPosition) resolved).getInternalOffset();
    } else if (position instanceof InMemoryPubSubPosition) {
      long inputOffset = ((InMemoryPubSubPosition) position).getInternalOffset();
      seekOffset = PubSubUtil.calculateSeekOffset(inputOffset, isInclusive);
    } else if (position instanceof ApacheKafkaOffsetPosition) {
      seekOffset =
          PubSubUtil.calculateSeekOffset(((ApacheKafkaOffsetPosition) position).getInternalOffset(), isInclusive);
    } else {
      throw new IllegalArgumentException("Unsupported PubSubPosition type: " + position.getClass());
    }

    InMemoryPubSubPosition seekToPosition = InMemoryPubSubPosition.of(seekOffset);
    InMemoryPubSubPosition lastReadPosition = InMemoryPubSubPosition.of(seekOffset - 1);

    pausedTopicPartitions.remove(pubSubTopicPartition);
    delegate.subscribe(pubSubTopicPartition, seekToPosition);
    lastReadPositions.put(pubSubTopicPartition, lastReadPosition);

    LOGGER.info(
        "Subscribed to topic-partition: {} from position: {} (last read set to {})",
        pubSubTopicPartition,
        seekToPosition,
        lastReadPosition);
  }

  @Override
  public synchronized void unSubscribe(PubSubTopicPartition pubSubTopicPartition) {
    delegate.unSubscribe(pubSubTopicPartition);
    lastReadPositions.remove(pubSubTopicPartition);
    pausedTopicPartitions.remove(pubSubTopicPartition);
  }

  @Override
  public synchronized void batchUnsubscribe(Set<PubSubTopicPartition> pubSubTopicPartitionSet) {
    delegate.batchUnsubscribe(pubSubTopicPartitionSet);
    for (PubSubTopicPartition topicPartition: pubSubTopicPartitionSet) {
      lastReadPositions.remove(topicPartition);
      pausedTopicPartitions.remove(topicPartition);
    }
  }

  @Override
  public synchronized void resetOffset(PubSubTopicPartition pubSubTopicPartition) {
    if (!hasSubscription(pubSubTopicPartition)) {
      throw new PubSubUnsubscribedTopicPartitionException(pubSubTopicPartition);
    }
    delegate.resetOffset(pubSubTopicPartition);
    lastReadPositions.put(pubSubTopicPartition, InMemoryPubSubPosition.of(-1L));
  }

  @Override
  public synchronized void close() {
    delegate.close();
    pausedTopicPartitions.clear();
    lastReadPositions.clear();
  }

  @Override
  public synchronized Map<PubSubTopicPartition, List<DefaultPubSubMessage>> poll(long timeout) {
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> delegatePolledMessages = delegate.poll(timeout);
    if (delegatePolledMessages != null && !delegatePolledMessages.isEmpty()) {
      throw new IllegalArgumentException(
          "The MockInMemoryConsumer's delegate can only be used to verify calls, not to return arbitrary instances.");
    }

    Map<PubSubTopicPartition, InMemoryPubSubPosition> offsetsToPoll = new HashMap<>();
    for (Map.Entry<PubSubTopicPartition, InMemoryPubSubPosition> entry: lastReadPositions.entrySet()) {
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
      if (lastReadPositions.containsKey(topicPartition)) {
        lastReadPositions.put(topicPartition, offsetToPoll);
      }
    }
    return pubSubMessages;
  }

  @Override
  public synchronized boolean hasAnySubscription() {
    return !lastReadPositions.isEmpty();
  }

  @Override
  public synchronized boolean hasSubscription(PubSubTopicPartition pubSubTopicPartition) {
    return lastReadPositions.containsKey(pubSubTopicPartition);
  }

  public synchronized Map<PubSubTopicPartition, InMemoryPubSubPosition> getLastReadPositions() {
    return lastReadPositions;
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
    return lastReadPositions.keySet();
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
  public synchronized PubSubPosition beginningPosition(PubSubTopicPartition pubSubTopicPartition, Duration timeout) {
    return InMemoryPubSubPosition.of(0);
  }

  @Override
  public Map<PubSubTopicPartition, PubSubPosition> beginningPositions(
      Collection<PubSubTopicPartition> partitions,
      Duration timeout) {
    Map<PubSubTopicPartition, PubSubPosition> retPositions = new HashMap<>(partitions.size());
    for (PubSubTopicPartition pubSubTopicPartition: partitions) {
      retPositions.put(pubSubTopicPartition, beginningPosition(pubSubTopicPartition, timeout));
    }
    return retPositions;
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
    return PubSubUtil.computeOffsetDelta(partition, position1, position2, this);
  }

  @Override
  public PubSubPosition decodePosition(PubSubTopicPartition partition, int positionTypeId, ByteBuffer buffer) {
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
