package com.linkedin.davinci.consumer;

import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.FAIL;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.SUCCESS;

import com.linkedin.davinci.consumer.stats.BasicConsumerStats;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.Time;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Periodically commits the positions of records a changelog consumer has consumed to the PubSub broker, so that
 * external consumer-group lag monitoring can track the consumer. Enabled by
 * {@link ChangelogClientConfig#setConsumerOffsetCommitEnabled(boolean)}.
 *
 * <p>Commits are monitoring-only and never throw: failures are logged (rate limited) and counted, and the consumer
 * keeps going. If the {@link PubSubConsumerAdapter} does not support commits, the committer disables itself.
 *
 * <p>The latest position of every tracked topic-partition is re-committed on every interval, even when unchanged, so
 * that idle consumers keep their committed positions alive on the broker. When a partition moves to another topic
 * (version swap) or is removed (unsubscribe), its last position is included in the next commit attempt only, whether
 * or not that attempt succeeds, so that a deleted old version topic cannot fail every subsequent commit.
 */
class ChangelogConsumerOffsetCommitter {
  private static final Logger LOGGER = LogManager.getLogger(ChangelogConsumerOffsetCommitter.class);
  private static final RedundantExceptionFilter REDUNDANT_EXCEPTION_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();

  private final PubSubConsumerAdapter pubSubConsumerAdapter;
  private final long commitIntervalMs;
  private final Duration commitTimeout;
  private final Time time;
  private final BasicConsumerStats consumerStats;
  private final String consumerName;

  private final Map<Integer, PubSubTopicPartition> partitionToTopicPartition = new HashMap<>();
  private final Map<PubSubTopicPartition, PubSubPosition> latestPositions = new HashMap<>();
  private final Map<PubSubTopicPartition, PubSubPosition> retiringPositions = new HashMap<>();
  private long lastCommitTimeMs;
  private boolean disabled = false;

  ChangelogConsumerOffsetCommitter(
      PubSubConsumerAdapter pubSubConsumerAdapter,
      long commitIntervalMs,
      long commitTimeoutMs,
      Time time,
      BasicConsumerStats consumerStats,
      String consumerName) {
    this.pubSubConsumerAdapter = Objects.requireNonNull(pubSubConsumerAdapter, "pubSubConsumerAdapter cannot be null");
    this.commitIntervalMs = commitIntervalMs;
    this.commitTimeout = Duration.ofMillis(commitTimeoutMs);
    this.time = Objects.requireNonNull(time, "time cannot be null");
    this.consumerStats = consumerStats;
    this.consumerName = consumerName;
    this.lastCommitTimeMs = time.getMilliseconds();
  }

  /**
   * Tracks the given position as the last consumed position of its topic-partition. Null and symbolic positions
   * (e.g. synthetic heartbeats) are ignored.
   */
  synchronized void recordConsumed(PubSubTopicPartition topicPartition, PubSubPosition position) {
    if (disabled || topicPartition == null || position == null || position.isSymbolic()) {
      return;
    }
    int partition = topicPartition.getPartitionNumber();
    PubSubTopicPartition previousTopicPartition = partitionToTopicPartition.put(partition, topicPartition);
    if (previousTopicPartition != null && !previousTopicPartition.equals(topicPartition)) {
      retire(previousTopicPartition);
      LOGGER.info(
          "Consumer: {} moved partition: {} from topic: {} to topic: {}. Committing the last position of the previous topic once.",
          consumerName,
          partition,
          previousTopicPartition.getTopicName(),
          topicPartition.getTopicName());
    }
    retiringPositions.remove(topicPartition);
    latestPositions.put(topicPartition, position);
  }

  /**
   * Tracks the last consumed position of every message whose topic-partition is current or still tracked, then retires
   * every tracked topic-partition that is no longer current. Recording before retiring includes records buffered before
   * a version swap or unsubscribe in the final commit, while records arriving after retirement cannot re-track it.
   */
  synchronized void recordConsumed(
      Iterable<? extends PubSubMessage<?, ?, VeniceChangeCoordinate>> messages,
      Predicate<PubSubTopicPartition> isCurrent) {
    for (PubSubMessage<?, ?, VeniceChangeCoordinate> message: messages) {
      PubSubTopicPartition topicPartition = message.getTopicPartition();
      if (isCurrent.test(topicPartition)
          || topicPartition.equals(partitionToTopicPartition.get(topicPartition.getPartitionNumber()))) {
        VeniceChangeCoordinate coordinate = message.getPosition();
        recordConsumed(topicPartition, coordinate == null ? null : coordinate.getPosition());
      }
    }
    retireIf(entry -> !isCurrent.test(entry.getValue()));
  }

  /**
   * Stops tracking the given partitions after one final commit attempt of their last positions. An empty set means
   * all.
   */
  synchronized void removePartitions(Set<Integer> partitions) {
    retireIf(entry -> partitions.isEmpty() || partitions.contains(entry.getKey()));
  }

  /**
   * Commits if at least the configured interval has elapsed since the previous commit attempt.
   */
  synchronized void maybeCommit() {
    if (time.getMilliseconds() - lastCommitTimeMs >= commitIntervalMs) {
      commit();
    }
  }

  /**
   * Commits the tracked positions now. Never throws.
   */
  synchronized void commit() {
    lastCommitTimeMs = time.getMilliseconds();
    if (disabled || (latestPositions.isEmpty() && retiringPositions.isEmpty())) {
      return;
    }
    Map<PubSubTopicPartition, PubSubPosition> positionsToCommit = new HashMap<>(retiringPositions);
    positionsToCommit.putAll(latestPositions);
    try {
      pubSubConsumerAdapter.commitSync(positionsToCommit, commitTimeout);
      emitMetric(true);
    } catch (UnsupportedOperationException e) {
      disabled = true;
      latestPositions.clear();
      partitionToTopicPartition.clear();
      LOGGER.warn(
          "Consumer: {} disabled consumed position commits because the PubSub consumer does not support them.",
          consumerName,
          e);
    } catch (Exception e) {
      emitMetric(false);
      String logKey = ChangelogConsumerOffsetCommitter.class.getName() + ":commitFailed:" + consumerName;
      if (!REDUNDANT_EXCEPTION_FILTER.isRedundantException(logKey)) {
        LOGGER.warn(
            "Consumer: {} failed to commit consumed positions for {} topic-partitions. Will retry on the next interval.",
            consumerName,
            positionsToCommit.size(),
            e);
      }
    } finally {
      retiringPositions.clear();
      lastCommitTimeMs = time.getMilliseconds();
    }
  }

  private void retireIf(Predicate<Map.Entry<Integer, PubSubTopicPartition>> predicate) {
    Iterator<Map.Entry<Integer, PubSubTopicPartition>> iterator = partitionToTopicPartition.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Integer, PubSubTopicPartition> entry = iterator.next();
      if (predicate.test(entry)) {
        retire(entry.getValue());
        iterator.remove();
      }
    }
  }

  private void retire(PubSubTopicPartition topicPartition) {
    PubSubPosition lastPosition = latestPositions.remove(topicPartition);
    if (lastPosition != null) {
      retiringPositions.put(topicPartition, lastPosition);
    }
  }

  private void emitMetric(boolean success) {
    if (consumerStats != null) {
      consumerStats.emitOffsetCommitCountMetrics(success ? SUCCESS : FAIL);
    }
  }
}
