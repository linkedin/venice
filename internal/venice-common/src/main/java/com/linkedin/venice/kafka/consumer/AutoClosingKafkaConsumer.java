package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.annotation.NotThreadsafe;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.lazy.LazyResettable;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;


/**
 * This implementation of Kafka's {@link Consumer} interface starts off completely inert, and
 * lazily instantiates a {@link org.apache.kafka.clients.consumer.KafkaConsumer} when the first
 * partition is subscribed to. Conversely, it closes itself when the last subscribed partition
 * is removed. It can go back and forth like this indefinitely.
 *
 * There are two motivations for this wrapper:
 *
 * 1. Efficiency. Reaping unused consumers in a way that is transparent to the using code is
 *    attractive, as it encapsulates the resource recuperation into a single location.
 *
 * 2. Stability. There are cases where swapping all brokers of a Kafka cluster while some
 *    {@link Consumer} instance is idle causes issues. Letting consumers auto-close themselves
 *    when not in use is a way to mitigate that issue. Note that it is not a perfect mitigation,
 *    since it is possible for a consumer to still be idle (i.e. not polling) while having some
 *    subscribed partitions. In the Venice use case, it is assumed that this edge case doesn't
 *    happen.
 *
 * TODO: If the metrics show that there is too much oscillation in the auto-creation and closing,
 *       we can tweak this implementation to include some delay before the idleness results in
 *       auto-closing. Since this adds significant complexity, we are avoiding it for now.
 */
@NotThreadsafe
public class AutoClosingKafkaConsumer<K, V> implements Consumer<K, V> {
  private static final long NEVER = -1;

  private final Optional<AutoClosingKafkaConsumerStats> stats;
  private final LazyResettable<KafkaConsumer> kafkaConsumer;

  private long lastInstantiationTime = NEVER;
  private long lastAutoCloseTime = NEVER;

  public AutoClosingKafkaConsumer(Properties properties, Optional<AutoClosingKafkaConsumerStats> stats) {
    this.stats = stats;
    this.kafkaConsumer = LazyResettable.of(() -> {
      stats.ifPresent(s -> {
        lastInstantiationTime = System.currentTimeMillis();
        if (lastAutoCloseTime == NEVER) {
          s.recordAutoCreate();
        } else {
          // N.B This implicitly records the auto_create as well
          s.recordDurationFromCloseToCreate(lastInstantiationTime - lastAutoCloseTime);
        }
      });
      return new KafkaConsumer(properties);
    });
  }

  private void closeIfIdle() {
    if (assignment().isEmpty()) {
      kafkaConsumer.ifPresent(consumer -> {
        Utils.closeQuietlyWithErrorLogged(consumer);
        kafkaConsumer.reset();
        stats.ifPresent(s -> {
          lastAutoCloseTime = System.currentTimeMillis();
          if (lastInstantiationTime == NEVER) {
            s.recordAutoClose();
          } else {
            // N.B. This implicitly records the auto_close as well
            s.recordDurationFromCreateToClose(lastAutoCloseTime - lastInstantiationTime);
          }
        });
      });
    }
  }

  /**
   * Some of the functions are not worth the complexity of supporting, due to their asynchronous nature.
   * The {@link #closeIfIdle()} call cannot happen right after delegating to the consumer's function because
   * the action might not be done by then. And we cannot necessarily assume that the operation is a no-op if
   * the {@link #kafkaConsumer} is not present. In any case, these functions are not used in Venice so let's
   * not bother.
   */
  private void callbacksAreUnsupported() {
    throw new VeniceException("Function with callbacks are not supported by " + this.getClass().getSimpleName());
  }

  @Override
  public Set<TopicPartition> assignment() {
    if (kafkaConsumer.isPresent()) {
      return kafkaConsumer.get().assignment();
    }
    return Collections.emptySet();
  }

  @Override
  public Set<String> subscription() {
    if (kafkaConsumer.isPresent()) {
      return kafkaConsumer.get().subscription();
    }
    return Collections.emptySet();
  }

  @Override
  public void subscribe(Collection<String> topics) {
    subscribe(topics, new NoOpConsumerRebalanceListener());
  }

  @Override
  public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
    callbacksAreUnsupported();
  }

  @Override
  public void assign(Collection<TopicPartition> partitions) {
    if (partitions == null) {
      throw new IllegalArgumentException("Topic partition collection to assign to cannot be null");
    }
    if (partitions.isEmpty() && !kafkaConsumer.isPresent()) {
      // No need to instantiate the consumer if we'll then assign nothing to it
      return;
    }

    try {
      kafkaConsumer.get().assign(partitions);
    } finally {
      /**
       * N.B. If {@code partitions.isEmpty()} then this call results in unsubscribing from
       *      all partitions. Therefore, we would want to auto-close in that case.
       */
      closeIfIdle();
    }
  }

  @Override
  public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
    callbacksAreUnsupported();
  }

  @Override
  public void subscribe(Pattern pattern) {
    /**
     * Although this function doesn't take in a callback param, internally it does delegate to
     * {@link KafkaConsumer#subscribe(Pattern, ConsumerRebalanceListener)} so we're nipping it
     * in the bud out of extra caution...
     */
    callbacksAreUnsupported();
  }

  @Override
  public void unsubscribe() {
    if (!kafkaConsumer.isPresent()) {
      // No need to instantiate the consumer if we'll then assign nothing to it
      return;
    }

    try {
      kafkaConsumer.get().unsubscribe();
    } finally {
      // In theory, it should always decide to close, but if an exception was thrown, maybe not...
      closeIfIdle();
    }
  }

  @Deprecated
  @Override
  public ConsumerRecords<K, V> poll(long timeout) {
    if (!kafkaConsumer.isPresent()) {
      throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
    }

    return kafkaConsumer.get().poll(timeout);
  }

  @Override
  public ConsumerRecords<K, V> poll(Duration timeout) {
    if (!kafkaConsumer.isPresent()) {
      throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
    }

    return kafkaConsumer.get().poll(timeout);
  }

  @Override
  public void commitSync() {
    kafkaConsumer.ifPresent(consumer -> consumer.commitSync());
  }

  @Override
  public void commitSync(Duration timeout) {
    kafkaConsumer.ifPresent(consumer -> consumer.commitSync(timeout));
  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
    try {
      kafkaConsumer.get().commitSync(offsets);
    } finally {
      closeIfIdle();
    }
  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
    try {
      kafkaConsumer.get().commitSync(offsets, timeout);
    } finally {
      closeIfIdle();
    }
  }

  @Override
  public void commitAsync() {
    kafkaConsumer.ifPresent(consumer -> consumer.commitAsync());
  }

  @Override
  public void commitAsync(OffsetCommitCallback callback) {
    callbacksAreUnsupported();
  }

  @Override
  public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
    callbacksAreUnsupported();
  }

  @Override
  public void seek(TopicPartition partition, long offset) {
    try {
      kafkaConsumer.get().seek(partition, offset);
    } finally {
      closeIfIdle();
    }
  }

  @Override
  public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
    try {
      kafkaConsumer.get().seek(partition, offsetAndMetadata);
    } finally {
      closeIfIdle();
    }
  }

  @Override
  public void seekToBeginning(Collection<TopicPartition> partitions) {
    if (partitions == null) {
      throw new IllegalArgumentException("Partitions collection cannot be null");
    }
    try {
      kafkaConsumer.get().seekToBeginning(partitions);
    } finally {
      closeIfIdle();
    }
  }

  @Override
  public void seekToEnd(Collection<TopicPartition> partitions) {
    if (partitions == null) {
      throw new IllegalArgumentException("Partitions collection cannot be null");
    }
    try {
      kafkaConsumer.get().seekToEnd(partitions);
    } finally {
      closeIfIdle();
    }
  }

  @Override
  public long position(TopicPartition partition) {
    try {
      return kafkaConsumer.get().position(partition);
    } finally {
      closeIfIdle();
    }
  }

  @Override
  public long position(TopicPartition partition, Duration timeout) {
    try {
      return kafkaConsumer.get().position(partition, timeout);
    } finally {
      closeIfIdle();
    }
  }

  @Override
  public OffsetAndMetadata committed(TopicPartition partition) {
    try {
      return kafkaConsumer.get().committed(partition);
    } finally {
      closeIfIdle();
    }
  }

  @Override
  public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
    try {
      return kafkaConsumer.get().committed(partition, timeout);
    } finally {
      closeIfIdle();
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
    if (partitions == null || partitions.isEmpty()) {
      Collections.emptyMap();
    }
    try {
      return kafkaConsumer.get().committed(partitions);
    } finally {
      closeIfIdle();
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, final Duration timeout) {
    if (partitions == null || partitions.isEmpty()) {
      Collections.emptyMap();
    }
    try {
      return kafkaConsumer.get().committed(partitions, timeout);
    } finally {
      closeIfIdle();
    }
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    if (kafkaConsumer.isPresent()) {
      return kafkaConsumer.get().metrics();
    } else {
      return Collections.emptyMap();
    }
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    try {
      return kafkaConsumer.get().partitionsFor(topic);
    } finally {
      closeIfIdle();
    }
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
    try {
      return kafkaConsumer.get().partitionsFor(topic, timeout);
    } finally {
      closeIfIdle();
    }
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics() {
    try {
      return kafkaConsumer.get().listTopics();
    } finally {
      closeIfIdle();
    }
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
    try {
      return kafkaConsumer.get().listTopics(timeout);
    } finally {
      closeIfIdle();
    }
  }

  @Override
  public Set<TopicPartition> paused() {
    if (kafkaConsumer.isPresent()) {
      return kafkaConsumer.get().paused();
    }
    return Collections.emptySet();
  }

  @Override
  public void pause(Collection<TopicPartition> partitions) {
    try {
      kafkaConsumer.get().pause(partitions);
    } finally {
      closeIfIdle();
    }
  }

  @Override
  public void resume(Collection<TopicPartition> partitions) {
    try {
      kafkaConsumer.get().resume(partitions);
    } finally {
      closeIfIdle();
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
    try {
      return kafkaConsumer.get().offsetsForTimes(timestampsToSearch);
    } finally {
      closeIfIdle();
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
      Map<TopicPartition, Long> timestampsToSearch,
      Duration timeout) {
    try {
      return kafkaConsumer.get().offsetsForTimes(timestampsToSearch, timeout);
    } finally {
      closeIfIdle();
    }
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
    try {
      return kafkaConsumer.get().beginningOffsets(partitions);
    } finally {
      closeIfIdle();
    }
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
    try {
      return kafkaConsumer.get().beginningOffsets(partitions, timeout);
    } finally {
      closeIfIdle();
    }
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
    try {
      return kafkaConsumer.get().endOffsets(partitions);
    } finally {
      closeIfIdle();
    }
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
    try {
      return kafkaConsumer.get().endOffsets(partitions, timeout);
    } finally {
      closeIfIdle();
    }
  }

  @Override
  public void close() {
    if (kafkaConsumer.isPresent()) {
      kafkaConsumer.get().close();
      kafkaConsumer.reset();
    }
  }

  @Override
  public void close(long timeout, TimeUnit unit) {
    if (kafkaConsumer.isPresent()) {
      kafkaConsumer.get().close(timeout, unit);
      kafkaConsumer.reset();
    }
  }

  @Override
  public void close(Duration timeout) {
    if (kafkaConsumer.isPresent()) {
      kafkaConsumer.get().close(timeout);
      kafkaConsumer.reset();
    }
  }

  @Override
  public void wakeup() {
    if (kafkaConsumer.isPresent()) {
      kafkaConsumer.get().wakeup();
    }
  }
}
