package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.utils.LatencyUtils.getElapsedTimeFromMsToMs;

import com.linkedin.davinci.stats.AggKafkaConsumerServiceStats;
import com.linkedin.venice.annotation.UnderDevelopment;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubUnsubscribedTopicPartitionException;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is a synchronized version of {@link PubSubConsumerAdapter}.
 *
 * In addition to the existing API of {@link PubSubConsumerAdapter}, this class also adds specific functions used by
 * {@link KafkaConsumerService}, notably: {@link #subscribe(PubSubTopic, PubSubTopicPartition, long)} which keeps track of the
 * mapping of which TopicPartition is used by which version-topic.
 *
 * It also provides some callbacks used by the {@link KafkaConsumerService} to react to certain changes, in a way that
 * minimizes bidirectional coupling as much as possible.
 * TODO: move this logic inside consumption task, this class does not need to be sub-class of {@link PubSubConsumerAdapter}
 */
class SharedKafkaConsumer implements PubSubConsumerAdapter {
  // StoreIngestionTask#consumerUnSubscribeForStateTransition() uses an increased max wait (30 mins by default) for
  // safety
  public static final long DEFAULT_MAX_WAIT_MS = TimeUnit.SECONDS.toMillis(10);

  private static final Logger LOGGER = LogManager.getLogger(SharedKafkaConsumer.class);

  protected final PubSubConsumerAdapter delegate;

  private final AggKafkaConsumerServiceStats stats;

  private final Runnable assignmentChangeListener;

  private final UnsubscriptionListener unsubscriptionListener;

  /**
   * This field is used to cache the size information of the current assignment in order to reduce threads contention because
   * getting the size no longer requires calling the synchronized {@link SharedKafkaConsumer#getAssignment}
   */
  private final AtomicInteger currentAssignmentSize;

  /**
   * This indicates if there is any thread waiting for a poll to happen
   */
  private final AtomicBoolean waitingForPoll = new AtomicBoolean(false);

  private long timeoutMsOverride = -1; // for unit test purposes

  private final Time time;

  /**
   * Used to keep track of which version-topic is intended to use a given subscription, in order to detect
   * regressions where we would end up using this consumer to subscribe to a given topic-partition on behalf
   * of multiple version-topics.
   */
  private final VeniceConcurrentHashMap<PubSubTopicPartition, PubSubTopic> subscribedTopicPartitionToVersionTopic =
      new VeniceConcurrentHashMap();

  /**
   * This cached assignment is for performance optimization purpose since {@link #hasSubscription} could be invoked frequently.
   * This set should be unmodifiable.
   */
  private Set<PubSubTopicPartition> currentAssignment;

  /**
   * an ever-increasing count of number of time poll has been invoked.
   */
  private volatile long pollTimes = 0;

  public SharedKafkaConsumer(
      PubSubConsumerAdapter delegate,
      AggKafkaConsumerServiceStats stats,
      Runnable assignmentChangeListener,
      UnsubscriptionListener unsubscriptionListener) {
    this(delegate, stats, assignmentChangeListener, unsubscriptionListener, new SystemTime());
  }

  SharedKafkaConsumer(
      PubSubConsumerAdapter delegate,
      AggKafkaConsumerServiceStats stats,
      Runnable assignmentChangeListener,
      UnsubscriptionListener unsubscriptionListener,
      Time time) {
    this.delegate = delegate;
    this.stats = stats;
    this.assignmentChangeListener = assignmentChangeListener;
    this.unsubscriptionListener = unsubscriptionListener;
    this.time = time;
    this.currentAssignment = Collections.emptySet();
    this.currentAssignmentSize = new AtomicInteger(0);
  }

  /**
   * Listeners may use this callback to clean up lingering state they may be holding about a consumer.
   */
  interface UnsubscriptionListener {
    void call(SharedKafkaConsumer consumer, PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition);
  }

  protected synchronized void updateCurrentAssignment(Set<PubSubTopicPartition> newAssignment) {
    final long updateCurrentAssignmentStartTime = System.currentTimeMillis();
    currentAssignmentSize.set(newAssignment.size());
    currentAssignment = Collections.unmodifiableSet(newAssignment);
    assignmentChangeListener.run();
    stats.recordTotalUpdateCurrentAssignmentLatency(getElapsedTimeFromMsToMs(updateCurrentAssignmentStartTime));
  }

  @Override
  public synchronized void subscribe(PubSubTopicPartition pubSubTopicPartition, long lastReadOffset) {
    throw new VeniceException(
        this.getClass().getSimpleName() + " does not support subscribe without specifying a version-topic.");
  }

  @UnderDevelopment(value = "This API may not be implemented in all PubSubConsumerAdapter implementations.")
  @Override
  public synchronized void subscribe(PubSubTopicPartition pubSubTopicPartition, PubSubPosition lastReadPubSubPosition) {
    throw new VeniceException(
        this.getClass().getSimpleName() + " does not support subscribe without specifying a version-topic.");
  }

  @Override
  public void subscribe(
      @Nonnull PubSubTopicPartition pubSubTopicPartition,
      @Nonnull PubSubPosition position,
      boolean isInclusive) {
    throw new VeniceException(
        this.getClass().getSimpleName() + " does not support subscribe without specifying a version-topic.");
  }

  synchronized void subscribe(
      PubSubTopic versionTopic,
      PubSubTopicPartition topicPartitionToSubscribe,
      PubSubPosition lastReadPosition) {
    long delegateSubscribeStartTime = System.currentTimeMillis();
    this.delegate.subscribe(topicPartitionToSubscribe, lastReadPosition);
    PubSubTopic previousVersionTopic =
        subscribedTopicPartitionToVersionTopic.put(topicPartitionToSubscribe, versionTopic);
    if (previousVersionTopic != null && !previousVersionTopic.equals(versionTopic)) {
      throw new IllegalStateException(
          "A shared consumer cannot be used to subscribe to the same topic-partition by different VTs!"
              + " versionTopic: " + versionTopic + ", previousVersionTopic: " + previousVersionTopic
              + ", topicPartitionToSubscribe: " + topicPartitionToSubscribe);
    }
    stats.recordTotalDelegateSubscribeLatency(getElapsedTimeFromMsToMs(delegateSubscribeStartTime));
    updateCurrentAssignment(delegate.getAssignment());
  }

  public synchronized void unSubscribe(PubSubTopicPartition pubSubTopicPartition) {
    unSubscribe(pubSubTopicPartition, DEFAULT_MAX_WAIT_MS);
  }

  /**
   * There is an additional goal of this function which is to make sure that all the records consumed for this {topic,partition} prior to
   * calling unsubscribe here is produced to drainer service. {@link ConsumptionTask#run()} ends up calling
   * {@link SharedKafkaConsumer#poll(long)} and produceToStoreBufferService sequentially. So waiting for at least one more
   * invocation of {@link SharedKafkaConsumer#poll(long)} achieves the above objective.
   */
  public synchronized void unSubscribe(PubSubTopicPartition pubSubTopicPartition, long timeoutMs) {
    unSubscribeAction(() -> {
      this.delegate.unSubscribe(pubSubTopicPartition);
      PubSubTopic versionTopic = subscribedTopicPartitionToVersionTopic.remove(pubSubTopicPartition);
      unsubscriptionListener.call(this, versionTopic, pubSubTopicPartition);
      return Collections.singleton(pubSubTopicPartition);
    }, timeoutMs);
  }

  @Override
  public synchronized void batchUnsubscribe(Set<PubSubTopicPartition> pubSubTopicPartitionSet) {
    unSubscribeAction(() -> {
      this.delegate.batchUnsubscribe(pubSubTopicPartitionSet);
      for (PubSubTopicPartition pubSubTopicPartition: pubSubTopicPartitionSet) {
        PubSubTopic versionTopic = subscribedTopicPartitionToVersionTopic.remove(pubSubTopicPartition);
        unsubscriptionListener.call(this, versionTopic, pubSubTopicPartition);
      }
      return pubSubTopicPartitionSet;
    }, DEFAULT_MAX_WAIT_MS);
  }

  /**
   * This function encapsulates the logging, bookkeeping and required waiting period surrounding the action of
   * unsubscribing some partition(s).
   *
   * @param supplier which performs the unsubscription and returns a set of partitions which were unsubscribed
   */
  protected synchronized void unSubscribeAction(Supplier<Set<PubSubTopicPartition>> supplier, long timeoutMs) {
    long currentPollTimes = pollTimes;
    long startTime = System.currentTimeMillis();
    Set<PubSubTopicPartition> topicPartitions = supplier.get();
    updateCurrentAssignment(delegate.getAssignment());
    waitAfterUnsubscribe(currentPollTimes, topicPartitions, timeoutMs);
    long elapsedTime = System.currentTimeMillis() - startTime;
    LOGGER.info(
        "Shared consumer {} unsubscribed {} partition(s): ({}) in {} ms",
        this.getClass().getSimpleName(),
        topicPartitions.size(),
        topicPartitions,
        elapsedTime);
  }

  protected void waitAfterUnsubscribe(
      long currentPollTimes,
      Set<PubSubTopicPartition> topicPartitions,
      long timeoutMs) {
    // This clause is only for unit test purposes, for when the timeout needs to be set to 0.
    if (timeoutMsOverride != -1) {
      timeoutMs = timeoutMsOverride;
    }

    currentPollTimes++;
    waitingForPoll.set(true);
    // Wait for the next poll or maximum 10 seconds. Interestingly wait api does not provide any indication if wait
    // returned
    // due to timeout. So an explicit time check is necessary.
    final long startTimeMs = time.getMilliseconds();
    final long endTimeMs = startTimeMs + timeoutMs;
    try {
      while (currentPollTimes > pollTimes) {
        final long waitMs = endTimeMs - time.getMilliseconds();
        if (waitMs <= 0) {
          LOGGER.warn(
              "Wait for poll request after unsubscribe topic partition(s) ({}) timed out after {} seconds",
              topicPartitions,
              TimeUnit.MILLISECONDS.toSeconds(timeoutMs));
          break;
        }
        wait(waitMs);
      }
      final long elapsedSeconds = TimeUnit.MILLISECONDS.toSeconds(getElapsedTimeFromMsToMs(startTimeMs));
      if (elapsedSeconds > 15) {
        LOGGER.warn(
            "Wait for poll request after unsubscribe topic partition(s) ({}) took {} seconds",
            topicPartitions,
            elapsedSeconds);
      }
      // no action to take actually, just return;
    } catch (InterruptedException e) {
      LOGGER.info("Wait for poll request in `unsubscribe` function got interrupted.");
      Thread.currentThread().interrupt();
    }
  }

  // Only for testing.
  void setTimeoutMsOverride(long timeoutMsOverride) {
    this.timeoutMsOverride = timeoutMsOverride;
  }

  // Only for testing.
  long getPollTimes() {
    return pollTimes;
  }

  @Override
  public synchronized void resetOffset(PubSubTopicPartition pubSubTopicPartition)
      throws PubSubUnsubscribedTopicPartitionException {
    this.delegate.resetOffset(pubSubTopicPartition);
  }

  @Override
  public synchronized void close() {
    this.delegate.close();
    updateCurrentAssignment(Collections.emptySet());
  }

  @Override
  public synchronized Map<PubSubTopicPartition, List<DefaultPubSubMessage>> poll(long timeoutMs) {
    /**
     * Always invoke this method no matter whether the consumer have subscription or not. Therefore we could notify any
     * waiter who might be waiting for a invocation of poll to happen even if the consumer does not have subscription
     * after calling {@link SharedKafkaConsumer#unSubscribe(PubSubTopicPartition)}.
     */
    pollTimes++;
    if (waitingForPoll.get()) {
      waitingForPoll.set(false);
      notifyAll();
    }

    /**
     * If the consumer does not have subscription, sleep the specified timeout and return.
     */
    try {
      if (!hasAnySubscription()) {
        // TODO: removing this sleep inside the poll with synchronization, this sleep should be added by the logic
        // calling this poll method.
        Thread.sleep(timeoutMs);
        return Collections.emptyMap();
      }
    } catch (InterruptedException e) {
      throw new VeniceException("Shared Consumer poll sleep got interrupted", e);
    }

    return this.delegate.poll(timeoutMs);
  }

  @Override
  public boolean hasAnySubscription() {
    return !this.currentAssignment.isEmpty();
  }

  @Override
  public boolean hasSubscription(PubSubTopicPartition pubSubTopicPartition) {
    return currentAssignment.contains(pubSubTopicPartition);
  }

  @Override
  public synchronized void pause(PubSubTopicPartition pubSubTopicPartition) {
    this.delegate.pause(pubSubTopicPartition);
  }

  @Override
  public synchronized void resume(PubSubTopicPartition pubSubTopicPartition) {
    this.delegate.resume(pubSubTopicPartition);
  }

  @Override
  public synchronized Set<PubSubTopicPartition> getAssignment() {
    return currentAssignment; // The assignment set is unmodifiable
  }

  public int getAssignmentSize() {
    return currentAssignmentSize.get();
  }

  // Visible for testing
  synchronized void setCurrentAssignment(Set<PubSubTopicPartition> assignment) {
    this.currentAssignment = assignment;
    this.currentAssignmentSize.set(assignment.size());
  }

  @Override
  public long getOffsetLag(PubSubTopicPartition pubSubTopicPartition) {
    return delegate.getOffsetLag(pubSubTopicPartition);
  }

  @Override
  public long getLatestOffset(PubSubTopicPartition pubSubTopicPartition) {
    return delegate.getLatestOffset(pubSubTopicPartition);
  }

  @Override
  public Long offsetForTime(PubSubTopicPartition pubSubTopicPartition, long timestamp, Duration timeout) {
    throw new UnsupportedOperationException("offsetForTime is not supported in SharedKafkaConsumer");
  }

  @Override
  public PubSubPosition getPositionByTimestamp(
      PubSubTopicPartition pubSubTopicPartition,
      long timestamp,
      Duration timeout) {
    throw new UnsupportedOperationException("getPositionByTimestamp is not supported in SharedKafkaConsumer");
  }

  @Override
  public Long offsetForTime(PubSubTopicPartition pubSubTopicPartition, long timestamp) {
    throw new UnsupportedOperationException("offsetForTime is not supported in SharedKafkaConsumer");
  }

  @Override
  public PubSubPosition getPositionByTimestamp(PubSubTopicPartition pubSubTopicPartition, long timestamp) {
    throw new UnsupportedOperationException("getPositionByTimestamp is not supported in SharedKafkaConsumer");
  }

  @Override
  public PubSubPosition beginningPosition(PubSubTopicPartition pubSubTopicPartition, Duration timeout) {
    throw new UnsupportedOperationException("beginningPosition is not supported in SharedKafkaConsumer");
  }

  @Override
  public Map<PubSubTopicPartition, PubSubPosition> beginningPositions(
      Collection<PubSubTopicPartition> partitions,
      Duration timeout) {
    throw new UnsupportedOperationException("beginningPositions is not supported in SharedKafkaConsumer");
  }

  @Override
  public Map<PubSubTopicPartition, Long> endOffsets(Collection<PubSubTopicPartition> partitions, Duration timeout) {
    throw new UnsupportedOperationException("endOffsets is not supported in SharedKafkaConsumer");
  }

  @Override
  public Map<PubSubTopicPartition, PubSubPosition> endPositions(
      Collection<PubSubTopicPartition> partitions,
      Duration timeout) {
    throw new UnsupportedOperationException("endPositions is not supported in SharedKafkaConsumer");
  }

  @Override
  public Long endOffset(PubSubTopicPartition pubSubTopicPartition) {
    throw new UnsupportedOperationException("endOffset is not supported in SharedKafkaConsumer");
  }

  @Override
  public PubSubPosition endPosition(PubSubTopicPartition pubSubTopicPartition) {
    throw new UnsupportedOperationException("endPosition is not supported in SharedKafkaConsumer");
  }

  @Override
  public List<PubSubTopicPartitionInfo> partitionsFor(PubSubTopic topic) {
    throw new UnsupportedOperationException("partitionsFor is not supported in SharedKafkaConsumer");
  }

  @Override
  public synchronized long comparePositions(
      PubSubTopicPartition partition,
      PubSubPosition position1,
      PubSubPosition position2) {
    return delegate.comparePositions(partition, position1, position2);
  }

  @Override
  public synchronized long positionDifference(
      PubSubTopicPartition partition,
      PubSubPosition position1,
      PubSubPosition position2) {
    return delegate.positionDifference(partition, position1, position2);
  }

  @Override
  public synchronized PubSubPosition decodePosition(
      PubSubTopicPartition partition,
      int positionTypeId,
      ByteBuffer buffer) {
    return delegate.decodePosition(partition, positionTypeId, buffer);
  }
}
