package com.linkedin.venice.vpj.pubsub.input;

import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.Utils;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * {@code PubSubSplitIterator} provides an iterator-like abstraction over a bounded range
 * of messages from a {@link PubSubConsumerAdapter}. It is designed for Venice Push Job
 * (VPJ) repush use cases where a split of a topic-partition needs
 * to be consumed deterministically between a start and an end position.
 *
 * <p>This class:
 * <ul>
 *   <li>Subscribes a {@link PubSubConsumerAdapter} to a specific {@link PubSubTopicPartition}.</li>
 *   <li>Starts reading from a caller-specified {@link PubSubPosition} (inclusive) and stops
 *       when reaching the end-exclusive position or the record count limit.</li>
 *   <li>Supports both numeric offsets and logical index–based offsets for progress tracking.</li>
 *   <li>Skips control messages transparently while tracking statistics about consumption.</li>
 * </ul>
 */
public class PubSubSplitIterator implements AutoCloseable {
  private static final Logger LOGGER = LogManager.getLogger(PubSubSplitIterator.class);

  public enum OffsetMode {
    NUMERIC, LOGICAL_INDEX
  }

  public static final long DEFAULT_POLL_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(1);
  public static final int DEFAULT_EMPTY_RESULT_RETRIES = 12;
  public static final long DEFAULT_EMPTY_SLEEP_MS = TimeUnit.SECONDS.toMillis(5);

  private final PubSubConsumerAdapter pubSubConsumer;
  private final PubSubTopicPartition topicPartition;
  private final PubSubPosition startPosition;
  private final PubSubPosition endPositionExclusive;
  private final long targetCount;
  private final long splitStartIndex;
  private final OffsetMode mode;
  private Iterator<DefaultPubSubMessage> consumedRecordsIterator;
  private PubSubPosition currentPosition;
  private boolean closed;

  // Total messages consumed from the broker, including control messages.
  private long readSoFar = 0;
  // Control messages encountered and intentionally skipped.
  private long controlMessagesSkipped = 0;
  // Data records actually delivered to the caller via {@link #next()}.
  private long recordsDelivered = 0;

  // progress logging throttle
  private float lastLoggedProgress = -1.0f;

  private final long pollTimeoutMs;
  private final int emptyRetryTimes;
  private final long emptySleepMs;

  public PubSubSplitIterator(
      PubSubConsumerAdapter pubSubConsumer,
      PubSubPartitionSplit split,
      boolean useLogicalIndexOffset,
      long pollTimeoutMs,
      int emptyRetryTimes,
      long emptySleepMs) {
    this.pubSubConsumer = pubSubConsumer;
    this.topicPartition = split.getPubSubTopicPartition();
    this.startPosition = split.getStartPubSubPosition();
    this.endPositionExclusive = split.getEndPubSubPosition();
    this.targetCount = split.getNumberOfRecords();
    this.splitStartIndex = split.getStartIndex();
    this.mode = useLogicalIndexOffset ? OffsetMode.LOGICAL_INDEX : OffsetMode.NUMERIC;
    this.pollTimeoutMs = pollTimeoutMs;
    this.emptyRetryTimes = emptyRetryTimes;
    this.emptySleepMs = emptySleepMs;
    start();
  }

  public PubSubSplitIterator(
      PubSubConsumerAdapter pubSubConsumer,
      PubSubPartitionSplit split,
      boolean useLogicalIndexOffset) {
    this(
        pubSubConsumer,
        split,
        useLogicalIndexOffset,
        DEFAULT_POLL_TIMEOUT_MS,
        DEFAULT_EMPTY_RESULT_RETRIES,
        DEFAULT_EMPTY_SLEEP_MS);
  }

  @VisibleForTesting
  final void start() {
    // defensive if caller reuses consumer
    pubSubConsumer.batchUnsubscribe(pubSubConsumer.getAssignment());
    pubSubConsumer.subscribe(topicPartition, startPosition, true);
  }

  public boolean hasNext() {
    if (readSoFar >= targetCount) {
      return false;
    }
    // If we haven't read anything yet, we still need to attempt the first poll
    if (currentPosition == null) {
      return true;
    }
    // Use difference against end-exclusive bound. Ending position is "last record + 1".
    return pubSubConsumer.positionDifference(topicPartition, endPositionExclusive, currentPosition) > 1;
  }

  private void loadRecords() throws InterruptedException {
    if (consumedRecordsIterator != null && consumedRecordsIterator.hasNext()) {
      return;
    }
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> polled = Collections.emptyMap();
    for (int i = 0; i < emptyRetryTimes; i++) {
      polled = pubSubConsumer.poll(pollTimeoutMs);
      if (!polled.isEmpty()) {
        break;
      }
      Thread.sleep(emptySleepMs);
    }
    if (polled.isEmpty()) {
      throw new VeniceException(
          "Empty poll after " + emptyRetryTimes + " retries, tp=" + topicPartition + " pos=" + currentPosition + " end="
              + endPositionExclusive + ". This may indicate no data available in the topic or partition.");
    }
    consumedRecordsIterator = Utils.iterateOnMapOfLists(polled);
  }

  public PubSubInputRecord next() throws IOException {
    while (hasNext()) {
      try {
        loadRecords();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new VeniceException("Interrupted while fetching next message batch", e);
      }

      DefaultPubSubMessage pubSubMessage = consumedRecordsIterator.hasNext() ? consumedRecordsIterator.next() : null;

      if (pubSubMessage == null) {
        throw new IOException(
            "Unable to read additional data. Partition " + topicPartition + " Current Position: " + currentPosition
                + " End Position: " + endPositionExclusive);
      }

      currentPosition = pubSubMessage.getPosition();
      long offset =
          (mode == OffsetMode.LOGICAL_INDEX) ? (splitStartIndex + readSoFar) : currentPosition.getNumericOffset();
      readSoFar += 1;

      KafkaKey messageKey = pubSubMessage.getKey();
      if (messageKey.isControlMessage()) {
        // Skip control messages and record the skip.
        controlMessagesSkipped += 1;
        continue;
      }

      // Deliver a data record and record the delivery.
      recordsDelivered += 1;
      // Return a PubSubInputRecord that knows how to set itself
      return new PubSubInputRecord(pubSubMessage, offset);
    }
    return null; // no more records
  }

  public float getProgress() {
    if (targetCount <= 0) {
      return 1.0f;
    }
    float progress = Math.min(1.0f, ((float) readSoFar / targetCount));

    // Throttled progress logging (every >=1% change or 100% completion)
    if (lastLoggedProgress < 0.0f || Math.abs(progress - lastLoggedProgress) >= 0.01f
        || (progress >= 1.0f && lastLoggedProgress < 1.0f)) {
      LOGGER.info(
          "PubSubSplitIterator progress for: {} read: {} / {}, delivered: {}, skipped: {}, progress: {}%",
          topicPartition,
          readSoFar,
          targetCount,
          recordsDelivered,
          controlMessagesSkipped,
          String.format("%.2f", progress * 100));
      lastLoggedProgress = progress;
    }
    return progress;
  }

  public long recordsRead() {
    return readSoFar;
  }

  public PubSubPosition getCurrentPosition() {
    return currentPosition;
  }

  public PubSubTopicPartition getTopicPartition() {
    return topicPartition;
  }

  @Override
  public void close() {
    if (!closed) {
      pubSubConsumer.close();
      closed = true;
      LOGGER.info(
          "PubSubSplitIterator closed for {}. read={}, delivered={}, skipped={}",
          topicPartition,
          readSoFar,
          recordsDelivered,
          controlMessagesSkipped);
    }
  }

  @VisibleForTesting
  boolean isClosed() {
    return closed;
  }

  @VisibleForTesting
  void setReadSoFar(long readSoFar) {
    this.readSoFar = readSoFar;
  }

  @VisibleForTesting
  void setCurrentPosition(PubSubPosition currentPosition) {
    this.currentPosition = currentPosition;
  }

  public static class PubSubInputRecord {
    DefaultPubSubMessage pubSubMessage;
    long offset;

    public PubSubInputRecord(DefaultPubSubMessage pubSubMessage, long offset) {
      this.pubSubMessage = pubSubMessage;
      this.offset = offset;
    }

    public DefaultPubSubMessage getPubSubMessage() {
      return pubSubMessage;
    }

    public long getOffset() {
      return offset;
    }
  }
}
