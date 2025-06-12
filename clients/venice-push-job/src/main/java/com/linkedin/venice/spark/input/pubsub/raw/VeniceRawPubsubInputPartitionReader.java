package com.linkedin.venice.spark.input.pubsub.raw;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubUtil;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.spark.input.pubsub.ConvertPubSubMessageToRow;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;


/**
 * A Spark SQL data source partition reader implementation for Venice PubSub messages.
 * <p>
 * This reader consumes messages from a specific partition of a PubSub topic between
 * specified start and end offsets, converting them into Spark's {@link InternalRow} format.
 * The reader provides functionality for:
 * <ul>
 *   <li>Reading messages from a specific topic partition</li>
 *   <li>Filtering control messages when configured</li>
 *   <li>Tracking consumption progress</li>
 * </ul>
 * <p>
 * This class is part of the Venice Spark connector enabling ETL and KIF functionality.
 */
public class VeniceRawPubsubInputPartitionReader implements PartitionReader<InternalRow> {
  private static final int CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES = 12;
  private static final long EMPTY_POLL_SLEEP_TIME_MS = TimeUnit.SECONDS.toMillis(5);
  private static final Long CONSUMER_POLL_TIMEOUT = TimeUnit.SECONDS.toMillis(1); // 1 second

  private static final Logger LOGGER = LogManager.getLogger(VeniceRawPubsubInputPartitionReader.class);

  private final boolean filterControlMessages = true;
  private final PubSubConsumerAdapter pubSubConsumer;
  private final ArrayDeque<PubSubMessage<KafkaKey, KafkaMessageEnvelope, PubSubPosition>> messageBuffer =
      new ArrayDeque<>();

  private final PubSubTopic pubSubTopic;
  private final PubSubTopicPartition targetPubSubTopicPartition;
  private final String topicName;
  private final String region;
  private final int targetPartitionNumber;

  private final PubSubPosition startingPosition;
  private final long startingOffset;
  private final PubSubPosition endingPosition;
  private final long endingOffset;
  private final long offsetLength;
  private final float lastKnownProgressPercent = 0;
  private PubSubPosition currentPosition;
  private InternalRow currentRow = null;
  private long recordsServed = 0;
  private long recordsSkipped = 0;
  private long recordsDeliveredByGet = 0;

  // the buffer that holds the relevant messages for the current partition

  public VeniceRawPubsubInputPartitionReader(
      VeniceBasicPubsubInputPartition inputPartition,
      PubSubConsumerAdapter consumer,
      PubSubTopic pubSubTopic,
      PubSubTopicPartition topicPartition) {

    this.pubSubConsumer = consumer;
    this.pubSubTopic = pubSubTopic;
    this.topicName = inputPartition.getTopicName();
    this.targetPartitionNumber = inputPartition.getPartitionNumber();
    this.region = inputPartition.getRegion();

    this.targetPubSubTopicPartition = topicPartition;

    // Set up offset positions
    this.startingOffset = inputPartition.getStartOffset();
    this.endingOffset = inputPartition.getEndOffset();
    this.startingPosition = ApacheKafkaOffsetPosition.of(startingOffset);
    this.endingPosition = ApacheKafkaOffsetPosition.of(endingOffset);
    this.offsetLength = endingOffset - startingOffset;
    this.currentPosition = startingPosition;

    // Subscribe to the topic partition
    pubSubConsumer.subscribe(targetPubSubTopicPartition, startingPosition);
    LOGGER.info("Subscribed to Topic: {} Partition {}.", this.topicName, this.targetPartitionNumber);

    initialize();
  }

  private void initialize() {
    next();// get the first record ready to go.
    recordsServed = 0; // reset the counter
  }

  /**
   *  Assuming that Current row has meaningful data, this method will return that and counts the number of invocations.
   * @return The current row as an {@link InternalRow}.
   */
  @Override
  public InternalRow get() {
    this.recordsDeliveredByGet++;
    // should return the same row if called multiple times
    return this.currentRow;
  }

  private boolean areWePastTheEndingPosition() {
    return PubSubUtil.comparePubSubPositions(this.currentPosition, this.endingPosition) > 0;
  }

  @Override
  public boolean next() {

    if (areWePastTheEndingPosition()) {
      return false;
    }

    if (prepareNextValidRow()) {
      // local buffer has a valid row, no need to poll
      return true;
    }

    // at this point, buffer is empty, need to poll from pubsub for new messages
    pollAndFillMessageBufferWithNewPubsubRecords();
    return prepareNextValidRow();
  }

  @Override
  public void close() {
    pubSubConsumer.close();
    // double progressPercent = (currentPosition.getNumericOffset() - startingOffset) * 100.0 / offsetLength;
    LOGGER.info(
        "Consumer closed for Topic: {} , partition: {}, consumed {} records,",
        this.topicName,
        this.targetPartitionNumber,
        this.recordsServed);
    LOGGER.info("Skipped {} records, delivered rows {} times .", this.recordsSkipped, this.recordsDeliveredByGet);
  }

  /**
   * This method polls the PubSub consumer for new messages and fills the message buffer.
   * It retries a few times if the poll returns empty results, waiting between retries.
   * If no messages are received after all retries, it throws an exception.
   */
  private void pollAndFillMessageBufferWithNewPubsubRecords() {
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> consumerBuffer;
    int retries = 0;

    while (retries < CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES) {
      consumerBuffer = this.pubSubConsumer.poll(CONSUMER_POLL_TIMEOUT);
      List<DefaultPubSubMessage> partitionMessagesBuffer = consumerBuffer.get(this.targetPubSubTopicPartition);

      if (partitionMessagesBuffer != null && !partitionMessagesBuffer.isEmpty()) {
        messageBuffer.addAll(partitionMessagesBuffer);
        return; // Successfully got messages, we're done
      }

      try {
        Thread.sleep(EMPTY_POLL_SLEEP_TIME_MS);
      } catch (InterruptedException e) {
        logProgress(getProgressPercent());
        LOGGER.error(
            "Interrupted while waiting for records from topic {} partition {}",
            this.topicName,
            this.targetPartitionNumber,
            e);
        // should we re-throw here to break the consumption task ?
        // Thread.currentThread().interrupt(); // very questionable genAI suggestion,
        // what's the intended way to terminate this task?
        return; // Exit on interruption
      }
      retries++;
    }

    // Exhausted all retries without getting messages
    throw new RuntimeException(
        String.format(
            "Empty poll after %d retries for topic: %s partition: %d. No messages were consumed.",
            CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES,
            this.topicName,
            this.targetPartitionNumber));
  }

  private float getProgressPercent() {
    return (float) ((this.currentPosition.getNumericOffset() - this.startingOffset) * 100.0 / this.offsetLength);
  }

  private void maybeLogProgress() {
    float progressPercent = getProgressPercent();
    if (progressPercent - this.lastKnownProgressPercent >= 10.0) {
      logProgress(progressPercent);
    }
  }

  private void logProgress(float progressPercent) {
    LOGGER.info(
        "Consuming progress for"
            + " Topic: {}, partition {} , consumed {}% of {} records. actual records delivered: {}, records skipped: {}",
        this.topicName,
        this.targetPartitionNumber,
        String.format("%.1f", progressPercent),
        this.offsetLength,
        this.recordsServed,
        this.recordsSkipped);
  }

  public List<Long> getStats() {
    return Arrays.asList(this.recordsServed, this.recordsSkipped, this.recordsDeliveredByGet);
  }

  /**
   * This method is used to prepare the next valid row for consumption.
   * It checks the message buffer for the next usable message, skipping control messages if filtering is enabled.
   * If a valid message is found, it processes it into an InternalRow and updates the current position.
   * If no valid messages are found, it returns false.
   * @return {@code true} if a valid message was found and processed into a row
   */
  private boolean prepareNextValidRow() {
    // Return early if buffer is empty
    if (this.messageBuffer.isEmpty()) {
      return false;
    }

    // Iterate through messages in the buffer to find first non-control message or exhaust the buffer
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, PubSubPosition> message;
    while (!this.messageBuffer.isEmpty()) {
      try {
        message = this.messageBuffer.pop();
      } catch (NoSuchElementException e) {
        // This shouldn't happen since we check isEmpty() in the loop condition
        // but keeping it to appease the compiler.
        return false;
      }

      this.currentPosition = message.getOffset(); // needs rework when the pubSub position is fully adopted.

      // Skip control messages if filtering is enabled
      if (this.filterControlMessages && message.getKey().isControlMessage()) {
        this.recordsSkipped++;
        continue; // Continue to next message in buffer
      }

      // Found a valid message - process it
      this.currentRow =
          ConvertPubSubMessageToRow.convertPubSubMessageToRow(message, this.region, this.targetPartitionNumber);

      recordsServed++;
      maybeLogProgress();
      return true;
    }

    // No usable messages found in the buffer
    return false;
  }
}
