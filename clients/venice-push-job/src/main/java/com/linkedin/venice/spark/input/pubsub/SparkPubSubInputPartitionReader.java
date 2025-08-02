package com.linkedin.venice.spark.input.pubsub;

import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.spark.input.pubsub.raw.VeniceRawPubsubStats;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import java.util.ArrayDeque;
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
 *
 * TODO: Refactor this class to remove duplication with
 * {@link com.linkedin.venice.hadoop.input.kafka.KafkaInputRecordReader}.
 * Introduce an abstraction for the record-fetching logic and allow passing
 * a converter for the corresponding engine type.
 */
public class SparkPubSubInputPartitionReader implements PartitionReader<InternalRow> {
  private static final int CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES = 12;
  private static final long EMPTY_POLL_SLEEP_TIME_MS = TimeUnit.SECONDS.toMillis(5);
  private static final Long CONSUMER_POLL_TIMEOUT = TimeUnit.SECONDS.toMillis(1); // 1 second
  private static final float REPORT_PROGRESS_STEP_PERCENT = 10.0f; // Report progress every 10%

  private static final Logger LOGGER = LogManager.getLogger(SparkPubSubInputPartitionReader.class);
  final VeniceRawPubsubStats readerStats = new VeniceRawPubsubStats();
  private final boolean filterControlMessages = true;
  private final PubSubConsumerAdapter pubSubConsumer;
  private final ArrayDeque<DefaultPubSubMessage> messageBuffer = new ArrayDeque<>();
  private final PubSubMessageConverter pubSubMessageConverter;
  private final PubSubTopicPartition topicPartition;
  private final String region;
  private final int targetPartitionNumber;
  private final PubSubPosition startingPosition;
  private final PubSubPosition endingPosition;

  // Added for testing purposes
  private final long consumerPollTimeout;
  private final int consumerPollEmptyResultRetryTimes;
  private final long emptyPollSleepTimeMs;
  private float lastLoggedProgress = -1.0f; // Track last logged progress to avoid excessive logging
  private PubSubPosition currentPosition;
  private InternalRow currentRow = null;

  private final long totalRecordsToRead;
  private final int splitIndex;
  private final long splitStartingRecordIndex;
  private final boolean shouldUseLocallyBuiltIndexAsOffset;
  private long recordsReadSoFar = 0;

  // the buffer that holds the relevant messages for the current partition

  public SparkPubSubInputPartitionReader(
      SparkPubSubInputPartition inputPartition,
      PubSubConsumerAdapter consumer,
      PubSubTopicPartition topicPartition,
      String regionName,
      boolean shouldUseLocallyBuiltIndexAsOffset) {
    this(
        inputPartition,
        consumer,
        topicPartition,
        regionName,
        shouldUseLocallyBuiltIndexAsOffset,
        CONSUMER_POLL_TIMEOUT,
        CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES,
        EMPTY_POLL_SLEEP_TIME_MS,
        new VenicePubSubMessageToRow());
  }

  /**
   * Constructor for testing with custom timeout and retry values.
   *
   * @param inputPartition The input partition to read from
   * @param consumer The PubSub consumer adapter
   * @param topicPartition The topic partition
   * @param pollTimeoutMs The timeout in milliseconds for each poll operation
   * @param pollRetryTimes The number of retry attempts when polling returns empty results
   * @param emptyPollSleepTimeMs The sleep time in milliseconds between retries when polling returns empty results
   * @param pubSubMessageConverter The converter to use for converting PubSub messages to Spark rows
   */
  public SparkPubSubInputPartitionReader(
      SparkPubSubInputPartition inputPartition,
      PubSubConsumerAdapter consumer,
      PubSubTopicPartition topicPartition,
      String regionName,
      boolean shouldUseLocallyBuiltIndexAsOffset,
      long pollTimeoutMs,
      int pollRetryTimes,
      long emptyPollSleepTimeMs,
      PubSubMessageConverter pubSubMessageConverter) {

    this.pubSubConsumer = consumer;
    PubSubPartitionSplit pubSubPartitionSplit = inputPartition.getPubSubPartitionSplit();
    this.targetPartitionNumber = pubSubPartitionSplit.getPartitionNumber();
    this.region = regionName;
    this.consumerPollTimeout = pollTimeoutMs;
    this.consumerPollEmptyResultRetryTimes = pollRetryTimes;
    this.emptyPollSleepTimeMs = emptyPollSleepTimeMs;
    this.pubSubMessageConverter = pubSubMessageConverter;
    this.topicPartition = topicPartition;

    // Set up offset positions
    this.startingPosition = pubSubPartitionSplit.getStartPubSubPosition();
    this.endingPosition = pubSubPartitionSplit.getEndPubSubPosition();
    this.totalRecordsToRead = pubSubPartitionSplit.getNumberOfRecords();
    this.splitIndex = pubSubPartitionSplit.getSplitIndex();
    this.splitStartingRecordIndex = pubSubPartitionSplit.getStartIndex();
    this.shouldUseLocallyBuiltIndexAsOffset = shouldUseLocallyBuiltIndexAsOffset;

    // Subscribe to the topic partition
    pubSubConsumer.subscribe(this.topicPartition, startingPosition);
    LOGGER.info(
        "Created SparkPubSubInputPartitionReader for split index: {}, to consume  topic-partition: {} from region: {} with starting position: {}",
        this.splitIndex,
        this.topicPartition,
        this.region,
        this.startingPosition);
  }

  public float getProgressPercent() {
    if (totalRecordsToRead <= 0) {
      return 1.0f;
    }
    float progress = Math.min(1.0f, ((float) recordsReadSoFar / totalRecordsToRead));

    // Only log progress if it has changed by at least 1% or this is the first time
    if (lastLoggedProgress < 0.0f || Math.abs(progress - lastLoggedProgress) >= 0.01f) {
      LOGGER.info(
          "SparkPubSubInputPartitionReader for TopicPartition: {} has read {} records out of total {} records, progress: {}%",
          this.topicPartition,
          recordsReadSoFar,
          totalRecordsToRead,
          String.format("%.2f", progress * 100));
      lastLoggedProgress = progress;
    }

    return progress * 100.0f; // Convert to percentage
  }

  /**
   *  Assuming that Current row has meaningful data, this method will return that and counts the number of invocations.
   * @return The current row as an {@link InternalRow}.
   */
  @Override
  public InternalRow get() {
    readerStats.incrementRecordsDeliveredByGet();
    // should return the same row if called multiple times
    return this.currentRow;
  }

  private boolean areWePastTheEndingPosition() {
    if (this.currentPosition == null) {
      // If current position is not set, this is the very first call to next(),
      logProgress(); // Log progress as 0%
      return false;
    }
    if (recordsReadSoFar > totalRecordsToRead) {
      return false;
    }
    // Use positionDifference instead of compare. If a topic partition has no more data,
    // currentPosition will never reach or exceed endingPosition. By definition,
    // endingPosition is set to "last record offset + 1".
    return pubSubConsumer.positionDifference(topicPartition, endingPosition, currentPosition) > 1;
  }

  @Override
  public boolean next() {

    // early exit if we are already past the ending position
    if (areWePastTheEndingPosition()) {
      return false;
    }

    if (prepareNextValidRow()) {
      // local buffer has a valid row, no need to poll
      return true;
    }

    // no more valid rows in the local buffer, we need to poll for new messages
    try {
      while (!this.areWePastTheEndingPosition()) { // poll all the way to the end of the topic partition
        pollAndFillMessageBufferWithNewPubsubRecords();
        while (!this.messageBuffer.isEmpty()) {
          if (prepareNextValidRow()) {
            // Successfully prepared a valid row
            return true;
          }
        }
      }
    } catch (RuntimeException e) {
      LOGGER.error(
          "Error while polling messages from topic-partition {}. error:  {}",
          this.topicPartition,
          e.getMessage());
      logProgress();
      // Now we need to experiment with Spark's behavior when the reader returns false.
      // we could throw exception
      // we could close()
      // we could return false , going with the last option for now.
      return false; // Return false if no valid messages could be prepared
    }

    // If we reach here, it means we have exhausted all messages and are past the ending position
    return false;
  }

  @Override
  public void close() {
    pubSubConsumer.close();
    LOGGER.info(
        "Consumer closed for topic-partition: {}, consumed {} records.",
        this.topicPartition,
        readerStats.getRecordsServed());
    LOGGER.info(
        "Skipped {} records, delivered rows {} times .",
        readerStats.getRecordsSkipped(),
        readerStats.getRecordsDeliveredByGet());
  }

  /**
   * This method polls the PubSub consumer for new messages and fills the message buffer.
   * It retries a few times if the poll returns empty results, waiting between retries.
   * If no messages are received after all retries, it throws an exception.
   */
  private void pollAndFillMessageBufferWithNewPubsubRecords() {
    Map<PubSubTopicPartition, List<DefaultPubSubMessage>> consumerBuffer;
    int retries = 0;

    while (retries < consumerPollEmptyResultRetryTimes) {
      consumerBuffer = this.pubSubConsumer.poll(this.consumerPollTimeout);
      List<DefaultPubSubMessage> partitionMessagesBuffer = consumerBuffer.get(this.topicPartition);

      if (partitionMessagesBuffer != null && !partitionMessagesBuffer.isEmpty()) {
        messageBuffer.addAll(partitionMessagesBuffer);
        return; // Successfully got messages, we're done
      }

      try {
        Thread.sleep(this.emptyPollSleepTimeMs);
      } catch (InterruptedException e) {
        logProgress();
        LOGGER.error("Interrupted while waiting for records from topic-partition {}", this.topicPartition, e);
        // rethrow the exception to exit the loop
        Thread.currentThread().interrupt();
        return; // Exit on interruption
      }
      retries++;
    }

    // Exhausted all retries without getting messages
    throw new RuntimeException(
        String.format(
            "Empty poll after %d retries for topic-partition: %s. No messages were consumed.",
            consumerPollEmptyResultRetryTimes,
            this.topicPartition));
  }

  private void maybeLogProgress() {
    float progressPercent = this.getProgressPercent();
    if (progressPercent - this.lastLoggedProgress >= REPORT_PROGRESS_STEP_PERCENT) {
      logProgress();
      this.lastLoggedProgress = progressPercent;
    }
  }

  private void logProgress() {
    // handle null currentOffset gracefully in the calculate method
    LOGGER.info(
        "Progress for topic-partition {} , consumed {}% of {} records. records delivered: {}, skipped: {}",
        this.topicPartition,
        String.format("%.1f", this.getProgressPercent()),
        this.totalRecordsToRead,
        readerStats.getRecordsServed(),
        readerStats.getRecordsSkipped());
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
    DefaultPubSubMessage message;
    while (!this.messageBuffer.isEmpty()) {
      try {
        message = this.messageBuffer.pop();
      } catch (NoSuchElementException e) {
        // This shouldn't happen since we check isEmpty() in the loop condition
        // but keeping it to appease the compiler.
        return false;
      }
      this.currentPosition = message.getPosition();
      long offset;
      if (shouldUseLocallyBuiltIndexAsOffset) {
        offset = splitStartingRecordIndex + recordsReadSoFar;
      } else {
        offset = message.getPosition().getNumericOffset();
      }
      recordsReadSoFar += 1;
      // Skip control messages if filtering is enabled
      if (this.filterControlMessages && message.getKey().isControlMessage()) {
        readerStats.incrementRecordsSkipped();
        continue; // Continue to next message in buffer
      }

      this.currentRow = this.pubSubMessageConverter.convert(message, this.region, this.targetPartitionNumber, offset);

      readerStats.incrementRecordsServed();
      maybeLogProgress();
      return true;
    }

    // No usable messages found in the buffer
    return false;
  }
}
