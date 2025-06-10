package com.linkedin.venice.spark.input.pubsub.raw;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.PubSubUtil;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.spark.input.pubsub.PubSubMessageProcessor;
import com.linkedin.venice.utils.VeniceProperties;
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


public class VeniceBasicPubsubInputPartitionReader implements PartitionReader<InternalRow> {
  private static final int CONSUMER_POLL_EMPTY_RESULT_RETRY_TIMES = 12;
  private static final long EMPTY_POLL_SLEEP_TIME_MS = TimeUnit.SECONDS.toMillis(5);
  private static final Long CONSUMER_POLL_TIMEOUT = TimeUnit.SECONDS.toMillis(1); // 1 second

  private static final Logger LOGGER = LogManager.getLogger(VeniceBasicPubsubInputPartitionReader.class);

  private final boolean filterControlMessages = true;
  private final PubSubTopic targetPubSubTopic;
  private final PubSubConsumerAdapter pubSubConsumer;
  private final ArrayDeque<PubSubMessage<KafkaKey, KafkaMessageEnvelope, PubSubPosition>> messageBuffer =
      new ArrayDeque<>();
  private final PubSubPosition endingPosition;
  private final long endingOffset;
  private final PubSubPosition startingPosition;
  private final long startingOffset;
  private final long offsetLength;
  private final int targetPartitionNumber;
  private final String topicName;
  private final PubSubTopic pubSubTopic;
  private final String region;
  // this is the buffer that holds the messages that are consumed from the pubsub
  private PubSubTopicPartition targetPubSubTopicPartition = null;
  private PubSubPosition currentPosition;
  private InternalRow currentRow = null;
  private long recordsServed = 0;
  private long recordsSkipped = 0;
  private long recordsDeliveredByGet = 0;
  private final float lastKnownProgressPercent = 0;

  // the buffer that holds the relevant messages for the current partition

  public VeniceBasicPubsubInputPartitionReader(
      VeniceProperties jobConfig,
      VeniceBasicPubsubInputPartition inputPartition) {
    this(
        jobConfig,
        inputPartition,
        PubSubClientsFactory.createConsumerFactory(jobConfig)
            .create(
                new PubSubConsumerAdapterContext.Builder().setVeniceProperties(jobConfig)
                    .setPubSubTopicRepository(new PubSubTopicRepository())
                    .setPubSubMessageDeserializer(PubSubMessageDeserializer.createOptimizedDeserializer())
                    .setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry.fromPropertiesOrDefault(jobConfig))
                    .setConsumerName(
                        "raw_kif_" + inputPartition.getTopicName() + "_" + inputPartition.getPartitionNumber())
                    .build()), // this is hideous
        new PubSubTopicRepository());
  }

  // testing constructor
  public VeniceBasicPubsubInputPartitionReader(
      VeniceProperties jobConfig,
      VeniceBasicPubsubInputPartition inputPartition,
      PubSubConsumerAdapter consumer,
      PubSubTopicRepository pubSubTopicRepository) {

    this.pubSubConsumer = consumer;
    this.topicName = inputPartition.getTopicName();
    this.targetPartitionNumber = inputPartition.getPartitionNumber();
    this.region = inputPartition.getRegion();

    // Get topic reference
    try {
      this.pubSubTopic = pubSubTopicRepository.getTopic(topicName);
      this.targetPubSubTopic = this.pubSubTopic; // Avoid duplicate lookup
    } catch (Exception e) {
      throw new RuntimeException("Failed to get topic: " + topicName, e);
    }

    // Find the target partition
    this.targetPubSubTopicPartition = pubSubConsumer.partitionsFor(pubSubTopic)
        .stream()
        .map(PubSubTopicPartitionInfo::getTopicPartition)
        .filter(partition -> partition.getPartitionNumber() == targetPartitionNumber)
        .findFirst()
        .orElseThrow(
            () -> new RuntimeException(
                "Partition not found for topic: " + topicName + " partition number: " + targetPartitionNumber));

    // Set up offset positions
    this.startingOffset = inputPartition.getStartOffset();
    this.endingOffset = inputPartition.getEndOffset();
    this.startingPosition = ApacheKafkaOffsetPosition.of(startingOffset);
    this.endingPosition = ApacheKafkaOffsetPosition.of(endingOffset);
    this.offsetLength = endingOffset - startingOffset;
    this.currentPosition = startingPosition;

    // Subscribe to the topic partition
    pubSubConsumer.subscribe(targetPubSubTopicPartition, startingPosition);
    LOGGER.info("Subscribed to Topic: {} Partition {}.", topicName, targetPartitionNumber);

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
    recordsDeliveredByGet++;
    // should return the same row if called multiple times
    return currentRow;
  }

  private boolean isThereMoreToConsume() {
    return PubSubUtil.comparePubSubPositions(currentPosition, endingPosition) < 0;
  }

  @Override
  public boolean next() {
    if (isThereMoreToConsume()) {
      return false;
    }

    if (prepareNextValidRow()) {
      // there is a fresh row to serve
      return true;
    }
    // at this point, buffer is empty.

    pollAndFillMessageBufferWithNewPubsubRecords(); // try to poll for some records and allow the exception to bubble up
    return prepareNextValidRow();
  }

  @Override
  public void close() {
    pubSubConsumer.close();
    // double progressPercent = (currentPosition.getNumericOffset() - startingOffset) * 100.0 / offsetLength;
    LOGGER.info(
        "Consumer closed for Topic: {} , partition: {}, consumed {} records,",
        topicName,
        targetPartitionNumber,
        recordsServed);
    LOGGER.info("Skipped {} records, delivered rows {} times .", recordsSkipped, recordsDeliveredByGet);
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
      consumerBuffer = pubSubConsumer.poll(CONSUMER_POLL_TIMEOUT);
      List<DefaultPubSubMessage> partitionMessagesBuffer = consumerBuffer.get(targetPubSubTopicPartition);

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
            topicName,
            targetPartitionNumber,
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
            topicName,
            targetPartitionNumber));
  }

  private float getProgressPercent() {
    return (float) ((currentPosition.getNumericOffset() - startingOffset) * 100.0 / offsetLength);
  }

  private void maybeLogProgress() {
    float progressPercent = getProgressPercent();
    if (progressPercent - lastKnownProgressPercent >= 10.0) {
      logProgress(progressPercent);
    }
  }

  private void logProgress(float progressPercent) {
    LOGGER.info(
        "Consuming progress for"
            + " Topic: {}, partition {} , consumed {}% of {} records. actual records delivered: {}, records skipped: {}",
        targetPubSubTopic,
        targetPartitionNumber,
        String.format("%.1f", progressPercent),
        offsetLength,
        recordsServed,
        recordsSkipped);
  }

  public List<Long> getStats() {
    return Arrays.asList(recordsServed, recordsSkipped, recordsDeliveredByGet);
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

    // Iterate through messages in the buffer until finding a non-control message or exhausting the buffer
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, PubSubPosition> message;
    while (!messageBuffer.isEmpty()) {
      try {
        message = messageBuffer.pop();
      } catch (NoSuchElementException e) {
        // This shouldn't happen since we check isEmpty() in the loop condition
        // but keeping as a safeguard
        return false;
      }

      currentPosition = message.getOffset(); // needs rework when the pubSub position is fully adopted.

      // Skip control messages if filtering is enabled
      if (filterControlMessages && message.getKey().isControlMessage()) {
        recordsSkipped++;
        continue; // Continue to next message in buffer
      }

      // Found a valid message - process it
      currentRow = PubSubMessageProcessor.convertPubSubMessageToRow(message, region, targetPartitionNumber);

      recordsServed++;
      maybeLogProgress();
      return true;
    }

    // No usable messages found in the buffer
    return false;
  }
}
