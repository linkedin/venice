package com.linkedin.venice.kafka.partitionoffset;

import static com.linkedin.venice.offsets.OffsetRecord.LOWEST_OFFSET;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.kafka.VeniceOperationAgainstKafkaTimedOut;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.api.PubSubAdminAdapter;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongMaps;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class PartitionOffsetFetcherImpl implements PartitionOffsetFetcher {
  private static final List<Class<? extends Throwable>> KAFKA_RETRIABLE_FAILURES =
      Collections.singletonList(org.apache.kafka.common.errors.RetriableException.class);
  public static final Duration DEFAULT_KAFKA_OFFSET_API_TIMEOUT = Duration.ofMinutes(1);
  public static final long NO_PRODUCER_TIME_IN_EMPTY_TOPIC_PARTITION = -1;
  private static final int KAFKA_POLLING_RETRY_MAX_ATTEMPT = 3;

  private final Logger logger;
  private final Lock adminConsumerLock;
  private final Lazy<PubSubAdminAdapter> kafkaAdminWrapper;
  private final Lazy<PubSubConsumerAdapter> pubSubConsumer;
  private final Duration kafkaOperationTimeout;

  public PartitionOffsetFetcherImpl(
      @Nonnull Lazy<PubSubAdminAdapter> kafkaAdminWrapper,
      @Nonnull Lazy<PubSubConsumerAdapter> pubSubConsumer,
      long kafkaOperationTimeoutMs,
      String kafkaBootstrapServers) {
    Validate.notNull(kafkaAdminWrapper);
    this.kafkaAdminWrapper = kafkaAdminWrapper;
    this.pubSubConsumer = pubSubConsumer;
    this.adminConsumerLock = new ReentrantLock();
    this.kafkaOperationTimeout = Duration.ofMillis(kafkaOperationTimeoutMs);
    this.logger =
        LogManager.getLogger(PartitionOffsetFetcherImpl.class.getSimpleName() + " [" + kafkaBootstrapServers + "]");
  }

  @Override
  public Int2LongMap getTopicLatestOffsets(PubSubTopic topic) {
    try (AutoCloseableLock ignore = AutoCloseableLock.of(adminConsumerLock)) {
      List<PubSubTopicPartitionInfo> partitionInfoList = pubSubConsumer.get().partitionsFor(topic);
      if (partitionInfoList == null || partitionInfoList.isEmpty()) {
        logger.warn("Unexpected! Topic: {} has a null partition set, returning empty map for latest offsets", topic);
        return Int2LongMaps.EMPTY_MAP;
      }
      List<PubSubTopicPartition> topicPartitions = partitionInfoList.stream()
          .map(partitionInfo -> new PubSubTopicPartitionImpl(topic, partitionInfo.partition()))
          .collect(Collectors.toList());

      Map<PubSubTopicPartition, Long> offsetsByTopicPartitions =
          pubSubConsumer.get().endOffsets(topicPartitions, DEFAULT_KAFKA_OFFSET_API_TIMEOUT);
      Int2LongMap offsetsByTopicPartitionIds = new Int2LongOpenHashMap(offsetsByTopicPartitions.size());
      for (Map.Entry<PubSubTopicPartition, Long> offsetByTopicPartition: offsetsByTopicPartitions.entrySet()) {
        offsetsByTopicPartitionIds
            .put(offsetByTopicPartition.getKey().getPartitionNumber(), offsetByTopicPartition.getValue().longValue());
      }
      return offsetsByTopicPartitionIds;
    }
  }

  private long getLatestOffset(PubSubTopicPartition pubSubTopicPartition) throws TopicDoesNotExistException {
    if (pubSubTopicPartition.getPartitionNumber() < 0) {
      throw new IllegalArgumentException(
          "Cannot retrieve latest offsets for invalid partition " + pubSubTopicPartition.getPartitionNumber());
    }
    try (AutoCloseableLock ignore = AutoCloseableLock.of(adminConsumerLock)) {
      if (!kafkaAdminWrapper.get().containsTopicWithPartitionCheckExpectationAndRetry(pubSubTopicPartition, 3, true)) {
        throw new TopicDoesNotExistException(
            "Topic " + pubSubTopicPartition.getPubSubTopic()
                + " does not exist or partition requested is less topic partition count!");
      }

      try {
        Map<PubSubTopicPartition, Long> offsetMap = pubSubConsumer.get()
            .endOffsets(Collections.singletonList(pubSubTopicPartition), DEFAULT_KAFKA_OFFSET_API_TIMEOUT);
        Long offset = offsetMap.get(pubSubTopicPartition);
        if (offset != null) {
          return offset;
        } else {
          throw new VeniceException(
              "offset result returned from endOffsets does not contain entry: " + pubSubTopicPartition);
        }
      } catch (Exception ex) {
        if (ex instanceof org.apache.kafka.common.errors.TimeoutException) {
          throw new VeniceOperationAgainstKafkaTimedOut(
              "Timeout exception when seeking to end to get latest offset" + " for topic partition: "
                  + pubSubTopicPartition,
              ex);
        } else {
          throw ex;
        }
      }
    }
  }

  @Override
  public long getPartitionLatestOffsetAndRetry(PubSubTopicPartition pubSubTopicPartition, int retries) {
    return getEndOffset(pubSubTopicPartition, retries, this::getLatestOffset);
  }

  @Override
  public long getPartitionEarliestOffsetAndRetry(PubSubTopicPartition pubSubTopicPartition, int retries) {
    return getEndOffset(pubSubTopicPartition, retries, this::getEarliestOffset);
  }

  private long getEndOffset(
      PubSubTopicPartition pubSubTopicPartition,
      int retries,
      Function<PubSubTopicPartition, Long> offsetSupplier) {
    if (retries < 1) {
      throw new IllegalArgumentException("Invalid retries. Got: " + retries);
    }
    int attempt = 0;
    VeniceOperationAgainstKafkaTimedOut lastException =
        new VeniceOperationAgainstKafkaTimedOut("This exception should not be thrown");
    while (attempt < retries) {
      try {
        return offsetSupplier.apply(pubSubTopicPartition);
      } catch (VeniceOperationAgainstKafkaTimedOut e) { // topic and partition is listed in the exception object
        logger.warn("Failed to get offset. Retries remaining: {}", retries - attempt, e);
        lastException = e;
        attempt++;
      }
    }
    throw lastException;
  }

  @Override
  public long getPartitionOffsetByTime(PubSubTopicPartition pubSubTopicPartition, long timestamp) {
    try (AutoCloseableLock ignore = AutoCloseableLock.of(adminConsumerLock)) {
      Long result = offsetsForTimesWithRetry(pubSubTopicPartition, timestamp);
      if (result == null) {
        result = getOffsetByTimeIfOutOfRange(pubSubTopicPartition, timestamp);
      } else if (result == -1L) {
        // The given timestamp exceed the timestamp of the last message. So return the last offset.
        logger.warn("Offsets result is empty. Will complement with the last offsets.");
        result = endOffsetsWithRetry(pubSubTopicPartition) + 1;
      }
      return result;
    }
  }

  private Long offsetsForTimesWithRetry(PubSubTopicPartition pubSubTopicPartition, long timestamp) {
    try (AutoCloseableLock ignore = AutoCloseableLock.of(adminConsumerLock)) {
      Long topicPartitionOffset = RetryUtils.executeWithMaxAttemptAndExponentialBackoff(
          () -> pubSubConsumer.get().offsetForTime(pubSubTopicPartition, timestamp, kafkaOperationTimeout),
          25,
          Duration.ofMillis(100),
          Duration.ofSeconds(5),
          Duration.ofMinutes(1),
          KAFKA_RETRIABLE_FAILURES);
      return topicPartitionOffset;
    }
  }

  private Long endOffsetsWithRetry(PubSubTopicPartition partition) {
    try (AutoCloseableLock ignore = AutoCloseableLock.of(adminConsumerLock)) {
      Long topicPartitionOffset = RetryUtils.executeWithMaxAttemptAndExponentialBackoff(
          () -> pubSubConsumer.get().endOffset(partition),
          25,
          Duration.ofMillis(100),
          Duration.ofSeconds(5),
          Duration.ofMinutes(1),
          KAFKA_RETRIABLE_FAILURES);
      return topicPartitionOffset;
    }
  }

  @Override
  public long getProducerTimestampOfLastDataRecord(PubSubTopicPartition pubSubTopicPartition, int retries) {
    if (retries < 1) {
      throw new IllegalArgumentException("Invalid retries. Got: " + retries);
    }
    int attempt = 0;
    long timestamp;
    VeniceOperationAgainstKafkaTimedOut lastException =
        new VeniceOperationAgainstKafkaTimedOut("This exception should not be thrown");
    while (attempt < retries) {
      try {
        timestamp = getProducerTimestampOfLastDataRecord(pubSubTopicPartition);
        return timestamp;
      } catch (VeniceOperationAgainstKafkaTimedOut e) {// topic and partition is listed in the exception object
        logger.warn(
            "Failed to get producer timestamp on the latest data record. Retries remaining: {}",
            retries - attempt,
            e);
        lastException = e;
        attempt++;
      }
    }
    throw lastException;
  }

  /**
   * If the topic is empty or all the messages are truncated (startOffset==endOffset), return -1;
   * otherwise, return the producer timestamp of the last message in the selected partition of a topic
   */
  private long getProducerTimestampOfLastDataRecord(PubSubTopicPartition pubSubTopicPartition)
      throws TopicDoesNotExistException {
    List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> lastConsumedRecords =
        consumeLatestRecords(pubSubTopicPartition, 1);
    if (lastConsumedRecords.isEmpty()) {
      return NO_PRODUCER_TIME_IN_EMPTY_TOPIC_PARTITION;
    }

    // Check the latest record as the first attempt
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> lastRecord = lastConsumedRecords.iterator().next();
    if (!lastRecord.getKey().isControlMessage()) {
      return lastRecord.getValue().producerMetadata.messageTimestamp;
    }

    // Second attempt and read 60 records this time. There could be several control messages at the end of a RT
    // partition
    // if multiple Samza jobs write to this topic partition. 60 should be sufficient.
    final int lastRecordsCount = 60;
    logger.info(
        "The last record in topic partition {} is a control message. Hence, try to find the "
            + "last data record among the last {} records from the end of that partition",
        pubSubTopicPartition,
        lastRecordsCount);

    lastConsumedRecords = consumeLatestRecords(pubSubTopicPartition, lastRecordsCount);
    if (lastConsumedRecords.isEmpty()) {
      // Topic partition becomes empty and it can happen if the topic gets truncated after the first attempt and before
      // this second attempt. This case should happen very rarely.
      logger.warn(
          "Second attempt to find producer timestamp from topic partition {} by consuming the last"
              + " {} record(s) consumed no record. Assume the topic partition is empty.",
          pubSubTopicPartition,
          lastRecordsCount);
      return NO_PRODUCER_TIME_IN_EMPTY_TOPIC_PARTITION;
    }
    Iterator<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> consumerRecordsIterator =
        lastConsumedRecords.iterator();

    Long latestDataRecordProducerTimestamp = null;
    Long startOffset = null;
    int recordsCount = 0;

    while (consumerRecordsIterator.hasNext()) {
      PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record = consumerRecordsIterator.next();
      startOffset = (startOffset == null ? record.getOffset() : startOffset);
      recordsCount++;

      if (record.getKey().isControlMessage()) {
        continue;
      }
      latestDataRecordProducerTimestamp = record.getValue().producerMetadata.messageTimestamp;
    }
    if (latestDataRecordProducerTimestamp == null) {
      // It is quite impossible to have no data record in this many records from the end of that partition
      throw new VeniceException(
          String.format(
              "Failed to find latest data record producer timestamp in topic partition %s "
                  + "since no data record is found in the last %d records starting from offset %d",
              pubSubTopicPartition,
              recordsCount,
              startOffset));
    }
    return latestDataRecordProducerTimestamp;
  }

  /**
   * This method retrieves last {@code lastRecordsCount} records from a topic partition and there are 4 steps below.
   *  1. Find the current end offset N
   *  2. Seek back {@code lastRecordsCount} records from the end offset N
   *  3. Keep consuming records until the last consumed offset is greater than or equal to N
   *  4. Return all consumed records
   *
   * There are 2 things to note:
   *   1. When this method returns, these returned records are not necessarily the "last" records because after step 2,
   *      there could be more records produced to this topic partition and this method only consume records until the end
   *      offset retrieved at the above step 2.
   *
   *   2. This method might return more than {@code lastRecordsCount} records since the consumer poll method gets a batch
   *      of consumer records each time and the batch size is arbitrary.
   */
  private List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> consumeLatestRecords(
      final PubSubTopicPartition pubSubTopicPartition,
      final int lastRecordsCount) {

    if (pubSubTopicPartition.getPartitionNumber() < 0) {
      throw new IllegalArgumentException(
          "Cannot retrieve latest producer timestamp for invalid topic partition " + pubSubTopicPartition);
    }
    if (lastRecordsCount < 1) {
      throw new IllegalArgumentException(
          "Last record count must be greater than or equal to 1. Got: " + lastRecordsCount);
    }

    try (AutoCloseableLock ignore = AutoCloseableLock.of(adminConsumerLock)) {
      if (!kafkaAdminWrapper.get()
          .containsTopicWithExpectationAndRetry(pubSubTopicPartition.getPubSubTopic(), 3, true)) {
        throw new TopicDoesNotExistException("Topic " + pubSubTopicPartition.getPubSubTopic() + " does not exist!");
      }
      try {
        Map<PubSubTopicPartition, Long> offsetByTopicPartition = pubSubConsumer.get()
            .endOffsets(Collections.singletonList(pubSubTopicPartition), DEFAULT_KAFKA_OFFSET_API_TIMEOUT);
        if (offsetByTopicPartition == null || !offsetByTopicPartition.containsKey(pubSubTopicPartition)) {
          throw new VeniceException(
              "Got no results of finding end offsets for topic partition: " + pubSubTopicPartition);
        }
        final long latestOffset = offsetByTopicPartition.get(pubSubTopicPartition);

        if (latestOffset <= 0) {
          // Empty topic
          return Collections.emptyList();
        } else {
          Long earliestOffset =
              pubSubConsumer.get().beginningOffset(pubSubTopicPartition, DEFAULT_KAFKA_OFFSET_API_TIMEOUT);
          if (earliestOffset == null) {
            throw new VeniceException(
                "Got no results of finding the earliest offset for topic partition: " + pubSubTopicPartition);
          }
          if (earliestOffset == latestOffset) {
            // Empty topic
            return Collections.emptyList();
          } else {
            // poll the last message and retrieve the producer timestamp
            final long startConsumeOffset = Math.max(latestOffset - lastRecordsCount, earliestOffset);
            pubSubConsumer.get().subscribe(pubSubTopicPartition, startConsumeOffset - 1);
            List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> allConsumedRecords =
                new ArrayList<>(lastRecordsCount);

            // Keep consuming records from that topic partition until the last consumed record's offset is greater or
            // equal
            // to the partition end offset retrieved before.
            do {
              List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> oneBatchConsumedRecords =
                  Collections.emptyList();
              int currAttempt = 0;

              while (currAttempt++ < KAFKA_POLLING_RETRY_MAX_ATTEMPT && oneBatchConsumedRecords.isEmpty()) {
                logger.info(
                    "Trying to get records from topic partition {} from offset {} to its log end "
                        + "offset. Attempt# {} / {}",
                    pubSubTopicPartition,
                    startConsumeOffset,
                    currAttempt,
                    KAFKA_POLLING_RETRY_MAX_ATTEMPT);

                oneBatchConsumedRecords =
                    pubSubConsumer.get().poll(kafkaOperationTimeout.toMillis()).get(pubSubTopicPartition);
              }
              if (oneBatchConsumedRecords.isEmpty()) {
                /**
                 * Failed the job if we cannot get the last offset of the topic.
                 */
                String errorMsg = "Failed to get the last record from topic-partition: " + pubSubTopicPartition
                    + " after " + KAFKA_POLLING_RETRY_MAX_ATTEMPT + " attempts";
                logger.error(errorMsg);
                throw new VeniceException(errorMsg);
              }
              logger.info(
                  "Consumed {} record(s) from topic partition {}",
                  oneBatchConsumedRecords.size(),
                  pubSubTopicPartition);

              allConsumedRecords.addAll(oneBatchConsumedRecords);
            } while (allConsumedRecords.get(allConsumedRecords.size() - 1).getOffset() + 1 < latestOffset);

            return allConsumedRecords;
          }
        }
      } catch (org.apache.kafka.common.errors.TimeoutException ex) {
        throw new VeniceOperationAgainstKafkaTimedOut(
            "Timeout exception when seeking to end to get latest offset" + " for topic and partition: "
                + pubSubTopicPartition,
            ex);
      } finally {
        pubSubConsumer.get().unSubscribe(pubSubTopicPartition);
      }
    }
  }

  @Override
  public List<PubSubTopicPartitionInfo> partitionsFor(PubSubTopic topic) {
    try (AutoCloseableLock ignore = AutoCloseableLock.of(adminConsumerLock)) {
      return pubSubConsumer.get().partitionsFor(topic);
    }
  }

  @Override
  public long getOffsetByTimeIfOutOfRange(PubSubTopicPartition pubSubTopicPartition, long timestamp)
      throws TopicDoesNotExistException {
    try (AutoCloseableLock ignore = AutoCloseableLock.of(adminConsumerLock)) {
      long latestOffset = getLatestOffset(pubSubTopicPartition);
      if (latestOffset <= 0) {
        long nextOffset = LOWEST_OFFSET + 1;
        logger.info("End offset for topic {} is {}; return offset {}", pubSubTopicPartition, latestOffset, nextOffset);
        return nextOffset;
      }

      long earliestOffset = getEarliestOffset(pubSubTopicPartition);
      if (earliestOffset == latestOffset) {
        /**
         * This topic/partition is empty or retention delete the entire partition
         */
        logger.info(
            "Both beginning offset and end offset is {} for topic {}; it's empty; return offset {}",
            latestOffset,
            pubSubTopicPartition,
            latestOffset);
        return latestOffset;
      }

      try {
        pubSubConsumer.get().subscribe(pubSubTopicPartition, latestOffset - 2);
        Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> records = new HashMap<>();
        /**
         * We should retry to get the last record from that topic/partition, never return 0L here because 0L offset
         * will result in replaying all the messages in real-time buffer. This function is mainly used during buffer
         * replay for hybrid stores.
         */
        int attempts = 0;
        while (attempts++ < KAFKA_POLLING_RETRY_MAX_ATTEMPT && records.isEmpty()) {
          logger.info(
              "Trying to get the last record from topic: {} at offset: {}. Attempt#{}/{}",
              pubSubTopicPartition,
              latestOffset - 1,
              attempts,
              KAFKA_POLLING_RETRY_MAX_ATTEMPT);
          records = pubSubConsumer.get().poll(kafkaOperationTimeout.toMillis());
        }
        if (records.isEmpty()) {
          /**
           * Failed the job if we cannot get the last offset of the topic.
           */
          String errorMsg = "Failed to get the last record from topic: " + pubSubTopicPartition + " after "
              + KAFKA_POLLING_RETRY_MAX_ATTEMPT + " attempts";
          logger.error(errorMsg);
          throw new VeniceException(errorMsg);
        }

        // Get the latest record from the poll result

        PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record = Utils.iterateOnMapOfLists(records).next();

        if (timestamp <= record.getPubSubMessageTime()) {
          /**
           * There could be a race condition in this function:
           * 1. In function: {@link #getPartitionsOffsetsByTime}, {@link Consumer#offsetsForTimes} is invoked.
           * 2. The asked timestamp is out of range.
           * 3. Some messages get produced to the topic.
           * 4. {@link #getOffsetByTimeIfOutOfRange} gets invoked, and it realizes that the latest message's timestamp
           * is higher than the seeking timestamp.
           *
           * In this case, we should call {@link #getPartitionOffsetByTime} again, since the seeking timestamp will be in the range
           * instead of returning the earliest offset.
           */
          Long result = pubSubConsumer.get().offsetForTime(pubSubTopicPartition, timestamp);
          if (result != null && result != -1L) {
            logger.info(
                "Successfully return offset: {} for topic: {} for timestamp: {}",
                result,
                pubSubTopicPartition,
                timestamp);
            return result;
          }
        }

        /**
         * 1. If the required timestamp is bigger than the timestamp of last record, return the offset after the last record.
         * 2. Otherwise, return earlier offset to consume from the beginning.
         */
        long resultOffset = (timestamp > record.getPubSubMessageTime()) ? latestOffset : earliestOffset;
        logger.info(
            "Successfully return offset: {} for topic: {} for timestamp: {}",
            resultOffset,
            pubSubTopicPartition,
            timestamp);
        return resultOffset;
      } finally {
        pubSubConsumer.get().unSubscribe(pubSubTopicPartition);
      }
    }
  }

  /**
   * @return the beginning offset of a topic/partition. Synchronized because it calls #getConsumer()
   */
  private long getEarliestOffset(PubSubTopicPartition pubSubTopicPartition) throws TopicDoesNotExistException {
    try (AutoCloseableLock ignore = AutoCloseableLock.of(adminConsumerLock)) {
      if (!kafkaAdminWrapper.get()
          .containsTopicWithExpectationAndRetry(pubSubTopicPartition.getPubSubTopic(), 3, true)) {
        throw new TopicDoesNotExistException("Topic " + pubSubTopicPartition.getPubSubTopic() + " does not exist!");
      }
      if (pubSubTopicPartition.getPartitionNumber() < 0) {
        throw new IllegalArgumentException(
            "Cannot retrieve latest offsets for invalid partition " + pubSubTopicPartition.getPartitionNumber());
      }
      long earliestOffset;
      try {
        Long offset = pubSubConsumer.get().beginningOffset(pubSubTopicPartition, DEFAULT_KAFKA_OFFSET_API_TIMEOUT);
        if (offset != null) {
          earliestOffset = offset;
        } else {
          throw new VeniceException(
              "offset result returned from beginningOffsets does not contain entry: " + pubSubTopicPartition);
        }
      } catch (Exception ex) {
        if (ex instanceof org.apache.kafka.common.errors.TimeoutException) {
          throw new VeniceOperationAgainstKafkaTimedOut(
              "Timeout exception when seeking to beginning to get earliest offset" + " for topic partition: "
                  + pubSubTopicPartition,
              ex);
        } else {
          throw ex;
        }
      }
      return earliestOffset;
    }
  }

  @Override
  public void close() {
    if (kafkaAdminWrapper.isPresent()) {
      IOUtils.closeQuietly(kafkaAdminWrapper.get(), logger::error);
    }
    if (pubSubConsumer.isPresent()) {
      IOUtils.closeQuietly(pubSubConsumer.get(), logger::error);
    }
  }
}
