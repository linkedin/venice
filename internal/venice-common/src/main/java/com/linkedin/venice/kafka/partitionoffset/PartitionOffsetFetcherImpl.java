package com.linkedin.venice.kafka.partitionoffset;

import static com.linkedin.venice.offsets.OffsetRecord.*;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.kafka.VeniceOperationAgainstKafkaTimedOut;
import com.linkedin.venice.kafka.admin.KafkaAdminWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongMaps;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class PartitionOffsetFetcherImpl implements PartitionOffsetFetcher {
  private static final List<Class<? extends Throwable>> KAFKA_RETRIABLE_FAILURES =
      Collections.singletonList(org.apache.kafka.common.errors.RetriableException.class);
  public static final Duration DEFAULT_KAFKA_OFFSET_API_TIMEOUT = Duration.ofMinutes(1);
  public static final long NO_PRODUCER_TIME_IN_EMPTY_TOPIC_PARTITION = -1;
  private static final int KAFKA_POLLING_RETRY_MAX_ATTEMPT = 3;

  private final Logger logger;
  private final Lock rawConsumerLock;
  private final Lock kafkaRecordConsumerLock;
  private final Lazy<Consumer<byte[], byte[]>> kafkaRawBytesConsumer;
  private final Lazy<Consumer<KafkaKey, KafkaMessageEnvelope>> kafkaRecordConsumer;
  private final Lazy<KafkaAdminWrapper> kafkaAdminWrapper;
  private final Duration kafkaOperationTimeout;

  public PartitionOffsetFetcherImpl(
      @Nonnull Lazy<Consumer<byte[], byte[]>> kafkaRawBytesConsumer,
      @Nonnull Lazy<Consumer<KafkaKey, KafkaMessageEnvelope>> kafkaRecordConsumer,
      @Nonnull Lazy<KafkaAdminWrapper> kafkaAdminWrapper,
      long kafkaOperationTimeoutMs,
      String kafkaBootstrapServers) {
    Validate.notNull(kafkaRawBytesConsumer);
    Validate.notNull(kafkaRecordConsumer);
    Validate.notNull(kafkaAdminWrapper);
    this.kafkaRawBytesConsumer = kafkaRawBytesConsumer;
    this.kafkaRecordConsumer = kafkaRecordConsumer;
    this.kafkaAdminWrapper = kafkaAdminWrapper;
    this.rawConsumerLock = new ReentrantLock();
    this.kafkaRecordConsumerLock = new ReentrantLock();
    this.kafkaOperationTimeout = Duration.ofMillis(kafkaOperationTimeoutMs);
    this.logger =
        LogManager.getLogger(PartitionOffsetFetcherImpl.class.getSimpleName() + " [" + kafkaBootstrapServers + "]");
  }

  @Override
  public Int2LongMap getTopicLatestOffsets(String topic) {
    try (AutoCloseableLock ignore = AutoCloseableLock.of(rawConsumerLock)) {
      List<PartitionInfo> partitionInfoList = kafkaRawBytesConsumer.get().partitionsFor(topic);
      if (partitionInfoList == null || partitionInfoList.isEmpty()) {
        logger
            .warn("Unexpected! Topic: " + topic + " has a null partition set, returning empty map for latest offsets");
        return Int2LongMaps.EMPTY_MAP;
      }
      List<TopicPartition> topicPartitions = partitionInfoList.stream()
          .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
          .collect(Collectors.toList());

      Map<TopicPartition, Long> offsetsByTopicPartitions =
          kafkaRawBytesConsumer.get().endOffsets(topicPartitions, DEFAULT_KAFKA_OFFSET_API_TIMEOUT);
      Int2LongMap offsetsByTopicPartitionIds = new Int2LongOpenHashMap(offsetsByTopicPartitions.size());
      for (Map.Entry<TopicPartition, Long> offsetByTopicPartition: offsetsByTopicPartitions.entrySet()) {
        offsetsByTopicPartitionIds
            .put(offsetByTopicPartition.getKey().partition(), offsetByTopicPartition.getValue().longValue());
      }
      return offsetsByTopicPartitionIds;
    }
  }

  private long getLatestOffset(String topic, int partition) throws TopicDoesNotExistException {
    if (partition < 0) {
      throw new IllegalArgumentException("Cannot retrieve latest offsets for invalid partition " + partition);
    }
    try (AutoCloseableLock ignore = AutoCloseableLock.of(rawConsumerLock)) {
      if (!kafkaAdminWrapper.get().containsTopicWithPartitionCheckExpectationAndRetry(topic, partition, 3, true)) {
        throw new TopicDoesNotExistException(
            "Topic " + topic + " does not exist or partition requested is less topic partition count!");
      }

      TopicPartition topicPartition = new TopicPartition(topic, partition);
      try {
        Map<TopicPartition, Long> offsetMap = kafkaRawBytesConsumer.get()
            .endOffsets(Collections.singletonList(topicPartition), DEFAULT_KAFKA_OFFSET_API_TIMEOUT);
        Long offset = offsetMap.get(topicPartition);
        if (offset != null) {
          return offset;
        } else {
          throw new VeniceException("offset result returned from endOffsets does not contain entry: " + topicPartition);
        }
      } catch (Exception ex) {
        if (ex instanceof org.apache.kafka.common.errors.TimeoutException) {
          throw new VeniceOperationAgainstKafkaTimedOut(
              "Timeout exception when seeking to end to get latest offset" + " for topic: " + topic + " and partition: "
                  + partition,
              ex);
        } else {
          throw ex;
        }
      }
    }
  }

  @Override
  public long getPartitionLatestOffsetAndRetry(String topic, int partition, int retries) {
    if (retries < 1) {
      throw new IllegalArgumentException("Invalid retries. Got: " + retries);
    }
    int attempt = 0;
    VeniceOperationAgainstKafkaTimedOut lastException =
        new VeniceOperationAgainstKafkaTimedOut("This exception should not be thrown");
    while (attempt < retries) {
      try {
        return getLatestOffset(topic, partition);
      } catch (VeniceOperationAgainstKafkaTimedOut e) { // topic and partition is listed in the exception object
        logger.warn("Failed to get latest offset.  Retries remaining: " + (retries - attempt), e);
        lastException = e;
        attempt++;
      }
    }
    throw lastException;
  }

  @Override
  public long getPartitionOffsetByTime(String topic, int partition, long timestamp) {
    final TopicPartition topicPartition = new TopicPartition(topic, partition);
    return getOffsetsByTimeWithRetry(Collections.singletonMap(topicPartition, timestamp)).get(topicPartition);
  }

  private Map<TopicPartition, Long> getOffsetsByTimeWithRetry(Map<TopicPartition, Long> timestampsToSearch) {
    try (AutoCloseableLock ignore = AutoCloseableLock.of(rawConsumerLock)) {
      final int expectedPartitionNum = timestampsToSearch.size();
      Map<TopicPartition, Long> result = offsetsForTimesWithRetry(timestampsToSearch).entrySet()
          .stream()
          .collect(Collectors.toMap(partitionToOffset -> {
            Validate.notNull(partitionToOffset.getKey(), "Got a null TopicPartition key out of the offsetsForTime API");
            return partitionToOffset.getKey();
          },
              partitionToOffset -> partitionToOffset.getValue() != null
                  ? partitionToOffset.getValue().offset()
                  : getOffsetByTimeIfOutOfRange(
                      partitionToOffset.getKey(),
                      timestampsToSearch.get(partitionToOffset.getKey()))));
      // The given timestamp exceed the timestamp of the last message. So return the last offset.
      if (result.isEmpty()) {
        logger.warn("Offsets result is empty. Will complement with the last offsets.");
        result = endOffsetsWithRetry(timestampsToSearch.keySet()).entrySet()
            .stream()
            .collect(Collectors.toMap(partitionToOffset -> {
              Validate.notNull(partitionToOffset);
              return partitionToOffset.getKey();
            }, partitionToOffset -> partitionToOffset.getValue() + 1));

      } else if (result.size() != expectedPartitionNum) {
        logger.warn(
            "Missing offsets for some partitions. Partition Number should be :" + expectedPartitionNum
                + " but only got: " + result.size() + ". Will complement with the last offsets.");
        // Get partial offsets result.
        Map<TopicPartition, Long> endOffsets = endOffsetsWithRetry(timestampsToSearch.keySet());

        for (TopicPartition topicPartition: timestampsToSearch.keySet()) {
          if (!result.containsKey(topicPartition)) {
            result.put(topicPartition, endOffsets.get(topicPartition) + 1);
          }
        }
      }

      if (result.size() < expectedPartitionNum) {
        throw new VeniceException(
            "Failed to get offsets for all partitions. Got offsets for " + result.size() + " partitions, should be: "
                + expectedPartitionNum);
      }
      return result;
    }
  }

  private Map<TopicPartition, OffsetAndTimestamp> offsetsForTimesWithRetry(
      Map<TopicPartition, Long> timestampsToSearch) {
    try (AutoCloseableLock ignore = AutoCloseableLock.of(rawConsumerLock)) {
      return RetryUtils.executeWithMaxAttemptAndExponentialBackoff(
          () -> kafkaRawBytesConsumer.get().offsetsForTimes(timestampsToSearch, kafkaOperationTimeout),
          25,
          Duration.ofMillis(100),
          Duration.ofSeconds(5),
          Duration.ofMinutes(1),
          KAFKA_RETRIABLE_FAILURES);
    }
  }

  private Map<TopicPartition, Long> endOffsetsWithRetry(Collection<TopicPartition> partitions) {
    try (AutoCloseableLock ignore = AutoCloseableLock.of(rawConsumerLock)) {
      return RetryUtils.executeWithMaxAttemptAndExponentialBackoff(
          () -> kafkaRawBytesConsumer.get().endOffsets(partitions),
          25,
          Duration.ofMillis(100),
          Duration.ofSeconds(5),
          Duration.ofMinutes(1),
          KAFKA_RETRIABLE_FAILURES);
    }
  }

  @Override
  public long getProducerTimestampOfLastDataRecord(String topic, int partition, int retries) {
    if (retries < 1) {
      throw new IllegalArgumentException("Invalid retries. Got: " + retries);
    }
    int attempt = 0;
    long timestamp;
    VeniceOperationAgainstKafkaTimedOut lastException =
        new VeniceOperationAgainstKafkaTimedOut("This exception should not be thrown");
    while (attempt < retries) {
      try {
        timestamp = getProducerTimestampOfLastDataRecord(topic, partition);
        return timestamp;
      } catch (VeniceOperationAgainstKafkaTimedOut e) {// topic and partition is listed in the exception object
        logger.warn(
            "Failed to get producer timestamp on the latest data record.  Retries remaining: " + (retries - attempt),
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
  private long getProducerTimestampOfLastDataRecord(String topic, int partition) throws TopicDoesNotExistException {
    List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> lastConsumedRecords =
        consumeLatestRecords(topic, partition, 1);
    if (lastConsumedRecords.isEmpty()) {
      return NO_PRODUCER_TIME_IN_EMPTY_TOPIC_PARTITION;
    }

    // Check the latest record as the first attempt
    ConsumerRecord<KafkaKey, KafkaMessageEnvelope> lastRecord = lastConsumedRecords.iterator().next();
    if (!lastRecord.key().isControlMessage()) {
      return lastRecord.value().producerMetadata.messageTimestamp;
    }

    // Second attempt and read 60 records this time. There could be several control messages at the end of a RT
    // partition
    // if multiple Samza jobs write to this topic partition. 60 should be sufficient.
    final int lastRecordsCount = 60;
    logger.info(
        String.format(
            "The last record in topic %s partition %d is a control message. Hence, try to find the "
                + "last data record among the last %d records from the end of that partition",
            topic,
            partition,
            lastRecordsCount));

    lastConsumedRecords = consumeLatestRecords(topic, partition, lastRecordsCount);
    if (lastConsumedRecords.isEmpty()) {
      // Topic partition becomes empty and it can happen if the topic gets truncated after the first attempt and before
      // this second attempt. This case should happen very rarely.
      logger.warn(
          String.format(
              "Second attempt to find producer timestamp from topic %s partition %d by consuming the last"
                  + " %d record(s) consumed no record. Assume the topic partition is empty.",
              topic,
              partition,
              lastRecordsCount));
      return NO_PRODUCER_TIME_IN_EMPTY_TOPIC_PARTITION;
    }
    Iterator<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> consumerRecordsIterator = lastConsumedRecords.iterator();

    Long latestDataRecordProducerTimestamp = null;
    Long startOffset = null;
    int recordsCount = 0;

    while (consumerRecordsIterator.hasNext()) {
      ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record = consumerRecordsIterator.next();
      startOffset = (startOffset == null ? record.offset() : startOffset);
      recordsCount++;

      if (record.key().isControlMessage()) {
        continue;
      }
      latestDataRecordProducerTimestamp = record.value().producerMetadata.messageTimestamp;
    }
    if (latestDataRecordProducerTimestamp == null) {
      // It is quite impossible to have no data record in this many records from the end of that partition
      throw new VeniceException(
          String.format(
              "Failed to find latest data record producer timestamp in topic %s "
                  + "partition %d since no data record is found in the last %d records starting from offset %d",
              topic,
              partition,
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
  private List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> consumeLatestRecords(
      final String topic,
      final int partition,
      final int lastRecordsCount) {
    if (partition < 0) {
      throw new IllegalArgumentException(
          "Cannot retrieve latest producer timestamp for invalid partition " + partition + " topic " + topic);
    }
    if (lastRecordsCount < 1) {
      throw new IllegalArgumentException(
          "Last record count must be greater than or equal to 1. Got: " + lastRecordsCount);
    }

    try (AutoCloseableLock ignore = AutoCloseableLock.of(kafkaRecordConsumerLock)) {
      if (!kafkaAdminWrapper.get().containsTopicWithExpectationAndRetry(topic, 3, true)) {
        throw new TopicDoesNotExistException("Topic " + topic + " does not exist!");
      }
      final TopicPartition topicPartition = new TopicPartition(topic, partition);
      try {
        Map<TopicPartition, Long> offsetByTopicPartition = kafkaRecordConsumer.get()
            .endOffsets(Collections.singletonList(topicPartition), DEFAULT_KAFKA_OFFSET_API_TIMEOUT);
        if (offsetByTopicPartition == null || !offsetByTopicPartition.containsKey(topicPartition)) {
          throw new VeniceException("Got no results of finding end offsets for topic partition: " + topicPartition);
        }
        final long latestOffset = offsetByTopicPartition.get(topicPartition);

        if (latestOffset <= 0) {
          // Empty topic
          return Collections.emptyList();
        } else {
          Long earliestOffset = kafkaRecordConsumer.get()
              .beginningOffsets(Collections.singletonList(topicPartition), DEFAULT_KAFKA_OFFSET_API_TIMEOUT)
              .get(topicPartition);
          if (earliestOffset == null) {
            throw new VeniceException(
                "Got no results of finding the earliest offset for topic partition: " + topicPartition);
          }
          if (earliestOffset == latestOffset) {
            // Empty topic
            return Collections.emptyList();
          } else {
            // poll the last message and retrieve the producer timestamp
            kafkaRecordConsumer.get().assign(Collections.singletonList(topicPartition));
            final long startConsumeOffset = Math.max(latestOffset - lastRecordsCount, earliestOffset);
            kafkaRecordConsumer.get().seek(topicPartition, startConsumeOffset);
            List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> allConsumedRecords = new ArrayList<>(lastRecordsCount);

            // Keep consuming records from that topic partition until the last consumed record's offset is greater or
            // equal
            // to the partition end offset retrieved before.
            do {
              List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> oneBatchConsumedRecords = Collections.emptyList();
              int currAttempt = 0;

              while (currAttempt++ < KAFKA_POLLING_RETRY_MAX_ATTEMPT && oneBatchConsumedRecords.isEmpty()) {
                logger.info(
                    String.format(
                        "Trying to get records from topic %s partition %d from offset %d to its log end "
                            + "offset. Attempt# %d / %d",
                        topic,
                        partition,
                        startConsumeOffset,
                        currAttempt,
                        KAFKA_POLLING_RETRY_MAX_ATTEMPT));

                oneBatchConsumedRecords = kafkaRecordConsumer.get().poll(kafkaOperationTimeout).records(topicPartition);
              }
              if (oneBatchConsumedRecords.isEmpty()) {
                /**
                 * Failed the job if we cannot get the last offset of the topic.
                 */
                String errorMsg = "Failed to get the last record from topic-partition: " + topicPartition.toString()
                    + " after " + KAFKA_POLLING_RETRY_MAX_ATTEMPT + " attempts";
                logger.error(errorMsg);
                throw new VeniceException(errorMsg);
              }

              logger.info(
                  String.format(
                      "Consumed %d record(s) from topic %s partition %d",
                      oneBatchConsumedRecords.size(),
                      topicPartition.topic(),
                      topicPartition.partition()));

              allConsumedRecords.addAll(oneBatchConsumedRecords);
            } while (allConsumedRecords.get(allConsumedRecords.size() - 1).offset() + 1 < latestOffset);

            return allConsumedRecords;
          }
        }
      } catch (org.apache.kafka.common.errors.TimeoutException ex) {
        throw new VeniceOperationAgainstKafkaTimedOut(
            "Timeout exception when seeking to end to get latest offset" + " for topic: " + topic + " and partition: "
                + partition,
            ex);
      } finally {
        kafkaRecordConsumer.get().assign(Collections.emptyList());
      }
    }
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    try (AutoCloseableLock ignore = AutoCloseableLock.of(rawConsumerLock)) {
      return kafkaRawBytesConsumer.get().partitionsFor(topic);
    }
  }

  @Override
  public long getOffsetByTimeIfOutOfRange(TopicPartition topicPartition, long timestamp)
      throws TopicDoesNotExistException {
    try (AutoCloseableLock ignore = AutoCloseableLock.of(rawConsumerLock)) {
      long latestOffset = getLatestOffset(topicPartition.topic(), topicPartition.partition());
      if (latestOffset <= 0) {
        long nextOffset = LOWEST_OFFSET + 1;
        logger.info("End offset for topic " + topicPartition + " is " + latestOffset + "; return offset " + nextOffset);
        return nextOffset;
      }

      long earliestOffset = getEarliestOffset(topicPartition.topic(), topicPartition.partition());
      if (earliestOffset == latestOffset) {
        /**
         * This topic/partition is empty or retention delete the entire partition
         */
        logger.info(
            "Both beginning offset and end offset is " + latestOffset + " for topic " + topicPartition
                + "; it's empty; return offset " + latestOffset);
        return latestOffset;
      }

      try {
        kafkaRawBytesConsumer.get().assign(Collections.singletonList(topicPartition));
        kafkaRawBytesConsumer.get().seek(topicPartition, latestOffset - 1);
        ConsumerRecords records = ConsumerRecords.EMPTY;
        /**
         * We should retry to get the last record from that topic/partition, never return 0L here because 0L offset
         * will result in replaying all the messages in real-time buffer. This function is mainly used during buffer
         * replay for hybrid stores.
         */
        int attempts = 0;
        while (attempts++ < KAFKA_POLLING_RETRY_MAX_ATTEMPT && records.isEmpty()) {
          logger.info(
              "Trying to get the last record from topic: " + topicPartition.toString() + " at offset: "
                  + (latestOffset - 1) + ". Attempt#" + attempts + "/" + KAFKA_POLLING_RETRY_MAX_ATTEMPT);
          records = kafkaRawBytesConsumer.get().poll(kafkaOperationTimeout);
        }
        if (records.isEmpty()) {
          /**
           * Failed the job if we cannot get the last offset of the topic.
           */
          String errorMsg = "Failed to get the last record from topic: " + topicPartition.toString() + " after "
              + KAFKA_POLLING_RETRY_MAX_ATTEMPT + " attempts";
          logger.error(errorMsg);
          throw new VeniceException(errorMsg);
        }

        // Get the latest record from the poll result
        ConsumerRecord record = (ConsumerRecord) records.iterator().next();

        if (timestamp <= record.timestamp()) {
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
          Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
          timestampsToSearch.put(topicPartition, timestamp);
          Map<TopicPartition, OffsetAndTimestamp> result =
              kafkaRawBytesConsumer.get().offsetsForTimes(timestampsToSearch);
          if (result.containsKey(topicPartition) && result.get(topicPartition) != null) {
            OffsetAndTimestamp offsetAndTimestamp = result.get(topicPartition);
            long resultOffset = offsetAndTimestamp.offset();
            logger.info(
                "Successfully return offset: " + resultOffset + " for topic: " + topicPartition.toString()
                    + " for timestamp: " + timestamp);
            return resultOffset;
          }
        }

        /**
         * 1. If the required timestamp is bigger than the timestamp of last record, return the offset after the last record.
         * 2. Otherwise, return earlier offset to consume from the beginning.
         */
        long resultOffset = (timestamp > record.timestamp()) ? latestOffset : earliestOffset;
        logger.info(
            "Successfully return offset: " + resultOffset + " for topic: " + topicPartition.toString()
                + " for timestamp: " + timestamp);
        return resultOffset;
      } finally {
        kafkaRawBytesConsumer.get().assign(Collections.emptyList());
      }
    }
  }

  /**
   * @return the beginning offset of a topic/partition. Synchronized because it calls #getConsumer()
   */
  private long getEarliestOffset(String topic, int partition) throws TopicDoesNotExistException {
    try (AutoCloseableLock ignore = AutoCloseableLock.of(rawConsumerLock)) {
      if (!kafkaAdminWrapper.get().containsTopicWithExpectationAndRetry(topic, 3, true)) {
        throw new TopicDoesNotExistException("Topic " + topic + " does not exist!");
      }
      if (partition < 0) {
        throw new IllegalArgumentException("Cannot retrieve latest offsets for invalid partition " + partition);
      }
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      long earliestOffset;
      try {
        Map<TopicPartition, Long> offsetMap = kafkaRawBytesConsumer.get()
            .beginningOffsets(Collections.singletonList(topicPartition), DEFAULT_KAFKA_OFFSET_API_TIMEOUT);
        if (offsetMap.containsKey(topicPartition)) {
          earliestOffset = offsetMap.get(topicPartition);
        } else {
          throw new VeniceException(
              "offset result returned from beginningOffsets does not contain entry: " + topicPartition);
        }
      } catch (Exception ex) {
        if (ex instanceof org.apache.kafka.common.errors.TimeoutException) {
          throw new VeniceOperationAgainstKafkaTimedOut(
              "Timeout exception when seeking to beginning to get earliest offset" + " for topic: " + topic
                  + " and partition: " + partition,
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
    if (kafkaRawBytesConsumer.isPresent()) {
      IOUtils.closeQuietly(kafkaRawBytesConsumer.get(), logger::error);
    }
    if (kafkaRecordConsumer.isPresent()) {
      IOUtils.closeQuietly(kafkaRecordConsumer.get(), logger::error);
    }
    if (kafkaAdminWrapper.isPresent()) {
      IOUtils.closeQuietly(kafkaAdminWrapper.get(), logger::error);
    }
  }
}
