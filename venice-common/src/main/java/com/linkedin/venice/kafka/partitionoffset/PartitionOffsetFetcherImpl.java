package com.linkedin.venice.kafka.partitionoffset;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.kafka.VeniceOperationAgainstKafkaTimedOut;
import com.linkedin.venice.kafka.admin.KafkaAdminWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.utils.Lazy;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import static com.linkedin.venice.offsets.OffsetRecord.*;


public class PartitionOffsetFetcherImpl implements PartitionOffsetFetcher {
  private static final Logger LOGGER = Logger.getLogger(PartitionOffsetFetcherImpl.class);
  public static final Duration DEFAULT_KAFKA_OFFSET_API_TIMEOUT = Duration.ofMinutes(1);
  private static final int KAFKA_POLLING_RETRY_ATTEMPT = 3;

  private final Lock rawConsumerLock;
  private final Lock kafkaRecordConsumerLock;
  private final Lazy<KafkaConsumer<byte[], byte[]>> kafkaRawBytesConsumer;
  private final Lazy<KafkaConsumer<KafkaKey, KafkaMessageEnvelope>> kafkaRecordConsumer;
  private final Lazy<KafkaAdminWrapper> kafkaAdminWrapper;
  private final Duration kafkaOperationTimeout;

  public PartitionOffsetFetcherImpl(
      Lazy<KafkaConsumer<byte[], byte[]>> kafkaRawBytesConsumer,
      Lazy<KafkaConsumer<KafkaKey, KafkaMessageEnvelope>> kafkaRecordConsumer,
      Lazy<KafkaAdminWrapper> kafkaAdminWrapper,
      long kafkaOperationTimeoutMs
  ) {
    this.kafkaRawBytesConsumer = Utils.notNull(kafkaRawBytesConsumer);
    this.kafkaRecordConsumer = Utils.notNull(kafkaRecordConsumer);
    this.kafkaAdminWrapper = Utils.notNull(kafkaAdminWrapper);
    this.rawConsumerLock = new ReentrantLock();
    this.kafkaRecordConsumerLock = new ReentrantLock();
    this.kafkaOperationTimeout = Duration.ofMillis(kafkaOperationTimeoutMs);
  }

  @Override
  public Map<Integer, Long> getLatestOffsets(String topic) {
    try (AutoCloseableLock ignore = new AutoCloseableLock(rawConsumerLock)) {
      List<PartitionInfo> partitionInfoList = kafkaRawBytesConsumer.get().partitionsFor(topic);
      if (null == partitionInfoList || partitionInfoList.isEmpty()) {
        LOGGER.warn("Unexpected! Topic: " + topic + " has a null partition set, returning empty map for latest offsets");
        return Collections.emptyMap();
      }
      return partitionInfoList.stream().map(PartitionInfo::partition)
          .collect(Collectors.toMap(p -> p, p -> getLatestOffset(topic, p, false)));
    }
  }

  private Long getLatestOffset(String topic, Integer partition, boolean doTopicCheck) throws TopicDoesNotExistException {
    if (partition < 0) {
      throw new IllegalArgumentException("Cannot retrieve latest offsets for invalid partition " + partition);
    }
    try (AutoCloseableLock ignore = new AutoCloseableLock(rawConsumerLock)) {
      if (doTopicCheck && !kafkaAdminWrapper.get().containsTopic(topic)) {
        throw new TopicDoesNotExistException("Topic " + topic + " does not exist!");
      }
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      long latestOffset;
      try {
        Map<TopicPartition, Long> offsetMap =
            kafkaRawBytesConsumer.get().endOffsets(Collections.singletonList(topicPartition), DEFAULT_KAFKA_OFFSET_API_TIMEOUT);
        if (offsetMap.containsKey(topicPartition)) {
          latestOffset = offsetMap.get(topicPartition);
        } else {
          throw new VeniceException("offset result returned from endOffsets does not contain entry: " + topicPartition);
        }
      } catch (Exception ex) {
        if (ex instanceof org.apache.kafka.common.errors.TimeoutException) {
          throw new VeniceOperationAgainstKafkaTimedOut(
              "Timeout exception when seeking to end to get latest offset" + " for topic: " + topic + " and partition: "
                  + partition, ex);
        } else {
          throw ex;
        }
      }
      return latestOffset;
    }
  }

  @Override
  public long getLatestOffset(String topic, int partition) throws TopicDoesNotExistException {
    return getLatestOffset(topic, partition, true);
  }

  @Override
  public long getLatestOffsetAndRetry(String topic, int partition, int retries) {
    if (retries < 1) {
      throw new IllegalArgumentException("Invalid retries. Got: " + retries);
    }
    int attempt = 0;
    long offset;
    VeniceOperationAgainstKafkaTimedOut lastException = new VeniceOperationAgainstKafkaTimedOut("This exception should not be thrown");
    while (attempt < retries) {
      try {
        return getLatestOffset(topic, partition);
      } catch (VeniceOperationAgainstKafkaTimedOut e){ // topic and partition is listed in the exception object
        LOGGER.warn("Failed to get latest offset.  Retries remaining: " + (retries - attempt), e);
        lastException = e;
        attempt ++;
      }
    }
    throw lastException;
  }

  @Override
  public Map<Integer, Long> getOffsetsByTime(String topic, Map<TopicPartition, Long> timestampsToSearch, long timestamp) {
    try (AutoCloseableLock ignore = new AutoCloseableLock(rawConsumerLock)) {
      final int expectedPartitionNum = timestampsToSearch.size();
      Map<Integer, Long> result = kafkaRawBytesConsumer.get().offsetsForTimes(timestampsToSearch)
          .entrySet()
          .stream()
          .collect(Collectors.toMap(partitionToOffset -> Utils.notNull(partitionToOffset.getKey(),
              "Got a null TopicPartition key out of the offsetsForTime API").partition(), partitionToOffset -> {
            Optional<Long> offsetOptional = Optional.ofNullable(partitionToOffset.getValue()).map(offset -> offset.offset());
            if (offsetOptional.isPresent()) {
              return offsetOptional.get();
            } else {
              return getOffsetByTimeIfOutOfRange(partitionToOffset.getKey(), timestamp);
            }
          }));
      // The given timestamp exceed the timestamp of the last message. So return the last offset.
      if (result.isEmpty()) {
        LOGGER.warn("Offsets result is empty. Will complement with the last offsets.");
        result = kafkaRawBytesConsumer.get().endOffsets(timestampsToSearch.keySet())
            .entrySet()
            .stream()
            .collect(Collectors.toMap(partitionToOffset -> Utils.notNull(partitionToOffset).getKey().partition(),
                partitionToOffset -> partitionToOffset.getValue()));
      } else if (result.size() != expectedPartitionNum) {
        Map<TopicPartition, Long> endOffsets = kafkaRawBytesConsumer.get().endOffsets(timestampsToSearch.keySet());
        // Get partial offsets result.
        LOGGER.warn("Missing offsets for some partitions. Partition Number should be :" + expectedPartitionNum + " but only got: " + result.size()
            + ". Will complement with the last offsets.");

        for (TopicPartition topicPartition : timestampsToSearch.keySet()) {
          int partitionId = topicPartition.partition();
          if (!result.containsKey(partitionId)) {
            result.put(partitionId, endOffsets.get(new TopicPartition(topic, partitionId)));
          }
        }
      }
      if (result.size() < expectedPartitionNum) {
        throw new VeniceException(
            "Failed to get offsets for all partitions. Got offsets for " + result.size() + " partitions, should be: " + expectedPartitionNum);
      }
      return result;
    }
  }

  @Override
  public Map<Integer, Long> getOffsetsByTime(String topic, long timestamp) {
    try (AutoCloseableLock ignore = new AutoCloseableLock(rawConsumerLock)) {
      int remainingAttempts = 5;
      List<PartitionInfo> partitionInfoList = kafkaRawBytesConsumer.get().partitionsFor(topic);
      // N.B.: During unit test development, getting a null happened occasionally without apparent
      //       reason. In their current state, the tests have been run with a high invocationCount and
      //       no failures, so it may be a non-issue. If this happens again, and we find some test case
      //       that can reproduce it, we may want to try adding a short amount of retries, and reporting
      //       a bug to Kafka.
      while (remainingAttempts > 0 && (null == partitionInfoList || partitionInfoList.isEmpty())) {
        Utils.sleep(500);
        partitionInfoList = kafkaRawBytesConsumer.get().partitionsFor(topic);
        remainingAttempts -= 1;
      }
      if (null == partitionInfoList || partitionInfoList.isEmpty()) {
        throw new VeniceException("Cannot get partition info for topic: " + topic + ", partitionInfoList: " + partitionInfoList);
      } else {
        Map<TopicPartition, Long> timestampsToSearch = partitionInfoList.stream()
            .collect(Collectors.toMap(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()),
                ignoredParam -> timestamp));
        return getOffsetsByTime(topic, timestampsToSearch, timestamp);
      }
    }
  }

  @Override
  public long getOffsetByTime(String topic, int partition, long timestamp) {
    return getOffsetsByTime(
        topic,
        Collections.singletonMap(new TopicPartition(topic, partition), timestamp),
        timestamp
    ).get(partition);
  }

  @Override
  public long getLatestProducerTimestampAndRetry(String topic, int partition, int retries) {
    if (retries < 1) {
      throw new IllegalArgumentException("Invalid retries. Got: " + retries);
    }
    int attempt = 0;
    long timestamp;
    VeniceOperationAgainstKafkaTimedOut lastException = new VeniceOperationAgainstKafkaTimedOut("This exception should not be thrown");
    while (attempt < retries){
      try {
        timestamp = getLatestProducerTimestamp(topic, partition);
        return timestamp;
      } catch (VeniceOperationAgainstKafkaTimedOut e){// topic and partition is listed in the exception object
        LOGGER.warn("Failed to get latest offset.  Retries remaining: " + (retries - attempt), e);
        lastException = e;
        attempt ++;
      }
    }
    throw lastException;
  }

  /**
   * If the topic is empty or all the messages are truncated (startOffset==endOffset), return -1;
   * otherwise, return the producer timestamp of the last message in the selected partition of a topic
   */
  private long getLatestProducerTimestamp(String topic, int partition) throws TopicDoesNotExistException {
    if (partition < 0) {
      throw new IllegalArgumentException(
          "Cannot retrieve latest producer timestamp for invalid partition " + partition + " topic " + topic);
    }
    try (AutoCloseableLock ignore = new AutoCloseableLock(kafkaRecordConsumerLock)) {
      if (!kafkaAdminWrapper.get().containsTopic(topic)) {
        throw new TopicDoesNotExistException("Topic " + topic + " does not exist!");
      }
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      long latestProducerTimestamp;
      try {
        Map<TopicPartition, Long> offsetByTopicPartition = kafkaRecordConsumer.get().endOffsets(Collections.singletonList(topicPartition), DEFAULT_KAFKA_OFFSET_API_TIMEOUT);
        if (offsetByTopicPartition == null || !offsetByTopicPartition.containsKey(topicPartition)) {
          throw new VeniceException("Got no results of finding end offsets for topic partition: " + topicPartition);
        }
        final long latestOffset = offsetByTopicPartition.get(topicPartition);

        if (latestOffset <= 0) {
          // empty topic
          latestProducerTimestamp = -1;
        } else {
          Long earliestOffset = kafkaRecordConsumer.get()
              .beginningOffsets(Collections.singletonList(topicPartition), DEFAULT_KAFKA_OFFSET_API_TIMEOUT)
              .get(topicPartition);
          if (earliestOffset == null) {
            throw new VeniceException("Got no results of finding the earliest offset for topic partition: " + topicPartition);
          }
          if (earliestOffset == latestOffset) {
            // empty topic
            latestProducerTimestamp = -1;
          } else {
            // poll the last message and retrieve the producer timestamp
            kafkaRecordConsumer.get().seek(topicPartition, latestOffset - 1);
            ConsumerRecords records = ConsumerRecords.EMPTY;
            int attempts = 0;
            while (attempts++ < KAFKA_POLLING_RETRY_ATTEMPT && records.isEmpty()) {
              LOGGER.info("Trying to get the last record from topic: " + topicPartition.toString() + " at offset: " + (
                  latestOffset - 1) + ". Attempt#" + attempts + "/" + KAFKA_POLLING_RETRY_ATTEMPT);
              records = kafkaRecordConsumer.get().poll(kafkaOperationTimeout);
            }
            if (records.isEmpty()) {
              /**
               * Failed the job if we cannot get the last offset of the topic.
               */
              String errorMsg = "Failed to get the last record from topic: " + topicPartition.toString() + " after " + KAFKA_POLLING_RETRY_ATTEMPT + " attempts";
              LOGGER.error(errorMsg);
              throw new VeniceException(errorMsg);
            }

            // Get the latest record from the poll result
            ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record = (ConsumerRecord<KafkaKey, KafkaMessageEnvelope>) records.iterator().next();
            latestProducerTimestamp = record.value().producerMetadata.messageTimestamp;
          }
        }
      } catch (org.apache.kafka.common.errors.TimeoutException ex) {
        throw new VeniceOperationAgainstKafkaTimedOut(
            "Timeout exception when seeking to end to get latest offset" + " for topic: " + topic + " and partition: " + partition, ex);
      } finally {
        kafkaRecordConsumer.get().assign(Collections.emptyList());
      }
      return latestProducerTimestamp;
    }
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    try (AutoCloseableLock ignore = new AutoCloseableLock(rawConsumerLock)) {
      return kafkaRawBytesConsumer.get().partitionsFor(topic);
    }
  }

  @Override
  public long getOffsetByTimeIfOutOfRange(TopicPartition topicPartition, long timestamp) throws TopicDoesNotExistException {
    try (AutoCloseableLock ignore = new AutoCloseableLock(rawConsumerLock)) {
      long latestOffset = getLatestOffset(topicPartition.topic(), topicPartition.partition(), true);
      if (latestOffset <= 0) {
        long nextOffset = LOWEST_OFFSET + 1;
        LOGGER.info("End offset for topic " + topicPartition + " is " + latestOffset + "; return offset " + nextOffset);
        return nextOffset;
      }

      long earliestOffset = getEarliestOffset(topicPartition.topic(), topicPartition.partition());
      if (earliestOffset == latestOffset) {
        /**
         * This topic/partition is empty or retention delete the entire partition
         */
        LOGGER.info("Both beginning offset and end offset is " + latestOffset + " for topic " + topicPartition + "; it's empty; return offset " + latestOffset);
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
        while (attempts++ < KAFKA_POLLING_RETRY_ATTEMPT && records.isEmpty()) {
          LOGGER.info("Trying to get the last record from topic: " + topicPartition.toString() + " at offset: " + (latestOffset
              - 1) + ". Attempt#" + attempts + "/" + KAFKA_POLLING_RETRY_ATTEMPT);
          records = kafkaRawBytesConsumer.get().poll(kafkaOperationTimeout);
        }
        if (records.isEmpty()) {
          /**
           * Failed the job if we cannot get the last offset of the topic.
           */
          String errorMsg = "Failed to get the last record from topic: " + topicPartition.toString() + " after " + KAFKA_POLLING_RETRY_ATTEMPT + " attempts";
          LOGGER.error(errorMsg);
          throw new VeniceException(errorMsg);
        }

        // Get the latest record from the poll result
        ConsumerRecord record = (ConsumerRecord) records.iterator().next();

        if (timestamp <= record.timestamp()) {
          /**
           * There could be a race condition in this function:
           * 1. In function: {@link #getOffsetsByTime}, {@link KafkaConsumer#offsetsForTimes} is invoked.
           * 2. The asked timestamp is out of range.
           * 3. Some messages get produced to the topic.
           * 4. {@link #getOffsetByTimeIfOutOfRange} gets invoked, and it realizes that the latest message's timestamp
           * is higher than the seeking timestamp.
           *
           * In this case, we should call {@link #getOffsetByTime} again, since the seeking timestamp will be in the range
           * instead of returning the earliest offset.
           */
          Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
          timestampsToSearch.put(topicPartition, timestamp);
          Map<TopicPartition, OffsetAndTimestamp> result = kafkaRawBytesConsumer.get().offsetsForTimes(timestampsToSearch);
          if (result.containsKey(topicPartition) && result.get(topicPartition) != null) {
            OffsetAndTimestamp offsetAndTimestamp = result.get(topicPartition);
            long resultOffset = offsetAndTimestamp.offset();
            LOGGER.info("Successfully return offset: " + resultOffset + " for topic: " + topicPartition.toString() + " for timestamp: " + timestamp);
            return resultOffset;
          }
        }

        /**
         * 1. If the required timestamp is bigger than the timestamp of last record, return the offset after the last record.
         * 2. Otherwise, return earlier offset to consume from the beginning.
         */
        long resultOffset = (timestamp > record.timestamp()) ? latestOffset : earliestOffset;
        LOGGER.info("Successfully return offset: " + resultOffset + " for topic: " + topicPartition.toString() + " for timestamp: " + timestamp);
        return resultOffset;
      } finally {
        kafkaRawBytesConsumer.get().assign(Collections.emptyList());
      }
    }
  }

  /**
   * Return the beginning offset of a topic/partition. Synchronized because it calls #getConsumer()
   *
   * @throws TopicDoesNotExistException
   */
  private long getEarliestOffset(String topic, int partition) throws TopicDoesNotExistException {
    try (AutoCloseableLock ignore = new AutoCloseableLock(rawConsumerLock)) {
      if (!kafkaAdminWrapper.get().containsTopic(topic)) {
        throw new TopicDoesNotExistException("Topic " + topic + " does not exist!");
      }
      if (partition < 0) {
        throw new IllegalArgumentException("Cannot retrieve latest offsets for invalid partition " + partition);
      }
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      long earliestOffset;
      try {
        Map<TopicPartition, Long> offsetMap =
            kafkaRawBytesConsumer.get().beginningOffsets(Collections.singletonList(topicPartition), DEFAULT_KAFKA_OFFSET_API_TIMEOUT);
        if (offsetMap.containsKey(topicPartition)) {
          earliestOffset = offsetMap.get(topicPartition);
        } else {
          throw new VeniceException("offset result returned from beginningOffsets does not contain entry: " + topicPartition);
        }
      } catch (Exception ex) {
        if (ex instanceof org.apache.kafka.common.errors.TimeoutException) {
          throw new VeniceOperationAgainstKafkaTimedOut(
              "Timeout exception when seeking to beginning to get earliest offset" + " for topic: " + topic + " and partition: " + partition, ex);
        } else {
          throw ex;
        }
      }
      return earliestOffset;
    }
  }

  @Override
  public void close() {
    kafkaRawBytesConsumer.ifPresent(IOUtils::closeQuietly);
    kafkaRecordConsumer.ifPresent(IOUtils::closeQuietly);
    kafkaAdminWrapper.ifPresent(IOUtils::closeQuietly);
  }
}
