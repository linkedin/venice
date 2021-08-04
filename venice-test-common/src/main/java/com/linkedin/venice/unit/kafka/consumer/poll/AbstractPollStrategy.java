package com.linkedin.venice.unit.kafka.consumer.poll;

import com.linkedin.venice.controller.kafka.AdminTopicUtils;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.unit.kafka.InMemoryKafkaBroker;
import com.linkedin.venice.unit.kafka.InMemoryKafkaMessage;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;

/**
 * A base class which encapsulates the common plumbing needed by all {@link PollStrategy} implementations.
 */
public abstract class AbstractPollStrategy implements PollStrategy {

  private static final int DEFAULT_MAX_MESSAGES_PER_POLL = 3; // We can make this configurable later on if need be...
  private final int maxMessagePerPoll;
  protected final boolean keepPollingWhenEmpty;
  protected final Set<TopicPartition> drainedPartitions = new HashSet<>();

  public AbstractPollStrategy(boolean keepPollingWhenEmpty) {
    this(keepPollingWhenEmpty, DEFAULT_MAX_MESSAGES_PER_POLL);
  }

  public AbstractPollStrategy(boolean keepPollingWhenEmpty, int maxMessagePerPoll){
    this.keepPollingWhenEmpty = keepPollingWhenEmpty;
    this.maxMessagePerPoll = maxMessagePerPoll;
  }

  protected abstract Pair<TopicPartition, Long> getNextPoll(Map<TopicPartition, Long> offsets);

  public synchronized ConsumerRecords poll(InMemoryKafkaBroker broker, Map<TopicPartition, Long> offsets, long timeout) {
    drainedPartitions.stream().forEach(topicPartition -> offsets.remove(topicPartition));

    Map<TopicPartition, List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>>> records = new HashMap<>();

    long startTime = System.currentTimeMillis();
    int numberOfRecords = 0;

    while (numberOfRecords < maxMessagePerPoll && System.currentTimeMillis() < startTime + timeout) {
      Pair<TopicPartition, Long> nextPoll = getNextPoll(offsets);
      if (null == nextPoll) {
        if (keepPollingWhenEmpty) {
          continue;
        } else {
          break;
        }
      }

      TopicPartition topicPartition = nextPoll.getFirst();
      long offset = nextPoll.getSecond();
      String topic = topicPartition.topic();
      int partition = topicPartition.partition();
      /**
       * TODO: need to understand why "+ 1" here, since for {@link ArbitraryOrderingPollStrategy}, it always
       * returns the next message specified in the delivery order, which is causing confusion.
        */
      long nextOffset = offset + 1;
      Optional<InMemoryKafkaMessage> message = broker.consume(topic, partition, nextOffset);
      if (message.isPresent()) {
        if (! AdminTopicUtils.isAdminTopic(topic)) {
          /**
           * Skip putValue adjustment since admin consumer is still using {@link com.linkedin.venice.serialization.avro.KafkaValueSerializer}.
           */
          KafkaMessageEnvelope kafkaMessageEnvelope = message.get().value;
          if (MessageType.valueOf(kafkaMessageEnvelope) == MessageType.PUT && !message.get().isPutValueChanged()) {
            /**
             * This is used to simulate the deserialization in {@link com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer}
             * to leave some room in {@link Put#putValue} byte buffer.
             */
            Put put = (Put) kafkaMessageEnvelope.payloadUnion;
            put.putValue = ByteUtils.enlargeByteBufferForIntHeader(put.putValue);
            message.get().putValueChanged();
          }
        }

        ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord = new ConsumerRecord<>(
            topic,
            partition,
            nextOffset,
            System.currentTimeMillis(),
            TimestampType.NO_TIMESTAMP_TYPE,
            -1, // checksum
            -1, // serializedKeySize
            -1, // serializedValueSize
            message.get().key,
            message.get().value);
        if (!records.containsKey(topicPartition)) {
          records.put(topicPartition, new ArrayList<>());
        }
        records.get(topicPartition).add(consumerRecord);
        incrementOffset(offsets, topicPartition, offset);
        numberOfRecords++;
      } else if (keepPollingWhenEmpty) {
        continue;
      } else {
        drainedPartitions.add(topicPartition);
        offsets.remove(topicPartition);
        continue;
      }
    }

    return new ConsumerRecords(records);
  }

  protected void incrementOffset(Map<TopicPartition, Long> offsets, TopicPartition topicPartition, long offset) {
    offsets.put(topicPartition, offset + 1);
  }
}
