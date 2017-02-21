package com.linkedin.venice.unit.kafka.consumer.poll;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.unit.kafka.InMemoryKafkaBroker;
import com.linkedin.venice.unit.kafka.InMemoryKafkaMessage;
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

  protected abstract Pair<TopicPartition, OffsetRecord> getNextPoll(Map<TopicPartition, OffsetRecord> offsets);

  public synchronized ConsumerRecords poll(InMemoryKafkaBroker broker, Map<TopicPartition, OffsetRecord> offsets, long timeout) {
    drainedPartitions.stream().forEach(topicPartition -> offsets.remove(topicPartition));

    Map<TopicPartition, List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>>> records = new HashMap<>();

    long startTime = System.currentTimeMillis();
    int numberOfRecords = 0;

    while (numberOfRecords < maxMessagePerPoll && System.currentTimeMillis() < startTime + timeout) {
      Pair<TopicPartition, OffsetRecord> nextPoll = getNextPoll(offsets);
      if (null == nextPoll) {
        if (keepPollingWhenEmpty) {
          continue;
        } else {
          break;
        }
      }

      TopicPartition topicPartition = nextPoll.getFirst();
      OffsetRecord offsetRecord = nextPoll.getSecond();

      String topic = topicPartition.topic();
      int partition = topicPartition.partition();
      long nextOffset = offsetRecord.getOffset() + 1;
      Optional<InMemoryKafkaMessage> message = broker.consume(topic, partition, nextOffset);
      if (message.isPresent()) {
        ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord = new ConsumerRecord<>(
            topic,
            partition,
            nextOffset,
            offsetRecord.getEventTimeEpochMs(),
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
        incrementOffset(offsets, topicPartition, offsetRecord);
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

  protected void incrementOffset(Map<TopicPartition, OffsetRecord> offsets, TopicPartition topicPartition, OffsetRecord offsetRecord) {
    // Doing a deep copy, otherwise Mockito keeps a handle on the reference only, which can mutate and lead to confusing verify() semantics
    OffsetRecord newOffsetRecord = new OffsetRecord(offsetRecord.toBytes());
    newOffsetRecord.setOffset(offsetRecord.getOffset() + 1);
    offsets.put(topicPartition, newOffsetRecord);
  }
}
