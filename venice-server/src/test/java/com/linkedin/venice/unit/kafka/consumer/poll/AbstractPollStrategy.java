package com.linkedin.venice.unit.kafka.consumer.poll;

import com.google.common.collect.Maps;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.unit.kafka.InMemoryKafkaBroker;
import com.linkedin.venice.unit.kafka.InMemoryKafkaMessage;
import com.linkedin.venice.utils.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;

/**
 * A base class which encapsulates the common plumbing needed by all {@link PollStrategy} implementations.
 */
public abstract class AbstractPollStrategy implements PollStrategy {

  private static final int MAX_MESSAGES_PER_POLL = 3; // We can make this configurable later on if need be...
  protected final boolean keepPollingWhenEmpty;

  public AbstractPollStrategy(boolean keepPollingWhenEmpty) {
    this.keepPollingWhenEmpty = keepPollingWhenEmpty;
  }

  protected abstract Pair<TopicPartition, OffsetRecord> getNextPoll(Map<TopicPartition, OffsetRecord> offsets);

  public synchronized ConsumerRecords poll(InMemoryKafkaBroker broker, Map<TopicPartition, OffsetRecord> offsets, long timeout) {
    Map<TopicPartition, List<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>>> records = Maps.newHashMap();

    long startTime = System.currentTimeMillis();
    int numberOfRecords = 0;

    while (numberOfRecords < MAX_MESSAGES_PER_POLL && System.currentTimeMillis() < startTime + timeout) {
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

      try {
        String topic = topicPartition.topic();
        int partition = topicPartition.partition();
        long nextOffset = offsetRecord.getOffset() + 1;
        InMemoryKafkaMessage message = broker.consume(topic, partition, nextOffset);
        ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord = new ConsumerRecord<>(
            topic,
            partition,
            nextOffset,
            offsetRecord.getEventTimeEpochMs(),
            TimestampType.NO_TIMESTAMP_TYPE,
            message.key,
            message.value);
        if (!records.containsKey(topicPartition)) {
          records.put(topicPartition, new ArrayList<>());
        }
        records.get(topicPartition).add(consumerRecord);
        incrementOffset(offsets, topicPartition, offsetRecord);
        numberOfRecords++;
      } catch (IllegalArgumentException e) {
        if (keepPollingWhenEmpty) {
          continue;
        } else {
          break;
        }
      }
    }

    return new ConsumerRecords(records);
  }

  protected void incrementOffset(Map<TopicPartition, OffsetRecord> offsets, TopicPartition topicPartition, OffsetRecord offsetRecord) {
    offsets.put(topicPartition, new OffsetRecord(offsetRecord.getOffset() + 1));
  }
}
