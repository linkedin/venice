package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface KafkaConsumerWrapper extends AutoCloseable {
  void subscribe(String topic, int partition, OffsetRecord offset);

  void unSubscribe(String topic, int partition);

  void resetOffset(String topic, int partition) throws UnsubscribedTopicPartitionException;

  void close();

  ConsumerRecords<KafkaKey, KafkaMessageEnvelope> poll(long timeout);
}
