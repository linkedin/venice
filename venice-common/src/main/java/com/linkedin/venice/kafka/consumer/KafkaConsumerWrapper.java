package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

public interface KafkaConsumerWrapper {
  void subscribe(String topic, int partition, OffsetRecord offset);

  void unSubscribe(String topic, int partition);

  void resetOffset(String topic, int partition);

  void close();

  ConsumerRecords<KafkaKey, KafkaMessageEnvelope> poll(long timeout);

  void commitSync(String topic, int partition, OffsetAndMetadata offsetAndMeta);

  OffsetAndMetadata committed(String topic, int partition);
}
