package com.linkedin.venice.writer;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.RecordMetadata;


public interface KafkaProducerWrapper {
  int getNumberOfPartitions(String topic);
  Future<RecordMetadata> sendMessage(String topic, KafkaKey key, KafkaMessageEnvelope value, int partition);
  void close(int closeTimeOutMs);
}
