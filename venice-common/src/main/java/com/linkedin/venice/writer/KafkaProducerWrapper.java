package com.linkedin.venice.writer;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


public interface KafkaProducerWrapper {
  int getNumberOfPartitions(String topic);
  Future<RecordMetadata> sendMessage(String topic, KafkaKey key, KafkaMessageEnvelope value, int partition, Callback callback);
  Future<RecordMetadata> sendMessage(ProducerRecord<KafkaKey, KafkaMessageEnvelope> record, Callback callback);
  void flush();
  void close(int closeTimeOutMs);
  Map<String, Double> getMeasurableProducerMetrics();
  String getBrokerLeaderHostname(String topic, int partition);
}
