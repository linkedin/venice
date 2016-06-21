package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.offsets.OffsetRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface VeniceConsumer<K, V> {
  void subscribe(String topic, int partition, OffsetRecord offset);

  void unSubscribe(String topic, int partition);

  void resetOffset(String topic, int partition);

  void close();

  ConsumerRecords<K, V> poll(long timeout);
}
