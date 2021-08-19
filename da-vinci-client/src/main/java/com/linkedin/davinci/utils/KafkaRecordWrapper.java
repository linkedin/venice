package com.linkedin.davinci.utils;

import com.linkedin.davinci.kafka.consumer.VeniceConsumerRecordWrapper;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import java.util.LinkedList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;


/**
 * This is a simple utility class to wrap a {@link ConsumerRecord} into {@link VeniceConsumerRecordWrapper}
 */

public class KafkaRecordWrapper {
  public static VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope> wrap(String kafkaUrl, ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord, int amplificationFactor) {
    return new VeniceConsumerRecordWrapper<>(kafkaUrl, consumerRecord, amplificationFactor);
  }

  public static Iterable<VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope>> wrap(String kafkaUrl, Iterable<ConsumerRecord<KafkaKey, KafkaMessageEnvelope>> records, int amplificationFactor) {
    List<VeniceConsumerRecordWrapper<KafkaKey, KafkaMessageEnvelope>> veniceRecords = new LinkedList<>();
    for (ConsumerRecord<KafkaKey, KafkaMessageEnvelope> consumerRecord : records) {
      veniceRecords.add(wrap(kafkaUrl, consumerRecord, amplificationFactor));
    }
    return veniceRecords;
  }
}
