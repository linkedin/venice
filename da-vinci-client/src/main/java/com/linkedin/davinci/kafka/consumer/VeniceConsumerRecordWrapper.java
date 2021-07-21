package com.linkedin.davinci.kafka.consumer;

import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;


/**
 * This is a wrapper class to contain the kafka server URL where a kafka ConsumerRecord was consumed from.
 * Ingestion task related classes should work with this object to get this extra context for the kafka record.
 */
public final class VeniceConsumerRecordWrapper<K,V> {

  private final String kafkaUrl;
  private final ConsumerRecord<K, V> consumerRecord;

  public VeniceConsumerRecordWrapper(ConsumerRecord<K,V> consumerRecord) {
    this(null, consumerRecord);
  }

  public VeniceConsumerRecordWrapper(String kafkaUrl, ConsumerRecord<K,V> consumerRecord) {
    this.kafkaUrl = kafkaUrl;
    this.consumerRecord = consumerRecord;
  }

  /**
   * The kafka server URL where this record is received from.
   */
  public String kafkaUrl() {
    return this.kafkaUrl;
  }

  /**
   * Return the underlying ConsumerRecord.
   */
  public ConsumerRecord<K, V> consumerRecord() {
    return consumerRecord;
  }

}
