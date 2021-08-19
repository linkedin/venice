package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.utils.PartitionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;


/**
 * This is a wrapper class to contain the kafka server URL where a kafka ConsumerRecord was consumed from.
 * Ingestion task related classes should work with this object to get this extra context for the kafka record.
 */
public final class VeniceConsumerRecordWrapper<K,V> {

  private final String kafkaUrl;
  private final ConsumerRecord<K, V> consumerRecord;
  private int subPartition;

  public VeniceConsumerRecordWrapper(ConsumerRecord<K,V> consumerRecord) {
    this(null, consumerRecord);
  }

  public VeniceConsumerRecordWrapper(String kafkaUrl, ConsumerRecord<K,V> consumerRecord) {
    this(kafkaUrl, consumerRecord, 1);
  }

  public VeniceConsumerRecordWrapper(String kafkaUrl, ConsumerRecord<K,V> consumerRecord, int amplificationFactor) {
    this.kafkaUrl = kafkaUrl;
    this.consumerRecord = consumerRecord;
    this.subPartition = PartitionUtils.getSubPartition(consumerRecord.topic(), consumerRecord.partition(), amplificationFactor);
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

  /**
   * Return the SubPartition of the consumerRecord.
   * If the consumer record's topic is RT, it's coming from leader consumption and we will multiply it with AMP factor.
   * If topic is VT, then the record's partition id is already correct, we will simply return it.
   */
  public int getSubPartition() {
    return subPartition;
  }
}
