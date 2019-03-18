package com.linkedin.venice.client.consumer;

/**
 * A wrapper for a single Venice record.
 *
 * Analogous to the {@link org.apache.kafka.clients.consumer.ConsumerRecord} class, but containing only the more
 * useful APIs which map directly to concepts compatible with Venice. If additional APIs from the Kafka class are
 * desired, they can be requested and considered on a case-by-base basis.
 */
public interface VeniceConsumerRecord<K, V> {
  /**
   * @return the key associated with this record.
   *
   * N.B.: Always available in every usage mode.
   */
  K key();

  /**
   * @return the value associated with this key, or null if the record is deleted
   *
   * @throws VeniceConsumerException if the consumer has been configured to be a key-only consumer.
   */
  V value();

  /**
   * @return the previous value associated with this key, or null if there was no previous value for the key.
   *
   * @throws VeniceConsumerException if the consumer has been configured to be a key-only or key-value consumer,
   *                                 or if the store is not configured to emit previous values.
   */
  V previousValue();
}
