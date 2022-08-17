package com.linkedin.venice.client.consumer;

import java.util.Iterator;


/**
 * A wrapper for a batch of Venice records.
 *
 * Analogous to the {@link org.apache.kafka.clients.consumer.ConsumerRecords} class, but containing only the more
 * useful APIs which map directly to concepts compatible with Venice. If additional APIs from the Kafka class are
 * desired, they can be requested and considered on a case-by-base basis.
 */
public interface VeniceConsumerRecords<K, V> {
  /**
   * @return An iterator for the records contained in this batch.
   *
   * N.B.: Venice control messages are not returned by this iterator.
   */
  Iterator<VeniceConsumerRecord<K, V>> iterator();

  /**
   * @return The count of records in this batch.
   *
   * N.B.: Venice control messages and other internal details are excluded from this count.
   */
  int count();

  /**
   * @return whether the batch is empty.
   */
  boolean isEmpty();
}
