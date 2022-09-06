package com.linkedin.venice.client.consumer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * A consumer which can subscribe to the changes happening in a Venice store.
 *
 * Analogous to the {@link org.apache.kafka.clients.consumer.Consumer} interface, but containing only the more
 * useful APIs which map directly to concepts compatible with Venice. If additional APIs from the Kafka class are
 * desired, they can be requested and considered on a case-by-base basis.
 */
public abstract class VeniceStoreConsumer<K, V> {
  protected final Properties properties;

  /**
   * Sub-classes must provide this constructor, in order for the user to specify their consumer group, store name, etc.
   */
  protected VeniceStoreConsumer(Properties properties) {
    this.properties = properties;
  }

  public abstract VeniceConsumerRecords<K, V> poll(long timeout);

  /**
   * Commit offsets returned on the last {@link #poll(long)} for all subscribed partitions.
   *
   * This is a synchronous commits and will block until either the commit succeeds or an unrecoverable error is
   * encountered (in which case it is thrown to the caller).
   */
  public void commitSync() {
  }

  /**
   * Close the consumer, waiting for up to the default timeout of 30 seconds for any needed cleanup.
   * If auto-commit is enabled, this will commit the current offsets if possible within the default
   * timeout. See {@link #close(long, TimeUnit)} for details.
   */
  public void close() {
  }

  /**
   * Tries to close the consumer cleanly within the specified timeout. This method waits up to
   * {@param timeout} for the consumer to complete pending commits and leave the group.
   * If auto-commit is enabled, this will commit the current offsets if possible within the
   * timeout. If the consumer is unable to complete offset commits and gracefully leave the group
   * before the timeout expires, the consumer is force closed.
   */
  public void close(long timeout, TimeUnit unit) {
  }
}
