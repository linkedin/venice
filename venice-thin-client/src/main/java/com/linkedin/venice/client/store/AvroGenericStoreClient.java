package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Venice avro generic client to communicate with Venice backend for key-value lookup.
 *
 * @param <V>
 */
public interface AvroGenericStoreClient<K, V> extends Closeable {

  /**
   * Lookup the value by given key, and get(key).get() will return null if it doesn't exist.
   *
   * For now, if any backend error/exception happens,
   * get(Object key).get() will throw {@link java.util.concurrent.ExecutionException},
   * which is a wrapper of the real exception.
   **
   * @param key
   * @return
   * @throws VeniceClientException
   */
  CompletableFuture<V> get(K key) throws VeniceClientException;

  /**
   * Get the values associated with the given keys and return them in a map of keys to values.
   * Note that the returned map will only contain entries for the keys which have a value associated
   * with them.
   * @param keys
   * @return
   * @throws VeniceClientException
   */
  CompletableFuture<Map<K, V>> multiGet(Set<K> keys) throws VeniceClientException;

  void start() throws VeniceClientException;

  /**
   * Release the internal resources.
   */
  @Override
  void close(); /* removes exception that Closeable can throw */

  String getStoreName();
}
