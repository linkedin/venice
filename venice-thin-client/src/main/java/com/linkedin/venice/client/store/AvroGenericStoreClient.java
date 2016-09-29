package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;

import java.io.Closeable;
import java.util.concurrent.Future;

/**
 * Venice avro generic client to communicate with Venice backend for key-value lookup.
 *
 * @param <V>
 */
public interface AvroGenericStoreClient<V> extends Closeable {

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
  Future<V> get(Object key) throws VeniceClientException;

  void start() throws VeniceClientException;

  /**
   * Release the internal resources.
   */
  @Override
  void close(); /* removes exception that Closeable can throw */
}
