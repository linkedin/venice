package com.linkedin.venice.client.store;

import com.linkedin.venice.annotation.Experimental;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.StoreSchemaFetcher;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import java.io.Closeable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;


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
   * Similar to {@link #get(Object)} except that it allows passing in a
   * {@param reusedValue} instance, to minimize GC.
   */
  default CompletableFuture<V> get(K key, V reusedValue) throws VeniceClientException {
    return get(key);
  }

  /**
   * Get the values associated with the given keys and return them in a map of keys to values.
   * Note that the returned map will only contain entries for the keys which have a value associated
   * with them.
   * @param keys
   * @return
   * @throws VeniceClientException
   */
  default CompletableFuture<Map<K, V>> batchGet(Set<K> keys) throws VeniceClientException {
    throw new VeniceClientException(
        "Please use CachingVeniceStoreClientFactory#getAndStartAvroGenericStoreClient() "
            + "or VeniceGenericStoreClientFactory#createInstance() to generate a Venice avro generic client");
  }

  // TODO: Once batchGet() defaults to start using streamingBatchGet(), need to rethink whether
  // streamingBatchGet() should be exposed
  /**
   * Get the values associated with the given keys and return them in a map of keys to values.
   *
   * When time-out happens for the following invocation:
   * {@code streamingBatchGet(keys).get(waitingTime, unit); }
   * This function will return the available response instead of throwing a {@link java.util.concurrent.TimeoutException}.
   * It means this function could return either full response or partial response.
   *
   * @param keys
   * @return
   * @throws VeniceClientException
   */
  default CompletableFuture<VeniceResponseMap<K, V>> streamingBatchGet(Set<K> keys) throws VeniceClientException {
    throw new VeniceClientException(
        "Please use CachingVeniceStoreClientFactory#getAndStartAvroGenericStoreClient() "
            + "or VeniceGenericStoreClientFactory#createInstance() to generate a Venice avro generic client");
  }

  /**
   * Streaming interface for {@link #batchGet(Set)}.
   * You can find more info in {@link StreamingCallback}.
   *
   * @param keys
   * @param callback
   * @throws VeniceClientException
   */
  default void streamingBatchGet(Set<K> keys, StreamingCallback<K, V> callback) throws VeniceClientException {
    throw new VeniceClientException(
        "Please use CachingVeniceStoreClientFactory#getAndStartAvroGenericStoreClient() "
            + "or VeniceGenericStoreClientFactory#createInstance() to generate a Venice avro generic client");
  }

  /**
   * This experimental feature is subject to backwards-incompatible changes and may even be removed in the future.
   * @return
   */
  @Experimental
  default ComputeRequestBuilder<K> compute() {
    throw new VeniceClientException(
        "Please use CachingVeniceStoreClientFactory#getAndStartAvroGenericStoreClient() "
            + "or VeniceGenericStoreClientFactory#createInstance() to generate a Venice avro generic client");
  }

  void start() throws VeniceClientException;

  /**
   * Release the internal resources.
   */
  @Override
  void close(); /* removes exception that Closeable can throw */

  String getStoreName();

  /**
   * Get key schema.
   * @deprecated This method is considered deprecated. Please use {@link StoreSchemaFetcher#getKeySchema()} to fetch
   * key schema instead.
   */
  @Deprecated
  Schema getKeySchema();

  /**
   * Get the latest value schema known in current store client.
   * This function doesn't guarantee it will return the latest schema if you add a new value schema
   * when current store client is running.
   * @deprecated This method is considered deprecated. Please use {@link StoreSchemaFetcher#getLatestValueSchema()} to fetch
   * latest value schema instead.
   */
  @Deprecated
  Schema getLatestValueSchema();
}
