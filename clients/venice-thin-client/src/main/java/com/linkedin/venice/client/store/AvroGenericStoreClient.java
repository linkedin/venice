package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.StoreSchemaFetcher;
import com.linkedin.venice.client.store.listeners.StoreConfigChangeListener;
import com.linkedin.venice.client.store.listeners.StoreVersionSwitchListener;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.VeniceResponseCompletableFuture;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import com.linkedin.venice.client.store.streaming.VeniceResponseMapImpl;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
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
    CompletableFuture<Map<K, V>> resultFuture = new CompletableFuture<>();
    CompletableFuture<VeniceResponseMap<K, V>> streamingResultFuture = streamingBatchGet(keys);

    streamingResultFuture.whenComplete((response, throwable) -> {
      if (throwable != null) {
        resultFuture.completeExceptionally(throwable);
      } else if (!response.isFullResponse()) {
        resultFuture.completeExceptionally(
            new VeniceClientException(
                "Received partial response, returned entry count: " + response.getTotalEntryCount()
                    + ", and key count: " + keys.size()));
      } else {
        resultFuture.complete(response);
      }
    });
    return resultFuture;
  }

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
    Map<K, V> resultMap = new VeniceConcurrentHashMap<>(keys.size());
    Queue<K> nonExistingKeyList = new ConcurrentLinkedQueue<>();

    VeniceResponseCompletableFuture<VeniceResponseMap<K, V>> resultFuture = new VeniceResponseCompletableFuture<>(
        () -> new VeniceResponseMapImpl(resultMap, nonExistingKeyList, false),
        keys.size(),
        Optional.empty());
    streamingBatchGet(keys, new StreamingCallback<K, V>() {
      @Override
      public void onRecordReceived(K key, V value) {
        if (value != null) {
          /**
           * {@link java.util.concurrent.ConcurrentHashMap#put} won't take 'null' as the value.
           */
          resultMap.put(key, value);
        } else {
          nonExistingKeyList.add(key);
        }
      }

      @Override
      public void onCompletion(Optional<Exception> exception) {
        if (exception.isPresent()) {
          resultFuture.completeExceptionally(exception.get());
        } else {
          boolean isFullResponse = (resultMap.size() + nonExistingKeyList.size() == keys.size());
          resultFuture.complete(new VeniceResponseMapImpl(resultMap, nonExistingKeyList, isFullResponse));
        }
      }
    });
    return resultFuture;
  }

  /**
   * Streaming interface for {@link #batchGet(Set)}.
   * You can find more info in {@link StreamingCallback}.
   *
   * @param keys
   * @param callback
   * @throws VeniceClientException
   */
  void streamingBatchGet(Set<K> keys, StreamingCallback<K, V> callback) throws VeniceClientException;

  /**
   * This API allows performing transformations (projection, vector arithmetic and aggregations like count) on the
   * values associated with the given set of keys. Check out {@link ComputeRequestBuilder} for details on the available
   * operations.
   * @see ComputeRequestBuilder
   */
  ComputeRequestBuilder<K> compute();

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
   * @deprecated This method is considered deprecated. Please use {@link StoreSchemaFetcher#getLatestValueSchemaEntry()} to fetch
   * latest value schema instead.
   */
  @Deprecated
  Schema getLatestValueSchema();

  /**
   * Re-entry point for callers that obtained Venice-format raw value bytes from a path other than the standard
   * Venice routing layer (e.g. an external storage system holding the same wire bytes) and want to run those bytes
   * through the Fast Client's existing decompression + Avro deserialization pipeline without duplicating that
   * logic.
   *
   * <p>This entry point is Fast Client only — implemented by
   * {@code com.linkedin.venice.fastclient.DispatchingAvroGenericStoreClient} and forwarded by Fast Client's
   * {@code DelegatingAvroStoreClient} wrapper chain. Thin-client implementations inherit the throwing default
   * because they lack the metadata refresh loop that supplies per-version compression strategy, the ZSTD
   * dictionary, and the latest value schema id.
   *
   * <p>The caller supplies only the store {@code version} the bytes were written under. The Fast Client resolves
   * the per-version compressor and decodes with the latest value schema internally, so integrators do not need
   * direct access to {@code StoreMetadata}.
   *
   * @param rawValue   the value payload exactly as Venice would have written it on the wire (post-compression,
   *                   post-Avro-serialization). Must not be {@code null}.
   * @param version    the store version the {@code rawValue} bytes were written under. Used to resolve the
   *                   per-version compressor (including any ZSTD dictionary).
   * @param key        the key that produced this value (used only for error messages and tracing)
   * @return the deserialized value
   * @throws UnsupportedOperationException by the default — including on every thin-client implementation
   * @throws VeniceClientException if decompression or deserialization fails
   */
  default V decompressAndDeserialize(ByteBuffer rawValue, int version, K key) throws VeniceClientException {
    throw new UnsupportedOperationException(
        getClass().getSimpleName() + " does not support external-storage deserialization re-entry. "
            + "This entry point is implemented by the Venice Fast Client only.");
  }

  /**
   * Register a {@link StoreVersionSwitchListener} that fires when the underlying metadata layer observes a change to
   * the store's current serving version.
   *
   * <p>Fast Client only. Thin-client implementations inherit the throwing default — there is no metadata refresh
   * loop on the thin client, so version-switch transitions are not observable. See
   * {@code com.linkedin.venice.fastclient.DispatchingAvroGenericStoreClient} for the Fast Client implementation and
   * {@link StoreVersionSwitchListener} for threading and exception semantics.
   *
   * <p>Listeners may be registered before or after {@link #start()} — registration is thread-safe and re-entrant
   * (a listener may register another listener from inside its own callback).
   *
   * @throws IllegalArgumentException if {@code listener} is {@code null}
   * @throws UnsupportedOperationException by the default — including on every thin-client implementation
   */
  default void registerVersionSwitchListener(StoreVersionSwitchListener listener) {
    throw new UnsupportedOperationException(
        getClass().getSimpleName() + " does not support version-switch listener registration. "
            + "This entry point is implemented by the Venice Fast Client only.");
  }

  /**
   * Register a {@link StoreConfigChangeListener} that fires when the underlying metadata layer observes a change to
   * the store-level config snapshot (e.g. operator-driven {@code externalStorageReadMode} flip).
   *
   * <p>Fast Client only. Thin-client implementations inherit the throwing default. See
   * {@code com.linkedin.venice.fastclient.DispatchingAvroGenericStoreClient} for the Fast Client implementation and
   * {@link StoreConfigChangeListener} for threading and exception semantics.
   *
   * <p>Listeners may be registered before or after {@link #start()} — registration is thread-safe and re-entrant.
   *
   * @throws IllegalArgumentException if {@code listener} is {@code null}
   * @throws UnsupportedOperationException by the default — including on every thin-client implementation
   */
  default void registerStoreConfigChangeListener(StoreConfigChangeListener listener) {
    throw new UnsupportedOperationException(
        getClass().getSimpleName() + " does not support store-config-change listener registration. "
            + "This entry point is implemented by the Venice Fast Client only.");
  }
}
