package com.linkedin.venice.fastclient.meta;

import com.linkedin.venice.client.store.listeners.StoreConfigChangeListener;
import com.linkedin.venice.client.store.listeners.StoreVersionSwitchListener;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.fastclient.RequestContext;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.concurrent.ChainedCompletableFuture;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


/**
 * This interface defines the APIs to retrieve store metadata and routing data,
 * and it also includes the feedback APIs: {@link #trackHealthBasedOnRequestToInstance}
 * to decide the healthiness of each replica.
 */
public interface StoreMetadata extends SchemaReader {
  String getClusterName();

  String getStoreName();

  int getCurrentStoreVersion();

  int getPartitionId(int version, ByteBuffer key);

  int getPartitionId(int version, byte[] key);

  List<String> getReplicas(int version, int partitionId);

  /**
   * This function is expected to return fully qualified URI, such as: "https://fake.host:8888".
   */
  String getReplica(long requestId, int groupId, int version, int partitionId, Set<String> excludedInstances);

  ChainedCompletableFuture<Integer, Integer> trackHealthBasedOnRequestToInstance(
      String instance,
      int version,
      int partitionId,
      CompletableFuture<TransportClientResponse> transportFuture);

  InstanceHealthMonitor getInstanceHealthMonitor();

  VeniceCompressor getCompressor(CompressionStrategy compressionStrategy, int version);

  /**
   * Returns the {@link CompressionStrategy} configured for the given store {@code version}.
   *
   * <p>The strategy is delivered to this metadata instance via the per-version {@code versionProperties} on each
   * metadata-refresh response. The Fast Client's {@code decompressAndDeserialize} seam uses this internally to
   * resolve the per-version compressor when decoding out-of-band value bytes.
   *
   * <p>External integration layers should not call this directly — use
   * {@link com.linkedin.venice.client.store.AvroGenericStoreClient#decompressAndDeserialize(java.nio.ByteBuffer, int, Object)}
   * instead, which handles the version → strategy → compressor → schema id resolution behind a single call.
   *
   * <p><b>Unknown-version contract.</b> The canonical {@link AbstractStoreMetadata}-derived implementation throws
   * {@code VeniceClientException} on a version it has never observed (e.g. caller typo, or a version evicted from
   * the active set on a prior refresh). The default implementation here returns {@link CompressionStrategy#NO_OP}
   * so that lightweight test fakes do not need to track per-version state — fake-based tests should not pass
   * unknown versions to begin with.
   */
  default CompressionStrategy getCompressionStrategy(int version) {
    return CompressionStrategy.NO_OP;
  }

  int getBatchGetLimit();

  void start();

  default boolean isReady() {
    return true;
  }

  <K> void routeRequest(RequestContext requestContext, RecordSerializer<K> keySerializer);

  /**
   * Register a callback that fires when the metadata refresh loop observes a change to the store's current serving
   * version. The callback is invoked after the new version has been committed to the local cache. See
   * {@link StoreVersionSwitchListener} for threading and exception semantics.
   *
   * <p><b>Registration timing.</b> Listeners must be registered before {@link #start()} to observe the initial
   * transition committed by the first refresh. Listeners registered after {@code start()} returns observe only
   * subsequent transitions.
   *
   * <p>{@code listener} must not be {@code null}. Default implementations of {@link StoreMetadata} (e.g. test fakes)
   * treat the registration itself as a no-op but still enforce the non-null contract so behavior is consistent with
   * the canonical {@link AbstractStoreMetadata} implementation.
   *
   * @throws IllegalArgumentException if {@code listener} is {@code null}
   */
  default void registerVersionSwitchListener(StoreVersionSwitchListener listener) {
    if (listener == null) {
      throw new IllegalArgumentException("StoreVersionSwitchListener must not be null");
    }
  }

  /**
   * Register a callback that fires when the metadata refresh loop observes a change to the store-level config
   * snapshot (e.g. operator-driven {@link com.linkedin.venice.meta.ExternalStorageReadMode} flip). The callback is invoked
   * after the new snapshot has been committed to the local cache. See {@link StoreConfigChangeListener} for
   * threading and exception semantics.
   *
   * <p><b>Registration timing.</b> Same as {@link #registerVersionSwitchListener} — register before {@link #start()}
   * to observe the initial snapshot; post-start registration observes only subsequent changes.
   *
   * <p>{@code listener} must not be {@code null}. Default implementations of {@link StoreMetadata} (e.g. test fakes)
   * treat the registration itself as a no-op but still enforce the non-null contract so behavior is consistent with
   * the canonical {@link AbstractStoreMetadata} implementation.
   *
   * @throws IllegalArgumentException if {@code listener} is {@code null}
   */
  default void registerStoreConfigChangeListener(StoreConfigChangeListener listener) {
    if (listener == null) {
      throw new IllegalArgumentException("StoreConfigChangeListener must not be null");
    }
  }
}
