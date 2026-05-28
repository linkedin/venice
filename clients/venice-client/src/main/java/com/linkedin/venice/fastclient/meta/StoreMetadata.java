package com.linkedin.venice.fastclient.meta;

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
   * Returns the {@link CompressionStrategy} configured for the given store {@code version}, or
   * {@link CompressionStrategy#NO_OP} if {@code version} is not currently known to this metadata instance.
   *
   * <p>The strategy is set by the server when a version is created and is delivered to this metadata instance via
   * the per-version {@code versionProperties} on each metadata-refresh response. Both in-band and out-of-band read
   * paths therefore have access to the same source of truth.
   *
   * <p>The in-band Venice read path ({@code DispatchingAvroGenericStoreClient}) does not call this accessor — it
   * reads the strategy from the {@code TransportClientResponse} header that the server stamps on every per-request
   * response, which avoids a separate lookup on the hot path. Out-of-band callers (e.g. the
   * {@code decompressAndDeserialize} seam fed bytes from an external storage system) have no such response header
   * and must query by version; this accessor is for them.
   *
   * <p>The default implementation returns {@link CompressionStrategy#NO_OP} so that lightweight test fakes do not
   * need to track per-version compression state; the canonical {@link AbstractStoreMetadata}-derived implementation
   * overrides this with a live per-version cache.
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
