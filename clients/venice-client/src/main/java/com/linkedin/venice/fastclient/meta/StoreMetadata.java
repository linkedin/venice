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

  int getBatchGetLimit();

  void start();

  default boolean isReady() {
    return true;
  }

  <K> void routeRequest(RequestContext requestContext, RecordSerializer<K> keySerializer);

}
