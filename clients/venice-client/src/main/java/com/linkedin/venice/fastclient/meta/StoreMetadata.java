package com.linkedin.venice.fastclient.meta;

import com.linkedin.restli.common.HttpStatus;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.schema.SchemaReader;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


/**
 * This interface defines the APIs to retrieve store metadata and routing data,
 * and it also includes the feedback APIs: {@link #trackHealthBasedOnRequestToInstance}
 * to decide the healthiness of each replica.
 */
public interface StoreMetadata extends SchemaReader {
  String getStoreName();

  int getCurrentStoreVersion();

  int getPartitionId(int version, ByteBuffer key);

  int getPartitionId(int version, byte[] key);

  List<String> getReplicas(int version, int partitionId);

  /**
   * This function is expected to return fully qualified URI, such as: "https://fake.host:8888".
   * @param version
   * @param partitionId
   * @return
   */

  default List<String> getReplicas(long requestId, int version, int partitionId, int requiredReplicaCount) {
    return getReplicas(requestId, version, partitionId, requiredReplicaCount, Collections.emptySet());
  }

  List<String> getReplicas(
      long requestId,
      int version,
      int partitionId,
      int requiredReplicaCount,
      Set<String> excludedInstances);

  CompletableFuture<HttpStatus> trackHealthBasedOnRequestToInstance(String instance, int version, int partitionId);

  InstanceHealthMonitor getInstanceHealthMonitor();

  VeniceCompressor getCompressor(CompressionStrategy compressionStrategy, int version);

  void start();

  default boolean isReady() {
    return true;
  }
}
