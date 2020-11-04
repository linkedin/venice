package com.linkedin.venice.fastclient.meta;

import com.linkedin.restli.common.HttpStatus;
import com.linkedin.venice.client.schema.SchemaRetriever;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;


/**
 * This interface defines the APIs to retrieve store metadata and routing data,
 * and it also includes the feedback APIs: {@link #sendRequestToInstance}
 * to decide the healthiness of each replica.
 */
public interface StoreMetadata extends SchemaRetriever {

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
  List<String> getReplicas(long requestId, int version, int partitionId, int requiredReplicaCount);

  CompletableFuture<HttpStatus> sendRequestToInstance(String instance, int version, int partitionId);

  InstanceHealthMonitor getInstanceHealthMonitor();
}
