package com.linkedin.venice.fastclient;

import com.google.protobuf.ByteString;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.fastclient.meta.StoreMetadata;
import com.linkedin.venice.fastclient.transport.GrpcTransportClient;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.protocols.BucketPredicate;
import com.linkedin.venice.protocols.CountByBucketRequest;
import com.linkedin.venice.protocols.CountByBucketResponse;
import com.linkedin.venice.protocols.CountByValueRequest;
import com.linkedin.venice.protocols.CountByValueResponse;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.CountByBucketUtils;
import com.linkedin.venice.utils.CountByValueUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


/**
 * Implementation of ServerSideAggregationRequestBuilder that uses gRPC
 * with client-side partitioning (similar to batchGet).
 * Each partition server processes only its local data, and client aggregates results.
 */
public class ServerSideAggregationRequestBuilderImpl<K> implements ServerSideAggregationRequestBuilder<K> {
  private final StoreMetadata metadata;
  private final GrpcTransportClient grpcTransportClient;
  private final RecordSerializer<K> keySerializer;

  private List<String> fieldNames = new ArrayList<>();
  private int topK;
  private Map<String, BucketPredicate> bucketPredicates = new HashMap<>();
  private boolean isCountByValue = false;
  private boolean isCountByBucket = false;

  public ServerSideAggregationRequestBuilderImpl(
      StoreMetadata metadata,
      GrpcTransportClient grpcTransportClient,
      RecordSerializer<K> keySerializer) {
    this.metadata = metadata;
    this.grpcTransportClient = grpcTransportClient;
    this.keySerializer = keySerializer;
  }

  @Override
  public ServerSideAggregationRequestBuilder<K> countByValue(List<String> fieldNames, int topK) {
    if (fieldNames == null || fieldNames.isEmpty()) {
      throw new VeniceClientException("Field names cannot be null or empty");
    }
    for (String fieldName: fieldNames) {
      if (fieldName == null || fieldName.isEmpty()) {
        throw new VeniceClientException("Field name cannot be null or empty");
      }
    }
    if (topK <= 0) {
      throw new VeniceClientException("TopK must be positive");
    }

    resetState();
    this.fieldNames.addAll(fieldNames);
    this.topK = topK;
    this.isCountByValue = true;
    return this;
  }

  @Override
  public ServerSideAggregationRequestBuilder<K> countByBucket(
      List<String> fieldNames,
      Map<String, BucketPredicate> bucketPredicates) {
    if (fieldNames == null || fieldNames.isEmpty()) {
      throw new VeniceClientException("Field names cannot be null or empty");
    }
    for (String fieldName: fieldNames) {
      if (fieldName == null || fieldName.isEmpty()) {
        throw new VeniceClientException("Field name cannot be null or empty");
      }
    }
    if (bucketPredicates == null || bucketPredicates.isEmpty()) {
      throw new VeniceClientException("Bucket predicates cannot be null or empty");
    }
    for (Map.Entry<String, BucketPredicate> entry: bucketPredicates.entrySet()) {
      if (entry.getKey() == null || entry.getKey().isEmpty()) {
        throw new VeniceClientException("Bucket name cannot be null or empty");
      }
      if (entry.getValue() == null) {
        throw new VeniceClientException("Bucket predicate cannot be null");
      }
    }

    resetState();
    this.fieldNames.addAll(fieldNames);
    this.bucketPredicates.putAll(bucketPredicates);
    this.isCountByBucket = true;
    return this;
  }

  private void resetState() {
    this.fieldNames.clear();
    this.bucketPredicates.clear();
    this.isCountByValue = false;
    this.isCountByBucket = false;
    this.topK = 0;
  }

  @Override
  public CompletableFuture<AggregationResponse> execute(Set<K> keys) throws VeniceException {
    if (keys == null || keys.isEmpty()) {
      throw new VeniceClientException("Keys cannot be null or empty");
    }

    if (fieldNames.isEmpty()) {
      throw new VeniceClientException("Must call countByValue() or countByBucket() before execute()");
    }

    if (!isCountByValue && !isCountByBucket) {
      throw new VeniceClientException("Must call countByValue() or countByBucket() before execute()");
    }

    if (isCountByValue && isCountByBucket) {
      throw new VeniceClientException("Cannot call both countByValue() and countByBucket() in the same request");
    }

    if (isCountByValue) {
      return executeCountByValue(keys);
    } else {
      return executeCountByBucket(keys);
    }
  }

  private CompletableFuture<AggregationResponse> executeCountByValue(Set<K> keys) {
    int currentVersion = metadata.getCurrentStoreVersion();
    String resourceName = Version.composeKafkaTopic(metadata.getStoreName(), currentVersion);

    // Step 1: Partition keys by their target partitions (client-side partitioning)
    Map<Integer, List<K>> partitionToKeysMap = partitionKeys(keys, currentVersion);

    // Step 2: Send requests to each partition server in parallel
    List<CompletableFuture<CountByValueResponse>> partitionFutures = new ArrayList<>();

    for (Map.Entry<Integer, List<K>> entry: partitionToKeysMap.entrySet()) {
      int partitionId = entry.getKey();
      List<K> partitionKeys = entry.getValue();

      // Get server address for this partition using the same routing strategy as other FastClient operations
      String serverAddress = metadata.getReplica(
          System.currentTimeMillis(), // Use current time as request ID
          0, // Group ID (typically 0 for simple requests)
          currentVersion,
          partitionId,
          java.util.Collections.emptySet() // No excluded instances
      );

      if (serverAddress == null) {
        throw new VeniceClientException(
            "No available replicas found for partition " + partitionId + " in store: " + metadata.getStoreName());
      }

      // Serialize keys for this partition
      List<ByteString> serializedKeys = partitionKeys.stream()
          .map(key -> ByteString.copyFrom(keySerializer.serialize(key)))
          .collect(Collectors.toList());

      // Build request for this partition (no topK - server returns all counts)
      CountByValueRequest request = CountByValueRequest.newBuilder()
          .setResourceName(resourceName)
          .addAllKeys(serializedKeys)
          .addAllFieldNames(fieldNames)
          .setTopK(Integer.MAX_VALUE) // Request all counts from each partition
          .build();

      // Send request to partition server
      CompletableFuture<CountByValueResponse> future =
          grpcTransportClient.countByValue(serverAddress, request).thenApply(response -> {
            if (response.getErrorCode() != VeniceReadResponseStatus.OK) {
              String errorMessage = response.getErrorMessage();
              throw new VeniceClientException(
                  String.format(
                      "Partition %d aggregation failed with error code %d: %s",
                      partitionId,
                      response.getErrorCode(),
                      errorMessage.isEmpty() ? "Unknown error" : errorMessage));
            }
            return response;
          });

      partitionFutures.add(future);
    }

    // Step 3: Aggregate results from all partitions and compute TopK on client side
    return CompletableFuture.allOf(partitionFutures.toArray(new CompletableFuture[0]))
        .thenApply(v -> aggregateCountByValueResults(partitionFutures));
  }

  private CompletableFuture<AggregationResponse> executeCountByBucket(Set<K> keys) {
    int currentVersion = metadata.getCurrentStoreVersion();
    String resourceName = Version.composeKafkaTopic(metadata.getStoreName(), currentVersion);

    // Step 1: Partition keys by their target partitions (client-side partitioning)
    Map<Integer, List<K>> partitionToKeysMap = partitionKeys(keys, currentVersion);

    // Step 2: Send requests to each partition server in parallel
    List<CompletableFuture<CountByBucketResponse>> partitionFutures = new ArrayList<>();

    for (Map.Entry<Integer, List<K>> entry: partitionToKeysMap.entrySet()) {
      int partitionId = entry.getKey();
      List<K> partitionKeys = entry.getValue();

      // Get server address for this partition
      String serverAddress = metadata
          .getReplica(System.currentTimeMillis(), 0, currentVersion, partitionId, java.util.Collections.emptySet());

      if (serverAddress == null) {
        throw new VeniceClientException(
            "No available replicas found for partition " + partitionId + " in store: " + metadata.getStoreName());
      }

      // Serialize keys for this partition
      List<ByteString> serializedKeys = partitionKeys.stream()
          .map(key -> ByteString.copyFrom(keySerializer.serialize(key)))
          .collect(Collectors.toList());

      // Build CountByBucket request for this partition
      CountByBucketRequest request = CountByBucketRequest.newBuilder()
          .setResourceName(resourceName)
          .addAllKeys(serializedKeys)
          .addAllFieldNames(fieldNames)
          .putAllBucketPredicates(bucketPredicates)
          .build();

      // Send request to partition server
      CompletableFuture<CountByBucketResponse> future =
          grpcTransportClient.countByBucket(serverAddress, request).thenApply(response -> {
            if (response.getErrorCode() != VeniceReadResponseStatus.OK) {
              String errorMessage = response.getErrorMessage();
              throw new VeniceClientException(
                  String.format(
                      "Partition %d aggregation failed with error code %d: %s",
                      partitionId,
                      response.getErrorCode(),
                      errorMessage.isEmpty() ? "Unknown error" : errorMessage));
            }
            return response;
          });

      partitionFutures.add(future);
    }

    // Step 3: Aggregate results from all partitions
    return CompletableFuture.allOf(partitionFutures.toArray(new CompletableFuture[0]))
        .thenApply(v -> aggregateCountByBucketResults(partitionFutures));
  }

  /**
   * Partition keys by their target partitions using the store's partitioner.
   */
  private Map<Integer, List<K>> partitionKeys(Set<K> keys, int version) {
    Map<Integer, List<K>> partitionToKeysMap = new HashMap<>();

    for (K key: keys) {
      byte[] keyBytes = keySerializer.serialize(key);
      int partitionId = metadata.getPartitionId(version, keyBytes);
      partitionToKeysMap.computeIfAbsent(partitionId, k -> new ArrayList<>()).add(key);
    }

    return partitionToKeysMap;
  }

  /**
   * Aggregate CountByValue results from all partition servers and compute TopK on client side.
   */
  private AggregationResponse aggregateCountByValueResults(List<CompletableFuture<CountByValueResponse>> futures) {
    try {
      // Extract responses from futures
      List<CountByValueResponse> responses = futures.stream().map(future -> {
        try {
          return future.get();
        } catch (Exception e) {
          throw new RuntimeException("Failed to get partition response", e);
        }
      }).collect(Collectors.toList());

      // Merge partition responses using shared utility
      Map<String, Map<String, Integer>> globalFieldCounts =
          CountByValueUtils.mergePartitionResponses(responses, fieldNames);

      // Apply TopK filtering using shared utility
      for (String fieldName: fieldNames) {
        Map<String, Integer> fieldCounts = globalFieldCounts.get(fieldName);
        if (fieldCounts != null) {
          // Use shared utility for TopK filtering
          Map<String, Integer> topKCounts = CountByValueUtils.filterTopKValues(fieldCounts, topK);
          globalFieldCounts.put(fieldName, topKCounts);
        }
      }

      // Build response using shared utility
      CountByValueResponse response = CountByValueUtils.buildResponse(fieldNames, globalFieldCounts);
      return new AggregationResponseImpl(response);

    } catch (Exception e) {
      throw new VeniceClientException("Failed to aggregate partition results", e);
    }
  }

  /**
   * Aggregate CountByBucket results from all partition servers.
   */
  private AggregationResponse aggregateCountByBucketResults(List<CompletableFuture<CountByBucketResponse>> futures) {
    try {
      // Extract responses from futures
      List<CountByBucketResponse> responses = futures.stream().map(future -> {
        try {
          return future.get();
        } catch (Exception e) {
          throw new RuntimeException("Failed to get partition response", e);
        }
      }).collect(Collectors.toList());

      // Merge partition responses using shared utility
      List<String> bucketNames = new ArrayList<>(bucketPredicates.keySet());
      Map<String, Map<String, Integer>> globalBucketCounts =
          CountByBucketUtils.mergePartitionResponses(responses, fieldNames, bucketNames);

      // Build response using shared utility
      CountByBucketResponse response = CountByBucketUtils.buildResponse(fieldNames, globalBucketCounts);
      return new AggregationResponseImpl(response);

    } catch (Exception e) {
      throw new VeniceClientException("Failed to aggregate bucket partition results", e);
    }
  }
}
