package com.linkedin.venice.fastclient;

import com.google.protobuf.ByteString;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.fastclient.meta.StoreMetadata;
import com.linkedin.venice.fastclient.transport.GrpcTransportClient;
import com.linkedin.venice.protocols.CountByValueRequest;
import com.linkedin.venice.response.VeniceReadResponseStatus;
import com.linkedin.venice.serializer.RecordSerializer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


/**
 * Implementation of ServerSideAggregationRequestBuilder that uses gRPC
 * to perform server-side aggregations.
 */
public class ServerSideAggregationRequestBuilderImpl<K> implements ServerSideAggregationRequestBuilder<K> {
  private final StoreMetadata metadata;
  private final GrpcTransportClient grpcTransportClient;
  private final RecordSerializer<K> keySerializer;

  private List<String> fieldNames = new ArrayList<>();
  private int topK;

  public ServerSideAggregationRequestBuilderImpl(
      StoreMetadata metadata,
      GrpcTransportClient grpcTransportClient,
      RecordSerializer<K> keySerializer) {
    this.metadata = metadata;
    this.grpcTransportClient = grpcTransportClient;
    this.keySerializer = keySerializer;
  }

  @Override
  public ServerSideAggregationRequestBuilder<K> countByValue(String fieldName, int topK) {
    if (fieldName == null || fieldName.isEmpty()) {
      throw new VeniceClientException("Field name cannot be null or empty");
    }
    if (topK <= 0) {
      throw new VeniceClientException("TopK must be positive");
    }
    this.fieldNames.clear();
    this.fieldNames.add(fieldName);
    this.topK = topK;
    return this;
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
    this.fieldNames.clear();
    this.fieldNames.addAll(fieldNames);
    this.topK = topK;
    return this;
  }

  @Override
  public CompletableFuture<AggregationResponse> execute(Set<K> keys) throws VeniceClientException {
    if (keys == null || keys.isEmpty()) {
      throw new VeniceClientException("Keys cannot be null or empty");
    }

    if (fieldNames.isEmpty()) {
      throw new VeniceClientException("Must call countByValue() before execute()");
    }

    // Serialize keys
    List<ByteString> serializedKeys = new ArrayList<>(keys.size());
    for (K key: keys) {
      byte[] keyBytes = keySerializer.serialize(key);
      serializedKeys.add(ByteString.copyFrom(keyBytes));
    }

    // Build the gRPC request with ALL keys (no client-side partitioning)
    int currentVersion = metadata.getCurrentStoreVersion();
    String resourceName = metadata.getStoreName() + "_v" + currentVersion;
    CountByValueRequest request = CountByValueRequest.newBuilder()
        .setResourceName(resourceName)
        .addAllKeys(serializedKeys)
        .addAllFieldNames(fieldNames)
        .setTopK(topK)
        .build();

    // Send single request to any available server - server handles partition routing and aggregation
    List<String> replicas = metadata.getReplicas(0, currentVersion); // Get any replica
    if (replicas.isEmpty()) {
      throw new VeniceClientException("No available replicas found for store: " + metadata.getStoreName());
    }

    String serverAddress = replicas.get(0); // Use first available server

    // Send single request to server - server does ALL the work
    return grpcTransportClient.countByValue(serverAddress, request).thenApply(response -> {
      if (response.getErrorCode() == VeniceReadResponseStatus.OK) {
        return new AggregationResponseImpl(response);
      } else {
        String errorMessage = response.getErrorMessage();
        String errorMsg = String.format(
            "Server-side aggregation failed with error code %d: %s",
            response.getErrorCode(),
            errorMessage.isEmpty() ? "Unknown error" : errorMessage);
        throw new VeniceClientException(errorMsg);
      }
    });
  }
}
