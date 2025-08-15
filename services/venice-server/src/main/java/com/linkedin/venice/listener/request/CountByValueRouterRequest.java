package com.linkedin.venice.listener.request;

import com.google.protobuf.ByteString;
import com.linkedin.venice.protocols.CountByValueRequest;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.read.RequestType;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Router request for CountByValue aggregation operations.
 * This class extends RouterRequest to support server-side aggregation through the standard Handler chain.
 */
public class CountByValueRouterRequest extends RouterRequest {
  private final List<byte[]> keys;
  private final List<String> fieldNames;
  private final int topK;
  private final int partition;

  public CountByValueRouterRequest(
      String resourceName,
      List<byte[]> keys,
      List<String> fieldNames,
      int topK,
      int partition,
      boolean isRetryRequest) {
    super(resourceName, isRetryRequest, false); // CountByValue is not streaming
    this.keys = keys;
    this.fieldNames = fieldNames;
    this.topK = topK;
    this.partition = partition;
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.COUNT_BY_VALUE;
  }

  @Override
  public int getKeyCount() {
    return keys.size();
  }

  public int getPartition() {
    return partition;
  }

  public List<byte[]> getKeys() {
    return keys;
  }

  public List<String> getFieldNames() {
    return fieldNames;
  }

  public int getTopK() {
    return topK;
  }

  /**
   * Parse gRPC CountByValue request, similar to GetRouterRequest.grpcGetRouterRequest()
   * and MultiGetRouterRequestWrapper.parseMultiGetGrpcRequest()
   */
  public static CountByValueRouterRequest parseGrpcCountByValueRequest(
      VeniceClientRequest clientRequest,
      CountByValueRequest countByValueRequest) {

    List<byte[]> keys =
        countByValueRequest.getKeysList().stream().map(ByteString::toByteArray).collect(Collectors.toList());

    // For simplicity, use partition 0. In real implementation,
    // partition would be computed from keys using the store's partitioner
    int partition = 0;

    return new CountByValueRouterRequest(
        countByValueRequest.getResourceName(),
        keys,
        countByValueRequest.getFieldNamesList(),
        countByValueRequest.getTopK(),
        partition,
        false // isRetryRequest - can be extended if needed
    );
  }
}
