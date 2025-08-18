package com.linkedin.venice.listener.request;

import com.linkedin.venice.protocols.CountByValueRequest;
import com.linkedin.venice.read.RequestType;
import io.netty.handler.codec.http.HttpRequest;
import java.util.List;
import java.util.stream.Collectors;


/**
 * {@code CountByValueRouterRequestWrapper} encapsulates a CountByValue request for gRPC.
 */
public class CountByValueRouterRequestWrapper extends RouterRequest {
  private final CountByValueRequest countByValueRequest;
  private final List<byte[]> keys;
  private final List<Integer> partitions;

  public CountByValueRouterRequestWrapper(
      String resourceName,
      CountByValueRequest countByValueRequest,
      HttpRequest request) {
    super(resourceName, request);
    this.countByValueRequest = countByValueRequest;
    this.keys = countByValueRequest.getKeysList()
        .stream()
        .map(byteString -> byteString.toByteArray())
        .collect(Collectors.toList());
    this.partitions = countByValueRequest.getPartitionsList();
  }

  public CountByValueRequest getCountByValueRequest() {
    return countByValueRequest;
  }

  public List<byte[]> getKeys() {
    return keys;
  }

  public int getKeyCount() {
    return keys.size();
  }

  public List<Integer> getPartitions() {
    return partitions;
  }

  public int getPartition(int keyIndex) {
    if (keyIndex < 0 || keyIndex >= partitions.size()) {
      throw new IndexOutOfBoundsException("Key index out of bounds: " + keyIndex);
    }
    return partitions.get(keyIndex);
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.COUNT_BY_VALUE;
  }

  @Override
  public String toString() {
    return "CountByValueRouterRequestWrapper(storeName: " + getStoreName() + ", key count: " + getKeyCount() + ")";
  }
}
