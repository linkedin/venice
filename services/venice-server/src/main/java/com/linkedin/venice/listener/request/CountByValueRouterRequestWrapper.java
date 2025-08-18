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

  @Override
  public RequestType getRequestType() {
    return RequestType.COUNT_BY_VALUE;
  }

  @Override
  public String toString() {
    return "CountByValueRouterRequestWrapper(storeName: " + getStoreName() + ", key count: " + getKeyCount() + ")";
  }
}
