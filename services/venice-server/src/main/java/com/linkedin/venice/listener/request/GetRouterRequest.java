package com.linkedin.venice.listener.request;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.RequestConstants;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.protocols.SingleGetRequest;
import com.linkedin.venice.protocols.VeniceClientRequest;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.streaming.StreamingUtils;
import com.linkedin.venice.utils.EncodingUtils;
import com.linkedin.venice.utils.NettyUtils;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import java.nio.charset.StandardCharsets;


/**
 * {@code GetRouterRequest} encapsulates a GET request to storage/resourcename/partition/key on the storage node for a single-get operation.
 */
public class GetRouterRequest extends RouterRequest {
  private final int partition;
  private final byte[] keyBytes;

  private GetRouterRequest(
      String resourceName,
      int partition,
      byte[] keyBytes,
      boolean isRetryRequest,
      boolean isStreamingRequest) {
    super(resourceName, isRetryRequest, isStreamingRequest);
    this.partition = partition;
    this.keyBytes = keyBytes;
  }

  public int getPartition() {
    return partition;
  }

  public byte[] getKeyBytes() {
    return keyBytes;
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.SINGLE_GET;
  }

  @Override
  public int getKeyCount() {
    return 1;
  }

  public static GetRouterRequest parseGetHttpRequest(HttpRequest request, String[] requestParts) {
    if (requestParts.length == 5) {
      // [0]""/[1]"action"/[2]"store"/[3]"partition"/[4]"key"
      String topicName = requestParts[2];
      int partition = Integer.parseInt(requestParts[3]);
      byte[] keyBytes = getKeyBytesFromUrlKeyString(requestParts[4]);
      return new GetRouterRequest(
          topicName,
          partition,
          keyBytes,
          NettyUtils.containRetryHeader(request),
          StreamingUtils.isStreamingEnabled(request));
    } else {
      throw new VeniceException("Not a valid request for a STORAGE action: " + request.uri());
    }
  }

  /**
   * @deprecated This method has been deprecated and will be removed once the corresponding legacy gRPC code is removed.
   */
  @Deprecated
  public static GetRouterRequest parseSingleGetGrpcRequest(VeniceClientRequest request) {
    String resourceName = request.getResourceName();
    int partition = request.getPartition();
    byte[] keyBytes = getKeyBytesFromUrlKeyString(request.getKeyString());
    boolean isRetryRequest = request.getIsRetryRequest();
    boolean isStreamingRequest = request.getIsStreamingRequest();
    return new GetRouterRequest(resourceName, partition, keyBytes, isRetryRequest, isStreamingRequest);
  }

  public static GetRouterRequest parseSingleGetGrpcRequest(SingleGetRequest singleGetRequest) {
    String resourceName = singleGetRequest.getResourceName();
    int partition = singleGetRequest.getPartition();
    byte[] keyBytes = getKeyBytesFromUrlKeyString(singleGetRequest.getKey());
    boolean isRetryRequest = singleGetRequest.getIsRetryRequest();
    return new GetRouterRequest(resourceName, partition, keyBytes, isRetryRequest, false);
  }

  public static byte[] getKeyBytesFromUrlKeyString(String keyString) {
    QueryStringDecoder queryStringParser = new QueryStringDecoder(keyString, StandardCharsets.UTF_8);
    String format = RequestConstants.DEFAULT_FORMAT;
    if (queryStringParser.parameters().containsKey(RequestConstants.FORMAT_KEY)) {
      format = queryStringParser.parameters().get(RequestConstants.FORMAT_KEY).get(0);
    }
    switch (format) {
      case RequestConstants.B64_FORMAT:
        return EncodingUtils.base64DecodeFromString(queryStringParser.path());
      default:
        return queryStringParser.path().getBytes(StandardCharsets.UTF_8);
    }
  }

  /***
   * throws VeniceException if we don't handle the specified api version
   * @param headers
   */
  public static void verifyApiVersion(HttpHeaders headers, String expectedVersion) {
    if (headers.contains(HttpConstants.VENICE_API_VERSION)) { /* if not present, assume latest version */
      String clientApiVersion = headers.get(HttpConstants.VENICE_API_VERSION);
      if (!clientApiVersion.equals(expectedVersion)) {
        throw new VeniceException("Storage node is not compatible with requested API version: " + clientApiVersion);
      }
    }
  }
}
