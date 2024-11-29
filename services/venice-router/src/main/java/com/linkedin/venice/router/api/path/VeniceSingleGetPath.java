package com.linkedin.venice.router.api.path;

import static com.linkedin.venice.router.api.VenicePathParser.TYPE_STORAGE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;

import com.linkedin.alpini.base.misc.QueryStringDecoder;
import com.linkedin.alpini.router.api.RouterException;
import com.linkedin.venice.RequestConstants;
import com.linkedin.venice.exceptions.VeniceNoHelixResourceException;
import com.linkedin.venice.meta.RetryManager;
import com.linkedin.venice.meta.StoreVersionName;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.RouterRetryConfig;
import com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils;
import com.linkedin.venice.router.api.RouterKey;
import com.linkedin.venice.router.api.VenicePartitionFinder;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.router.api.VeniceResponseDecompressor;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import io.netty.handler.codec.http.HttpMethod;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import javax.annotation.Nonnull;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;


public class VeniceSingleGetPath extends VenicePath {
  private static final String ROUTER_REQUEST_VERSION =
      Integer.toString(ReadAvroProtocolDefinition.SINGLE_GET_ROUTER_REQUEST_V1.getProtocolVersion());

  private final RouterKey routerKey;

  public VeniceSingleGetPath(
      StoreVersionName storeVersionName,
      String key,
      String uri,
      VenicePartitionFinder partitionFinder,
      RouterStats<AggRouterHttpRequestStats> stats,
      RouterRetryConfig retryConfig,
      RetryManager retryManager,
      VeniceResponseDecompressor responseDecompressor) throws RouterException {
    super(storeVersionName, retryConfig, retryManager, responseDecompressor);
    if (StringUtils.isEmpty(key)) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
          null,
          null,
          BAD_REQUEST,
          "Request URI must have non-empty key.  Uri is: " + uri);
    }

    if (isFormatB64(uri)) {
      routerKey = RouterKey.fromBase64(key);
    } else {
      routerKey = RouterKey.fromString(key);
    }

    stats.getStatsByType(RequestType.SINGLE_GET).recordKeySize(routerKey.getKeyBuffer().remaining());

    try {
      int partitionNum = partitionFinder.getNumPartitions(storeVersionName.getName());
      int partitionId = partitionFinder.findPartitionNumber(
          routerKey,
          partitionNum,
          storeVersionName.getStoreName(),
          storeVersionName.getVersionNumber());
      routerKey.setPartitionId(partitionId);
      setPartitionKeys(Collections.singleton(routerKey));
    } catch (VeniceNoHelixResourceException e) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTrackingResourceNotFound(
          getStoreName(),
          RequestType.SINGLE_GET,
          e.getHttpResponseStatus(),
          e.getMessage());
    }
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.SINGLE_GET;
  }

  @Override
  protected RequestType getStreamingRequestType() {
    throw new IllegalStateException("This should not be called on " + this.getClass().getSimpleName());
  }

  /**
   * For single-get request, the substituted request is same as the original request.
   */
  @Override
  public VenicePath substitutePartitionKey(RouterKey s) {
    if (!routerKey.equals(s)) {
      throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(
          getStoreName(),
          getRequestType(),
          INTERNAL_SERVER_ERROR,
          "RouterKey: " + routerKey + " is expected, but received: " + s);
    }
    return this;
  }

  @Override
  public VenicePath substitutePartitionKey(@Nonnull Collection<RouterKey> s) {
    throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(
        getStoreName(),
        getRequestType(),
        INTERNAL_SERVER_ERROR,
        "substitutePartitionKey(@Nonnull Collection<RouterKey> s) is not expected to be invoked for single-get request");
  }

  @Override
  public HttpUriRequest composeRouterRequestInternal(String storageNodeUri) {
    HttpGet routerRequest = new HttpGet(storageNodeUri + getLocation());

    return routerRequest;
  }

  @Nonnull
  @Override
  public String getLocation() {
    String sep = VenicePathParser.SEP;
    StringBuilder sb = new StringBuilder();
    sb.append(TYPE_STORAGE)
        .append(sep)
        .append(getResourceName())
        .append(sep)
        .append(routerKey.getPartitionId())
        .append(sep)
        .append(getPartitionKey().base64Encoded())
        .append("?")
        .append(RequestConstants.FORMAT_KEY)
        .append("=")
        .append(RequestConstants.B64_FORMAT);
    return sb.toString();
  }

  protected static boolean isFormatB64(String key) {
    String format = RequestConstants.DEFAULT_FORMAT; // "string"
    QueryStringDecoder queryStringParser = new QueryStringDecoder(key, StandardCharsets.UTF_8);
    if (queryStringParser.getParameters().keySet().contains(RequestConstants.FORMAT_KEY)) {
      format = queryStringParser.getParameters().get(RequestConstants.FORMAT_KEY).get(0);
    }
    return format.equals(RequestConstants.B64_FORMAT);
  }

  @Override
  public HttpMethod getHttpMethod() {
    return HttpMethod.GET;
  }

  @Override
  public byte[] getBody() {
    return null;
  }

  @Override
  public String getVeniceApiVersionHeader() {
    return ROUTER_REQUEST_VERSION;
  }

  public int getLongTailRetryThresholdMs() {
    return this.retryConfig.getLongTailRetryForSingleGetThresholdMs();
  }
}
