package com.linkedin.venice.router.api.path;

import static com.linkedin.venice.router.api.VenicePathParser.TYPE_STORAGE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;

import com.linkedin.alpini.base.misc.QueryStringDecoder;
import com.linkedin.alpini.router.api.RouterException;
import com.linkedin.venice.RequestConstants;
import com.linkedin.venice.exceptions.VeniceNoHelixResourceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils;
import com.linkedin.venice.router.api.RouterKey;
import com.linkedin.venice.router.api.VenicePartitionFinder;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import io.netty.handler.codec.http.HttpMethod;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;


public class VeniceSingleGetPath extends VenicePath {
  private static final String ROUTER_REQUEST_VERSION =
      Integer.toString(ReadAvroProtocolDefinition.SINGLE_GET_ROUTER_REQUEST_V1.getProtocolVersion());

  private final RouterKey routerKey;
  private final String partition;

  public VeniceSingleGetPath(
      String storeName,
      int versionNumber,
      String resourceName,
      String key,
      String uri,
      VenicePartitionFinder partitionFinder,
      RouterStats<AggRouterHttpRequestStats> stats) throws RouterException {
    super(storeName, versionNumber, resourceName, false, -1);
    if (StringUtils.isEmpty(key)) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
          Optional.empty(),
          Optional.empty(),
          BAD_REQUEST,
          "Request URI must have non-empty key.  Uri is: " + uri);
    }

    if (isFormatB64(uri)) {
      routerKey = RouterKey.fromBase64(key);
    } else {
      routerKey = RouterKey.fromString(key);
    }

    stats.getStatsByType(RequestType.SINGLE_GET).recordKeySize(storeName, routerKey.getKeySize());

    try {
      int partitionNum = partitionFinder.getNumPartitions(resourceName);
      int partitionId = partitionFinder.findPartitionNumber(routerKey, partitionNum, storeName, versionNumber);
      routerKey.setPartitionId(partitionId);
      String partition = Integer.toString(partitionId);
      setPartitionKeys(Collections.singleton(routerKey));
      this.partition = partition;
    } catch (VeniceNoHelixResourceException e) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTrackingResourceNotFound(
          Optional.of(getStoreName()),
          Optional.of(RequestType.SINGLE_GET),
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
          Optional.of(getStoreName()),
          Optional.of(getRequestType()),
          INTERNAL_SERVER_ERROR,
          "RouterKey: " + routerKey + " is expected, but received: " + s);
    }
    return this;
  }

  @Override
  public VenicePath substitutePartitionKey(@Nonnull Collection<RouterKey> s) {
    throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(
        Optional.of(getStoreName()),
        Optional.of(getRequestType()),
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
        .append(partition)
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

  public String getPartition() {
    return this.partition;
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
}
