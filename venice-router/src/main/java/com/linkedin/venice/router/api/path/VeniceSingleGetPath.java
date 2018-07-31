package com.linkedin.venice.router.api.path;

import com.linkedin.ddsstorage.base.misc.QueryStringDecoder;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.RequestConstants;
import com.linkedin.venice.exceptions.VeniceNoHelixResourceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils;
import com.linkedin.venice.router.api.RouterKey;
import com.linkedin.venice.router.api.VenicePartitionFinder;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.utils.Utils;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;

import static io.netty.handler.codec.http.HttpResponseStatus.*;


public class VeniceSingleGetPath extends VenicePath {
  private static final String ROUTER_REQUEST_VERSION = Integer.toString(
      ReadAvroProtocolDefinition.SINGLE_GET_ROUTER_REQUEST_V1.getProtocolVersion());

  private final RouterKey routerKey;
  private final String partition;

  public VeniceSingleGetPath(String resourceName, String key, String uri, VenicePartitionFinder partitionFinder)
      throws RouterException {
    super(resourceName);
    if (Utils.isNullOrEmpty(key)) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.empty(), Optional.empty(), BAD_REQUEST,
          "Request URI must have non-empty key.  Uri is: " + uri);
    }

    if (isFormatB64(uri)){
      routerKey = RouterKey.fromBase64(key);
    } else {
      routerKey = RouterKey.fromString(key);
    }
    try {
      int partitionId = partitionFinder.findPartitionNumber(resourceName, routerKey);
      routerKey.setPartitionId(partitionId);
      String partition = Integer.toString(partitionId);
      setPartitionKeys(Collections.singleton(routerKey));
      this.partition = partition;
    } catch (VeniceNoHelixResourceException e) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
          Optional.of(getStoreName()),
          Optional.of(RequestType.SINGLE_GET),
          e.getHttpResponseStatus(),
          e.getMessage());
    }
  }

  public VeniceSingleGetPath(String resourceName, RouterKey routerKey, String partition) {
    super(resourceName);
    this.routerKey = routerKey;
    this.partition = partition;
    setPartitionKeys(Collections.singleton(routerKey));
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.SINGLE_GET;
  }

  /**
   * For single-get request, the substituted request is same as the original request.
   * @param s
   * @return
   */
  @Override
  public VenicePath substitutePartitionKey(RouterKey s) {
    if (!routerKey.equals(s)) {
      throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.of(getStoreName()), Optional.of(getRequestType()),
          INTERNAL_SERVER_ERROR, "RouterKey: " + routerKey + " is expected, but received: " + s);
    }
    return this;
  }

  @Override
  public VenicePath substitutePartitionKey(@Nonnull Collection<RouterKey> s) {
    throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.of(getStoreName()), Optional.of(getRequestType()),
        INTERNAL_SERVER_ERROR, "substitutePartitionKey(@Nonnull Collection<RouterKey> s) is not expected to be invoked for single-get request");
  }

  @Override
  public HttpUriRequest composeRouterRequest(String storageNodeUri) {
    HttpGet routerRequest = new HttpGet(storageNodeUri + getLocation());
    // Setup API version header
    routerRequest.addHeader(HttpConstants.VENICE_API_VERSION, ROUTER_REQUEST_VERSION);

    return routerRequest;
  }

  @Nonnull
  @Override
  public String getLocation() {
    String sep = VenicePathParser.SEP;
    StringBuilder sb = new StringBuilder();
    sb.append(VenicePathParser.TYPE_STORAGE).append(sep)
        .append(getResourceName()).append(sep)
        .append(partition).append(sep)
        .append(getPartitionKey().base64Encoded()).append("?")
        .append(RequestConstants.FORMAT_KEY).append("=").append(RequestConstants.B64_FORMAT);
    return sb.toString();
  }

  protected static boolean isFormatB64(String key) {
    String format = RequestConstants.DEFAULT_FORMAT; //"string"
    QueryStringDecoder queryStringParser = new QueryStringDecoder(key, StandardCharsets.UTF_8);
    if (queryStringParser.getParameters().keySet().contains(RequestConstants.FORMAT_KEY)) {
      format = queryStringParser.getParameters().get(RequestConstants.FORMAT_KEY).get(0);
    }
    return format.equals(RequestConstants.B64_FORMAT);
  }

  public String getPartition() {
    return this.partition;
  }
}
