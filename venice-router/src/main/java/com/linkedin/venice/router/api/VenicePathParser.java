package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.netty4.misc.BasicFullHttpRequest;
import com.linkedin.ddsstorage.netty4.misc.BasicHttpRequest;
import com.linkedin.ddsstorage.router.api.ExtendedResourcePathParser;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.router.api.path.VeniceSingleGetPath;
import com.linkedin.venice.router.api.path.VeniceMultiGetPath;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.utils.VeniceRouterUtils;
import com.linkedin.venice.utils.Utils;
import io.netty.handler.codec.http.HttpMethod;
import java.util.Collection;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.apache.log4j.Logger;

import static io.netty.handler.codec.rtsp.RtspResponseStatuses.*;


/***
 *   Inbound request to the router will look like:
 *   /read/storename/key?f=fmt
 *
 *   'read' is a literal, meaning we will request the value for a single key
 *   storename will be the name of the requested store
 *   key is the key being looked up
 *   fmt is an optional format parameter, one of 'string' or 'b64'.  If ommitted, assumed to be 'string'
 *
 *   The VenicePathParser is responsible for looking up the active version of the store, and constructing the store-version
 */
public class VenicePathParser<HTTP_REQUEST extends BasicHttpRequest>
    implements ExtendedResourcePathParser<VenicePath, RouterKey, HTTP_REQUEST> {
  private static final Logger LOGGER = Logger.getLogger(VenicePathParser.class);

  public static final String STORE_VERSION_SEP = "_v";
  public static final Pattern STORE_PATTERN = Pattern.compile("\\A[a-zA-Z][a-zA-Z0-9_-]*\\z"); // \A and \z are start and end of string
  public static final int STORE_MAX_LENGTH = 128;
  public static final String SEP = "/";

  public static final String TYPE_STORAGE = "storage";
  // Right now, we hardcoded url path for getting master controller to be same as the one
  // being used in Venice Controller, so that ControllerClient can use the same API to get
  // master controller without knowing whether the host is Router or Controller.
  // Without good reason, please don't update this path.
  public static final String TYPE_MASTER_CONTROLLER = ControllerRoute.MASTER_CONTROLLER.getPath().replace("/", "");
  public static final String TYPE_KEY_SCHEMA = "key_schema";
  public static final String TYPE_VALUE_SCHEMA = "value_schema";
  public static final String TYPE_CLUSTER_DISCOVERY = "discover_cluster";

  private final VeniceVersionFinder versionFinder;
  private final VenicePartitionFinder partitionFinder;
  private final AggRouterHttpRequestStats statsForSingleGet;
  private final AggRouterHttpRequestStats statsForMultiGet;

  private final int maxKeyCountInMultiGetReq;

  public VenicePathParser(VeniceVersionFinder versionFinder, VenicePartitionFinder partitionFinder,
      AggRouterHttpRequestStats statsForSingleGet, AggRouterHttpRequestStats statsForMultiGet,
      int maxKeyCountInMultiGetReq){
    this.versionFinder = versionFinder;
    this.partitionFinder = partitionFinder;
    this.statsForSingleGet = statsForSingleGet;
    this.statsForMultiGet = statsForMultiGet;
    this.maxKeyCountInMultiGetReq = maxKeyCountInMultiGetReq;
  };

  @Override
  public VenicePath parseResourceUri(String uri, HTTP_REQUEST request) throws RouterException {
    if (!(request instanceof BasicFullHttpRequest)) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.empty(), Optional.empty(),
          BAD_GATEWAY, "parseResourceUri should receive a BasicFullHttpRequest");
    }
    BasicFullHttpRequest fullHttpRequest = (BasicFullHttpRequest)request;
    VenicePathParserHelper pathHelper = new VenicePathParserHelper(uri);
    String resourceType = pathHelper.getResourceType();
    if (! resourceType.equals(TYPE_STORAGE)) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.empty(), Optional.empty(),
          BAD_REQUEST, "Requested resource type: " + resourceType + " is not a valid type");
    }
    String storeName = pathHelper.getResourceName();
    if (Utils.isNullOrEmpty(storeName)) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.empty(), Optional.empty(),
          BAD_REQUEST, "Request URI must have storeName.  Uri is: " + uri);
    }
    String resourceName = getResourceFromStoreName(storeName);

    String method = fullHttpRequest.method().name();
    VenicePath path = null;
    AggRouterHttpRequestStats stats = null;
    if (VeniceRouterUtils.isHttpGet(method)) {
      // single-get request
      path = new VeniceSingleGetPath(resourceName, pathHelper.getKey(), uri, partitionFinder);
      stats = statsForSingleGet;
    } else if (VeniceRouterUtils.isHttpPost(method)) {
      // multi-get request
      path = new VeniceMultiGetPath(resourceName, fullHttpRequest, partitionFinder, maxKeyCountInMultiGetReq);
      stats = statsForMultiGet;
    } else {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.empty(), Optional.empty(),
          BAD_REQUEST, "Method: " + method + " is not allowed");
    }
    stats.recordRequest(storeName);
    stats.recordRequestSize(storeName, path.getRequestSize());
    return path;
  }

  @Nonnull
  @Override
  public VenicePath parseResourceUri(@Nonnull String uri) throws RouterException {
    throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.empty(), Optional.empty(),
        BAD_REQUEST, "parseResourceUri without param: request should not be invoked");
  }

  @Nonnull
  @Override
  public VenicePath substitutePartitionKey(@Nonnull VenicePath path, RouterKey s) {
    return path.substitutePartitionKey(s);
  }

  @Nonnull
  @Override
  public VenicePath substitutePartitionKey(@Nonnull VenicePath path, @Nonnull Collection<RouterKey> s) {
    return path.substitutePartitionKey(s);
  }

  /***
   * Queries the helix metadata repository for the
   *
   * @param storeName
   * @return store-version, matches the helix resource
   */
  private String getResourceFromStoreName(String storeName) throws RouterException {
    int version = versionFinder.getVersion(storeName);
    return storeName + STORE_VERSION_SEP + version;
  }

  public static boolean isStoreNameValid(String storeName){
    if (storeName.length() > STORE_MAX_LENGTH){
      return false;
    }
    Matcher m = STORE_PATTERN.matcher(storeName);
    return m.matches();
  }

}
