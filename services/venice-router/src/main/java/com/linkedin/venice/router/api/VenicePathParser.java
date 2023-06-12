package com.linkedin.venice.router.api;

import static com.linkedin.venice.read.RequestType.SINGLE_GET;
import static com.linkedin.venice.router.api.VenicePathParserHelper.parseRequest;
import static io.netty.handler.codec.rtsp.RtspResponseStatuses.BAD_GATEWAY;
import static io.netty.handler.codec.rtsp.RtspResponseStatuses.BAD_REQUEST;
import static io.netty.handler.codec.rtsp.RtspResponseStatuses.MOVED_PERMANENTLY;

import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.alpini.netty4.misc.BasicHttpRequest;
import com.linkedin.alpini.router.api.ExtendedResourcePathParser;
import com.linkedin.alpini.router.api.RouterException;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceStoreIsMigratedException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.api.path.VeniceComputePath;
import com.linkedin.venice.router.api.path.VeniceMultiGetPath;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.api.path.VeniceSingleGetPath;
import com.linkedin.venice.router.exception.VeniceKeyCountLimitException;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.router.streaming.VeniceChunkedWriteHandler;
import com.linkedin.venice.router.utils.VeniceRouterUtils;
import com.linkedin.venice.streaming.StreamingUtils;
import io.netty.channel.ChannelHandlerContext;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;


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
  public static final Pattern STORE_PATTERN = Pattern.compile("\\A[a-zA-Z][a-zA-Z0-9_-]*\\z"); // \A and \z are start
                                                                                               // and end of string
  public static final int STORE_MAX_LENGTH = 128;
  public static final String SEP = "/";

  public static final String TYPE_STORAGE = "storage";
  public static final String TYPE_COMPUTE = "compute";

  // Admin tasks
  public static final String TASK_READ_QUOTA_THROTTLE = "readQuotaThrottle";

  // Admin actions
  public static final String ACTION_ENABLE = "enable";
  public static final String ACTION_DISABLE = "disable";

  // Right now, we hardcoded url path for getting leader controller to be same as the one
  // being used in Venice Controller, so that ControllerClient can use the same API to get
  // leader controller without knowing whether the host is Router or Controller.
  // Without good reason, please don't update this path.
  public static final String TYPE_LEADER_CONTROLLER = ControllerRoute.LEADER_CONTROLLER.getPath().replace("/", "");
  @Deprecated
  public static final String TYPE_LEADER_CONTROLLER_LEGACY =
      ControllerRoute.MASTER_CONTROLLER.getPath().replace("/", "");
  public static final String TYPE_KEY_SCHEMA = RouterResourceType.TYPE_KEY_SCHEMA.toString();
  public static final String TYPE_VALUE_SCHEMA = RouterResourceType.TYPE_VALUE_SCHEMA.toString();
  public static final String TYPE_GET_UPDATE_SCHEMA = RouterResourceType.TYPE_GET_UPDATE_SCHEMA.toString();
  public static final String TYPE_CLUSTER_DISCOVERY = RouterResourceType.TYPE_CLUSTER_DISCOVERY.toString();
  public static final String TYPE_REQUEST_TOPIC = RouterResourceType.TYPE_REQUEST_TOPIC.toString();
  public static final String TYPE_HEALTH_CHECK = RouterResourceType.TYPE_ADMIN.toString();
  public static final String TYPE_ADMIN = RouterResourceType.TYPE_ADMIN.toString(); // Creating a new variable name for
                                                                                    // code sanity
  public static final String TYPE_RESOURCE_STATE = RouterResourceType.TYPE_RESOURCE_STATE.toString();

  private final VeniceVersionFinder versionFinder;
  private final VenicePartitionFinder partitionFinder;
  private final RouterStats<AggRouterHttpRequestStats> routerStats;
  private final ReadOnlyStoreRepository storeRepository;
  private final VeniceRouterConfig routerConfig;
  private final CompressorFactory compressorFactory;

  public VenicePathParser(
      VeniceVersionFinder versionFinder,
      VenicePartitionFinder partitionFinder,
      RouterStats<AggRouterHttpRequestStats> routerStats,
      ReadOnlyStoreRepository storeRepository,
      VeniceRouterConfig routerConfig,
      CompressorFactory compressorFactory) {
    this.versionFinder = versionFinder;
    this.partitionFinder = partitionFinder;
    this.routerStats = routerStats;
    this.storeRepository = storeRepository;
    this.routerConfig = routerConfig;
    this.compressorFactory = compressorFactory;
  };

  @Override
  public VenicePath parseResourceUri(String uri, HTTP_REQUEST request) throws RouterException {
    if (!(request instanceof BasicFullHttpRequest)) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
          Optional.empty(),
          Optional.empty(),
          BAD_GATEWAY,
          "parseResourceUri should receive a BasicFullHttpRequest");
    }
    BasicFullHttpRequest fullHttpRequest = (BasicFullHttpRequest) request;

    VenicePathParserHelper pathHelper = parseRequest(request);
    RouterResourceType resourceType = pathHelper.getResourceType();
    if (resourceType != RouterResourceType.TYPE_STORAGE && resourceType != RouterResourceType.TYPE_COMPUTE) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
          Optional.empty(),
          Optional.empty(),
          BAD_REQUEST,
          "Requested resource type: " + resourceType + " is not a valid type");
    }
    String storeName = pathHelper.getResourceName();
    if (StringUtils.isEmpty(storeName)) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
          Optional.empty(),
          Optional.empty(),
          BAD_REQUEST,
          "Request URI must have storeName.  Uri is: " + uri);
    }

    VenicePath path = null;
    int keyNum = 1;
    try {
      // this method may throw store not exist exception; track the exception under unhealthy request metric
      int version = versionFinder.getVersion(storeName, fullHttpRequest);
      String resourceName = Version.composeKafkaTopic(storeName, version);

      RouterStats<AggRouterHttpRequestStats> stats = routerConfig.isKeyValueProfilingEnabled() ? routerStats : null;

      String method = fullHttpRequest.method().name();
      if (VeniceRouterUtils.isHttpGet(method)) {
        // single-get request
        path =
            new VeniceSingleGetPath(storeName, version, resourceName, pathHelper.getKey(), uri, partitionFinder, stats);
      } else if (VeniceRouterUtils.isHttpPost(method)) {
        if (resourceType == RouterResourceType.TYPE_STORAGE) {
          // multi-get request
          path = new VeniceMultiGetPath(
              storeName,
              version,
              resourceName,
              fullHttpRequest,
              partitionFinder,
              getBatchGetLimit(storeName),
              routerConfig.isSmartLongTailRetryEnabled(),
              routerConfig.getSmartLongTailRetryAbortThresholdMs(),
              stats,
              routerConfig.getLongTailRetryMaxRouteForMultiKeyReq());
        } else if (resourceType == RouterResourceType.TYPE_COMPUTE) {
          // read compute request
          path = new VeniceComputePath(
              storeName,
              version,
              resourceName,
              fullHttpRequest,
              partitionFinder,
              getBatchGetLimit(storeName),
              routerConfig.isSmartLongTailRetryEnabled(),
              routerConfig.getSmartLongTailRetryAbortThresholdMs(),
              routerConfig.isComputeFastAvroEnabled(),
              routerConfig.getLongTailRetryMaxRouteForMultiKeyReq());
        } else {
          throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
              Optional.of(storeName),
              Optional.empty(),
              BAD_REQUEST,
              "The passed in request must be either a GET or " + "be a POST with a resource type of " + TYPE_STORAGE
                  + " or " + TYPE_COMPUTE + ", but instead it was: " + request.toString());
        }
      } else {
        throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
            Optional.empty(),
            Optional.empty(),
            BAD_REQUEST,
            "Method: " + method + " is not allowed");
      }
      RequestType requestType = path.getRequestType();
      if (StreamingUtils.isStreamingEnabled(request)) {
        if (requestType.equals(RequestType.MULTI_GET) || requestType.equals(RequestType.COMPUTE)) {
          // Right now, streaming support is only available for multi-get and compute
          // Extract ChunkedWriteHandler reference
          VeniceChunkedWriteHandler chunkedWriteHandler =
              fullHttpRequest.attr(VeniceChunkedWriteHandler.CHUNKED_WRITE_HANDLER_ATTRIBUTE_KEY).get();
          ChannelHandlerContext ctx =
              fullHttpRequest.attr(VeniceChunkedWriteHandler.CHANNEL_HANDLER_CONTEXT_ATTRIBUTE_KEY).get();
          /**
           * If the streaming is disabled on Router, the following objects will be null since {@link VeniceChunkedWriteHandler}
           * won't be in the pipeline when streaming is disabled, check {@link RouterServer#addStreamingHandler} for more
           * details.
            */
          if (Objects.nonNull(chunkedWriteHandler) && Objects.nonNull(ctx)) {
            // Streaming is enabled
            path.setChunkedWriteHandler(ctx, chunkedWriteHandler, routerStats);
          }
          /**
           * Request type will be changed to streaming request after setting up the proper streaming handler
           */
          requestType = path.getRequestType();
        }
      }

      boolean decompressOnClient = routerConfig.isDecompressOnClient();
      if (decompressOnClient) {
        Store store = storeRepository.getStore(storeName);
        if (store == null) {
          throw new VeniceNoStoreException(storeName);
        }
        decompressOnClient = store.getClientDecompressionEnabled();
      }

      // TODO: maybe we should use the builder pattern here??
      // Setup decompressor
      VeniceResponseDecompressor responseDecompressor = new VeniceResponseDecompressor(
          decompressOnClient,
          routerStats,
          fullHttpRequest,
          storeName,
          version,
          compressorFactory);
      path.setResponseDecompressor(responseDecompressor);

      AggRouterHttpRequestStats aggRouterHttpRequestStats = routerStats.getStatsByType(requestType);
      if (!requestType.equals(SINGLE_GET)) {
        /**
         * Here we only track key num for non single-get request, since single-get request will be always 1.
         */
        keyNum = path.getPartitionKeys().size();
        aggRouterHttpRequestStats.recordKeyNum(storeName, keyNum);
      }

      aggRouterHttpRequestStats.recordRequest(storeName);
      aggRouterHttpRequestStats.recordRequestSize(storeName, path.getRequestSize());
    } catch (VeniceException e) {
      Optional<RequestType> requestTypeOptional =
          (path == null) ? Optional.empty() : Optional.of(path.getRequestType());
      if (e instanceof VeniceStoreIsMigratedException) {
        throw RouterExceptionAndTrackingUtils
            .newRouterExceptionAndTracking(Optional.of(storeName), Optional.empty(), MOVED_PERMANENTLY, e.getMessage());
      }
      if (e instanceof VeniceKeyCountLimitException) {
        VeniceKeyCountLimitException keyCountLimitException = (VeniceKeyCountLimitException) e;
        requestTypeOptional = Optional.of(keyCountLimitException.getRequestType());
        routerStats.getStatsByType(keyCountLimitException.getRequestType())
            .recordBadRequestKeyCount(
                keyCountLimitException.getStoreName(),
                keyCountLimitException.getRequestKeyCount());
      }
      /**
       * Tracking the bad requests in {@link RouterExceptionAndTrackingUtils} by logging and metrics.
       */
      throw RouterExceptionAndTrackingUtils
          .newRouterExceptionAndTracking(Optional.of(storeName), requestTypeOptional, BAD_REQUEST, e.getMessage());
    } finally {
      // Always record request usage in the single get stats, so we could compare it with the quota easily.
      // Right now we use key num as request usage, in the future we might consider the Capacity unit.
      routerStats.getStatsByType(SINGLE_GET).recordRequestUsage(storeName, keyNum);
    }

    return path;
  }

  @Override
  public VenicePath parseResourceUri(String uri) throws RouterException {
    throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
        Optional.empty(),
        Optional.empty(),
        BAD_REQUEST,
        "parseResourceUri without param: request should not be invoked");
  }

  @Override
  public VenicePath substitutePartitionKey(VenicePath path, RouterKey s) {
    return path.substitutePartitionKey(s);
  }

  @Override
  public VenicePath substitutePartitionKey(VenicePath path, Collection<RouterKey> s) {
    return path.substitutePartitionKey(s);
  }

  public static boolean isStoreNameValid(String storeName) {
    if (storeName.length() > STORE_MAX_LENGTH) {
      return false;
    }
    Matcher m = STORE_PATTERN.matcher(storeName);
    return m.matches();
  }

  private int getBatchGetLimit(String storeName) {
    int batchGetLimit = storeRepository.getBatchGetLimit(storeName);
    if (batchGetLimit <= 0) {
      batchGetLimit = routerConfig.getMaxKeyCountInMultiGetReq();
    }
    return batchGetLimit;
  }

}
