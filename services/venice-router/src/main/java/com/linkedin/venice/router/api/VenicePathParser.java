package com.linkedin.venice.router.api;

import static com.linkedin.venice.HttpConstants.VENICE_CLIENT_COMPUTE_FALSE;
import static com.linkedin.venice.HttpConstants.VENICE_CLIENT_COMPUTE_TRUE;
import static com.linkedin.venice.HttpConstants.VENICE_SUPPORTED_COMPRESSION_STRATEGY;
import static com.linkedin.venice.read.RequestType.SINGLE_GET;
import static com.linkedin.venice.router.api.VenicePathParserHelper.parseRequest;
import static com.linkedin.venice.router.api.VeniceResponseDecompressor.getCompressionStrategy;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;
import static io.netty.handler.codec.rtsp.RtspResponseStatuses.BAD_REQUEST;
import static io.netty.handler.codec.rtsp.RtspResponseStatuses.MOVED_PERMANENTLY;

import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.alpini.router.api.ExtendedResourcePathParser;
import com.linkedin.alpini.router.api.RouterException;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceStoreIsMigratedException;
import com.linkedin.venice.meta.NameRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RetryManager;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.StoreName;
import com.linkedin.venice.meta.StoreVersionName;
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
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.tehuti.metrics.MetricsRepository;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;


/**
 *   Inbound single get request to the router will look like:
 *   GET /storage/storeName/key?f=fmt
 *
 *   'storage' is a literal, meaning we will request the value for a single key
 *   storeName will be the name of the requested store
 *   key is the key being looked up
 *   fmt is an optional format parameter, one of 'string' or 'b64'. If omitted, assumed to be 'string'
 *
 *   Batch get requests look like:
 *   POST /storage/storeName
 *
 *   And the keys are concatenated in the POST body.
 *
 *   The VenicePathParser is responsible for looking up the active version of the store, and constructing the store-version
 */
public class VenicePathParser implements ExtendedResourcePathParser<VenicePath, RouterKey, BasicFullHttpRequest> {
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

  public static final String TYPE_CURRENT_VERSION = RouterResourceType.TYPE_CURRENT_VERSION.toString();

  public static final String TYPE_BLOB_DISCOVERY = RouterResourceType.TYPE_BLOB_DISCOVERY.toString();

  private static final String SINGLE_KEY_RETRY_MANAGER_STATS_PREFIX = "single-key-long-tail-retry-manager-";
  private static final String MULTI_KEY_RETRY_MANAGER_STATS_PREFIX = "multi-key-long-tail-retry-manager-";

  private final VeniceVersionFinder versionFinder;
  private final VenicePartitionFinder partitionFinder;
  private final RouterStats<AggRouterHttpRequestStats> routerStats;
  private final ReadOnlyStoreRepository storeRepository;
  private final VeniceRouterConfig routerConfig;
  private final CompressorFactory compressorFactory;
  private final MetricsRepository metricsRepository;
  private final ScheduledExecutorService retryManagerScheduler;
  private final Map<StoreName, RetryManager> routerSingleKeyRetryManagers;
  private final Map<StoreName, RetryManager> routerMultiKeyRetryManagers;
  private final NameRepository nameRepository;
  /** Outer map of {client's desired compression strategy -> inner map of {store-version -> decompressor}} */
  private final EnumMap<CompressionStrategy, Map<StoreVersionName, VeniceResponseDecompressor>> decompressorMaps;

  public VenicePathParser(
      VeniceVersionFinder versionFinder,
      VenicePartitionFinder partitionFinder,
      RouterStats<AggRouterHttpRequestStats> routerStats,
      ReadOnlyStoreRepository storeRepository,
      VeniceRouterConfig routerConfig,
      CompressorFactory compressorFactory,
      MetricsRepository metricsRepository,
      ScheduledExecutorService retryManagerScheduler,
      NameRepository nameRepository) {
    this.versionFinder = versionFinder;
    this.partitionFinder = partitionFinder;
    this.routerStats = routerStats;
    this.storeRepository = storeRepository;
    this.decompressorMaps = new EnumMap(CompressionStrategy.class);
    for (CompressionStrategy compressionStrategy: CompressionStrategy.values()) {
      this.decompressorMaps.put(compressionStrategy, new VeniceConcurrentHashMap<>());
    }
    this.nameRepository = nameRepository;
    this.storeRepository.registerStoreDataChangedListener(new StoreDataChangedListener() {
      @Override
      public void handleStoreDeleted(String storeNameString) {
        StoreName storeName = VenicePathParser.this.nameRepository.getStoreName(storeNameString);
        routerSingleKeyRetryManagers.remove(storeName);
        routerMultiKeyRetryManagers.remove(storeName);
        cleanDecompressorMaps(storeName, storeVersionName -> true);
      }

      @Override
      public void handleStoreChanged(Store store) {
        StoreName storeName = VenicePathParser.this.nameRepository.getStoreName(store.getName());
        IntSet upToDateVersionsSet = store.getVersionNumbers();
        cleanDecompressorMaps(
            storeName,
            storeVersionName -> !upToDateVersionsSet.contains(storeVersionName.getVersionNumber()));
      }

      private void cleanDecompressorMaps(StoreName storeName, Function<StoreVersionName, Boolean> criteriaForRemoval) {
        for (Map<StoreVersionName, VeniceResponseDecompressor> decompressorMap: decompressorMaps.values()) {
          // remove out dated versions (if any) from the map
          Iterator<StoreVersionName> storeVersionNameIterator = decompressorMap.keySet().iterator();
          StoreVersionName storeVersionName;
          while (storeVersionNameIterator.hasNext()) {
            storeVersionName = storeVersionNameIterator.next();
            if (storeVersionName.getStore().equals(storeName)) {
              if (criteriaForRemoval.apply(storeVersionName)) {
                storeVersionNameIterator.remove();
              }
            }
          }
        }
      }
    });
    this.routerConfig = routerConfig;
    this.compressorFactory = compressorFactory;
    this.metricsRepository = metricsRepository;
    this.retryManagerScheduler = retryManagerScheduler;
    this.routerSingleKeyRetryManagers = new VeniceConcurrentHashMap<>();
    this.routerMultiKeyRetryManagers = new VeniceConcurrentHashMap<>();
  }

  @Override
  public VenicePath parseResourceUri(String uri, BasicFullHttpRequest fullHttpRequest) throws RouterException {
    VenicePathParserHelper pathHelper = parseRequest(fullHttpRequest);
    RouterResourceType resourceType = pathHelper.getResourceType();
    if (resourceType != RouterResourceType.TYPE_STORAGE && resourceType != RouterResourceType.TYPE_COMPUTE) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
          null,
          null,
          BAD_REQUEST,
          "Requested resource type: " + resourceType + " is not a valid type");
    }
    String storeName = pathHelper.getResourceName();
    if (StringUtils.isEmpty(storeName)) {
      throw RouterExceptionAndTrackingUtils
          .newRouterExceptionAndTracking(null, null, BAD_REQUEST, "Request URI must have storeName.  Uri is: " + uri);
    }

    VenicePath path = null;
    int keyNum = 1;
    try {
      // this method may throw store not exist exception; track the exception under unhealthy request metric
      int version = versionFinder.getVersion(storeName, fullHttpRequest);
      StoreVersionName storeVersionName = this.nameRepository.getStoreVersionName(storeName, version);
      VeniceResponseDecompressor responseDecompressor = getDecompressor(storeVersionName, fullHttpRequest);
      String method = fullHttpRequest.method().name();

      if (VeniceRouterUtils.isHttpGet(method)) {
        RetryManager singleKeyRetryManager = routerSingleKeyRetryManagers.computeIfAbsent(
            storeVersionName.getStore(),
            ignored -> new RetryManager(
                metricsRepository,
                SINGLE_KEY_RETRY_MANAGER_STATS_PREFIX + storeName,
                routerConfig.getLongTailRetryBudgetEnforcementWindowInMs(),
                routerConfig.getSingleKeyLongTailRetryBudgetPercentDecimal(),
                retryManagerScheduler));
        // single-get request
        path = new VeniceSingleGetPath(
            storeVersionName,
            pathHelper.getKey(),
            uri,
            partitionFinder,
            routerStats,
            routerConfig,
            singleKeyRetryManager,
            responseDecompressor);
      } else if (VeniceRouterUtils.isHttpPost(method)) {
        RetryManager multiKeyRetryManager = routerMultiKeyRetryManagers.computeIfAbsent(
            storeVersionName.getStore(),
            ignored -> new RetryManager(
                metricsRepository,
                MULTI_KEY_RETRY_MANAGER_STATS_PREFIX + storeName,
                routerConfig.getLongTailRetryBudgetEnforcementWindowInMs(),
                routerConfig.getMultiKeyLongTailRetryBudgetPercentDecimal(),
                retryManagerScheduler));
        boolean isReadComputationEnabled = storeRepository.isReadComputationEnabled(storeName);
        if (resourceType == RouterResourceType.TYPE_STORAGE) {
          // multi-get request
          path = new VeniceMultiGetPath(
              storeVersionName,
              fullHttpRequest,
              partitionFinder,
              getBatchGetLimit(storeName),
              routerStats.getStatsByType(RequestType.MULTI_GET),
              routerConfig,
              multiKeyRetryManager,
              responseDecompressor,
              isReadComputationEnabled ? VENICE_CLIENT_COMPUTE_FALSE : VENICE_CLIENT_COMPUTE_TRUE);
        } else if (resourceType == RouterResourceType.TYPE_COMPUTE) {
          // read compute request
          VeniceComputePath computePath = new VeniceComputePath(
              storeVersionName,
              fullHttpRequest,
              partitionFinder,
              getBatchGetLimit(storeName),
              routerStats.getStatsByType(RequestType.COMPUTE),
              routerConfig,
              multiKeyRetryManager,
              responseDecompressor);

          if (isReadComputationEnabled) {
            path = computePath;
          } else {
            if (!fullHttpRequest.headers().contains(HttpConstants.VENICE_CLIENT_COMPUTE)) {
              throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
                  storeName,
                  computePath.getRequestType(),
                  METHOD_NOT_ALLOWED,
                  "Read compute is not enabled for the store. Please contact Venice team to enable the feature.");
            }
            path = computePath.toMultiGetPath();
            routerStats.getStatsByType(RequestType.COMPUTE)
                .recordMultiGetFallback(storeName, path.getPartitionKeys().size());
          }
        } else {
          throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
              storeName,
              null,
              BAD_REQUEST,
              "The passed in request must be either a GET or " + "be a POST with a resource type of " + TYPE_STORAGE
                  + " or " + TYPE_COMPUTE + ", but instead it was: " + fullHttpRequest.toString());
        }
      } else {
        throw RouterExceptionAndTrackingUtils
            .newRouterExceptionAndTracking(null, null, BAD_REQUEST, "Method: " + method + " is not allowed");
      }
      RequestType requestType = path.getRequestType();
      if (StreamingUtils.isStreamingEnabled(fullHttpRequest)) {
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
      RequestType requestType = path == null ? null : path.getRequestType();
      HttpResponseStatus responseStatus = BAD_REQUEST;
      if (e instanceof VeniceStoreIsMigratedException) {
        requestType = null;
        responseStatus = MOVED_PERMANENTLY;
      }
      if (e instanceof VeniceKeyCountLimitException) {
        VeniceKeyCountLimitException keyCountLimitException = (VeniceKeyCountLimitException) e;
        requestType = keyCountLimitException.getRequestType();
        responseStatus = REQUEST_ENTITY_TOO_LARGE;
        routerStats.getStatsByType(keyCountLimitException.getRequestType())
            .recordBadRequestKeyCount(
                keyCountLimitException.getStoreName(),
                responseStatus,
                keyCountLimitException.getRequestKeyCount());
      }
      /**
       * Tracking the bad requests in {@link RouterExceptionAndTrackingUtils} by logging and metrics.
       */
      throw RouterExceptionAndTrackingUtils
          .newRouterExceptionAndTracking(storeName, requestType, responseStatus, e.getMessage());
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
        null,
        null,
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

  /** Package-private for test purposes */
  VeniceResponseDecompressor getDecompressor(StoreVersionName storeVersionName, HttpRequest request) {
    boolean decompressOnClient = this.routerConfig.isDecompressOnClient();
    if (decompressOnClient) {
      Store store = this.storeRepository.getStoreOrThrow(storeVersionName.getStoreName());
      decompressOnClient = store.getClientDecompressionEnabled();
    }

    CompressionStrategy clientCompression = decompressOnClient
        ? getCompressionStrategy(request.headers().get(VENICE_SUPPORTED_COMPRESSION_STRATEGY))
        : CompressionStrategy.NO_OP;

    return this.decompressorMaps.get(clientCompression)
        .computeIfAbsent(
            storeVersionName,
            key -> new VeniceResponseDecompressor(
                clientCompression,
                this.routerStats,
                storeVersionName,
                this.compressorFactory));
  }
}
