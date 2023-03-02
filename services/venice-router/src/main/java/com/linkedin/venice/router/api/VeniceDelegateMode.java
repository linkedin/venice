package com.linkedin.venice.router.api;

import static com.linkedin.venice.read.RequestType.COMPUTE_STREAMING;
import static com.linkedin.venice.read.RequestType.MULTI_GET_STREAMING;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;

import com.linkedin.alpini.base.misc.Metrics;
import com.linkedin.alpini.router.api.HostFinder;
import com.linkedin.alpini.router.api.HostHealthMonitor;
import com.linkedin.alpini.router.api.PartitionFinder;
import com.linkedin.alpini.router.api.ResourcePath;
import com.linkedin.alpini.router.api.RouterException;
import com.linkedin.alpini.router.api.Scatter;
import com.linkedin.alpini.router.api.ScatterGatherMode;
import com.linkedin.alpini.router.api.ScatterGatherRequest;
import com.linkedin.venice.exceptions.QuotaExceededException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.api.routing.helix.HelixGroupSelector;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouteHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.router.throttle.RouterThrottler;
import com.linkedin.venice.utils.HelixUtils;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.annotation.Nonnull;


/**
 * This class contains all the {@link ScatterGatherMode} being used in Venice Router.
 * IMPORTANT!!!
 * In {@link #scatter} function, only {@link RouterException} is expected, otherwise, the netty buffer leaking issue
 * will happen.
 * This vulnerability is related to Alpini since {@link ScatterGatherMode#scatter} only catches {@link RouterException} to
 * return the exceptional future, otherwise, that function will throw exception to miss the release operation in {@text com.linkedin.alpini.router.ScatterGatherRequestHandlerImpl#prepareRetry}.
 * Here are the details:
 * 1. If the current implementation of {@link #scatter} throws other exceptions than {@link RouterException}, {@link ScatterGatherMode#scatter}
 *    will rethrow the exception instead of returning an exceptional future.
 * 2. For long-tail retry, {@text com.linkedin.alpini.router.ScatterGatherRequestHandlerImpl#prepareRetry} will retain the request
 *    every time, and it will try to release the request in the handling function of "_scatterGatherHelper.scatter" in
 *    {@text com.linkedin.alpini.router.ScatterGatherRequestHandlerImpl#prepareRetry}.
 * 3. If #1 happens, the handling function mentioned in #2 won't be invoked, which means the release won't happen, and this
 *    is causing the request leaking.
 * TODO: maybe we should improve DDS lib to catch all kinds of exception in {@link ScatterGatherMode#scatter} to avoid
 * this potential leaking issue.
 */
public class VeniceDelegateMode extends ScatterGatherMode {
  /**
   * This mode will route single get to the least loaded replica.
   */
  private final ScatterGatherMode LEAST_LOADED_MODE_FOR_SINGLE_GET = new LeastLoadedModeForSingleGet();

  /**
   * This mode will group all requests to the same host into a single request.  Hosts are selected as the first host returned
   * by the VeniceHostFinder, so we must shuffle the order to get an even distribution.
   */
  private static final ScatterGatherMode GROUP_BY_PRIMARY_HOST_MODE_FOR_MULTI_KEY_REQUEST =
      ScatterGatherMode.GROUP_BY_PRIMARY_HOST;

  /**
   * This mode will do the aggregation per host first, and then initiate a request per host.
   */
  private static final ScatterGatherMode GROUP_BY_GREEDY_MODE_FOR_MULTI_KEY_REQUEST =
      ScatterGatherMode.GROUP_BY_GREEDY_HOST;

  /**
   * Least loaded replica routing to avoid requests keeping hitting a busy/slow node.
   */
  private final ScatterGatherMode LEAST_LOADED_MODE_FOR_MULTI_KEY_REQUEST =
      new LeastLoadedRoutingModeForMultiKeyRequest();

  /**
   * Helix assisted routing to limit the fanout size for the large fanout use cases.
   */
  private final ScatterGatherMode HELIX_ASSISTED_MODE_FOR_MULTI_KEY_REQUEST = new HelixAssistedScatterGatherMode();

  private RouterThrottler readRequestThrottler;
  private RouteHttpRequestStats routeHttpRequestStats;

  private HelixGroupSelector helixGroupSelector;

  private final VeniceMultiKeyRoutingStrategy multiKeyRoutingStrategy;
  private final ScatterGatherMode scatterGatherModeForMultiKeyRequest;
  private final RouterStats<AggRouterHttpRequestStats> routerStats;

  public VeniceDelegateMode(
      VeniceRouterConfig config,
      RouterStats<AggRouterHttpRequestStats> routerStats,
      RouteHttpRequestStats routeHttpRequestStats) {
    super("VENICE_DELEGATE_MODE", false);
    this.routerStats = routerStats;
    this.routeHttpRequestStats = routeHttpRequestStats;
    this.multiKeyRoutingStrategy = config.getMultiKeyRoutingStrategy();
    switch (this.multiKeyRoutingStrategy) {
      case GROUP_BY_PRIMARY_HOST_ROUTING:
        this.scatterGatherModeForMultiKeyRequest = GROUP_BY_PRIMARY_HOST_MODE_FOR_MULTI_KEY_REQUEST;
        break;
      case GREEDY_ROUTING:
        this.scatterGatherModeForMultiKeyRequest = GROUP_BY_GREEDY_MODE_FOR_MULTI_KEY_REQUEST;
        break;
      case LEAST_LOADED_ROUTING:
        this.scatterGatherModeForMultiKeyRequest = LEAST_LOADED_MODE_FOR_MULTI_KEY_REQUEST;
        break;
      case HELIX_ASSISTED_ROUTING:
        this.scatterGatherModeForMultiKeyRequest = HELIX_ASSISTED_MODE_FOR_MULTI_KEY_REQUEST;
        break;
      default:
        throw new VeniceException("Unknown multi-key routing strategy: " + this.multiKeyRoutingStrategy);
    }
  }

  public void initReadRequestThrottler(RouterThrottler requestThrottler) {
    this.readRequestThrottler = requestThrottler;
  }

  public void initHelixGroupSelector(HelixGroupSelector helixGroupSelector) {
    if (this.helixGroupSelector != null) {
      throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(
          Optional.empty(),
          Optional.empty(),
          INTERNAL_SERVER_ERROR,
          "HelixGroupSelector has already been initialized before, and no further update expected!");
    }
    this.helixGroupSelector = helixGroupSelector;
  }

  @Nonnull
  @Override
  public <H, P extends ResourcePath<K>, K, R> Scatter<H, P, K> scatter(
      @Nonnull Scatter<H, P, K> scatter,
      @Nonnull String requestMethod,
      @Nonnull String resourceName,
      @Nonnull PartitionFinder<K> partitionFinder,
      @Nonnull HostFinder<H, R> hostFinder,
      @Nonnull HostHealthMonitor<H> hostHealthMonitor,
      @Nonnull R roles,
      Metrics metrics) throws RouterException {
    if (readRequestThrottler == null) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
          Optional.empty(),
          Optional.empty(),
          INTERNAL_SERVER_ERROR,
          "Read request throttler has not been setup yet");
    }
    if (multiKeyRoutingStrategy.equals(VeniceMultiKeyRoutingStrategy.HELIX_ASSISTED_ROUTING)
        && helixGroupSelector == null) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
          Optional.empty(),
          Optional.empty(),
          INTERNAL_SERVER_ERROR,
          "HelixGroupSelector has not been setup yet");
    }
    P path = scatter.getPath();
    if (!(path instanceof VenicePath)) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
          Optional.empty(),
          Optional.empty(),
          INTERNAL_SERVER_ERROR,
          "VenicePath is expected, but received " + path.getClass());
    }
    VenicePath venicePath = (VenicePath) path;
    String storeName = venicePath.getStoreName();
    if (venicePath.isRetryRequest()) {
      /**
       * The following logic is to measure the actual retry delay.
       */
      long retryDelay = System.currentTimeMillis() - venicePath.getOriginalRequestStartTs();
      routerStats.getStatsByType(venicePath.getRequestType()).recordRetryDelay(storeName, retryDelay);
    }

    // Check whether retry request is too late or not
    if (venicePath.isRetryRequestTooLate()) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
          Optional.of(storeName),
          Optional.of(venicePath.getRequestType()),
          SERVICE_UNAVAILABLE,
          "The retry request aborted because of delay constraint of smart long-tail retry",
          RouterExceptionAndTrackingUtils.FailureType.SMART_RETRY_ABORTED_BY_DELAY_CONSTRAINT);
    }
    ScatterGatherMode scatterMode;
    switch (venicePath.getRequestType()) {
      case MULTI_GET:
      case MULTI_GET_STREAMING:
      case COMPUTE:
      case COMPUTE_STREAMING:
        scatterMode = scatterGatherModeForMultiKeyRequest;
        break;
      case SINGLE_GET:
        scatterMode = LEAST_LOADED_MODE_FOR_SINGLE_GET;
        break;
      default:
        throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
            Optional.of(storeName),
            Optional.of(venicePath.getRequestType()),
            INTERNAL_SERVER_ERROR,
            "Unknown request type: " + venicePath.getRequestType());
    }
    Scatter finalScatter = scatterMode
        .scatter(scatter, requestMethod, resourceName, partitionFinder, hostFinder, hostHealthMonitor, roles, metrics);
    int offlineRequestNum = scatter.getOfflineRequestCount();
    int onlineRequestNum = scatter.getOnlineRequestCount();

    if (offlineRequestNum > 0) {
      // For streaming request do not reject request as long as there is some replica available to serve some keys.
      if (onlineRequestNum != 0
          && (venicePath.getRequestType() == MULTI_GET_STREAMING || venicePath.getRequestType() == COMPUTE_STREAMING)) {
        RouterExceptionAndTrackingUtils
            .recordUnavailableReplicaStreamingRequest(storeName, venicePath.getRequestType());
      } else {
        Collection<ScatterGatherRequest<H, K>> offlineRequests = scatter.getOfflineRequests();
        StringBuilder partitions = new StringBuilder();

        for (ScatterGatherRequest scatterGatherRequest: offlineRequests) {
          partitions.append(scatterGatherRequest.getPartitionsNames());
        }
        RouterExceptionAndTrackingUtils.FailureType failureType = RouterExceptionAndTrackingUtils.FailureType.REGULAR;
        if (venicePath.isRetryRequest()) {
          // don't record it as unhealthy request.
          failureType = RouterExceptionAndTrackingUtils.FailureType.RETRY_ABORTED_BY_NO_AVAILABLE_REPLICA;
        }
        String isRetry = venicePath.isRetryRequest() ? "retry " : "";
        String errMsg = "Partitions : " + partitions + " not available to serve " + isRetry + "request of type: "
            + venicePath.getRequestType();
        throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
            Optional.of(storeName),
            Optional.of(venicePath.getRequestType()),
            SERVICE_UNAVAILABLE,
            errMsg,
            failureType);
      }
    }

    for (ScatterGatherRequest<H, K> part: scatter.getOnlineRequests()) {
      int hostCount = part.getHosts().size();
      if (hostCount == 0) {
        throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
            Optional.of(storeName),
            Optional.of(venicePath.getRequestType()),
            SERVICE_UNAVAILABLE,
            "Could not find ready-to-serve replica for request: " + part);
      }
      H host = part.getHosts().get(0);
      if (hostCount > 1) {
        List<H> hosts = part.getHosts();
        host = hosts.get((int) (System.currentTimeMillis() % hostCount)); // cheap random host selection
        // Update host selection
        // The downstream (VeniceDispatcher) will only expect one host for a given scatter request.
        H finalHost = host;
        hosts.removeIf(aHost -> !aHost.equals(finalHost));
      }
      if (!(host instanceof Instance)) {
        throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
            Optional.of(storeName),
            Optional.of(venicePath.getRequestType()),
            INTERNAL_SERVER_ERROR,
            "Ready-to-serve host must be an 'Instance'");
      }
      Instance veniceInstance = (Instance) host;

      if (!venicePath.isRetryRequest()) {
        /**
         * Here is the only suitable place to throttle multi-get/compute request since we want to fail the whole request
         * if some scatter request gets throttled.
         *
         * Venice doesn't apply quota enforcement for retry request since retry is a way for latency guarantee,
         * which should be transparent to customers.
         */
        int keyCount = part.getPartitionKeys().size();
        try {
          readRequestThrottler.mayThrottleRead(
              storeName,
              keyCount * readRequestThrottler.getReadCapacity(),
              veniceInstance.getNodeId());
        } catch (QuotaExceededException e) {
          /**
           * Exception thrown here won't go through {@link VeniceResponseAggregator}, and DDS lib will return an error response
           * with the corresponding response status directly.
           */
          throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
              Optional.of(storeName),
              Optional.of(venicePath.getRequestType()),
              TOO_MANY_REQUESTS,
              "Quota exceeded for '" + storeName + "' while serving a " + venicePath.getRequestType()
                  + " request! msg: " + e.getMessage());
        }
      }
    }

    if (venicePath.isRetryRequest()) {
      // Check whether the retry request is allowed or not according to the max allowed retry route config
      if (!venicePath.isLongTailRetryAllowedForNewRoute()) {
        routerStats.getStatsByType(venicePath.getRequestType()).recordDisallowedRetryRequest(storeName);
        throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
            Optional.of(storeName),
            Optional.of(venicePath.getRequestType()),
            SERVICE_UNAVAILABLE,
            "The retry request aborted because there are too many retries for current request",
            RouterExceptionAndTrackingUtils.FailureType.SMART_RETRY_ABORTED_BY_MAX_RETRY_ROUTE_LIMIT);
      } else {
        routerStats.getStatsByType(venicePath.getRequestType()).recordAllowedRetryRequest(storeName);
      }
    }

    return finalScatter;
  }

  // Select host with the least pending queue depth.
  private <H> H selectLeastLoadedHost(List<H> hosts, VenicePath path) throws RouterException {
    H host;
    long minCount = Long.MAX_VALUE;
    H minHost = null;
    for (H h: hosts) {
      Instance node = (Instance) h;
      if (!path.canRequestStorageNode(node.getNodeId()))
        continue;
      long pendingRequestCount = routeHttpRequestStats.getPendingRequestCount(node.getNodeId());
      if (pendingRequestCount < minCount) {
        minCount = pendingRequestCount;
        minHost = h;
      }
    }
    if (minHost == null) {
      if (path.isRetryRequest()) {
        throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
            Optional.of(path.getStoreName()),
            Optional.of(path.getRequestType()),
            SERVICE_UNAVAILABLE,
            "Retry request aborted because of slow route for request path: " + path.getResourceName(),
            RouterExceptionAndTrackingUtils.FailureType.SMART_RETRY_ABORTED_BY_SLOW_ROUTE);
      } else {
        throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
            Optional.of(path.getStoreName()),
            Optional.of(path.getRequestType()),
            SERVICE_UNAVAILABLE,
            "Could not find ready-to-serve replica for request path: " + path.getResourceName());
      }
    }
    H finalHost = minHost;
    hosts.removeIf(aHost -> !aHost.equals(finalHost));
    host = finalHost;
    return host;
  }

  /**
   * This mode route the request to the least loaded replica for single get.
   */
  class LeastLoadedModeForSingleGet extends ScatterGatherMode {
    protected LeastLoadedModeForSingleGet() {
      super("LEAST_LOADED_MODE_FOR_SINGLE_GET", false);
    }

    @Nonnull
    @Override
    public <H, P extends ResourcePath<K>, K, R> Scatter<H, P, K> scatter(
        @Nonnull Scatter<H, P, K> scatter,
        @Nonnull String requestMethod,
        @Nonnull String resourceName,
        @Nonnull PartitionFinder<K> partitionFinder,
        @Nonnull HostFinder<H, R> hostFinder,
        @Nonnull HostHealthMonitor<H> hostHealthMonitor,
        @Nonnull R roles,
        Metrics metrics) throws RouterException {
      P path = scatter.getPath();
      if (!(path instanceof VenicePath)) {
        throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
            Optional.empty(),
            Optional.empty(),
            INTERNAL_SERVER_ERROR,
            "VenicePath is expected, but received " + path.getClass());
      }
      K key = path.getPartitionKey();
      String partitionName = partitionFinder.findPartitionName(resourceName, key);
      List<H> hosts = hostFinder.findHosts(requestMethod, resourceName, partitionName, hostHealthMonitor, roles);
      SortedSet<K> keySet = new TreeSet<>();
      keySet.add(key);
      if (hosts.isEmpty()) {
        scatter.addOfflineRequest(new ScatterGatherRequest<>(Collections.emptyList(), keySet, partitionName));
      } else if (hosts.size() > 1) {
        VenicePath venicePath = (VenicePath) path;

        H host = selectLeastLoadedHost(hosts, venicePath);

        scatter.addOnlineRequest(new ScatterGatherRequest<>(Collections.singletonList(host), keySet, partitionName));
      } else {
        scatter.addOnlineRequest(new ScatterGatherRequest<>(hosts, keySet, partitionName));
      }
      return scatter;
    }
  }

  abstract class ScatterGatherModeForMultiKeyRequest extends ScatterGatherMode {
    /**
     * This class contains all the partitions/keys belonging to the same host.
     */
    class KeyPartitionSet<H, K> {
      public TreeSet<K> keySet = new TreeSet<>();
      public Set<String> partitionNames = new HashSet<>();
      public List<H> hosts;

      public KeyPartitionSet(List<H> hosts) {
        this.hosts = hosts;
      }

      public void addKeyPartitions(List<K> keys, String partitionName) {
        this.keySet.addAll(keys);
        this.partitionNames.add(partitionName);
      }

      public void addKeyPartition(K key, String partitionName) {
        this.keySet.add(key);
        this.partitionNames.add(partitionName);
      }
    }

    protected ScatterGatherModeForMultiKeyRequest(@Nonnull String name) {
      super(name, false);
    }

    /**
     * This function is used to select a host if there are multiple healthy replicas for the given partition.
     * @throws RouterException
     */
    protected abstract <H, K> void selectHostForPartition(
        String partitionName,
        List<H> partitionReplicas,
        List<K> partitionKeys,
        VenicePath venicePath,
        Map<H, KeyPartitionSet<H, K>> hostMap,
        Optional<Integer> helixGroupNum,
        Optional<Integer> assignedHelixGroupId) throws RouterException;

    /**
     * This method is for {@link HelixAssistedScatterGatherMode}.
     * @return
     */
    protected Optional<Integer> getHelixGroupNum() {
      return Optional.empty();
    }

    /**
     * This method is for {@link HelixAssistedScatterGatherMode}.
     * @return
     */
    protected Optional<Integer> getAssignedHelixGroupId(VenicePath venicePath) {
      return Optional.empty();
    }

    @Nonnull
    @Override
    public <H, P extends ResourcePath<K>, K, R> Scatter<H, P, K> scatter(
        @Nonnull Scatter<H, P, K> scatter,
        @Nonnull String requestMethod,
        @Nonnull String resourceName,
        @Nonnull PartitionFinder<K> partitionFinder,
        @Nonnull HostFinder<H, R> hostFinder,
        @Nonnull HostHealthMonitor<H> hostHealthMonitor,
        @Nonnull R roles,
        Metrics metrics) throws RouterException {
      P path = scatter.getPath();
      if (!(path instanceof VenicePath)) {
        throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
            Optional.empty(),
            Optional.empty(),
            INTERNAL_SERVER_ERROR,
            "VenicePath is expected, but received " + path.getClass());
      }
      VenicePath venicePath = (VenicePath) path;
      /**
       * Group by partition
       *
       * Using simple data structure, such as {@link LinkedList} instead of {@link SortedSet} to speed up operation,
       * which is helpful for large batch-get use case.
       */
      Map<Integer, List<K>> partitionKeys = new HashMap<>();
      for (K key: scatter.getPath().getPartitionKeys()) {
        int partitionId = ((RouterKey) key).getPartitionId();
        List<K> partitionKeyList = partitionKeys.computeIfAbsent(partitionId, k -> new LinkedList<>());
        partitionKeyList.add(key);
      }

      /**
       * Group by host
       */
      Map<H, KeyPartitionSet<H, K>> hostMap = new HashMap<>();
      Optional<Integer> helixGroupNum = getHelixGroupNum();
      Optional<Integer> assignedHelixGroupId = getAssignedHelixGroupId(venicePath);
      for (Map.Entry<Integer, List<K>> entry: partitionKeys.entrySet()) {
        String partitionName = HelixUtils.getPartitionName(resourceName, entry.getKey());
        List<H> hosts = hostFinder.findHosts(requestMethod, resourceName, partitionName, hostHealthMonitor, roles);

        if (hosts.isEmpty()) {
          scatter.addOfflineRequest(
              new ScatterGatherRequest<>(Collections.emptyList(), new TreeSet<>(entry.getValue()), partitionName));
        } else if (hosts.size() == 1) {
          H host = hosts.get(0);
          KeyPartitionSet<H, K> keyPartitionSet = hostMap.get(host);
          if (keyPartitionSet == null) {
            keyPartitionSet = new KeyPartitionSet<>(Collections.singletonList(host));
            hostMap.put(host, keyPartitionSet);
          }
          keyPartitionSet.addKeyPartitions(entry.getValue(), partitionName);
        } else {
          try {
            selectHostForPartition(
                partitionName,
                hosts,
                entry.getValue(),
                venicePath,
                hostMap,
                helixGroupNum,
                assignedHelixGroupId);
          } catch (RouterException e) {
            /**
             * We don't want to throw exception here to fail the whole request since for streaming, partial scatter is acceptable.
             */
            scatter.addOfflineRequest(
                new ScatterGatherRequest<>(Collections.emptyList(), new TreeSet<>(entry.getValue()), partitionName));
          }
        }
      }

      /**
       * Populate online requests
       */
      for (Map.Entry<H, KeyPartitionSet<H, K>> entry: hostMap.entrySet()) {
        scatter.addOnlineRequest(
            new ScatterGatherRequest<>(
                entry.getValue().hosts,
                entry.getValue().keySet,
                entry.getValue().partitionNames));
      }

      return scatter;
    }
  }

  /**
   * This mode route the request to the least loaded replica that's available.
   */
  class LeastLoadedRoutingModeForMultiKeyRequest extends ScatterGatherModeForMultiKeyRequest {
    protected LeastLoadedRoutingModeForMultiKeyRequest() {
      super("LEAST_LOADED_MODE_FOR_MULTI_GET");
    }

    @Override
    protected <H, K> void selectHostForPartition(
        String partitionName,
        List<H> partitionReplicas,
        List<K> partitionKeys,
        VenicePath venicePath,
        Map<H, KeyPartitionSet<H, K>> hostMap,
        Optional<Integer> helixGroupNum,
        Optional<Integer> assignedHelixGroupId) throws RouterException {
      for (K key: partitionKeys) {
        H selectedHost = selectLeastLoadedHost(partitionReplicas, venicePath);
        /**
         * Using {@link HashMap#get} and checking whether the result is null or not
         * is faster than {@link HashMap#containsKey(Object)} and {@link HashMap#get}
         */
        KeyPartitionSet<H, K> keyPartitionSet = hostMap.get(selectedHost);
        if (keyPartitionSet == null) {
          keyPartitionSet = new KeyPartitionSet<>(Collections.singletonList(selectedHost));
          hostMap.put(selectedHost, keyPartitionSet);
        }
        keyPartitionSet.addKeyPartition(key, partitionName);
      }
    }
  }

  /**
   * This following mode will leverage Helix Zone/Group for routing.
   * Here are the steps:
   * 1. Router will assign an unique id to each Router request(the retry requests/scattered requests belonging
   *    to the same Router request will share the same request id).
   * 2. {@link HelixAssistedScatterGatherMode} will assign a group id to each request in a round-robin fashion to guarantee
   *    the evenness across different groups.
   * 3. If there is no healthy replica in the assigned group, it will try to find a healthy replica from the nearest group
   *    in one direction. For example, if there are 3 groups: 0, 1, 2 and group 1 is assigned to current request, and if
   *    there is no healthy replica in group 1, it will first look at group 2, then group 0.
   *
   * The idea behind this routing mode is that:
   * 1. Each Helix group will contain a full replication for a given resource.
   * 2. This routing mode will try best to limit the fanout inside one group.
   * So in this way, even with increased replication factors/Helix groups, the fanout size won't change, and this could
   * be a way to horizontally scale the large fanout use cases.
   */
  class HelixAssistedScatterGatherMode extends ScatterGatherModeForMultiKeyRequest {
    HelixAssistedScatterGatherMode() {
      super("HELIX_ASSISTED_SCATTER_GATHER_MODE");
    }

    @Override
    protected Optional<Integer> getHelixGroupNum() {
      return Optional.of(helixGroupSelector.getGroupCount());
    }

    @Override
    protected Optional<Integer> getAssignedHelixGroupId(VenicePath venicePath) {
      if (!venicePath.isRetryRequest()) {
        /**
         * This function only needs to assign a group id to the original Router request, and all the retried requests
         * will share the same group id as the original Router request.
         */
        venicePath.setHelixGroupId(helixGroupSelector.selectGroup(venicePath.getRequestId(), getHelixGroupNum().get()));
      }
      return Optional.of(venicePath.getHelixGroupId());
    }

    @Override
    protected <H, K> void selectHostForPartition(
        String partitionName,
        List<H> partitionReplicas,
        List<K> partitionKeys,
        VenicePath venicePath,
        Map<H, KeyPartitionSet<H, K>> hostMap,
        Optional<Integer> helixGroupNum,
        Optional<Integer> assignedHelixGroupId) throws RouterException {
      H selectedHost = null;
      int groupDistance = Integer.MAX_VALUE;
      int assignedGroupId = assignedHelixGroupId.get();
      int groupNum = helixGroupNum.get();

      for (H host: partitionReplicas) {
        if (!(host instanceof Instance)) {
          throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
              Optional.of(venicePath.getStoreName()),
              Optional.of(venicePath.getRequestType()),
              INTERNAL_SERVER_ERROR,
              "The chosen host is not an 'Instance'");
        }
        Instance instance = (Instance) host;
        String nodeId = instance.getNodeId();
        if (!venicePath.canRequestStorageNode(nodeId)) {
          // Skip the slow host
          continue;
        }
        int currentGroupId = helixGroupSelector.getInstanceGroupId(nodeId);
        if (assignedGroupId == currentGroupId) {
          selectedHost = host;
          break;
        }
        int currentDistance = currentGroupId > assignedGroupId
            ? (currentGroupId - assignedGroupId)
            : (currentGroupId + groupNum - assignedGroupId);
        if (currentDistance < groupDistance) {
          groupDistance = currentDistance;
          selectedHost = host;
        }
      }
      if (selectedHost == null) {
        if (venicePath.isRetryRequest()) {
          throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
              Optional.of(venicePath.getStoreName()),
              Optional.of(venicePath.getRequestType()),
              SERVICE_UNAVAILABLE,
              "Retry request aborted! Could not find any healthy replica.",
              RouterExceptionAndTrackingUtils.FailureType.SMART_RETRY_ABORTED_BY_SLOW_ROUTE);
        } else {
          throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
              Optional.of(venicePath.getStoreName()),
              Optional.of(venicePath.getRequestType()),
              SERVICE_UNAVAILABLE,
              "Could not find any healthy replica.");
        }
      }
      KeyPartitionSet<H, K> keyPartitionSet = hostMap.get(selectedHost);
      if (keyPartitionSet == null) {
        keyPartitionSet = new KeyPartitionSet<>(Collections.singletonList(selectedHost));
        hostMap.put(selectedHost, keyPartitionSet);
      }
      keyPartitionSet.addKeyPartitions(partitionKeys, partitionName);
    }
  }
}
