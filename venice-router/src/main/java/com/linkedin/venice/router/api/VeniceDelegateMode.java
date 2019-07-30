package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.base.misc.Metrics;
import com.linkedin.ddsstorage.router.api.HostFinder;
import com.linkedin.ddsstorage.router.api.HostHealthMonitor;
import com.linkedin.ddsstorage.router.api.PartitionFinder;
import com.linkedin.ddsstorage.router.api.ResourcePath;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.ddsstorage.router.api.Scatter;
import com.linkedin.ddsstorage.router.api.ScatterGatherMode;
import com.linkedin.ddsstorage.router.api.ScatterGatherRequest;
import com.linkedin.venice.exceptions.QuotaExceededException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.throttle.RouterThrottler;
import com.linkedin.venice.utils.HelixUtils;
import java.util.Arrays;
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

import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class VeniceDelegateMode extends ScatterGatherMode {
  /**
   * This mode will initiate a request per partition, which is suitable for single-get, and it could be the default mode
   * for all other requests.
   */
  private static final ScatterGatherMode SCATTER_GATHER_MODE_FOR_SINGLE_GET = ScatterGatherMode.GROUP_BY_PARTITION;

  /**
   * This mode will assume there is only host for single-get request (sticky routing), and the scattering logic is
   * optimized based on this assumption.
   */
  private static final ScatterGatherMode SCATTER_GATHER_MODE_FOR_STICKY_SINGLE_GET = new ScatterGatherModeForStickySingleGet();

  /**
   * This mode will group all requests to the same host into a single request.  Hosts are selected as the first host returned
   * by the VeniceHostFinder, so we must shuffle the order to get an even distribution.
   */
  private static final ScatterGatherMode SCATTER_GATHER_MODE_FOR_MULTI_GET = ScatterGatherMode.GROUP_BY_PRIMARY_HOST;

  /**
   * This mode will do the aggregation per host first, and then initiate a request per host.
   */
  private static final ScatterGatherMode SCATTER_GATHER_MODE_FOR_MULTI_GET_GREEDY = ScatterGatherMode.GROUP_BY_GREEDY_HOST;

  /**
   * This mode assumes there is only one host per partition (sticky routing), and the scattering logic is optimized
   * based on this assumption.
   */
  private static final ScatterGatherMode SCATTER_GATHER_MODE_FOR_STICKY_MULTI_GET = new ScatterGatherModeForStickyMultiGet();

  private RouterThrottler readRequestThrottler;

  private final boolean stickyRoutingEnabledForSingleGet;
  private final boolean stickyRoutingEnabledForMultiGet;
  private final boolean greedyMultiget;

  public VeniceDelegateMode(VeniceDelegateModeConfig config) {
    super("VENICE_DELEGATE_MODE", false);
    this.stickyRoutingEnabledForSingleGet = config.isStickyRoutingEnabledForSingleGet();
    this.stickyRoutingEnabledForMultiGet = config.isStickyRoutingEnabledForMultiGet();
    this.greedyMultiget = config.isGreedyMultiGetScatter();
  }

  public void initReadRequestThrottler(RouterThrottler requestThrottler) {
    if (null != this.readRequestThrottler) {
      throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.empty(), Optional.empty(), INTERNAL_SERVER_ERROR,
          "ReadRequestThrottle has already been initialized before, and no further update expected!");
    }
    this.readRequestThrottler = requestThrottler;
  }

  @Nonnull
  @Override
  public <H, P extends ResourcePath<K>, K, R> Scatter<H, P, K> scatter(@Nonnull Scatter<H, P, K> scatter,
      @Nonnull String requestMethod, @Nonnull String resourceName, @Nonnull PartitionFinder<K> partitionFinder,
      @Nonnull HostFinder<H, R> hostFinder, @Nonnull HostHealthMonitor<H> hostHealthMonitor, @Nonnull R roles,
      Metrics metrics) throws RouterException {
    if (null == readRequestThrottler) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.empty(), Optional.empty(), INTERNAL_SERVER_ERROR,
          "Read request throttle has not been setup yet");
    }
    P path = scatter.getPath();
    if (!  (path instanceof VenicePath)) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.empty(), Optional.empty(),INTERNAL_SERVER_ERROR,
          "VenicePath is expected, but received " + path.getClass());
    }
    VenicePath venicePath = (VenicePath)path;
    String storeName = venicePath.getStoreName();

    // Check whether retry request is too late or not
    if (venicePath.isRetryRequestTooLate()) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.of(storeName), Optional.of(venicePath.getRequestType()),
          SERVICE_UNAVAILABLE, "The retry request aborted because of delay constraint of smart long-tail retry",
          RouterExceptionAndTrackingUtils.FailureType.SMART_RETRY_ABORTED_BY_DELAY_CONSTRAINT);
    }
    ScatterGatherMode scatterMode = null;
    switch (venicePath.getRequestType()) {
      case MULTI_GET:
      case MULTI_GET_STREAMING:
      case COMPUTE:
      case COMPUTE_STREAMING:
        if (stickyRoutingEnabledForMultiGet) {
          scatterMode = SCATTER_GATHER_MODE_FOR_STICKY_MULTI_GET;
        } else {
          if (greedyMultiget){
            scatterMode = SCATTER_GATHER_MODE_FOR_MULTI_GET_GREEDY;
          } else {
            scatterMode = SCATTER_GATHER_MODE_FOR_MULTI_GET;
          }
        }
        break;
      case SINGLE_GET:
        if (stickyRoutingEnabledForSingleGet) {
          scatterMode = SCATTER_GATHER_MODE_FOR_STICKY_SINGLE_GET;
        } else {
          scatterMode = SCATTER_GATHER_MODE_FOR_SINGLE_GET;
        }
        break;
      default:
        throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.of(storeName), Optional.of(venicePath.getRequestType()),
            INTERNAL_SERVER_ERROR, "Unknown request type: " + venicePath.getRequestType());
    }
    Scatter finalScatter = scatterMode.scatter(scatter, requestMethod, resourceName, partitionFinder, hostFinder,
        hostHealthMonitor, roles, metrics);
    int offlineRequestNum = scatter.getOfflineRequestCount();
    if (offlineRequestNum > 0) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.of(storeName), Optional.of(venicePath.getRequestType()),
          SERVICE_UNAVAILABLE, "Some partition is not available for store: " + storeName + " with request type: " + venicePath.getRequestType());
    }

    for (ScatterGatherRequest<H, K> part : scatter.getOnlineRequests()) {
      int hostCount = part.getHosts().size();
      if (0 == hostCount) {
        throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.of(storeName), Optional.of(venicePath.getRequestType()),
            SERVICE_UNAVAILABLE, "Could not find ready-to-serve replica for request: " + part);
      }
      H host = part.getHosts().get(0);
      if (hostCount > 1) {
        List<H> hosts = part.getHosts();
        host = hosts.get((int) (System.currentTimeMillis() % hostCount));  //cheap random host selection
        // Update host selection
        // The downstream (VeniceDispatcher) will only expect one host for a given scatter request.
        H finalHost = host;
        hosts.removeIf(aHost -> !aHost.equals(finalHost));
      }
      if (!(host instanceof Instance)) {
        throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.of(storeName), Optional.of(venicePath.getRequestType()),
            INTERNAL_SERVER_ERROR, "Ready-to-serve host must be an 'Instance'");
      }
      Instance veniceInstance = (Instance)host;
      String instanceNodeId = veniceInstance.getNodeId();
      if (! venicePath.canRequestStorageNode(instanceNodeId)) {
        /**
         * When retry request is aborted, Router will wait for the original request.
         */
        throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.of(storeName), Optional.of(venicePath.getRequestType()),
            SERVICE_UNAVAILABLE, "Retry request aborted because of slow route: " + instanceNodeId,
            RouterExceptionAndTrackingUtils.FailureType.SMART_RETRY_ABORTED_BY_SLOW_ROUTE);
      }

      if (!venicePath.getRequestType().equals(RequestType.SINGLE_GET) && !venicePath.isRetryRequest()) {
        /**
         * Here is the only suitable place to throttle multi-get/compute request since we want to fail the whole request
         * if some scatter request gets throttled.
         *
         * Venice doesn't apply quota enforcement for retry request since retry is a way for latency guarantee,
         * which should be transparent to customers.
         *
         * For single-get request, the throttling logic is happening in {@link VeniceDispatcher} because of caching logic.
         */
        int keyCount = part.getPartitionKeys().size();
        try {
          readRequestThrottler.mayThrottleRead(storeName, keyCount * readRequestThrottler.getReadCapacity(), Optional.of(veniceInstance.getNodeId()));
        } catch (QuotaExceededException e) {
          throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.of(storeName), Optional.of(venicePath.getRequestType()),
              TOO_MANY_REQUESTS, "Quota exceeds! msg: " + e.getMessage());
        }
      }
    }

    return finalScatter;
  }

  /**
   * This mode assumes there is only one available host for single-get request (sticky routing),
   * so it is optimized based on this assumption to avoid unnecessary memory allocation.
   */
  static class ScatterGatherModeForStickySingleGet extends ScatterGatherMode {

    protected ScatterGatherModeForStickySingleGet() {
      super("SCATTER_GATHER_MODE_FOR_STICKY_SINGLE_GET", false);
    }

    @Nonnull
    @Override
    public <H, P extends ResourcePath<K>, K, R> Scatter<H, P, K> scatter(@Nonnull Scatter<H, P, K> scatter,
        @Nonnull String requestMethod, @Nonnull String resourceName, @Nonnull PartitionFinder<K> partitionFinder,
        @Nonnull HostFinder<H, R> hostFinder, @Nonnull HostHealthMonitor<H> hostHealthMonitor, @Nonnull R roles,
        Metrics metrics) throws RouterException {
      P path = scatter.getPath();
      if (!  (path instanceof VenicePath)) {
        throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.empty(), Optional.empty(),INTERNAL_SERVER_ERROR,
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
        /**
         * Key based sticky routing for single-get requests;
         * use the hashcode of the key to choose a host; in this way, keys from the same partition are evenly
         * distributed to all the replicas of that partition; besides, it's guaranteed that the same key will always
         * go to the same host unless some hosts are down/stopped.
         */
        H host = avoidSlowHost((VenicePath)path, key, hosts);
        scatter.addOnlineRequest(new ScatterGatherRequest<>(Arrays.asList(host), keySet, partitionName));
      } else {
        scatter.addOnlineRequest(new ScatterGatherRequest<>(hosts, keySet, partitionName));
      }
      return scatter;
    }
  }

  /**
   * This mode assumes there is only one available host per partition (sticky routing),
   * and it is optimized based on this assumption to avoid unnecessary memory allocation and speed up scattering logic.
   */
  static class ScatterGatherModeForStickyMultiGet extends ScatterGatherMode {

    /**
     * This class contains all the partitions/keys belonging to the same host.
     */
    static class KeyPartitionSet<H, K> {
      public TreeSet<K> keySet = new TreeSet<>();
      public Set<String> partitionNames = new HashSet<>();
      // Only considering the first host because of sticky routing
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

    protected ScatterGatherModeForStickyMultiGet() {
      super("SCATTER_GATHER_MODE_FOR_STICKY_MULTI_GET", false);
    }

    @Nonnull
    @Override
    public <H, P extends ResourcePath<K>, K, R> Scatter<H, P, K> scatter(@Nonnull Scatter<H, P, K> scatter,
        @Nonnull String requestMethod, @Nonnull String resourceName, @Nonnull PartitionFinder<K> partitionFinder,
        @Nonnull HostFinder<H, R> hostFinder, @Nonnull HostHealthMonitor<H> hostHealthMonitor, @Nonnull R roles,
        Metrics metrics) throws RouterException {
      P path = scatter.getPath();
      if (!  (path instanceof VenicePath)) {
        throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.empty(), Optional.empty(),INTERNAL_SERVER_ERROR,
            "VenicePath is expected, but received " + path.getClass());
      }
      /**
       * Group by partition
       *
       * Using simple data structure, such as {@link LinkedList} instead of {@link SortedSet} to speed up operation,
       * which is helpful for large batch-get use case.
       */
      Map<Integer, List<K>> partitionKeys = new HashMap<>();
      for (K key : scatter.getPath().getPartitionKeys()) {
        int partitionId = ((RouterKey)key).getPartitionId();
        List<K> partitionKeyList = partitionKeys.get(partitionId);
        if (null == partitionKeyList) {
          partitionKeyList = new LinkedList<>();
          partitionKeys.put(partitionId, partitionKeyList);
        }
        partitionKeyList.add(key);
      }

      /**
       * Group by host
       */
      Map<H, KeyPartitionSet<H, K>> hostMap = new HashMap<>();
      for (Map.Entry<Integer, List<K>> entry : partitionKeys.entrySet()) {
        String partitionName = HelixUtils.getPartitionName(resourceName, entry.getKey());
        List<H> hosts = hostFinder.findHosts(requestMethod, resourceName, partitionName, hostHealthMonitor, roles);
        if (hosts.isEmpty()) {
          scatter.addOfflineRequest(new ScatterGatherRequest<>(Collections.emptyList(), new TreeSet<>(entry.getValue()), partitionName));
        } else if (hosts.size() > 1) {
          for (K key : entry.getValue()) {
            /**
             * Key based sticky routing for multi-get requests;
             * for each key, use the hashcode of each key to choose a host; in this way, keys are evenly distributed
             * to all the hosts; besides, it's guaranteed that the same key will always go to the same host unless
             * some hosts are down/stopped.
             */
            H host = avoidSlowHost((VenicePath)path, key, hosts);
            /**
             * Using {@link HashMap#get} and checking whether the result is null or not
             * is faster than {@link HashMap#containsKey(Object)} and {@link HashMap#get}
             */
            KeyPartitionSet<H, K> keyPartitionSet = hostMap.get(host);
            if (null == keyPartitionSet) {
              keyPartitionSet = new KeyPartitionSet<>(Arrays.asList(host));
              hostMap.put(host, keyPartitionSet);
            }
            keyPartitionSet.addKeyPartition(key, partitionName);
          }
        }
        else {
          H host = hosts.get(0);
          KeyPartitionSet<H, K> keyPartitionSet = hostMap.get(host);
          if (null == keyPartitionSet) {
            keyPartitionSet = new KeyPartitionSet<>(Arrays.asList(host));
            hostMap.put(host, keyPartitionSet);
          }
          keyPartitionSet.addKeyPartitions(entry.getValue(), partitionName);
        }
      }

      /**
       * Populate online requests
       */
      for (Map.Entry<H, KeyPartitionSet<H, K>> entry : hostMap.entrySet()) {
        scatter.addOnlineRequest(new ScatterGatherRequest<>(entry.getValue().hosts, entry.getValue().keySet, entry.getValue().partitionNames));
      }

      return scatter;
    }
  }

  protected static <H, K> H chooseHostByKey(K key, List<H> hosts) {
    int hostsSize = hosts.size();
    /**
     * Use {@link Math.floorMod} to avoid negative index here.
     */
    int chosenIndex = Math.floorMod(key.hashCode(), hostsSize);
    return hosts.get(chosenIndex);
  }

  /**
   * If the first chosen host is slow, choose another replica that is not in the
   * slow hosts set; if all replicas are slow, abort the retry request.
   *
   * @param path
   * @param key
   * @param hosts a list of replica
   * @param <H> {@link Instance}
   * @return
   */
  protected static <H, K> H avoidSlowHost(VenicePath path, K key, List<H> hosts) throws RouterException {
    H host = chooseHostByKey(key, hosts);
    if (!(host instanceof Instance)) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(Optional.of(path.getStoreName()), Optional.of(path.getRequestType()),
          INTERNAL_SERVER_ERROR, "The chosen host is not an 'Instance'");
    }
    Instance firstPickHost = (Instance) host;
    if (path.canRequestStorageNode(firstPickHost.getNodeId())) {
      return host;
    }

    /**
     * If the first pick is a slow host, find another replica that is not slow.
     */
    for (H h : hosts) {
      if (!firstPickHost.equals(h) && path.canRequestStorageNode(((Instance)h).getNodeId())) {
        return h;
      }
    }

    throw RouterExceptionAndTrackingUtils.newVeniceExceptionAndTracking(Optional.of(path.getStoreName()), Optional.of(path.getRequestType()),
        SERVICE_UNAVAILABLE, "Retry request aborted! Could not find any healthy replica.", RouterExceptionAndTrackingUtils.FailureType.SMART_RETRY_ABORTED_BY_SLOW_ROUTE);
  }
}
