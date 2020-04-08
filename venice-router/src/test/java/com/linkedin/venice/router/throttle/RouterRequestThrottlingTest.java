package com.linkedin.venice.router.throttle;

import com.linkedin.ddsstorage.base.concurrency.AsyncFuture;
import com.linkedin.ddsstorage.base.concurrency.AsyncPromise;
import com.linkedin.ddsstorage.base.misc.Metrics;
import com.linkedin.ddsstorage.netty4.misc.BasicHttpRequest;
import com.linkedin.ddsstorage.router.api.HostFinder;
import com.linkedin.ddsstorage.router.api.HostHealthMonitor;
import com.linkedin.ddsstorage.router.api.PartitionFinder;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.ddsstorage.router.api.Scatter;
import com.linkedin.ddsstorage.router.api.ScatterGatherRequest;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils;
import com.linkedin.venice.router.api.RouterKey;
import com.linkedin.venice.router.api.VeniceDelegateMode;
import com.linkedin.venice.router.api.VeniceDelegateModeConfig;
import com.linkedin.venice.router.api.VeniceDispatcher;
import com.linkedin.venice.router.api.VeniceHostHealth;
import com.linkedin.venice.router.api.VeniceRole;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.httpclient.StorageNodeClient;
import com.linkedin.venice.router.stats.AggHostHealthStats;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouteHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.Executor;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class RouterRequestThrottlingTest {
  private long totalQuota = 1000;
  private String storeName;
  private ReadRequestThrottler throttler;

  private Store store;
  private ReadOnlyStoreRepository storeRepository;

  @BeforeMethod(alwaysRun = true)
  public void setup() {
    // mock a ReadRequestThrottler
    storeName = TestUtils.getUniqueString("store");
    store = TestUtils.createTestStore(storeName, "test", System.currentTimeMillis());
    store.setReadQuotaInCU(totalQuota);

    storeRepository = mock(ReadOnlyStoreRepository.class);
    doReturn(false).when(storeRepository).isReadComputationEnabled(storeName);
    doReturn(false).when(storeRepository).isSingleGetRouterCacheEnabled(storeName);
    doReturn(false).when(storeRepository).isBatchGetRouterCacheEnabled(storeName);
    doReturn(store).when(storeRepository).getStore(storeName);
    doReturn(Arrays.asList(new Store[]{store})).when(storeRepository).getAllStores();
    doReturn(totalQuota).when(storeRepository).getTotalStoreReadQuota();

    AggRouterHttpRequestStats stats = mock(AggRouterHttpRequestStats.class);
    ZkRoutersClusterManager zkRoutersClusterManager = mock(ZkRoutersClusterManager.class);
    doReturn(1).when(zkRoutersClusterManager).getLiveRoutersCount();
    doReturn(true).when(zkRoutersClusterManager).isQuotaRebalanceEnabled();
    doReturn(true).when(zkRoutersClusterManager).isThrottlingEnabled();
    doReturn(true).when(zkRoutersClusterManager).isMaxCapacityProtectionEnabled();
    RoutingDataRepository routingDataRepository = mock(RoutingDataRepository.class);
    throttler = new ReadRequestThrottler(zkRoutersClusterManager, storeRepository, routingDataRepository, 2000, stats, 0.0, 1000, 1000);
  }

  @Test (timeOut = 30000, groups = {"flaky"})
  public void testSingleGetThrottling() throws Exception {
    VeniceRouterConfig routerConfig = mock(VeniceRouterConfig.class);
    doReturn(1.0d).when(routerConfig).getCacheHitRequestThrottleWeight();
    doReturn(Long.MAX_VALUE).when(routerConfig).getMaxPendingRequest();

    VeniceHostHealth healthMonitor = mock(VeniceHostHealth.class);

    MetricsRepository metricsRepository = new MetricsRepository();
    StorageNodeClient storageNodeClient = mock(StorageNodeClient.class);
    RouteHttpRequestStats routeHttpRequestStats = new RouteHttpRequestStats(metricsRepository);
    VeniceDispatcher dispatcher = new VeniceDispatcher(routerConfig, healthMonitor, storeRepository, Optional.empty(),
        mock(RouterStats.class), metricsRepository, storageNodeClient, routeHttpRequestStats, mock(AggHostHealthStats.class));
    // set the ReadRequestThrottler
    dispatcher.initReadRequestThrottler(throttler);
    RouterExceptionAndTrackingUtils.setRouterStats(new RouterStats<>( requestType -> new AggRouterHttpRequestStats(metricsRepository, requestType)));

    // mock inputs for VeniceDispatcher#dispatch()
    Scatter<Instance, VenicePath, RouterKey> scatter = mock(Scatter.class);
    ScatterGatherRequest<Instance, RouterKey> part = mock(ScatterGatherRequest.class);
    Instance instance = new Instance(Utils.getHelixNodeIdentifier(10000), "localhost", 10000);
    Set<String> partitionNames = new HashSet<>();
    partitionNames.add(storeName + "_v1-0");
    doReturn(Arrays.asList(instance)).when(part).getHosts();
    doReturn(partitionNames).when(part).getPartitionsNames();

    VenicePath path = mock(VenicePath.class);
    doReturn(storeName).when(path).getStoreName();
    // mock single-get request
    doReturn(RequestType.SINGLE_GET).when(path).getRequestType();
    doReturn(false).when(path).isRetryRequest();

    BasicHttpRequest request = mock(BasicHttpRequest.class);

    AsyncPromise<Instance> hostSelected = mock(AsyncPromise.class);
    AsyncPromise<List<FullHttpResponse>> responseFuture = mock(AsyncPromise.class);
    AsyncPromise<HttpResponseStatus> retryFuture = mock(AsyncPromise.class);
    AsyncFuture<Void> timeoutFuture = mock(AsyncFuture.class);
    Executor contextExecutor = mock(Executor.class);

    // The router shouldn't throttle any request if the QPS is below 1000
    for (int iter = 0; iter < 3; iter++) {
      for (int i = 0; i < totalQuota; i++) {
        try {
          dispatcher.dispatch(scatter, part, path, request, hostSelected, responseFuture, retryFuture, timeoutFuture,
              contextExecutor);
        } catch (Exception e) {
          if (e instanceof RouterException) {
            Assert.fail("Router shouldn't throttle any single-get requests if the QPS is below 1000");
          } else {
            Assert.fail("Router should not throw exception : ", e);
          }
        }
      }

      // restore the throttler
      throttler.restoreAllThrottlers();
    }

    // Router should throttle the single-get requests if QPS exceeds 1000
    boolean singleGetThrottled = false;
    for (int i = 0; i < totalQuota + 200; i++) {
      try {
        dispatcher.dispatch(scatter, part, path, request, hostSelected, responseFuture, retryFuture, timeoutFuture,
            contextExecutor);

      } catch (Exception e) {
        singleGetThrottled = true;
        if (i < totalQuota) {
          // Shouldn't throttle if QPS is below 1000
          Assert.fail("router shouldn't throttle any single-get requests if the QPS is below 1000");
        }
      }
    }

    // restore the throttler so that it doesn't affect the following test case
    throttler.restoreAllThrottlers();
    Assert.assertTrue(singleGetThrottled);
  }

  @DataProvider(name = "multiGet_compute")
  public static Object[][] requestType() {
    return new Object[][]{{RequestType.MULTI_GET}, {RequestType.COMPUTE}};
  }
  @Test (timeOut = 30000, dataProvider = "multiGet_compute")
  public void testMultiKeyThrottling(RequestType requestType) throws Exception {
    // Allow 10 multi-key requests per second
    int batchGetSize = 100;
    int allowedQPS = (int) totalQuota / batchGetSize;

    // mock a scatter gather helper for multi-key requests
    VeniceDelegateModeConfig config = mock(VeniceDelegateModeConfig.class);
    doReturn(true).when(config).isStickyRoutingEnabledForSingleGet();
    doReturn(true).when(config).isStickyRoutingEnabledForMultiGet();
    doReturn(true).when(config).isGreedyMultiGetScatter();

    // multi-get/compute requests are throttled in VeniceDelegateMode
    VeniceDelegateMode delegateMode = new VeniceDelegateMode(config, mock(RouterStats.class));
    delegateMode.initReadRequestThrottler(throttler);

    VenicePath path = mock(VenicePath.class);
    doReturn(false).when(path).isRetryRequestTooLate();
    doReturn(false).when(path).isRetryRequest();
    doReturn(requestType).when(path).getRequestType();
    doReturn(true).when(path).canRequestStorageNode(any());
    doReturn(storeName).when(path).getStoreName();
    // return empty key list to skip the scattering part
    doReturn(new ArrayList<RouterKey>()).when(path).getPartitionKeys();

    // mock a DDS Scatter instance
    Scatter<Instance, VenicePath, RouterKey> scatter = mock(Scatter.class);
    doReturn(path).when(scatter).getPath();
    doReturn(0).when(scatter).getOfflineRequestCount();
    Instance instance = new Instance(Utils.getHelixNodeIdentifier(10000), "localhost", 10000);
    ScatterGatherRequest<Instance, RouterKey> part = mock(ScatterGatherRequest.class);
    doReturn(Arrays.asList(instance)).when(part).getHosts();
    SortedSet<RouterKey> keys = mock(SortedSet.class);
    // 100 keys per request
    doReturn(batchGetSize).when(keys).size();
    doReturn(keys).when(part).getPartitionKeys();
    // return an multi-key request that contains 100 keys
    doReturn(Arrays.asList(part)).when(scatter).getOnlineRequests();

    // mock other inputs for VeniceDelegateMode#scatter()
    PartitionFinder<RouterKey> partitionFinder = mock(PartitionFinder.class);
    HostFinder<Instance, VeniceRole> hostFinder = mock(HostFinder.class);
    HostHealthMonitor<Instance> hostHealthMonitor = mock(HostHealthMonitor.class);
    Metrics metrics = mock(Metrics.class);

    // The router shouldn't throttle any request if the multi-get QPS is below 10
    for (int iter = 0; iter < 3; iter++) {
      for (int i = 0; i < allowedQPS; i++) {
        try {
          delegateMode.scatter(scatter, HttpMethod.POST.name(), storeName + "_v1", partitionFinder, hostFinder,
              hostHealthMonitor, VeniceRole.REPLICA, metrics);
        } catch (Exception e) {
          Assert.fail("router shouldn't throttle any multi-get requests if the QPS is below " + allowedQPS);
        }
      }

      // restore the throttler
      throttler.restoreAllThrottlers();
    }

    // Router should throttle the multi-get requests if QPS exceeds 10
    boolean multiGetThrottled = false;
    for (int i = 0; i < allowedQPS + 1; i++) {
      try {
        delegateMode.scatter(scatter, HttpMethod.POST.name(), storeName + "_v1", partitionFinder, hostFinder,
            hostHealthMonitor, VeniceRole.REPLICA, metrics);
      } catch (Exception e) {
        multiGetThrottled = true;
        if (i < allowedQPS) {
          // Shouldn't throttle if QPS is below 10
          Assert.fail("router shouldn't throttle any multi-get requests if the QPS is below 10");
        }
      }
    }

    // restore the throttler so that it doesn't affect the following test case
    throttler.restoreAllThrottlers();
    Assert.assertTrue(multiGetThrottled);
  }
}
