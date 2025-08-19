package com.linkedin.venice.router.throttle;

import static com.linkedin.venice.router.api.VeniceMultiKeyRoutingStrategy.LEAST_LOADED_ROUTING;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertTrue;

import com.linkedin.alpini.router.api.HostFinder;
import com.linkedin.alpini.router.api.HostHealthMonitor;
import com.linkedin.alpini.router.api.PartitionFinder;
import com.linkedin.alpini.router.api.RouterException;
import com.linkedin.alpini.router.api.Scatter;
import com.linkedin.alpini.router.api.ScatterGatherRequest;
import com.linkedin.venice.helix.ZkRoutersClusterManager;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.api.RouterKey;
import com.linkedin.venice.router.api.RoutingComputationMode;
import com.linkedin.venice.router.api.VeniceDelegateMode;
import com.linkedin.venice.router.api.VeniceHostFinder;
import com.linkedin.venice.router.api.VenicePartitionFinder;
import com.linkedin.venice.router.api.VeniceRole;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouteHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.netty.handler.codec.http.HttpMethod;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.SortedSet;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class RouterRequestThrottlingTest {
  private long totalQuota = 1000;
  private String storeName;
  private ReadRequestThrottler throttler;

  private Store store;
  private ReadOnlyStoreRepository storeRepository;

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    // mock a ReadRequestThrottler
    storeName = Utils.getUniqueString("store");
    store = TestUtils.createTestStore(storeName, "test", System.currentTimeMillis());
    store.setReadQuotaInCU(totalQuota);
    store.setCurrentVersion(1);

    storeRepository = mock(ReadOnlyStoreRepository.class);
    doReturn(false).when(storeRepository).isReadComputationEnabled(storeName);
    doReturn(store).when(storeRepository).getStore(storeName);
    doReturn(Arrays.asList(new Store[] { store })).when(storeRepository).getAllStores();
    doReturn(totalQuota).when(storeRepository).getTotalStoreReadQuota();

    AggRouterHttpRequestStats stats = mock(AggRouterHttpRequestStats.class);
    ZkRoutersClusterManager zkRoutersClusterManager = mock(ZkRoutersClusterManager.class);
    doReturn(1).when(zkRoutersClusterManager).getLiveRoutersCount();
    doReturn(true).when(zkRoutersClusterManager).isThrottlingEnabled();
    doReturn(true).when(zkRoutersClusterManager).isMaxCapacityProtectionEnabled();
    throttler = new ReadRequestThrottler(zkRoutersClusterManager, storeRepository, 2000, stats, 1.5, 1000);
  }

  @DataProvider(name = "multiGet_compute")
  public static Object[][] requestType() {
    return new Object[][] { { RequestType.MULTI_GET }, { RequestType.COMPUTE } };
  }

  /** A simple Runnable-like interface just to reduce boilerplate in the test... */
  private interface ScatterCall {
    void run() throws RouterException;
  }

  @Test(timeOut = 30000, dataProvider = "multiGet_compute")
  public void testMultiKeyThrottling(RequestType requestType) throws Exception {
    // Allow 10 multi-key requests per second
    int batchGetSize = 100;
    int allowedQPS = (int) totalQuota / batchGetSize * 2;

    // mock a scatter gather helper for multi-key requests
    VeniceRouterConfig config = mock(VeniceRouterConfig.class);
    doReturn(LEAST_LOADED_ROUTING).when(config).getMultiKeyRoutingStrategy();
    doReturn(RoutingComputationMode.SEQUENTIAL).when(config).getRoutingComputationMode();

    // multi-get/compute requests are throttled in VeniceDelegateMode
    VeniceDelegateMode delegateMode =
        new VeniceDelegateMode(config, mock(RouterStats.class), mock(RouteHttpRequestStats.class));
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
    Instance instance = new Instance(Utils.getHelixNodeIdentifier(Utils.getHostName(), 10000), "localhost", 10000);
    ScatterGatherRequest<Instance, RouterKey> part = mock(ScatterGatherRequest.class);
    doReturn(Arrays.asList(instance)).when(part).getHosts();
    SortedSet<RouterKey> keys = mock(SortedSet.class);
    // 100 keys per request
    doReturn(batchGetSize).when(keys).size();
    doReturn(keys).when(part).getPartitionKeys();
    // return an multi-key request that contains 100 keys
    doReturn(Arrays.asList(part)).when(scatter).getOnlineRequests();

    // mock other inputs for VeniceDelegateMode#scatter()
    PartitionFinder<RouterKey> partitionFinder = mock(VenicePartitionFinder.class);
    HostFinder<Instance, VeniceRole> hostFinder = mock(VeniceHostFinder.class);
    HostHealthMonitor<Instance> hostHealthMonitor = mock(HostHealthMonitor.class);

    ScatterCall scatterCall = () -> delegateMode.scatter(
        scatter,
        HttpMethod.POST.name(),
        storeName + "_v1",
        partitionFinder,
        hostFinder,
        hostHealthMonitor,
        VeniceRole.REPLICA);

    // The router shouldn't throttle any request if the multi-get QPS is below 10
    for (int iter = 0; iter < 3; iter++) {
      for (int i = 0; i < allowedQPS; i++) {
        try {
          scatterCall.run();
        } catch (Exception e) {
          Assert.fail("router shouldn't throttle any multi-get requests if the QPS is below " + allowedQPS);
        }
      }

      // restore the throttler
      throttler.restoreAllThrottlers();
    }

    // Router should throttle the multi-get requests if QPS exceeds 10
    boolean multiGetThrottled = false;
    int queriesSent = allowedQPS + 1;
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < queriesSent; i++) {
      try {
        scatterCall.run();
      } catch (Exception e) {
        multiGetThrottled = true;
        if (i < allowedQPS) {
          // Shouldn't throttle if QPS is below 10
          Assert.fail("router shouldn't throttle any multi-get requests if the QPS is below 10");
        }
      }
    }
    long elapsedTime = System.currentTimeMillis() - startTime;

    if (!multiGetThrottled) {
      int additionalQueriesNeeded = -1;
      for (int i = 1; i < queriesSent * 10; i++) {
        try {
          scatterCall.run();
        } catch (Exception e) {
          additionalQueriesNeeded = i;
          break;
        }
      }
      long totalElapsedTime = System.currentTimeMillis() - startTime;
      if (additionalQueriesNeeded < 0) {
        Assert.fail(
            "Never triggered quota at all, even after sending 10x more than it should have needed! Original elapsed time: "
                + elapsedTime + "; total elapsed time: " + totalElapsedTime);
      } else {
        /**
         * N.B.: This test used to be flaky because it would be 1 request short of triggering the quota. It's not clear
         * why it sometimes takes a little more, and sometimes doesn't (floating point arithmetic being wonky, perhaps?)
         * but here we're adding a small tolerance threshold, while still ensuring that larger deviations still result
         * in failure.
         */
        int toleranceThreshold = 1;
        assertTrue(
            additionalQueriesNeeded <= toleranceThreshold,
            "Should have triggered quota in " + queriesSent + " requests, but it took " + additionalQueriesNeeded
                + " additional request(s) before finally triggering, which is higher than the tolerance threshold of "
                + toleranceThreshold + ". Original elapsed time: " + elapsedTime + "; total elapsed time: "
                + totalElapsedTime);
      }
    }
  }
}
