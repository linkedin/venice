package com.linkedin.venice.router.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.alpini.router.api.ScatterGatherRequest;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.httpclient.PortableHttpResponse;
import com.linkedin.venice.router.httpclient.StorageNodeClient;
import com.linkedin.venice.router.stats.AggHostHealthStats;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouteHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import io.tehuti.metrics.MetricsRepository;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import org.apache.http.HttpStatus;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceDispatcherTest {
  private VeniceRouterConfig mockConfig;
  private ReadOnlyStoreRepository mockStoreRepository;
  private RouterStats<AggRouterHttpRequestStats> mockPerStoreStats;
  private MetricsRepository mockMetricsRepository;
  private StorageNodeClient mockStorageNodeClient;
  private RouteHttpRequestStats mockRouteHttpRequestStats;
  private AggHostHealthStats mockAggHostHealthStats;
  private RouterStats<AggRouterHttpRequestStats> mockRouterStats;
  private AggRouterHttpRequestStats mockAggRouterHttpRequestStats;

  @BeforeMethod
  public void setUp() {
    mockConfig = mock(VeniceRouterConfig.class);
    mockStoreRepository = mock(ReadOnlyStoreRepository.class);
    mockPerStoreStats = mock(RouterStats.class);
    mockMetricsRepository = mock(MetricsRepository.class);
    mockStorageNodeClient = mock(StorageNodeClient.class);
    mockRouteHttpRequestStats = mock(RouteHttpRequestStats.class);
    mockAggHostHealthStats = mock(AggHostHealthStats.class);
    mockRouterStats = mock(RouterStats.class);
    mockAggRouterHttpRequestStats = mock(AggRouterHttpRequestStats.class);

    when(mockConfig.getRouterUnhealthyPendingConnThresholdPerRoute()).thenReturn(100);
    when(mockConfig.isStatefulRouterHealthCheckEnabled()).thenReturn(false);
    when(mockConfig.getMaxPendingRequest()).thenReturn(1000L);
    when(mockConfig.getLeakedFutureCleanupPollIntervalMs()).thenReturn(60000L);
    when(mockConfig.getLeakedFutureCleanupThresholdMs()).thenReturn(600000L);
    when(mockConfig.getSlowScatterRequestThresholdMs()).thenReturn(1000L);
    when(mockRouterStats.getStatsByType(any())).thenReturn(mockAggRouterHttpRequestStats);
  }

  /**
   * Test that slow scatter request logging is called when a streaming request exceeds the threshold.
   * This test uses reflection to call the private logSlowScatterRequest method directly.
   */
  @Test
  public void testSlowScatterRequestLogging() throws Exception {
    VeniceDispatcher dispatcher = new VeniceDispatcher(
        mockConfig,
        mockStoreRepository,
        mockPerStoreStats,
        mockMetricsRepository,
        mockStorageNodeClient,
        mockRouteHttpRequestStats,
        mockAggHostHealthStats,
        mockRouterStats);

    try {
      // Create mock objects for the log method parameters
      VenicePath mockPath = mock(VenicePath.class);
      when(mockPath.getStoreName()).thenReturn("test_store");
      when(mockPath.getVersionNumber()).thenReturn(1);
      when(mockPath.isRetryRequest()).thenReturn(false);

      Instance mockStorageNode = mock(Instance.class);
      when(mockStorageNode.getNodeId()).thenReturn("test_host_1234");

      // Create router keys with partition IDs
      RouterKey mockKey1 = mock(RouterKey.class);
      when(mockKey1.hasPartitionId()).thenReturn(true);
      when(mockKey1.getPartitionId()).thenReturn(5);

      RouterKey mockKey2 = mock(RouterKey.class);
      when(mockKey2.hasPartitionId()).thenReturn(true);
      when(mockKey2.getPartitionId()).thenReturn(10);

      Set<RouterKey> keys = new HashSet<>();
      keys.add(mockKey1);
      keys.add(mockKey2);

      ScatterGatherRequest<Instance, RouterKey> mockPart = mock(ScatterGatherRequest.class);
      when(mockPart.getPartitionKeys()).thenReturn(keys);

      PortableHttpResponse mockResponse = mock(PortableHttpResponse.class);
      when(mockResponse.getStatusCode()).thenReturn(HttpStatus.SC_OK);

      // Use reflection to call the private method
      Method logMethod = VeniceDispatcher.class.getDeclaredMethod(
          "logSlowScatterRequest",
          VenicePath.class,
          ScatterGatherRequest.class,
          Instance.class,
          double.class,
          PortableHttpResponse.class,
          Throwable.class);
      logMethod.setAccessible(true);

      // Call the method - should not throw any exception
      logMethod.invoke(dispatcher, mockPath, mockPart, mockStorageNode, 1500.0, mockResponse, null);

      // Verify the mock interactions
      verify(mockPath).getStoreName();
      verify(mockPath).getVersionNumber();
      verify(mockPath).isRetryRequest();
      verify(mockStorageNode).getNodeId();
      verify(mockPart).getPartitionKeys();
    } finally {
      dispatcher.stop();
    }
  }

  /**
   * Test that slow scatter request logging handles retry requests correctly.
   */
  @Test
  public void testSlowScatterRequestLoggingForRetryRequest() throws Exception {
    VeniceDispatcher dispatcher = new VeniceDispatcher(
        mockConfig,
        mockStoreRepository,
        mockPerStoreStats,
        mockMetricsRepository,
        mockStorageNodeClient,
        mockRouteHttpRequestStats,
        mockAggHostHealthStats,
        mockRouterStats);

    try {
      VenicePath mockPath = mock(VenicePath.class);
      when(mockPath.getStoreName()).thenReturn("test_store_retry");
      when(mockPath.getVersionNumber()).thenReturn(2);
      when(mockPath.isRetryRequest()).thenReturn(true); // This is a retry request

      Instance mockStorageNode = mock(Instance.class);
      when(mockStorageNode.getNodeId()).thenReturn("retry_host_5678");

      RouterKey mockKey = mock(RouterKey.class);
      when(mockKey.hasPartitionId()).thenReturn(true);
      when(mockKey.getPartitionId()).thenReturn(15);

      Set<RouterKey> keys = new HashSet<>();
      keys.add(mockKey);

      ScatterGatherRequest<Instance, RouterKey> mockPart = mock(ScatterGatherRequest.class);
      when(mockPart.getPartitionKeys()).thenReturn(keys);

      PortableHttpResponse mockResponse = mock(PortableHttpResponse.class);
      when(mockResponse.getStatusCode()).thenReturn(HttpStatus.SC_OK);

      Method logMethod = VeniceDispatcher.class.getDeclaredMethod(
          "logSlowScatterRequest",
          VenicePath.class,
          ScatterGatherRequest.class,
          Instance.class,
          double.class,
          PortableHttpResponse.class,
          Throwable.class);
      logMethod.setAccessible(true);

      // Call the method
      logMethod.invoke(dispatcher, mockPath, mockPart, mockStorageNode, 2000.0, mockResponse, null);

      // Verify retry request flag was checked
      verify(mockPath).isRetryRequest();
    } finally {
      dispatcher.stop();
    }
  }
}
