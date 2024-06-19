package com.linkedin.venice.router;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.alpini.base.concurrency.AsyncPromise;
import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.alpini.router.api.RouterException;
import com.linkedin.alpini.router.api.Scatter;
import com.linkedin.alpini.router.api.ScatterGatherRequest;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils;
import com.linkedin.venice.router.api.VeniceDispatcher;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.httpclient.StorageNodeClient;
import com.linkedin.venice.router.stats.AggHostHealthStats;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouteHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.http.client.methods.HttpGet;
import org.testng.annotations.Test;


public class VeniceDispacherTest {
  @Test
  public void testDispatch() throws RouterException {
    Scatter mockScatter = mock(Scatter.class);
    ScatterGatherRequest mockScatterGatherRequest = mock(ScatterGatherRequest.class);
    RouterStats<AggRouterHttpRequestStats> routerStats = mock(RouterStats.class);
    AggRouterHttpRequestStats stats = mock(AggRouterHttpRequestStats.class);
    AsyncPromise mockHostSelected = mock(AsyncPromise.class);
    AsyncPromise mockTimeoutFuture = mock(AsyncPromise.class);
    BasicFullHttpRequest mockRequest = mock(BasicFullHttpRequest.class);
    AsyncPromise<List<FullHttpResponse>> mockResponseFuture = mock(AsyncPromise.class);
    AsyncPromise<HttpResponseStatus> mockRetryFuture = mock(AsyncPromise.class);
    doReturn(true).when(mockRetryFuture).isCancelled();
    Executor contextExecutor = mock(Executor.class);
    doReturn(stats).when(routerStats).getStatsByType(any());
    Instance badInstance = new Instance("abd:123", "abc", 123);

    List<Instance> instanceList = new ArrayList<>();
    instanceList.add(badInstance);
    doReturn(instanceList).when(mockScatterGatherRequest).getHosts();

    RouterExceptionAndTrackingUtils.setRouterStats(routerStats);

    VenicePath mockPath = mock(VenicePath.class);
    doReturn("test_store").when(mockPath).getStoreName();
    doReturn(RequestType.SINGLE_GET).when(mockPath).getRequestType();
    doReturn(HttpMethod.GET).when(mockPath).getHttpMethod();
    doReturn(Integer.toString(ReadAvroProtocolDefinition.SINGLE_GET_ROUTER_REQUEST_V1.getProtocolVersion()))
        .when(mockPath)
        .getVeniceApiVersionHeader();
    doReturn(new HttpGet("http://" + "abc:123" + "/" + "mock_get")).when(mockPath).composeRouterRequest(any());
    VeniceDispatcher mockDispatcher = getMockDispatcher();

    mockDispatcher.dispatch(
        mockScatter,
        mockScatterGatherRequest,
        mockPath,
        mockRequest,
        mockHostSelected,
        mockResponseFuture,
        mockRetryFuture,
        mockTimeoutFuture,
        contextExecutor);
  }

  private VeniceDispatcher getMockDispatcher() {
    VeniceRouterConfig routerConfig = mock(VeniceRouterConfig.class);
    doReturn(2).when(routerConfig).getHttpClientPoolSize();
    doReturn(10).when(routerConfig).getMaxOutgoingConn();
    doReturn(5).when(routerConfig).getMaxOutgoingConnPerRoute();
    doReturn(10l).when(routerConfig).getMaxPendingRequest();
    doReturn(false).when(routerConfig).isSslToStorageNodes();
    doReturn(true).when(routerConfig).isSmartLongTailRetryEnabled();

    doReturn(TimeUnit.MINUTES.toMillis(1)).when(routerConfig).getLeakedFutureCleanupPollIntervalMs();
    doReturn(TimeUnit.MINUTES.toMillis(1)).when(routerConfig).getLeakedFutureCleanupThresholdMs();
    doReturn(24).when(routerConfig).getIoThreadCountInPoolMode();
    ReadOnlyStoreRepository mockStoreRepo = mock(ReadOnlyStoreRepository.class);
    MetricsRepository mockMetricsRepo = new MetricsRepository();
    RouterStats mockRouterStats = mock(RouterStats.class);
    RouteHttpRequestStats routeHttpRequestStats = mock(RouteHttpRequestStats.class);
    when(mockRouterStats.getStatsByType(any())).thenReturn(mock(AggRouterHttpRequestStats.class));

    StorageNodeClient storageNodeClient = mock(StorageNodeClient.class);
    VeniceDispatcher dispatcher = new VeniceDispatcher(
        routerConfig,
        mockStoreRepo,
        mockRouterStats,
        mockMetricsRepo,
        storageNodeClient,
        routeHttpRequestStats,
        mock(AggHostHealthStats.class),
        mock(RouterStats.class));
    return dispatcher;
  }

}
