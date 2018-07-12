package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.base.concurrency.AsyncPromise;
import com.linkedin.ddsstorage.netty4.misc.BasicHttpRequest;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.ddsstorage.router.api.Scatter;
import com.linkedin.ddsstorage.router.api.ScatterGatherRequest;
import com.linkedin.venice.integration.utils.MockHttpServerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.throttle.ReadRequestThrottler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.tehuti.metrics.MetricsRepository;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.http.client.methods.HttpGet;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class TestVeniceDispatcher {
  @Test
  public void parsesPartitionName(){
    String name = "myCountry_v1_2";
    String number = VeniceDispatcher.numberFromPartitionName(name);

    Assert.assertEquals(number, "2");
  }

  @Test
  public void testErrorRetry() throws RouterException {
    VeniceRouterConfig routerConfig = mock(VeniceRouterConfig.class);
    doReturn(2).when(routerConfig).getHttpClientPoolSize();
    doReturn(10).when(routerConfig).getMaxOutgoingConn();
    doReturn(5).when(routerConfig).getMaxOutgoingConnPerRoute();
    ReadOnlyStoreRepository mockStoreRepo = mock(ReadOnlyStoreRepository.class);
    AggRouterHttpRequestStats mockStatsForSingleGet = mock(AggRouterHttpRequestStats.class);
    AggRouterHttpRequestStats mockStatsForMultiGet = mock(AggRouterHttpRequestStats.class);
    MetricsRepository mockMetricsRepo = new MetricsRepository();
    VeniceHostHealth mockHostHealth = mock(VeniceHostHealth.class);

    VeniceDispatcher dispatcher = new VeniceDispatcher(routerConfig, mockHostHealth, Optional.empty(), mockStoreRepo,
        Optional.empty(), mockStatsForSingleGet, mockStatsForMultiGet, Optional.empty(), mockMetricsRepo);
    dispatcher.initReadRequestThrottler(mock(ReadRequestThrottler.class));

    Scatter mockScatter = mock(Scatter.class);
    ScatterGatherRequest mockScatterGatherRequest = mock(ScatterGatherRequest.class);
    Set<String> partitionNames = new HashSet<>();
    partitionNames.add("test_store_v1_1");
    doReturn(partitionNames).when(mockScatterGatherRequest).getPartitionsNames();

    VenicePath mockPath = mock(VenicePath.class);
    doReturn("test_store").when(mockPath).getStoreName();
    doReturn(RequestType.SINGLE_GET).when(mockPath).getRequestType();

    BasicHttpRequest mockRequest = mock(BasicHttpRequest.class);
    AsyncPromise mockHostSelected =mock(AsyncPromise.class);
    AsyncPromise mockResponseFuture = mock(AsyncPromise.class);
    AsyncPromise mockRetryFuture = mock(AsyncPromise.class);
    AsyncPromise mockTimeoutFuture = mock(AsyncPromise.class);
    Executor contextExecutor = Executors.newSingleThreadExecutor();

    try (MockHttpServerWrapper mockHttpServerWrapper = ServiceFactory.getMockHttpServer("mock_storage_node")) {
      FullHttpResponse errorResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
      mockHttpServerWrapper.addResponseForUriPattern(".*", errorResponse);
      String serverAddr = mockHttpServerWrapper.getAddress();

      Instance badInstance = new Instance(serverAddr, mockHttpServerWrapper.getHost(), mockHttpServerWrapper.getPort());
      List<Instance> instanceList = new ArrayList<>();
      instanceList.add(badInstance);
      doReturn(instanceList).when(mockScatterGatherRequest).getHosts();

      doReturn(new HttpGet("http://" + serverAddr + "/" + "mock_get")).when(mockPath).composeRouterRequest(any());

      dispatcher.dispatch(mockScatter, mockScatterGatherRequest, mockPath, mockRequest, mockHostSelected,
          mockResponseFuture, mockRetryFuture, mockTimeoutFuture, contextExecutor);

      verify(mockRetryFuture, timeout(1000)).setSuccess(HttpResponseStatus.valueOf(HttpResponseStatus.INTERNAL_SERVER_ERROR.code()));
    }

  }
}
