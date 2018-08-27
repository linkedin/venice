package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.base.concurrency.AsyncPromise;
import com.linkedin.ddsstorage.netty4.misc.BasicHttpRequest;
import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.ddsstorage.router.api.Scatter;
import com.linkedin.ddsstorage.router.api.ScatterGatherRequest;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.MockHttpServerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.throttle.ReadRequestThrottler;
import com.linkedin.venice.utils.TestUtils;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.tehuti.metrics.MetricsRepository;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.http.client.methods.HttpGet;
import org.jetbrains.annotations.NotNull;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

//TODO: refactor Dispatcher to take a HttpClient Factory, so we don't need to spin up an HTTP server for these tests
public class TestVeniceDispatcher {
  @Test
  public void parsesPartitionName(){
    String name = "myCountry_v1_2";
    String number = VeniceDispatcher.numberFromPartitionName(name);

    Assert.assertEquals(number, "2");
  }

  @Test
  public void testErrorRetry() {
    VeniceDispatcher dispatcher = getMockDispatcher();

    AsyncPromise<List<FullHttpResponse>> mockResponseFuture = mock(AsyncPromise.class);
    AsyncPromise<HttpResponseStatus> mockRetryFuture = mock(AsyncPromise.class);
    List<HttpResponseStatus> successRetries = new ArrayList<>();
    doAnswer((invocation -> {
      successRetries.add(invocation.getArgument(0));
      return null;
    })).when(mockRetryFuture).setSuccess(any());

    triggerResponse(dispatcher, HttpResponseStatus.INTERNAL_SERVER_ERROR, mockRetryFuture, mockResponseFuture, () -> {
      Assert.assertEquals(successRetries.size(), 1);
      Assert.assertEquals(successRetries.get(0), HttpResponseStatus.INTERNAL_SERVER_ERROR);
    });

  }

  @Test
  public void passesThroughHttp429() {
    VeniceDispatcher dispatcher = getMockDispatcher();

    AsyncPromise<List<FullHttpResponse>> mockResponseFuture = mock(AsyncPromise.class);
    AsyncPromise<HttpResponseStatus> mockRetryFuture = mock(AsyncPromise.class);
    List<FullHttpResponse> responses = new ArrayList<>();
    doAnswer((invocation -> {
      responses.addAll(invocation.getArgument(0));
      return null;
    })).when(mockResponseFuture).setSuccess(any());

    triggerResponse(dispatcher, HttpResponseStatus.TOO_MANY_REQUESTS, mockRetryFuture, mockResponseFuture, () -> {
      Assert.assertEquals(responses.size(), 1);
      Assert.assertEquals(responses.get(0).status(), HttpResponseStatus.TOO_MANY_REQUESTS);
    });

  }

  private VeniceDispatcher getMockDispatcher(){
    VeniceRouterConfig routerConfig = mock(VeniceRouterConfig.class);
    doReturn(2).when(routerConfig).getHttpClientPoolSize();
    doReturn(10).when(routerConfig).getMaxOutgoingConn();
    doReturn(5).when(routerConfig).getMaxOutgoingConnPerRoute();
    doReturn(10l).when(routerConfig).getMaxPendingRequestPerHttpClient();
    ReadOnlyStoreRepository mockStoreRepo = mock(ReadOnlyStoreRepository.class);
    AggRouterHttpRequestStats mockStatsForSingleGet = mock(AggRouterHttpRequestStats.class);
    AggRouterHttpRequestStats mockStatsForMultiGet = mock(AggRouterHttpRequestStats.class);
    MetricsRepository mockMetricsRepo = new MetricsRepository();
    VeniceHostHealth mockHostHealth = mock(VeniceHostHealth.class);

    VeniceDispatcher dispatcher = new VeniceDispatcher(routerConfig, mockHostHealth, Optional.empty(), mockStoreRepo,
        Optional.empty(), mockStatsForSingleGet, mockStatsForMultiGet, Optional.empty(), mockMetricsRepo);
    dispatcher.initReadRequestThrottler(mock(ReadRequestThrottler.class));
    return dispatcher;
  }

  private void triggerResponse(VeniceDispatcher dispatcher, HttpResponseStatus responseStatus,
      AsyncPromise mockRetryFuture, AsyncPromise mockResponseFuture, Runnable assertions) {

    Scatter mockScatter = mock(Scatter.class);
    ScatterGatherRequest mockScatterGatherRequest = mock(ScatterGatherRequest.class);
    Set<String> partitionNames = new HashSet<>();
    partitionNames.add("test_store_v1_1");
    doReturn(partitionNames).when(mockScatterGatherRequest).getPartitionsNames();

    VenicePath mockPath = mock(VenicePath.class);
    doReturn("test_store").when(mockPath).getStoreName();
    doReturn(RequestType.SINGLE_GET).when(mockPath).getRequestType();

    BasicHttpRequest mockRequest = mock(BasicHttpRequest.class);

    AsyncPromise mockHostSelected = mock(AsyncPromise.class);
    AsyncPromise mockTimeoutFuture = mock(AsyncPromise.class);
    Executor contextExecutor = (runnable) -> runnable.run();

    try (MockHttpServerWrapper mockHttpServerWrapper = ServiceFactory.getMockHttpServer("mock_storage_node")) {
      FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, responseStatus);
      mockHttpServerWrapper.addResponseForUriPattern(".*", response);
      String serverAddr = mockHttpServerWrapper.getAddress();

      Instance badInstance = new Instance(serverAddr, mockHttpServerWrapper.getHost(), mockHttpServerWrapper.getPort());
      List<Instance> instanceList = new ArrayList<>();
      instanceList.add(badInstance);
      doReturn(instanceList).when(mockScatterGatherRequest).getHosts();

      doReturn(new HttpGet("http://" + serverAddr + "/" + "mock_get")).when(mockPath).composeRouterRequest(any());

      try {
        dispatcher.dispatch(mockScatter, mockScatterGatherRequest, mockPath, mockRequest, mockHostSelected,
            mockResponseFuture, mockRetryFuture, mockTimeoutFuture, contextExecutor);
      } catch (RouterException e) {
        throw new VeniceException(e);
      }

      TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.SECONDS, () -> assertions.run());

    }
  }
}
