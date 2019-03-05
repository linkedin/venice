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
import com.linkedin.venice.router.httpclient.ApacheHttpAsyncStorageNodeClient;
import com.linkedin.venice.router.httpclient.NettyStorageNodeClient;
import com.linkedin.venice.router.httpclient.StorageNodeClient;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.router.throttle.ReadRequestThrottler;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.utils.TestUtils;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
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
import java.util.concurrent.TimeUnit;
import org.apache.http.client.methods.HttpGet;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
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

  @DataProvider(name = "isNettyClientEnabled")
  public static Object[][] routerHttpClientStatus() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "isNettyClientEnabled")
  public void testErrorRetry(boolean useNettyClient) {
    VeniceDispatcher dispatcher = getMockDispatcher(useNettyClient);

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

  @Test(dataProvider = "isNettyClientEnabled")
  public void passesThroughHttp429(boolean useNettyClient) {
    VeniceDispatcher dispatcher = getMockDispatcher(useNettyClient);

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

  private VeniceDispatcher getMockDispatcher(boolean useNettyClient){
    VeniceRouterConfig routerConfig = mock(VeniceRouterConfig.class);
    doReturn(2).when(routerConfig).getHttpClientPoolSize();
    doReturn(10).when(routerConfig).getMaxOutgoingConn();
    doReturn(5).when(routerConfig).getMaxOutgoingConnPerRoute();
    doReturn(10).when(routerConfig).getNettyClientChannelPoolMaxPendingAcquires();
    doReturn(10l).when(routerConfig).getMaxPendingRequest();
    doReturn(false).when(routerConfig).isSslToStorageNodes();
    ReadOnlyStoreRepository mockStoreRepo = mock(ReadOnlyStoreRepository.class);
    MetricsRepository mockMetricsRepo = new MetricsRepository();
    RouterStats mockRouterStats = mock(RouterStats.class);
    when(mockRouterStats.getStatsByType(any())).thenReturn(mock(AggRouterHttpRequestStats.class));
    VeniceHostHealth mockHostHealth = mock(VeniceHostHealth.class);

    StorageNodeClient storageNodeClient;
    if (useNettyClient) {
      MultithreadEventLoopGroup workerEventLoopGroup;
      Class<? extends Channel> channelClass;
      try {
        workerEventLoopGroup = new EpollEventLoopGroup(1);
        channelClass = EpollSocketChannel.class;
      } catch (NoClassDefFoundError e) {
        workerEventLoopGroup = new NioEventLoopGroup(1);
        channelClass = NioSocketChannel.class;
      } catch (UnsatisfiedLinkError ee) {
        workerEventLoopGroup = new NioEventLoopGroup(1);
        channelClass = NioSocketChannel.class;
      }
      storageNodeClient = new NettyStorageNodeClient(routerConfig, Optional.empty(),
          mockRouterStats, workerEventLoopGroup, channelClass);
    } else {
      storageNodeClient = new ApacheHttpAsyncStorageNodeClient(routerConfig, Optional.empty(), mockMetricsRepo);
    }
    VeniceDispatcher dispatcher = new VeniceDispatcher(routerConfig, mockHostHealth, mockStoreRepo, Optional.empty(),
        mockRouterStats, mockMetricsRepo, storageNodeClient);
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
    doReturn(Unpooled.EMPTY_BUFFER).when(mockPath).getRequestBody();
    doReturn(HttpMethod.GET).when(mockPath).getHttpMethod();
    doReturn(Integer.toString(
        ReadAvroProtocolDefinition.SINGLE_GET_ROUTER_REQUEST_V1.getProtocolVersion())).when(mockPath).getVeniceApiVersionHeader();

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
