package com.linkedin.venice.router.api;

import static com.linkedin.venice.HttpConstants.VENICE_COMPRESSION_STRATEGY;
import static com.linkedin.venice.HttpConstants.VENICE_REQUEST_RCU;
import static com.linkedin.venice.HttpConstants.VENICE_SUPPORTED_COMPRESSION_STRATEGY;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.alpini.base.concurrency.AsyncPromise;
import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.alpini.router.api.RouterException;
import com.linkedin.alpini.router.api.Scatter;
import com.linkedin.alpini.router.api.ScatterGatherRequest;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.MockHttpServerWrapper;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.LiveInstanceMonitor;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.VeniceRouterConfig;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.httpclient.ApacheHttpAsyncStorageNodeClient;
import com.linkedin.venice.router.httpclient.StorageNodeClient;
import com.linkedin.venice.router.stats.AggHostHealthStats;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouteHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.utils.TestUtils;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.http.client.methods.HttpGet;
import org.testng.Assert;
import org.testng.annotations.Test;


//TODO: refactor Dispatcher to take a HttpClient Factory, so we don't need to spin up an HTTP server for these tests
public class TestVeniceDispatcher {
  @Test
  public void testErrorRetry() {
    VeniceDispatcher dispatcher = getMockDispatcher(false, false);
    try {
      AsyncPromise<List<FullHttpResponse>> mockResponseFuture = mock(AsyncPromise.class);
      AsyncPromise<HttpResponseStatus> mockRetryFuture = mock(AsyncPromise.class);
      List<HttpResponseStatus> successRetries = new ArrayList<>();
      doAnswer((invocation -> {
        successRetries.add(invocation.getArgument(0));
        return null;
      })).when(mockRetryFuture).setSuccess(any());

      triggerResponse(
          dispatcher,
          RequestType.SINGLE_GET,
          HttpResponseStatus.INTERNAL_SERVER_ERROR,
          mockRetryFuture,
          mockResponseFuture,
          () -> {
            Assert.assertEquals(successRetries.size(), 1);
            Assert.assertEquals(successRetries.get(0), HttpResponseStatus.INTERNAL_SERVER_ERROR);
          },
          false,
          CompressionStrategy.NO_OP);
    } finally {
      dispatcher.stop();
    }
  }

  @Test
  public void testErrorRetryOnPendingCheckFail() {
    VeniceDispatcher dispatcher = getMockDispatcher(true, false);
    try {
      AsyncPromise<List<FullHttpResponse>> mockResponseFuture = mock(AsyncPromise.class);
      AsyncPromise<HttpResponseStatus> mockRetryFuture = mock(AsyncPromise.class);
      List<HttpResponseStatus> successRetries = new ArrayList<>();
      doAnswer((invocation -> {
        successRetries.add(invocation.getArgument(0));
        return null;
      })).when(mockRetryFuture).setSuccess(any());

      triggerResponse(
          dispatcher,
          RequestType.SINGLE_GET,
          HttpResponseStatus.OK,
          mockRetryFuture,
          mockResponseFuture,
          () -> {
            Assert.assertEquals(successRetries.size(), 2);
            Assert.assertEquals(successRetries.get(0), HttpResponseStatus.INTERNAL_SERVER_ERROR);
            Assert.assertEquals(dispatcher.getPendingRequestThrottler().getCurrentPendingRequestCount(), 0);
          },
          false,
          CompressionStrategy.NO_OP);
    } finally {
      dispatcher.stop();
    }
  }

  @Test
  public void testErrorRetryOnPendingCheckLeak() {
    VeniceDispatcher dispatcher = getMockDispatcher(false, true);
    try {
      AsyncPromise<List<FullHttpResponse>> mockResponseFuture = mock(AsyncPromise.class);
      AsyncPromise<HttpResponseStatus> mockRetryFuture = mock(AsyncPromise.class);
      List<HttpResponseStatus> successRetries = new ArrayList<>();
      doAnswer((invocation -> {
        successRetries.add(invocation.getArgument(0));
        return null;
      })).when(mockRetryFuture).setSuccess(any());

      triggerResponse(
          dispatcher,
          RequestType.SINGLE_GET,
          HttpResponseStatus.OK,
          mockRetryFuture,
          mockResponseFuture,
          () -> {
            Assert.assertEquals(successRetries.size(), 0);
            verify(dispatcher.getRouteHttpRequestStats(), times(1)).recordFinishedRequest(any());
            verify(dispatcher.getRouteHttpRequestStats(), times(1)).getPendingRequestCount(any());
          },
          true,
          CompressionStrategy.NO_OP);
    } finally {
      dispatcher.stop();
    }
  }

  @Test
  public void passesThroughHttp429() {
    VeniceDispatcher dispatcher = getMockDispatcher(false, false);
    try {
      AsyncPromise<List<FullHttpResponse>> mockResponseFuture = mock(AsyncPromise.class);
      AsyncPromise<HttpResponseStatus> mockRetryFuture = mock(AsyncPromise.class);
      List<FullHttpResponse> responses = new ArrayList<>();
      doAnswer((invocation -> {
        responses.addAll(invocation.getArgument(0));
        return null;
      })).when(mockResponseFuture).setSuccess(any());

      triggerResponse(
          dispatcher,
          RequestType.SINGLE_GET,
          HttpResponseStatus.TOO_MANY_REQUESTS,
          mockRetryFuture,
          mockResponseFuture,
          () -> {
            Assert.assertEquals(responses.size(), 1);
            Assert.assertEquals(responses.get(0).status(), HttpResponseStatus.TOO_MANY_REQUESTS);
          },
          false,
          CompressionStrategy.NO_OP);
    } finally {
      dispatcher.stop();
    }
  }

  @Test
  public void passThroughCompressedDataIfClientSupportsDecompressionForSingleGet() {
    VeniceDispatcher dispatcher = getMockDispatcher(false, false);
    try {
      AsyncPromise<List<FullHttpResponse>> mockResponseFuture = mock(AsyncPromise.class);
      AsyncPromise<HttpResponseStatus> mockRetryFuture = mock(AsyncPromise.class);
      List<FullHttpResponse> responses = new ArrayList<>();
      doAnswer((invocation -> {
        responses.addAll(invocation.getArgument(0));
        return null;
      })).when(mockResponseFuture).setSuccess(any());

      triggerResponse(
          dispatcher,
          RequestType.SINGLE_GET,
          HttpResponseStatus.OK,
          mockRetryFuture,
          mockResponseFuture,
          () -> {
            Assert.assertEquals(responses.size(), 1);
            Assert.assertEquals(responses.get(0).status(), HttpResponseStatus.OK);
            Assert.assertEquals(
                responses.get(0).headers().get(VENICE_COMPRESSION_STRATEGY),
                String.valueOf(CompressionStrategy.GZIP.getValue()));
            Assert.assertTrue(responses.get(0).headers().contains(VENICE_REQUEST_RCU));
          },
          false,
          CompressionStrategy.GZIP);
    } finally {
      dispatcher.stop();
    }
  }

  @Test
  public void decompressRecordIfClientDoesntSupportsDecompressionForSingleGet() {
    VeniceDispatcher dispatcher = getMockDispatcher(false, false);
    try {
      AsyncPromise<List<FullHttpResponse>> mockResponseFuture = mock(AsyncPromise.class);
      AsyncPromise<HttpResponseStatus> mockRetryFuture = mock(AsyncPromise.class);
      List<FullHttpResponse> responses = new ArrayList<>();
      doAnswer((invocation -> {
        responses.addAll(invocation.getArgument(0));
        return null;
      })).when(mockResponseFuture).setSuccess(any());

      triggerResponse(
          dispatcher,
          RequestType.SINGLE_GET,
          HttpResponseStatus.OK,
          mockRetryFuture,
          mockResponseFuture,
          () -> {
            Assert.assertEquals(responses.size(), 1);
            Assert.assertEquals(responses.get(0).status(), HttpResponseStatus.OK);
            Assert.assertEquals(
                responses.get(0).headers().get(VENICE_COMPRESSION_STRATEGY),
                String.valueOf(CompressionStrategy.NO_OP.getValue()));
          },
          false,
          CompressionStrategy.ZSTD_WITH_DICT);
    } finally {
      dispatcher.stop();
    }
  }

  @Test
  public void passThroughCompressedDataIfClientSupportsDecompressionForMultiGet() {
    VeniceDispatcher dispatcher = getMockDispatcher(false, false);
    try {
      AsyncPromise<List<FullHttpResponse>> mockResponseFuture = mock(AsyncPromise.class);
      AsyncPromise<HttpResponseStatus> mockRetryFuture = mock(AsyncPromise.class);
      List<FullHttpResponse> responses = new ArrayList<>();
      doAnswer((invocation -> {
        responses.addAll(invocation.getArgument(0));
        return null;
      })).when(mockResponseFuture).setSuccess(any());

      triggerResponse(
          dispatcher,
          RequestType.MULTI_GET,
          HttpResponseStatus.OK,
          mockRetryFuture,
          mockResponseFuture,
          () -> {
            Assert.assertEquals(responses.size(), 1);
            Assert.assertEquals(responses.get(0).status(), HttpResponseStatus.OK);
            Assert.assertEquals(
                responses.get(0).headers().get(VENICE_COMPRESSION_STRATEGY),
                String.valueOf(CompressionStrategy.GZIP.getValue()));
          },
          false,
          CompressionStrategy.GZIP);
    } finally {
      dispatcher.stop();
    }
  }

  @Test
  public void decompressRecordIfClientDoesntSupportsDecompressionForMultiGet() {
    VeniceDispatcher dispatcher = getMockDispatcher(false, false);
    try {
      AsyncPromise<List<FullHttpResponse>> mockResponseFuture = mock(AsyncPromise.class);
      AsyncPromise<HttpResponseStatus> mockRetryFuture = mock(AsyncPromise.class);
      List<FullHttpResponse> responses = new ArrayList<>();
      doAnswer((invocation -> {
        responses.addAll(invocation.getArgument(0));
        return null;
      })).when(mockResponseFuture).setSuccess(any());

      triggerResponse(
          dispatcher,
          RequestType.MULTI_GET,
          HttpResponseStatus.OK,
          mockRetryFuture,
          mockResponseFuture,
          () -> {
            Assert.assertEquals(responses.size(), 1);
            Assert.assertEquals(responses.get(0).status(), HttpResponseStatus.OK);
            Assert.assertEquals(
                responses.get(0).headers().get(VENICE_COMPRESSION_STRATEGY),
                String.valueOf(CompressionStrategy.NO_OP.getValue()));
          },
          false,
          CompressionStrategy.ZSTD_WITH_DICT);
    } finally {
      dispatcher.stop();
    }
  }

  private VeniceDispatcher getMockDispatcher(boolean forcePendingCheck, boolean forceLeakPending) {
    VeniceRouterConfig routerConfig = mock(VeniceRouterConfig.class);
    doReturn(2).when(routerConfig).getHttpClientPoolSize();
    doReturn(10).when(routerConfig).getMaxOutgoingConn();
    doReturn(5).when(routerConfig).getMaxOutgoingConnPerRoute();
    doReturn(10l).when(routerConfig).getMaxPendingRequest();
    doReturn(false).when(routerConfig).isSslToStorageNodes();

    doReturn(TimeUnit.MINUTES.toMillis(1)).when(routerConfig).getLeakedFutureCleanupPollIntervalMs();
    doReturn(TimeUnit.MINUTES.toMillis(1)).when(routerConfig).getLeakedFutureCleanupThresholdMs();
    doReturn(24).when(routerConfig).getIoThreadCountInPoolMode();
    ReadOnlyStoreRepository mockStoreRepo = mock(ReadOnlyStoreRepository.class);
    MetricsRepository mockMetricsRepo = new MetricsRepository();
    RouterStats mockRouterStats = mock(RouterStats.class);
    RouteHttpRequestStats routeHttpRequestStats = mock(RouteHttpRequestStats.class);
    when(mockRouterStats.getStatsByType(any())).thenReturn(mock(AggRouterHttpRequestStats.class));
    if (forcePendingCheck) {
      doReturn(true).when(routerConfig).isStatefulRouterHealthCheckEnabled();
      doReturn(5).when(routerConfig).getRouterUnhealthyPendingConnThresholdPerRoute();
      doReturn(10l).when(routeHttpRequestStats).getPendingRequestCount(anyString());
    }
    if (forceLeakPending) {
      doReturn(1l).doReturn(0l).when(routerConfig).getMaxPendingRequest();
      doReturn(true).when(routerConfig).isStatefulRouterHealthCheckEnabled();
      doReturn(15).when(routerConfig).getRouterUnhealthyPendingConnThresholdPerRoute();
      doReturn(10l).when(routeHttpRequestStats).getPendingRequestCount(anyString());
    }
    LiveInstanceMonitor mockLiveInstanceMonitor = mock(LiveInstanceMonitor.class);
    StorageNodeClient storageNodeClient =
        new ApacheHttpAsyncStorageNodeClient(routerConfig, Optional.empty(), mockMetricsRepo, mockLiveInstanceMonitor);
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

  private void triggerResponse(
      VeniceDispatcher dispatcher,
      RequestType requestType,
      HttpResponseStatus responseStatus,
      AsyncPromise mockRetryFuture,
      AsyncPromise mockResponseFuture,
      Runnable assertions,
      boolean forceLeakPending,
      CompressionStrategy compressionStrategy) {

    Scatter mockScatter = mock(Scatter.class);
    ScatterGatherRequest mockScatterGatherRequest = mock(ScatterGatherRequest.class);
    RouterStats<AggRouterHttpRequestStats> routerStats = mock(RouterStats.class);
    AggRouterHttpRequestStats stats = mock(AggRouterHttpRequestStats.class);

    doReturn(stats).when(routerStats).getStatsByType(any());

    RouterExceptionAndTrackingUtils.setRouterStats(routerStats);

    VenicePath mockPath = mock(VenicePath.class);
    doReturn("test_store").when(mockPath).getStoreName();
    doReturn(requestType).when(mockPath).getRequestType();
    doReturn(HttpMethod.GET).when(mockPath).getHttpMethod();

    if (requestType.equals(RequestType.SINGLE_GET)) {
      doReturn(Integer.toString(ReadAvroProtocolDefinition.SINGLE_GET_ROUTER_REQUEST_V1.getProtocolVersion()))
          .when(mockPath)
          .getVeniceApiVersionHeader();
    } else if (requestType.equals(RequestType.MULTI_GET)) {
      doReturn(Integer.toString(ReadAvroProtocolDefinition.MULTI_GET_ROUTER_REQUEST_V1.getProtocolVersion()))
          .when(mockPath)
          .getVeniceApiVersionHeader();
    }

    BasicFullHttpRequest mockRequest = mock(BasicFullHttpRequest.class);

    Map<String, String> requestHeadersMap = new HashMap<>();
    requestHeadersMap.put(VENICE_SUPPORTED_COMPRESSION_STRATEGY, String.valueOf(CompressionStrategy.GZIP.getValue()));

    HttpHeaders requestHeaders = new DefaultHttpHeaders();

    for (Map.Entry<String, String> header: requestHeadersMap.entrySet()) {
      requestHeaders.add(header.getKey(), header.getValue());
    }

    doReturn(requestHeaders).when(mockRequest).headers();

    CompressorFactory compressorFactory = mock(CompressorFactory.class);

    VeniceCompressor modifyingCompressor = mock(VeniceCompressor.class);
    VeniceCompressor noOpCompressor = mock(VeniceCompressor.class);

    try {
      doReturn(ByteBuffer.allocate(10)).when(modifyingCompressor).decompress(any(ByteBuffer.class));
    } catch (IOException e) {
      Assert.fail();
    }

    doReturn(modifyingCompressor).when(compressorFactory).getVersionSpecificCompressor(anyString());

    doAnswer((invocation -> {
      if (CompressionStrategy.NO_OP.equals(invocation.getArgument(0))) {
        return noOpCompressor;
      }

      return modifyingCompressor;
    })).when(compressorFactory).getCompressor(any());

    doReturn(new VeniceResponseDecompressor(true, routerStats, mockRequest, "test_store", 1, compressorFactory))
        .when(mockPath)
        .getResponseDecompressor();

    AsyncPromise mockHostSelected = mock(AsyncPromise.class);
    AsyncPromise mockTimeoutFuture = mock(AsyncPromise.class);
    Executor contextExecutor = (runnable) -> runnable.run();

    try (MockHttpServerWrapper mockHttpServerWrapper = ServiceFactory.getMockHttpServer("mock_storage_node")) {
      FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, responseStatus);
      mockHttpServerWrapper.addResponseForUriPattern(".*", response);

      response.headers()
          .set(HttpHeaderNames.CONTENT_TYPE, "avro/binary")
          .set(HttpConstants.VENICE_STORE_VERSION, "1")
          .set(HttpHeaderNames.CONTENT_LENGTH, "0")
          .set(HttpConstants.VENICE_SCHEMA_ID, "1")
          .set(HttpConstants.VENICE_COMPRESSION_STRATEGY, compressionStrategy.getValue());

      String serverAddr = mockHttpServerWrapper.getAddress();

      Instance badInstance = new Instance(serverAddr, mockHttpServerWrapper.getHost(), mockHttpServerWrapper.getPort());
      List<Instance> instanceList = new ArrayList<>();
      instanceList.add(badInstance);
      doReturn(instanceList).when(mockScatterGatherRequest).getHosts();

      doReturn(new HttpGet("http://" + serverAddr + "/" + "mock_get")).when(mockPath).composeRouterRequest(any());

      try {
        dispatcher.dispatch(
            mockScatter,
            mockScatterGatherRequest,
            mockPath,
            mockRequest,
            mockHostSelected,
            mockResponseFuture,
            mockRetryFuture,
            mockTimeoutFuture,
            contextExecutor);
      } catch (RouterException e) {
        if (!forceLeakPending) {
          throw new VeniceException(e);
        }
      }

      TestUtils.waitForNonDeterministicAssertion(2, TimeUnit.SECONDS, () -> assertions.run());
    }
  }
}
