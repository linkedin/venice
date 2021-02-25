package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.netty4.misc.BasicFullHttpRequest;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.venice.HttpConstants.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static org.mockito.Mockito.*;


public class TestVeniceResponseDecompressor {
  /**
   * If client supports decompression and the single get request was successful, then the router should return the
   * compression strategy in the response header.
   */
  @Test
  public void TestRouterReturnsCompressionStrategyHeaderIfClientSupportsDecompression() {
    BasicFullHttpRequest request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "storage/ZstdThreeStringFieldWithPrefix/ApqFzqwN?f=b64", System.currentTimeMillis(), 100000);
    request.headers().add(VENICE_SUPPORTED_COMPRESSION_STRATEGY, CompressionStrategy.GZIP.getValue());

    VeniceResponseDecompressor responseDecompressor = new VeniceResponseDecompressor(true, null, request, "test-store", 1);

    CompositeByteBuf content = Unpooled.compositeBuffer();
    FullHttpResponse storageNodeResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK, content);
    storageNodeResponse.headers().add(VENICE_COMPRESSION_STRATEGY, CompressionStrategy.GZIP.getValue());

    FullHttpResponse routerResponse = responseDecompressor.processSingleGetResponse(storageNodeResponse);

    Assert.assertEquals(routerResponse.status(), OK);
    Assert.assertEquals(routerResponse.headers().get(VENICE_COMPRESSION_STRATEGY), String.valueOf(CompressionStrategy.GZIP.getValue()));
  }

  /**
   * If client doesn't support decompression and the single get request was successful, then the router should return
   * NO_OP compression strategy in the response header.
   */
  @Test
  public void TestRouterReturnsNoopCompressionStrategyHeaderIfClientDoesntSupportsDecompression() {
    BasicFullHttpRequest request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "storage/ZstdThreeStringFieldWithPrefix/ApqFzqwN?f=b64", System.currentTimeMillis(), 100000);
    request.headers().add(VENICE_SUPPORTED_COMPRESSION_STRATEGY, CompressionStrategy.GZIP.getValue());

    RouterStats<AggRouterHttpRequestStats> routerStats = mock(RouterStats.class);
    AggRouterHttpRequestStats stats = mock(AggRouterHttpRequestStats.class);

    doReturn(stats).when(routerStats).getStatsByType(any());

    RouterExceptionAndTrackingUtils.setRouterStats(routerStats);

    CompressorFactory.createVersionSpecificCompressorIfNotExist(CompressionStrategy.ZSTD_WITH_DICT, "test-store_v1", new byte[]{}, 22);

    VeniceResponseDecompressor responseDecompressor = new VeniceResponseDecompressor(true, routerStats, request, "test-store", 1);

    CompositeByteBuf content = Unpooled.compositeBuffer();
    FullHttpResponse storageNodeResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK, content);
    storageNodeResponse.headers().add(VENICE_COMPRESSION_STRATEGY, CompressionStrategy.ZSTD_WITH_DICT.getValue());

    FullHttpResponse routerResponse = responseDecompressor.processSingleGetResponse(storageNodeResponse);

    Assert.assertEquals(routerResponse.status(), OK);
    Assert.assertEquals(routerResponse.headers().get(VENICE_COMPRESSION_STRATEGY), String.valueOf(CompressionStrategy.NO_OP.getValue()));
  }

  /**
   * If client doesn't support decompression and the single get request was unsuccessful, then the router should return
   * NO_OP compression strategy in the response header.
   */
  @Test
  public void TestRouterReturnsNoopCompressionStrategyHeaderIfClientDoesntSupportsDecompressionAndServerReturnsError() {
    BasicFullHttpRequest request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "storage/ZstdThreeStringFieldWithPrefix/ApqFzqwN?f=b64", System.currentTimeMillis(), 100000);
    request.headers().add(VENICE_SUPPORTED_COMPRESSION_STRATEGY, CompressionStrategy.GZIP.getValue());

    RouterStats<AggRouterHttpRequestStats> routerStats = mock(RouterStats.class);
    AggRouterHttpRequestStats stats = mock(AggRouterHttpRequestStats.class);

    doReturn(stats).when(routerStats).getStatsByType(any());

    RouterExceptionAndTrackingUtils.setRouterStats(routerStats);

    CompressorFactory.createVersionSpecificCompressorIfNotExist(CompressionStrategy.ZSTD_WITH_DICT, "test-store_v1", new byte[]{}, 22);

    VeniceResponseDecompressor responseDecompressor = new VeniceResponseDecompressor(true, routerStats, request, "test-store", 1);

    CompositeByteBuf content = Unpooled.compositeBuffer();
    FullHttpResponse storageNodeResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, NOT_FOUND, content);
    storageNodeResponse.headers().add(VENICE_COMPRESSION_STRATEGY, CompressionStrategy.ZSTD_WITH_DICT.getValue());

    FullHttpResponse routerResponse = responseDecompressor.processSingleGetResponse(storageNodeResponse);

    Assert.assertEquals(routerResponse.status(), NOT_FOUND);
    Assert.assertEquals(routerResponse.headers().get(VENICE_COMPRESSION_STRATEGY), String.valueOf(CompressionStrategy.NO_OP.getValue()));
  }

  /**
   * If client supports decompression and the single get request was unsuccessful, then the router should return the
   * compression strategy in the response header.
   */
  @Test
  public void TestRouterReturnsCompressionStrategyHeaderIfClientSupportsDecompressionAndServerReturnsError() {
    BasicFullHttpRequest request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "storage/ZstdThreeStringFieldWithPrefix/ApqFzqwN?f=b64", System.currentTimeMillis(), 100000);
    request.headers().add(VENICE_SUPPORTED_COMPRESSION_STRATEGY, CompressionStrategy.GZIP.getValue());

    RouterStats<AggRouterHttpRequestStats> routerStats = mock(RouterStats.class);
    AggRouterHttpRequestStats stats = mock(AggRouterHttpRequestStats.class);

    doReturn(stats).when(routerStats).getStatsByType(any());

    RouterExceptionAndTrackingUtils.setRouterStats(routerStats);

    VeniceResponseDecompressor responseDecompressor = new VeniceResponseDecompressor(true, routerStats, request, "test-store", 1);

    CompositeByteBuf content = Unpooled.compositeBuffer();
    FullHttpResponse storageNodeResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, NOT_FOUND, content);
    storageNodeResponse.headers().add(VENICE_COMPRESSION_STRATEGY, CompressionStrategy.GZIP.getValue());

    FullHttpResponse routerResponse = responseDecompressor.processSingleGetResponse(storageNodeResponse);

    Assert.assertEquals(routerResponse.status(), NOT_FOUND);
    Assert.assertEquals(routerResponse.headers().get(VENICE_COMPRESSION_STRATEGY), String.valueOf(CompressionStrategy.GZIP.getValue()));
  }

  /**
   * If client supports decompression and the multi get request was successful, then the router should return the
   * compression strategy in the response header.
   */
  @Test
  public void TestRouterReturnsCompressionStrategyHeaderIfClientSupportsDecompressionForMultiGet() {
    BasicFullHttpRequest request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "storage/ZstdThreeStringFieldWithPrefix/ApqFzqwN?f=b64", System.currentTimeMillis(), 100000);
    request.headers().add(VENICE_SUPPORTED_COMPRESSION_STRATEGY, CompressionStrategy.GZIP.getValue());

    RouterStats<AggRouterHttpRequestStats> routerStats = mock(RouterStats.class);
    AggRouterHttpRequestStats stats = mock(AggRouterHttpRequestStats.class);

    doReturn(stats).when(routerStats).getStatsByType(any());

    RouterExceptionAndTrackingUtils.setRouterStats(routerStats);

    VeniceResponseDecompressor responseDecompressor = new VeniceResponseDecompressor(true, routerStats, request, "test-store", 1);

    CompositeByteBuf content = Unpooled.compositeBuffer();
    FullHttpResponse storageNodeResponse1 = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK, content);
    storageNodeResponse1.headers().add(VENICE_COMPRESSION_STRATEGY, CompressionStrategy.GZIP.getValue());
    storageNodeResponse1.headers().add(VENICE_SCHEMA_ID, "1");
    storageNodeResponse1.headers().add(HttpHeaderNames.CONTENT_TYPE, HttpConstants.AVRO_BINARY);

    FullHttpResponse storageNodeResponse2 = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK, content);
    storageNodeResponse2.headers().add(VENICE_COMPRESSION_STRATEGY, CompressionStrategy.GZIP.getValue());
    storageNodeResponse2.headers().add(VENICE_SCHEMA_ID, "1");
    storageNodeResponse2.headers().add(HttpHeaderNames.CONTENT_TYPE, HttpConstants.AVRO_BINARY);

    FullHttpResponse routerResponse = responseDecompressor.processMultiGetResponses(Arrays.asList(new FullHttpResponse[]{storageNodeResponse1, storageNodeResponse2}));

    Assert.assertEquals(routerResponse.status(), OK);
    Assert.assertEquals(routerResponse.headers().get(VENICE_COMPRESSION_STRATEGY), String.valueOf(CompressionStrategy.GZIP.getValue()));
  }

  /**
   * If client supports decompression and the multi get request was unsuccessful, then the router should return the
   * compression strategy in the response header.
   */
  @Test
  public void TestRouterReturnsCompressionStrategyHeaderIfClientSupportsDecompressionAndServerReturnsErrorForMultiGet() {
    BasicFullHttpRequest request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "storage/ZstdThreeStringFieldWithPrefix/ApqFzqwN?f=b64", System.currentTimeMillis(), 100000);
    request.headers().add(VENICE_SUPPORTED_COMPRESSION_STRATEGY, CompressionStrategy.GZIP.getValue());

    RouterStats<AggRouterHttpRequestStats> routerStats = mock(RouterStats.class);
    AggRouterHttpRequestStats stats = mock(AggRouterHttpRequestStats.class);

    doReturn(stats).when(routerStats).getStatsByType(any());

    RouterExceptionAndTrackingUtils.setRouterStats(routerStats);

    VeniceResponseDecompressor responseDecompressor = new VeniceResponseDecompressor(true, routerStats, request, "test-store", 1);

    CompositeByteBuf content = Unpooled.compositeBuffer();
    FullHttpResponse storageNodeResponse1 = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, NOT_FOUND, content);
    storageNodeResponse1.headers().add(VENICE_COMPRESSION_STRATEGY, CompressionStrategy.GZIP.getValue());
    storageNodeResponse1.headers().add(VENICE_SCHEMA_ID, "1");
    storageNodeResponse1.headers().add(HttpHeaderNames.CONTENT_TYPE, HttpConstants.AVRO_BINARY);

    FullHttpResponse storageNodeResponse2 = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, NOT_FOUND, content);
    storageNodeResponse2.headers().add(VENICE_COMPRESSION_STRATEGY, CompressionStrategy.GZIP.getValue());
    storageNodeResponse2.headers().add(VENICE_SCHEMA_ID, "1");
    storageNodeResponse2.headers().add(HttpHeaderNames.CONTENT_TYPE, HttpConstants.AVRO_BINARY);

    FullHttpResponse routerResponse = responseDecompressor.processMultiGetResponses(
        Arrays.asList(new FullHttpResponse[]{storageNodeResponse1, storageNodeResponse2}));

    Assert.assertEquals(routerResponse.status(), NOT_FOUND);
    Assert.assertEquals(routerResponse.headers().get(VENICE_COMPRESSION_STRATEGY), String.valueOf(CompressionStrategy.GZIP.getValue()));
  }

  /**
   * If client doesn't support decompression and the multi get request was successful, then the router should return
   * NO_OP compression strategy in the response header.
   */
  @Test
  public void TestRouterReturnsNoopCompressionStrategyHeaderIfClientDoesntSupportsDecompressionForMultiGet() {
    BasicFullHttpRequest request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "storage/ZstdThreeStringFieldWithPrefix/ApqFzqwN?f=b64", System.currentTimeMillis(), 100000);
    request.headers().add(VENICE_SUPPORTED_COMPRESSION_STRATEGY, CompressionStrategy.GZIP.getValue());

    RouterStats<AggRouterHttpRequestStats> routerStats = mock(RouterStats.class);
    AggRouterHttpRequestStats stats = mock(AggRouterHttpRequestStats.class);

    doReturn(stats).when(routerStats).getStatsByType(any());

    RouterExceptionAndTrackingUtils.setRouterStats(routerStats);

    CompressorFactory.createVersionSpecificCompressorIfNotExist(CompressionStrategy.ZSTD_WITH_DICT, "test-store_v1", new byte[]{}, 22);

    VeniceResponseDecompressor responseDecompressor = new VeniceResponseDecompressor(true, routerStats, request, "test-store", 1);

    CompositeByteBuf content1 = Unpooled.compositeBuffer();
    FullHttpResponse storageNodeResponse1 = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK, content1);
    storageNodeResponse1.headers().add(VENICE_COMPRESSION_STRATEGY, CompressionStrategy.ZSTD_WITH_DICT.getValue());
    storageNodeResponse1.headers().add(VENICE_SCHEMA_ID, "1");
    storageNodeResponse1.headers().add(HttpHeaderNames.CONTENT_TYPE, HttpConstants.AVRO_BINARY);

    CompositeByteBuf content2 = Unpooled.compositeBuffer();
    FullHttpResponse storageNodeResponse2 = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK, content2);
    storageNodeResponse2.headers().add(VENICE_COMPRESSION_STRATEGY, CompressionStrategy.ZSTD_WITH_DICT.getValue());
    storageNodeResponse2.headers().add(VENICE_SCHEMA_ID, "1");
    storageNodeResponse2.headers().add(HttpHeaderNames.CONTENT_TYPE, HttpConstants.AVRO_BINARY);

    FullHttpResponse routerResponse = responseDecompressor.processMultiGetResponses(Arrays.asList(new FullHttpResponse[]{storageNodeResponse1, storageNodeResponse2}));

    Assert.assertEquals(routerResponse.status(), OK);
    Assert.assertEquals(routerResponse.headers().get(VENICE_COMPRESSION_STRATEGY), String.valueOf(CompressionStrategy.NO_OP.getValue()));
  }

  /**
   * If client doesn't support decompression and the multi get request was unsuccessful, then the router should return
   * NO_OP compression strategy in the response header.
   */
  @Test
  public void TestRouterReturnsNoopCompressionStrategyHeaderIfClientDoesntSupportsDecompressionAndServerReturnsErrorForMultiGet() {
    BasicFullHttpRequest request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "storage/ZstdThreeStringFieldWithPrefix/ApqFzqwN?f=b64", System.currentTimeMillis(), 100000);
    request.headers().add(VENICE_SUPPORTED_COMPRESSION_STRATEGY, CompressionStrategy.GZIP.getValue());

    RouterStats<AggRouterHttpRequestStats> routerStats = mock(RouterStats.class);
    AggRouterHttpRequestStats stats = mock(AggRouterHttpRequestStats.class);

    doReturn(stats).when(routerStats).getStatsByType(any());

    RouterExceptionAndTrackingUtils.setRouterStats(routerStats);

    CompressorFactory.createVersionSpecificCompressorIfNotExist(CompressionStrategy.ZSTD_WITH_DICT, "test-store_v1", new byte[]{}, 22);

    VeniceResponseDecompressor responseDecompressor = new VeniceResponseDecompressor(true, routerStats, request, "test-store", 1);

    CompositeByteBuf content1 = Unpooled.compositeBuffer();
    FullHttpResponse storageNodeResponse1 = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, NOT_FOUND, content1);
    storageNodeResponse1.headers().add(VENICE_COMPRESSION_STRATEGY, CompressionStrategy.ZSTD_WITH_DICT.getValue());
    storageNodeResponse1.headers().add(VENICE_SCHEMA_ID, "1");
    storageNodeResponse1.headers().add(HttpHeaderNames.CONTENT_TYPE, HttpConstants.AVRO_BINARY);

    CompositeByteBuf content2 = Unpooled.compositeBuffer();
    FullHttpResponse storageNodeResponse2 = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, NOT_FOUND, content2);
    storageNodeResponse2.headers().add(VENICE_COMPRESSION_STRATEGY, CompressionStrategy.ZSTD_WITH_DICT.getValue());
    storageNodeResponse2.headers().add(VENICE_SCHEMA_ID, "1");
    storageNodeResponse2.headers().add(HttpHeaderNames.CONTENT_TYPE, HttpConstants.AVRO_BINARY);

    FullHttpResponse routerResponse = responseDecompressor.processMultiGetResponses(Arrays.asList(new FullHttpResponse[]{storageNodeResponse1, storageNodeResponse2}));

    Assert.assertEquals(routerResponse.status(), NOT_FOUND);
    Assert.assertEquals(routerResponse.headers().get(VENICE_COMPRESSION_STRATEGY), String.valueOf(CompressionStrategy.NO_OP.getValue()));
  }
}
