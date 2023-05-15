package com.linkedin.venice.router.api;

import static com.linkedin.venice.HttpConstants.VENICE_COMPRESSION_STRATEGY;
import static com.linkedin.venice.HttpConstants.VENICE_SCHEMA_ID;
import static com.linkedin.venice.HttpConstants.VENICE_SUPPORTED_COMPRESSION_STRATEGY;
import static io.netty.handler.codec.http.HttpResponseStatus.MOVED_PERMANENTLY;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.alpini.base.misc.HeaderNames;
import com.linkedin.alpini.base.misc.Metrics;
import com.linkedin.alpini.base.misc.TimeValue;
import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.alpini.router.api.MetricNames;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceResponseAggregator {
  private static final Schema STRING_SCHEMA = Schema.parse("\"string\"");

  private VenicePath getPath(
      String storeName,
      RequestType requestType,
      RouterStats routerStats,
      BasicFullHttpRequest request,
      CompressorFactory compressorFactory) {
    VenicePath path = mock(VenicePath.class);
    doReturn(requestType).when(path).getRequestType();
    doReturn(storeName).when(path).getStoreName();
    doReturn(null).when(path).getChunkedResponse();
    doReturn(new VeniceResponseDecompressor(false, routerStats, request, storeName, 1, compressorFactory)).when(path)
        .getResponseDecompressor();
    return path;
  }

  @Test
  public void testBuildResponseForSingleGet() {
    String storeName = Utils.getUniqueString("test_store");
    byte[] fakeContent = "abc".getBytes();
    FullHttpResponse response =
        new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK, Unpooled.wrappedBuffer(fakeContent));
    List<FullHttpResponse> gatheredResponses = new ArrayList<>();
    gatheredResponses.add(response);

    BasicFullHttpRequest request =
        new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/storage/test_store/abc", -1, -1);

    AggRouterHttpRequestStats mockStatsForSingleGet = mock(AggRouterHttpRequestStats.class);
    AggRouterHttpRequestStats mockStatsForMultiGet = mock(AggRouterHttpRequestStats.class);
    AggRouterHttpRequestStats mockStatsForCompute = mock(AggRouterHttpRequestStats.class);
    RouterStats mockRouterStat = mock(RouterStats.class);
    when(mockRouterStat.getStatsByType(RequestType.SINGLE_GET)).thenReturn(mockStatsForSingleGet);
    when(mockRouterStat.getStatsByType(RequestType.MULTI_GET)).thenReturn(mockStatsForMultiGet);
    when(mockRouterStat.getStatsByType(RequestType.COMPUTE)).thenReturn(mockStatsForCompute);

    CompressorFactory compressorFactory = mock(CompressorFactory.class);

    Metrics metrics = new Metrics();
    metrics.setPath(getPath(storeName, RequestType.SINGLE_GET, mockRouterStat, request, compressorFactory));

    VeniceResponseAggregator responseAggregator = new VeniceResponseAggregator(mockRouterStat, Optional.empty());
    FullHttpResponse finalResponse = responseAggregator.buildResponse(request, metrics, gatheredResponses);
    Assert.assertEquals(finalResponse.status(), OK);
    Assert.assertEquals(finalResponse.content().array(), fakeContent);
    verify(mockStatsForSingleGet).recordFanoutRequestCount(storeName, 1);
    verify(mockStatsForSingleGet).recordResponseSize(storeName, fakeContent.length);
  }

  private byte[] getResponseContentWithSchemaString(String value) {
    RecordSerializer<Object> serializer = SerializerDeserializerFactory.getAvroGenericSerializer(STRING_SCHEMA);
    return serializer.serialize(value);
  }

  private Iterable<CharSequence> deserializeResponse(byte[] content) {
    RecordDeserializer<CharSequence> deserializer =
        SerializerDeserializerFactory.getAvroGenericDeserializer(STRING_SCHEMA);

    return deserializer.deserializeObjects(content);
  }

  private FullHttpResponse buildFullHttpResponse(
      HttpResponseStatus responseStatus,
      byte[] content,
      Map<String, String> headers) {
    FullHttpResponse response =
        new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, responseStatus, Unpooled.wrappedBuffer(content));
    headers.forEach((k, v) -> response.headers().add(k, v));

    return response;
  }

  @Test
  public void testBuildResponseForMultiGet() {
    String storeName = Utils.getUniqueString("test_store");
    String value1 = "value1";
    String value2 = "value2";
    String value3 = "value3";
    Map<String, String> headers = new HashMap<>();
    headers.put(HttpHeaderNames.CONTENT_TYPE.toString(), "avro/binary");
    headers.put(HttpConstants.VENICE_STORE_VERSION, "1");
    headers.put(HttpConstants.VENICE_SCHEMA_ID, "1");

    FullHttpResponse response1 = buildFullHttpResponse(OK, getResponseContentWithSchemaString(value1), headers);
    FullHttpResponse response2 = buildFullHttpResponse(OK, getResponseContentWithSchemaString(value2), headers);
    FullHttpResponse response3 = buildFullHttpResponse(OK, getResponseContentWithSchemaString(value3), headers);
    List<FullHttpResponse> gatheredResponses = new ArrayList<>();
    gatheredResponses.add(response1);
    gatheredResponses.add(response2);
    gatheredResponses.add(response3);

    BasicFullHttpRequest request =
        new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/storage/test_store", -1, -1);

    AggRouterHttpRequestStats mockStatsForSingleGet = mock(AggRouterHttpRequestStats.class);
    AggRouterHttpRequestStats mockStatsForMultiGet = mock(AggRouterHttpRequestStats.class);
    AggRouterHttpRequestStats mockStatsForCompute = mock(AggRouterHttpRequestStats.class);
    RouterStats mockRouterStat = mock(RouterStats.class);
    when(mockRouterStat.getStatsByType(RequestType.SINGLE_GET)).thenReturn(mockStatsForSingleGet);
    when(mockRouterStat.getStatsByType(RequestType.MULTI_GET)).thenReturn(mockStatsForMultiGet);
    when(mockRouterStat.getStatsByType(RequestType.COMPUTE)).thenReturn(mockStatsForCompute);

    CompressorFactory compressorFactory = mock(CompressorFactory.class);

    Metrics metrics = new Metrics();
    metrics.setPath(getPath(storeName, RequestType.MULTI_GET, mockRouterStat, request, compressorFactory));

    VeniceResponseAggregator responseAggregator = new VeniceResponseAggregator(mockRouterStat, Optional.empty());
    FullHttpResponse finalResponse = responseAggregator.buildResponse(request, metrics, gatheredResponses);
    Assert.assertEquals(finalResponse.status(), OK);
    byte[] finalContent;
    if (finalResponse.content() instanceof CompositeByteBuf) {
      CompositeByteBuf compositeByteBuf = (CompositeByteBuf) finalResponse.content();
      ByteBuf result = Unpooled.buffer(compositeByteBuf.readableBytes());
      for (int i = 0; i < compositeByteBuf.numComponents(); i++) {
        result.writeBytes(compositeByteBuf.internalComponent(i).array());
      }
      finalContent = result.array();
    } else {
      finalContent = finalResponse.content().array();
    }
    Iterable<CharSequence> values = deserializeResponse(finalContent);
    Set<String> expectedValues = new HashSet<>();
    expectedValues.add(value1);
    expectedValues.add(value2);
    expectedValues.add(value3);
    int count = 0;
    for (CharSequence value: values) {
      ++count;
      Assert.assertTrue(expectedValues.contains(value.toString()));
    }
    Assert.assertEquals(count, 3, "There should be 3 records in the final response");
    verify(mockStatsForMultiGet).recordFanoutRequestCount(storeName, 3);

    // Test with different headers among sub responses
    // build the previous 3 response again because the ByteBuf in the response.content() has been released
    response1 = buildFullHttpResponse(OK, getResponseContentWithSchemaString(value1), headers);
    response2 = buildFullHttpResponse(OK, getResponseContentWithSchemaString(value2), headers);
    response3 = buildFullHttpResponse(OK, getResponseContentWithSchemaString(value3), headers);
    FullHttpResponse response4 = buildFullHttpResponse(OK, getResponseContentWithSchemaString(value3), new HashMap<>());
    gatheredResponses.clear();
    gatheredResponses.add(response1);
    gatheredResponses.add(response2);
    gatheredResponses.add(response3);
    gatheredResponses.add(response4);

    try {
      responseAggregator.buildResponse(request, metrics, gatheredResponses);
      Assert.fail("RouterException is expected!");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof VeniceException);
    }

    // test aggregator is able to identify quota exceeded response and
    // record it properly
    FullHttpResponse response5 = buildFullHttpResponse(TOO_MANY_REQUESTS, new byte[0], headers);
    metrics.setMetric(MetricNames.ROUTER_SERVER_TIME.name(), new TimeValue(1, TimeUnit.MILLISECONDS));
    responseAggregator.buildResponse(request, metrics, Collections.singletonList(response5));
    verify(mockStatsForMultiGet).recordThrottledRequest(storeName, 1.0);
  }

  @Test
  public void testBuildResponseForMigratedStore() {
    RouterStats mockRouterStat = mock(RouterStats.class);
    VeniceResponseAggregator responseAggregator = new VeniceResponseAggregator(mockRouterStat, Optional.empty());

    BasicFullHttpRequest request =
        new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/storage/tesStore", -1, -1);
    Metrics metrics = new Metrics();
    List<FullHttpResponse> gatheredResponses = new ArrayList<>();
    Map<String, String> headers = new HashMap<>();
    headers.put(
        HeaderNames.X_ERROR_MESSAGE,
        "Store: testStore is migrated to cluster testCluster, d2Service testD2Service");
    FullHttpResponse response =
        buildFullHttpResponse(MOVED_PERMANENTLY, getResponseContentWithSchemaString("value"), headers);
    gatheredResponses.add(response);
    FullHttpResponse finalResponse = responseAggregator.buildResponse(request, metrics, gatheredResponses);
    Assert.assertEquals(finalResponse.headers().get(HttpHeaderNames.LOCATION), "d2://testD2Service/storage/tesStore");
  }

  /**
   * If client supports decompression and the multi get request was successful, then the router should return the
   * compression strategy in the response header.
   */
  @Test
  public void testRouterReturnsCompressionStrategyHeaderIfClientSupportsDecompressionForMultiGet() {
    String storeName = "GzipEnabledStore";
    String requestPath = String.format("storage/%s/ApqFzqwN?f=b64", storeName);
    BasicFullHttpRequest request =
        new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, requestPath, System.currentTimeMillis(), 100000);
    request.headers().add(VENICE_SUPPORTED_COMPRESSION_STRATEGY, CompressionStrategy.GZIP.getValue());

    RouterStats<AggRouterHttpRequestStats> routerStats = mock(RouterStats.class);
    VeniceResponseAggregator responseAggregator = new VeniceResponseAggregator(routerStats, Optional.empty());

    CompositeByteBuf content = Unpooled.compositeBuffer();
    FullHttpResponse storageNodeResponse1 = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK, content);
    storageNodeResponse1.headers().add(VENICE_COMPRESSION_STRATEGY, CompressionStrategy.GZIP.getValue());
    storageNodeResponse1.headers().add(VENICE_SCHEMA_ID, "1");
    storageNodeResponse1.headers().add(HttpHeaderNames.CONTENT_TYPE, HttpConstants.AVRO_BINARY);

    FullHttpResponse storageNodeResponse2 = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK, content);
    storageNodeResponse2.headers().add(VENICE_COMPRESSION_STRATEGY, CompressionStrategy.GZIP.getValue());
    storageNodeResponse2.headers().add(VENICE_SCHEMA_ID, "1");
    storageNodeResponse2.headers().add(HttpHeaderNames.CONTENT_TYPE, HttpConstants.AVRO_BINARY);

    FullHttpResponse routerResponse = responseAggregator
        .processMultiGetResponses(Arrays.asList(storageNodeResponse1, storageNodeResponse2), storeName, 1);

    Assert.assertEquals(routerResponse.status(), OK);
    Assert.assertEquals(
        routerResponse.headers().get(VENICE_COMPRESSION_STRATEGY),
        String.valueOf(CompressionStrategy.GZIP.getValue()));
  }

  /**
   * If client supports decompression and the multi get request was unsuccessful, then the router should return the
   * compression strategy in the response header.
   */
  @Test
  public void testRouterReturnsNoOpCompressionHeaderIfServerReturnsErrorForMultiGet() {
    String storeName = "GzipEnabledStore";
    String requestPath = String.format("storage/%s/ApqFzqwN?f=b64", storeName);
    BasicFullHttpRequest request =
        new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, requestPath, System.currentTimeMillis(), 100000);
    request.headers().add(VENICE_SUPPORTED_COMPRESSION_STRATEGY, CompressionStrategy.GZIP.getValue());

    RouterStats<AggRouterHttpRequestStats> routerStats = mock(RouterStats.class);
    VeniceResponseAggregator responseAggregator = new VeniceResponseAggregator(routerStats, Optional.empty());

    CompositeByteBuf content = Unpooled.compositeBuffer();
    FullHttpResponse storageNodeResponse1 = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, NOT_FOUND, content);
    storageNodeResponse1.headers().add(VENICE_COMPRESSION_STRATEGY, CompressionStrategy.GZIP.getValue());
    storageNodeResponse1.headers().add(VENICE_SCHEMA_ID, "1");
    storageNodeResponse1.headers().add(HttpHeaderNames.CONTENT_TYPE, HttpConstants.AVRO_BINARY);

    FullHttpResponse storageNodeResponse2 = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, NOT_FOUND, content);
    storageNodeResponse2.headers().add(VENICE_COMPRESSION_STRATEGY, CompressionStrategy.GZIP.getValue());
    storageNodeResponse2.headers().add(VENICE_SCHEMA_ID, "1");
    storageNodeResponse2.headers().add(HttpHeaderNames.CONTENT_TYPE, HttpConstants.AVRO_BINARY);

    FullHttpResponse routerResponse = responseAggregator
        .processMultiGetResponses(Arrays.asList(storageNodeResponse1, storageNodeResponse2), storeName, 1);

    Assert.assertEquals(routerResponse.status(), NOT_FOUND);
    Assert.assertEquals(
        routerResponse.headers().get(VENICE_COMPRESSION_STRATEGY),
        String.valueOf(CompressionStrategy.NO_OP.getValue()));
  }

  /**
   * If client doesn't support decompression and the multi get request was unsuccessful, then the router should return
   * NO_OP compression strategy in the response header.
   */
  @Test
  public void testRouterReturnsNoopCompressionStrategyHeaderIfClientDoesntSupportsDecompressionAndServerReturnsErrorForMultiGet() {
    String storeName = "ZstdWithDictEnabledStore";
    String requestPath = String.format("storage/%s/ApqFzqwN?f=b64", storeName);
    BasicFullHttpRequest request =
        new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, requestPath, System.currentTimeMillis(), 100000);
    request.headers().add(VENICE_SUPPORTED_COMPRESSION_STRATEGY, CompressionStrategy.GZIP.getValue());

    RouterStats<AggRouterHttpRequestStats> routerStats = mock(RouterStats.class);
    VeniceResponseAggregator responseAggregator = new VeniceResponseAggregator(routerStats, Optional.empty());

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

    FullHttpResponse routerResponse = responseAggregator
        .processMultiGetResponses(Arrays.asList(storageNodeResponse1, storageNodeResponse2), storeName, 1);

    Assert.assertEquals(routerResponse.status(), NOT_FOUND);
    Assert.assertEquals(
        routerResponse.headers().get(VENICE_COMPRESSION_STRATEGY),
        String.valueOf(CompressionStrategy.NO_OP.getValue()));
  }
}
