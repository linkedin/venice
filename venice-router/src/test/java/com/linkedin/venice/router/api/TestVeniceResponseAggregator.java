package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.base.misc.Metrics;
import com.linkedin.ddsstorage.base.misc.TimeValue;
import com.linkedin.ddsstorage.netty4.misc.BasicFullHttpRequest;
import com.linkedin.ddsstorage.router.api.MetricNames;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.TestUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import java.util.*;

import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static org.mockito.Mockito.*;


public class TestVeniceResponseAggregator {
  private static Schema STRING_SCHEMA = Schema.parse("\"string\"");

  private VenicePath getPath(String storeName, RequestType requestType) {
    VenicePath path = mock(VenicePath.class);
    doReturn(requestType).when(path).getRequestType();
    doReturn(storeName).when(path).getStoreName();
    return path;
  }

  @Test
  public void testBuildResponseForSingleGet() {
    String storeName = TestUtils.getUniqueString("test_store");
    byte[] fakeContent = "abc".getBytes();
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, OK,
        Unpooled.wrappedBuffer(fakeContent));
    List<FullHttpResponse> gatheredResponses = new ArrayList<>();
    gatheredResponses.add(response);
    Metrics metrics = new Metrics();
    metrics.setPath(getPath(storeName, RequestType.SINGLE_GET));

    BasicFullHttpRequest request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
        "/storage/test_store/abc", -1, -1);

    AggRouterHttpRequestStats mockStatsForSingleGet = mock(AggRouterHttpRequestStats.class);
    AggRouterHttpRequestStats mockStatsForMultiGet = mock(AggRouterHttpRequestStats.class);
    AggRouterHttpRequestStats mockStatsForCompute = mock(AggRouterHttpRequestStats.class);

    VeniceResponseAggregator responseAggregator =
        new VeniceResponseAggregator(mockStatsForSingleGet, mockStatsForMultiGet, mockStatsForCompute);
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
    RecordDeserializer<CharSequence> deserializer = SerializerDeserializerFactory.getAvroGenericDeserializer(
        STRING_SCHEMA);

    return deserializer.deserializeObjects(content);
  }

  private FullHttpResponse buildFullHttpResponse(HttpResponseStatus responseStatus, byte[] content, Map<String, String> headers) {
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, responseStatus, Unpooled.wrappedBuffer(content));
    headers.forEach((k, v) -> response.headers().add(k, v));

    return response;
  }

  @Test
  public void testBuildResponseForMultiGet() {
    String storeName = TestUtils.getUniqueString("test_store");
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
    Metrics metrics = new Metrics();
    metrics.setPath(getPath(storeName, RequestType.MULTI_GET));

    BasicFullHttpRequest request = new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
        "/storage/test_store", -1, -1);

    AggRouterHttpRequestStats mockStatsForSingleGet = mock(AggRouterHttpRequestStats.class);
    AggRouterHttpRequestStats mockStatsForMultiGet = mock(AggRouterHttpRequestStats.class);
    AggRouterHttpRequestStats mockStatsForCompute = mock(AggRouterHttpRequestStats.class);

    VeniceResponseAggregator responseAggregator =
        new VeniceResponseAggregator(mockStatsForSingleGet, mockStatsForMultiGet, mockStatsForCompute);
    FullHttpResponse finalResponse = responseAggregator.buildResponse(request, metrics, gatheredResponses);
    Assert.assertEquals(finalResponse.status(), OK);
    byte[] finalContent;
    if (finalResponse.content() instanceof CompositeByteBuf) {
      CompositeByteBuf compositeByteBuf = (CompositeByteBuf)finalResponse.content();
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
    int cnt = 0;
    Iterator<CharSequence> valueIterator = values.iterator();
    while (valueIterator.hasNext()) {
      ++cnt;
      Assert.assertTrue(expectedValues.contains(valueIterator.next().toString()));
    }
    Assert.assertEquals(cnt, 3, "There should be 3 records in the final response");
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

    //test aggregator is able to identify quota exceeded response and
    //record it properly
    FullHttpResponse response5 = buildFullHttpResponse(TOO_MANY_REQUESTS, new byte[0], headers);
    metrics.setMetric(MetricNames.ROUTER_SERVER_TIME.name(), new TimeValue(1, TimeUnit.MILLISECONDS));
    responseAggregator.buildResponse(request, metrics, Arrays.asList(response5));
    verify(mockStatsForMultiGet).recordThrottledRequest(storeName);
  }
}
