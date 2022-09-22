package com.linkedin.alpini.netty4.handlers;

import static org.mockito.Mockito.*;

import com.linkedin.alpini.base.misc.HeaderNames;
import com.linkedin.alpini.base.misc.MemoryPressureIndexMonitor;
import com.linkedin.alpini.netty4.misc.BasicHttpRequest;
import com.linkedin.alpini.netty4.misc.BasicHttpResponse;
import com.linkedin.alpini.netty4.misc.MemoryPressureIndexUtils;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.util.function.Supplier;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestMemoryPressureIndexMonitor {
  String URI = "/dontcare/CommentThread2/xxx";

  private static final Supplier<MemoryPressureIndexMonitor<HttpRequest, String, MockStats>> MONITOR_SUPPLIER =
      () -> new MemoryPressureIndexMonitor<>(
          MemoryPressureIndexUtils.defaultRequestToKeyFunction(),
          new MockStats(),
          MockStats::ignore);

  static class MockStats {
    public void ignore(Long value) {
    }
  }

  @Test(groups = "unit")
  public void givenARequestCapacityUpStreamHandlerAddedTheBytes() throws Exception {
    long bytes = 108L;
    HttpRequest request = mockHttpRequestForUpStream(bytes);
    ChannelHandlerContext context = mockContext();

    MemoryPressureIndexMonitor<HttpRequest, String, MockStats> monitor = MONITOR_SUPPLIER.get();
    MemoryPressureIndexHandler<HttpRequest, String, MockStats> handler =
        new MemoryPressureIndexHandler<>(monitor, MemoryPressureIndexUtils.defaultResponseToKeyFunction());
    EmbeddedChannel channel = new EmbeddedChannel(handler);
    channel.pipeline().fireChannelRead(request);
    Assert.assertEquals(monitor.currentMemoryPressureIndex(), bytes);
    Assert.assertEquals(monitor.getBytesByReferent((HttpRequest) request), bytes);

    HttpResponse response = mockHttpResponseForDownStream(context, bytes);
    channel.writeOneOutbound(response);
    Assert.assertEquals(monitor.currentMemoryPressureIndex(), 0);
    // The map should have this entry removed.
    Assert.assertEquals(monitor.getBytesByReferent(request), 0);
  }

  @Test(groups = "unit")
  public void givenARequestCapacityUpStreamHandlerAddedTheBytesUsingBasicHttpRequest() throws Exception {
    long bytes = 108L;
    String requestId = "blah1";
    BasicHttpRequest request = buildBasicHttpRequestForUpStream(bytes, requestId);

    MemoryPressureIndexMonitor<HttpRequest, String, MockStats> monitor = MONITOR_SUPPLIER.get();
    MemoryPressureIndexHandler<HttpRequest, String, MockStats> handler =
        new MemoryPressureIndexHandler<>(monitor, MemoryPressureIndexUtils.defaultResponseToKeyFunction());
    EmbeddedChannel channel = new EmbeddedChannel(handler);
    channel.pipeline().fireChannelRead(request);
    Assert.assertEquals(monitor.currentMemoryPressureIndex(), bytes);
    Assert.assertEquals(monitor.getBytesByReferent(request), bytes);

    HttpResponse response = buildBasicHttpResponse(bytes, request);
    channel.writeOneOutbound(response);
    Assert.assertEquals(monitor.currentMemoryPressureIndex(), 0);
    // The map should have this entry removed.
    Assert.assertEquals(monitor.getBytesByReferent(request), 0);
  }

  @Test(groups = "unit")
  public void givenARequestCapacityUpStreamHandlerAddedTheBytesWithPhantomReference() throws Exception {
    long requestBytes = 108L;
    long responseBytes = 100L;
    HttpRequest request = mockHttpRequestForUpStream(requestBytes);
    ChannelHandlerContext context = mockContext();

    MemoryPressureIndexMonitor<HttpRequest, String, MockStats> monitor = MONITOR_SUPPLIER.get();
    MemoryPressureIndexHandler<HttpRequest, String, MockStats> handler =
        new MemoryPressureIndexHandler<>(monitor, MemoryPressureIndexUtils.defaultResponseToKeyFunction())
            .phantomMode(true);
    EmbeddedChannel channel = new EmbeddedChannel(handler);
    channel.pipeline().fireChannelRead(request);
    Assert.assertEquals(monitor.currentMemoryPressureIndex(), requestBytes);
    Assert.assertEquals(monitor.getBytesByReferent(request), requestBytes);

    HttpResponse response = mockHttpResponseForDownStream(context, responseBytes);
    channel.writeOneOutbound(response);
    // verify that the bytes is not immediately removed.
    Assert.assertEquals(monitor.currentMemoryPressureIndex(), requestBytes + responseBytes);
    Assert.assertEquals(monitor.getBytesByReferent(request), requestBytes + responseBytes);
    Assert.assertTrue(monitor.isPhantomSetForReferent(request));
  }

  @Test(groups = "unit")
  public void givenARequestCapacityUpStreamHandlerAddedTheBytesWithPhantomReferenceUsingBasicHttpRequest()
      throws Exception {
    long requestBytes = 108L;
    long responseBytes = 100L;
    String requestId = "blah1";
    BasicHttpRequest request = buildBasicHttpRequestForUpStream(requestBytes, requestId);

    MemoryPressureIndexMonitor<HttpRequest, String, MockStats> monitor = MONITOR_SUPPLIER.get();
    MemoryPressureIndexHandler<HttpRequest, String, MockStats> handler =
        new MemoryPressureIndexHandler<>(monitor, MemoryPressureIndexUtils.defaultResponseToKeyFunction())
            .phantomMode(true);
    EmbeddedChannel channel = new EmbeddedChannel(handler);
    channel.pipeline().fireChannelRead(request);
    Assert.assertEquals(monitor.currentMemoryPressureIndex(), requestBytes);
    Assert.assertEquals(monitor.getBytesByReferent(request), requestBytes);

    BasicHttpResponse response = buildBasicHttpResponse(responseBytes, request);
    channel.writeOneOutbound(response);
    // verify that the bytes is not immediately removed.
    Assert.assertEquals(monitor.currentMemoryPressureIndex(), requestBytes + responseBytes);
    Assert.assertEquals(monitor.getBytesByReferent(request), requestBytes + responseBytes);
    Assert.assertTrue(monitor.isPhantomSetForReferent(request));
  }

  @Test(groups = "unit")
  public void givenARequestWithZeroLengthTheMinimumIsAdded() throws Exception {
    long requestBytes = 0L;
    String requestId = "blah1";
    BasicHttpRequest request = buildBasicHttpRequestForUpStream(requestBytes, requestId);

    MemoryPressureIndexMonitor<HttpRequest, String, MockStats> monitor = MONITOR_SUPPLIER.get();
    MemoryPressureIndexHandler<HttpRequest, String, MockStats> handler =
        new MemoryPressureIndexHandler<>(monitor, MemoryPressureIndexUtils.defaultResponseToKeyFunction())
            .phantomMode(true);
    EmbeddedChannel channel = new EmbeddedChannel(handler);
    channel.pipeline().fireChannelRead(request);

    Assert.assertEquals(monitor.currentMemoryPressureIndex(), MemoryPressureIndexUtils.getMinimumBytesToAdd());
    Assert.assertEquals(monitor.getBytesByReferent(request), MemoryPressureIndexUtils.getMinimumBytesToAdd());
  }

  private ChannelHandlerContext mockContext() {
    ChannelHandlerContext context = mock(ChannelHandlerContext.class);
    when(context.fireChannelRead(any(Object.class))).thenReturn(context);
    return context;
  }

  private HttpResponse mockHttpResponseForDownStream(ChannelHandlerContext context, long bytes) {
    HttpResponse response = mock(HttpResponse.class);
    HttpHeaders headers = mockHeaders(bytes);
    when(response.headers()).thenReturn(headers);
    when(context.write(any(Object.class))).thenReturn(mock(ChannelFuture.class));
    return response;
  }

  private HttpHeaders mockHeaders(long bytes) {
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.get(eq(HeaderNames.CONTENT_LENGTH))).thenReturn(String.valueOf(bytes));
    when(headers.get(eq(MemoryPressureIndexUtils.X_ESPRESSO_REQUEST_ID))).thenReturn("blah");
    return headers;

  }

  private BasicHttpRequest buildBasicHttpRequestForUpStream(long bytes, String requestId) {
    BasicHttpRequest request = new BasicHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, URI);
    request.headers().set(HeaderNames.CONTENT_LENGTH, bytes);
    request.headers().set(MemoryPressureIndexUtils.X_ESPRESSO_REQUEST_ID, requestId);
    return request;
  }

  private BasicHttpResponse buildBasicHttpResponse(long bytes, BasicHttpRequest request) {
    BasicHttpResponse response = new BasicHttpResponse(request, HttpResponseStatus.CREATED);
    response.headers().set(HeaderNames.CONTENT_LENGTH, bytes);
    response.headers()
        .set(
            MemoryPressureIndexUtils.X_ESPRESSO_REQUEST_ID,
            request.headers().get(MemoryPressureIndexUtils.X_ESPRESSO_REQUEST_ID));
    return response;
  }

  private HttpRequest mockHttpRequestForUpStream(long bytes) {
    HttpRequest request = mock(HttpRequest.class);
    HttpHeaders headers = mockHeaders(bytes);
    when(request.headers()).thenReturn(headers);
    return request;
  }
}
