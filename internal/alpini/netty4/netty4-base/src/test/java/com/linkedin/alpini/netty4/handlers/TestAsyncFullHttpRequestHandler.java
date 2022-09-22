package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.AsyncPromise;
import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.AsciiString;
import io.netty.util.AttributeKey;
import io.netty.util.AttributeMap;
import io.netty.util.ReferenceCountUtil;
import java.nio.charset.StandardCharsets;
import java.util.function.BooleanSupplier;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.IRetryAnalyzer;
import org.testng.ITestResult;
import org.testng.Reporter;
import org.testng.annotations.Test;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
@Test(groups = { "unit", "unit-leak", "leak" }, singleThreaded = true)
public final class TestAsyncFullHttpRequestHandler extends AbstractLeakDetect {
  @Test
  public void testBasicRequest() throws Exception {

    BooleanSupplier shutdownFlag = Mockito.mock(BooleanSupplier.class);
    AsyncPromise<FullHttpResponse> responsePromise = AsyncFuture.deferred(false);
    AsyncFullHttpRequestHandler.RequestHandler requestHandler =
        Mockito.mock(AsyncFullHttpRequestHandler.RequestHandler.class);
    AsyncFullHttpRequestHandler handler = new AsyncFullHttpRequestHandler(requestHandler, shutdownFlag);
    EmbeddedChannel channel = new EmbeddedChannel(handler);
    channel.config().setAllocator(POOLED_ALLOCATOR);

    channel.pipeline().fireChannelActive();
    channel.flushInbound();
    channel.flushOutbound();

    HttpHeaders headers = new DefaultHttpHeaders();
    FullHttpRequest request = Mockito.mock(FullHttpRequest.class);

    Mockito.when(request.protocolVersion()).thenReturn(HttpVersion.HTTP_1_1);
    Mockito.when(request.method()).thenReturn(HttpMethod.GET);
    Mockito.when(request.uri()).thenReturn("/hello/world");
    Mockito.when(request.headers()).thenReturn(headers);
    Mockito.when(request.trailingHeaders()).thenReturn(LastHttpContent.EMPTY_LAST_CONTENT.trailingHeaders());
    Mockito.when(request.touch()).thenReturn(request);
    Mockito.when(request.touch(Mockito.any())).thenReturn(request);

    Mockito.when(requestHandler.handler(Mockito.any(), Mockito.any())).thenReturn(responsePromise);

    channel.writeInbound(request);

    Mockito.verify(request, Mockito.times(1)).headers();
    Mockito.verify(request, Mockito.times(1)).protocolVersion();
    Mockito.verify(request, Mockito.times(1)).method();
    Mockito.verify(request, Mockito.times(1)).uri();
    Mockito.verify(request, Mockito.times(2)).touch(Mockito.any());
    Mockito.verify(request, Mockito.times(1)).release();
    Mockito.verify(shutdownFlag).getAsBoolean();
    Mockito.verify(requestHandler).handler(Mockito.any(), Mockito.eq(request));

    Mockito.verifyNoMoreInteractions(request, shutdownFlag, requestHandler);

    Assert.assertNull(channel.readOutbound());

    ByteBuf responseContent = encodeString("Hello world", StandardCharsets.UTF_8);
    headers.set(HttpHeaderNames.CONTENT_LENGTH, AsciiString.of(String.valueOf(responseContent.readableBytes())));
    FullHttpResponse response = Mockito.mock(FullHttpResponse.class);
    Mockito.when(response.protocolVersion()).thenReturn(HttpVersion.HTTP_1_1);
    Mockito.when(response.headers()).thenReturn(headers);
    Mockito.when(response.trailingHeaders()).thenReturn(LastHttpContent.EMPTY_LAST_CONTENT.trailingHeaders());
    Mockito.when(response.content()).thenReturn(responseContent);
    Mockito.when(response.release()).thenAnswer(invocation -> responseContent.release());
    Mockito.when(response.retain()).thenAnswer(invocation -> {
      responseContent.retain();
      return invocation.getMock();
    });
    Mockito.when(response.touch()).thenAnswer(invocation -> {
      responseContent.touch();
      return invocation.getMock();
    });
    Mockito.when(response.touch(Mockito.any())).thenReturn(response);

    responsePromise.setSuccess(response);
    channel.flushOutbound();

    Mockito.verify(response).protocolVersion();
    Mockito.verify(response).headers();
    Mockito.verify(response).touch(Mockito.any());
    Mockito.verify(response, Mockito.times(2)).content();
    Mockito.verify(response).retain();
    Mockito.verify(response).release();

    Mockito.verify(shutdownFlag, Mockito.times(2)).getAsBoolean();

    Mockito.verifyNoMoreInteractions(request, response, shutdownFlag, requestHandler);

    Assert.assertSame(channel.readOutbound(), response);
    response.release();
  }

  @Test
  public void testBasicRequestOnShutdown() throws Exception {

    BooleanSupplier shutdownFlag = Mockito.mock(BooleanSupplier.class);
    AsyncPromise<FullHttpResponse> responsePromise = AsyncFuture.deferred(false);
    AsyncFullHttpRequestHandler.RequestHandler requestHandler =
        Mockito.mock(AsyncFullHttpRequestHandler.RequestHandler.class);
    AsyncFullHttpRequestHandler handler = new AsyncFullHttpRequestHandler(requestHandler, shutdownFlag);
    EmbeddedChannel channel = new EmbeddedChannel(handler);
    channel.config().setAllocator(POOLED_ALLOCATOR);

    channel.pipeline().fireChannelActive();
    channel.flushInbound();
    channel.flushOutbound();

    BasicFullHttpRequest request =
        new BasicFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/", Time.currentTimeMillis(), Time.nanoTime());

    AttributeKey<Integer> testKey = AttributeKey.newInstance("testAsyncFullHttpRequest");
    request.attr(testKey).set(42);

    Mockito.when(shutdownFlag.getAsBoolean()).thenReturn(true);

    Mockito.when(requestHandler.handler(Mockito.any(), Mockito.any())).thenReturn(responsePromise);

    channel.writeInbound(request);

    Mockito.verify(shutdownFlag).getAsBoolean();

    Mockito.verifyNoMoreInteractions(shutdownFlag, requestHandler);

    HttpResponse response = channel.readOutbound();
    Assert.assertNotNull(response);
    Assert.assertSame(response.status(), AsyncFullHttpRequestHandler.SERVICE_SHUTDOWN);
    Assert.assertTrue(((AttributeMap) response).hasAttr(testKey));

    Assert.assertNull(channel.readOutbound());
    Assert.assertNull(channel.readInbound());

    ReferenceCountUtil.release(response);
    channel.finishAndReleaseAll();
  }

  @Test
  public void testBasicRequestAutoRead() throws Exception {

    BooleanSupplier shutdownFlag = Mockito.mock(BooleanSupplier.class);
    AsyncPromise<FullHttpResponse> responsePromise = AsyncFuture.deferred(false);
    AsyncFullHttpRequestHandler.RequestHandler requestHandler =
        Mockito.mock(AsyncFullHttpRequestHandler.RequestHandler.class);
    AsyncFullHttpRequestHandler handler =
        new AsyncFullHttpRequestHandler(requestHandler, shutdownFlag, Boolean.TRUE::booleanValue);
    EmbeddedChannel channel = new EmbeddedChannel(handler);
    channel.config().setAllocator(POOLED_ALLOCATOR);

    channel.pipeline().fireChannelActive();
    channel.flushInbound();
    channel.flushOutbound();

    HttpHeaders headers = new DefaultHttpHeaders();
    FullHttpRequest request = Mockito.mock(FullHttpRequest.class);

    Mockito.when(request.protocolVersion()).thenReturn(HttpVersion.HTTP_1_1);
    Mockito.when(request.method()).thenReturn(HttpMethod.GET);
    Mockito.when(request.uri()).thenReturn("/hello/world");
    Mockito.when(request.headers()).thenReturn(headers);
    Mockito.when(request.trailingHeaders()).thenReturn(LastHttpContent.EMPTY_LAST_CONTENT.trailingHeaders());
    Mockito.when(request.touch()).thenReturn(request);
    Mockito.when(request.touch(Mockito.any())).thenReturn(request);

    Mockito.when(requestHandler.handler(Mockito.any(), Mockito.any())).thenReturn(responsePromise);

    channel.writeInbound(request);

    Mockito.verify(request, Mockito.times(1)).headers();
    Mockito.verify(request, Mockito.times(1)).protocolVersion();
    Mockito.verify(request, Mockito.times(1)).method();
    Mockito.verify(request, Mockito.times(1)).uri();
    Mockito.verify(request, Mockito.times(2)).touch(Mockito.any());
    Mockito.verify(request, Mockito.times(1)).release();
    Mockito.verify(shutdownFlag).getAsBoolean();
    Mockito.verify(requestHandler).handler(Mockito.any(), Mockito.eq(request));

    Mockito.verifyNoMoreInteractions(request, shutdownFlag, requestHandler);

    Assert.assertFalse(channel.config().isAutoRead());
    Assert.assertNull(channel.readOutbound());

    ByteBuf responseContent = encodeString("Hello world", StandardCharsets.UTF_8);
    headers.set(HttpHeaderNames.CONTENT_LENGTH, AsciiString.of(String.valueOf(responseContent.readableBytes())));
    FullHttpResponse response = Mockito.mock(FullHttpResponse.class);
    Mockito.when(response.protocolVersion()).thenReturn(HttpVersion.HTTP_1_1);
    Mockito.when(response.headers()).thenReturn(headers);
    Mockito.when(response.trailingHeaders()).thenReturn(LastHttpContent.EMPTY_LAST_CONTENT.trailingHeaders());
    Mockito.when(response.content()).thenReturn(responseContent);
    Mockito.when(response.release()).thenAnswer(invocation -> responseContent.release());
    Mockito.when(response.retain()).thenAnswer(invocation -> {
      responseContent.retain();
      return invocation.getMock();
    });
    Mockito.when(response.touch()).thenAnswer(invocation -> {
      responseContent.touch();
      return invocation.getMock();
    });
    Mockito.when(response.touch(Mockito.any())).thenReturn(response);

    responsePromise.setSuccess(response);
    channel.flushOutbound();

    Mockito.verify(response).protocolVersion();
    Mockito.verify(response).headers();
    Mockito.verify(response).touch(Mockito.any());
    Mockito.verify(response, Mockito.times(2)).content();
    Mockito.verify(response).retain();
    Mockito.verify(response).release();

    Mockito.verify(shutdownFlag, Mockito.times(2)).getAsBoolean();

    Mockito.verifyNoMoreInteractions(request, response, shutdownFlag, requestHandler);

    Assert.assertTrue(channel.config().isAutoRead());
    Assert.assertSame(channel.readOutbound(), response);
    response.release();
  }

  @Test
  public void testBasicRequestException() throws Exception {

    BooleanSupplier shutdownFlag = Mockito.mock(BooleanSupplier.class);
    AsyncFullHttpRequestHandler.RequestHandler requestHandler =
        Mockito.mock(AsyncFullHttpRequestHandler.RequestHandler.class);
    AsyncFullHttpRequestHandler handler = new AsyncFullHttpRequestHandler(requestHandler, shutdownFlag);
    EmbeddedChannel channel = new EmbeddedChannel(handler);
    Throwable exception = new Error("testBasicRequestException");

    channel.pipeline().fireChannelActive();
    channel.flushInbound();
    channel.flushOutbound();

    HttpHeaders headers = new DefaultHttpHeaders();
    FullHttpRequest request = Mockito.mock(FullHttpRequest.class);

    Mockito.when(request.protocolVersion()).thenReturn(HttpVersion.HTTP_1_1);
    Mockito.when(request.method()).thenReturn(HttpMethod.GET);
    Mockito.when(request.uri()).thenReturn("/hello/world");
    Mockito.when(request.headers()).thenReturn(headers);
    Mockito.when(request.trailingHeaders()).thenReturn(LastHttpContent.EMPTY_LAST_CONTENT.trailingHeaders());
    Mockito.when(request.touch()).thenReturn(request);
    Mockito.when(request.touch(Mockito.any())).thenReturn(request);

    Mockito.when(requestHandler.handler(Mockito.any(), Mockito.any())).thenThrow(exception);

    channel.writeInbound(request);

    Mockito.verify(request, Mockito.times(1)).headers();
    Mockito.verify(request, Mockito.times(1)).protocolVersion();
    Mockito.verify(request, Mockito.times(1)).method();
    Mockito.verify(request, Mockito.times(1)).uri();
    Mockito.verify(request, Mockito.times(2)).touch(Mockito.any());
    Mockito.verify(request, Mockito.times(1)).release();
    Mockito.verify(shutdownFlag, Mockito.times(2)).getAsBoolean();
    Mockito.verify(requestHandler).handler(Mockito.any(), Mockito.eq(request));

    Mockito.verifyNoMoreInteractions(request, shutdownFlag, requestHandler);

    FullHttpResponse response = channel.readOutbound();
    Assert.assertNotNull(response);

    Assert.assertEquals(
        response.toString()
            .replaceAll("(\\$[^(]+)?\\(ridx: \\d+, widx: \\d+, cap: \\d+\\)", "")
            .replaceAll("content-length: \\d+\n", "content-length: XXXX\n"),
        "BasicFullHttpResponse(decodeResult: success, version: HTTP/1.1, " + "content: UnpooledByteBufAllocator)\n"
            + "HTTP/1.1 500 Internal Server Error\n" + "content-length: XXXX\n" + "content-type: text/html");

    Assert.assertEquals(
        response.content().toString(StandardCharsets.US_ASCII).split("\n")[0],
        "<html><head><title>INTERNAL SERVER ERROR</title></head><body><h1>Internal Server Error</h1>"
            + "<pre>java.lang.Error: testBasicRequestException");
  }

  @Test
  public void testBasicRequestLateException() throws Exception {

    BooleanSupplier shutdownFlag = Mockito.mock(BooleanSupplier.class);
    AsyncPromise<FullHttpResponse> responsePromise = AsyncFuture.deferred(false);
    AsyncFullHttpRequestHandler.RequestHandler requestHandler =
        Mockito.mock(AsyncFullHttpRequestHandler.RequestHandler.class);
    AsyncFullHttpRequestHandler handler = new AsyncFullHttpRequestHandler(requestHandler, shutdownFlag);
    EmbeddedChannel channel = new EmbeddedChannel(handler);
    Throwable exception = new Error("testBasicRequestLateException");

    channel.pipeline().fireChannelActive();
    channel.flushInbound();
    channel.flushOutbound();

    HttpHeaders headers = new DefaultHttpHeaders();
    FullHttpRequest request = Mockito.mock(FullHttpRequest.class);

    Mockito.when(request.protocolVersion()).thenReturn(HttpVersion.HTTP_1_1);
    Mockito.when(request.method()).thenReturn(HttpMethod.GET);
    Mockito.when(request.uri()).thenReturn("/hello/world");
    Mockito.when(request.headers()).thenReturn(headers);
    Mockito.when(request.trailingHeaders()).thenReturn(LastHttpContent.EMPTY_LAST_CONTENT.trailingHeaders());
    Mockito.when(request.touch()).thenReturn(request);
    Mockito.when(request.touch(Mockito.any())).thenReturn(request);

    Mockito.when(requestHandler.handler(Mockito.any(), Mockito.any())).thenReturn(responsePromise);

    channel.writeInbound(request);

    Mockito.verify(request, Mockito.times(1)).headers();
    Mockito.verify(request, Mockito.times(1)).protocolVersion();
    Mockito.verify(request, Mockito.times(1)).method();
    Mockito.verify(request, Mockito.times(1)).uri();
    Mockito.verify(request, Mockito.times(2)).touch(Mockito.any());
    Mockito.verify(request, Mockito.times(1)).release();
    Mockito.verify(shutdownFlag, Mockito.times(1)).getAsBoolean();
    Mockito.verify(requestHandler).handler(Mockito.any(), Mockito.eq(request));

    Mockito.verifyNoMoreInteractions(request, shutdownFlag, requestHandler);
    Assert.assertNull(channel.readOutbound());

    responsePromise.setFailure(exception);
    channel.flushOutbound();

    Mockito.verify(shutdownFlag, Mockito.times(2)).getAsBoolean();

    Mockito.verifyNoMoreInteractions(request, shutdownFlag, requestHandler);

    FullHttpResponse response = channel.readOutbound();
    Assert.assertNotNull(response);

    Assert.assertEquals(
        response.toString()
            .replaceAll("(\\$[^(]+)?\\(ridx: \\d+, widx: \\d+, cap: \\d+\\)", "")
            .replaceAll("content-length: \\d+\n", "content-length: XXXX\n"),
        "BasicFullHttpResponse(decodeResult: success, version: HTTP/1.1, " + "content: UnpooledByteBufAllocator)\n"
            + "HTTP/1.1 500 Internal Server Error\n" + "content-length: XXXX\n" + "content-type: text/html");

    Assert.assertEquals(
        response.content().toString(StandardCharsets.US_ASCII).split("\n")[0],
        "<html><head><title>INTERNAL SERVER ERROR</title></head><body><h1>Internal Server Error</h1>"
            + "<pre>java.lang.Error: testBasicRequestLateException");
  }

  /**
   * The garbage collector is sometimes lazy and so we may need to retry this test a couple of times.
   */
  public static class RetryBasicRequestReferenceLost implements IRetryAnalyzer {
    private int remaining = 3;

    @Override
    public boolean retry(ITestResult result) {
      if (!result.isSuccess() && remaining-- > 0) {
        try {
          result.setStatus(ITestResult.SUCCESS_PERCENTAGE_FAILURE);
          Reporter.log("Retries remaining: " + remaining, true);
          Thread.sleep(5000);
          return true;
        } catch (InterruptedException e) {
          // Ignore
        }
      }
      return false;
    }
  }

  @Test(retryAnalyzer = RetryBasicRequestReferenceLost.class, successPercentage = 10)
  public void testBasicRequestReferenceLost() throws Exception {

    BooleanSupplier shutdownFlag = Mockito.mock(BooleanSupplier.class);
    AsyncFullHttpRequestHandler.RequestHandler requestHandler =
        Mockito.mock(AsyncFullHttpRequestHandler.RequestHandler.class);
    AsyncFullHttpRequestHandler handler = new AsyncFullHttpRequestHandler(requestHandler, shutdownFlag);
    EmbeddedChannel channel = new EmbeddedChannel(handler);
    channel.config().setAllocator(POOLED_ALLOCATOR);

    channel.pipeline().fireChannelActive();
    channel.flushInbound();
    channel.flushOutbound();

    HttpHeaders headers = new DefaultHttpHeaders();
    FullHttpRequest request = Mockito.mock(FullHttpRequest.class);

    Mockito.when(request.protocolVersion()).thenReturn(HttpVersion.HTTP_1_1);
    Mockito.when(request.method()).thenReturn(HttpMethod.GET);
    Mockito.when(request.uri()).thenReturn("/hello/world");
    Mockito.when(request.headers()).thenReturn(headers);
    Mockito.when(request.trailingHeaders()).thenReturn(LastHttpContent.EMPTY_LAST_CONTENT.trailingHeaders());
    Mockito.when(request.touch()).thenReturn(request);
    Mockito.when(request.touch(Mockito.any())).thenReturn(request);

    Mockito.when(requestHandler.handler(Mockito.any(), Mockito.any())).thenReturn(AsyncFuture.deferred(false));

    channel.writeInbound(request);

    Mockito.verify(request, Mockito.times(1)).headers();
    Mockito.verify(request, Mockito.times(1)).protocolVersion();
    Mockito.verify(request, Mockito.times(1)).method();
    Mockito.verify(request, Mockito.times(1)).uri();
    Mockito.verify(request, Mockito.times(2)).touch(Mockito.any());
    Mockito.verify(request, Mockito.times(1)).release();
    Mockito.verify(shutdownFlag, Mockito.times(1)).getAsBoolean();
    Mockito.verify(requestHandler).handler(Mockito.any(), Mockito.eq(request));

    Mockito.verifyNoMoreInteractions(request, shutdownFlag, requestHandler);
    Assert.assertNull(channel.readOutbound());

    Mockito.reset(requestHandler); // must also make Mockito forget about the promise
    for (int i = 10; i > 0; i--) {
      System.gc();
      Thread.sleep(200);
      channel.runPendingTasks();
    }

    Mockito.verify(shutdownFlag, Mockito.times(2)).getAsBoolean();

    Mockito.verifyNoMoreInteractions(request, shutdownFlag, requestHandler);

    FullHttpResponse response = channel.readOutbound();
    Assert.assertNotNull(response);

    Assert.assertEquals(
        response.toString()
            .replaceAll("(\\$[^(]+)?\\(ridx: \\d+, widx: \\d+, cap: \\d+\\)", "")
            .replaceAll("content-length: \\d+\n", "content-length: XXXX\n"),
        "BasicFullHttpResponse(decodeResult: success, version: HTTP/1.1, " + "content: UnpooledByteBufAllocator)\n"
            + "HTTP/1.1 500 Internal Server Error\n" + "content-length: XXXX\n" + "content-type: text/html");

    Assert.assertEquals(
        response.content().toString(StandardCharsets.US_ASCII).split("\n")[0],
        "<html><head><title>INTERNAL SERVER ERROR</title></head><body><h1>Internal Server Error</h1>"
            + "<pre>java.lang.IllegalStateException: Reference lost");
  }

  @Test
  public void testBasicRequestSequential() throws Exception {
    // TODO test that sequential requests are handled okay.

    BooleanSupplier shutdownFlag = Mockito.mock(BooleanSupplier.class);
    AsyncPromise<FullHttpResponse> responsePromise1 = AsyncFuture.deferred(false);
    AsyncPromise<FullHttpResponse> responsePromise2 = AsyncFuture.deferred(false);
    AsyncFullHttpRequestHandler.RequestHandler requestHandler =
        Mockito.mock(AsyncFullHttpRequestHandler.RequestHandler.class);
    AsyncFullHttpRequestHandler handler = new AsyncFullHttpRequestHandler(requestHandler, shutdownFlag);
    EmbeddedChannel channel = new EmbeddedChannel(handler);
    channel.config().setAllocator(POOLED_ALLOCATOR);

    channel.pipeline().fireChannelActive();
    channel.flushInbound();
    channel.flushOutbound();

    HttpHeaders headers = new DefaultHttpHeaders();
    FullHttpRequest request1 = Mockito.mock(FullHttpRequest.class, "request1");
    FullHttpRequest request2 = Mockito.mock(FullHttpRequest.class, "request2");

    Mockito.when(request1.protocolVersion()).thenReturn(HttpVersion.HTTP_1_1);
    Mockito.when(request1.method()).thenReturn(HttpMethod.GET);
    Mockito.when(request1.uri()).thenReturn("/hello/world");
    Mockito.when(request1.headers()).thenReturn(headers);
    Mockito.when(request1.trailingHeaders()).thenReturn(LastHttpContent.EMPTY_LAST_CONTENT.trailingHeaders());
    Mockito.when(request1.touch()).thenReturn(request1);
    Mockito.when(request1.touch(Mockito.any())).thenReturn(request1);

    Mockito.when(request2.protocolVersion()).thenReturn(HttpVersion.HTTP_1_1);
    Mockito.when(request2.method()).thenReturn(HttpMethod.GET);
    Mockito.when(request2.uri()).thenReturn("/hello/world2");
    Mockito.when(request2.headers()).thenReturn(headers);
    Mockito.when(request2.trailingHeaders()).thenReturn(LastHttpContent.EMPTY_LAST_CONTENT.trailingHeaders());
    Mockito.when(request2.touch()).thenReturn(request2);
    Mockito.when(request2.touch(Mockito.any())).thenReturn(request2);

    Mockito.when(requestHandler.handler(Mockito.any(), Mockito.any())).thenReturn(responsePromise1, responsePromise2);

    channel.writeInbound(request1);

    Mockito.verify(request1, Mockito.times(1)).headers();
    Mockito.verify(request1, Mockito.times(1)).protocolVersion();
    Mockito.verify(request1, Mockito.times(1)).method();
    Mockito.verify(request1, Mockito.times(1)).uri();
    Mockito.verify(request1, Mockito.times(2)).touch(Mockito.any());
    Mockito.verify(request1, Mockito.times(1)).release();

    Mockito.verify(shutdownFlag, Mockito.times(1)).getAsBoolean();
    Mockito.verify(requestHandler).handler(Mockito.any(), Mockito.eq(request1));

    Mockito.verifyNoMoreInteractions(request1, request2, shutdownFlag, requestHandler);

    Assert.assertNull(channel.readOutbound());

    FullHttpResponse response1 = Mockito.mock(FullHttpResponse.class, "response1");
    Mockito.when(response1.protocolVersion()).thenReturn(HttpVersion.HTTP_1_1);
    Mockito.when(response1.headers()).thenReturn(headers);
    Mockito.when(response1.trailingHeaders()).thenReturn(LastHttpContent.EMPTY_LAST_CONTENT.trailingHeaders());
    Mockito.when(response1.content()).thenReturn(Unpooled.EMPTY_BUFFER);
    Mockito.when(response1.release()).thenReturn(false);
    Mockito.when(response1.retain()).thenReturn(response1);
    Mockito.when(response1.touch()).thenReturn(response1);
    Mockito.when(response1.touch(Mockito.any())).thenReturn(response1);

    FullHttpResponse response2 = Mockito.mock(FullHttpResponse.class, "response2");
    Mockito.when(response2.protocolVersion()).thenReturn(HttpVersion.HTTP_1_1);
    Mockito.when(response2.headers()).thenReturn(headers);
    Mockito.when(response2.trailingHeaders()).thenReturn(LastHttpContent.EMPTY_LAST_CONTENT.trailingHeaders());
    Mockito.when(response2.content()).thenReturn(Unpooled.EMPTY_BUFFER);
    Mockito.when(response2.release()).thenReturn(false);
    Mockito.when(response2.retain()).thenReturn(response2);
    Mockito.when(response2.touch()).thenReturn(response2);
    Mockito.when(response2.touch(Mockito.any())).thenReturn(response2);

    responsePromise1.setSuccess(response1);
    channel.flushOutbound();
    Assert.assertSame(channel.readOutbound(), response1);
    Assert.assertNull(channel.readOutbound());

    Mockito.verify(response1).protocolVersion();
    Mockito.verify(response1).headers();
    Mockito.verify(response1, Mockito.times(2)).touch(Mockito.any());
    Mockito.verify(response1, Mockito.times(2)).content();
    Mockito.verify(response1).retain();
    Mockito.verify(response1).release();
    Mockito.verify(shutdownFlag, Mockito.times(2)).getAsBoolean();

    Mockito.verifyNoMoreInteractions(request1, request2, response1, response2, shutdownFlag, requestHandler);

    channel.writeInbound(request2);

    Mockito.verify(request2, Mockito.times(1)).headers();
    Mockito.verify(request2, Mockito.times(1)).protocolVersion();
    Mockito.verify(request2, Mockito.times(1)).method();
    Mockito.verify(request2, Mockito.times(1)).uri();
    Mockito.verify(request2, Mockito.times(2)).touch(Mockito.any());
    Mockito.verify(request2, Mockito.times(1)).release();

    Mockito.verify(shutdownFlag, Mockito.times(3)).getAsBoolean();
    Mockito.verify(requestHandler).handler(Mockito.any(), Mockito.eq(request2));

    Mockito.verifyNoMoreInteractions(request1, request2, shutdownFlag, requestHandler);

    Assert.assertNull(channel.readOutbound());

    responsePromise2.setSuccess(response2);
    channel.flushOutbound();
    Assert.assertSame(channel.readOutbound(), response2);
    Assert.assertNull(channel.readOutbound());

    Mockito.verify(response2).protocolVersion();
    Mockito.verify(response2).headers();
    Mockito.verify(response2, Mockito.times(2)).touch(Mockito.any());
    Mockito.verify(response2, Mockito.times(2)).content();
    Mockito.verify(response2).retain();
    Mockito.verify(response2).release();
    Mockito.verify(shutdownFlag, Mockito.times(4)).getAsBoolean();

    Mockito.verifyNoMoreInteractions(request1, request2, response1, response2, shutdownFlag, requestHandler);
  }

  @Test
  public void testPipelinedRequest() throws Exception {
    // test that pipelined requests are handled okay, even if the request handler completes out of order.

    BooleanSupplier shutdownFlag = Mockito.mock(BooleanSupplier.class);
    AsyncPromise<FullHttpResponse> responsePromise1 = AsyncFuture.deferred(false);
    AsyncPromise<FullHttpResponse> responsePromise2 = AsyncFuture.deferred(false);
    AsyncFullHttpRequestHandler.RequestHandler requestHandler =
        Mockito.mock(AsyncFullHttpRequestHandler.RequestHandler.class);
    AsyncFullHttpRequestHandler handler = new AsyncFullHttpRequestHandler(requestHandler, shutdownFlag);
    EmbeddedChannel channel = new EmbeddedChannel(handler);
    channel.config().setAllocator(POOLED_ALLOCATOR);

    channel.pipeline().fireChannelActive();
    channel.flushInbound();
    channel.flushOutbound();

    HttpHeaders headers = new DefaultHttpHeaders();
    FullHttpRequest request1 = Mockito.mock(FullHttpRequest.class, "request1");
    FullHttpRequest request2 = Mockito.mock(FullHttpRequest.class, "request2");

    Mockito.when(request1.protocolVersion()).thenReturn(HttpVersion.HTTP_1_1);
    Mockito.when(request1.method()).thenReturn(HttpMethod.GET);
    Mockito.when(request1.uri()).thenReturn("/hello/world");
    Mockito.when(request1.headers()).thenReturn(headers);
    Mockito.when(request1.trailingHeaders()).thenReturn(LastHttpContent.EMPTY_LAST_CONTENT.trailingHeaders());
    Mockito.when(request1.touch()).thenReturn(request1);
    Mockito.when(request1.touch(Mockito.any())).thenReturn(request1);

    Mockito.when(request2.protocolVersion()).thenReturn(HttpVersion.HTTP_1_1);
    Mockito.when(request2.method()).thenReturn(HttpMethod.GET);
    Mockito.when(request2.uri()).thenReturn("/hello/world2");
    Mockito.when(request2.headers()).thenReturn(headers);
    Mockito.when(request2.trailingHeaders()).thenReturn(LastHttpContent.EMPTY_LAST_CONTENT.trailingHeaders());
    Mockito.when(request2.touch()).thenReturn(request2);
    Mockito.when(request2.touch(Mockito.any())).thenReturn(request2);

    Mockito.when(requestHandler.handler(Mockito.any(), Mockito.any())).thenReturn(responsePromise1, responsePromise2);

    channel.writeInbound(request1, request2);

    Mockito.verify(request1, Mockito.times(1)).headers();
    Mockito.verify(request1, Mockito.times(1)).protocolVersion();
    Mockito.verify(request1, Mockito.times(1)).method();
    Mockito.verify(request1, Mockito.times(1)).uri();
    Mockito.verify(request1, Mockito.times(2)).touch(Mockito.any());
    Mockito.verify(request1, Mockito.times(1)).release();
    Mockito.verify(request2, Mockito.times(1)).headers();
    Mockito.verify(request2, Mockito.times(1)).protocolVersion();
    Mockito.verify(request2, Mockito.times(1)).method();
    Mockito.verify(request2, Mockito.times(1)).uri();
    Mockito.verify(request2, Mockito.times(2)).touch(Mockito.any());
    Mockito.verify(request2, Mockito.times(1)).release();
    Mockito.verify(shutdownFlag, Mockito.times(2)).getAsBoolean();
    Mockito.verify(requestHandler).handler(Mockito.any(), Mockito.eq(request1));
    Mockito.verify(requestHandler).handler(Mockito.any(), Mockito.eq(request2));

    Mockito.verifyNoMoreInteractions(request1, request2, shutdownFlag, requestHandler);

    Assert.assertNull(channel.readOutbound());

    FullHttpResponse response1 = Mockito.mock(FullHttpResponse.class, "response1");
    Mockito.when(response1.protocolVersion()).thenReturn(HttpVersion.HTTP_1_1);
    Mockito.when(response1.headers()).thenReturn(headers);
    Mockito.when(response1.trailingHeaders()).thenReturn(LastHttpContent.EMPTY_LAST_CONTENT.trailingHeaders());
    Mockito.when(response1.content()).thenReturn(Unpooled.EMPTY_BUFFER);
    Mockito.when(response1.release()).thenReturn(false);
    Mockito.when(response1.retain()).thenReturn(response1);
    Mockito.when(response1.touch()).thenReturn(response1);
    Mockito.when(response1.touch(Mockito.any())).thenReturn(response1);

    FullHttpResponse response2 = Mockito.mock(FullHttpResponse.class, "response2");
    Mockito.when(response2.protocolVersion()).thenReturn(HttpVersion.HTTP_1_1);
    Mockito.when(response2.headers()).thenReturn(headers);
    Mockito.when(response2.trailingHeaders()).thenReturn(LastHttpContent.EMPTY_LAST_CONTENT.trailingHeaders());
    Mockito.when(response2.content()).thenReturn(Unpooled.EMPTY_BUFFER);
    Mockito.when(response2.release()).thenReturn(false);
    Mockito.when(response2.retain()).thenReturn(response2);
    Mockito.when(response2.touch()).thenReturn(response2);
    Mockito.when(response2.touch(Mockito.any())).thenReturn(response2);

    responsePromise2.setSuccess(response2);
    channel.flushOutbound();
    Assert.assertNull(channel.readOutbound());

    Mockito.verifyNoMoreInteractions(request1, request2, response1, response2, shutdownFlag, requestHandler);

    responsePromise1.setSuccess(response1);
    channel.flushOutbound();

    Assert.assertSame(channel.readOutbound(), response1);
    Assert.assertSame(channel.readOutbound(), response2);

    Mockito.verify(response1).protocolVersion();
    Mockito.verify(response1).headers();
    Mockito.verify(response1, Mockito.times(2)).touch(Mockito.any());
    Mockito.verify(response1, Mockito.times(2)).content();
    Mockito.verify(response1).retain();
    Mockito.verify(response1).release();
    Mockito.verify(response2).protocolVersion();
    Mockito.verify(response2).headers();
    Mockito.verify(response2, Mockito.times(2)).touch(Mockito.any());
    Mockito.verify(response2, Mockito.times(2)).content();
    Mockito.verify(response2).retain();
    Mockito.verify(response2).release();
    Mockito.verify(shutdownFlag, Mockito.times(4)).getAsBoolean();

    Mockito.verifyNoMoreInteractions(request1, request2, response1, response2, shutdownFlag, requestHandler);
  }

  @Test(enabled = false) // TODO would be nice if handler should defer close until request handled.
  public void testRequestCloseInflight() throws Exception {

    BooleanSupplier shutdownFlag = Mockito.mock(BooleanSupplier.class);
    AsyncPromise<FullHttpResponse> responsePromise = AsyncFuture.deferred(false);
    AsyncFullHttpRequestHandler.RequestHandler requestHandler =
        Mockito.mock(AsyncFullHttpRequestHandler.RequestHandler.class);
    AsyncFullHttpRequestHandler handler = new AsyncFullHttpRequestHandler(requestHandler, shutdownFlag);
    EmbeddedChannel channel = new EmbeddedChannel(handler);

    channel.pipeline().fireChannelActive();
    channel.flushInbound();
    channel.flushOutbound();

    HttpHeaders headers = new DefaultHttpHeaders();
    FullHttpRequest request = Mockito.mock(FullHttpRequest.class);

    Mockito.when(request.protocolVersion()).thenReturn(HttpVersion.HTTP_1_1);
    Mockito.when(request.method()).thenReturn(HttpMethod.GET);
    Mockito.when(request.uri()).thenReturn("/hello/world");
    Mockito.when(request.headers()).thenReturn(headers);
    Mockito.when(request.trailingHeaders()).thenReturn(LastHttpContent.EMPTY_LAST_CONTENT.trailingHeaders());
    Mockito.when(request.touch()).thenReturn(request);
    Mockito.when(request.touch(Mockito.any())).thenReturn(request);

    Mockito.when(requestHandler.handler(Mockito.any(), Mockito.any())).thenReturn(responsePromise);

    channel.writeInbound(request);

    Mockito.verify(request, Mockito.times(1)).headers();
    Mockito.verify(request, Mockito.times(1)).protocolVersion();
    Mockito.verify(request, Mockito.times(1)).method();
    Mockito.verify(request, Mockito.times(1)).uri();
    Mockito.verify(request, Mockito.times(2)).touch(Mockito.any());
    Mockito.verify(request, Mockito.times(1)).release();
    Mockito.verify(shutdownFlag).getAsBoolean();
    Mockito.verify(requestHandler).handler(Mockito.any(), Mockito.eq(request));

    Mockito.verifyNoMoreInteractions(request, shutdownFlag, requestHandler);

    ChannelFuture closeFuture = channel.close();

    Assert.assertFalse(closeFuture.isDone());

    Assert.assertNull(channel.readOutbound());

    FullHttpResponse response = Mockito.mock(FullHttpResponse.class);
    Mockito.when(response.protocolVersion()).thenReturn(HttpVersion.HTTP_1_1);
    Mockito.when(response.headers()).thenReturn(headers);
    Mockito.when(response.trailingHeaders()).thenReturn(LastHttpContent.EMPTY_LAST_CONTENT.trailingHeaders());
    Mockito.when(response.content()).thenReturn(Unpooled.EMPTY_BUFFER);
    Mockito.when(response.release()).thenReturn(false);
    Mockito.when(response.retain()).thenReturn(response);
    Mockito.when(response.touch()).thenReturn(response);
    Mockito.when(response.touch(Mockito.any())).thenReturn(response);

    Assert.assertFalse(closeFuture.isDone());

    responsePromise.setSuccess(response);

    Assert.assertTrue(closeFuture.isDone());

    Mockito.verify(response).protocolVersion();
    Mockito.verify(response).headers();
    Mockito.verify(response).touch(Mockito.any());
    Mockito.verify(response, Mockito.times(2)).content();
    Mockito.verify(response).retain();
    Mockito.verify(response).release();

    Mockito.verify(shutdownFlag, Mockito.times(1)).getAsBoolean();

    Mockito.verifyNoMoreInteractions(request, response, shutdownFlag, requestHandler);

    Assert.assertSame(channel.readOutbound(), response);
  }

  /**
   * since TestNG tends to sort by method name, this tries to be the last test
   * in the class. We do this because the AfterClass annotated methods may
   * execute after other tests classes have run and doesn't execute immediately
   * after the methods in this test class.
   */
  @Test(alwaysRun = true)
  public final void zz9PluralZAlpha() throws InterruptedException {
    finallyLeakDetect();
  }
}
