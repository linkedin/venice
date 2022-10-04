package com.linkedin.alpini.netty4.handlers;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.linkedin.alpini.base.misc.Msg;
import com.linkedin.alpini.base.misc.Time;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import org.apache.logging.log4j.Logger;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * @author Jemiah Westerman <jwesterman@linkedin.com>
 *
 * @version $Revision$
 */
@Test(groups = { "unit", "unit-leak", "leak" }, singleThreaded = true)
public final class TestRequestLogHandler extends AbstractLeakDetect {
  private static Logger _mockLog = mock(Logger.class);

  @BeforeClass
  public void beforeMethod() {
    reset(_mockLog);
  }

  @AfterClass
  public void afterClass() {
    Mockito.reset(_mockLog);
  }

  @Test
  public void testCatchEx() {
    class TestException extends Error {
    }

    Mockito.reset(_mockLog);
    when(_mockLog.isWarnEnabled()).thenReturn(true);
    RequestLogHandler handler = new RequestLogHandler(_mockLog, "pipeline");

    handler.catchException(_mockLog, () -> {
      throw new TestException();
    }, "foo");

    ArgumentCaptor<String> nameCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Throwable> exceptionCaptor = ArgumentCaptor.forClass(Throwable.class);

    verify(_mockLog).getName();
    verify(_mockLog).warn(eq("Error in {}"), nameCaptor.capture(), exceptionCaptor.capture());

    Assert.assertEquals(nameCaptor.getValue(), "foo");
    Assert.assertTrue(exceptionCaptor.getValue() instanceof TestException);

    verifyNoMoreInteractions(_mockLog);
  }

  @Test
  public void testConnectInfo() {
    try {
      Time.freeze();
      long ts = Time.currentTimeMillis();
      SocketAddress address = InetSocketAddress.createUnresolved("address", 1234);
      RequestLogHandler.ConnectInfo info = new RequestLogHandler.ConnectInfo(address);

      Assert.assertSame(info._remoteAddress, address);
      Assert.assertEquals(info._startMillis, ts);
    } finally {
      Time.restore();
    }
  }

  @Test
  public void testResponseInfo() {
    try {
      Time.freeze();
      HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
      HttpUtil.setContentLength(response, 0);
      long ts = Time.currentTimeMillis();
      RequestLogHandler.ResponseInfo info = new RequestLogHandler.ResponseInfo(response, false);

      Assert.assertEquals(info._endMillis, ts);
      Assert.assertSame(info._status, HttpResponseStatus.OK);
      Assert.assertEquals(info._contentLength, 0);
      Assert.assertEquals(info._contentLocation, null);

      HttpUtil.setContentLength(response, 100);
      response.headers().set(HttpHeaderNames.CONTENT_LOCATION, "/foo");
      info = new RequestLogHandler.ResponseInfo(response, false);
      Assert.assertEquals(info._contentLength, 100);
      Assert.assertEquals(info._contentLocation, "/foo");
    } finally {
      Time.restore();
    }
  }

  @Test
  public void testRequestInfo() {
    try {
      Time.freeze();
      HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/foo/bar?query=123");
      long ts = Time.currentTimeMillis();
      RequestLogHandler.RequestInfo info = new RequestLogHandler.RequestInfo(request);

      Assert.assertEquals(info._startMillis, ts);
      Assert.assertSame(info._method, HttpMethod.GET);
      Assert.assertSame(info._protocolVersion, HttpVersion.HTTP_1_1);
      Assert.assertEquals(info._uri, "/foo/bar?query=123");
    } finally {
      Time.restore();
    }
  }

  @Test
  public void testHappyPath() {
    // Set up our mock request/response and constants
    String pipeline = "myPipeline";
    SocketAddress remoteAddress = InetSocketAddress.createUnresolved("remoteAddress", 0);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/foo/bar?query=123");
    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    HttpUtil.setContentLength(response, 100);
    response.headers().add(HttpHeaderNames.CONTENT_LOCATION, "/foo");

    // Set up the mock logger and create the handler
    Mockito.reset(_mockLog);
    when(_mockLog.isDebugEnabled()).thenReturn(true);
    when(_mockLog.isInfoEnabled()).thenReturn(true);
    RequestLogHandler handler = new RequestLogHandler(_mockLog, pipeline);

    RequestLogHandler.ConnectInfo connectInfo = new RequestLogHandler.ConnectInfo(remoteAddress);

    // Happy Path: connect, request, response, disconnect
    handler.handleConnect(connectInfo);
    verify(_mockLog).info(eq("{} {} CONNECTED"), anyString(), any(SocketAddress.class)); // 1 DEBUG messages - connect
    handler.handleHttpRequest(connectInfo, request);
    handler.handleLastHttpRequestContent(connectInfo, LastHttpContent.EMPTY_LAST_CONTENT, true, true);
    handler.handleHttpResponse(connectInfo, response, false);
    handler.handleLastHttpResponseContent(connectInfo, LastHttpContent.EMPTY_LAST_CONTENT, true, false);
    verify(_mockLog).debug(eq("{}"), any(Object.class)); // 1 INFO message - request+response pair
    handler.handleDisconnect(connectInfo);
    verify(_mockLog).info(eq("{} {} DISCONNECTED {}"), eq("myPipeline"), any(), any()); // 1 more DEBUG messages -
                                                                                        // disconnect

    // Happy Path: connect, request, response, request, response, request, response, disconnect
    reset(_mockLog);
    when(_mockLog.isDebugEnabled()).thenReturn(true);
    when(_mockLog.isInfoEnabled()).thenReturn(true);
    connectInfo = new RequestLogHandler.ConnectInfo(remoteAddress);
    handler.handleConnect(connectInfo);
    handler.handleHttpRequest(connectInfo, request);
    handler.handleLastHttpRequestContent(connectInfo, LastHttpContent.EMPTY_LAST_CONTENT, true, true);
    handler.handleHttpResponse(connectInfo, response, false);
    handler.handleLastHttpResponseContent(connectInfo, LastHttpContent.EMPTY_LAST_CONTENT, true, false);
    handler.handleHttpRequest(connectInfo, request);
    handler.handleLastHttpRequestContent(connectInfo, LastHttpContent.EMPTY_LAST_CONTENT, true, true);
    handler.handleHttpResponse(connectInfo, response, false);
    handler.handleLastHttpResponseContent(connectInfo, LastHttpContent.EMPTY_LAST_CONTENT, true, false);
    handler.handleHttpRequest(connectInfo, request);
    handler.handleLastHttpRequestContent(connectInfo, LastHttpContent.EMPTY_LAST_CONTENT, true, true);
    handler.handleHttpResponse(connectInfo, response, false);
    handler.handleLastHttpResponseContent(connectInfo, LastHttpContent.EMPTY_LAST_CONTENT, true, false);
    handler.handleDisconnect(connectInfo);
    verify(_mockLog).info(eq("{} {} CONNECTED"), anyString(), any(SocketAddress.class)); // 1 DEBUG message - connect
    verify(_mockLog).info(eq("{} {} DISCONNECTED {}"), eq("myPipeline"), any(), any()); // 1 DEBUG message - disconnect
    verify(_mockLog, times(3)).debug(eq("{}"), any(Object.class)); // 3 INFO messages - request+response pair x 3
  }

  @Test
  public void testHappyPath2() throws InterruptedException {
    // Set up our mock request/response and constants
    String pipeline = "myPipeline";
    SocketAddress remoteAddress = InetSocketAddress.createUnresolved("remoteAddress", 0);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/foo/bar?query=123");
    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    HttpUtil.setContentLength(response, 100);
    response.headers().add(HttpHeaderNames.CONTENT_LOCATION, "/foo");

    Mockito.reset(_mockLog);
    // Set up the mock logger and create the handler
    when(_mockLog.isDebugEnabled()).thenReturn(true);
    when(_mockLog.isInfoEnabled()).thenReturn(true);
    RequestLogHandler handler = new RequestLogHandler(_mockLog, pipeline);

    class TestChannel extends EmbeddedChannel {
      public TestChannel(ChannelHandler... handler) {
        super(handler);
      }

      @Override
      protected SocketAddress remoteAddress0() {
        return remoteAddress;
      }
    }

    EmbeddedChannel ch = new TestChannel(handler);

    verify(_mockLog).info(eq("{} {} CONNECTED"), anyString(), any(SocketAddress.class)); // 1 DEBUG messages - connect

    ch.writeInbound(
        request,
        new DefaultHttpContent(ByteBufUtil.writeAscii(UnpooledByteBufAllocator.DEFAULT, "foo")),
        LastHttpContent.EMPTY_LAST_CONTENT);
    ch.writeOutbound(
        response,
        new DefaultHttpContent(ByteBufUtil.writeAscii(UnpooledByteBufAllocator.DEFAULT, "foo")),
        LastHttpContent.EMPTY_LAST_CONTENT);

    ArgumentCaptor<Msg> messageCaptor = ArgumentCaptor.forClass(Msg.class);
    verify(_mockLog).debug(eq("{}"), messageCaptor.capture()); // 1 INFO message - request+response pair
    Assert.assertTrue(
        messageCaptor.getValue()
            .toString()
            .startsWith("myPipeline remoteAddress:0 HTTP/1.1 GET /foo/bar?query=123 3--> 200 OK 100 /foo "));

    ch.releaseInbound();
    ch.releaseOutbound();

    ch.disconnect().sync();

    verify(_mockLog).info(eq("{} {} DISCONNECTED {}"), eq("myPipeline"), any(), any()); // 1 more DEBUG messages -
                                                                                        // disconnect

    // Happy Path: connect, request, response, request, response, request, response, disconnect
    reset(_mockLog);
    when(_mockLog.isDebugEnabled()).thenReturn(true);
    when(_mockLog.isInfoEnabled()).thenReturn(true);

    ch = new TestChannel(handler);

    ch.writeInbound(request, LastHttpContent.EMPTY_LAST_CONTENT);
    ch.writeOutbound(response, LastHttpContent.EMPTY_LAST_CONTENT);

    ch.writeInbound(request, LastHttpContent.EMPTY_LAST_CONTENT);
    ch.writeOutbound(response, LastHttpContent.EMPTY_LAST_CONTENT);

    ch.writeInbound(request, LastHttpContent.EMPTY_LAST_CONTENT);
    ch.writeOutbound(response, LastHttpContent.EMPTY_LAST_CONTENT);

    ch.disconnect().sync();

    ch.releaseInbound();
    ch.releaseOutbound();

    verify(_mockLog).info(eq("{} {} CONNECTED"), anyString(), any(SocketAddress.class)); // 1 DEBUG message - connect
    verify(_mockLog).info(eq("{} {} DISCONNECTED {}"), eq("myPipeline"), any(), any()); // 1 DEBUG message - disconnect
    verify(_mockLog, times(3)).debug(eq("{}"), any(Object.class)); // 3 INFO messages - request+response pair x 3
    verifyNoMoreInteractions(_mockLog);
  }

  @Test
  public void testHappyPath3() throws InterruptedException {
    // Set up our mock request/response and constants
    String pipeline = "myPipeline";
    SocketAddress remoteAddress = InetSocketAddress.createUnresolved("remoteAddress", 0);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/foo/bar?query=123");
    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    HttpUtil.setContentLength(response, 100);
    response.headers().add(HttpHeaderNames.CONTENT_LOCATION, "/foo");

    Mockito.reset(_mockLog);
    // Set up the mock logger and create the handler
    when(_mockLog.isDebugEnabled()).thenReturn(true);
    when(_mockLog.isInfoEnabled()).thenReturn(true);
    RequestLogHandler handler = new RequestLogHandler(_mockLog, pipeline);

    class TestChannel extends EmbeddedChannel {
      public TestChannel(ChannelHandler... handler) {
        super(handler);
      }

      @Override
      protected SocketAddress remoteAddress0() {
        return remoteAddress;
      }
    }

    EmbeddedChannel ch = new TestChannel(handler);

    verify(_mockLog).info(eq("{} {} CONNECTED"), anyString(), any(SocketAddress.class)); // 1 DEBUG messages - connect

    ch.writeOutbound(
        request,
        new DefaultHttpContent(ByteBufUtil.writeAscii(UnpooledByteBufAllocator.DEFAULT, "foo")),
        LastHttpContent.EMPTY_LAST_CONTENT);
    ch.writeInbound(
        response,
        new DefaultHttpContent(ByteBufUtil.writeAscii(UnpooledByteBufAllocator.DEFAULT, "foo")),
        LastHttpContent.EMPTY_LAST_CONTENT);

    ArgumentCaptor<Msg> messageCaptor = ArgumentCaptor.forClass(Msg.class);
    verify(_mockLog).debug(eq("{}"), messageCaptor.capture()); // 1 INFO message - request+response pair
    Assert.assertTrue(
        messageCaptor.getValue()
            .toString()
            .startsWith("myPipeline remoteAddress:0 HTTP/1.1 GET /foo/bar?query=123 3--> 200 OK 100 /foo "),
        messageCaptor.getValue().toString());

    ch.disconnect().sync();

    ch.releaseInbound();
    ch.releaseOutbound();

    verify(_mockLog).info(eq("{} {} DISCONNECTED {}"), eq("myPipeline"), any(), any()); // 1 more DEBUG messages -
                                                                                        // disconnect

    // Happy Path: connect, request, response, request, response, request, response, disconnect
    reset(_mockLog);
    when(_mockLog.isDebugEnabled()).thenReturn(true);
    when(_mockLog.isInfoEnabled()).thenReturn(true);

    ch = new TestChannel(handler);

    ch.writeOutbound(request, LastHttpContent.EMPTY_LAST_CONTENT);
    ch.writeInbound(response, LastHttpContent.EMPTY_LAST_CONTENT);

    ch.writeOutbound(request, LastHttpContent.EMPTY_LAST_CONTENT);
    ch.writeInbound(response, LastHttpContent.EMPTY_LAST_CONTENT);

    ch.writeOutbound(request, LastHttpContent.EMPTY_LAST_CONTENT);
    ch.writeInbound(response, LastHttpContent.EMPTY_LAST_CONTENT);

    ch.disconnect().sync();

    ch.releaseInbound();
    ch.releaseOutbound();

    verify(_mockLog).info(eq("{} {} CONNECTED"), anyString(), any(SocketAddress.class)); // 1 DEBUG message - connect
    verify(_mockLog).info(eq("{} {} DISCONNECTED {}"), eq("myPipeline"), any(), any()); // 1 DEBUG message - disconnect
    verify(_mockLog, times(3)).debug(eq("{}"), any(Object.class)); // 3 INFO messages - request+response pair x 3
    verifyNoMoreInteractions(_mockLog);
  }

  @Test
  public void testDisconnect1() throws InterruptedException {
    // Set up our mock request/response and constants
    String pipeline = "myPipeline";
    SocketAddress remoteAddress = InetSocketAddress.createUnresolved("remoteAddress", 0);
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/foo/bar?query=123");

    Mockito.reset(_mockLog);
    // Set up the mock logger and create the handler
    when(_mockLog.isDebugEnabled()).thenReturn(true);
    when(_mockLog.isInfoEnabled()).thenReturn(true);
    RequestLogHandler handler = new RequestLogHandler(_mockLog, pipeline);

    verify(_mockLog).getName();

    class TestChannel extends EmbeddedChannel {
      public TestChannel(ChannelHandler... handler) {
        super(handler);
      }

      @Override
      protected SocketAddress remoteAddress0() {
        return remoteAddress;
      }
    }

    EmbeddedChannel ch = new TestChannel(handler);

    verify(_mockLog).info(eq("{} {} CONNECTED"), anyString(), any(InetSocketAddress.class)); // 1 DEBUG messages -
                                                                                             // connect

    ch.writeOutbound(
        request,
        new DefaultHttpContent(ByteBufUtil.writeAscii(UnpooledByteBufAllocator.DEFAULT, "foo")),
        LastHttpContent.EMPTY_LAST_CONTENT);

    ch.disconnect().sync();

    ch.releaseInbound();
    ch.releaseOutbound();

    verify(_mockLog).info(eq("{} {} DISCONNECTED {}"), eq("myPipeline"), any(), any()); // 1 more DEBUG messages -
                                                                                        // disconnect

    verify(_mockLog).info(
        eq("{} {} {} {} {} --> CHANNEL-CLOSED {}"),
        eq("myPipeline"),
        any(),
        same(HttpVersion.HTTP_1_1),
        same(HttpMethod.GET),
        eq("/foo/bar?query=123"),
        any());

    verifyNoMoreInteractions(_mockLog);
  }

  @Test
  public void testMissingRequestInfo() throws InterruptedException {
    // Set up our mock request/response and constants
    String pipeline = "myPipeline";
    HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    HttpUtil.setContentLength(response, 100);
    response.headers().set(HttpHeaderNames.CONTENT_LOCATION, "/foo");

    // Set up the mock logger and create the handler
    reset(_mockLog);
    RequestLogHandler handler = new RequestLogHandler(_mockLog, pipeline);

    Mockito.verify(_mockLog).getName();
    Mockito.verifyNoMoreInteractions(_mockLog);

    EmbeddedChannel ch = new EmbeddedChannel(handler);

    verify(_mockLog).info(eq("{} {} CONNECTED"), anyString(), any(SocketAddress.class)); // 1 DEBUG message - connect
    Mockito.verifyNoMoreInteractions(_mockLog);
    Mockito.reset(_mockLog);

    when(_mockLog.isDebugEnabled()).thenReturn(true);
    when(_mockLog.isInfoEnabled()).thenReturn(true);

    // Bad Path: connect, response, disconnect (with no request)
    ch.writeOneOutbound(response);
    verify(_mockLog).error(
        eq("{} {} without corresponding requestInfo or connectInfo. {}"),
        anyString(),
        eq("handleHttpResponse"),
        any()); // 1 ERROR message - response w/o request
    Mockito.verifyNoMoreInteractions(_mockLog);
    Mockito.reset(_mockLog);

    ch.disconnect().sync();
    verify(_mockLog).info(eq("{} {} DISCONNECTED {}"), eq("myPipeline"), any(), any()); // 1 DEBUG message - disconnect
    Mockito.verifyNoMoreInteractions(_mockLog);
  }

  /** Test that the infoToString method does not NPE when given null values. */
  @Test
  public void testInfoToStringNoNPE() {
    Assert.assertNotNull(String.valueOf(Msg.make(() -> RequestLogHandler.infoToString(null, null, null, null))));
  }

  @Test
  public void testHandler() throws InterruptedException {
    reset(_mockLog);
    Mockito.when(_mockLog.getName()).thenReturn("mockLog");
    RequestLogHandler handler = new RequestLogHandler(_mockLog, "testHandler");

    Mockito.verify(_mockLog).getName();
    Mockito.verifyNoMoreInteractions(_mockLog);

    EmbeddedChannel ch = new EmbeddedChannel(handler);

    verify(_mockLog).info(eq("{} {} CONNECTED"), anyString(), any()); // 1 DEBUG message - connect

    Mockito.verifyNoMoreInteractions(_mockLog);
    Mockito.reset(_mockLog);

    ch.disconnect().sync();

    verify(_mockLog).info(eq("{} {} DISCONNECTED {}"), eq("testHandler"), any(), any()); // 1 DEBUG message - disconnect

    Mockito.verifyNoMoreInteractions(_mockLog);
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
