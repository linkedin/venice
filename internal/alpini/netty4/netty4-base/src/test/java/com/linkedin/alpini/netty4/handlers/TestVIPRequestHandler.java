package com.linkedin.alpini.netty4.handlers;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import java.util.Objects;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 11/27/17.
 */
public class TestVIPRequestHandler {
  @Test(groups = "unit")
  public void testInitial() throws InterruptedException {
    VIPRequestHandler handler = new VIPRequestHandler("/healthcheck");

    EmbeddedChannel ch = new EmbeddedChannel(handler);

    HttpRequest healthcheckRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/healthcheck");
    ch.writeOneInbound(healthcheckRequest).sync();

    FullHttpResponse response = Objects.requireNonNull(ch.readOutbound());
    Assert.assertEquals(response.status(), HttpResponseStatus.SERVICE_UNAVAILABLE);
    Assert.assertFalse(HttpUtil.isKeepAlive(response));
    Assert.assertEquals(handler.state(), VIPRequestHandler.State.INITIAL);

    ch.disconnect().sync();
  }

  @Test(groups = "unit")
  public void testRunning() throws InterruptedException {
    VIPRequestHandler handler = new VIPRequestHandler("/healthcheck");

    handler.start();

    EmbeddedChannel ch = new EmbeddedChannel(handler);

    HttpRequest healthcheckRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/healthcheck");
    ch.writeOneInbound(healthcheckRequest).sync();

    FullHttpResponse response = Objects.requireNonNull(ch.readOutbound());
    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    Assert.assertTrue(HttpUtil.isKeepAlive(response));
    Assert.assertEquals(handler.state(), VIPRequestHandler.State.RUNNING);

    ch.disconnect().sync();
  }

  @Test(groups = "unit")
  public void testRunningResponseWithContinue() throws InterruptedException {
    VIPRequestHandler handler = new VIPRequestHandler("/healthcheck");

    handler.start();

    EmbeddedChannel ch = new EmbeddedChannel(handler);

    HttpRequest healthcheckRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    healthcheckRequest.headers().set(HttpHeaderNames.EXPECT, HttpHeaderValues.CONTINUE);
    ch.writeOneInbound(healthcheckRequest).sync();

    FullHttpResponse response = Objects.requireNonNull(ch.readOutbound());
    Assert.assertEquals(response.status(), HttpResponseStatus.CONTINUE);
    Assert.assertTrue(HttpUtil.isKeepAlive(response));

    response = Objects.requireNonNull(ch.readOutbound());
    Assert.assertEquals(response.status(), HttpResponseStatus.OK);
    Assert.assertTrue(HttpUtil.isKeepAlive(response));
    Assert.assertEquals(handler.state(), VIPRequestHandler.State.RUNNING);

    ch.disconnect().sync();
  }

  @Test(groups = "unit")
  public void testShutdown() throws InterruptedException {
    VIPRequestHandler handler = new VIPRequestHandler("/healthcheck");

    handler.shutdown();
    Thread.sleep(6000);

    EmbeddedChannel ch = new EmbeddedChannel(handler);

    HttpRequest healthcheckRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/healthcheck");
    ch.writeOneInbound(healthcheckRequest).sync();

    FullHttpResponse response = Objects.requireNonNull(ch.readOutbound());
    Assert.assertEquals(response.status(), HttpResponseStatus.SERVICE_UNAVAILABLE);
    Assert.assertFalse(HttpUtil.isKeepAlive(response));
    Assert.assertEquals(handler.state(), VIPRequestHandler.State.SHUTDOWN);

    ch.disconnect().sync();
  }
}
