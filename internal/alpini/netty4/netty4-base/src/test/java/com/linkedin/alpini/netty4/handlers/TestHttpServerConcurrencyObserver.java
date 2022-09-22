package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.monitoring.CallTracker;
import com.linkedin.alpini.base.monitoring.CallTrackerImpl;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import java.nio.charset.StandardCharsets;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Created by acurtis on 12/12/17.
 */
public class TestHttpServerConcurrencyObserver {
  @Test(groups = "unit")
  public void testBasic() throws InterruptedException {
    CallTracker callTracker = new CallTrackerImpl();
    EmbeddedChannel channel = new EmbeddedChannel(new HttpServerConcurrencyObserver(callTracker));

    HttpObject object = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    channel.writeOneInbound(object).sync();
    Assert.assertSame(channel.readInbound(), object);
    Assert.assertEquals(callTracker.getCurrentConcurrency(), 1);
    channel.writeOneInbound(LastHttpContent.EMPTY_LAST_CONTENT).sync();
    Assert.assertSame(channel.readInbound(), LastHttpContent.EMPTY_LAST_CONTENT);
    Assert.assertEquals(callTracker.getCurrentConcurrency(), 1);

    channel.writeOneInbound(object).sync();
    Assert.assertSame(channel.readInbound(), object);
    Assert.assertEquals(callTracker.getCurrentConcurrency(), 2);
    object = new DefaultHttpContent(Unpooled.copiedBuffer("Hello world", StandardCharsets.UTF_8));
    channel.writeOneInbound(object).sync();
    Assert.assertSame(channel.readInbound(), object);
    Assert.assertEquals(callTracker.getCurrentConcurrency(), 2);
    channel.writeOneInbound(LastHttpContent.EMPTY_LAST_CONTENT).sync();
    Assert.assertSame(channel.readInbound(), LastHttpContent.EMPTY_LAST_CONTENT);
    Assert.assertEquals(callTracker.getCurrentConcurrency(), 2);

    object = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
    channel.writeOneInbound(object).sync();
    Assert.assertSame(channel.readInbound(), object);
    Assert.assertEquals(callTracker.getCurrentConcurrency(), 3);
    channel.writeOneInbound(LastHttpContent.EMPTY_LAST_CONTENT).sync();
    Assert.assertSame(channel.readInbound(), LastHttpContent.EMPTY_LAST_CONTENT);
    Assert.assertEquals(callTracker.getCurrentConcurrency(), 3);

    object = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    channel.writeOneOutbound(object);
    channel.flushOutbound();
    Assert.assertSame(channel.readOutbound(), object);
    Assert.assertEquals(callTracker.getCurrentConcurrency(), 3);
    channel.writeOneOutbound(LastHttpContent.EMPTY_LAST_CONTENT);
    channel.flushOutbound();
    Assert.assertSame(channel.readOutbound(), LastHttpContent.EMPTY_LAST_CONTENT);
    Assert.assertEquals(callTracker.getCurrentConcurrency(), 2);

    object = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    channel.writeOneOutbound(object);
    channel.flushOutbound();
    Assert.assertSame(channel.readOutbound(), object);
    Assert.assertEquals(callTracker.getCurrentConcurrency(), 2);
    Assert.assertEquals(callTracker.getCurrentErrorCountTotal(), 0);
    channel.writeOneOutbound(LastHttpContent.EMPTY_LAST_CONTENT);
    channel.flushOutbound();
    Assert.assertSame(channel.readOutbound(), LastHttpContent.EMPTY_LAST_CONTENT);
    Assert.assertEquals(callTracker.getCurrentConcurrency(), 1);
    Assert.assertEquals(callTracker.getCurrentErrorCountTotal(), 1);

    channel.writeOneOutbound(object);
    channel.flushOutbound();
    Assert.assertSame(channel.readOutbound(), object);
    Assert.assertEquals(callTracker.getCurrentConcurrency(), 1);
    object = new DefaultHttpContent(Unpooled.copiedBuffer("Goodbye world", StandardCharsets.UTF_8));
    channel.writeOneOutbound(object);
    channel.flushOutbound();
    Assert.assertSame(channel.readOutbound(), object);
    Assert.assertEquals(callTracker.getCurrentConcurrency(), 1);
    channel.writeOneOutbound(LastHttpContent.EMPTY_LAST_CONTENT);
    channel.flushOutbound();
    Assert.assertSame(channel.readOutbound(), LastHttpContent.EMPTY_LAST_CONTENT);
    Assert.assertEquals(callTracker.getCurrentConcurrency(), 0);

    channel.close().sync();
  }
}
