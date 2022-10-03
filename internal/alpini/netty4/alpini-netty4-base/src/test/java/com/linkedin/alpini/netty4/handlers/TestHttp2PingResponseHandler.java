package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.Time;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http2.DefaultHttp2PingFrame;
import io.netty.handler.codec.http2.Http2PingFrame;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


/*
* This class is designed to test Http2PingResponseHandler.
*
* @author Binbing Hou <bhou@linkedin.com>
* */
public class TestHttp2PingResponseHandler {
  @Test
  public void basicTest() throws Exception {
    ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
    Channel channel = Mockito.mock(Channel.class);
    EventLoop eventLoop = Mockito.mock(EventLoop.class);
    EventLoopGroup eventLoopGroup = Mockito.mock(EventLoopGroup.class);

    Mockito.doReturn(channel).when(ctx).channel();
    Mockito.doReturn(eventLoop).when(channel).eventLoop();
    Mockito.doReturn(eventLoopGroup).when(eventLoop).parent();
    Mockito.doNothing().when(eventLoopGroup).forEach(Mockito.any());

    Http2PingResponseHandler handler = new Http2PingResponseHandler();

    long pingId = Time.currentTimeMillis();
    handler.channelRead0(ctx, new DefaultHttp2PingFrame(pingId, false));
    Http2PingFrame pingAckFrame = handler.getLastPingAckFrame();
    Assert.assertNotNull(pingAckFrame);
    Assert.assertTrue(pingAckFrame.ack());
    Assert.assertEquals(pingAckFrame.content(), pingId);
    Mockito.verify(ctx).writeAndFlush(pingAckFrame);

    // reset
    Mockito.reset(ctx, channel, eventLoop, eventLoopGroup);
  }

  @Test
  public void nonHttp2PingFrameTest() {
    Http2PingResponseHandler handler = new Http2PingResponseHandler();
    EmbeddedChannel channel = new EmbeddedChannel(handler);
    channel.writeInbound("hello world");
    Assert.assertNull(handler.getLastPingAckFrame());
  }
}
