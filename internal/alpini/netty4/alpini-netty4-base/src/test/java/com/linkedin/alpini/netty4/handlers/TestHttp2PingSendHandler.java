package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.netty4.pool.Http2PingCallListener;
import com.linkedin.alpini.netty4.pool.Http2PingHelper;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http2.DefaultHttp2PingFrame;
import io.netty.handler.codec.http2.Http2PingFrame;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

/*
* This class is implemented to test Http2PingSendHandler.
*
* @author Binbing Hou <bhou@linkedin.com>
* */


public class TestHttp2PingSendHandler {
  @Test
  public void basicTest() {
    Http2PingCallListener listener = Mockito.spy(new Http2PingHelper());
    Http2PingSendHandler handler = Mockito.spy(new Http2PingSendHandler(listener));

    EmbeddedChannel channel = new EmbeddedChannel(handler);

    // write http2 ping
    long pingId = Time.currentTimeMillis();
    long currentTimeNanosForPing = 123456L;
    Mockito.doReturn(currentTimeNanosForPing).when(handler).getCurrentTimeNanos();
    Http2PingFrame http2PingFrame = new DefaultHttp2PingFrame(pingId, false);
    channel.writeOutbound(http2PingFrame);

    Assert.assertEquals(handler.getLastPingSendTime(), currentTimeNanosForPing);
    Assert.assertEquals(handler.getLastPingAckTime(), 0L);
    Assert.assertEquals(handler.getLastPingId(), pingId);
    Assert.assertEquals(handler.getLastPingChannel(), channel);
    Mockito.verify(listener).callStart(pingId, currentTimeNanosForPing);

    // write http2 ping ack
    Http2PingFrame http2PingAckFrame = new DefaultHttp2PingFrame(pingId, true);
    long currentTimeNanosForPingAck = 123457L;
    Mockito.doReturn(currentTimeNanosForPingAck).when(handler).getCurrentTimeNanos();
    channel.writeInbound(http2PingAckFrame);

    Assert.assertEquals(handler.getLastPingSendTime(), currentTimeNanosForPing);
    Assert.assertEquals(handler.getLastPingAckTime(), currentTimeNanosForPingAck);
    Assert.assertEquals(handler.getLastPingId(), pingId);
    Assert.assertEquals(handler.getLastPingChannel(), channel);
    Mockito.verify(listener).callClose(pingId, currentTimeNanosForPingAck);

    // reset
    Mockito.reset(listener, handler);
  }

  @Test
  public void pingAckTimeOutTest() {
    Http2PingCallListener listener = Mockito.spy(new Http2PingHelper());
    Http2PingSendHandler handler = Mockito.spy(new Http2PingSendHandler(listener));

    EmbeddedChannel channel = new EmbeddedChannel(handler);

    // write first http2 ping
    long pingId1 = Time.currentTimeMillis();
    long currentTimeNanosForPing1 = 123456L;
    Mockito.doReturn(currentTimeNanosForPing1).when(handler).getCurrentTimeNanos();
    Http2PingFrame http2PingFrame1 = new DefaultHttp2PingFrame(pingId1, false);
    channel.writeOutbound(http2PingFrame1);

    // write the second http2 ping
    long pingId2 = Time.currentTimeMillis();
    long currentTimeNanosForPing2 = 123457L;
    Mockito.doReturn(currentTimeNanosForPing2).when(handler).getCurrentTimeNanos();
    Http2PingFrame http2PingFrame2 = new DefaultHttp2PingFrame(pingId2, false);
    channel.writeOutbound(http2PingFrame2);

    // the ack fo the first ping was not received, we consider it as a timeout error
    Mockito.verify(listener).callCloseWithError(Mockito.eq(pingId1), Mockito.eq(currentTimeNanosForPing2));

    Mockito.verify(listener).callStart(Mockito.eq(pingId2), Mockito.eq(currentTimeNanosForPing2));
    Assert.assertEquals(handler.getLastPingSendTime(), currentTimeNanosForPing2);
    Assert.assertEquals(handler.getLastPingAckTime(), 0L);
    Assert.assertEquals(handler.getLastPingId(), pingId2);
    Assert.assertEquals(handler.getLastPingChannel(), channel);

    // reset
    Mockito.reset(listener, handler);
  }

  @Test
  public void ignoredPingAckTest() {
    Http2PingCallListener listener = Mockito.spy(new Http2PingHelper());
    Http2PingSendHandler handler = Mockito.spy(new Http2PingSendHandler(listener));

    EmbeddedChannel channel = new EmbeddedChannel(handler);

    // write the http2 ping
    long pingId1 = Time.currentTimeMillis();
    long currentTimeNanosForPing1 = 123456L;
    Mockito.doReturn(currentTimeNanosForPing1).when(handler).getCurrentTimeNanos();
    Http2PingFrame http2PingFrame1 = new DefaultHttp2PingFrame(pingId1, false);
    channel.writeOutbound(http2PingFrame1);

    // write http2 ping ack with different ping id, which will be ignored
    long pingId2 = pingId1 + 1;
    Http2PingFrame http2PingAckFrame = new DefaultHttp2PingFrame(pingId2, true);
    channel.writeInbound(http2PingAckFrame);

    Assert.assertEquals(handler.getLastPingSendTime(), currentTimeNanosForPing1);
    Assert.assertEquals(handler.getLastPingAckTime(), 0L);
    Assert.assertEquals(handler.getLastPingId(), pingId1);
    Assert.assertEquals(handler.getLastPingChannel(), channel);
    Mockito.verify(listener, Mockito.never()).callClose(Mockito.eq(pingId2), Mockito.anyLong());

    // reset
    Mockito.reset(listener, handler);
  }

  @Test
  public void nonHttp2FrameTest() {
    Http2PingCallListener listener = Mockito.spy(new Http2PingHelper());
    Http2PingSendHandler handler = Mockito.spy(new Http2PingSendHandler(listener));

    EmbeddedChannel channel = new EmbeddedChannel(handler);
    channel.writeOutbound("hello world");

    Assert.assertEquals(handler.getLastPingSendTime(), 0L);
    Assert.assertEquals(handler.getLastPingAckTime(), 0L);
    Assert.assertEquals(handler.getLastPingId(), 0L);
    Assert.assertNull(handler.getLastPingChannel());
    Mockito.verify(listener, Mockito.never()).callStart(Mockito.anyLong(), Mockito.anyLong());
    Mockito.verify(listener, Mockito.never()).callCloseWithError(Mockito.anyLong(), Mockito.anyLong());

    channel.writeInbound("hello world");
    Assert.assertEquals(handler.getLastPingSendTime(), 0L);
    Assert.assertEquals(handler.getLastPingAckTime(), 0L);
    Assert.assertEquals(handler.getLastPingId(), 0L);
    Assert.assertNull(handler.getLastPingChannel());
    Mockito.verify(listener, Mockito.never()).callStart(Mockito.anyLong(), Mockito.anyLong());

    // reset
    Mockito.reset(listener, handler);
  }
}
