package com.linkedin.alpini.netty4.pool;

import com.linkedin.alpini.base.statistics.LongStats;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http2.DefaultHttp2PingFrame;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


/*
* This class is designed to test the Http2PingHelper
*
* @author Binbing Hou <bhou@linkedin.com>
* */
public class TestHttp2PingHelper {
  @Test
  public void sendPingTest() {
    Http2PingHelper http2PingHelper = new Http2PingHelper();

    Channel channel = Mockito.mock(Channel.class);
    ChannelFuture writeFuture = Mockito.mock(ChannelFuture.class);
    Mockito.doReturn(writeFuture).when(channel).writeAndFlush(Mockito.any());

    ChannelPipeline pipeline = Mockito.mock(ChannelPipeline.class);
    Http2ConnectionHandler http2ConnectionHandler = Mockito.mock(Http2ConnectionHandler.class);
    Mockito.doReturn(http2ConnectionHandler).when(pipeline).get(Mockito.eq(Http2ConnectionHandler.class));
    Mockito.doReturn(pipeline).when(channel).pipeline();

    http2PingHelper.sendPing(channel);
    Mockito.verify(channel).writeAndFlush(Mockito.any(DefaultHttp2PingFrame.class));

    // reset
    Mockito.reset(channel, writeFuture, pipeline, http2ConnectionHandler);
  }

  @Test
  public void pingCallTrackerTest_fewPingCalls() {
    Http2PingHelper http2PingHelper = new Http2PingHelper();
    for (int i = 1; i < 5; i++) {
      http2PingHelper.callStart(i, i);
      http2PingHelper.callClose(i, i + 1);
    }
    Assert.assertEquals(http2PingHelper.getAvgResponseTimeOfLatestPings(), 0D);
    LongStats pingLongStats = http2PingHelper.pingCallTracker().getCallStats().getCallTimeStats();
    Assert.assertNotNull(pingLongStats);
    Assert.assertEquals(pingLongStats.getMinimum().longValue(), 1L);
    Assert.assertEquals(pingLongStats.getMaximum().longValue(), 1L);
  }

  @Test
  public void pingCallTrackerTest_manyPingCalls() {
    Http2PingHelper http2PingHelper = new Http2PingHelper();
    for (int i = 1; i < 101; i++) {
      http2PingHelper.callStart(i, i);
      http2PingHelper.callClose(i, 2 * i);
    }
    Assert.assertEquals(http2PingHelper.getAvgResponseTimeOfLatestPings(), 98D);
    LongStats pingLongStats = http2PingHelper.pingCallTracker().getCallStats().getCallTimeStats();
    Assert.assertNotNull(pingLongStats);
    Assert.assertEquals(pingLongStats.getMinimum().longValue(), 1L);
    Assert.assertEquals(pingLongStats.getMaximum().longValue(), 100L);
  }

  @Test
  public void pingCallTrackerTest_errorPingCalls() {
    Http2PingHelper http2PingHelper = new Http2PingHelper();
    for (int i = 1; i < 5; i++) {
      http2PingHelper.callStart(i, i);
      http2PingHelper.callClose(i, i + 1);
    }
    http2PingHelper.callStart(5, 6);
    http2PingHelper.callCloseWithError(5, 102);
    Assert.assertEquals(http2PingHelper.getAvgResponseTimeOfLatestPings(), 20D);
    LongStats pingLongStats = http2PingHelper.pingCallTracker().getCallStats().getCallTimeStats();
    Assert.assertNotNull(pingLongStats);
    Assert.assertEquals(pingLongStats.getMinimum().longValue(), 1L);
    Assert.assertEquals(pingLongStats.getMaximum().longValue(), 96L);
  }

  @Test
  public void pingCallTrackerTest_wrongPingIds() {
    Http2PingHelper http2PingHelper = new Http2PingHelper();
    for (int i = 1; i < 5; i++) {
      http2PingHelper.callStart(i, i);
      http2PingHelper.callClose(i, i + 1);
    }
    http2PingHelper.callStart(5, 6);
    http2PingHelper.callCloseWithError(6, 102);
    Assert.assertEquals(http2PingHelper.getAvgResponseTimeOfLatestPings(), 0D);
    LongStats pingLongStats = http2PingHelper.pingCallTracker().getCallStats().getCallTimeStats();
    Assert.assertNotNull(pingLongStats);
    Assert.assertEquals(pingLongStats.getMinimum().longValue(), 1L);
    Assert.assertEquals(pingLongStats.getMaximum().longValue(), 1L);
  }

  @Test
  public void pingCallCompleteTest() {
    Http2PingHelper http2PingHelper = new Http2PingHelper();
    Assert.assertTrue(http2PingHelper.isCallComplete(0));
    http2PingHelper.callStart(1, 1);
    Assert.assertFalse(http2PingHelper.isCallComplete(1));
    http2PingHelper.callClose(1, 2);
    Assert.assertTrue(http2PingHelper.isCallComplete(1));
  }

  @Test
  public void createHttp2PingHandlerTest() {
    Http2PingHelper http2PingHelper = new Http2PingHelper();
    Assert.assertNotNull(http2PingHelper.getHttp2PingSendHandler());
  }
}
