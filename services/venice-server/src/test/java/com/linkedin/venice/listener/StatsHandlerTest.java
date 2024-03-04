package com.linkedin.venice.listener;

import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.testng.annotations.Test;


public class StatsHandlerTest {
  @Test
  public void testQuotaRejection() {
    StatsHandler statsHandler = mock(StatsHandler.class);
    ServerStatsContext serverStatsContext = mock(ServerStatsContext.class);
    doReturn(serverStatsContext).when(statsHandler).getServerStatsContext();
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    Object msg = new Object();
    ChannelPromise promise = mock(ChannelPromise.class);
    doReturn(TOO_MANY_REQUESTS).when(serverStatsContext).getResponseStatus();
    ChannelFuture future = mock(ChannelFuture.class);
    doReturn(future).when(ctx).writeAndFlush(any());
    doCallRealMethod().when(statsHandler).write(ctx, msg, promise);
    verify(serverStatsContext, times(0)).errorRequest(any(), anyLong());
  }
}
