package com.linkedin.venice.listener;

import com.linkedin.venice.stats.ServerNettyThreadStats;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpRequest;
import io.tehuti.metrics.MetricsRepository;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Counts threads executing the synchronous HTTP request pipeline, not requests awaiting asynchronous responses.
 * One instance is shared by all HTTP/1.1 connections and HTTP/2 streams belonging to a listener.
 */
@ChannelHandler.Sharable
public class ServerNettyThreadStatsHandler extends ChannelInboundHandlerAdapter {
  private final AtomicInteger activeRequestThreads = new AtomicInteger();
  private final ThreadLocal<Boolean> processingRequest = new ThreadLocal<>();
  private final ServerNettyThreadStats stats;

  public ServerNettyThreadStatsHandler(MetricsRepository metricsRepository) {
    stats = new ServerNettyThreadStats(metricsRepository);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (!(msg instanceof HttpRequest)) {
      ctx.fireChannelRead(msg);
      return;
    }

    // Reentrant request dispatch on the same event-loop thread still occupies only one worker.
    boolean isOutermostRequest = !Boolean.TRUE.equals(processingRequest.get());
    if (isOutermostRequest) {
      processingRequest.set(true);
      activeRequestThreads.incrementAndGet();
    }
    try {
      stats.recordActiveRequestThreadCount(activeRequestThreads.get());
      ctx.fireChannelRead(msg);
    } finally {
      if (isOutermostRequest) {
        activeRequestThreads.decrementAndGet();
        processingRequest.remove();
      }
    }
  }
}
