package com.linkedin.venice.listener;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.venice.stats.AggServerHttpRequestStats;
import com.linkedin.venice.stats.ServerHttpRequestStats;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.GenericFutureListener;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.Property;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;


public class StatsHandlerTest {
  /**
   * An unattributed error (no store name resolved, e.g. a malformed-URI BAD_REQUEST that is rejected upstream of
   * {@link RouterRequestHttpHandler}) should:
   * <ul>
   *   <li>still be recorded against the aggregate {@code total--error_request} stats, and</li>
   *   <li>have its source logged exactly once, with repeats suppressed by the redundant-exception filter.</li>
   * </ul>
   */
  @Test
  public void logsUnattributedErrorSourceAndIsRateLimited() throws Exception {
    // The first agg is used for SINGLE_GET (the default request type); an unattributed error resolves a per-store
    // stats object under the "unknown" store name, so error metrics land on this store-stats mock.
    AggServerHttpRequestStats singleGetStats = mock(AggServerHttpRequestStats.class);
    ServerHttpRequestStats unknownStoreStats = mock(ServerHttpRequestStats.class);
    when(singleGetStats.getStoreStats(any())).thenReturn(unknownStoreStats);
    StatsHandler statsHandler = new StatsHandler(
        singleGetStats,
        mock(AggServerHttpRequestStats.class),
        mock(AggServerHttpRequestStats.class),
        null);

    // A unique remote host so the process-wide RedundantExceptionFilter state cannot leak across tests/runs, and so the
    // appender can match this test's WARN by substring.
    String remoteHost = "unattributed-error-test-" + System.nanoTime() + ".invalid";

    AtomicInteger warnCount = new AtomicInteger();
    WarnMessageCountAppender appender =
        new WarnMessageCountAppender.Builder().setCounter(warnCount).setMarker(remoteHost).build();
    appender.start();
    LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
    Configuration config = loggerContext.getConfiguration();
    config.addLoggerAppender((org.apache.logging.log4j.core.Logger) LogManager.getLogger(StatsHandler.class), appender);

    // First unattributed BAD_REQUEST: error recorded against total + one WARN identifying the source.
    driveErrorResponse(statsHandler, remoteHost);
    // Second identical request: error still recorded, but the WARN is suppressed by the redundant filter.
    driveErrorResponse(statsHandler, remoteHost);

    // The error is recorded (against the "unknown" store stats) for BOTH requests...
    verify(unknownStoreStats, atLeast(2)).recordErrorRequestAndLatency(any(), any(), anyDouble(), anyInt());
    // ...but the source is logged only once thanks to the redundant filter.
    assertEquals(warnCount.get(), 1, "Identical unattributed errors should be logged exactly once");
  }

  /**
   * Drives a single error response (with no store name, hence "unattributed") through {@link StatsHandler#write} and
   * invokes the write-completion listener that records the metrics and logs the source.
   */
  private void driveErrorResponse(StatsHandler statsHandler, String remoteHost) throws Exception {
    // Re-arm for a fresh request.
    ServerStatsContext statsContext = statsHandler.getServerStatsContext();
    statsContext.setStatCallBackExecuted(false);
    statsHandler.setResponseStatus(BAD_REQUEST);
    // storeName intentionally left null -> error attributed only to the aggregate "total" stats.

    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    Channel channel = mock(Channel.class);
    ChannelPipeline pipeline = mock(ChannelPipeline.class);
    ChannelFuture future = mock(ChannelFuture.class);
    when(ctx.channel()).thenReturn(channel);
    when(ctx.pipeline()).thenReturn(pipeline);
    when(pipeline.get(SslHandler.class)).thenReturn(null);
    when(channel.parent()).thenReturn(null);
    when(channel.remoteAddress()).thenReturn(InetSocketAddress.createUnresolved(remoteHost, 12345));
    Object msg = new Object();
    when(ctx.writeAndFlush(msg)).thenReturn(future);
    when(future.isSuccess()).thenReturn(true);

    ArgumentCaptor<GenericFutureListener> captor = ArgumentCaptor.forClass(GenericFutureListener.class);
    statsHandler.write(ctx, msg, mock(ChannelPromise.class));
    verify(future).addListener(captor.capture());
    captor.getValue().operationComplete(future);
  }

  /**
   * Counts WARN log events whose formatted message contains a marker substring. Modeled on {@link ErrorCountAppender}.
   */
  private static class WarnMessageCountAppender extends AbstractAppender {
    private final AtomicInteger counter;
    private final String marker;

    protected WarnMessageCountAppender(
        String name,
        Filter filter,
        Layout<? extends Serializable> layout,
        boolean ignoreExceptions,
        Property[] properties,
        AtomicInteger counter,
        String marker) {
      super(name, filter, layout, ignoreExceptions, properties);
      this.counter = counter;
      this.marker = marker;
    }

    public static class Builder<B extends Builder<B>> extends AbstractAppender.Builder<B>
        implements org.apache.logging.log4j.core.util.Builder<WarnMessageCountAppender> {
      private AtomicInteger counter;
      private String marker;

      public B setCounter(AtomicInteger counter) {
        this.counter = counter;
        return asBuilder();
      }

      public B setMarker(String marker) {
        this.marker = marker;
        return asBuilder();
      }

      @Override
      public WarnMessageCountAppender build() {
        return new WarnMessageCountAppender(
            "WarnMessageCountAppender",
            getFilter(),
            null,
            false,
            getPropertyArray(),
            counter,
            marker);
      }
    }

    @Override
    public void append(LogEvent event) {
      if (event.getLevel().equals(Level.WARN) && event.getMessage().getFormattedMessage().contains(marker)) {
        counter.addAndGet(1);
      }
    }
  }
}
