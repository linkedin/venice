package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.netty4.misc.BasicHttpRequest;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;


/**
 * An abstract handler which may be used to examine latencies in the Netty4 HTTP request processing.
 *
 * It is intended to be placed immediately after {@linkplain io.netty.handler.codec.http.HttpServerCodec}
 * or {@link BasicHttpServerCodec} handlers. When the {@linkplain BasicHttpServerCodec} is used, the request
 * start time is the time when the first line of the request containing the URI was received.
 *
 * Extend this class, overriding the {@link HttpServerStatisticsHandler#complete(Stats)} method to process
 * the {@link HttpServerStatisticsHandler.Stats} objects. The instance is sharable.
 *
 * <pre>
 *   final HttpServerStatisticsHandler _stats = new HttpServerStatisticsHandler() {
 *     &at;Override
 *     protected void complete(Stats stats) {
 *       ...
 *     }
 *   };
 *
 *   final ChannelHandler _initialization = new ChannelInitializer&lt;Channel&gt;() {
 *     &at;Override
 *     protected void initChannel(Channel ch) {
 *       ChannelPipeline p = ch.pipeline();
 *
 *       ...
 *
 *       p.addLast("http-server-codec", new HttpServerCodec());
 *       p.addLast("http-server-stats", _stats);
 *
 *       ...
 *     }
 *   }
 *
 * </pre>
 *
 * Created by acurtis on 5/1/18.
 */
@ChannelHandler.Sharable
public abstract class HttpServerStatisticsHandler extends SimpleChannelInitializer<Channel> {
  @Override
  protected void initChannel(Channel ch) throws Exception {
    addAfter(ch, new Handler());
  }

  public static final class Stats {
    /** method of the request */
    public HttpMethod _method;

    /** nanos at time the request was received */
    public long _requestHeaderReceivedTime;

    /** nanos at time when start of request content was received */
    public long _requestContentStartTime;

    /** nanos at time when end of request content was received */
    public long _requestContentReceivedTime;

    /** nanos at time when response header was written */
    public long _responseHeaderReadyTime;

    /** nanos at time when response content start was written */
    public long _responseContentStartTime;

    /** nanos at time when response content end was written */
    public long _responseContentReadyTime;

    /** nanos at time when response was completely written to the OS */
    public long _responseContentWrittenTime;

    /** nanos at time when flush was called after response header was written */
    public long _responseHeaderFlushTime;

    /** nanos at time when flush was called after response content start was written */
    public long _responseFirstDataFlushTime;

    /** nanos at time when flush was called after response content end was written */
    public long _responseContentFlushTime;

    /** nanos at time when last flush was seen by this request */
    public long _responseLastFlushTime;

    /** aggregate nanos of inbound channelRead after this handler */
    public long _aggregateReadProcessingTime;

    /** aggregate nanos of outbound write before this handler */
    public long _aggregateWriteProcessingTime;

    /** aggregate nanos of outbound flush before this handler */
    public long _aggregateFlushProcessingTime;

    private boolean _completed;

    @Override
    public String toString() {
      return "Stats{" + "_method=" + _method + ", _requestHeaderReceivedTime=" + _requestHeaderReceivedTime
          + ", _requestContentStartTime=" + _requestContentStartTime + ", _requestContentReceivedTime="
          + _requestContentReceivedTime + ", _responseHeaderReadyTime=" + _responseHeaderReadyTime
          + ", _responseContentStartTime=" + _responseContentStartTime + ", _responseContentReadyTime="
          + _responseContentReadyTime + ", _responseContentWrittenTime=" + _responseContentWrittenTime
          + ", _responseHeaderFlushTime=" + _responseHeaderFlushTime + ", _responseFirstDataFlushTime="
          + _responseFirstDataFlushTime + ", _responseContentFlushTime=" + _responseContentFlushTime
          + ", _responseLastFlushTime=" + _responseLastFlushTime + ", _aggregateReadProcessingTime="
          + _aggregateReadProcessingTime + ", _aggregateWriteProcessingTime=" + _aggregateWriteProcessingTime
          + ", _aggregateFlushProcessingTime=" + _aggregateFlushProcessingTime + '}';
    }
  }

  /**
   * This method is invoked for every complete {@link Stats} object.
   * @param stats statistics
   */
  protected abstract void complete(Stats stats);

  private class Handler extends ChannelDuplexHandler {
    final LinkedList<Stats> _statsQueue = new LinkedList<>();
    final Set<Stats> _flushable = new HashSet<>();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      long methodStart = Time.nanoTime();
      Stats stats;

      if (msg instanceof HttpRequest) {
        HttpRequest request = (HttpRequest) msg;
        stats = new Stats();
        _statsQueue.addLast(stats);
        stats._requestHeaderReceivedTime =
            request instanceof BasicHttpRequest ? ((BasicHttpRequest) request).getRequestNanos() : methodStart;
        stats._method = request.method();
      } else {
        stats = null;
      }

      if (msg instanceof HttpContent) {
        stats = _statsQueue.peekLast();

        if (stats._requestContentStartTime == 0) {
          stats._requestContentStartTime = methodStart;
        }

        if (msg instanceof LastHttpContent) {
          stats._requestContentReceivedTime = methodStart;
        }
      }

      try {
        super.channelRead(ctx, msg);
      } finally {
        long delta = Time.nanoTime() - methodStart;
        if (stats != null) {
          stats._aggregateReadProcessingTime += delta;
        }
      }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
      Stats stats = _statsQueue.peekFirst();
      long methodStart = Time.nanoTime();
      if (msg instanceof HttpResponse) {
        stats._responseHeaderReadyTime = methodStart;
        _flushable.add(stats);
      }

      if (msg instanceof HttpContent) {
        if (stats._responseContentStartTime == 0) {
          stats._responseContentStartTime = methodStart;
        }

        if (msg instanceof LastHttpContent) {
          _statsQueue.pollFirst();
          stats._responseContentReadyTime = methodStart;
          promise.addListener(f -> {
            stats._responseContentWrittenTime = Time.nanoTime();
            stats._completed = true;
          });
        }
      }
      try {
        super.write(ctx, msg, promise);
      } finally {
        long delta = Time.nanoTime() - methodStart;
        if (stats != null) {
          stats._aggregateWriteProcessingTime += delta;
        }
      }
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
      long flushTime = Time.nanoTime();
      _flushable.forEach(stats -> {

        if (stats._responseHeaderFlushTime == 0 && stats._responseHeaderReadyTime != 0) {
          stats._responseHeaderFlushTime = flushTime;
        }

        if (stats._responseFirstDataFlushTime == 0 && stats._responseContentStartTime != 0) {
          stats._responseFirstDataFlushTime = flushTime;
        }

        if (stats._responseContentFlushTime == 0 && stats._responseContentReadyTime != 0) {
          stats._responseContentFlushTime = flushTime;
        }

        stats._responseLastFlushTime = flushTime;
      });
      try {
        super.flush(ctx);
      } finally {
        long delta = (Time.nanoTime() - flushTime) / Math.max(1, _flushable.size());
        _flushable.removeIf(stats -> {
          stats._aggregateFlushProcessingTime += delta;
          boolean completed = stats._completed;
          if (completed) {
            complete(stats);
          }
          return completed;
        });
      }
    }
  }
}
