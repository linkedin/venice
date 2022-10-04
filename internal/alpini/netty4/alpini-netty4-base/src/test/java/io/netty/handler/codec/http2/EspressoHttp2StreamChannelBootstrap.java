package io.netty.handler.codec.http2;

import com.linkedin.alpini.netty4.misc.NettyUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.StringUtil;
import io.netty.util.internal.UnstableApi;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import java.nio.channels.ClosedChannelException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Forked from Netty 4 {@link Http2StreamChannelBootstrap} to allow using {@link EspressoHttp2MultiplexHandler} in the pipeline.
 *
 * @author Yaoming Zhan <yzhan@linkedin.com>
 */
@UnstableApi
public final class EspressoHttp2StreamChannelBootstrap {
  private static final InternalLogger logger = InternalLoggerFactory.getInstance(Http2StreamChannelBootstrap.class);

  private final Map<ChannelOption<?>, Object> options = new ConcurrentHashMap<>();
  private final Map<AttributeKey<?>, Object> attrs = new ConcurrentHashMap<>();
  private final Channel channel;
  private volatile ChannelHandler handler;
  private final boolean offload;

  // Cache the ChannelHandlerContext to speed up open(...) operations.
  private volatile ChannelHandlerContext multiplexCtx;

  public EspressoHttp2StreamChannelBootstrap(Channel channel) {
    this(channel, false);
  }

  public EspressoHttp2StreamChannelBootstrap(Channel channel, boolean offload) {
    this.channel = ObjectUtil.checkNotNull(channel, "channel");
    this.offload = offload;
  }

  /**
   * Allow to specify a {@link ChannelOption} which is used for the {@link Http2StreamChannel} instances once they got
   * created. Use a value of {@code null} to remove a previous set {@link ChannelOption}.
   */
  public <T> EspressoHttp2StreamChannelBootstrap option(ChannelOption<T> option, T value) {
    if (option == null) {
      throw new NullPointerException("option");
    }
    if (value == null) {
      synchronized (options) {
        options.remove(option);
      }
    } else {
      synchronized (options) {
        options.put(option, value);
      }
    }
    return this;
  }

  /**
   * Allow to specify an initial attribute of the newly created {@link Http2StreamChannel}.  If the {@code value} is
   * {@code null}, the attribute of the specified {@code key} is removed.
   */
  public <T> EspressoHttp2StreamChannelBootstrap attr(AttributeKey<T> key, T value) {
    if (key == null) {
      throw new NullPointerException("key");
    }
    if (value == null) {
      synchronized (attrs) {
        attrs.remove(key);
      }
    } else {
      synchronized (attrs) {
        attrs.put(key, value);
      }
    }
    return this;
  }

  /**
   * the {@link ChannelHandler} to use for serving the requests.
   */
  public EspressoHttp2StreamChannelBootstrap handler(ChannelHandler handler) {
    this.handler = ObjectUtil.checkNotNull(handler, "handler");
    return this;
  }

  /**
   * Open a new {@link Http2StreamChannel} to use.
   * @return the {@link Future} that will be notified once the channel was opened successfully or it failed.
   */
  public Future<Http2StreamChannel> open() {
    return open(channel.eventLoop().newPromise(), null);
  }

  public Future<Http2StreamChannel> open(EventLoop childChannelEventLoop) {
    return open(channel.eventLoop().newPromise(), childChannelEventLoop);
  }

  /**
   * Open a new {@link Http2StreamChannel} to use and notifies the given {@link Promise}.
   * @return the {@link Future} that will be notified once the channel was opened successfully or it failed.
   */
  public Future<Http2StreamChannel> open(final Promise<Http2StreamChannel> promise, EventLoop childChannelEventLoop) {
    try {
      ChannelHandlerContext ctx = findCtx();
      EventExecutor executor = ctx.executor();
      if (executor.inEventLoop()) {
        open0(ctx, promise, childChannelEventLoop);
      } else {
        final ChannelHandlerContext finalCtx = ctx;
        executor.execute(() -> open0(finalCtx, promise, childChannelEventLoop));
      }
    } catch (Throwable cause) {
      promise.setFailure(cause);
    }
    return promise;
  }

  private ChannelHandlerContext findCtx() throws ClosedChannelException {
    // First try to use cached context and if this not work lets try to lookup the context.
    ChannelHandlerContext ctx = this.multiplexCtx;
    if (ctx != null && !ctx.isRemoved()) {
      return ctx;
    }
    ChannelPipeline pipeline = channel.pipeline();
    ctx = pipeline.context(EspressoHttp2MultiplexHandler.class);
    if (ctx == null) {
      ctx = pipeline.context(Http2MultiplexHandler.class);
    }
    if (ctx == null) {
      if (channel.isActive()) {
        throw new IllegalStateException(
            StringUtil.simpleClassName(Http2MultiplexHandler.class) + " or "
                + StringUtil.simpleClassName(EspressoHttp2MultiplexHandler.class)
                + " must be in the ChannelPipeline of Channel " + channel);
      } else {
        throw new ClosedChannelException();
      }
    }
    this.multiplexCtx = ctx;
    return ctx;
  }

  /**
   * @deprecated should not be used directly. Use {@link #open()} or {@link }
   */
  @Deprecated
  public void open0(
      ChannelHandlerContext ctx,
      final Promise<Http2StreamChannel> promise,
      EventLoop childChannelEventLoop) {
    assert ctx.executor().inEventLoop();
    if (!promise.setUncancellable()) {
      return;
    }
    final Http2StreamChannel streamChannel;
    if (ctx.handler() instanceof EspressoHttp2MultiplexHandler) {
      streamChannel = ((EspressoHttp2MultiplexHandler) ctx.handler()).newOutboundStream();
    } else {
      streamChannel = ((Http2MultiplexHandler) ctx.handler()).newOutboundStream();
    }
    try {
      init(streamChannel);
    } catch (Exception e) {
      streamChannel.unsafe().closeForcibly();
      promise.setFailure(e);
      return;
    }

    // Register the channel on either parent or offload to a different I/O worker.
    ChannelFuture future = (!offload || childChannelEventLoop == null)
        ? ctx.channel().eventLoop().register(streamChannel)
        : childChannelEventLoop.register(streamChannel);

    // ChannelFuture future = ctx.channel().eventLoop().register(streamChannel);
    future.addListener((ChannelFutureListener) future1 -> {
      if (future1.isSuccess()) {
        promise.setSuccess(streamChannel);
      } else if (future1.isCancelled()) {
        promise.cancel(false);
      } else {
        if (streamChannel.isRegistered()) {
          streamChannel.close();
        } else {
          streamChannel.unsafe().closeForcibly();
        }

        promise.setFailure(future1.cause());
      }
    });
  }

  @SuppressWarnings("unchecked")
  private void init(Channel channel) {
    ChannelPipeline p = channel.pipeline();
    EventExecutorGroup executorGroup = NettyUtils.executorGroup(channel);
    ChannelHandler handler = this.handler;
    if (handler != null) {
      p.addLast(executorGroup, handler);
    }
    synchronized (options) {
      setChannelOptions(channel, options);
    }

    synchronized (attrs) {
      for (Map.Entry<AttributeKey<?>, Object> e: attrs.entrySet()) {
        channel.attr((AttributeKey<Object>) e.getKey()).set(e.getValue());
      }
    }
  }

  private static void setChannelOptions(Channel channel, Map<ChannelOption<?>, Object> options) {
    for (Map.Entry<ChannelOption<?>, Object> e: options.entrySet()) {
      setChannelOption(channel, e.getKey(), e.getValue());
    }
  }

  @SuppressWarnings("unchecked")
  private static void setChannelOption(Channel channel, ChannelOption<?> option, Object value) {
    try {
      if (!channel.config().setOption((ChannelOption<Object>) option, value)) {
        logger.warn("Unknown channel option '{}' for channel '{}'", option, channel);
      }
    } catch (Throwable t) {
      logger.warn("Failed to set channel option '{}' with value '{}' for channel '{}'", option, value, channel, t);
    }
  }
}
