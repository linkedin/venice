package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.TypedFieldAccessor;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A special {@link io.netty.channel.ChannelInboundHandler} which offers an easy way to initialize a {@link Channel} once it was
 * registered to its {@link io.netty.channel.EventLoop}.
 *
 * Implementations are most often used in the context of {@link io.netty.bootstrap.Bootstrap#handler(io.netty.channel.ChannelHandler)} ,
 * {@link io.netty.bootstrap.ServerBootstrap#handler(io.netty.channel.ChannelHandler)} and
 * {@link io.netty.bootstrap.ServerBootstrap#childHandler(io.netty.channel.ChannelHandler)} to
 * setup the {@link io.netty.channel.ChannelPipeline} of a {@link Channel}.
 *
 * <pre>
 *
 * public class MyChannelInitializer extends {@link io.netty.channel.ChannelInitializer} {
 *     public void initChannel({@link Channel} channel) {
 *         channel.pipeline().addLast("myHandler", new MyHandler());
 *     }
 * }
 *
 * {@link io.netty.bootstrap.ServerBootstrap} bootstrap = ...;
 * ...
 * bootstrap.childHandler(new MyChannelInitializer());
 * ...
 * </pre>
 * Be aware that this class is marked as {@link Sharable} and so the implementation must be safe to be re-used.
 *
 * <p>Note that this contains a workaround for https://github.com/netty/netty/issues/8616</p>
 *
 * @param <C>   A sub-type of {@link Channel}
 */
@Sharable
public abstract class ChannelInitializer<C extends Channel> extends ChannelInboundHandlerAdapter {
  private static final ThreadLocal<ChannelHandlerContext> CURRENT_CONTEXT = new ThreadLocal<>();

  private static final Logger logger = LogManager.getLogger(ChannelInitializer.class);

  // Use a WeakHashMap so that if the Channel was discarded via exception etc, we don't leak ChannelHandlerContex.
  // Since creating channels does not occur frequently, having the Set synchronized should not cause contention.
  private final Set<ChannelHandlerContext> initMap =
      Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));

  /**
   * This method will be called once the {@link Channel} was registered. After the method returns this instance
   * will be removed from the {@link io.netty.channel.ChannelPipeline} of the {@link Channel}.
   *
   * @param ch            the {@link Channel} which was registered.
   * @throws Exception    is thrown if an error occurs. In that case it will be handled by
   *                      {@link #exceptionCaught(ChannelHandlerContext, Throwable)} which will by default close
   *                      the {@link Channel}.
   */
  protected abstract void initChannel(C ch) throws Exception;

  @Override
  @SuppressWarnings("unchecked")
  public final void channelRegistered(ChannelHandlerContext ctx) throws Exception {
    // Normally this method will never be called as handlerAdded(...) should call initChannel(...) and remove
    // the handler.
    if (!isRemoved(ctx) && initChannel(ctx)) {
      // we called initChannel(...) so we need to call now pipeline.fireChannelRegistered() to ensure we not
      // miss an event.
      ctx.pipeline().fireChannelRegistered();
    } else {
      // Called initChannel(...) before which is the expected behavior, so just forward the event.
      ctx.fireChannelRegistered();
    }
  }

  /**
   * Handle the {@link Throwable} by logging and closing the {@link Channel}. Sub-classes may override this.
   */
  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    logger.warn("Failed to initialize a channel. Closing: {}", ctx.channel(), cause);
    ctx.close();
  }

  /**
   * {@inheritDoc} If override this method ensure you call super!
   */
  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    if (!isRemoved(ctx) && ctx.channel().isRegistered()) {
      // This should always be true with our current DefaultChannelPipeline implementation.
      // The good thing about calling initChannel(...) in handlerAdded(...) is that there will be no ordering
      // surprises if a ChannelInitializer will add another ChannelInitializer. This is as all handlers
      // will be added in the expected order.
      initChannel(ctx);
    }
  }

  /**
   * {@inheritDoc} If override this method ensure you call super!
   */
  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    initMap.remove(ctx);
  }

  @SuppressWarnings("unchecked")
  private boolean initChannel(ChannelHandlerContext ctx) throws Exception {
    if (initMap.add(ctx)) { // Guard against re-entrance.
      try {
        CURRENT_CONTEXT.set(ctx);
        initChannel((C) ctx.channel());
      } catch (Throwable cause) {
        // Explicitly call exceptionCaught(...) as we removed the handler before calling initChannel(...).
        // We do so to prevent multiple calls to initChannel(...).
        exceptionCaught(ctx, cause);
      } finally {
        remove(ctx);
        CURRENT_CONTEXT.set(null);
      }
      return true;
    }
    return false;
  }

  private void remove(ChannelHandlerContext ctx) {
    try {
      if (!isRemoved(ctx)) {
        ctx.pipeline().remove(this);
      }
    } finally {
      initMap.remove(ctx);
    }
  }

  protected ChannelHandlerContext currentContext() {
    return CURRENT_CONTEXT.get();
  }

  private static boolean isRemoved(ChannelHandlerContext ctx) {
    if (ctx.isRemoved()) {
      return true;
    }

    assert "DefaultChannelHandlerContext".equals(ctx.getClass().getSimpleName());

    if (_prevAccessor == null) {
      // noinspection unchecked
      _nextAccessor =
          TypedFieldAccessor.forField((Class<ChannelHandlerContext>) ctx.getClass().getSuperclass(), "next");
      // noinspection unchecked
      _prevAccessor =
          TypedFieldAccessor.forField((Class<ChannelHandlerContext>) ctx.getClass().getSuperclass(), "prev");
    }

    // if the ctx->next->prev == ctx->prev, then we know our ctx has already been removed

    ChannelHandlerContext next = _nextAccessor.apply(ctx);
    ChannelHandlerContext prev = _prevAccessor.apply(ctx);
    ChannelHandlerContext nextPrev = _prevAccessor.apply(next);
    return prev == nextPrev;
  }

  private static volatile Function<ChannelHandlerContext, ChannelHandlerContext> _prevAccessor;
  private static volatile Function<ChannelHandlerContext, ChannelHandlerContext> _nextAccessor;
}
