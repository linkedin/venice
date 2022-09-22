package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.CollectionUtil;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import javax.annotation.Nonnull;


/**
 * Created by acurtis on 3/19/18.
 */
@ChannelHandler.Sharable
public class RateLimitConnectHandler extends ChannelOutboundHandlerAdapter {
  private final ScheduledExecutorService _scheduledExecutorService;
  private final ConcurrentMap<InetAddress, Entry> _futures = new ConcurrentHashMap<>();
  private final LongSupplier _betweenConnectDelayMillis;
  private final LongSupplier _betweenFailureDelayMillis;
  private final LongSupplier _maxConnectDelayMillis;

  public RateLimitConnectHandler(
      @Nonnull ScheduledExecutorService scheduledExecutorService,
      long betweenConnectDelayMillis,
      long betweenFailureDelayMillis) {
    this(scheduledExecutorService, constant(betweenConnectDelayMillis), constant(betweenFailureDelayMillis));
  }

  public RateLimitConnectHandler(
      @Nonnull ScheduledExecutorService scheduledExecutorService,
      @Nonnull LongSupplier betweenConnectDelayMillis,
      @Nonnull LongSupplier betweenFailureDelayMillis) {
    this(scheduledExecutorService, betweenConnectDelayMillis, betweenFailureDelayMillis, constant(5000));
  }

  public RateLimitConnectHandler(
      @Nonnull ScheduledExecutorService scheduledExecutorService,
      @Nonnull LongSupplier betweenConnectDelayMillis,
      @Nonnull LongSupplier betweenFailureDelayMillis,
      @Nonnull LongSupplier maxConnectDelayMillis) {
    _scheduledExecutorService = scheduledExecutorService;
    _betweenConnectDelayMillis = betweenConnectDelayMillis;
    _betweenFailureDelayMillis = betweenFailureDelayMillis;
    _maxConnectDelayMillis = maxConnectDelayMillis;
  }

  private long getBetweenConnectDelayMillis() {
    return Math.max(0L, _betweenConnectDelayMillis.getAsLong());
  }

  private long getBetweenFailureDelayMillis() {
    return Math.max(0L, _betweenFailureDelayMillis.getAsLong());
  }

  private long getMaxConnectDelayMillis() {
    return Math.max(0, _maxConnectDelayMillis.getAsLong());
  }

  private void scheduleNext(Entry ref, boolean success, Connect next) {
    try {
      long delay;
      if (success) {
        delay = getBetweenConnectDelayMillis();
      } else {
        delay = ref._delay + getBetweenConnectDelayMillis()
            + ThreadLocalRandom.current().nextLong(getBetweenFailureDelayMillis());
      }
      ref._delay = Math.min(getMaxConnectDelayMillis(), delay);
    } finally {
      _scheduledExecutorService.schedule(next, Math.max(0, ref._delay), TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public void connect(
      ChannelHandlerContext ctx,
      SocketAddress remoteAddress,
      SocketAddress localAddress,
      ChannelPromise promise) throws Exception {
    Entry ref = CollectionUtil.computeIfAbsent(_futures, ((InetSocketAddress) remoteAddress).getAddress(), Entry::new);
    Connect future = new Connect(ref, ctx, remoteAddress, localAddress, promise);
    CompletableFuture<Void> oldValue = ref.get();
    while (!ref.compareAndSet(oldValue, future)) {
      oldValue = ref.get();
    }
    oldValue.whenCompleteAsync(future, ctx.executor());
  }

  private class Connect extends CompletableFuture<Void>
      implements Runnable, ChannelFutureListener, BiConsumer<Void, Throwable> {
    private final Entry _ref;
    private final ChannelHandlerContext _ctx;
    private final SocketAddress _remoteAddress;
    private final SocketAddress _localAddress;
    private final ChannelPromise _connectPromise;

    private Connect(
        Entry ref,
        ChannelHandlerContext ctx,
        SocketAddress remoteAddress,
        SocketAddress localAddress,
        ChannelPromise connectPromise) {
      _ref = ref;
      _ctx = ctx;
      _remoteAddress = remoteAddress;
      _localAddress = localAddress;
      _connectPromise = connectPromise;
    }

    @Override
    public void run() {
      complete(null);
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      try {
        scheduleNext(_ref, future.isSuccess(), this);
      } finally {
        try {
          _ctx.pipeline().remove(RateLimitConnectHandler.this);
        } catch (NoSuchElementException ex) {
          // ignore
        }
      }
    }

    @Override
    public void accept(Void aVoid, Throwable throwable) {
      try {
        _ctx.connect(_remoteAddress, _localAddress, _connectPromise);
        _connectPromise.addListener(this);
      } catch (Throwable ex) {
        scheduleNext(_ref, false, this);
        _connectPromise.tryFailure(ex);
      }
    }
  }

  private static class Entry extends AtomicReference<CompletableFuture<Void>> {
    final InetAddress _address;
    long _delay;

    public Entry(InetAddress address) {
      super(CompletableFuture.completedFuture(null));
      _address = address;
    }
  }

  private static LongSupplier constant(long value) {
    return () -> value;
  }
}
