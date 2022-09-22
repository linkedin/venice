package com.linkedin.alpini.netty4.pool;

import static io.netty.util.internal.ObjectUtil.*;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import io.netty.util.internal.ThrowableUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.atomic.LongAdder;


/**
 * Forked from Netty's SimpleChannelPool
 *
 * Simple {@link ChannelPool} implementation which will create new {@link Channel}s if someone tries to acquire
 * a {@link Channel} but none is in the pool atm. No limit on the maximal concurrent {@link Channel}s is enforced.
 *
 * This implementation uses FIFO order for {@link Channel}s in the {@link ChannelPool}.
 *
 */
public class FastSimpleChannelPool implements ChannelPool {
  private static final AttributeKey<FastSimpleChannelPool> POOL_KEY =
      AttributeKey.valueOf(FastSimpleChannelPool.class, "channelPool");
  private static final IllegalStateException FULL_EXCEPTION = ThrowableUtil.unknownStackTrace(
      new IllegalStateException("ChannelPool full"),
      FastSimpleChannelPool.class,
      "releaseAndOffer(...)");

  private static final InternalLogger LOG = InternalLoggerFactory.getInstance(FastSimpleChannelPool.class);

  private static final AtomicReferenceFieldUpdater<FastSimpleChannelPool, DualQueueEntry> ACQUIRE =
      AtomicReferenceFieldUpdater.newUpdater(FastSimpleChannelPool.class, DualQueueEntry.class, "_acquirePos");
  private static final AtomicReferenceFieldUpdater<FastSimpleChannelPool, DualQueueEntry> RELEASE =
      AtomicReferenceFieldUpdater.newUpdater(FastSimpleChannelPool.class, DualQueueEntry.class, "_releasePos");

  private volatile DualQueueEntry _acquirePos = new DualQueueEntry();
  private volatile DualQueueEntry _releasePos = _acquirePos;
  private final ChannelPoolHandler handler;
  private final ChannelHealthChecker healthCheck;
  private final Bootstrap bootstrap;
  private final boolean releaseHealthCheck;
  private final Semaphore connectSemaphore;
  private final LongAdder connectInProgress = new LongAdder();
  protected final ImmediateEventExecutor _immediateEventExecutor = ImmediateEventExecutor.INSTANCE;

  /**
   * Dual Queues, introduced by Scherer and Scott
   * (http://www.cs.rochester.edu/~scott/papers/2004_DISC_dual_DS.pdf)
   * are (linked) queues in which nodes may represent either data or
   * requests.  When a thread tries to enqueue a data node, but
   * encounters a request node, it instead "matches" and removes it;
   * and vice versa for enqueuing requests.
   */
  private static final class DualQueueEntry extends AtomicReference<DualQueueEntry> {
    private volatile Channel _channel;
    private volatile Promise<Channel> _promise;

    DualQueueEntry() {
    }

    DualQueueEntry(Channel channel) {
      _channel = channel;
    }

    DualQueueEntry(Promise<Channel> promise) {
      _promise = promise;
    }

    Channel channel() {
      return _channel;
    }

    Promise<Channel> promise() {
      return _promise;
    }

    boolean acquire(Channel channel) {
      return CHANNEL.compareAndSet(this, channel, null);
    }

    boolean acquire(Promise<Channel> promise) {
      return PROMISE.compareAndSet(this, promise, null);
    }

    private static final AtomicReferenceFieldUpdater<DualQueueEntry, Channel> CHANNEL =
        AtomicReferenceFieldUpdater.newUpdater(DualQueueEntry.class, Channel.class, "_channel");
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static final AtomicReferenceFieldUpdater<DualQueueEntry, Promise<Channel>> PROMISE =
        AtomicReferenceFieldUpdater
            .newUpdater(DualQueueEntry.class, (Class<Promise<Channel>>) (Class) Promise.class, "_promise");
  }

  /**
   * Creates a new instance using the {@link ChannelHealthChecker#ACTIVE}.
   *
   * @param bootstrap         the {@link Bootstrap} that is used for connections
   * @param handler           the {@link ChannelPoolHandler} that will be notified for the different pool actions
   */
  public FastSimpleChannelPool(Bootstrap bootstrap, final ChannelPoolHandler handler) {
    this(bootstrap, handler, ChannelHealthChecker.ACTIVE);
  }

  /**
   * Creates a new instance.
   *
   * @param bootstrap         the {@link Bootstrap} that is used for connections
   * @param handler           the {@link ChannelPoolHandler} that will be notified for the different pool actions
   * @param healthCheck       the {@link ChannelHealthChecker} that will be used to check if a {@link Channel} is
   *                          still healthy when obtain from the {@link ChannelPool}
   */
  public FastSimpleChannelPool(
      Bootstrap bootstrap,
      final ChannelPoolHandler handler,
      ChannelHealthChecker healthCheck) {
    this(bootstrap, handler, healthCheck, true);
  }

  /**
   * Creates a new instance.
   *
   * @param bootstrap          the {@link Bootstrap} that is used for connections
   * @param handler            the {@link ChannelPoolHandler} that will be notified for the different pool actions
   * @param healthCheck        the {@link ChannelHealthChecker} that will be used to check if a {@link Channel} is
   *                           still healthy when obtain from the {@link ChannelPool}
   * @param releaseHealthCheck will check channel health before offering back if this parameter set to {@code true};
   *                           otherwise, channel health is only checked at acquisition time
   */
  public FastSimpleChannelPool(
      Bootstrap bootstrap,
      final ChannelPoolHandler handler,
      ChannelHealthChecker healthCheck,
      boolean releaseHealthCheck) {
    this(bootstrap, handler, healthCheck, releaseHealthCheck, 1);
  }

  /**
   * Creates a new instance.
   *
   * @param bootstrap          the {@link Bootstrap} that is used for connections
   * @param handler            the {@link ChannelPoolHandler} that will be notified for the different pool actions
   * @param healthCheck        the {@link ChannelHealthChecker} that will be used to check if a {@link Channel} is
   *                           still healthy when obtain from the {@link ChannelPool}
   * @param releaseHealthCheck will check channel health before offering back if this parameter set to {@code true};
   *                           otherwise, channel health is only checked at acquisition time
   * @param connectConcurrency the number of concurrent {@literal bootstrap.connect()} calls permitted
   */
  public FastSimpleChannelPool(
      Bootstrap bootstrap,
      final ChannelPoolHandler handler,
      ChannelHealthChecker healthCheck,
      boolean releaseHealthCheck,
      int connectConcurrency) {
    this.handler = checkNotNull(handler, "handler");
    this.healthCheck = checkNotNull(healthCheck, "healthCheck");
    this.releaseHealthCheck = releaseHealthCheck;
    // Clone the original Bootstrap as we want to set our own handler
    this.bootstrap = checkNotNull(bootstrap, "bootstrap").clone();
    this.bootstrap.handler(new ChannelInitializer<Channel>() {
      @Override
      protected void initChannel(Channel ch) throws Exception {
        assert ch.eventLoop().inEventLoop();
        handler.channelCreated(ch);
      }
    });
    this.connectSemaphore = new Semaphore(connectConcurrency);
  }

  /**
   * Counts the number of idle channels available within the pool.
   * @return available channels
   */
  protected int getAvailableChannels() {
    int count = 0;
    for (DualQueueEntry entry = _acquirePos; entry != null; entry = entry.get()) {
      if (entry._channel == null) {
        continue;
      }
      count++;
    }
    return count;
  }

  /**
   * Returns the {@link Bootstrap} this pool will use to open new connections.
   *
   * @return the {@link Bootstrap} this pool will use to open new connections
   */
  protected Bootstrap bootstrap() {
    return bootstrap;
  }

  /**
   * Returns the {@link ChannelPoolHandler} that will be notified for the different pool actions.
   *
   * @return the {@link ChannelPoolHandler} that will be notified for the different pool actions
   */
  protected ChannelPoolHandler handler() {
    return handler;
  }

  /**
   * Returns the {@link ChannelHealthChecker} that will be used to check if a {@link Channel} is healthy.
   *
   * @return the {@link ChannelHealthChecker} that will be used to check if a {@link Channel} is healthy
   */
  protected ChannelHealthChecker healthChecker() {
    return healthCheck;
  }

  /**
   * Indicates whether this pool will check the health of channels before offering them back into the pool.
   *
   * @return {@code true} if this pool will check the health of channels before offering them back into the pool, or
   * {@code false} if channel health is only checked at acquisition time
   */
  protected boolean releaseHealthCheck() {
    return releaseHealthCheck;
  }

  @Override
  public final Future<Channel> acquire() {
    return acquire(_immediateEventExecutor.<Channel>newPromise());
  }

  @Override
  public Future<Channel> acquire(final Promise<Channel> promise) {
    checkNotNull(promise, "promise");
    return acquireHealthyFromPoolOrNew(promise);
  }

  /**
   * Tries to retrieve healthy channel from the pool if any or creates a new channel otherwise.
   * @param promise the promise to provide acquire result.
   * @return future for acquiring a channel.
   */
  private Future<Channel> acquireHealthyFromPoolOrNew(final Promise<Channel> promise) {
    try {
      final Channel ch = pollChannel();
      if (ch == null) {
        // No Channel left in the pool
        if (addWaiter(promise)) {
          return promise;
        }
        if (promise.isDone() || !acquireConnectSemaphore()) {
          return promise;
        }
        // bootstrap a new Channel
        return bootstrapChannel(promise);
      }
      doHealthCheck(ch, promise);
    } catch (Throwable cause) {
      promise.tryFailure(cause);
    }
    return promise;
  }

  protected final int connectInProgress() {
    return connectInProgress.intValue();
  }

  protected boolean acquireConnectSemaphore() {
    if (connectSemaphore.tryAcquire()) {
      connectInProgress.increment();
      return true;
    }
    return false;
  }

  protected Future<Channel> bootstrapChannel(Promise<Channel> promise) throws Exception {
    Bootstrap bs = bootstrap.clone();
    bs.attr(POOL_KEY, this);
    Future<Channel> f = connectChannel(bs, _immediateEventExecutor.newPromise());
    if (f.isDone()) {
      notifyConnect(f, promise);
    } else {
      f.addListener(new FutureListener<Channel>() {
        @Override
        public void operationComplete(Future<Channel> future) throws Exception {
          notifyConnect(future, promise);
        }
      });
    }
    return promise;
  }

  protected void onConnect(Channel channel, Promise<Channel> promise) {
  }

  protected final void releaseConnectSemaphore() {
    connectSemaphore.release();
    connectInProgress.decrement();
  }

  private void notifyConnect(Future<Channel> future, Promise<Channel> promise) throws Exception {
    releaseConnectSemaphore();
    if (future.isSuccess()) {
      Channel channel = future.getNow();
      onConnect(channel, promise);
      if (promise.isDone()) {
        if (offerChannel(channel)) {
          return;
        }
        closeChannel(channel);
        return;
      }
      try {
        promise.setUncancellable();
        handler.channelAcquired(channel);
        if (!promise.trySuccess(channel)) {
          // Promise was completed in the meantime (like cancelled), just release the channel again
          release(channel);
        }
      } catch (Throwable ex) {
        closeAndFail(channel, ex, promise);
      }
    } else {
      onConnect(null, promise);
      if (!promise.tryFailure(future.cause())) {
        LOG.warn("Failure in connect", future.cause());
      }
    }
  }

  private void doHealthCheck(final Channel ch, final Promise<Channel> promise) {
    Future<Boolean> f = healthCheck.isHealthy(ch);
    if (f.isDone()) {
      notifyHealthCheck(f, ch, promise);
    } else {
      f.addListener(new FutureListener<Boolean>() {
        @Override
        public void operationComplete(Future<Boolean> future) throws Exception {
          notifyHealthCheck(future, ch, promise);
        }
      });
    }
  }

  private void notifyHealthCheck(Future<Boolean> future, Channel ch, Promise<Channel> promise) {
    if (future.isSuccess()) {
      if (Boolean.TRUE.equals(future.getNow())) {
        if (promise.isDone()) {
          if (offerChannel(ch)) {
            return;
          }
          closeChannel(ch);
        }
        try {
          ch.attr(POOL_KEY).set(this);
          handler.channelAcquired(ch);
          if (!promise.trySuccess(ch)) {
            handler.channelReleased(ch);
            tryOffer(ch, _immediateEventExecutor.newPromise());
          }
        } catch (Throwable cause) {
          closeAndFail(ch, cause, promise);
        }
      } else {
        closeChannel(ch);
        acquireHealthyFromPoolOrNew(promise);
      }
    } else {
      closeChannel(ch);
      acquireHealthyFromPoolOrNew(promise);
    }
  }

  /**
   * Bootstrap a new {@link Channel}. The default implementation uses {@link Bootstrap#connect()}, sub-classes may
   * override this.
   * <p>
   * The {@link Bootstrap} that is passed in here is cloned via {@link Bootstrap#clone()}, so it is safe to modify.
   * @return
   */
  protected Future<Channel> connectChannel(Bootstrap bs, Promise<Channel> promise) {
    if (!promise.isDone()) {
      ChannelFuture future = bs.connect();
      if (future.isDone()) {
        return connectChannel0(future, promise);
      } else {
        future.addListener((ChannelFuture connected) -> connectChannel0(connected, promise));
      }
    }
    return promise;
  }

  private Future<Channel> connectChannel0(ChannelFuture future, Promise<Channel> promise) {
    if (future.isSuccess()) {
      if (promise.trySuccess(future.channel())) {
        return promise;
      }
      closeChannel(future.channel());
      return promise;
    } else {
      return promise.setFailure(future.cause());
    }
  }

  @Override
  public final Future<Void> release(Channel channel) {
    return release(channel, _immediateEventExecutor.<Void>newPromise());
  }

  @Override
  public Future<Void> release(final Channel channel, final Promise<Void> promise) {
    checkNotNull(channel, "channel");
    checkNotNull(promise, "promise");
    try {
      doReleaseChannel(channel, promise);
    } catch (Throwable cause) {
      closeAndFail(channel, cause, promise);
    }
    return promise;
  }

  private void doReleaseChannel(Channel channel, Promise<Void> promise) {
    // Remove the POOL_KEY attribute from the Channel and check if it was acquired from this pool, if not fail.
    if (channel.attr(POOL_KEY).getAndSet(null) != this) {
      closeAndFail(
          channel,
          // Better include a stacktrace here as this is an user error.
          new IllegalArgumentException("Channel " + channel + " was not acquired from this ChannelPool"),
          promise);
    } else {
      try {
        handler.channelReleased(channel);
        if (releaseHealthCheck) {
          doHealthCheckOnRelease(channel, promise);
        } else {
          tryOffer(channel, promise);
        }
      } catch (Throwable cause) {
        closeAndFail(channel, cause, promise);
      }
    }
  }

  private void doHealthCheckOnRelease(final Channel channel, final Promise<Void> promise) {
    final Future<Boolean> f = healthCheck.isHealthy(channel);
    if (f.isDone()) {
      offerIfHealthy(channel, promise, f);
    } else {
      f.addListener(new FutureListener<Boolean>() {
        @Override
        public void operationComplete(Future<Boolean> future) throws Exception {
          offerIfHealthy(channel, promise, future);
        }
      });
    }
  }

  /**
   * Adds the channel back to the pool only if the channel is healthy.
   * @param channel the channel to put back to the pool
   * @param promise offer operation promise.
   * @param future the future that contains information fif channel is healthy or not.
   * @throws Exception in case when failed to notify handler about release operation.
   */
  private void offerIfHealthy(Channel channel, Promise<Void> promise, Future<Boolean> future) {
    if (future.isSuccess() && Boolean.TRUE.equals(future.getNow())) { // channel turns out to be healthy, offering and
                                                                      // releasing it.
      tryOffer(channel, promise);
    } else { // channel not healthy, just releasing it.
      if (future.isSuccess()) {
        if (!channel.closeFuture().isDone()) {
          LOG.warn("Closing unhealthy channel: {}", channel);
          closeChannel(channel).addListener(f -> promise.setSuccess(null));
        } else {
          promise.setSuccess(null);
        }
      } else {
        closeAndFail(channel, future.cause(), promise);
      }
    }
  }

  private void tryOffer(Channel channel, Promise<Void> promise) {
    try {
      if (offerChannel(channel)) {
        promise.setSuccess(null);
        return;
      }
    } catch (Throwable ex) {
      LOG.error("Unexpected", ex);
      closeAndFail(channel, ex, promise);
      return;
    }
    closeAndFail(channel, FULL_EXCEPTION, promise);
  }

  protected static ChannelFuture closeChannel(Channel channel) {
    channel.attr(POOL_KEY).getAndSet(null);
    return channel.close();
  }

  private static void closeAndFail(Channel channel, Throwable cause, Promise<?> promise) {
    closeChannel(channel).addListener(closed -> promise.tryFailure(cause));
  }

  /**
   * Poll a {@link Channel} out of the internal storage to reuse it. This will return {@code null} if no
   * {@link Channel} is ready to be reused.
   *
   * Sub-classes may override {@link #pollChannel()} and {@link #offerChannel(Channel)}. Be aware that
   * implementations of these methods needs to be thread-safe!
   */
  protected Channel pollChannel() {
    for (DualQueueEntry pos = _acquirePos; pos != null; pos = pos.get()) {
      Channel channel = pos.channel();
      if (channel != null && pos.acquire(channel)) {
        channelAcquiredFromPool(pos, channel);
        return channel;
      }
    }
    return null;
  }

  private void channelAcquiredFromPool(DualQueueEntry pos, Channel channel) {
    DualQueueEntry next = pos.get();
    ACQUIRE.lazySet(this, next != null ? next : pos);
  }

  protected boolean addWaiter(Promise<Channel> promise) {
    DualQueueEntry entry =
        new DualQueueEntry(_immediateEventExecutor.<Channel>newPromise().addListener((Future<Channel> future) -> {
          if (future.isSuccess()) {
            if (promise.isDone()) {
              if (offerChannel(future.getNow())) {
                return;
              }
              closeChannel(future.getNow());
            } else {
              doHealthCheck(future.getNow(), promise);
            }
          } else {
            promise.setFailure(future.cause());
          }
        }));
    DualQueueEntry pos = _acquirePos;
    for (;;) {
      Channel channel = pos.channel();
      if (channel != null && pos.acquire(channel)) {
        channelAcquiredFromPool(pos, channel);
        doHealthCheck(channel, promise);
        return true;
      }
      DualQueueEntry next = pos.get();
      if (next == null) {
        if (pos.compareAndSet(null, entry)) {
          ACQUIRE.lazySet(this, entry);
          break;
        }
        continue;
      }
      pos = next;
    }
    return false;
  }

  /**
   * Offer a {@link Channel} back to the internal storage. This will return {@code true} if the {@link Channel}
   * could be added, {@code false} otherwise.
   *
   * Sub-classes may override {@link #pollChannel()} and {@link #offerChannel(Channel)}. Be aware that
   * implementations of these methods needs to be thread-safe!
   */
  protected boolean offerChannel(Channel channel) {
    DualQueueEntry entry = null;
    for (DualQueueEntry pos = _releasePos;;) {
      Promise<Channel> promise = pos.promise();
      if (promise != null && pos.acquire(promise)) {
        DualQueueEntry next = pos.get();
        RELEASE.lazySet(this, next != null ? next : pos);
        if (promise.trySuccess(channel)) {
          return true;
        }
      }
      DualQueueEntry next = pos.get();
      if (next == null) {
        if (entry == null) {
          entry = new DualQueueEntry(channel);
        }
        if (pos.compareAndSet(null, entry)) {
          RELEASE.lazySet(this, entry);
          return true;
        }
        continue;
      }
      pos = next;
    }
  }

  @Override
  public void close() {
    for (;;) {
      Channel channel = pollChannel();
      if (channel == null) {
        break;
      }
      channel.close();
    }
  }
}
