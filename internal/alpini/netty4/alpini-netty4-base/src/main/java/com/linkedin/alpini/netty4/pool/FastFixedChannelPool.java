package com.linkedin.alpini.netty4.pool;

import com.linkedin.alpini.base.misc.PromiseDelegate;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.PlatformDependent;
import java.util.Queue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Forked from Netty's FixedChannelPool
 *
 * {ChannelPool} implementation that takes another {ChannelPool} implementation and enforce a maximum
 * number of concurrent connections.
 */
public class FastFixedChannelPool extends FastSimpleChannelPool implements ChannelPoolWithStats {
  private static final Logger LOG = LogManager.getLogger(FixedChannelPoolImpl.class);

  private static final AtomicReferenceFieldUpdater<FastFixedChannelPool, Boolean> CLOSED =
      AtomicReferenceFieldUpdater.newUpdater(FastFixedChannelPool.class, Boolean.class, "closed");
  private static final AtomicIntegerFieldUpdater<FastFixedChannelPool> CONNECT_IN_FLIGHT =
      AtomicIntegerFieldUpdater.newUpdater(FastFixedChannelPool.class, "_connectInFlight");
  private static final AttributeKey<FastFixedChannelPool> CHANNEL_IS_ACQUIRED =
      AttributeKey.valueOf(FastFixedChannelPool.class, "acquired");

  private final String _name;
  private final long acquireTimeoutNanos;
  private final TimeoutTask timeoutTask;
  protected final ChannelGroup _channelGroup;

  /** Contains runnables to call connect which would have exceeded maxConnections, normally empty. */
  private final Queue<Runnable> _pendingConnect = PlatformDependent.newConcurrentDeque();
  private volatile int _connectInFlight;

  private IntSupplier maxConnections;
  private final int maxPendingAcquires;
  private final LongAdder acquiredChannelCount = new LongAdder();
  private final LongAdder pendingAcquireCount = new LongAdder();
  volatile private Boolean closed = false;

  public boolean isPoolClosed() {
    return closed;
  }

  /**
   * Creates a new instance using the {@link ChannelHealthChecker#ACTIVE}.
   *
   * @param bootstrap         the {@link Bootstrap} that is used for connections
   * @param handler           the {@link ChannelPoolHandler} that will be notified for the different pool actions
   * @param maxConnections    the number of maximal active connections, once this is reached new tries to acquire
   *                          a {@link Channel} will be delayed until a connection is returned to the pool again.
   */
  public FastFixedChannelPool(Bootstrap bootstrap, ChannelPoolHandler handler, int maxConnections) {
    this(bootstrap, handler, maxConnections, Integer.MAX_VALUE);
  }

  /**
   * Creates a new instance using the {@link ChannelHealthChecker#ACTIVE}.
   *
   * @param bootstrap             the {@link Bootstrap} that is used for connections
   * @param handler               the {@link ChannelPoolHandler} that will be notified for the different pool actions
   * @param maxConnections        the number of maximal active connections, once this is reached new tries to
   *                              acquire a {@link Channel} will be delayed until a connection is returned to the
   *                              pool again.
   * @param maxPendingAcquires    the maximum number of pending acquires. Once this is exceed acquire tries will
   *                              be failed.
   */
  public FastFixedChannelPool(
      Bootstrap bootstrap,
      ChannelPoolHandler handler,
      int maxConnections,
      int maxPendingAcquires) {
    this(bootstrap, handler, ChannelHealthChecker.ACTIVE, null, -1, maxConnections, maxPendingAcquires);
  }

  /**
   * Creates a new instance.
   *
   * @param bootstrap             the {@link Bootstrap} that is used for connections
   * @param handler               the {@link ChannelPoolHandler} that will be notified for the different pool actions
   * @param healthCheck           the {@link ChannelHealthChecker} that will be used to check if a {@link Channel} is
   *                              still healthy when obtain from the {ChannelPool}
   * @param action                the {@link io.netty.channel.pool.FixedChannelPool.AcquireTimeoutAction} to use or {@code null} if non should be used.
   *                              In this case {@param acquireTimeoutMillis} must be {@code -1}.
   * @param acquireTimeoutMillis  the time (in milliseconds) after which an pending acquire must complete or
   *                              the {@link io.netty.channel.pool.FixedChannelPool.AcquireTimeoutAction} takes place.
   * @param maxConnections        the number of maximal active connections, once this is reached new tries to
   *                              acquire a {@link Channel} will be delayed until a connection is returned to the
   *                              pool again.
   * @param maxPendingAcquires    the maximum number of pending acquires. Once this is exceed acquire tries will
   *                              be failed.
   */
  public FastFixedChannelPool(
      Bootstrap bootstrap,
      ChannelPoolHandler handler,
      ChannelHealthChecker healthCheck,
      io.netty.channel.pool.FixedChannelPool.AcquireTimeoutAction action,
      final long acquireTimeoutMillis,
      int maxConnections,
      int maxPendingAcquires) {
    this(bootstrap, handler, healthCheck, action, acquireTimeoutMillis, maxConnections, maxPendingAcquires, true);
  }

  /**
   * Creates a new instance.
   *
   * @param bootstrap             the {@link Bootstrap} that is used for connections
   * @param handler               the {@link ChannelPoolHandler} that will be notified for the different pool actions
   * @param healthCheck           the {@link ChannelHealthChecker} that will be used to check if a {@link Channel} is
   *                              still healthy when obtain from the {ChannelPool}
   * @param action                the {@link io.netty.channel.pool.FixedChannelPool.AcquireTimeoutAction} to use or {@code null} if non should be used.
   *                              In this case {@literal acquireTimeoutMillis} must be {@code -1}.
   * @param acquireTimeoutMillis  the time (in milliseconds) after which an pending acquire must complete or
   *                              the {@link io.netty.channel.pool.FixedChannelPool.AcquireTimeoutAction} takes place.
   * @param maxConnections        the number of maximal active connections, once this is reached new tries to
   *                              acquire a {@link Channel} will be delayed until a connection is returned to the
   *                              pool again.
   * @param maxPendingAcquires    the maximum number of pending acquires. Once this is exceed acquire tries will
   *                              be failed.
   * @param releaseHealthCheck    will check channel health before offering back if this parameter set to
   *                              {@code true}.
   */
  public FastFixedChannelPool(
      Bootstrap bootstrap,
      ChannelPoolHandler handler,
      ChannelHealthChecker healthCheck,
      io.netty.channel.pool.FixedChannelPool.AcquireTimeoutAction action,
      final long acquireTimeoutMillis,
      int maxConnections,
      int maxPendingAcquires,
      final boolean releaseHealthCheck) {
    this(
        bootstrap,
        handler,
        healthCheck,
        action,
        acquireTimeoutMillis,
        maxConnections,
        maxPendingAcquires,
        releaseHealthCheck,
        1);
  }

  /**
   * Creates a new instance.
   *
   * @param bootstrap             the {@link Bootstrap} that is used for connections
   * @param handler               the {@link ChannelPoolHandler} that will be notified for the different pool actions
   * @param healthCheck           the {@link ChannelHealthChecker} that will be used to check if a {@link Channel} is
   *                              still healthy when obtain from the {ChannelPool}
   * @param action                the {@link io.netty.channel.pool.FixedChannelPool.AcquireTimeoutAction} to use or {@code null} if non should be used.
   *                              In this case {@literal acquireTimeoutMillis} must be {@code -1}.
   * @param acquireTimeoutMillis  the time (in milliseconds) after which an pending acquire must complete or
   *                              the {@link io.netty.channel.pool.FixedChannelPool.AcquireTimeoutAction} takes place.
   * @param maxConnections        the number of maximal active connections, once this is reached new tries to
   *                              acquire a {@link Channel} will be delayed until a connection is returned to the
   *                              pool again.
   * @param maxPendingAcquires    the maximum number of pending acquires. Once this is exceed acquire tries will
   *                              be failed.
   * @param releaseHealthCheck    will check channel health before offering back if this parameter set to
   *                              {@code true}.
   * @param connectConcurrency    the number of concurrent {@literal bootstrap.connect()} calls permitted
   */
  public FastFixedChannelPool(
      Bootstrap bootstrap,
      ChannelPoolHandler handler,
      ChannelHealthChecker healthCheck,
      io.netty.channel.pool.FixedChannelPool.AcquireTimeoutAction action,
      final long acquireTimeoutMillis,
      int maxConnections,
      int maxPendingAcquires,
      boolean releaseHealthCheck,
      int connectConcurrency) {
    this(
        bootstrap,
        handler,
        healthCheck,
        action,
        acquireTimeoutMillis,
        () -> maxConnections,
        maxPendingAcquires,
        releaseHealthCheck,
        connectConcurrency);
  }

  /**
   * Creates a new instance.
   *
   * @param bootstrap             the {@link Bootstrap} that is used for connections
   * @param handler               the {@link ChannelPoolHandler} that will be notified for the different pool actions
   * @param healthCheck           the {@link ChannelHealthChecker} that will be used to check if a {@link Channel} is
   *                              still healthy when obtain from the {ChannelPool}
   * @param action                the {@link io.netty.channel.pool.FixedChannelPool.AcquireTimeoutAction} to use or {@code null} if non should be used.
   *                              In this case {@literal acquireTimeoutMillis} must be {@code -1}.
   * @param acquireTimeoutMillis  the time (in milliseconds) after which an pending acquire must complete or
   *                              the {@link io.netty.channel.pool.FixedChannelPool.AcquireTimeoutAction} takes place.
   * @param maxConnections        the number of maximal active connections, once this is reached new tries to
   *                              acquire a {@link Channel} will be delayed until a connection is returned to the
   *                              pool again.
   * @param maxPendingAcquires    the maximum number of pending acquires. Once this is exceed acquire tries will
   *                              be failed.
   * @param releaseHealthCheck    will check channel health before offering back if this parameter set to
   *                              {@code true}.
   * @param connectConcurrency    the number of concurrent {@literal bootstrap.connect()} calls permitted
   */
  public FastFixedChannelPool(
      Bootstrap bootstrap,
      ChannelPoolHandler handler,
      ChannelHealthChecker healthCheck,
      io.netty.channel.pool.FixedChannelPool.AcquireTimeoutAction action,
      final long acquireTimeoutMillis,
      IntSupplier maxConnections,
      int maxPendingAcquires,
      boolean releaseHealthCheck,
      int connectConcurrency) {
    super(bootstrap, handler, healthCheck, releaseHealthCheck, connectConcurrency);
    if (maxConnections.getAsInt() < 1) {
      throw new IllegalArgumentException("maxConnections: " + maxConnections.getAsInt() + " (expected: >= 1)");
    }
    if (maxPendingAcquires < 1) {
      throw new IllegalArgumentException("maxPendingAcquires: " + maxPendingAcquires + " (expected: >= 1)");
    }
    if (action == null && acquireTimeoutMillis == -1) {
      timeoutTask = null;
      acquireTimeoutNanos = -1;
    } else if (action == null && acquireTimeoutMillis != -1) {
      throw new NullPointerException("action");
    } else if (action != null && acquireTimeoutMillis < 0) {
      throw new IllegalArgumentException("acquireTimeoutMillis: " + acquireTimeoutMillis + " (expected: >= 0)");
    } else {
      acquireTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(acquireTimeoutMillis);
      switch (action) {
        case FAIL:
          timeoutTask = new TimeoutTask() {
            private final TimeoutException _timeoutException =
                new TimeoutException("Acquire operation took longer then configured maximum time") {
                  @Override
                  public synchronized Throwable fillInStackTrace() {
                    return this;
                  }
                };

            @Override
            public void onTimeout(AcquireTask task) {
              // Fail the promise as we timed out.
              task.tryFailure(_timeoutException);
            }
          };
          break;
        case NEW:
          timeoutTask = new TimeoutTask() {
            @Override
            public void onTimeout(AcquireTask task) {
              // If there is any pending connect operation, allow it to execute even if it may
              // cause the number of connections to exceed maxConnections.
              Runnable pending = _pendingConnect.poll();
              if (pending != null) {
                pending.run();
              }
            }
          };
          break;
        default:
          throw new Error();
      }
    }
    this.maxConnections = maxConnections;
    this.maxPendingAcquires = maxPendingAcquires;
    _name = bootstrap.config().remoteAddress().toString();
    _channelGroup = newPoolChannelGroup();
  }

  @Override
  public String name() {
    return _name;
  }

  public void setMaxConnections(IntSupplier maxConnections) {
    this.maxConnections = maxConnections;
  }

  protected PoolChannelGroup newPoolChannelGroup() {
    return new PoolChannelGroup(name());
  }

  protected class PoolChannelGroup extends DefaultChannelGroup {
    protected PoolChannelGroup(String name) {
      super(name, _immediateEventExecutor, true);
    }

    @Override
    public boolean remove(Object o) {
      if (super.remove(o)) {
        if (!_pendingConnect.isEmpty() && size() + _connectInFlight < getMaxConnections()) {
          Runnable task = _pendingConnect.poll();
          if (task != null) {
            task.run();
          }
        }
        return true;
      }
      return false;
    }
  }

  /** Returns the number of acquired channels that this pool thinks it has. */
  public int acquiredChannelCount() {
    return acquiredChannelCount.intValue();
  }

  private void incrementAcquiredChannelCount() {
    acquiredChannelCount.increment();
  }

  private void decrementAcquiredChannelCount() {
    acquiredChannelCount.decrement();
  }

  @Override
  public Future<Channel> acquire(final Promise<Channel> promise) {
    try {
      acquire0(promise);
    } catch (Throwable cause) {
      if (!promise.tryFailure(cause)) {
        LOG.warn("Exception during acquire", cause);
      }
    }
    return promise;
  }

  private void acquire0(final Promise<Channel> promise) {
    if (closed) {
      promise.setFailure(new IllegalStateException("FixedChannelPool was closed"));
      return;
    }
    if (promise.isDone()) {
      return;
    }
    if (pendingAcquireCount.intValue() >= maxPendingAcquires) {
      tooManyOutstanding(promise);
    } else {
      AcquireTask task = new AcquireTask(promise);
      super.acquire(task);
      if (!task.isDone()) {
        if (timeoutTask != null && !promise.isDone()) {
          task.timeoutFuture = bootstrap().config()
              .group()
              .schedule(() -> timeoutTask.onTimeout(task), acquireTimeoutNanos, TimeUnit.NANOSECONDS);
        }
      }
    }
  }

  private void tooManyOutstanding(Promise<?> promise) {
    promise.setFailure(new IllegalStateException("Too many outstanding acquire operations"));
  }

  /**
   * Initiate establishing a new connection with the provided bootstrap.
   * This implementation attempts to not exceed a preset number of max connections by
   * storing deferred connections that would have caused the number of connections to
   * be exceeded in the {@literal _pendingConnect} queue.
   * As connections are established, they are added to {@literal _channelGroup}.
   * @param bs Channel Bootstrap
   * @param promise Promise to complete with result of Bootstrap connect
   * @return the promise
   */
  @Override
  protected Future<Channel> connectChannel(Bootstrap bs, Promise<Channel> promise) {
    Promise<Channel> p = new PromiseDelegate<Channel>(promise) {
      @Override
      public boolean trySuccess(Channel channel) {
        _channelGroup.add(channel);
        return super.trySuccess(channel);
      }
    };
    if (_channelGroup.size() + _connectInFlight >= getMaxConnections()) {
      _pendingConnect.add(() -> connectChannel0(bs, p));
    } else {
      connectChannel0(bs, p);
    }
    return promise;
  }

  private void connectChannel0(Bootstrap bs, Promise<Channel> promise) {
    CONNECT_IN_FLIGHT.incrementAndGet(this);
    super.connectChannel(bs, promise.addListener(future -> {
      int connectInFlight = CONNECT_IN_FLIGHT.decrementAndGet(this);
      // If there's another pending connection, we probably should ensure that it runs.
      if (!_pendingConnect.isEmpty() && _channelGroup.size() + connectInFlight < getMaxConnections()) {
        Runnable connect = _pendingConnect.poll();
        if (connect != null) {
          connect.run();
        }
      }
    }));
  }

  @Override
  public Future<Void> release(final Channel channel, final Promise<Void> promise) {
    ObjectUtil.checkNotNull(promise, "promise");
    Attribute<FastFixedChannelPool> attr = channel.attr(CHANNEL_IS_ACQUIRED);
    if (attr.get() != this) {
      return promise.setFailure(new IllegalArgumentException("Wrong pool for channel"));
    }
    if (attr.compareAndSet(FastFixedChannelPool.this, null)) {
      decrementAcquiredChannelCount();
    }
    final Promise<Void> p = _immediateEventExecutor.newPromise();
    super.release(channel, p.addListener(new FutureListener<Void>() {
      @Override
      public void operationComplete(Future<Void> future) throws Exception {
        if (closed) {
          // Since the pool is closed, we have no choice but to close the channel
          channel.close();
          promise.setFailure(new IllegalStateException("FixedChannelPool was closed"));
          return;
        }

        if (future.isSuccess()) {
          promise.setSuccess(null);
        } else {
          promise.setFailure(future.cause());
        }
      }
    }));
    return promise;
  }

  @Override
  public int getMaxConnections() {
    return maxConnections.getAsInt();
  }

  @Override
  public int getMaxPendingAcquires() {
    return maxPendingAcquires;
  }

  @Override
  public int getAcquiredChannelCount() {
    return acquiredChannelCount.intValue();
  }

  @Override
  public int getPendingAcquireCount() {
    return pendingAcquireCount.intValue();
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  private static final AtomicReferenceFieldUpdater<AcquireTask, Boolean> ACQUIRE_TASK_COMPLETE =
      AtomicReferenceFieldUpdater.newUpdater(AcquireTask.class, Boolean.class, "completed");

  private final class AcquireTask extends PromiseDelegate<Channel> {
    ScheduledFuture<?> timeoutFuture;
    volatile Boolean completed = false;

    public AcquireTask(Promise<Channel> promise) {
      super(promise);
      pendingAcquireCount.increment();
    }

    boolean complete() {
      if (completed || !ACQUIRE_TASK_COMPLETE.compareAndSet(this, false, true)) {
        return false;
      }
      pendingAcquireCount.decrement();
      if (timeoutFuture != null) {
        timeoutFuture.cancel(false);
      }
      return true;
    }

    @Override
    public boolean trySuccess(Channel result) {
      if (complete()) {
        Attribute<FastFixedChannelPool> attr = result.attr(CHANNEL_IS_ACQUIRED);
        attr.set(FastFixedChannelPool.this);
        incrementAcquiredChannelCount();
        if (super.trySuccess(result)) {
          return true;
        }
        attr.set(null);
        decrementAcquiredChannelCount();
      }
      return false;
    }

    @Override
    public boolean tryFailure(Throwable cause) {
      return complete() && super.tryFailure(cause);
    }
  }

  private interface TimeoutTask {
    void onTimeout(AcquireTask task);
  }

  @Override
  public void close() {
    try {
      closeAsync().await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  /**
   * Closes the pool in an async manner.
   *
   * @return Future which represents completion of the close task
   */
  public Future<Void> closeAsync() {
    return close0();
  }

  private Future<Void> close0() {
    if (!closed && !CLOSED.getAndSet(this, true)) {
      acquiredChannelCount.reset();
      pendingAcquireCount.reset();
      // Calls close on the actual channels
      super.close();
    }

    return _immediateEventExecutor.newSucceededFuture(null);
  }
}
