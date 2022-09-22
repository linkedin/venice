package com.linkedin.alpini.netty4.pool;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.util.AsciiString;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.net.ConnectException;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Created by acurtis on 5/8/18.
 */
public class FixedFastChannelPoolImpl extends FastFixedChannelPool implements ManagedChannelPool {
  private static final Logger LOG = LogManager.getLogger(FixedFastChannelPoolImpl.class);

  private static final AtomicIntegerFieldUpdater<FixedFastChannelPoolImpl> GROW_IN_PROGRESS =
      AtomicIntegerFieldUpdater.newUpdater(FixedFastChannelPoolImpl.class, "_growInProgress");

  private IntSupplier _minConnections;
  private volatile int _growInProgress = 0;
  private final Promise<?> _closing;
  private final Future<Void> _closeFuture;
  private final BooleanSupplier _useQueueSizeForAcquiredChannelCount;

  public FixedFastChannelPoolImpl(
      @Nonnull Bootstrap bootstrap,
      @Nonnull ChannelPoolHandler handler,
      @Nonnull ChannelHealthChecker healthCheck,
      @Nonnull FixedChannelPool.AcquireTimeoutAction action,
      long acquireTimeoutMillis,
      @Nonnull IntSupplier minConnections,
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
        minConnections,
        maxConnections,
        maxPendingAcquires,
        releaseHealthCheck,
        connectConcurrency,
        () -> false);
  }

  public FixedFastChannelPoolImpl(
      @Nonnull Bootstrap bootstrap,
      @Nonnull ChannelPoolHandler handler,
      @Nonnull ChannelHealthChecker healthCheck,
      @Nonnull FixedChannelPool.AcquireTimeoutAction action,
      long acquireTimeoutMillis,
      int minConnections,
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
        Integer.valueOf(minConnections)::intValue,
        maxConnections,
        maxPendingAcquires,
        releaseHealthCheck,
        connectConcurrency,
        () -> false);
  }

  public FixedFastChannelPoolImpl(
      @Nonnull Bootstrap bootstrap,
      @Nonnull ChannelPoolHandler handler,
      @Nonnull ChannelHealthChecker healthCheck,
      @Nonnull FixedChannelPool.AcquireTimeoutAction action,
      long acquireTimeoutMillis,
      int minConnections,
      int maxConnections,
      int maxPendingAcquires,
      boolean releaseHealthCheck,
      int connectConcurrency,
      @Nonnull BooleanSupplier useQueueSizeForAcquiredChannelCount) {
    this(
        bootstrap,
        handler,
        healthCheck,
        action,
        acquireTimeoutMillis,
        Integer.valueOf(minConnections)::intValue,
        maxConnections,
        maxPendingAcquires,
        releaseHealthCheck,
        connectConcurrency,
        useQueueSizeForAcquiredChannelCount);
  }

  public FixedFastChannelPoolImpl(
      @Nonnull Bootstrap bootstrap,
      @Nonnull ChannelPoolHandler handler,
      @Nonnull ChannelHealthChecker healthCheck,
      @Nonnull FixedChannelPool.AcquireTimeoutAction action,
      long acquireTimeoutMillis,
      @Nonnull IntSupplier minConnections,
      int maxConnections,
      int maxPendingAcquires,
      boolean releaseHealthCheck,
      int connectConcurrency,
      @Nonnull BooleanSupplier useQueueSizeForAcquiredChannelCount) {
    super(
        bootstrap,
        handler,
        healthCheck,
        action,
        acquireTimeoutMillis,
        maxConnections,
        maxPendingAcquires,
        releaseHealthCheck,
        connectConcurrency);
    EventLoop eventLoop = bootstrap.config().group().next();
    Promise<Void> closeFuture = eventLoop.newPromise();
    _closing = eventLoop.newPromise().addListener(f -> {
      LOG.info("begin closing pool {}", bootstrap.config().remoteAddress());

      super.closeAsync().addListener(closeAsyncFuture -> {
        /* Close on super (EspressoFixedChannelPool) sets the pool to closed state and calls close on channels not in
        use. Doesn't wait for the actual channels to close. To close the channels that are in use, we call close on
        channel group. So that we can handle any requests in pipeline. For the requests in the pipeline, a premature
        close exception is set in HttpClientHandler's ResponseConsumer (StorageNodeRequest) accept */
        ChannelGroupFuture groupClose = _channelGroup.close();

        if (!groupClose.isDone()) {
          Future<?> scheduleFuture = eventLoop.schedule(() -> {
            LOG.info("Force closing connections to {}", bootstrap.config().remoteAddress());
            _channelGroup.forEach(channel -> {
              try {
                if (channel.isActive()) {
                  channel.eventLoop().submit(() -> channel.unsafe().close(channel.voidPromise()));
                }
              } catch (Exception e) {
                // Ignore the Exception
                LOG.warn("couldn't force close the connection {}", bootstrap.config().remoteAddress());
              }
            });
          }, 5000, TimeUnit.MILLISECONDS);

          groupClose.addListener(groupClosed -> scheduleFuture.cancel(false));
        }

        LOG.info("end closing pool {}", bootstrap.config().remoteAddress());
        // Marks the close successful
        closeFuture.setSuccess(null);
      });
    });

    _closeFuture = closeFuture;
    setMinConnections(minConnections);
    _useQueueSizeForAcquiredChannelCount = useQueueSizeForAcquiredChannelCount;
  }

  @Override
  protected PoolChannelGroup newPoolChannelGroup() {
    return new FixedPoolChannelGroup(name());
  }

  protected class FixedPoolChannelGroup extends PoolChannelGroup {
    protected FixedPoolChannelGroup(String name) {
      super(name);
    }

    @Override
    public boolean remove(Object o) {
      if (super.remove(o)) {
        LOG.info("closing channel {}", o);
        release((Channel) o, _immediateEventExecutor.newPromise());
        return true;
      } else {
        return false;
      }
    }
  }

  @Override
  public ChannelPoolHandler handler() {
    return super.handler();
  }

  @Override
  public int getConnectedChannels() {
    return _channelGroup.size();
  }

  public void setMinConnections(int minConnections) {
    setMinConnections(Integer.valueOf(minConnections)::intValue);
  }

  public void setMinConnections(@Nonnull IntSupplier minConnections) {
    _minConnections = minConnections;
  }

  public int getMinConnections() {
    return _minConnections.getAsInt();
  }

  private boolean needsMoreConnections() {
    return !isClosing() && getConnectedChannels() + connectInProgress() < getMinConnections();
  }

  @Override
  protected boolean acquireConnectSemaphore() {
    return getConnectedChannels() + connectInProgress() < getMaxConnections() && super.acquireConnectSemaphore();
  }

  @Override
  protected void onConnect(Channel ch, Promise<Channel> promise) {
    if (ch != null) {
      assert _channelGroup.contains(ch);
      if (promise instanceof GrowPoolSize) {
        if (needsMoreConnections()) {
          GROW_IN_PROGRESS.incrementAndGet(this);
          growPoolSize();
        }
      } else {
        checkGrowPoolSize();
      }
    }
    super.onConnect(ch, promise);
  }

  @Override
  protected Future<Channel> connectChannel(Bootstrap bs, Promise<Channel> promise) {
    return connectChannel0(bs, promise);
  }

  protected Future<Channel> connectChannel0(Bootstrap bs, Promise<Channel> promise) {
    return super.connectChannel(bs, promise);
  }

  private void checkGrowPoolSize() {
    if (isClosing()) {
      return;
    }
    if (_growInProgress == 0 && needsMoreConnections() && GROW_IN_PROGRESS.compareAndSet(this, 0, 1)) {
      growPoolSize();
    }
  }

  private void growPoolSize() {
    assert _growInProgress > 0;
    if (acquireConnectSemaphore()) {
      if (getConnectedChannels() + connectInProgress() > getMinConnections()) {
        releaseConnectSemaphore();
        return;
      }
      Promise<Channel> grow = new GrowPoolSize(_immediateEventExecutor).addListener((Future<Channel> future) -> {
        GROW_IN_PROGRESS.decrementAndGet(this);
        if (future.isSuccess()) {
          assert future.getNow() == null : "Expected null";
          // We expected null because the GrowPoolSize.isDone() method should complete the promise with null
          // and after the connection is established, FastSimpleChannelPool.notifyConnect will first check
          // the promise with isDone before trying to complete it.
        } else {
          notifyGrowFailure(bootstrap().config().remoteAddress(), future.cause());
        }
      });
      try {
        bootstrapChannel(grow);
      } catch (Throwable ex) {
        if (!grow.tryFailure(ex)) {
          LOG.info("Exception trying to grow pool", ex);
        }
      }
    } else {
      GROW_IN_PROGRESS.decrementAndGet(this);
    }
  }

  @Override
  public boolean isHealthy() {
    return getConnectedChannels() >= Math.max(1, getMinConnections());
  }

  static final AttributeKey<Boolean> CHECKED_OUT = AttributeKey.valueOf(FixedFastChannelPoolImpl.class, "checkedOut");

  @Override
  public Future<Channel> acquire(Promise<Channel> promise) {
    Promise<Channel> p = _immediateEventExecutor.newPromise();
    super.acquire(p);
    p.addListener((Future<Channel> f) -> {
      if (f.isSuccess() && f.getNow() != null) {
        Attribute<Boolean> checkedOut = f.getNow().attr(CHECKED_OUT);
        checkedOut.set(Boolean.TRUE);
        if (!promise.trySuccess(f.getNow())) {
          checkedOut.set(false);
          super.release(f.getNow(), _immediateEventExecutor.newPromise());
        }
      } else if (f.cause() != null) {
        promise.tryFailure(f.cause());
      } else {
        promise.tryFailure(new IllegalStateException("Failed to retrieve connection from pool"));
      }
    });
    if (!p.isDone()) {
      checkGrowPoolSize();
    }
    return promise;
  }

  @Override
  public Future<Void> release(Channel channel, Promise<Void> promise) {
    if (!channel.hasAttr(CHECKED_OUT)) {
      return super.release(channel, promise);
    }

    Attribute<Boolean> checkedOut = channel.attr(CHECKED_OUT);
    if (checkedOut.get()) {
      checkedOut.set(Boolean.FALSE);
      try {
        super.release(channel, _immediateEventExecutor.newPromise()).addListener((Future<Void> future) -> {
          if (future.isSuccess()) {
            promise.trySuccess(future.getNow());
            checkGrowPoolSize();
          } else if ((future.cause() instanceof IllegalStateException)
              && AsciiString.contains(future.cause().getMessage(), "closed")) {
            // Pool is already closed, ignore the exception
          } else {
            LOG.warn("Release failed with unexpected exception", future.cause());
            promise.setFailure(future.cause());
          }
        });
      } catch (Exception e) {
        LOG.warn("A really unexpected exception occurred", e);
        promise.setFailure(e);
      }
      return promise;
    } else {
      return promise.setSuccess(null);
    }
  }

  @Override
  public void close() {
    _closing.trySuccess(null);
  }

  @Override
  public final Future<Void> closeFuture() {
    return _closeFuture;
  }

  public final boolean isClosing() {
    return _closing.isDone();
  }

  @Override
  public int acquiredChannelCount() {
    /** Returns the number of channels in use based on queue size.
     *
     * This can be calculated using channelGroup (ChannelGroup has a list of all the healthy channels, this pool owns)
     * and getAvailableChannels (gets the number of channels in the SimpleChannelPool, not checked out).
     */
    if (_useQueueSizeForAcquiredChannelCount.getAsBoolean()) {
      int availableChannels = getAvailableChannels();
      int channelGroupSize = _channelGroup.size();
      int newAcquiredChannelCount = (channelGroupSize - availableChannels);
      // acquired channel count can never be less than zero
      if (newAcquiredChannelCount < 0) {
        LOG.warn(
            "Negative acquired channel count {} (group size {}, deque size {}) for {}",
            newAcquiredChannelCount,
            channelGroupSize,
            availableChannels,
            bootstrap().config().remoteAddress());
        return super.acquiredChannelCount();
      }
      int oldAcquiredChannelCount = super.acquiredChannelCount();
      if (newAcquiredChannelCount != oldAcquiredChannelCount) {
        LOG.warn(
            "The acquired channel count based on the queue size is {} (group size {}, deque size {}) "
                + "doesn't match {} FixedChannelPool acquiredChannelCount {} ",
            newAcquiredChannelCount,
            channelGroupSize,
            availableChannels,
            bootstrap().config().remoteAddress(),
            oldAcquiredChannelCount);
      }
      return newAcquiredChannelCount;
    }
    return super.acquiredChannelCount();
  }

  static class GrowPoolSize extends DefaultPromise<Channel> {
    public GrowPoolSize(EventExecutor executor) {
      super(executor);
    }

    @Override
    public boolean isDone() {
      return super.isDone() || trySuccess(null);
    }
  }

  protected void notifyGrowFailure(SocketAddress remoteAddress, Throwable cause) {
    if (cause instanceof ConnectException) {
      LOG.info("Pool grow ConnectException: {} {}", remoteAddress, cause.getMessage());
    } else {
      LOG.warn("Pool grow failure: {}", remoteAddress, cause);
    }
  }
}
