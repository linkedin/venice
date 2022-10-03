package com.linkedin.alpini.netty4.pool;

import com.linkedin.alpini.base.misc.ExceptionUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoop;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.util.AsciiString;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.net.ConnectException;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Created by acurtis on 5/8/18.
 */
public class FixedChannelPoolImpl extends EspressoFixedChannelPool implements ManagedChannelPool {
  private static final Logger LOG = LogManager.getLogger(FixedChannelPoolImpl.class);

  private final String _name;
  private IntSupplier _minConnections;
  private int _connectsInProgress;
  private boolean _growInProgress;
  private final ChannelGroup _channelGroup;
  private final Promise<?> _closing;
  private final Future<Void> _closeFuture;
  private final EventLoop _eventLoop;
  private final BooleanSupplier _useQueueSizeForAcquiredChannelCount;

  public FixedChannelPoolImpl(
      @Nonnull Bootstrap bootstrap,
      @Nonnull ChannelPoolHandler handler,
      @Nonnull ChannelHealthChecker healthCheck,
      @Nonnull FixedChannelPool.AcquireTimeoutAction action,
      long acquireTimeoutMillis,
      @Nonnull IntSupplier minConnections,
      int maxConnections,
      int maxPendingAcquires,
      boolean releaseHealthCheck,
      boolean lastRecentUsed) {
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
        lastRecentUsed,
        () -> false);
  }

  public FixedChannelPoolImpl(
      @Nonnull Bootstrap bootstrap,
      @Nonnull ChannelPoolHandler handler,
      @Nonnull ChannelHealthChecker healthCheck,
      @Nonnull FixedChannelPool.AcquireTimeoutAction action,
      long acquireTimeoutMillis,
      int minConnections,
      int maxConnections,
      int maxPendingAcquires,
      boolean releaseHealthCheck,
      boolean lastRecentUsed) {
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
        lastRecentUsed,
        () -> false);
  }

  public FixedChannelPoolImpl(
      @Nonnull Bootstrap bootstrap,
      @Nonnull ChannelPoolHandler handler,
      @Nonnull ChannelHealthChecker healthCheck,
      @Nonnull FixedChannelPool.AcquireTimeoutAction action,
      long acquireTimeoutMillis,
      int minConnections,
      int maxConnections,
      int maxPendingAcquires,
      boolean releaseHealthCheck,
      boolean lastRecentUsed,
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
        lastRecentUsed,
        useQueueSizeForAcquiredChannelCount);
  }

  public FixedChannelPoolImpl(
      @Nonnull Bootstrap bootstrap,
      @Nonnull ChannelPoolHandler handler,
      @Nonnull ChannelHealthChecker healthCheck,
      @Nonnull FixedChannelPool.AcquireTimeoutAction action,
      long acquireTimeoutMillis,
      @Nonnull IntSupplier minConnections,
      int maxConnections,
      int maxPendingAcquires,
      boolean releaseHealthCheck,
      boolean lastRecentUsed,
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
        lastRecentUsed);
    _name = bootstrap.config().remoteAddress().toString();
    _eventLoop = bootstrap.config().group().next();
    _channelGroup = new DefaultChannelGroup(_name, _eventLoop, true) {
      @Override
      public boolean remove(Object o) {
        if (super.remove(o)) {
          LOG.info("closing channel {}", o);
          if (o instanceof Channel) {
            Channel ch = (Channel) o;
            if (ch.hasAttr(CHECKED_OUT) && Boolean.TRUE.equals(ch.attr(CHECKED_OUT).get())) {
              release(ch, _eventLoop.newPromise());
            }
          }
          return true;
        } else {
          return false;
        }
      }
    };
    Promise<Void> closeFuture = _eventLoop.newPromise();
    _closing = _eventLoop.newPromise().addListener(f -> {
      LOG.info("begin closing pool {}", bootstrap.config().remoteAddress());

      super.closeAsync().addListener(closeAsyncFuture -> {
        /* Close on super (EspressoFixedChannelPool) sets the pool to closed state and calls close on channels not in
        use. Doesn't wait for the actual channels to close. To close the channels that are in use, we call close on
        channel group. So that we can handle any requests in pipeline. For the requests in the pipeline, a premature
        close exception is set in HttpClientHandler's ResponseConsumer (StorageNodeRequest) accept */
        ChannelGroupFuture groupClose = _channelGroup.close();

        if (!groupClose.isDone()) {
          Future<?> scheduleFuture = _eventLoop.schedule(() -> {
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

        // Marks the close successful
        closeFuture.setSuccess(null);
        LOG.info("end closing pool {}", bootstrap.config().remoteAddress());
      });
    });

    _closeFuture = closeFuture;
    setMinConnections(minConnections);
    _useQueueSizeForAcquiredChannelCount = useQueueSizeForAcquiredChannelCount;
  }

  @Override
  public String name() {
    return _name;
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
    return !isClosing() && getConnectedChannels() + _connectsInProgress < getMinConnections();
  }

  private void addCloseListener(ChannelFuture future) {
    _channelGroup.add(future.channel());
  }

  protected ChannelFuture connectChannel0(Bootstrap bs) {
    _connectsInProgress++;
    return super.connectChannel(bs);
  }

  @Override
  protected ChannelFuture connectChannel(Bootstrap bs) {
    return connectChannel0(bs).addListener((ChannelFuture future) -> {
      _connectsInProgress--;
      if (future.isSuccess()) {
        addCloseListener(future);
        if (!_growInProgress && needsMoreConnections()) {
          connectChannel0(bs).addListener(new GrowPoolSize(bs));
        }
      }
    });
  }

  @Override
  public boolean isHealthy() {
    return getConnectedChannels() >= Math.max(1, getMinConnections());
  }

  static final AttributeKey<Boolean> CHECKED_OUT = AttributeKey.valueOf(FixedChannelPoolImpl.class, "checkedOut");

  @Override
  public Future<Channel> acquire(Promise<Channel> promise) {
    Promise<Channel> p = _eventLoop.newPromise();
    super.acquire(p);
    p.addListener((Future<Channel> f) -> {
      if (f.isSuccess() && f.getNow() != null) {
        Attribute<Boolean> checkedOut = f.getNow().attr(CHECKED_OUT);
        checkedOut.set(Boolean.TRUE);
        if (!promise.trySuccess(f.getNow())) {
          checkedOut.set(false);
          super.release(f.getNow(), _eventLoop.newPromise());
        }
      } else if (f.cause() != null) {
        promise.tryFailure(f.cause());
      } else {
        promise.tryFailure(new IllegalStateException("Failed to retrieve connection from pool"));
      }
    });
    return promise;
  }

  @Override
  public Future<Void> release(Channel channel, Promise<Void> promise) {
    if (!channel.hasAttr(CHECKED_OUT)) {
      // Channel was not checked out, do nothing here.
      return promise.setSuccess(null);
    }

    Attribute<Boolean> checkedOut = channel.attr(CHECKED_OUT);
    if (Boolean.TRUE.equals(checkedOut.getAndSet(false))) {
      try {
        super.release(channel, _eventLoop.newPromise()).addListener((Future<Void> future) -> {
          if (future.isSuccess()) {
            promise.trySuccess(future.getNow());
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
      int newAcquiredChannelCount = (_channelGroup.size() - getAvailableChannels()) + _connectsInProgress;
      // acquired channel count can never be less than zero
      if (newAcquiredChannelCount < 0) {
        LOG.warn(
            "Negative acquired channel count {} (group size {}, deque size {}, connectsInProgress {}) for {}",
            newAcquiredChannelCount,
            _channelGroup.size(),
            getAvailableChannels(),
            _connectsInProgress,
            bootstrap().config().remoteAddress());
        return super.acquiredChannelCount();
      }
      int oldAcquiredChannelCount = super.acquiredChannelCount();
      if (newAcquiredChannelCount != oldAcquiredChannelCount) {
        LOG.warn(
            "The acquired channel count based on the queue size is {} (group size {}, deque size {}, connectsInProgress {}) "
                + "doesn't match {} FixedChannelPool acquiredChannelCount {} ",
            newAcquiredChannelCount,
            _channelGroup.size(),
            getAvailableChannels(),
            _connectsInProgress,
            bootstrap().config().remoteAddress(),
            oldAcquiredChannelCount);
      }
      return newAcquiredChannelCount;
    }
    return super.acquiredChannelCount();
  }

  private class GrowPoolSize implements ChannelFutureListener {
    private final Bootstrap _bootstrap;

    private GrowPoolSize(Bootstrap bootstrap) {
      _bootstrap = bootstrap;
      _growInProgress = true;
      LOG.debug(
          "Growing connections {} from {} to {}",
          bootstrap.config().remoteAddress(),
          getConnectedChannels(),
          getMinConnections());
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      _connectsInProgress--;
      if (future.isSuccess()) {
        addCloseListener(future);
        incrementAcquiredChannelCount();
        FixedChannelPoolImpl.super.release(future.channel(), ImmediateEventExecutor.INSTANCE.newPromise());
        if (needsMoreConnections()) {
          connectChannel0(_bootstrap).addListener(this);
          return;
        }
      } else {
        notifyGrowFailure(_bootstrap.config().remoteAddress(), ExceptionUtil.unwrapCompletion(future.cause()));
      }
      _growInProgress = false;
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
