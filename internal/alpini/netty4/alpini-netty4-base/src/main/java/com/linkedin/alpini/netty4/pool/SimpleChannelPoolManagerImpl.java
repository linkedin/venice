package com.linkedin.alpini.netty4.pool;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.alpini.base.concurrency.Lazy;
import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.base.monitoring.CallCompletion;
import com.linkedin.alpini.base.monitoring.CallTracker;
import com.linkedin.alpini.consts.QOS;
import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import io.netty.util.internal.PlatformDependent;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * In order to simplify the implementation of this {@link ChannelPoolManager}, this implementation requires that
 * {@link #open(String)} is called to create the {@literal hostNameAndPort} instead of
 * lazily creating them upon first reference.
 *
 * In the case for Espresso Router, it is informed of all the valid {@literal hostNameAndPort} from Helix and should
 * then call {@link #open(String)} for each host. Calling it for a host that is already existant is a no-operation.
 */
public class SimpleChannelPoolManagerImpl implements ChannelPoolManager {
  private static final AttributeKey<HostPool> POOL_ATTRIBUTE_KEY =
      AttributeKey.valueOf(SimpleChannelPoolManagerImpl.class, "pool");
  private static final AttributeKey<CompletableFuture<CallCompletion>> BUSY_ATTRIBUTE_KEY =
      AttributeKey.valueOf(SimpleChannelPoolManagerImpl.class, "busy");

  private final Logger _log = LogManager.getLogger(getClass());
  private final ConcurrentMap<String, HostPool> _map = PlatformDependent.newConcurrentHashMap();
  private final Executor _closeExecutor = Executors.newSingleThreadExecutor(Executors.daemonThreadFactory()); // will be
                                                                                                              // gc
                                                                                                              // automatically
  private final Executor _statsExecutor;
  private final EventLoopGroup _eventLoopGroup;
  private final ChannelPoolFactory _channelPoolFactory;

  public <E extends MultithreadEventLoopGroup> SimpleChannelPoolManagerImpl(
      @Nonnull E eventLoopGroup,
      @Nonnull ChannelPoolFactory channelPoolFactory) {
    _eventLoopGroup = eventLoopGroup;
    _channelPoolFactory = channelPoolFactory;

    // will be garbage collected automatically
    Executor statsExecutor = Executors.newSingleThreadExecutor(Executors.daemonThreadFactory());

    // To avoid stats collection to incurr a cost in the critical path,
    // they are collected into a separate queue and run in a separate thread.
    // which is scheduled to run periodically.
    Queue<Runnable> statsQueue = PlatformDependent.newMpscQueue();
    _statsExecutor = statsQueue::add;
    Runnable collectStats = () -> {
      for (Runnable task = statsQueue.poll(); task != null; task = statsQueue.poll()) {
        task.run();
      }
    };

    _eventLoopGroup.schedule(new Runnable() {
      @Override
      public void run() {
        // drain all the collected stats
        statsExecutor.execute(collectStats);
        // schedule the next drain to occur in 100 milliseconds after completion
        statsExecutor.execute(this::reschedule);
      }

      private void reschedule() {
        _eventLoopGroup.schedule(this, 100, TimeUnit.MILLISECONDS);
      }
    }, 1, TimeUnit.SECONDS);

  }

  protected ManagedChannelPool constructManagedPool(ChannelPoolHandler poolHandler, InetSocketAddress address) {
    return _channelPoolFactory.construct(this, poolHandler, _eventLoopGroup, address);
  }

  @Nonnull
  protected CallTracker createHostAcquireCallTracker(@Nonnull String hostAndPort) {
    return CallTracker.create();
  }

  @Nonnull
  protected CallTracker createHostBusyCallTracker(@Nonnull String hostAndPort) {
    return CallTracker.create();
  }

  class HostPool implements ChannelPoolHandler {
    private final Supplier<ManagedChannelPool> _channelPool;
    private final ChannelGroup _channelGroup;
    private final ChannelGroup _activeGroup;
    private final LongAdder _createCount;
    private final LongAdder _closeCount;
    private final InetSocketAddress _socketAddress;
    private final CallTracker _acquireCallTracker;
    private final CallTracker _busyCallTracker;
    private final CompletableFuture<Boolean> _closeCompleted;
    private boolean closing;

    HostPool(String name) {
      EventExecutor eventExecutor = _eventLoopGroup.next();
      URI uri = URI.create("socket://" + name);
      _socketAddress = InetSocketAddress.createUnresolved(uri.getHost(), uri.getPort());
      _createCount = new LongAdder();
      _closeCount = new LongAdder();
      _channelGroup = new DefaultChannelGroup(name, eventExecutor, true) {
        @Override
        public boolean add(Channel channel) {
          if (super.add(channel)) {
            _createCount.increment();
            return true;
          }
          return false;
        }

        @Override
        public boolean remove(Object o) {
          if (super.remove(o)) {
            _closeCount.increment();
            return true;
          }
          return false;
        }
      };
      _acquireCallTracker = createHostAcquireCallTracker(name);
      _busyCallTracker = createHostBusyCallTracker(name);
      _closeCompleted = new CompletableFuture<>();
      _activeGroup = new DefaultChannelGroup("active:" + name, eventExecutor);
      _channelPool = Lazy.of(() -> constructManagedPool(this, remoteAddress()));
    }

    @Override
    public void channelReleased(Channel ch) throws Exception {
      CompletableFuture<Long> acquireTime = CompletableFuture.completedFuture(Time.nanoTime());
      boolean success = _activeGroup.remove(ch);

      CompletableFuture<CallCompletion> cc = ch.attr(BUSY_ATTRIBUTE_KEY).getAndSet(null);
      if (cc != null) {
        if (success) {
          cc.thenAcceptBothAsync(acquireTime, CallCompletion::close, _statsExecutor);
        } else {
          cc.thenAcceptBothAsync(acquireTime, CallCompletion::closeWithError, _statsExecutor);
        }
      }
    }

    @Override
    public void channelAcquired(Channel ch) throws Exception {
      CompletableFuture<Long> acquireTime = CompletableFuture.completedFuture(Time.nanoTime());
      CompletableFuture<CallCompletion> cc = ch.attr(BUSY_ATTRIBUTE_KEY)
          .getAndSet(acquireTime.thenApplyAsync(busyCallTracker()::startCall, _statsExecutor));
      if (cc != null) {
        cc.thenAcceptBothAsync(acquireTime, CallCompletion::closeWithError, _statsExecutor);
      }
      _activeGroup.add(ch);
    }

    @Override
    public void channelCreated(Channel ch) throws Exception {
      _channelGroup.add(ch);
      ch.attr(POOL_ATTRIBUTE_KEY).set(this);
    }

    public InetSocketAddress remoteAddress() {
      return _socketAddress;
    }

    public int openConnections() {
      return _channelGroup.size();
    }

    public boolean isHealthy() {
      return _channelPool.get().isHealthy();
    }

    public boolean isClosing() {
      return closing || _channelPool.get().isClosing();
    }

    private FutureListener<Channel> acquireListener0(Promise<Channel> channelPromise) {
      return (Future<Channel> future) -> {
        if (future.isSuccess()) {
          future.getNow().attr(POOL_ATTRIBUTE_KEY).set(this);
          if (channelPromise.trySuccess(future.getNow())) {
            return;
          }
          _channelPool.get().release(future.getNow());
        } else {
          channelPromise.setFailure(future.cause());
        }
      };
    }

    private Future<Channel> acquire0(FutureListener<Channel> futureListener) {
      Future<Channel> channelFuture = _channelPool.get().acquire();
      if (channelFuture.isSuccess()) {
        channelFuture.getNow().attr(POOL_ATTRIBUTE_KEY).set(this);
        return channelFuture.addListener(futureListener);
      }
      Promise<Channel> channelPromise = ImmediateEventExecutor.INSTANCE.newPromise();
      channelFuture.addListener(acquireListener0(channelPromise));
      return channelPromise.addListener(futureListener);
    }

    private Future<Channel> acquire0(Promise<Channel> promise) {
      _channelPool.get().acquire().addListener(acquireListener0(promise));
      return promise;
    }

    public Future<Channel> acquire() {
      return acquire0(acquireListener(startAcquire()));
    }

    public Future<Channel> acquire(Promise<Channel> promise) {
      return acquire0(promise.addListener(acquireListener(startAcquire())));
    }

    private CompletableFuture<CallCompletion> startAcquire() {
      long startTime = Time.nanoTime();
      return CompletableFuture.completedFuture(startTime)
          .thenApplyAsync(acquireCallTracker()::startCall, _statsExecutor);
    }

    private FutureListener<Channel> acquireListener(CompletableFuture<CallCompletion> cc) {
      return acquireFuture -> {
        long endTime = Time.nanoTime();
        if (acquireFuture.isSuccess()) {
          cc.thenAcceptBothAsync(CompletableFuture.completedFuture(endTime), CallCompletion::close);
        } else {
          cc.thenAcceptBothAsync(CompletableFuture.completedFuture(endTime), CallCompletion::closeWithError);
        }
      };
    }

    public Future<Void> release(Channel channel) {
      return release(channel, channel.eventLoop().newPromise());
    }

    public Future<Void> release(Channel channel, Promise<Void> promise) {
      return _channelPool.get().release(channel, promise);
    }

    Future<Void> closeAsync() {
      closing = true;
      ManagedChannelPool channelPool = _channelPool.get();
      Future<Void> closeFuture = channelPool.closeFuture();
      CompletableFuture.runAsync(channelPool::close, _closeExecutor).thenRun(() -> closeFuture.addListener(future -> {
        _closeCompleted.complete(_map.remove(name(), this));
      }));
      return closeFuture;
    }

    @Nonnull
    public String name() {
      return _channelGroup.name();
    }

    public int activeCount() {
      return _activeGroup.size();
    }

    @Nonnull
    public CallTracker acquireCallTracker() {
      return _acquireCallTracker;
    }

    @Nonnull
    public CallTracker busyCallTracker() {
      return _busyCallTracker;
    }
  }

  @Override
  @Deprecated
  public int executorCount() {
    return subpoolCount();
  }

  @Override
  public int subpoolCount() {
    return 1;
  }

  @Nonnull
  @Override
  public Future<Channel> acquire(@Nonnull String hostNameAndPort, @Nonnull String queueName, @Nonnull QOS qos) {
    HostPool hostPool = _map.get(hostNameAndPort);
    return hostPool == null
        ? ImmediateEventExecutor.INSTANCE.newFailedFuture(new UnknownHostException(hostNameAndPort))
        : hostPool.acquire();
  }

  @Nonnull
  @Override
  public Future<Channel> acquire(
      @Nonnull EventLoop eventLoop,
      @Nonnull String hostNameAndPort,
      @Nonnull String queueName,
      @Nonnull QOS qos) {
    HostPool hostPool = _map.get(hostNameAndPort);
    if (hostPool == null) {
      return eventLoop.newFailedFuture(new UnknownHostException(hostNameAndPort));
    }
    if (eventLoop.inEventLoop()) {
      return hostPool.acquire(eventLoop.newPromise());
    }
    Promise<Channel> promise = eventLoop.newPromise();
    eventLoop.execute(() -> hostPool.acquire(promise));
    return promise;
  }

  @Nonnull
  @Override
  public Future<Void> release(@Nonnull Channel channel) {
    if (channel.hasAttr(POOL_ATTRIBUTE_KEY)) {
      return channel.attr(POOL_ATTRIBUTE_KEY).get().release(channel);
    }
    return channel.eventLoop().newFailedFuture(new IllegalStateException());
  }

  @Override
  public Future<Void> open(@Nonnull String hostNameAndPort) {
    return open(hostNameAndPort, ImmediateEventExecutor.INSTANCE.newPromise());
  }

  private Future<Void> open(@Nonnull String hostNameAndPort, Promise<Void> promise) {
    HostPool hostPool = _map.computeIfAbsent(hostNameAndPort, HostPool::new);
    if (hostPool.isClosing()) {
      hostPool._closeCompleted.thenRunAsync(() -> open(hostNameAndPort, promise), _eventLoopGroup);
      return promise;
    } else {
      return promise.setSuccess(null);
    }
  }

  @Nonnull
  @Override
  public Future<Void> close(@Nonnull String hostNameAndPort) {
    HostPool hostPool = _map.get(hostNameAndPort);
    return hostPool == null ? ImmediateEventExecutor.INSTANCE.newSucceededFuture(null) : hostPool.closeAsync();
  }
}
