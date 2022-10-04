package com.linkedin.alpini.netty4.pool;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.alpini.base.concurrency.Lazy;
import com.linkedin.alpini.base.concurrency.NamedThreadFactory;
import com.linkedin.alpini.base.misc.CollectionUtil;
import com.linkedin.alpini.base.misc.ExceptionUtil;
import com.linkedin.alpini.base.misc.Time;
import com.linkedin.alpini.base.monitoring.CallCompletion;
import com.linkedin.alpini.base.monitoring.CallTracker;
import com.linkedin.alpini.base.monitoring.NullCallTracker;
import com.linkedin.alpini.consts.QOS;
import com.linkedin.alpini.netty4.handlers.AllChannelsHandler;
import com.linkedin.alpini.netty4.handlers.Http2PingSendHandler;
import com.linkedin.alpini.netty4.misc.BalancedEventLoopGroup;
import com.linkedin.alpini.netty4.misc.ExceptionWithResponseStatus;
import com.linkedin.alpini.netty4.misc.Futures;
import com.linkedin.alpini.netty4.misc.Http2Utils;
import com.linkedin.alpini.netty4.misc.LocalThreadEventLoopGroup;
import com.linkedin.alpini.netty4.misc.SingleThreadEventLoopGroupSupplier;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.SingleThreadEventLoop;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * An implementation of a {@link ChannelPoolManager}.
 *
 * @author acurtis on 3/29/17.
 */
public class ChannelPoolManagerImpl implements ChannelPoolManager {
  private static final ImmediateEventExecutor IMMEDIATE = ImmediateEventExecutor.INSTANCE;
  private static final Future<Void> COMPLETED_VOID_FUTURE = IMMEDIATE.newSucceededFuture(null);

  private static final long CLOSE_ALL_DELAY = 1000L;

  private static final QOS[] QOS_HIGH_NORMAL_LOW = { QOS.HIGH, QOS.NORMAL, QOS.LOW };
  private static final QOS[] QOS_NORMAL_HIGH_LOW = { QOS.NORMAL, QOS.HIGH, QOS.LOW };
  private static final QOS[] QOS_LOW_HIGH_NORMAL = { QOS.LOW, QOS.HIGH, QOS.NORMAL };
  public static final String DEFAULT_DB_QUEUE = "default";

  private static final Exception TOO_MANY_REQUESTS_EXCEPTION = ExceptionUtil.withoutStackTrace(
      new ExceptionWithResponseStatus(HttpResponseStatus.TOO_MANY_REQUESTS, "Too many requests in queue"));
  private static final Exception UNABLE_TO_ENQUEUE_REQUEST_EXCEPTION = ExceptionUtil.withoutStackTrace(
      new ExceptionWithResponseStatus(HttpResponseStatus.TOO_MANY_REQUESTS, "Unable to enqueue request"));
  private static final Exception POOL_CLOSED_EXCEPTION = ExceptionUtil.withoutStackTrace(
      new ExceptionWithResponseStatus(HttpResponseStatus.SERVICE_UNAVAILABLE, "Connection pool closed"));

  private final Logger _log = LogManager.getLogger(getClass());

  private final AttributeKey<ChannelPool> _ownerKey = AttributeKey.valueOf(getClass(), "ownerKey:" + toString());
  private final AttributeKey<Supplier<CallCompletion>> _busyKey =
      AttributeKey.valueOf(getClass(), "busyKey:" + toString());

  private final SingleThreadEventLoopGroupSupplier _localThreadGroup;
  private final ChannelPoolFactory _channelPoolFactory;
  private final ChannelPoolResolver _channelPoolResolver;
  private final ConcurrentMap<String, Pool> _pools = new ConcurrentHashMap<>();
  private final IntSupplier _eventLoopConcurrency;
  private final EventLoopGroup _workerEventLoopGroup;
  private final Consumer<Runnable> _runInEveryThread;
  private final LongAdder _globalActiveCount = new LongAdder();
  private final LongAdder _globalChannelCount = new LongAdder();

  private AllChannelsHandler _allChannelsHandler;

  private int _maxWaitersPerPool;

  private BooleanSupplier _enableFairScheduling = () -> true;
  private BooleanSupplier _enableDeferredExecution = () -> false;

  /** Use a named thread factory for the handling channel released */
  private static final ThreadFactory CHANNEL_RELEASED_FACTORY = new NamedThreadFactory("channel-released");

  /** To reduce contention in the worker queue, we have seperate worker queues for each io worker */
  private static final ThreadLocal<Executor> CHANNEL_RELEASED_EXECUTOR =
      ThreadLocal.withInitial(() -> Executors.newSingleThreadExecutor(CHANNEL_RELEASED_FACTORY));

  // Global Pool
  private boolean _useH2GlobalPool = false;
  private boolean _createConnectionsOnWorkerGroup = false;
  private boolean _enableSimpleAcquire = false;

  // Http2 ping
  private ScheduledFuture<?> _periodicPing;
  private int _pingIntervalSeconds;

  public <E extends MultithreadEventLoopGroup> ChannelPoolManagerImpl(
      @Nonnull E eventLoopGroup,
      @Nonnull ChannelPoolFactory channelPoolFactory,
      @Nonnull ChannelPoolResolver channelPoolResolver,
      @Nonnegative int maxWaitersPerPool) {
    _localThreadGroup = new LocalThreadEventLoopGroup<>(Objects.requireNonNull(eventLoopGroup, "eventLoopGroup"));
    _channelPoolFactory = Objects.requireNonNull(channelPoolFactory, "channelPoolFactory");
    _channelPoolResolver = Objects.requireNonNull(channelPoolResolver, "channelPoolResolver");
    _workerEventLoopGroup = eventLoopGroup;
    _eventLoopConcurrency = () -> _useH2GlobalPool ? 1 : eventLoopGroup.executorCount();
    _maxWaitersPerPool = maxWaitersPerPool;
    _runInEveryThread =
        command -> eventLoopGroup.iterator().forEachRemaining(eventExecutor -> eventExecutor.execute(command));
  }

  public <E extends MultithreadEventLoopGroup> ChannelPoolManagerImpl(
      @Nonnull E eventLoopGroup,
      @Nonnull ChannelPoolFactory channelPoolFactory,
      @Nonnull ChannelPoolResolver channelPoolResolver,
      @Nonnegative int maxWaitersPerPool,
      boolean useH2GlobalPool,
      boolean createConnectionsOnWorkerGroup,
      boolean enableSimpleAcquire) {
    this(
        eventLoopGroup,
        channelPoolFactory,
        channelPoolResolver,
        maxWaitersPerPool,
        useH2GlobalPool,
        createConnectionsOnWorkerGroup,
        enableSimpleAcquire,
        0);
  }

  public <E extends MultithreadEventLoopGroup> ChannelPoolManagerImpl(
      @Nonnull E eventLoopGroup,
      @Nonnull ChannelPoolFactory channelPoolFactory,
      @Nonnull ChannelPoolResolver channelPoolResolver,
      @Nonnegative int maxWaitersPerPool,
      boolean useH2GlobalPool,
      boolean createConnectionsOnWorkerGroup,
      boolean enableSimpleAcquire,
      int pingIntervalSeconds) {
    this(eventLoopGroup, channelPoolFactory, channelPoolResolver, maxWaitersPerPool);
    this._useH2GlobalPool = useH2GlobalPool;
    this._createConnectionsOnWorkerGroup = createConnectionsOnWorkerGroup;
    this._enableSimpleAcquire = enableSimpleAcquire;
    this._pingIntervalSeconds = pingIntervalSeconds;
    this.updateChannelPoolFactoryWithHttp2PingEnabled();
  }

  @Deprecated
  @Override
  public int executorCount() {
    return subpoolCount();
  }

  private void updateChannelPoolFactoryWithHttp2PingEnabled() {
    if (enablePeriodicPing()) {
      Function<Channel, Http2PingSendHandler> http2PingSendHandlerFn = channel -> {
        Pool pool = FixedChannelPoolFactory.getChannelPoolHandler(channel);
        return pool != null ? pool.getHttp2PingSendHandler() : null;
      };
      _channelPoolFactory.setHttp2PingSendHandlerFunction(http2PingSendHandlerFn);
    }
  }

  public synchronized void startPeriodicPing() {
    if (enablePeriodicPing() && _periodicPing == null) {
      _periodicPing =
          _workerEventLoopGroup.scheduleAtFixedRate(this::sendPing, 0, _pingIntervalSeconds, TimeUnit.SECONDS);
    }
  }

  public void sendPing() {
    getPools().forEach(pool -> _workerEventLoopGroup.execute(pool::sendPing));
  }

  public boolean enablePeriodicPing() {
    return _pingIntervalSeconds > 0;
  }

  public Collection<Pool> getPools() {
    return _pools.values();
  }

  public synchronized void stopPeriodicPing() {
    if (_periodicPing != null) {
      _periodicPing.cancel(false);
      _periodicPing = null;
    }
  }

  @Override
  public int subpoolCount() {
    return _eventLoopConcurrency.getAsInt();
  }

  @Override
  public int activeCount() {
    return _globalActiveCount.intValue();
  }

  @Override
  public int openConnections() {
    return _globalChannelCount.intValue();
  }

  @Nonnull
  protected CallTracker createHostAcquireCallTracker(@Nonnull String hostAndPort) {
    return NullCallTracker.INSTANCE;
  }

  @Nonnull
  protected CallTracker createHostBusyCallTracker(@Nonnull String hostAndPort) {
    return NullCallTracker.INSTANCE;
  }

  @Nonnull
  @Deprecated
  protected CallTracker createQueueAcquireCallTracker(@Nonnull String queueName) {
    return NullCallTracker.INSTANCE;
  }

  @Nonnull
  @Deprecated
  protected CallTracker createQueueBusyCallTracker(@Nonnull String queueName) {
    return NullCallTracker.INSTANCE;
  }

  @Override
  @Nonnull
  public Optional<PoolStats> getPoolStats(@Nonnull String hostAndPort) {
    return Optional.ofNullable(_pools.get(hostAndPort)).map(Pool::poolStats);
  }

  @Override
  @Nonnull
  public Map<String, PoolStats> getPoolStats() {
    return new ArrayList<>(_pools.values()).stream()
        .map(Pool::poolStats)
        .collect(Collectors.toMap(PoolStats::name, Function.identity()));
  }

  public void setEnableFairScheduling(@Nonnull BooleanSupplier enableFairScheduling) {
    _enableFairScheduling = enableFairScheduling;
  }

  public void setEnableDeferredExecution(@Nonnull BooleanSupplier enableDeferredExecution) {
    _enableDeferredExecution = enableDeferredExecution;
  }

  public void setAllChannelsHandler(@Nonnull AllChannelsHandler allChannelsHandler) {
    _allChannelsHandler = allChannelsHandler;
  }

  private class Pool implements ChannelPoolHandler, PoolStats {
    final String _hostAndPort;
    final InetSocketAddress _address;
    final Set<ThreadQueue> _all = new CopyOnWriteArraySet<>();
    // Single Queue for all threads.
    private final ThreadQueue _globalThreadQueue;
    private final ThreadLocal<ThreadQueue> _local;
    private final LongAdder _activeCount = new LongAdder();
    private final LongAdder _createCount = new LongAdder();
    private final LongAdder _closeCount = new LongAdder();
    private final LongAdder _waitingCount = new LongAdder();
    private final LongAdder _inFlightCount = new LongAdder();
    private final LongAdder _closeErrorCount = new LongAdder();
    private final LongAdder _closeBadCount = new LongAdder();

    private final Http2PingHelper _http2PingHelper;
    private Iterator<ThreadQueue> _channelPoolIterator = Collections.emptyIterator();
    private boolean _isClosing = false;

    private final ChannelFutureListener _closeCountListener = future -> {
      if (!future.isSuccess()) {
        _closeErrorCount.increment();
      } else if (Boolean.TRUE.equals(future.channel().attr(FAILED_HEALTH_CHECK).get())) {
        _closeBadCount.increment();
      }
      channelClosed(future.channel());
    };

    private final CallTracker _acquireCallTracker;
    private final CallTracker _busyCallTracker;

    Pool(InetSocketAddress address) {
      assert address.isUnresolved();
      _address = address;
      _hostAndPort = address.getHostString() + ":" + address.getPort();
      _acquireCallTracker = createHostAcquireCallTracker(_hostAndPort);
      _busyCallTracker = createHostBusyCallTracker(_hostAndPort);
      if (enablePeriodicPing()) {
        _http2PingHelper = new Http2PingHelper();
      } else {
        _http2PingHelper = null;
      }
      if (_useH2GlobalPool) {
        _globalThreadQueue = newThreadQueue();
        _local = null;
      } else {
        _local = ThreadLocal.withInitial(this::newThreadQueue);
        _globalThreadQueue = null;
      }
    }

    private void initialize() {
      if (!_useH2GlobalPool) {
        ChannelPool channelPool = _local.get()._pool;
        channelPool.acquire().addListener((Future<Channel> future) -> {
          if (future.isSuccess()) {
            channelPool.release(future.getNow());
          }
        });
      }
    }

    private Channel getHttp2ChannelToPingFromChannelPool() {
      if (!isHealthy() || isClosing()) {
        return null;
      }
      if (_useH2GlobalPool) {
        return _globalThreadQueue.getHttp2ChannelToPingFromChannelGroup();
      }
      synchronized (this) {
        if (!_channelPoolIterator.hasNext()) {
          _channelPoolIterator = _all.iterator();
        }
        return _channelPoolIterator.hasNext()
            ? _channelPoolIterator.next().getHttp2ChannelToPingFromChannelGroup()
            : null;
      }
    }

    @Nonnull
    PoolStats poolStats() {
      return this;
    }

    public Map<String, ThreadPoolStats> getThreadPoolStats() {
      Map<String, ThreadPoolStats> map = new HashMap<>();
      for (ThreadQueue threadQueue: _all) {
        map.put(threadQueue._threadName, threadQueue);
      }
      return map;
    }

    private void sendPing() {
      if (_http2PingHelper != null) {
        Channel channel = getHttp2ChannelToPingFromChannelPool();
        if (channel != null && channel.isActive()) {
          _http2PingHelper.sendPing(channel);
        }
      }
    }

    public Http2PingSendHandler getHttp2PingSendHandler() {
      return _http2PingHelper == null ? null : _http2PingHelper.getHttp2PingSendHandler();
    }

    @Override
    public double getAvgResponseTimeOfLatestPings() {
      return _http2PingHelper == null ? 0 : _http2PingHelper.getAvgResponseTimeOfLatestPings();
    }

    @Override
    public long totalActiveStreamCounts() {
      return getThreadPoolStats().entrySet()
          .stream()
          .map(Map.Entry::getValue)
          .mapToLong(ThreadPoolStats::getActiveStreamCount)
          .sum();
    }

    @Override
    public long currentStreamChannelsReused() {
      return getThreadPoolStats().values().stream().mapToLong(ThreadPoolStats::getActiveStreamChannelReUsed).sum();
    }

    @Override
    public long totalStreamChannelsReused() {
      return getThreadPoolStats().values().stream().mapToLong(ThreadPoolStats::getStreamChannelReUsedCount).sum();
    }

    @Override
    public long totalStreamCreations() {
      return getThreadPoolStats().values().stream().mapToLong(ThreadPoolStats::getTotalStreamCreations).sum();
    }

    @Override
    public long totalChannelReusePoolSize() {
      return getThreadPoolStats().values().stream().mapToLong(ThreadPoolStats::getChannelReusePoolSize).sum();
    }

    @Override
    public long getActiveStreamsLimitReachedCount() {
      return getThreadPoolStats().values().stream().mapToLong(ThreadPoolStats::getActiveStreamsLimitReachedCount).sum();
    }

    @Override
    public long getTotalAcquireRetries() {
      return getThreadPoolStats().values().stream().mapToLong(ThreadPoolStats::getTotalAcquireRetries).sum();
    }

    @Override
    public long getTotalActiveStreamChannels() {
      return getThreadPoolStats().values().stream().mapToLong(ThreadPoolStats::getTotalActiveStreamChannels).sum();
    }

    Future<Void> close() {
      _isClosing = true;
      Future[] futures = _all.stream().map(ThreadQueue::close).toArray(Future[]::new);
      return Futures.allOf(futures);
    }

    private ThreadQueue newThreadQueue() {
      ThreadQueue pool = new ThreadQueue(_address, this);
      _all.add(pool);
      return pool;
    }

    private ThreadQueue get() {
      return _useH2GlobalPool ? _globalThreadQueue : _local.get();
    }

    private void channelClosed(Channel ch) {
      _log.debug("channelClosed({}/{})", ch.id(), ch.eventLoop());
      _closeCount.increment();
      _globalChannelCount.decrement();
      channelReleased(ch);
    }

    void incrementWait() {
      _waitingCount.increment();
    }

    void incrementDone() {
      _waitingCount.decrement();
    }

    void incrementInFlight() {
      _inFlightCount.increment();
    }

    void decrementInFlight() {
      _inFlightCount.decrement();
    }

    @Override
    public void channelReleased(Channel ch) {
      // TODO: Uncomment after we fix logging
      // _log.debug("channelReleased({}/{})", ch.id(), ch.eventLoop());
      complete(Time.nanoTime(), ch.attr(_busyKey).getAndSet(null)); // atomic
    }

    void complete(long now, Supplier<CallCompletion> callCompletion) {
      if (callCompletion != null) {
        _activeCount.decrement();
        _globalActiveCount.decrement();

        // Offload the call graphs to a different thread
        CHANNEL_RELEASED_EXECUTOR.get().execute(() -> callCompletion.get().close(now));
      }
    }

    @Override
    public void channelAcquired(Channel ch) {
      // _log.debug("channelAcquired({}/{})", ch.id(), ch.eventLoop());
    }

    @Override
    public void channelCreated(Channel ch) {
      if (!(ch instanceof Http2StreamChannel)) {
        _log.debug("channelCreated({}/{})", ch.id(), ch.eventLoop());
        _createCount.increment();
        _globalChannelCount.increment();
        ch.closeFuture().addListener(_closeCountListener);
      }
    }

    @Override
    @Nonnull
    public String name() {
      return _hostAndPort;
    }

    @Override
    public int activeCount() {
      int h2ActiveCount = 0;
      boolean isUsingHttp2Connections = false;
      for (ThreadPoolStats stats: getThreadPoolStats().values()) {
        int h2Active = stats.getH2ActiveConnections();
        if (h2Active > -1) {
          h2ActiveCount += h2Active;
          isUsingHttp2Connections = true;
        }
      }
      return isUsingHttp2Connections ? h2ActiveCount : _activeCount.intValue();
    }

    @Override
    public SocketAddress remoteAddress() {
      return _address;
    }

    @Override
    public long createCount() {
      return _createCount.longValue();
    }

    @Override
    public long closeCount() {
      return _closeCount.longValue();
    }

    @Override
    public long closeErrorCount() {
      return _closeErrorCount.longValue();
    }

    @Override
    public long closeBadCount() {
      return _closeBadCount.longValue();
    }

    @Override
    public int inFlightCount() {
      return _inFlightCount.intValue();
    }

    @Override
    public int openConnections() {
      return getThreadPoolStats().entrySet()
          .stream()
          .map(Map.Entry::getValue)
          .mapToInt(ThreadPoolStats::getConnectedChannels)
          .sum();
    }

    @Override
    public boolean isHealthy() {
      for (ThreadQueue q: _all.toArray(new ThreadQueue[0])) {
        // The concurrentHashMap can't have a null q.
        if (!q.isHealthy()) {
          return false;
        }
      }
      return true;
    }

    public boolean isClosing() {
      return _isClosing;
    }

    @Override
    public int waitingCount() {
      return _waitingCount.intValue();
    }

    @Override
    @Nonnull
    public CallTracker acquireCallTracker() {
      return _acquireCallTracker;
    }

    @Override
    @Nonnull
    public CallTracker busyCallTracker() {
      return _busyCallTracker;
    }

    @Override
    public CallTracker http2PingCallTracker() {
      return _http2PingHelper == null ? NullCallTracker.INSTANCE : _http2PingHelper.pingCallTracker();
    }

    @Override
    public String toString() {
      return "activeCount=" + activeCount() + ", openConnections=" + openConnections() + ", waitingCount="
          + waitingCount();
    }
  }

  /**
   * To ensure fairness of scheduling connections to all the DB's. ThreadQueue maintains a circular buffer of DB queues (PoolQueue).
   *
   * (DB3) PQ --> PQ (DB1)
   *        ^      ^
   *        |      |
   *        PQ --> PQ  (DB2)
   *        /
   *       TQ (One per ioWorker / in case of a global pool - one per SN)
   *
   *  ThreadQueue - Maintains a map <DBName, PoolQueue>
   *  PoolQueue - Maintains a map <QOS, Promise<Channel>>
   */
  private class ThreadQueue implements ThreadPoolStats {
    final ManagedChannelPool _pool;
    final Map<String, PoolQueue> _perDBPoolQueue = _useH2GlobalPool ? new ConcurrentHashMap<>() : new HashMap<>();
    Supplier<Future<Void>> _close;
    private PoolQueue _head;
    private AtomicInteger _inFlight = new AtomicInteger();
    private final String _threadName;
    private Iterator<Channel> _channelIterator = Collections.emptyIterator();

    ThreadQueue(InetSocketAddress address, Pool pool) {
      _threadName = Thread.currentThread().getName();
      EventLoopGroup eventLoopGroup = Objects.requireNonNull(localThreadGroup().syncUninterruptibly().getNow());
      EventLoop eventLoop = eventLoopGroup.next();
      /*
        When using a H2 global pool, using BalancedEventLoopGroup ensures the connections are distributed across
        all the worker threads evenly.
       */
      _pool = _channelPoolFactory.construct(
          ChannelPoolManagerImpl.this,
          pool,
          _createConnectionsOnWorkerGroup
              ? (_allChannelsHandler != null
                  ? new BalancedEventLoopGroup(_workerEventLoopGroup, _allChannelsHandler)
                  : _workerEventLoopGroup)
              : eventLoopGroup,
          Objects.requireNonNull(address));

      Supplier<Future<Void>> closeNow = Lazy.of(() -> {
        _pool.close();
        return _pool.closeFuture();
      });

      _close = Lazy.of(() -> {
        Promise<Void> future = IMMEDIATE.newPromise();
        CompletableFuture.supplyAsync(closeNow, eventLoop).whenComplete(Futures.completeFuture(future));
        return future;
      });

      eventLoop.terminationFuture().addListener(future -> {
        _close = () -> COMPLETED_VOID_FUTURE;
        closeNow.get();
      });
    }

    boolean isHealthy() {
      return _pool.isHealthy();
    }

    private Channel getHttp2ChannelToPingFromChannelGroup() {
      if (!_pool.isHealthy() || _pool.isClosing()) {
        return null;
      }
      synchronized (this) {
        if (!_channelIterator.hasNext()) {
          ChannelGroup channelGroup = _pool.getHttp2ChannelGroup();
          if (channelGroup == null) {
            return null;
          }
          _channelIterator = channelGroup.iterator();
        }
        return _channelIterator.hasNext() ? _channelIterator.next() : null;
      }
    }

    @Override
    public int getMaxConnections() {
      return _pool.getMaxConnections();
    }

    @Override
    public int getMaxPendingAcquires() {
      return _pool.getMaxPendingAcquires();
    }

    @Override
    public int getAcquiredChannelCount() {
      return _pool.getAcquiredChannelCount();
    }

    @Override
    public int getPendingAcquireCount() {
      return _pool.getPendingAcquireCount();
    }

    @Override
    public long getActiveStreamCount() {
      return _pool.getTotalActiveStreams();
    }

    @Override
    public long getActiveStreamChannelReUsed() {
      return _pool.getCurrentStreamChannelsReused();
    }

    @Override
    public long getStreamChannelReUsedCount() {
      return _pool.getTotalStreamChannelsReused();
    }

    @Override
    public long getTotalStreamCreations() {
      return _pool.getTotalStreamCreations();
    }

    @Override
    public long getChannelReusePoolSize() {
      return _pool.getChannelReusePoolSize();
    }

    @Override
    public long getActiveStreamsLimitReachedCount() {
      return _pool.getActiveStreamsLimitReachedCount();
    }

    @Override
    public long getTotalAcquireRetries() {
      return _pool.getTotalAcquireRetries();
    }

    @Override
    public long getTotalActiveStreamChannels() {
      return _pool.getTotalActiveStreamChannels();
    }

    @Override
    public boolean isClosed() {
      return _pool.isClosed();
    }

    @Override
    public int getConnectedChannels() {
      return _pool.getConnectedChannels();
    }

    @Override
    public int getH2ActiveConnections() {
      return _pool.getH2ActiveConnections();
    }

    public boolean isClosing() {
      return _pool.isClosing();
    }

    Future close() {
      return _close.get();
    }

    PoolQueue get(String queueName) {
      return _perDBPoolQueue.computeIfAbsent(queueName, this::newPoolQueue);
    }

    PoolQueue newPoolQueue(String queueName) {
      return new PoolQueue(this, _pool, _useH2GlobalPool);
    }

    // Randomly chooses a DB from the circular buffer and assigns the connection to a promise in the queue.
    void dispatch(Pool pool, Future<Channel> future) {
      PoolQueue oldHead = _head;

      if (future.isSuccess()) {
        future.getNow().attr(_ownerKey).setIfAbsent(_pool);
      }

      do { // Loop through the circular buffer of DB's
        PoolQueue head = _head;
        _head = _head._next;
        for (QOS qos: qosArray()) {
          Queue<PromiseHolder> queue = head._qosQueueEnumMap.get(qos);
          PromiseHolder promise;
          while (queue != null && (promise = queue.poll()) != null) { // SUPPRESS CHECKSTYLE InnerAssignment
            head.incrementDone();
            pool.incrementDone();
            if (future.isSuccess()) {
              if (promise.trySuccess(future.getNow())) {
                return;
              }
            } else if (promise.tryFailure(mapException(future.cause()))) {
              return;
            }
            // loop to try the next promise
          }
          // loop to try the next QOS
        }
        // loop to try the next DB
      } while (oldHead != _head); // Loops through the circular buffer once

      if (future.isSuccess()) {
        _log.debug("Returning connection immediately to pool: {}", future.getNow().id());
        _pool.release(future.getNow());
      } else {
        _log.debug("Failed to propagate exception", future.cause());
      }
    }

    void clearDoneWaiters(Pool pool) {
      _perDBPoolQueue.values()
          .forEach(poolQueue -> poolQueue._qosQueueEnumMap.values().forEach(queue -> queue.removeIf(promise -> {
            if (promise.isDone()) {
              poolQueue.incrementDone();
              pool.incrementDone();
              return true;
            }
            return false;
          })));
    }
  }

  /**
   * This class holds the promise which is returned to the caller.
   * When the channel is successfully transferred to the caller, we start busy call tracker
   * at the same time.
   */
  private final class PromiseHolder implements Runnable {
    private final Promise<Channel> _promise;
    private final Supplier<CallCompletion> _completion;
    private final LongAdder _activeCount;
    private final Pool _pool;
    private long _completionTime;
    private Supplier<CallCompletion> _busyCallCompletion;

    private PromiseHolder(
        @Nonnull Promise<Channel> promise,
        @Nonnull Supplier<CallCompletion> completion,
        LongAdder activeCount,
        Pool pool) {
      _promise = promise;
      _completion = Lazy.of(completion);
      _activeCount = activeCount;
      _pool = pool;
    }

    boolean isDone() {
      return _promise.isDone();
    }

    private CallCompletion completion() {
      return _completion.get() != null ? _completion.get() : CallTracker.nullTracker().startCall();
    }

    Future<Channel> getFuture() {
      completion();
      if (!_promise.isSuccess()) {
        /** If the promise is killed, before it can get a connection. This usually happens, if the request timeout
         before it can get a connection. */
        _promise.addListener(f -> {
          if (!f.isSuccess()) {
            closeCompletionWithError(Time.nanoTime(), f.cause());
          }
        });
      }
      return _promise;
    }

    private boolean closeCompletionWithError(long now, Throwable ex) {
      completion().closeWithError(now, ex);
      return true;
    }

    public void run() {
      _busyCallCompletion.get();
      completion().close(_completionTime);
    }

    boolean trySuccess(Channel channel) {
      if (isDone()) {
        return false;
      }
      long now = Time.nanoTime();
      debugAcquireInTrySuccess(_promise);
      _activeCount.increment();
      _globalActiveCount.increment();
      Supplier<CallCompletion> supplier = Lazy.of(() -> _pool.busyCallTracker().startCall(now));
      _pool.complete(now, channel.attr(_busyKey).getAndSet(supplier));
      if (_promise.trySuccess(channel)) {
        _completionTime = now;
        _busyCallCompletion = supplier;
        CHANNEL_RELEASED_EXECUTOR.get().execute(this); // calls run()
        return true;
      } else {
        channel.attr(_busyKey).set(null);
        _activeCount.decrement();
        _globalActiveCount.decrement();
        return false;
      }
    }

    boolean tryFailure(Throwable ex) {
      long now = Time.nanoTime();
      return _promise.tryFailure(ex) && closeCompletionWithError(now, ex);
    }
  }

  /**
   * Instead of having a final reference to a LongAdder for waitCount, we simply make the PoolQueue
   * class extend LongAdder and save on an object reference.
   */
  private static class PoolQueue extends LongAdder {
    final ChannelPool _pool;
    final EnumMap<QOS, Queue<PromiseHolder>> _qosQueueEnumMap = new EnumMap<>(QOS.class);
    boolean _useH2GlobalPool;

    PoolQueue _prev = this;
    PoolQueue _next = this;

    private PoolQueue(ThreadQueue threadPool, ChannelPool pool, boolean useH2GlobalPool) {
      this(threadPool, pool);
      _useH2GlobalPool = useH2GlobalPool;
    }

    private PoolQueue(ThreadQueue threadPool, ChannelPool pool) {
      _pool = pool;

      if (threadPool._head != null) {
        _prev = threadPool._head;
        _next = _prev._next;
        _prev._next = this;
        _next._prev = this;
      }
      threadPool._head = this;
      reset();
    }

    boolean add(QOS qos, PromiseHolder promise) {
      return _qosQueueEnumMap.computeIfAbsent(qos, this::newQueue).add(promise);
    }

    private Queue<PromiseHolder> newQueue(QOS qos) {
      return _useH2GlobalPool ? new ConcurrentLinkedDeque<>() : new LinkedList<>();
    }

    void incrementWait() {
      // waitCount is number of waiters for a IoWorker per DB
      increment();
    }

    void incrementDone() {
      // waitCount is number of waiters for a IoWorker per DB
      decrement();
    }

    int waitingCount() {
      return intValue();
    }
  }

  @Nonnull
  private QOS[] qosArray() {
    // To ensure fairness for QOS LOW
    ThreadLocalRandom random = ThreadLocalRandom.current();
    int percent = random.nextInt(100);

    if (percent < 80) {
      return QOS_HIGH_NORMAL_LOW;
    } else if (percent < 95) {
      return QOS_NORMAL_HIGH_LOW;
    } else {
      return QOS_LOW_HIGH_NORMAL;
    }
  }

  @Nonnull
  protected InetSocketAddress createUnresolved(@Nonnull String hostAndPort) {
    int pos = hostAndPort.indexOf(':');
    if (pos == -1) {
      throw new IllegalArgumentException("Bad hostname. Should be host:port.");
    }

    String host = hostAndPort.substring(0, pos);
    int port = Integer.parseInt(hostAndPort.substring(pos + 1));
    return InetSocketAddress.createUnresolved(host, port);
  }

  @Nonnull
  protected InetSocketAddress createUnresolved(@Nonnull InetSocketAddress inetSocketAddress) {
    if (inetSocketAddress.isUnresolved()) {
      return inetSocketAddress;
    } else {
      return InetSocketAddress.createUnresolved(inetSocketAddress.getHostString(), inetSocketAddress.getPort());
    }
  }

  @Override
  @Nonnull
  public Future<Void> close(@Nonnull String hostName) {
    return close0(hostName);
  }

  private Future<Void> close0(@Nonnull String hostName) {
    Pool pool = _pools.remove(hostName);
    if (pool == null) {
      return COMPLETED_VOID_FUTURE;
    } else {
      return pool.close();
    }
  }

  @Override
  @Nonnull
  public Future<Void> closeAll() {
    // stop the periodic ping if all host pools are closed
    stopPeriodicPing();
    return Futures.asNettyFuture(
        CompletableFuture.completedFuture(new ArrayList<>(_pools.keySet()))
            .thenCompose(new Function<List<String>, CompletionStage<Void>>() {
              @Override
              public CompletionStage<Void> apply(List<String> poolNames) {
                if (poolNames.isEmpty()) {
                  return CompletableFuture.completedFuture(null);
                }

                return CompletableFuture
                    .allOf(
                        poolNames.stream()
                            .map(ChannelPoolManagerImpl.this::close)
                            .map(Futures::asCompletableFuture)
                            .toArray(CompletableFuture[]::new))
                    .thenCompose(aVoid -> {
                      CompletableFuture<Void> delay = new CompletableFuture<>();
                      _localThreadGroup.schedule(() -> delay.complete(null), CLOSE_ALL_DELAY, TimeUnit.MILLISECONDS);
                      return delay;
                    })
                    .thenCompose(
                        (aVoid -> CompletableFuture.completedFuture(new ArrayList<>(_pools.keySet()))
                            .thenCompose(this)));
              }
            }));
  }

  protected Future<EventLoopGroup> localThreadGroup() {
    return _localThreadGroup.singleThreadGroup();
  }

  protected Future<EventLoopGroup> localThreadGroup(@Nonnull EventLoop eventLoop) {
    return _localThreadGroup.singleThreadGroup(eventLoop);
  }

  @Override
  @Nonnull
  public Future<Channel> acquire(@Nonnull String hostName, @Nonnull String queueName, @Nonnull QOS qos) {
    long startTime = Time.nanoTime();
    Future<EventLoopGroup> group = localThreadGroup();
    return acquire(
        startTime,
        hostName,
        Objects.requireNonNull(queueName, "queueName"),
        Objects.requireNonNull(qos, "qos"),
        group,
        IMMEDIATE.newPromise());
  }

  @Override
  @Nonnull
  public Future<Channel> acquire(
      @Nonnull EventLoop eventLoop,
      @Nonnull String hostName,
      @Nonnull String queueName,
      @Nonnull QOS qos) {
    long startTime = Time.nanoTime();
    Future<EventLoopGroup> group = localThreadGroup(eventLoop);
    return acquire(
        startTime,
        hostName,
        Objects.requireNonNull(queueName, "queueName"),
        Objects.requireNonNull(qos, "qos"),
        group,
        eventLoop.newPromise());
  }

  private Future<Channel> acquire(
      long startTime,
      String hostName,
      String queueName,
      QOS qos,
      Future<EventLoopGroup> group,
      Promise<Channel> promise) {

    // Check if the pool already exists
    Pool pool = _pools.get(hostName);
    if (pool != null) {
      return acquireWithHostName(startTime, hostName, pool, queueName, qos, group, promise);
    }

    // Pool doesn't exist, resolve the host to IP and create the pool
    InetSocketAddress unresolved = createUnresolved(hostName);

    Future<InetSocketAddress> addressFuture = _channelPoolResolver
        .resolve(unresolved, group.isSuccess() ? group.getNow().next().newPromise() : IMMEDIATE.newPromise());

    if (addressFuture.isSuccess()) {
      return acquireResolved(startTime, hostName, addressFuture.getNow(), queueName, qos, group, promise);
    } else {
      addressFuture.addListener((Future<InetSocketAddress> future) -> {
        if (future.isSuccess()) {
          acquireResolved(startTime, hostName, future.getNow(), queueName, qos, group, promise);
        } else {
          _log.log(
              promise.tryFailure(future.cause()) ? Level.DEBUG : Level.INFO,
              "Unable to resolve {}",
              unresolved,
              future.cause());
        }
      });
      return promise;
    }
  }

  private Future<Channel> acquireWithHostName(
      long startTime,
      String hostName,
      Pool pool,
      String queueName,
      QOS qos,
      Future<EventLoopGroup> group,
      Promise<Channel> promise) {

    if (group.isSuccess()) {
      return acquireResolved(startTime, pool, queueName, qos, group.getNow(), promise);
    } else {
      group.addListener((Future<EventLoopGroup> future) -> {
        if (group.isSuccess()) {
          acquireResolved(startTime, pool, queueName, qos, future.getNow(), promise);
        } else {
          pool._acquireCallTracker.trackCallWithError(Time.nanoTime() - startTime);
          if (!promise.tryFailure(group.cause())) {
            _log.debug("acquire failure {} {} {}", hostName, queueName, qos, group.cause());
          }
        }
      });
      return promise;
    }
  }

  private Future<Channel> acquireResolved(
      long startTime,
      String hostName,
      InetSocketAddress address,
      String queueName,
      QOS qos,
      Future<EventLoopGroup> group,
      Promise<Channel> promise) {

    if (address.isUnresolved()) {
      Exception exception = new UnknownHostException(address.getHostString());
      _log.log(promise.tryFailure(exception) ? Level.DEBUG : Level.INFO, "Unresolved hostname", exception);
      return promise;
    }

    Pool pool = CollectionUtil.computeIfAbsent(
        _pools,
        hostName,
        (k) -> new Pool(createUnresolved(address)),
        (key, newPool) -> _runInEveryThread.accept(newPool::initialize));

    if (group.isSuccess()) {
      return acquireResolved(startTime, pool, queueName, qos, group.getNow(), promise);
    } else {
      group.addListener((Future<EventLoopGroup> future) -> {
        if (group.isSuccess()) {
          acquireResolved(startTime, pool, queueName, qos, future.getNow(), promise);
        } else {
          pool._acquireCallTracker.trackCallWithError(Time.nanoTime() - startTime);
          if (!promise.tryFailure(group.cause())) {
            _log.debug("acquire failure {} {} {}", address.getHostString(), queueName, qos, group.cause());
          }
        }
      });
      return promise;
    }
  }

  private Future<Channel> acquireResolved(
      long startTime,
      Pool pool,
      String queueName,
      QOS qos,
      EventLoopGroup group,
      Promise<Channel> promise) {

    debugAcquireResolved(promise);
    if (promise.isDone()) {
      _log.debug("promise completed before acquire");
      return promise;
    }

    EventLoop loop = group.next();

    if (loop.inEventLoop()) {
      return acquireInEventLoop(startTime, loop, pool, queueName, qos, promise);
    } else {
      loop.execute(() -> acquireInEventLoop(startTime, loop, pool, queueName, qos, promise));
      return promise;
    }
  }

  private Future<Channel> acquireInEventLoop(
      long startTime,
      EventLoop loop,
      Pool pool,
      String queueName,
      QOS qos,
      Promise<Channel> promise) {

    debugAcquireInEventLoop(promise);
    if (promise.isDone()) {
      _log.debug("promise completed before acquire");
      return promise;
    }

    // Get thread local pool or Global Pool for the SN
    ThreadQueue threadQueue = pool.get();
    // Get the queue for the DB
    PoolQueue queue = threadQueue.get(_enableFairScheduling.getAsBoolean() ? queueName : DEFAULT_DB_QUEUE);

    pool.incrementWait();
    queue.incrementWait();

    PromiseHolder promiseHolder =
        new PromiseHolder(promise, () -> pool.acquireCallTracker().startCall(startTime), pool._activeCount, pool);

    if (threadQueue._inFlight.get() > _maxWaitersPerPool / 2) {
      threadQueue.clearDoneWaiters(pool);
    }

    /*
      Simple acquire, avoids all the Pool and Thread Queue complexities when acquiring a connection.
     */
    if (_useH2GlobalPool && _enableSimpleAcquire) {
      Promise<Channel> acquirePromise = IMMEDIATE.newPromise();
      threadQueue._pool.acquire(acquirePromise);
      acquirePromise.addListener((GenericFutureListener<Future<Channel>>) channelFuture -> {
        pool.incrementDone();
        queue.incrementDone();
        if (channelFuture.isSuccess()) {
          channelFuture.getNow().attr(_ownerKey).setIfAbsent(threadQueue._pool);
          promiseHolder.trySuccess(channelFuture.getNow());
        } else {
          promiseHolder.tryFailure(channelFuture.cause());
        }
      });
      return promiseHolder.getFuture();
    }

    // A request to acquire a connection is always added to a queue of the corresponding DB.
    if (threadQueue._inFlight.get() < _maxWaitersPerPool && queue.add(qos, promiseHolder)) {
      class Listener implements FutureListener<Channel>, Runnable {
        private final Executor deferred = _enableDeferredExecution.getAsBoolean()
            ? ((SingleThreadEventLoop) loop)::executeAfterEventLoopIteration
            : loop::execute;

        public void run() {
          debugAcquireInEventLoopListener(promiseHolder.getFuture());
          Promise<Channel> acquirePromise = loop.newPromise();
          acquirePromise.addListener(this);
          deferred.execute(() -> threadQueue._pool.acquire(acquirePromise));
        }

        @Override
        public void operationComplete(Future<Channel> future) throws Exception {
          decrementInFlightCount(threadQueue, pool);
          if (!future.isSuccess() && pool.waitingCount() > 0) {
            if (isReschedulableError(future.cause())) {
              _log.debug("Retrying because {}", future.cause().getMessage());
              threadQueue.clearDoneWaiters(pool);
              int waiters = threadQueue._perDBPoolQueue.values().stream().mapToInt(PoolQueue::waitingCount).sum();
              // Reschedule only if the waiters are greater than the number of requests inflight.
              if (threadQueue._inFlight.get() < waiters) {
                // small back-off
                incrementInFlightCount(pool, threadQueue);
                loop.schedule(this, 100, TimeUnit.MICROSECONDS);
              }
              return;
            }
            _log.debug("Exception in acquire", future.cause());
          }
          // when a connection is available, the dispatch assigns the connection to random DB.
          threadQueue.dispatch(pool, future);
        }
      }
      incrementInFlightCount(pool, threadQueue);
      new Listener().run();
    } else {
      pool.incrementDone();
      queue.incrementDone();

      Exception exception = UNABLE_TO_ENQUEUE_REQUEST_EXCEPTION;
      _log.warn(
          "Unable to acquire connection for SN {}, db {}, inflight {}, waitingCount {}, maxWaiterPerPool {}, ioWorker {}",
          pool._hostAndPort,
          queueName,
          threadQueue._inFlight,
          queue.waitingCount(),
          _maxWaitersPerPool,
          Thread.currentThread().getName());
      promiseHolder.tryFailure(exception);
    }
    return promiseHolder.getFuture();
  }

  private static boolean isReschedulableError(Throwable t) {
    return t instanceof TimeoutException || Http2Utils.isTooManyActiveStreamsError(t);
  }

  private void incrementInFlightCount(Pool pool, ThreadQueue threadQueue) {
    pool.incrementInFlight();
    threadQueue._inFlight.incrementAndGet();
  }

  private void decrementInFlightCount(ThreadQueue threadQueue, Pool pool) {
    threadQueue._inFlight.decrementAndGet();
    pool.decrementInFlight();
  }

  static Throwable mapException(Throwable ex) {
    while ((ex instanceof CompletionException || ex instanceof RuntimeException) && ex.getCause() != null
        && ex.getCause() != ex) {
      ex = ex.getCause();
    }
    if (ex instanceof IllegalStateException && String.valueOf(ex.getMessage()).startsWith("Too many")) {
      return TOO_MANY_REQUESTS_EXCEPTION;
    }
    if (ex instanceof IllegalStateException && String.valueOf(ex.getMessage()).endsWith("was closed")) {
      return POOL_CLOSED_EXCEPTION;
    }
    return ex;
  }

  @Override
  @Nonnull
  public Future<Void> release(@Nonnull Channel channel) {
    ChannelPool pool = Objects.requireNonNull(channel.attr(_ownerKey).get());
    EventLoop eventLoop = channel.eventLoop();
    Promise<Void> promise = eventLoop.newPromise();
    if (eventLoop.inEventLoop()) {
      return release0(pool, channel, promise);
    } else {
      eventLoop.submit(() -> release0(pool, channel, promise));
    }
    return promise;
  }

  private Future<Void> release0(ChannelPool pool, Channel channel, Promise<Void> promise) {
    try {
      return pool.release(channel, promise);
    } catch (Exception ex) {
      if (!promise.tryFailure(ex)) {
        _log.warn("Exception in release:", ex);
      }
      return promise;
    }
  }

  /**
   * Used for unit-test verification
   */
  void debugAcquireResolved(Future<Channel> promise) {
  }

  /**
   * Used for unit-test verification
   */
  void debugAcquireInEventLoop(Future<Channel> promise) {
  }

  /**
   * Used for unit-test verification
   */
  void debugAcquireInEventLoopListener(Future<Channel> promise) {
  }

  /**
   * Used for unit-test verification
   */
  void debugAcquireInTrySuccess(Future<Channel> promise) {
  }

  /**
   * Used for unit-test verification
   */
  ScheduledFuture<?> getPeriodicPingScheduledFuture() {
    return _periodicPing;
  }
}
