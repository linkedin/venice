package com.linkedin.alpini.netty4.pool;

import com.linkedin.alpini.netty4.handlers.ChannelInitializer;
import com.linkedin.alpini.netty4.handlers.HttpObjectToBasicHttpObjectAdapter;
import com.linkedin.alpini.netty4.http2.Http2StreamFrameClientCodec;
import com.linkedin.alpini.netty4.misc.Http2Utils;
import com.linkedin.alpini.netty4.misc.NettyUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.handler.codec.http2.EspressoHttp2MultiplexHandler;
import io.netty.handler.codec.http2.EspressoHttp2StreamChannelBootstrap;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2FrameCodec;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.nio.channels.ShutdownChannelGroupException;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A ChannelPool Wrapper that would return either a ChannelPool if the request is an HTTP 1.1 request or
 * A Stream if HTTP2
 */
public class Http2AwareChannelPool implements ManagedChannelPool {
  private static final Logger LOG = LogManager.getLogger(Http2AwareChannelPool.class);

  private static final AttributeKey<EspressoHttp2StreamChannelBootstrap> BOOTSTRAP_ATTRIBUTE_KEY =
      AttributeKey.valueOf(Http2AwareChannelPool.class, "http2Bootstrap");
  private static final AttributeKey<Queue<Http2StreamChannel>> RECYCLE_STREAMS_KEY =
      AttributeKey.valueOf(Http2AwareChannelPool.class, "recycleQueue");

  // Consumers of the stream channel should set this attribute to true so that it can be reused
  public static final AttributeKey<Boolean> HTTP2_STREAM_CHANNEL_AVAILABLE_FOR_REUSE =
      AttributeKey.valueOf(Http2AwareChannelPool.class, "http2StreamChannelAvailableForReuse");
  public static final AttributeKey<Boolean> HTTP2_REUSED_STREAM_CHANNEL =
      AttributeKey.valueOf(Http2AwareChannelPool.class, "http2ReusedStreamChannel");
  public static final AttributeKey<Http2Connection> HTTP2_CONNECTION =
      AttributeKey.valueOf(Http2AwareChannelPool.class, "http2Connection");

  // We maintain a ChannelGroup which contains all the stream channels created for a h2 connection.
  // This allows us to close them all should the h2 connection suddenly fails due to network glitch.
  /* package */ static final AttributeKey<ChannelGroup> STREAM_GROUP =
      AttributeKey.valueOf(Http2AwareChannelPool.class, "streamGroup");

  public static final int DEFAULT_MAX_REUSE_STREAM_CHANNELS_LIMIT = 100;
  public static final int DEFAULT_MAX_CONCURRENT_STREAMS = 500;
  // 1 initial acquire attempt + 2 retry attempts = 3 acquire attempts allowed in total
  private static final int MAX_ACQUIRE_RETRY_LIMIT = 2;

  private final ManagedChannelPool _parentPool;

  // preStreamChannelInitializer would be called before this class's own ChannelInitializer calling p.addLast, giving
  // the outsider a chance to insert any handler before Http2AwareChannelPool's own handler.
  private final Consumer<Channel> _preStreamChannelInitializer;

  // postStreamChannelInitializer would be called after this class's own ChannelInitializer calling p.addLast, giving
  // the outsider a chance to append any handler after Http2AwareChannelPool's own handler.
  private final Consumer<Channel> _postStreamChannelInitializer;

  // set only by setMoreThanOneHttp2Connection, indicates if the pool should try to
  // create more than one HTTP/2 connection.
  private boolean _moreThanOneHttp2Connection;

  // Semaphore to guard extra HTTP/2 connection creation.
  private final Semaphore _extraHttp2Connections = new Semaphore(1);

  private final ChannelHandler _basicHttpAdapter = new HttpObjectToBasicHttpObjectAdapter();

  // Channel group of HTTP/2 connections created by this pool.
  private final ChannelGroup _http2ChannelGroup;

  // Global channel group of HTTP/2 connections, this will allow us to keep track of the total number of HTTP/2
  // connections
  private final Consumer<Channel> _allHttp2ChannelGroup;

  private final LongAdder _activeStreamsLimitReachedCount = new LongAdder();
  private final LongAdder _totalStreamsReused = new LongAdder();
  private final LongAdder _totalStreamCreations = new LongAdder();
  private final LongAdder _currentStreamsReused = new LongAdder();
  private final LongAdder _totalAcquireRetries = new LongAdder();
  private final LongAdder _totalActiveStreamChannels = new LongAdder();

  // Use custom codec
  private boolean _useCustomH2Codec = false;

  private boolean _channelReuse = false;
  private boolean _retryOnMaxStreamsLimit = false;
  private int _maxReuseStreamChannelsLimit = DEFAULT_MAX_REUSE_STREAM_CHANNELS_LIMIT;
  private long _maxConcurrentStreamsLimit = DEFAULT_MAX_CONCURRENT_STREAMS;

  public Http2AwareChannelPool(
      ManagedChannelPool parentPool,
      Consumer<Channel> preStreamChannelInitializer,
      Consumer<Channel> postStreamInitializer) {
    this(parentPool, preStreamChannelInitializer, postStreamInitializer, true);
  }

  public Http2AwareChannelPool(
      ManagedChannelPool parentPool,
      Consumer<Channel> preStreamChannelInitializer,
      Consumer<Channel> postStreamInitializer,
      boolean moreThanOneHttp2Connection) {
    this(parentPool, preStreamChannelInitializer, postStreamInitializer, moreThanOneHttp2Connection, null);
  }

  public Http2AwareChannelPool(
      ManagedChannelPool parentPool,
      Consumer<Channel> preStreamChannelInitializer,
      Consumer<Channel> postStreamInitializer,
      boolean moreThanOneHttp2Connection,
      ChannelGroup http2ChannelGroup) {
    _parentPool = Objects.requireNonNull(parentPool, "parentPool");
    _preStreamChannelInitializer = Objects.requireNonNull(preStreamChannelInitializer, "preStreamChannelInitializer");
    _postStreamChannelInitializer = Objects.requireNonNull(postStreamInitializer, "postStreamInitializer");
    _moreThanOneHttp2Connection = moreThanOneHttp2Connection;
    _http2ChannelGroup = new DefaultChannelGroup(name(), ImmediateEventExecutor.INSTANCE);
    _allHttp2ChannelGroup = http2ChannelGroup != null ? http2ChannelGroup::add : ch -> {};
  }

  @Override
  public String name() {
    return _parentPool.name();
  }

  public void setMaxConcurrentStreams(long maxConcurrentStreams) {
    _maxConcurrentStreamsLimit = maxConcurrentStreams;
  }

  @Override
  public long getActiveStreamsLimitReachedCount() {
    return _activeStreamsLimitReachedCount.longValue();
  }

  public long getMaxConcurrentStreamsLimit() {
    return _maxConcurrentStreamsLimit;
  }

  public void setMoreThanOneHttp2Connection(boolean value) {
    _moreThanOneHttp2Connection = value;
  }

  public void setChannelReuse(boolean channelReuse) {
    _channelReuse = channelReuse;
  }

  @Override
  public long getTotalActiveStreams() {
    return _http2ChannelGroup.stream()
        .mapToInt(ch -> ch.pipeline().get(Http2FrameCodec.class).connection().numActiveStreams())
        .sum();
  }

  @Override
  public long getCurrentStreamChannelsReused() {
    return _currentStreamsReused.longValue();
  }

  @Override
  public long getTotalStreamChannelsReused() {
    return _totalStreamsReused.longValue();
  }

  @Override
  public long getTotalStreamCreations() {
    return _totalStreamCreations.longValue();
  }

  @Override
  public long getTotalAcquireRetries() {
    return _totalAcquireRetries.longValue();
  }

  @Override
  public long getTotalActiveStreamChannels() {
    return _totalActiveStreamChannels.longValue();
  }

  public boolean wantMoreThanOneHttp2Connection() {
    return _moreThanOneHttp2Connection;
  }

  public boolean hasMoreThanOneHttp2Connection() {
    return _http2ChannelGroup.size() > 1;
  }

  public boolean useCustomH2Codec() {
    return _useCustomH2Codec;
  }

  public void setUseCustomH2Codec(boolean useCustomH2Codec) {
    _useCustomH2Codec = useCustomH2Codec;
  }

  public void setMaxReuseStreamChannelsLimit(int maxReuseStreamChannelsLimit) {
    _maxReuseStreamChannelsLimit = maxReuseStreamChannelsLimit;
  }

  public int getMaxReuseStreamChannelsLimit() {
    return _maxReuseStreamChannelsLimit;
  }

  public void setRetryOnMaxStreamsLimit(boolean retryOnMaxStreamsLimit) {
    _retryOnMaxStreamsLimit = retryOnMaxStreamsLimit;
  }

  public boolean shouldRetryOnMaxStreamsLimit() {
    return _retryOnMaxStreamsLimit;
  }

  @Override
  public Future<Channel> acquire() {
    return acquire(ImmediateEventExecutor.INSTANCE.newPromise());
  }

  protected EspressoHttp2StreamChannelBootstrap createHttp2StreamChannelBootstrap(Channel ch) {
    ChannelGroup group = new DefaultChannelGroup(ch.eventLoop(), true);
    ch.attr(STREAM_GROUP).set(group);
    ch.closeFuture().addListener(f -> group.close());
    return new EspressoHttp2StreamChannelBootstrap(ch).handler(new ChannelInitializer<Channel>() {
      @Override
      protected void initChannel(Channel ch) {
        // commenting this line will cause TestHttp2AwareChannelPoolFactory.testHttp2WithRemoteBadConnection to fail.
        group.add(ch);
        ChannelPipeline p = ch.pipeline();
        EventExecutorGroup executorGroup = NettyUtils.executorGroup(ch);
        _preStreamChannelInitializer.accept(ch);
        if (useCustomH2Codec()) {
          p.addLast(executorGroup, "child-client-frame-converter", new Http2StreamFrameClientCodec(false));
        } else {
          p.addLast(executorGroup, "child-client-frame-converter", new Http2StreamFrameToHttpObjectCodec(false));
          p.addLast(executorGroup, "basic-http-from-http-converter", _basicHttpAdapter);
        }
        _postStreamChannelInitializer.accept(ch);
      }
    });
  }

  /**
   * Return a new recycle queue.
   *
   * If queue size is within range [0, Integer.MAX_VALUE),
   * Use array blocking queue to handle concurrency issues,
   * and limit the number of entries allowed in the queue to avoid caching too many stream channels.
   *
   * Else, use unbounded ConcurrentLinkedQueue.
   * @return New recycle queue
   */
  private Queue<Http2StreamChannel> newRecycleQueue() {
    int queueSize = getMaxReuseStreamChannelsLimit();
    return 0 <= queueSize && queueSize < Integer.MAX_VALUE
        ? new ArrayBlockingQueue<>(queueSize)
        : new ConcurrentLinkedQueue<>();
  }

  @Override
  public Future<Channel> acquire(Promise<Channel> promise) {
    if (shouldRetryOnMaxStreamsLimit()) {
      acquire0(ImmediateEventExecutor.INSTANCE.newPromise()).addListener(createAcquireRetryListener(promise));
      return promise;
    }
    return acquire0(promise);
  }

  private FutureListener<Channel> createAcquireRetryListener(Promise<Channel> returnPromise) {
    return new FutureListener<Channel>() {
      private int _retries = 0;

      @Override
      public void operationComplete(Future<Channel> future) {
        if (future.isSuccess()) {
          if (returnPromise.trySuccess(future.getNow())) {
            return;
          }
          release(future.getNow());
        } else if (Http2Utils.isTooManyActiveStreamsError(future.cause()) && _retries++ < MAX_ACQUIRE_RETRY_LIMIT
            && !returnPromise.isDone()) {
          // If the failure is caused by too many streams exception, and we have retry budget, retry acquiring
          _totalAcquireRetries.increment();
          acquire0(ImmediateEventExecutor.INSTANCE.newPromise()).addListener(this);
        } else {
          // If the failure is caused by other exceptions, don't retry
          returnPromise.tryFailure(future.cause());
        }
      }
    };
  }

  protected Promise<Channel> acquire0(Promise<Channel> promise) {
    _parentPool.acquire().addListener((Future<Channel> acquired) -> {
      if (acquired.isSuccess()) {
        final Channel ch = acquired.getNow();
        EspressoHttp2StreamChannelBootstrap http2Bootstrap;
        boolean returnChannel = true;
        try {
          if (isActiveStreamsLimitReached(promise, ch)) {
            return;
          }
          if (tryReuseStreamChannel(ch.attr(RECYCLE_STREAMS_KEY).get(), promise)) {
            return;
          }
          setupHttp2ParentChannel(ch);
          http2Bootstrap = ch.attr(BOOTSTRAP_ATTRIBUTE_KEY).get();
          if (http2Bootstrap == null) {
            returnChannel = !promise.trySuccess(ch);
            return;
          }
        } catch (Throwable ex) {
          promise.setFailure(ex);
          return;
        } finally {
          if (returnChannel) {
            _parentPool.release(ch);
          }
        }
        createStreamChannel(http2Bootstrap, promise);
      } else {
        promise.setFailure(acquired.cause());
      }
    });
    return promise;
  }

  private boolean isActiveStreamsLimitReached(Promise<Channel> promise, Channel ch) {
    if (Http2Utils.isConfiguredHttp2Connection(ch) && !Http2Utils.canOpenLocalStream(ch)) {
      tooManyActiveStreams(promise, getTotalActiveStreams());
      return true;
    }
    return false;
  }

  private void setupHttp2ParentChannel(Channel ch) {
    if (!ch.hasAttr(BOOTSTRAP_ATTRIBUTE_KEY)) {
      if (Http2Utils.isHttp2ParentChannelPipeline(ch.pipeline())) {
        addChannelToHttp2ChannelGroups(ch);
        configureHttp2Connection(ch);
        if (Http2Utils.isHttp2MultiplexPipeline(_channelReuse, ch.pipeline())) {
          ch.attr(BOOTSTRAP_ATTRIBUTE_KEY).set(createHttp2StreamChannelBootstrap(ch));
          if (_channelReuse) {
            createRecycleQueue(ch);
          }
        } else {
          // Not a multiplex pipeline, there is no bootstrap
          ch.attr(BOOTSTRAP_ATTRIBUTE_KEY).set(null);
        }
        if (wantMoreThanOneHttp2Connection() && !isClosing() && !hasMoreThanOneHttp2Connection()
            && _extraHttp2Connections.tryAcquire()) {
          createExtraHttp2Connection();
        }
      } else {
        // Not a HTTP/2 connection, Http2 bootstrap set to null
        ch.attr(BOOTSTRAP_ATTRIBUTE_KEY).set(null);
      }
    }
  }

  private void createRecycleQueue(Channel ch) {
    Queue<Http2StreamChannel> recycleQueue = newRecycleQueue();
    ch.attr(RECYCLE_STREAMS_KEY).set(recycleQueue);
  }

  private void tooManyActiveStreams(Promise<Channel> promise, long totalActiveStreams) {
    _activeStreamsLimitReachedCount.increment();
    promise.setFailure(
        Http2Utils.tooManyStreamsException(
            String.format(
                "Reached maxConcurrentStreamsLimit=%d, totalActiveStream=%d",
                getMaxConcurrentStreamsLimit(),
                totalActiveStreams)));
  }

  @Deprecated
  public static boolean isTooManyActiveStreamsError(Throwable t) {
    return Http2Utils.isTooManyActiveStreamsError(t);
  }

  private void addChannelToHttp2ChannelGroups(Channel ch) {
    _http2ChannelGroup.add(ch);
    _allHttp2ChannelGroup.accept(ch);
  }

  /**
   * Configure the HTTP/2 connection on the parent channel.
   *
   * The connection is added to the channel attributes for fast retrieval.
   * We also override the max active streams limit after SETTINGS frame has been received from remote endpoint
   * as the remote endpoint normally grants a much larger limit.
   *
   * @param ch Parent HTTP/2 channel
   */
  private void configureHttp2Connection(Channel ch) {
    Http2Connection connection = ch.pipeline().get(Http2FrameCodec.class).connection();
    ch.attr(HTTP2_CONNECTION).set(connection);

    int limit = (int) getMaxConcurrentStreamsLimit();
    LOG.info(
        "Overriding local endpoint maxActiveStreams for channel {} from {} to {}",
        ch,
        connection.local().maxActiveStreams(),
        limit);
    connection.local().maxActiveStreams(limit);
  }

  private void createExtraHttp2Connection() {
    LOG.debug("Creating extra HTTP/2 connection");
    _parentPool.acquire().addListener((Future<Channel> extra) -> {
      _extraHttp2Connections.release();
      if (extra.isSuccess()) {
        Channel ch = extra.getNow();
        // check that the pool really is a HTTP/2 connection
        if (!Http2Utils.isHttp2MultiplexPipeline(_channelReuse, ch.pipeline())) {
          LOG.warn("Extra connection was not a HTTP/2 connection to {}", ch.remoteAddress());
        }
        _parentPool.release(ch);
      } else {
        if (_parentPool.isClosing() || _parentPool.isClosed()) {
          // for closed pool, we don't have to print the exception stack
          LOG.warn(
              "Failed to create extra HTTP/2 connection because {}",
              Optional.ofNullable(extra.cause()).map(Throwable::getMessage).orElse("Unknown reason"));
        } else {
          LOG.warn("Failed to create extra HTTP/2 connection", extra.cause());
        }
      }
    });
  }

  private void createStreamChannel(EspressoHttp2StreamChannelBootstrap bootstrap, Promise<Channel> promise) {
    if (!promise.isDone()) {
      try {
        bootstrapStreamChannel(bootstrap, promise);
      } catch (Exception ex) {
        promise.setFailure(ex);
      }
    }
  }

  private void bootstrapStreamChannel(EspressoHttp2StreamChannelBootstrap bootstrap, Promise<Channel> promise) {
    bootstrap.open().addListener((Future<Http2StreamChannel> s) -> {
      if (s.isSuccess()) {
        Http2StreamChannel streamChannel = s.getNow();
        _totalStreamCreations.increment();
        boolean closeStream = true;
        try {
          if (streamChannel.isOpen()) {
            // TODO: ESPENG-38541: Make channel active with streams
            handler().channelAcquired(streamChannel);
            if (promise.trySuccess(streamChannel)) {
              closeStream = false;
              _totalActiveStreamChannels.increment();
            } else {
              handler().channelReleased(streamChannel);
              if (streamChannel.parent().hasAttr(RECYCLE_STREAMS_KEY)) {
                Http2Utils.markChannelAvailableForReuse(streamChannel);
                // If recycle fails, close the stream
                closeStream = !streamChannel.parent().attr(RECYCLE_STREAMS_KEY).get().offer(streamChannel);
              }
            }
          } else if (!promise.isDone()) {
            // Shutdown is likely if adding it to the groups caused the channel to be closed
            promise.setFailure(new ShutdownChannelGroupException());
          }
        } catch (Exception ex) {
          promise.setFailure(ex);
        } finally {
          if (closeStream) {
            Http2Utils.closeStreamChannel(streamChannel);
          }
        }
      } else {
        if (!promise.tryFailure(s.cause())) {
          LOG.warn("unhandled exception", s.cause());
        }
      }
    });
  }

  private boolean tryReuseStreamChannel(@Nullable Queue<Http2StreamChannel> queue, Promise<Channel> promise)
      throws Exception {
    if (queue == null) {
      return false;
    }
    // Loop to find a stream channel good for reuse
    for (Http2StreamChannel streamChannel = queue.poll(); streamChannel != null; streamChannel = queue.poll()) {
      boolean closeStream = true;
      try {
        // Reuse if possible
        if (streamChannel.isOpen() && Http2Utils.channelAvailableForReuse(streamChannel)) {
          ((EspressoHttp2MultiplexHandler.EspressoHttp2MultiplexHandlerStreamChannel) streamChannel).init();
          // TODO: ESPENG-38541: Make channel active with streams
          handler().channelAcquired(streamChannel);
          if (promise.trySuccess(streamChannel)) {
            markStreamForReuse(streamChannel);
            closeStream = false;
          } else {
            handler().channelReleased(streamChannel);
            closeStream = !queue.offer(streamChannel);
          }
          // If we found a good stream channel, exit
          return true;
        }
      } finally {
        if (closeStream) {
          Http2Utils.closeStreamChannel(streamChannel);
        }
      }
    }
    return false;
  }

  private void markStreamForReuse(Http2StreamChannel streamChannel) {
    Http2Utils.markChannelForReuse(streamChannel);
    _totalStreamsReused.increment();
    _currentStreamsReused.increment();
    _totalActiveStreamChannels.increment();
  }

  @Override
  public Future<Void> release(Channel channel) {
    return release(channel, ImmediateEventExecutor.INSTANCE.newPromise());
  }

  private boolean canReuseChannel(Http2StreamChannel channel) {
    if (!channel.isOpen() || !channel.parent().hasAttr(RECYCLE_STREAMS_KEY)
        || !(channel instanceof EspressoHttp2MultiplexHandler.EspressoHttp2MultiplexHandlerStreamChannel)) {
      return false;
    }
    Queue<Http2StreamChannel> queue = channel.parent().attr(RECYCLE_STREAMS_KEY).get();
    // Haven't hit channel reuse limit and channel is available for reuse
    return queue.size() < getMaxReuseStreamChannelsLimit() && Http2Utils.channelAvailableForReuse(channel);
  }

  @Override
  public Future<Void> release(Channel channel, Promise<Void> promise) {
    // If we can use reuse the stream channel, use it.
    if (channel instanceof Http2StreamChannel) {
      Http2StreamChannel streamChannel = (Http2StreamChannel) channel;
      boolean closeChannel = true;
      try {
        handler().channelReleased(streamChannel);

        if (canReuseChannel(streamChannel)) {
          // If recycle fails, close the stream
          closeChannel = !streamChannel.parent().attr(RECYCLE_STREAMS_KEY).get().offer(streamChannel);
          // when closeChannel == true, we call releaseReusedStream in the close listener
          if (!closeChannel) {
            releaseReusedStream(streamChannel);
          }
          return promise.setSuccess(null);
        }
        return promise;
      } catch (Exception ex) {
        return promise.setFailure(ex);
      } finally {
        _totalActiveStreamChannels.decrement();
        if (closeChannel) {
          streamChannel.close().addListener(closeFuture -> {
            releaseReusedStream(streamChannel);
            if (closeFuture.isSuccess()) {
              promise.setSuccess(null);
            } else {
              promise.setFailure(closeFuture.cause());
            }
          });
        }
      }
    } else {
      return _parentPool.release(channel, promise);
    }
  }

  private void releaseReusedStream(Http2StreamChannel channel) {
    if (Http2Utils.isReusedChannel(channel)) {
      _currentStreamsReused.decrement();
      Http2Utils.unmarkReusedStream(channel);
    }
  }

  @Override
  public void close() {
    _parentPool.close();
  }

  @Override
  public ChannelPoolHandler handler() {
    return _parentPool.handler();
  }

  @Override
  public int getConnectedChannels() {
    return _parentPool.getConnectedChannels();
  }

  @Override
  public boolean isHealthy() {
    // Check if parent pool is healthy directly. Parent pool will check if min connections are reached under H2 mode
    return _parentPool.isHealthy();
  }

  @Override
  public final Future<Void> closeFuture() {
    return _parentPool.closeFuture();
  }

  @Override
  public final boolean isClosing() {
    return _parentPool.isClosing();
  }

  @Override
  public int getMaxConnections() {
    return _parentPool.getMaxConnections();
  }

  @Override
  public int getH2ActiveConnections() {
    // we are using h2
    if (_http2ChannelGroup.size() > 0) {
      return (int) _http2ChannelGroup.stream()
          .filter(channel -> channel.pipeline().get(Http2FrameCodec.class).connection().numActiveStreams() > 0)
          .count();
    }
    // no h2 active connections or we are using h1
    return -1;
  }

  @Override
  public ChannelGroup getHttp2ChannelGroup() {
    return _http2ChannelGroup;
  }

  @Override
  public int getMaxPendingAcquires() {
    return _parentPool.getMaxPendingAcquires();
  }

  @Override
  public int getAcquiredChannelCount() {
    return _parentPool.getAcquiredChannelCount();
  }

  @Override
  public int getPendingAcquireCount() {
    return _parentPool.getPendingAcquireCount();
  }

  @Override
  public boolean isClosed() {
    return _parentPool.isClosed();
  }

  @Override
  public long getChannelReusePoolSize() {
    return _http2ChannelGroup.stream()
        .filter(channel -> channel.hasAttr(RECYCLE_STREAMS_KEY))
        .map(channel -> channel.attr(RECYCLE_STREAMS_KEY).get())
        .filter(Objects::nonNull)
        .mapToInt(Queue::size)
        .sum();
  }
}
