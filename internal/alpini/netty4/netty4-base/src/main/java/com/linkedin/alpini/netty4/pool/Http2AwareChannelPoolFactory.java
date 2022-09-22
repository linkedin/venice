package com.linkedin.alpini.netty4.pool;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.alpini.base.concurrency.ScheduledExecutorService;
import com.linkedin.alpini.base.monitoring.CallTracker;
import com.linkedin.alpini.netty4.handlers.AllChannelsHandler;
import com.linkedin.alpini.netty4.handlers.BasicHttpClientCodec;
import com.linkedin.alpini.netty4.handlers.ChannelInitializer;
import com.linkedin.alpini.netty4.handlers.Http2PingSendHandler;
import com.linkedin.alpini.netty4.handlers.Http2SettingsFrameLogger;
import com.linkedin.alpini.netty4.handlers.RateLimitConnectHandler;
import com.linkedin.alpini.netty4.http2.Http2ClientResponseHandler;
import com.linkedin.alpini.netty4.misc.Http2Utils;
import com.linkedin.alpini.netty4.misc.NettyUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.PendingConnectFuturePromise;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.handler.codec.PrematureChannelClosureException;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpScheme;
import io.netty.handler.codec.http2.EspressoHttp2MultiplexHandler;
import io.netty.handler.codec.http2.Http2FrameCodec;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2SettingsFrame;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import io.netty.handler.ssl.SslCloseCompletionEvent;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Created by acurtis on 4/19/18.
 */
public class Http2AwareChannelPoolFactory extends FixedChannelPoolFactory {
  private static final Logger LOG = LogManager.getLogger(Http2AwareChannelPoolFactory.class);

  private static final AttributeKey<ChannelHandler> INITIAL_HANDLER =
      AttributeKey.valueOf(Http2AwareChannelPoolFactory.class, "handler");
  private static final AttributeKey<SocketAddress> REMOTE_ADDRESS =
      AttributeKey.valueOf(Http2AwareChannelPoolFactory.class, "remoteAddress");
  public static final AttributeKey<SslContext> SSL_CONTEXT =
      AttributeKey.valueOf(Http2AwareChannelPoolFactory.class, "sslContext");

  private final Http2SettingsFrameLogger _clientFrameLogger = new Http2SettingsFrameLogger(LogLevel.INFO, "router");

  private final ConcurrentMap<SocketAddress, RateLimitConnectHandler> _rateLimitConnectHandlers =
      new ConcurrentHashMap<>();
  private final ScheduledExecutorService _scheduler = Executors.newSingleThreadScheduledExecutor();
  private static final LongSupplier DEFAULT_CONNECT_DELAY_MILLIS = constant(100);
  private static final LongSupplier DEFAULT_FAILURE_DELAY_MILLIS = constant(5000);

  private LongSupplier _betweenConnectDelayMillis = DEFAULT_CONNECT_DELAY_MILLIS;
  private LongSupplier _betweenFailureDelayMillis = DEFAULT_FAILURE_DELAY_MILLIS;

  private static final Http1Ready HTTP_1_READY = new Http1Ready();

  private final ChannelHandler _http11Initializer = new ChannelInitializer<Channel>() {
    @Override
    protected void initChannel(Channel ch) throws Exception {
      initHttp1(ch.pipeline());
    }
  };
  private final ChannelHandler _protoInitializer = constructAlpnInitializer().orElse(_http11Initializer);

  private ChannelGroup _http2ChannelGroup;

  // HTTP/2 Settings.
  private boolean _moreThanOneHttp2Connection = false;
  private boolean _useCustomH2Codec = false;
  private int _maxFrameSize = 8 * 1024 * 1024;
  private int _initialWindowSize = 8 * 1024 * 1024;
  private long _maxConcurrentStreams = Http2AwareChannelPool.DEFAULT_MAX_CONCURRENT_STREAMS;
  private long _maxHeaderListSize = 20 * 1024 * 1024;
  private int _headerTableSize = 0;

  private boolean _reuseChannels = false;
  private boolean _offloadStreams = false;
  private boolean _retryWhenMaxStreamsReached = false;
  private boolean _usingMultiplexHandler = true;
  private int _maxReuseStreamChannelsLimit = Http2AwareChannelPool.DEFAULT_MAX_REUSE_STREAM_CHANNELS_LIMIT;
  private AllChannelsHandler _allChannelsHandler;

  private Function<Channel, Http2PingSendHandler> _http2PingSendHandlerFunction;

  private IntSupplier _http1MinConnections;
  private IntSupplier _http1MaxConnections;

  public Http2AwareChannelPoolFactory(
      @Nonnull Bootstrap bootstrap,
      long acquireTimeoutMillis,
      int minConnections,
      int maxConnections,
      int maxPendingAcquires,
      boolean releaseHeathCheck,
      long healthCheckIntervalMillis,
      @Nonnull ChannelHealthChecker channelHealthChecker,
      @Nullable Function<SocketAddress, CallTracker> healthCheckerTracker) {
    super(
        bootstrap,
        acquireTimeoutMillis,
        minConnections,
        maxConnections,
        maxPendingAcquires,
        releaseHeathCheck,
        healthCheckIntervalMillis,
        channelHealthChecker,
        healthCheckerTracker);
  }

  public Http2AwareChannelPoolFactory(
      @Nonnull Bootstrap bootstrap,
      long acquireTimeoutMillis,
      @Nonnull IntSupplier minConnections,
      @Nonnull IntSupplier maxConnections,
      @Nonnull IntSupplier maxPendingAcquires,
      boolean releaseHeathCheck,
      @Nonnull LongSupplier healthCheckIntervalMillis,
      @Nonnull ChannelHealthChecker channelHealthChecker,
      @Nullable Function<SocketAddress, CallTracker> healthCheckerTracker) {
    this(
        bootstrap,
        acquireTimeoutMillis,
        minConnections,
        maxConnections,
        maxPendingAcquires,
        releaseHeathCheck,
        healthCheckIntervalMillis,
        channelHealthChecker,
        healthCheckerTracker,
        DEFAULT_CONNECT_DELAY_MILLIS);
  }

  public Http2AwareChannelPoolFactory(
      @Nonnull Bootstrap bootstrap,
      long acquireTimeoutMillis,
      @Nonnull IntSupplier minConnections,
      @Nonnull IntSupplier maxConnections,
      @Nonnull IntSupplier maxPendingAcquires,
      boolean releaseHeathCheck,
      @Nonnull LongSupplier healthCheckIntervalMillis,
      @Nonnull ChannelHealthChecker channelHealthChecker,
      @Nullable Function<SocketAddress, CallTracker> healthCheckerTracker,
      @Nonnull LongSupplier betweenConnectDelayMillis) {
    super(
        bootstrap,
        acquireTimeoutMillis,
        minConnections,
        maxConnections,
        maxPendingAcquires,
        releaseHeathCheck,
        healthCheckIntervalMillis,
        channelHealthChecker,
        healthCheckerTracker);
    _betweenConnectDelayMillis = betweenConnectDelayMillis;
  }

  public Http2AwareChannelPoolFactory(
      @Nonnull Bootstrap bootstrap,
      long acquireTimeoutMillis,
      int maxConnections,
      int maxPendingAcquires,
      boolean releaseHeathCheck,
      long healthCheckIntervalMillis,
      @Nonnull ChannelHealthChecker channelHealthChecker,
      @Nullable CallTracker healthCheckerTracker) {
    super(
        bootstrap,
        acquireTimeoutMillis,
        maxConnections,
        maxPendingAcquires,
        releaseHeathCheck,
        healthCheckIntervalMillis,
        channelHealthChecker,
        healthCheckerTracker);
  }

  public Http2AwareChannelPoolFactory(
      @Nonnull Bootstrap bootstrap,
      long acquireTimeoutMillis,
      int maxConnections,
      int maxPendingAcquires,
      boolean releaseHeathCheck,
      long healthCheckIntervalMillis,
      @Nonnull ChannelHealthChecker channelHealthChecker,
      @Nullable Function<SocketAddress, CallTracker> healthCheckerTracker) {
    super(
        bootstrap,
        acquireTimeoutMillis,
        maxConnections,
        maxPendingAcquires,
        releaseHeathCheck,
        healthCheckIntervalMillis,
        channelHealthChecker,
        healthCheckerTracker);
  }

  public void setBetweenConnectDelayMillis(long millis) {
    _betweenConnectDelayMillis = constant(millis);
  }

  public void setBetweenFailureDelayMillis(long millis) {
    _betweenFailureDelayMillis = constant(millis);
  }

  public LongSupplier getBetweenConnectDelayMillis() {
    return _betweenConnectDelayMillis;
  }

  public LongSupplier getBetweenFailureDelayMillis() {
    return _betweenFailureDelayMillis;
  }

  public void setHttp1MinConnections(IntSupplier http1MinConnections) {
    _http1MinConnections = http1MinConnections;
  }

  public void setHttp1MaxConnections(IntSupplier http1MaxConnections) {
    _http1MaxConnections = http1MaxConnections;
  }

  public Http2AwareChannelPoolFactory setMoreThanOneHttp2Connection(boolean value) {
    _moreThanOneHttp2Connection = value;
    return this;
  }

  public boolean wantMoreThanOneHttp2Connection() {
    return _moreThanOneHttp2Connection;
  }

  public Http2AwareChannelPoolFactory setHttp2ChannelGroup(ChannelGroup http2ChannelGroup) {
    _http2ChannelGroup = http2ChannelGroup;
    return this;
  }

  public boolean useCustomH2Codec() {
    return _useCustomH2Codec;
  }

  public void setUseCustomH2Codec(boolean useCustomH2Codec) {
    _useCustomH2Codec = useCustomH2Codec;
  }

  public boolean isUsingMultiplexHandler() {
    return _usingMultiplexHandler;
  }

  public void setUsingMultiplexHandler(boolean usingMultiplexHandler) {
    _usingMultiplexHandler = usingMultiplexHandler;
  }

  public int getMaxFrameSize() {
    return _maxFrameSize;
  }

  public void setMaxFrameSize(int maxFrameSize) {
    _maxFrameSize = maxFrameSize;
  }

  public int getInitialWindowSize() {
    return _initialWindowSize;
  }

  public void setInitialWindowSize(int initialWindowSize) {
    _initialWindowSize = initialWindowSize;
  }

  public long getMaxConcurrentStreams() {
    return _maxConcurrentStreams;
  }

  public void setMaxConcurrentStreams(long maxConcurrentStreams) {
    _maxConcurrentStreams = maxConcurrentStreams;
  }

  public long getMaxHeaderListSize() {
    return _maxHeaderListSize;
  }

  public void setMaxHeaderListSize(long maxHeaderListSize) {
    _maxHeaderListSize = maxHeaderListSize;
  }

  // Child class can override this based on configs
  public boolean reuseChannels() {
    return _reuseChannels;
  }

  public void setReuseChannels(boolean reuseChannels) {
    _reuseChannels = reuseChannels;
  }

  // Child class can override this based on configs
  public boolean offloadStreams() {
    return _offloadStreams;
  }

  public void setOffloadStreams(boolean offloadStreams) {
    _offloadStreams = offloadStreams;
  }

  public int getHeaderTableSize() {
    return _headerTableSize;
  }

  public void setHeaderTableSize(int headerTableSize) {
    _headerTableSize = headerTableSize;
  }

  public void setMaxReuseStreamChannelsLimit(int maxReuseStreamChannelsLimit) {
    _maxReuseStreamChannelsLimit = maxReuseStreamChannelsLimit;
  }

  public int getMaxReuseStreamChannelsLimit() {
    return _maxReuseStreamChannelsLimit;
  }

  public void setAllChannelsHandler(AllChannelsHandler allChannelsHandler) {
    _allChannelsHandler = allChannelsHandler;
  }

  public void setHttp2PingSendHandlerFunction(Function<Channel, Http2PingSendHandler> pingSendHandlerFn) {
    _http2PingSendHandlerFunction = pingSendHandlerFn;
  }

  public Http2PingSendHandler getHttp2PingResponseHandler(@Nonnull Channel channel) {
    Function<Channel, Http2PingSendHandler> function = _http2PingSendHandlerFunction;
    return function != null ? function.apply(channel) : null;
  }

  public void setRetryWhenMaxStreamsReached(boolean retryWhenMaxStreamsReached) {
    _retryWhenMaxStreamsReached = retryWhenMaxStreamsReached;
  }

  public boolean shouldRetryWhenMaxStreamsReached() {
    return _retryWhenMaxStreamsReached;
  }

  protected RateLimitConnectHandler getRateLimitConnectionHandler(SocketAddress remoteAddress) {
    return _rateLimitConnectHandlers
        .computeIfAbsent(remoteAddress, Http2AwareChannelPoolFactory.this::createRateLimitConnectHandler);
  }

  protected void maxStreamsReached(Throwable ex) {
    LOG.warn("Failed to write request to channel", ex);
  }

  protected ChannelInitializer<Channel> constructServerPushChannelInitializer() {
    return new ChannelInitializer<Channel>() {
      @Override
      protected void initChannel(Channel ch) throws Exception {
        throw new IllegalStateException("Server Push not supported");
      }
    };
  }

  private static LongSupplier constant(long value) {
    return () -> value;
  }

  @ChannelHandler.Sharable
  private class ProtocolNegotiationHandler extends ApplicationProtocolNegotiationHandler {
    /**
     * Creates a new instance with the specified fallback protocol name.
     *
    
     * @param fallbackProtocol the name of the protocol to use when
     *                         ALPN/NPN negotiation fails or the client does not support ALPN/NPN
     */
    ProtocolNegotiationHandler(String fallbackProtocol) {
      super(fallbackProtocol);
    }

    protected Http2Settings clientHttp2Settings() {
      return Http2Settings.defaultSettings()
          .maxFrameSize(getMaxFrameSize())
          .initialWindowSize(getInitialWindowSize())
          .maxConcurrentStreams(getMaxConcurrentStreams())
          .maxHeaderListSize(getMaxHeaderListSize()) // 20MB
          .headerTableSize(getHeaderTableSize());
    }

    protected Http2FrameCodecBuilder clientHttp2FrameCodecBuilder() {
      return Http2FrameCodecBuilder.forClient()
          .initialSettings(clientHttp2Settings())
          .validateHeaders(false)
          // The underlying HTTP 1.1 pipeline checks the header lengths, so we can safely ignore this
          .encoderIgnoreMaxHeaderListSize(true)
          .frameLogger(_clientFrameLogger);
    }

    @Override
    protected void configurePipeline(ChannelHandlerContext ctx, String protocol) {
      LOG.debug("configurePipeline: {} {}", ctx.channel(), protocol);
      ChannelPipeline pipeline = ctx.pipeline();
      if (ApplicationProtocolNames.HTTP_2.equals(protocol)) {
        // Converts bytes to HTTP/2 Frames
        Http2FrameCodecBuilder http2FrameCodecBuilder = clientHttp2FrameCodecBuilder();
        Http2FrameCodec http2FrameCodec = http2FrameCodecBuilder.build();

        EventExecutorGroup executorGroup = NettyUtils.executorGroup(pipeline);

        pipeline.addLast(executorGroup, "http2FrameCodec", http2FrameCodec);

        if (isUsingMultiplexHandler()) {
          ChannelHandlerAdapter inboundStreamHandler = new ChannelHandlerAdapter() {
            @Override
            public void handlerAdded(ChannelHandlerContext ctx) {
              LOG.error("Not supported");
              ctx.channel().close();
            }
          };
          pipeline.addLast(
              executorGroup,
              "http2MultiplexHandler",
              reuseChannels()
                  ? new EspressoHttp2MultiplexHandler(inboundStreamHandler, true, offloadStreams())
                  : new Http2MultiplexHandler(inboundStreamHandler));
        } else {
          pipeline.addLast(
              executorGroup,
              "http2ClientResponseHandler",
              new Http2ClientResponseHandler(
                  HttpScheme.HTTPS,
                  false,
                  Http2AwareChannelPoolFactory.this::maxStreamsReached));
        }

        pipeline.addLast(executorGroup, new ChannelInboundHandlerAdapter() {
          boolean complete;

          @Override
          public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            LOG.debug("channelRead {}", msg.getClass());
            complete |= msg instanceof Http2SettingsFrame;
            super.channelRead(ctx, msg);
          }

          @Override
          public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            LOG.debug("channelReadComplete {}", complete);
            if (complete) {
              ReadyHandler.fireReadyEvent(pipeline);
              pipeline.remove(this);
            }
            super.channelReadComplete(ctx);
          }
        });

        initHttp2(pipeline, executorGroup, protocol);
      } else if (ApplicationProtocolNames.HTTP_1_1.equals(protocol)) {
        if (pipeline.context(this) != null) {
          // ensure that the ALPN handler is gone before we continue
          pipeline.remove(this);
        }
        // Just normal HTTP/1.1
        initHttp1(pipeline);
      } else {
        ctx.close();
        throw new IllegalStateException("unknown protocol from " + ctx.channel().remoteAddress() + " : " + protocol);
      }
    }
  }

  protected void initHttp2(ChannelPipeline pipeline, EventExecutorGroup executorGroup, String protocol) {
    // add the handler of the host to the pipeline for handling pings
    Http2PingSendHandler http2PingSendHandler = getHttp2PingResponseHandler(pipeline.channel());
    if (http2PingSendHandler != null) {
      pipeline.addLast(executorGroup, "http2PingSendHandler", http2PingSendHandler);
      LOG.debug(
          "configurePipeline for Http2PingSendHandler: {} {} {}",
          pipeline.channel(),
          protocol,
          http2PingSendHandler);
    }
  }

  protected Optional<ChannelHandler> constructAlpnInitializer() {
    try {
      return Optional.of(new ProtocolNegotiationHandler(ApplicationProtocolNames.HTTP_1_1));
    } catch (Throwable ignored) {
      return Optional.empty();
    }
  }

  private void initHttp1(ChannelPipeline pipeline) {
    ChannelHandler channelInit = pipeline.channel().attr(INITIAL_HANDLER).get();
    LOG.debug("initHttp1, {}", channelInit);

    ChannelDuplexHandler httpCodec =
        Optional.ofNullable((ChannelDuplexHandler) createHttpClientCodec()).orElseGet(this::createBasicHttpClientCodec);

    pipeline.addLast(NettyUtils.executorGroup(pipeline), httpCodec, channelInit, HTTP_1_READY);
  }

  /**
   * @deprecated use {@link #createBasicHttpClientCodec()}
   * @return HttpClientCodec
   */
  @Deprecated
  protected HttpClientCodec createHttpClientCodec() {
    return null;
  }

  protected BasicHttpClientCodec createBasicHttpClientCodec() {
    return new BasicHttpClientCodec(4096, 8192, 8192, false, false);
  }

  protected RateLimitConnectHandler createRateLimitConnectHandler(SocketAddress inetAddress) {
    return new RateLimitConnectHandler(_scheduler, getBetweenConnectDelayMillis(), getBetweenFailureDelayMillis());
  }

  @Nonnull
  @Override
  protected ManagedChannelPool construct(
      @Nonnull Bootstrap bootstrap,
      @Nonnull ChannelPoolHandler channelPoolHandler,
      @Nonnull ChannelHealthChecker healthChecker,
      @Nonnull FixedChannelPool.AcquireTimeoutAction acquireTimeoutAction,
      @Nonnull IntSupplier minConnections,
      int maxConnections,
      boolean releaseHeathCheck) {

    LOG.debug("construct {}", bootstrap.config().remoteAddress());
    Http2AwareChannelPool http2AwareChannelPool = createHttp2AwareChannelPool(
        newPool(
            bootstrap,
            channelPoolHandler,
            healthChecker,
            acquireTimeoutAction,
            minConnections,
            maxConnections,
            releaseHeathCheck),
        this::preCreateHttp2,
        this::postCreateHttp2,
        wantMoreThanOneHttp2Connection(),
        _http2ChannelGroup);
    http2AwareChannelPool.setUseCustomH2Codec(useCustomH2Codec());
    http2AwareChannelPool.setChannelReuse(reuseChannels());
    http2AwareChannelPool.setMaxConcurrentStreams(getMaxConcurrentStreams());
    http2AwareChannelPool.setMaxReuseStreamChannelsLimit(getMaxReuseStreamChannelsLimit());
    http2AwareChannelPool.setRetryOnMaxStreamsLimit(shouldRetryWhenMaxStreamsReached());
    return http2AwareChannelPool;
  }

  protected Http2AwareChannelPool createHttp2AwareChannelPool(
      ManagedChannelPool parentPool,
      Consumer<Channel> preStreamChannelInitializer,
      Consumer<Channel> postStreamInitializer,
      boolean moreThanOneHttp2Connection,
      ChannelGroup http2ChannelGroup) {
    return new Http2AwareChannelPool(
        parentPool,
        preStreamChannelInitializer,
        postStreamInitializer,
        moreThanOneHttp2Connection,
        http2ChannelGroup);
  }

  protected ManagedChannelPool newPool(
      Bootstrap bootstrap,
      ChannelPoolHandler channelPoolHandler,
      ChannelHealthChecker healthChecker,
      FixedChannelPool.AcquireTimeoutAction acquireTimeoutAction,
      IntSupplier minConnections,
      int maxConnections,
      boolean releaseHeathCheck) {
    if (isUsingFastPool()) {
      return new FastPool(
          bootstrap.attr(REMOTE_ADDRESS, bootstrap.config().remoteAddress()),
          channelPoolHandler,
          healthChecker,
          acquireTimeoutAction,
          getAcquireTimeoutMillis(),
          minConnections,
          maxConnections,
          getMaxPendingAcquires(),
          releaseHeathCheck);
    } else {
      return new Pool(
          bootstrap.attr(REMOTE_ADDRESS, bootstrap.config().remoteAddress()),
          channelPoolHandler,
          healthChecker,
          acquireTimeoutAction,
          getAcquireTimeoutMillis(),
          minConnections,
          maxConnections,
          getMaxPendingAcquires(),
          releaseHeathCheck);
    }
  }

  @Override
  public Future<Boolean> isHealthy(Channel channel) {
    if (channel.isActive() && channel instanceof Http2StreamChannel) {
      // TODO check stream-id so that we can close connections before stream-id rollover.
      return channel.eventLoop().newSucceededFuture(Boolean.TRUE);
    }
    if (channel.isActive() && Http2Utils.isHttp2ParentChannelPipeline(channel.pipeline())) {
      // TODO send Http2PingFrame for health check
      return channel.eventLoop().newSucceededFuture(Boolean.TRUE);
    }
    return super.isHealthy(channel);
  }

  protected void preCreateHttp2(Channel channel) {
    LOG.debug("preCreateHttp2 {}", channel);
  }

  protected void postCreateHttp2(Channel channel) {
    ChannelHandler handler = channel.parent().attr(INITIAL_HANDLER).get();
    LOG.debug("postCreateHttp2 {} {}", channel, handler);
    channel.pipeline().addLast(NettyUtils.executorGroup(channel), handler);
  }

  protected ChannelPipeline addSslContextHandler(
      ChannelPipeline pipeline,
      SslContext sslContext,
      String hostString,
      int hostPort) {
    LOG.debug("sslContext.newHandler({}, {})", hostString, hostPort);
    return pipeline.addLast(
        NettyUtils.executorGroup(pipeline),
        sslContext.newHandler(pipeline.channel().alloc(), hostString, hostPort));
  }

  protected class Pool extends FixedChannelPoolImpl {
    private final IntSupplier _http1MinConnections;
    private final ReadyHandler _readyHandler = new ReadyHandler(this::done);

    protected Pool(
        Bootstrap bootstrap,
        ChannelPoolHandler handler,
        ChannelHealthChecker healthCheck,
        FixedChannelPool.AcquireTimeoutAction action,
        long acquireTimeoutMillis,
        IntSupplier minConnections,
        int maxConnections,
        int maxPendingAcquires,
        boolean releaseHealthCheck) {
      super(
          bootstrap,
          handler,
          healthCheck,
          action,
          acquireTimeoutMillis,
          minConnections,
          maxConnections,
          maxPendingAcquires,
          releaseHealthCheck,
          false,
          _useQueueSizeForAcquiredChannelCount);
      _http1MinConnections = minConnections;
    }

    private void done(Channel ch) {
      if (Http2Utils.isHttp2ParentChannelPipeline(ch.pipeline())) {
        return;
      }
      setHttp1MinConnections();
    }

    private void setHttp1MinConnections() {
      Pool.this.setMinConnections(_http1MinConnections);
    }

    @Override
    public boolean isClosed() {
      return isPoolClosed();
    }

    @Override
    protected ChannelFuture connectChannel(Bootstrap bs) {
      LOG.debug("connectChannel {}", bs.config().remoteAddress());
      return super.connectChannel(bs.handler(new PoolInitializer(bs, _readyHandler, this::setHttp1MinConnections)));
    }

    protected ChannelFuture connectChannel0(Bootstrap bs) {
      LOG.debug("connectChannel0 {}", bs.config().remoteAddress());
      Promise<Channel> done = ImmediateEventExecutor.INSTANCE.newPromise();
      done.addListener((Future<Channel> future) -> {
        if (future.isSuccess()) {
          if (null != future.getNow().pipeline().get(ApplicationProtocolNegotiationHandler.class)) {
            AssertionError error = new AssertionError("Protocol negotiation is expected to be complete");
            LOG.error("Assertion failed", error);
            throw error;
          }
        }
      });
      super.connectChannel0(bs.clone().attr(ReadyHandler.DONE, done)).addListener((ChannelFuture f) -> {
        if (!f.isSuccess()) {
          LOG.debug("connect failure: {}", bs.config().remoteAddress(), f.cause());
          done.setFailure(f.cause());
        }
      });
      return new PendingConnectFuturePromise(done);
    }
  }

  protected class FastPool extends FixedFastChannelPoolImpl {
    private final IntSupplier _defaultHttp1MinConnections;
    private final ReadyHandler _readyHandler = new ReadyHandler(this::done);

    protected FastPool(
        Bootstrap bootstrap,
        ChannelPoolHandler handler,
        ChannelHealthChecker healthCheck,
        FixedChannelPool.AcquireTimeoutAction action,
        long acquireTimeoutMillis,
        IntSupplier minConnections,
        int maxConnections,
        int maxPendingAcquires,
        boolean releaseHealthCheck) {
      super(
          bootstrap,
          handler,
          healthCheck,
          action,
          acquireTimeoutMillis,
          minConnections,
          maxConnections,
          maxPendingAcquires,
          releaseHealthCheck,
          1,
          _useQueueSizeForAcquiredChannelCount);
      _defaultHttp1MinConnections = minConnections;
    }

    private void done(Channel ch) {
      if (Http2Utils.isHttp2ParentChannelPipeline(ch.pipeline())) {
        return;
      }
      setHttp1MinMaxConnections();
    }

    private void setHttp1MinMaxConnections() {
      IntSupplier http1MinConnections =
          _http1MinConnections == null ? _defaultHttp1MinConnections : _http1MinConnections;
      FastPool.this.setMinConnections(http1MinConnections);
      if (_http1MaxConnections != null && FastPool.this.getMaxConnections() != _http1MaxConnections.getAsInt()) {
        LOG.info(
            "Falling back to HTTP/1.1 connections and overriding minConnections={}, maxConnections={} for remote={}",
            http1MinConnections.getAsInt(),
            _http1MaxConnections.getAsInt(),
            bootstrap().config().remoteAddress());
        FastPool.this.setMaxConnections(_http1MaxConnections);
      }
    }

    @Override
    public boolean isClosed() {
      return isPoolClosed();
    }

    @Override
    protected Future<Channel> connectChannel(Bootstrap bs, Promise<Channel> promise) {
      LOG.debug("connectChannel {}", bs.config().remoteAddress());
      return super.connectChannel(
          bs.handler(new PoolInitializer(bs, _readyHandler, this::setHttp1MinMaxConnections)),
          _immediateEventExecutor.newPromise());
    }

    @Override
    protected Future<Channel> connectChannel0(Bootstrap bs, Promise<Channel> promise) {
      super.connectChannel0(bs.clone().attr(ReadyHandler.DONE, promise), _immediateEventExecutor.newPromise())
          .addListener((Future<Channel> f) -> {
            if (!f.isSuccess()) {
              LOG.debug("connect failure: {}", bs.config().remoteAddress(), f.cause());
              promise.setFailure(f.cause());
            }
          });
      return promise;
    }
  }

  private class PoolInitializer extends ChannelInitializer<Channel> {
    private final ChannelHandler initialHandler;
    private final ReadyHandler _readyHandler;
    private final Runnable _onHttp1;

    PoolInitializer(Bootstrap bs, ReadyHandler readyHandler, Runnable onHttp1) {
      initialHandler = bs.config().handler();
      _readyHandler = readyHandler;
      _onHttp1 = onHttp1;
    }

    @Override
    protected void initChannel(Channel ch) throws Exception {
      LOG.debug("initChannel {}", ch);
      ChannelPipeline pipeline = ch.pipeline();
      SslContext sslContext = ch.attr(SSL_CONTEXT).get();
      SocketAddress remoteAddress = ch.attr(REMOTE_ADDRESS).get();
      ch.attr(INITIAL_HANDLER).set(initialHandler);
      String hostString = ((InetSocketAddress) remoteAddress).getHostString();
      int hostPort = ((InetSocketAddress) remoteAddress).getPort();
      EventExecutorGroup executorGroup = NettyUtils.executorGroup(ch);

      pipeline.addFirst(executorGroup, getRateLimitConnectionHandler(remoteAddress))
          .addLast(executorGroup, _readyHandler);

      // AllChannelsHandler will be maintain a list of connections per worker thread.
      // This will be used when registering a connection on a worker thread
      if (_allChannelsHandler != null) {
        pipeline.addFirst(executorGroup, _allChannelsHandler);
      }

      if (sslContext != null) {
        addSslContextHandler(pipeline, sslContext, hostString, hostPort).addLast(executorGroup, _protoInitializer);
      } else {
        pipeline.addLast(executorGroup, _http11Initializer);
        _onHttp1.run();
      }
      LOG.debug("initChannel {} done", ch);
    }
  }

  @ChannelHandler.Sharable
  private static final class ReadyHandler extends ChannelInboundHandlerAdapter {
    private final Consumer<Channel> _onDone;

    private ReadyHandler(Consumer<Channel> onDone) {
      assert isSharable();
      _onDone = onDone;
    }

    public enum Event {
      READY_EVENT
    }

    private static final AttributeKey<Promise<Channel>> DONE = AttributeKey.valueOf(ReadyHandler.class, "DONE");

    public Promise<Channel> getFuture(Channel ch) {
      return ch.attr(DONE).get();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
      LOG.debug("ReadyHandler:handlerRemoved {}", ctx.channel());
      super.handlerRemoved(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      failed(getFuture(ctx.channel()));
      super.channelInactive(ctx);
    }

    private void failed(Promise<Channel> channelPromise) {
      channelPromise.tryFailure(new PrematureChannelClosureException());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      if (!getFuture(ctx.channel()).tryFailure(cause)) {
        super.exceptionCaught(ctx, cause);
      }
    }

    static void fireReadyEvent(ChannelPipeline pipeline) {
      pipeline.fireUserEventTriggered(Event.READY_EVENT);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      LOG.debug("ReadyHandler:userEventTriggered {} {}", ctx.channel(), evt);
      if (evt == Event.READY_EVENT) {
        Promise<Channel> promise = getFuture(ctx.channel());
        try {
          ctx.pipeline().remove(this);
          if (!promise.isDone()) {
            _onDone.accept(ctx.channel());
          }
          promise.setSuccess(ctx.channel());
        } catch (Exception ex) {
          if (!promise.tryFailure(ex)) {
            throw ex;
          }
        }
      } else {
        if (evt == SslCloseCompletionEvent.SUCCESS) {
          // Server closed the connection during initialization
          LOG.error("Server closed the connection during channel initialization {}", ctx.channel());
        } else if (evt == SslHandshakeCompletionEvent.SUCCESS) {
          // handshake success
          LOG.debug("SSL handshake successful {}", ctx.channel());
        } else if (evt instanceof SslHandshakeCompletionEvent) {
          SslHandshakeCompletionEvent event = (SslHandshakeCompletionEvent) evt;
          // handshake failure
          LOG.error("SSL handshake failure {}", ctx.channel(), event.cause());
          getFuture(ctx.channel()).tryFailure(event.cause());
        }
        super.userEventTriggered(ctx, evt);
      }
    }
  }

  @ChannelHandler.Sharable
  private static final class Http1Ready extends ChannelInboundHandlerAdapter {
    private Http1Ready() {
      assert isSharable();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      super.channelActive(ctx);
      fireReadyEvent(ctx);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
      super.handlerAdded(ctx);
      // If the channel is already in an active state, the channelActive method would not be called
      // so we must fire the ready event from here.
      if (ctx.channel().isActive()) {
        fireReadyEvent(ctx);
      }
    }

    private void fireReadyEvent(ChannelHandlerContext ctx) {
      ctx.pipeline().remove(this);
      ReadyHandler.fireReadyEvent(ctx.pipeline());
    }
  }
}
