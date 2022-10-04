package com.linkedin.alpini.netty4.handlers;

import com.linkedin.alpini.base.misc.Preconditions;
import com.linkedin.alpini.netty4.http2.Http2PipelineInitializer;
import com.linkedin.alpini.netty4.misc.NettyUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http2.ActiveStreamsCountHandler;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.Timer;
import io.netty.util.concurrent.EventExecutorGroup;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class BasicServerChannelInitializer<C extends Channel, FACTORY extends BasicServerChannelInitializer<C, FACTORY>>
    extends ChannelInitializer<C> {
  private final ConnectionLimitHandler _connectionLimit;
  protected final ActiveStreamsCountHandler _activeStreamsCountHandler;
  protected final Http2SettingsFrameLogger _http2SettingsFrameLogger;
  private final Timer _idleTimer;
  private final BooleanSupplier _shutdownFlag;
  private final BooleanSupplier _busyAutoReadDisable;
  private final AsyncFullHttpRequestHandler.RequestHandler _handler;
  private int _maxInitialLineLength = 4096;
  private int _maxHeaderSize = 8192;
  private int _maxChunkSize = 8192;
  private long _maxContentLength = 1024 * 1024;
  private long _idleConnectionTimeoutMillis = 300000;
  private long _handshakeConnectionTimeoutMillis = 300000;
  private final boolean _validateHeaders = false;

  /* HTTP/2 Settings */
  private boolean _http2InboundEnabled = false;
  // The number of max concurrent streams at any point in time.
  private int _http2MaxConcurrentStreams = 5;
  // The maximum frame size that can be exchanged
  private int _http2MaxFrameSize = 8 * 1024 * 1024;
  // Initial window size for stream level flow control. Receiver can use WINDOW_UPDATE frame to advertise on how much
  // they can receive.
  private int _http2InitialWindowSize = 8 * 1024 * 1024;
  // HTTP/2 supports header compression using HPACK. It basically maintains a dictionary on both the sender and
  // receiver.
  private int _http2HeaderTableSize = 4096;
  // The maximum size of headers
  private int _http2MaxHeaderListSize = 8192;
  // Register child channels on different I/O workers
  private boolean _useCustomMultiplexHandler = false;

  public BasicServerChannelInitializer(
      @Nonnull ConnectionLimitHandler connectionLimit,
      @Nonnull ActiveStreamsCountHandler activeStreamsCountHandler,
      @Nonnull Http2SettingsFrameLogger http2SettingsFrameLogger,
      @Nonnull Timer idleTimer,
      @Nonnull BooleanSupplier shutdownFlag,
      @Nonnull AsyncFullHttpRequestHandler.RequestHandler handler) {
    this(
        connectionLimit,
        activeStreamsCountHandler,
        http2SettingsFrameLogger,
        idleTimer,
        shutdownFlag,
        Boolean.FALSE::booleanValue,
        handler);
  }

  public BasicServerChannelInitializer(
      @Nonnull ConnectionLimitHandler connectionLimit,
      @Nonnull ActiveStreamsCountHandler activeStreamsCountHandler,
      @Nonnull Http2SettingsFrameLogger http2SettingsFrameLogger,
      // @Nonnull ExecutionHandler executionHandler,
      @Nonnull Timer idleTimer,
      @Nonnull BooleanSupplier shutdownFlag,
      @Nonnull BooleanSupplier busyAutoReadDisable,
      @Nonnull AsyncFullHttpRequestHandler.RequestHandler handler) {
    _connectionLimit = Objects.requireNonNull(connectionLimit, "connectionLimit");
    _activeStreamsCountHandler = Objects.requireNonNull(activeStreamsCountHandler, "activeStreamsHandler");
    _http2SettingsFrameLogger = Objects.requireNonNull(http2SettingsFrameLogger, "http2SettingsFrameLogger");
    _idleTimer = Objects.requireNonNull(idleTimer, "idleTimer");
    _shutdownFlag = Objects.requireNonNull(shutdownFlag, "shutdownFlag");
    _busyAutoReadDisable = Objects.requireNonNull(busyAutoReadDisable, "busyAutoReadDisable");
    _handler = Objects.requireNonNull(handler, "handler");
  }

  @SuppressWarnings("unchecked")
  protected final FACTORY factory() {
    return (FACTORY) this;
  }

  public FACTORY maxInitialLineLength(int maxInitialLineLength) {
    _maxInitialLineLength = Preconditions.notLessThan(maxInitialLineLength, 256, "maxInitialLineLength");
    return factory();
  }

  public FACTORY maxHeaderSize(int maxHeaderSize) {
    _maxHeaderSize = Preconditions.notLessThan(maxHeaderSize, 256, "maxHeaderSize");
    return factory();
  }

  public FACTORY maxChunkSize(int maxChunkSize) {
    _maxChunkSize = Preconditions.notLessThan(maxChunkSize, 256, "maxChunkSize");
    return factory();
  }

  public FACTORY maxContentLength(long maxContentLength) {
    _maxContentLength = Preconditions.notLessThan(maxContentLength, 0L, "maxContentLength");
    return factory();
  }

  public FACTORY idleConnectionTimeoutMillis(long idleConnectionTimeoutMillis) {
    _idleConnectionTimeoutMillis =
        Preconditions.notLessThan(idleConnectionTimeoutMillis, 1000L, "idleConnectionTimeoutMillis");
    return factory();
  }

  public FACTORY handshakeConnectionTimeoutMillis(long idleConnectionTimeoutMillis) {
    _handshakeConnectionTimeoutMillis =
        Preconditions.notLessThan(idleConnectionTimeoutMillis, 1000L, "handshakeConnectionTimeoutMillis");
    return factory();
  }

  public FACTORY http2MaxConcurrentStreams(int maxConcurrentStreams) {
    _http2MaxConcurrentStreams = Preconditions.notLessThan(maxConcurrentStreams, 5, "maxConcurrentStreams");
    return factory();
  }

  public FACTORY http2MaxFrameSize(int http2MaxFrameSize) {
    _http2MaxFrameSize = Preconditions.notLessThan(http2MaxFrameSize, 8 * 1024 * 1024, "http2MaxFrameSize");
    return factory();
  }

  public FACTORY http2InitialWindowSize(int http2InitialWindowSize) {
    _http2InitialWindowSize =
        Preconditions.notLessThan(http2InitialWindowSize, 8 * 1024 * 1024, "http2InitialWindowSize");
    return factory();
  }

  public FACTORY http2HeaderTableSize(int http2HeaderTableSize) {
    _http2HeaderTableSize = Preconditions.notLessThan(http2HeaderTableSize, 0, "http2HeaderTableSize");
    return factory();
  }

  public FACTORY http2MaxHeaderListSize(int http2MaxHeaderListSize) {
    _http2MaxHeaderListSize = Preconditions.notLessThan(http2MaxHeaderListSize, 8192, "http2MaxHeaderListSize");
    return factory();
  }

  public FACTORY enableInboundHttp2(boolean enableHttp2) {
    _http2InboundEnabled = enableHttp2;
    return factory();
  }

  @Deprecated
  public FACTORY useCustomMultiplexHandler(boolean useCustomMultiplexHandler) {
    _useCustomMultiplexHandler = useCustomMultiplexHandler;
    return factory();
  }

  protected final int getMaxInitialLineLength() {
    return _maxInitialLineLength;
  }

  protected final int getMaxHeaderSize() {
    return _maxHeaderSize;
  }

  protected final int getMaxChunkSize() {
    return _maxChunkSize;
  }

  protected final boolean isValidateHeaders() {
    return _validateHeaders;
  }

  @Deprecated
  protected final boolean isUseCustomMultiplexHandler() {
    return _useCustomMultiplexHandler;
  }

  /**
   * This method will be called once the {@link io.netty.channel.Channel} was registered. After the method returns this instance
   * will be removed from the {@link ChannelPipeline} of the {@link Channel}.
   *
   * @param ch the {@link Channel} which was registered.
   * @throws Exception is thrown if an error occurs. In that case it will be handled by
   *                   {@link #exceptionCaught(io.netty.channel.ChannelHandlerContext, Throwable)} which will by default close
   *                   the {@link Channel}.
   */
  @Override
  protected void initChannel(C ch) throws Exception {
    ChannelPipeline pipeline = ch.pipeline();
    EventExecutorGroup executorGroup = NettyUtils.executorGroup(ch);

    // Connection limit occurs first, before SSL handshake
    pipeline.addLast(executorGroup, "connection-limit", _connectionLimit);

    // SSL
    beforeHttpServerCodec(pipeline);

    /* Enable HTTP/2 only if both HTTP/2 and SSL are enabled */
    // Check that there is a handler in the pipeline whose name starts with SSL
    if (_http2InboundEnabled && ch.pipeline().names().stream().anyMatch(name -> name.toLowerCase().startsWith("ssl"))) {
      pipeline.addLast(executorGroup, "HTTP2PipelineInitializer", createHttp2PipelineInitializer());
    } else {
      // Existing HTTP/1.1 pipeline
      pipeline.addLast(
          executorGroup,
          "http",
          new BasicHttpServerCodec(
              getMaxInitialLineLength(),
              getMaxHeaderSize(),
              getMaxChunkSize(),
              isValidateHeaders()));
      afterHttpServerCodec(pipeline);
    }
  }

  protected Http2PipelineInitializer.BuilderSupplier getHttp2PipelineInitializerBuilderSupplier() {
    return Http2PipelineInitializer.DEFAULT_BUILDER;
  }

  protected Http2PipelineInitializer createHttp2PipelineInitializer() {
    return getHttp2PipelineInitializerBuilderSupplier().get()
        .http2Settings(getServerHttp2Settings())
        .activeStreamsCountHandler(_activeStreamsCountHandler)
        .http2SettingsFrameLogger(_http2SettingsFrameLogger)
        .existingHttpPipelineInitializer(this::afterHttpServerCodec)
        .maxInitialLineLength(getMaxInitialLineLength())
        .maxHeaderSize(getMaxHeaderSize())
        .maxChunkSize(getMaxChunkSize())
        .validateHeaders(isValidateHeaders())
        .build();
  }

  // HTTP2 settings
  protected Http2Settings getServerHttp2Settings() {
    return new Http2Settings().maxConcurrentStreams(_http2MaxConcurrentStreams)
        .maxFrameSize(_http2MaxFrameSize)
        .initialWindowSize(_http2InitialWindowSize)
        .headerTableSize(_http2HeaderTableSize)
        .maxHeaderListSize(_http2MaxHeaderListSize);
  }

  protected void afterHttpServerCodec(ChannelPipeline pipeline) {
    beforeChunkAggregator(pipeline);

    EventExecutorGroup executorGroup = NettyUtils.executorGroup(pipeline.channel());

    // Aggregate chunks
    pipeline.addLast(
        executorGroup,
        "chunk-aggregator",
        new BasicHttpObjectAggregator((int) Math.min(_maxContentLength, Integer.MAX_VALUE)));

    beforeIdleStateHandler(pipeline);

    // Detect Idle state
    pipeline.addLast(
        executorGroup,
        "idle-state-handler",
        new ReadTimeoutHandler(_idleConnectionTimeoutMillis, TimeUnit.MILLISECONDS));
    pipeline.addLast(executorGroup, "idle-connection-handler", new StaleConnectionHandler());

    beforeHttpRequestHandler(pipeline);

    // Dispatch handler
    pipeline.addLast(
        executorGroup,
        "http-request-handler",
        new AsyncFullHttpRequestHandler(_handler, _shutdownFlag, _busyAutoReadDisable));
  }

  protected void beforeHttpServerCodec(@Nonnull ChannelPipeline pipeline) {
  }

  protected void beforeChunkAggregator(@Nonnull ChannelPipeline pipeline) {
  }

  protected void beforeIdleStateHandler(@Nonnull ChannelPipeline pipeline) {
  }

  protected void beforeHttpRequestHandler(@Nonnull ChannelPipeline pipeline) {
  }
}
