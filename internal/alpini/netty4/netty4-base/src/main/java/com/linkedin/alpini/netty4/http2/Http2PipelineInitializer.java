package com.linkedin.alpini.netty4.http2;

import com.linkedin.alpini.netty4.handlers.BasicHttpServerCodec;
import com.linkedin.alpini.netty4.handlers.Http2ExceptionHandler;
import com.linkedin.alpini.netty4.handlers.Http2SettingsFrameLogger;
import com.linkedin.alpini.netty4.handlers.HttpObjectToBasicHttpObjectAdapter;
import com.linkedin.alpini.netty4.misc.NettyUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http2.ActiveStreamsCountHandler;
import io.netty.handler.codec.http2.EspressoHttp2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2ChannelDuplexHandler;
import io.netty.handler.codec.http2.Http2FrameCodec;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.Http2StreamFrame;
import io.netty.handler.codec.http2.Http2StreamFrameToHttpObjectCodec;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import io.netty.handler.timeout.ForceCloseOnWriteTimeoutHandler;
import io.netty.util.concurrent.EventExecutorGroup;
import java.lang.reflect.Constructor;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Initializes HTTP/2 Pipeline.
 *
 * @author Abhishek Andhavarapu
 */
public class Http2PipelineInitializer extends ApplicationProtocolNegotiationHandler {
  protected static final Logger LOG = LogManager.getLogger(Http2PipelineInitializer.class);

  private final Http2Settings _http2Settings;
  /* HTTP/1.1 Settings */
  private final Consumer<ChannelPipeline> _existingHttpPipelineInitializer;
  private final ActiveStreamsCountHandler _activeStreamsCountHandler;
  private final Http2SettingsFrameLogger _http2SettingsFrameLogger;
  private final int _maxInitialLineLength;
  private final int _maxHeaderSize;
  private final int _maxChunkSize;
  private final boolean _validateHeaders;
  private int _writeTimeoutSeconds;

  @Deprecated
  public Http2PipelineInitializer(
      Http2Settings http2Settings,
      ActiveStreamsCountHandler activeStreamsCountHandler,
      Http2SettingsFrameLogger http2SettingsFrameLogger,
      Consumer<ChannelPipeline> existingHttpPipelineInitializer,
      int maxInitialLineLength,
      int maxHeaderSize,
      int maxChunkSize,
      boolean validateHeaders,
      boolean useCustomMultiplexHandler) {
    this(
        http2Settings,
        activeStreamsCountHandler,
        http2SettingsFrameLogger,
        existingHttpPipelineInitializer,
        maxInitialLineLength,
        maxHeaderSize,
        maxChunkSize,
        validateHeaders);
    if (useCustomMultiplexHandler) {
      LOG.debug("deprecated constructor", new Exception());
    }
  }

  public Http2PipelineInitializer(
      Http2Settings http2Settings,
      ActiveStreamsCountHandler activeStreamsCountHandler,
      Http2SettingsFrameLogger http2SettingsFrameLogger,
      Consumer<ChannelPipeline> existingHttpPipelineInitializer,
      int maxInitialLineLength,
      int maxHeaderSize,
      int maxChunkSize,
      boolean validateHeaders) {
    super(ApplicationProtocolNames.HTTP_1_1);
    _http2Settings = http2Settings;
    _activeStreamsCountHandler = activeStreamsCountHandler;
    _http2SettingsFrameLogger = http2SettingsFrameLogger;
    _existingHttpPipelineInitializer = existingHttpPipelineInitializer;
    _maxInitialLineLength = maxInitialLineLength;
    _maxChunkSize = maxChunkSize;
    _maxHeaderSize = maxHeaderSize;
    _validateHeaders = validateHeaders;
  }

  @Nonnull
  public Http2PipelineInitializer writeTimeoutSeconds(@Nonnegative int writeTimeoutSeconds) {
    _writeTimeoutSeconds = writeTimeoutSeconds;
    return this;
  }

  // Can be overridden by super class
  protected int getWriteTimeoutSeconds() {
    return _writeTimeoutSeconds;
  }

  protected boolean canCreateStream(Channel channel) {
    return channel.isOpen();
  }

  protected Http2FrameCodecBuilder serverHttp2FrameCodecBuilder() {
    return EspressoHttp2FrameCodecBuilder.forServer()
        .canCreateStreams(this::canCreateStream)
        .initialSettings(_http2Settings)
        .validateHeaders(_validateHeaders);
  }

  @Override
  protected void configurePipeline(ChannelHandlerContext ctx, String protocol) throws Exception {
    if (ApplicationProtocolNames.HTTP_2.equals(protocol)) {
      logInitializingHttp2(ctx);
      int writeOutSeconds = getWriteTimeoutSeconds();

      // Converts bytes to HTTP/2 Frames
      Http2FrameCodecBuilder http2FrameCodecBuilder = serverHttp2FrameCodecBuilder()
          // Directly close the underlying transport, and not attempt graceful closure via GOAWAY.
          .decoupleCloseAndGoAway(writeOutSeconds > 0);
      if (_http2SettingsFrameLogger != null) {
        http2FrameCodecBuilder.frameLogger(_http2SettingsFrameLogger);
      }
      Http2FrameCodec http2FrameCodec = http2FrameCodecBuilder.build();

      ChannelHandler inboundStreamHandler = new io.netty.channel.ChannelInitializer<Channel>() {
        @Override
        protected void initChannel(Channel channel) {
          EventExecutorGroup executorGroup = NettyUtils.executorGroup(channel);
          // Child channel which can handle HTTP/1.1 requests
          ChannelPipeline p2 = channel.pipeline()
              .addLast(
                  executorGroup,
                  "multiplex-server-frame-converter",
                  createHttp2StreamFrameToHttpObjectCodec(_validateHeaders))
              .addLast(executorGroup, "Http-to-BasicHttp-converter", new HttpObjectToBasicHttpObjectAdapter());
          // Add the existing HTTP/1.1 pipeline
          _existingHttpPipelineInitializer.accept(p2);
        }
      };

      // Dispatch the HTTP/2 frame to corresponding stream channels
      Http2ChannelDuplexHandler multiplexHandler = createHttp2MultiplexHandler(inboundStreamHandler);

      EventExecutorGroup executorGroup = NettyUtils.executorGroup(ctx.channel());
      if (writeOutSeconds > 0) {
        // Add the timeout handler only if the write timeout is greater than zero
        ctx.pipeline().addLast(executorGroup, new ForceCloseOnWriteTimeoutHandler(writeOutSeconds));
      }
      // HTTP/2 handlers
      ChannelPipeline p = ctx.pipeline()
          .addLast(executorGroup, Http2ExceptionHandler.INSTANCE)
          .addLast(executorGroup, http2FrameCodec)
          // Counts the number of active streams
          .addLast(executorGroup, _activeStreamsCountHandler)
          .addLast(executorGroup, multiplexHandler);
      return;
    }

    if (ApplicationProtocolNames.HTTP_1_1.equals(protocol)) {
      logInitializingHttp1(ctx);
      ChannelPipeline p1 = ctx.pipeline();
      EventExecutorGroup executorGroup = NettyUtils.executorGroup(ctx.channel());

      p1.addLast(
          executorGroup,
          "http",
          new BasicHttpServerCodec(_maxInitialLineLength, _maxHeaderSize, _maxChunkSize, false));
      _existingHttpPipelineInitializer.accept(p1);
      return;
    }

    throw new IllegalStateException("unknown protocol: " + protocol);
  }

  @Nonnull
  protected Http2ChannelDuplexHandler createHttp2MultiplexHandler(@Nonnull ChannelHandler inboundStreamHandler) {
    return new Http2MultiplexHandler(inboundStreamHandler);
  }

  @Nonnull
  protected MessageToMessageCodec<Http2StreamFrame, HttpObject> createHttp2StreamFrameToHttpObjectCodec(
      boolean validateHeaders) {
    return new Http2StreamFrameToHttpObjectCodec(true, validateHeaders);
  }

  // protected so that they may be overridden to provide more information
  protected void logInitializingHttp1(ChannelHandlerContext ctx) {
    LOG.info("Initializing HTTP/1.1 channel {}", ctx.channel());
  }

  // protected so that they may be overridden to provide more information
  protected void logInitializingHttp2(ChannelHandlerContext ctx) {
    LOG.info("Initializing HTTP/2 channel {}", ctx.channel());
  }

  public interface BuilderSupplier extends Supplier<Builder<?>> {
    // empty
  }

  public static final BuilderSupplier DEFAULT_BUILDER = DefaultBuilder::new;

  public static BuilderSupplier builderOf(Class<? extends Http2PipelineInitializer> clazz) {
    if (clazz != null && Http2PipelineInitializer.class != clazz) {
      try {
        Constructor<? extends Http2PipelineInitializer> constructor = clazz.asSubclass(Http2PipelineInitializer.class)
            .getConstructor(
                Http2Settings.class,
                ActiveStreamsCountHandler.class,
                Http2SettingsFrameLogger.class,
                Consumer.class,
                Integer.TYPE,
                Integer.TYPE,
                Integer.TYPE,
                Boolean.TYPE,
                Boolean.TYPE);
        return () -> new LegacyBuilder(constructor);
      } catch (Exception ex) {
        LOG.warn("Unable to find legacy constructor in {}", clazz);
      }
    }
    return DEFAULT_BUILDER;
  }

  private static final class LegacyBuilder extends Builder<DefaultBuilder> {
    private final Constructor<? extends Http2PipelineInitializer> _constructor;
    private boolean _fallback;

    LegacyBuilder(@Nonnull Constructor<? extends Http2PipelineInitializer> constructor) {
      _constructor = constructor;
    }

    @Nonnull
    public Http2PipelineInitializer build() {
      if (!_fallback) {
        try {
          return _constructor
              .newInstance(
                  getHttp2Settings(),
                  getActiveStreamsCountHandler(),
                  getHttp2SettingsFrameLogger(),
                  getExistingHttpPipelineInitializer(),
                  getMaxInitialLineLength(),
                  getMaxHeaderSize(),
                  getMaxChunkSize(),
                  isValidateHeaders(),
                  false)
              .writeTimeoutSeconds(getWriteTimeoutSeconds());
        } catch (Exception ex) {
          LOG.warn("Failed to use legacy constructor", ex);
          _fallback = true;
        }
      }
      return new Http2PipelineInitializer(
          getHttp2Settings(),
          getActiveStreamsCountHandler(),
          getHttp2SettingsFrameLogger(),
          getExistingHttpPipelineInitializer(),
          getMaxInitialLineLength(),
          getMaxHeaderSize(),
          getMaxChunkSize(),
          isValidateHeaders()).writeTimeoutSeconds(getWriteTimeoutSeconds());
    }
  }

  private static final class DefaultBuilder extends Builder<DefaultBuilder> {
    @Nonnull
    public Http2PipelineInitializer build() {
      return new Http2PipelineInitializer(
          getHttp2Settings(),
          getActiveStreamsCountHandler(),
          getHttp2SettingsFrameLogger(),
          getExistingHttpPipelineInitializer(),
          getMaxInitialLineLength(),
          getMaxHeaderSize(),
          getMaxChunkSize(),
          isValidateHeaders()).writeTimeoutSeconds(getWriteTimeoutSeconds());
    }
  }

  public static abstract class Builder<T extends Builder<T>> {
    private Http2Settings http2Settings;
    private ActiveStreamsCountHandler activeStreamsCountHandler;
    private Http2SettingsFrameLogger http2SettingsFrameLogger;
    private Consumer<ChannelPipeline> existingHttpPipelineInitializer;
    private int maxInitialLineLength;
    private int maxHeaderSize;
    private int maxChunkSize;
    private int writeTimeoutSeconds;
    private boolean validateHeaders;

    @Nonnull
    protected T self() {
      // noinspection unchecked
      return (T) this;
    }

    @Nonnull
    public Http2Settings getHttp2Settings() {
      return Objects.requireNonNull(http2Settings);
    }

    @Nonnull
    public T http2Settings(@Nonnull Http2Settings http2Settings) {
      this.http2Settings = http2Settings;
      return self();
    }

    @Nonnull
    public ActiveStreamsCountHandler getActiveStreamsCountHandler() {
      return Objects.requireNonNull(activeStreamsCountHandler);
    }

    @Nonnull
    public T activeStreamsCountHandler(@Nonnull ActiveStreamsCountHandler activeStreamsCountHandler) {
      this.activeStreamsCountHandler = activeStreamsCountHandler;
      return self();
    }

    public Http2SettingsFrameLogger getHttp2SettingsFrameLogger() {
      return http2SettingsFrameLogger;
    }

    @Nonnull
    public T http2SettingsFrameLogger(Http2SettingsFrameLogger http2SettingsFrameLogger) {
      this.http2SettingsFrameLogger = http2SettingsFrameLogger;
      return self();
    }

    @Nonnull
    public Consumer<ChannelPipeline> getExistingHttpPipelineInitializer() {
      return Objects.requireNonNull(existingHttpPipelineInitializer);
    }

    @Nonnull
    public T existingHttpPipelineInitializer(@Nonnull Consumer<ChannelPipeline> existingHttpPipelineInitializer) {
      this.existingHttpPipelineInitializer = existingHttpPipelineInitializer;
      return self();
    }

    @Nonnegative
    public int getMaxInitialLineLength() {
      return maxInitialLineLength;
    }

    @Nonnull
    public T maxInitialLineLength(@Nonnegative int maxInitialLineLength) {
      this.maxInitialLineLength = maxInitialLineLength;
      return self();
    }

    @Nonnegative
    public int getMaxHeaderSize() {
      return maxHeaderSize;
    }

    @Nonnull
    public T maxHeaderSize(@Nonnegative int maxHeaderSize) {
      this.maxHeaderSize = maxHeaderSize;
      return self();
    }

    @Nonnegative
    public int getMaxChunkSize() {
      return maxChunkSize;
    }

    @Nonnull
    public T maxChunkSize(@Nonnegative int maxChunkSize) {
      this.maxChunkSize = maxChunkSize;
      return self();
    }

    public int getWriteTimeoutSeconds() {
      return writeTimeoutSeconds;
    }

    @Nonnull
    public T writeTimeoutSeconds(@Nonnegative int writeTimeoutSeconds) {
      this.writeTimeoutSeconds = writeTimeoutSeconds;
      return self();
    }

    public boolean isValidateHeaders() {
      return validateHeaders;
    }

    @Nonnull
    public T validateHeaders(boolean validateHeaders) {
      this.validateHeaders = validateHeaders;
      return self();
    }

    @Nonnull
    public abstract Http2PipelineInitializer build();
  }
}
