package com.linkedin.alpini.netty4.http2;

import com.linkedin.alpini.netty4.handlers.Http2SettingsFrameLogger;
import com.linkedin.alpini.netty4.handlers.Log4J2LoggingHandler;
import com.linkedin.alpini.netty4.handlers.SimpleChannelInitializer;
import com.linkedin.alpini.netty4.misc.NettyUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpServerUpgradeHandler;
import io.netty.handler.codec.http2.ActiveStreamsCountHandler;
import io.netty.handler.codec.http2.EspressoHttp2MultiplexHandler;
import io.netty.handler.codec.http2.Http2ChannelDuplexHandler;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.ssl.SslContext;
import io.netty.util.AttributeKey;
import java.net.InetSocketAddress;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Created by acurtis on 4/26/18.
 */
public class SkeletonHttp2Server {
  static final Logger _log = LogManager.getLogger(SkeletonHttp2Server.class);

  private SkeletonHttp2Server() {
  }

  public static ServerBootstrap setupBootstrap(
      @Nonnull ServerBootstrap bootstrap,
      @Nonnull SslContext sslContext,
      @Nonnull ChannelHandler handler) {
    return setupBootstrap(bootstrap, sslContext, handler, LogLevel.DEBUG);
  }

  public static ServerBootstrap setupBootstrap(
      ServerBootstrap bootstrap,
      SslContext sslContext,
      ChannelHandler handler,
      LogLevel level) {

    return bootstrap.option(ChannelOption.SO_BACKLOG, 1024)
        .handler(new Log4J2LoggingHandler(SkeletonHttp2Server.class, level))
        .childHandler(new ServerInitializer(sslContext, handler));
  }

  public static class ServerInitializer extends SimpleChannelInitializer<Channel> {
    final SslContext _sslContext;
    final ChannelHandler _handler;
    private Http2PipelineInitializer _http2PipelineInitializer;
    private HttpServerUpgradeHandler.UpgradeCodecFactory _upgradeCodecFactory;

    ServerInitializer(SslContext sslContext, ChannelHandler handler) {
      _sslContext = sslContext;
      _handler = handler;
    }

    public void existingHttpPipelineInitializer(ChannelPipeline pipeline) {
      pipeline.addLast(NettyUtils.executorGroup(pipeline), _handler);
    }

    public interface CanCreateStream {
      boolean canCreateStream(Channel channel);
    }

    public static final AttributeKey<CanCreateStream> CREATE_STREAM_ATTRIBUTE_KEY =
        AttributeKey.valueOf(SkeletonHttp2Server.class, "canCreateStream");

    public static class SkeletonInitializer extends Http2PipelineInitializer {
      public SkeletonInitializer(
          Http2Settings http2Settings,
          ActiveStreamsCountHandler activeStreamsCountHandler,
          Http2SettingsFrameLogger http2SettingsFrameLogger,
          Consumer<ChannelPipeline> existingHttpPipelineInitializer,
          int maxInitialLineLength,
          int maxHeaderSize,
          int maxChunkSize,
          boolean validateHeaders) {
        super(
            http2Settings,
            activeStreamsCountHandler,
            http2SettingsFrameLogger,
            existingHttpPipelineInitializer,
            maxInitialLineLength,
            maxHeaderSize,
            maxChunkSize,
            validateHeaders);
      }

      public SkeletonInitializer(
          Http2Settings http2Settings,
          ActiveStreamsCountHandler activeStreamsCountHandler,
          Http2SettingsFrameLogger http2SettingsFrameLogger,
          Consumer<ChannelPipeline> existingHttpPipelineInitializer,
          int maxInitialLineLength,
          int maxHeaderSize,
          int maxChunkSize,
          boolean validateHeaders,
          boolean useCustomMultiplexHandler) {
        super(
            http2Settings,
            activeStreamsCountHandler,
            http2SettingsFrameLogger,
            existingHttpPipelineInitializer,
            maxInitialLineLength,
            maxHeaderSize,
            maxChunkSize,
            validateHeaders,
            useCustomMultiplexHandler);
      }

      @Override
      protected boolean canCreateStream(Channel channel) {
        return channel.hasAttr(CREATE_STREAM_ATTRIBUTE_KEY)
            ? channel.attr(CREATE_STREAM_ATTRIBUTE_KEY).get().canCreateStream(channel)
            : super.canCreateStream(channel);
      }

      @Nonnull
      @Override
      protected Http2ChannelDuplexHandler createHttp2MultiplexHandler(@Nonnull ChannelHandler inboundStreamHandler) {
        return new EspressoHttp2MultiplexHandler(inboundStreamHandler);
      }
    }

    @Override
    protected void initChannel(Channel ch) throws Exception {
      InetSocketAddress remote = (InetSocketAddress) ch.remoteAddress();
      String host = remote.getHostString();
      int port = remote.getPort();
      addAfter(
          ch,
          _sslContext.newHandler(ch.alloc(), host, port),
          Http2PipelineInitializer.builderOf(SkeletonInitializer.class)
              .get()
              .http2Settings(new Http2Settings())
              .activeStreamsCountHandler(new ActiveStreamsCountHandler())
              .http2SettingsFrameLogger(new Http2SettingsFrameLogger(LogLevel.INFO))
              .existingHttpPipelineInitializer(this::existingHttpPipelineInitializer)
              .maxInitialLineLength(8192)
              .maxHeaderSize(8192)
              .maxChunkSize(256)
              .validateHeaders(false)
              .writeTimeoutSeconds(30)
              .build());
    }
  }
}
