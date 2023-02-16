package com.linkedin.venice.httpclient5;

import com.linkedin.alpini.netty4.ssl.SslInitializer;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import io.netty.util.CharsetUtil;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;


/**
 * This class will spin up a simple Netty-based H2 server.
 */
public class NettyH2Server {
  private static final Logger LOGGER = LogManager.getLogger(NettyH2Server.class);
  private final int port;
  private final String tempFilePathToNotifyServerFullyStarted;

  private final ServerBootstrap bootstrap;
  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private ChannelFuture serverFuture;

  public NettyH2Server(int port, String tempFilePathToNotifyServerFullyStarted) throws Exception {
    this.port = port;
    this.tempFilePathToNotifyServerFullyStarted = tempFilePathToNotifyServerFullyStarted;
    this.bossGroup = new NioEventLoopGroup(1);
    this.workerGroup = new NioEventLoopGroup(4);
    this.bootstrap = new ServerBootstrap();
    SSLFactory sslFactory = SslUtils.getVeniceLocalSslFactory();
    bootstrap.group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .childHandler(new HttpChannelInitializer(sslFactory))
        .option(ChannelOption.SO_BACKLOG, 10)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true)
        .childOption(ChannelOption.TCP_NODELAY, true);
  }

  public void start() throws Exception {
    serverFuture = bootstrap.bind(port).sync();
    // Notify
    try (FileWriter fileWriter = new FileWriter(tempFilePathToNotifyServerFullyStarted);
        BufferedWriter writer = new BufferedWriter(fileWriter)) {
      writer.write("Started");
    }

    LOGGER.info("NettyH2Server started on port: {}", port);
  }

  public void stop() throws Exception {
    ChannelFuture shutdown = serverFuture.channel().closeFuture();
    Thread.sleep(TimeUnit.SECONDS.toMillis(1));
    workerGroup.shutdownGracefully();
    bossGroup.shutdownGracefully();
    shutdown.sync();
    LOGGER.info("NettyH2Server stopped");
  }

  private static class HttpChannelInitializer extends ChannelInitializer<SocketChannel> {
    private final SSLFactory sslFactory;

    public HttpChannelInitializer(SSLFactory sslFactory) {
      this.sslFactory = sslFactory;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
      ch.pipeline().addLast(new SslInitializer(SslUtils.toAlpiniSSLFactory(sslFactory), false), getServerAPNHandler());
    }

    public static ApplicationProtocolNegotiationHandler getServerAPNHandler() {
      ApplicationProtocolNegotiationHandler serverAPNHandler =
          new ApplicationProtocolNegotiationHandler(ApplicationProtocolNames.HTTP_1_1) {
            @Override
            protected void configurePipeline(ChannelHandlerContext ctx, String protocol) throws Exception {
              if (ApplicationProtocolNames.HTTP_2.equals(protocol)) {
                LOGGER.info("Received ALPN request");
                Http2Settings settings = new Http2Settings().maxConcurrentStreams(100)
                    .maxFrameSize(8 * 1024 * 1024)
                    .initialWindowSize(8 * 1024 * 1024)
                    .headerTableSize(4096)
                    .maxHeaderListSize(8192);
                ctx.pipeline()
                    .addLast(
                        Http2FrameCodecBuilder.forServer().initialSettings(settings).build(),
                        new Http2ServerResponseHandler());
                return;
              }
              throw new IllegalStateException("Protocol: " + protocol + " not supported");
            }
          };
      return serverAPNHandler;
    }
  }

  private static class Http2ServerResponseHandler extends ChannelInboundHandlerAdapter {
    private static final ByteBuf RESPONSE_BYTES =
        Unpooled.unreleasableBuffer(Unpooled.copiedBuffer("Hello World", CharsetUtil.UTF_8));

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof Http2HeadersFrame) {
        Http2HeadersFrame msgHeader = (Http2HeadersFrame) msg;
        if (msgHeader.isEndStream()) {
          ByteBuf content = ctx.alloc().buffer();
          content.writeBytes(RESPONSE_BYTES.duplicate());

          Http2Headers headers = new DefaultHttp2Headers().status(HttpResponseStatus.OK.codeAsText());
          ctx.write(new DefaultHttp2HeadersFrame(headers).stream(msgHeader.stream()));
          ctx.write(new DefaultHttp2DataFrame(content, true).stream(msgHeader.stream()));
        }
      } else {
        super.channelRead(ctx, msg);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Assert.assertEquals(args.length, 2, "port and temp file path are expected");

    String port = args[0];
    String tempFilePath = args[1];
    NettyH2Server server = new NettyH2Server(Integer.parseInt(port), tempFilePath);
    server.start();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        server.stop();
        System.out.println("Forked process stopped");
      } catch (Exception e) {
        System.err.println("Received exception during shutdown, error: " + e.getMessage());
      }
    }));
  }
}
