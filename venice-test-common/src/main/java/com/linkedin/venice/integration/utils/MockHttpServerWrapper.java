package com.linkedin.venice.integration.utils;

import com.linkedin.venice.utils.Utils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

//TODO: It is worth taking a look of Netty handler here.
//TODO: It seems that each request goes through the channel twice. (checkout AvroGenericStoreClientImplTest)
//TODO: The request has no msg at the second time and trigger ""Unknown message type" exception.


public class MockHttpServerWrapper extends ProcessWrapper {
  private static final Logger logger = LogManager.getLogger(MockHttpServerWrapper.class);

  private final ServerBootstrap bootstrap;
  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private ChannelFuture serverFuture;
  private final int port;
  private final Map<String, FullHttpResponse> uriToResponseMap = new ConcurrentHashMap<>();
  private final Map<String, FullHttpResponse> uriPatternToResponseMap = new ConcurrentHashMap<>();

  static StatefulServiceProvider<MockHttpServerWrapper> generateService() {
    return (serviceName, dataDirectory) -> new MockHttpServerWrapper(serviceName, Utils.getFreePort());
  }

  public MockHttpServerWrapper(String serviceName, int port) {
    super(serviceName, null);
    this.port = port;

    bossGroup = new NioEventLoopGroup(1);
    workerGroup = new NioEventLoopGroup();

    bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            ch.pipeline()
                .addLast(new HttpServerCodec())
                /**
                 * To consolidate multiple parts of one request, so the downstream handler will receive
                 * {@link FullHttpRequest}.
                  */
                .addLast(new HttpObjectAggregator(1024 * 1024)) // Maximum request is 1MB, will return 413 if exceeds.
                .addLast(new MockServerHandler(uriToResponseMap, uriPatternToResponseMap));
          }
        })
        .option(ChannelOption.SO_BACKLOG, 128)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true)
        .childOption(ChannelOption.TCP_NODELAY, true);
  }

  @Override
  public String getHost() {
    return DEFAULT_HOST_NAME;
  }

  @Override
  public int getPort() {
    return port;
  }

  @Override
  protected void internalStart() throws Exception {
    serverFuture = bootstrap.bind(port).sync();
    logger.info("Mock Http Server has been started.");
  }

  @Override
  protected void internalStop() throws Exception {
    ChannelFuture shutdown = serverFuture.channel().closeFuture();
    workerGroup.shutdownGracefully();
    bossGroup.shutdownGracefully();
    shutdown.sync();
    logger.info("Mock Http Server has been stopped.");
  }

  @Override
  protected void newProcess() throws Exception {
    throw new UnsupportedOperationException("Mock Http server does not support restart.");
  }

  public void addResponseForUri(String uri, FullHttpResponse response) {
    uriToResponseMap.put(uri, response);
  }

  public void addResponseForUriPattern(String uriPattern, FullHttpResponse response) {
    uriPatternToResponseMap.put(uriPattern, response);
  }

  public void clearResponseMapping() {
    uriToResponseMap.clear();
  }

  private static class MockServerHandler extends SimpleChannelInboundHandler {
    private static final Logger logger = LogManager.getLogger(MockServerHandler.class);
    private final Map<String, FullHttpResponse> responseMap;
    private final Map<String, FullHttpResponse> uriPatternToResponseMap;
    private final FullHttpResponse notFoundResponse;
    private final FullHttpResponse internalErrorResponse;

    public MockServerHandler(
        Map<String, FullHttpResponse> responseMap,
        Map<String, FullHttpResponse> uriPatternToResponseMap) {
      this.responseMap = responseMap;
      this.uriPatternToResponseMap = uriPatternToResponseMap;

      this.notFoundResponse =
          new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND, Unpooled.buffer(1));
      this.notFoundResponse.headers().add(HttpHeaderNames.CONTENT_TYPE, "text/plain");
      this.notFoundResponse.headers().add(HttpHeaderNames.CONTENT_LENGTH, notFoundResponse.content().readableBytes());
      this.internalErrorResponse = new DefaultFullHttpResponse(
          HttpVersion.HTTP_1_1,
          HttpResponseStatus.INTERNAL_SERVER_ERROR,
          Unpooled.buffer(1));
      this.internalErrorResponse.headers().add(HttpHeaderNames.CONTENT_TYPE, "text/plain");
      this.internalErrorResponse.headers()
          .add(HttpHeaderNames.CONTENT_LENGTH, internalErrorResponse.content().readableBytes());
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
      ctx.flush();
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) {
      if (msg instanceof HttpRequest) {
        URI uri = URI.create(((HttpRequest) msg).uri());
        // stripe URI scheme, host and port
        String uriStr = uri.getPath();
        uriStr = uri.getQuery() == null ? uriStr : uriStr + "?" + uri.getQuery();
        logger.trace("Receive request uri: " + uriStr);

        if (responseMap.containsKey(uriStr)) {
          logger.trace("Found matched response");
          ctx.writeAndFlush(responseMap.get(uriStr).copy());
        } else {
          for (Map.Entry<String, FullHttpResponse> entry: uriPatternToResponseMap.entrySet()) {
            String uriPattern = entry.getKey();
            if (uriStr.matches(uriPattern)) {
              logger.trace("Found matched response by uri pattern: " + uriPattern);
              ctx.writeAndFlush(entry.getValue().copy()).addListener(ChannelFutureListener.CLOSE);
              return;
            }
          }
          logger.trace("No matched response");
          ctx.writeAndFlush(notFoundResponse.copy()).addListener(ChannelFutureListener.CLOSE);
        }
      } else {
        throw new RuntimeException("Unknown message type:" + msg.getClass());
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      logger.error("Got exception during serving:", cause);
      ctx.writeAndFlush(internalErrorResponse.copy()).addListener(ChannelFutureListener.CLOSE);
      // Close connection
      ctx.close();
    }
  }
}
