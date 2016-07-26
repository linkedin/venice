package com.linkedin.venice.integration.utils;

import com.google.common.net.HttpHeaders;
import com.linkedin.d2.server.factory.D2Server;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MockHttpServerWrapper extends ProcessWrapper {
  private final Logger logger = Logger.getLogger(MockHttpServerWrapper.class);

  private ServerBootstrap bootstrap;
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private ChannelFuture serverFuture;
  private int port;
  private Map<String, FullHttpResponse> uriToResponseMap = new ConcurrentHashMap<>();

  private List<D2Server> d2ServerList;

  static StatefulServiceProvider<MockHttpServerWrapper> generateService(List<D2Server> d2ServerList) {
    return ((serviceName, port, dataDirectory) -> {
      D2TestUtils.assignLocalUriToD2Servers(d2ServerList, port);
      return new MockHttpServerWrapper(serviceName, port, d2ServerList);
    });
  }

  public MockHttpServerWrapper(String serviceName, int port, List<D2Server> d2ServerList) {
    super(serviceName, null);
    this.port = port;

    bossGroup = new NioEventLoopGroup(1);
    workerGroup = new NioEventLoopGroup();

    bootstrap = new ServerBootstrap();
    bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            ch.pipeline()
                .addLast(new HttpServerCodec())
                .addLast(new MockServerHandler(uriToResponseMap));
          }
        })
        .option(ChannelOption.SO_BACKLOG, 128)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true)
        .childOption(ChannelOption.TCP_NODELAY, true);
    this.d2ServerList = d2ServerList;
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
  protected void start() throws Exception {
    serverFuture = bootstrap.bind(port).sync();
    if (null != d2ServerList) {
      for (D2Server d2Server : d2ServerList) {
        d2Server.forceStart();
      }
    }
  }

  @Override
  protected void stop() throws Exception {
    if (null != d2ServerList) {
      for (D2Server d2Server : d2ServerList) {
        try {
          d2Server.notifyShutdown();
        } catch (RuntimeException e) {
          logger.error("D2 announcer " + d2Server + " failed to shutdown properly", e);
        }
      }
    }

    ChannelFuture shutdown = serverFuture.channel().closeFuture();
    workerGroup.shutdownGracefully();
    bossGroup.shutdownGracefully();
    shutdown.sync();
  }

  public void addResponseForUri(String uri, FullHttpResponse response) {
    uriToResponseMap.put(uri, response);
  }

  public void clearResponseMapping() {
    uriToResponseMap.clear();
  }


  private static class MockServerHandler extends ChannelInboundHandlerAdapter {
    private final Logger logger = Logger.getLogger(MockServerHandler.class);
    private final Map<String, FullHttpResponse> responseMap;
    private final FullHttpResponse notFoundResponse;
    private final FullHttpResponse internalErrorResponse;

    public MockServerHandler(Map<String, FullHttpResponse> responseMap) {
      this.responseMap = responseMap;

      this.notFoundResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND, Unpooled.buffer(1));
      this.notFoundResponse.headers().add(HttpHeaders.CONTENT_TYPE, "text/plain");
      this.notFoundResponse.headers().add(HttpHeaders.CONTENT_LENGTH, notFoundResponse.content().readableBytes());
      this.internalErrorResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR, Unpooled.buffer(1));
      this.internalErrorResponse.headers().add(HttpHeaders.CONTENT_TYPE, "text/plain");
      this.internalErrorResponse.headers().add(HttpHeaders.CONTENT_LENGTH, internalErrorResponse.content().readableBytes());
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
      ctx.flush();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      if (msg instanceof HttpRequest) {
        HttpRequest req = (HttpRequest)msg;
        String uri = req.getUri();
        logger.info("Receive request uri: " + uri);
        if (responseMap.containsKey(uri)) {
          logger.info("Found matched response");
          ctx.writeAndFlush(responseMap.get(uri).copy()).addListener(ChannelFutureListener.CLOSE);
        } else {
          logger.info("No matched response");
          ctx.writeAndFlush(notFoundResponse.copy()).addListener(ChannelFutureListener.CLOSE);
        }
      } else {
        throw new RuntimeException("Unknown message type");
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
