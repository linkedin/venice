package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.ingestion.handler.IngestionRequestClientHandler;
import com.linkedin.venice.meta.IngestionAction;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpVersion;
import java.io.Closeable;
import org.apache.log4j.Logger;

/**
 * IngestionRequestClient is a Netty client that sends HttpRequest to listener services
 * and retrieves HttpResponse from channel.
 */
public class IngestionRequestClient implements Closeable {
  private static final Logger logger = Logger.getLogger(IngestionRequestClient.class);

  private final int port;
  private IngestionRequestClientHandler responseHandler;
  private final EventLoopGroup workerGroup;
  private final Bootstrap bootstrap;

  public IngestionRequestClient(int port) {
    this.port = port;
    this.responseHandler = new IngestionRequestClientHandler();
    workerGroup = new NioEventLoopGroup();
    bootstrap = new Bootstrap();
    bootstrap.group(workerGroup);
    bootstrap.channel(NioSocketChannel.class);
    bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
    bootstrap.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new HttpResponseDecoder());
        ch.pipeline().addLast(new HttpObjectAggregator(1024 * 1024));
        ch.pipeline().addLast(new HttpRequestEncoder());
        ch.pipeline().addLast(getResponseHandler());
      }
    });
    logger.info("Request client created for target port: " + port);
  }

  public synchronized FullHttpResponse sendRequest(HttpRequest request) throws Exception {
    String host = "localhost";
    ChannelFuture f = bootstrap.connect(host, port).sync();
    f.channel().writeAndFlush(request);
    f.channel().closeFuture().sync();
    return responseHandler.getResponse();
  }

  public HttpRequest buildHttpRequest(IngestionAction action, byte[] content) {
    String endpoint = "/" + action.toString();
    String hostAndPort = "localhost:" + port;
    ByteBuf contentBuf = Unpooled.wrappedBuffer(content);
    HttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, endpoint, contentBuf);
    httpRequest.headers()
        .set(HttpHeaderNames.HOST, hostAndPort)
        .set(HttpHeaderNames.CONTENT_LENGTH, contentBuf.readableBytes());
    return httpRequest;
  }

  @Override
  public void close() {
    workerGroup.shutdownGracefully();
  }

  private IngestionRequestClientHandler getResponseHandler() {
    IngestionRequestClientHandler handler = new IngestionRequestClientHandler();
    responseHandler = handler;
    return handler;
  }
}
