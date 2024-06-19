package com.linkedin.venice.blobtransfer.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;


public class NettyFileTransferClient {
  EventLoopGroup workerGroup;
  Bootstrap clientBootstrap;
  private final String baseDir;
  private final int serverPort;

  // TODO 1: move tunable configs to a config class
  // TODO 2: consider either increasing worker threads or have a dedicated thread pool to handle requests.
  public NettyFileTransferClient(int serverPort, String baseDir) {
    this.baseDir = baseDir;
    this.serverPort = serverPort;
    clientBootstrap = new Bootstrap();
    workerGroup = new NioEventLoopGroup();
    clientBootstrap.group(workerGroup);
    clientBootstrap.channel(NioSocketChannel.class);
    clientBootstrap.option(ChannelOption.SO_KEEPALIVE, true);
    clientBootstrap.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) {
        ch.pipeline().addLast(new HttpClientCodec());
      }
    });
  }

  public CompletionStage<InputStream> get(String host, String storeName, int version, int partition)
      throws InterruptedException {
    CompletionStage<InputStream> inputStream = new CompletableFuture<>();
    // Connects to the remote host
    Channel ch = clientBootstrap.connect(host, serverPort).sync().channel();
    // Attach the file handler to the pipeline
    ch.pipeline().addLast(new P2PFileTransferClientHandler(baseDir, inputStream, storeName, version, partition));
    // Send a GET request
    ch.writeAndFlush(prepareRequest(storeName, version, partition));
    return inputStream;
  }

  public void close() {
    workerGroup.shutdownGracefully();
  }

  private FullHttpRequest prepareRequest(String storeName, int version, int partition) {
    FullHttpRequest request = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        String.format("/%s/%d/%d", storeName, version, partition));
    request.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
    return request;
  }
}
