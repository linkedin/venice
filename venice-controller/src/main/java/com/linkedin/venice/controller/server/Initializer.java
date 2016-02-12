package com.linkedin.venice.controller.server;

import com.linkedin.venice.controller.Admin;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.ssl.SslContext;


public class Initializer extends ChannelInitializer<SocketChannel> {

  private final SslContext sslCtx;
  private String clusterName;
  private Admin admin;

  public Initializer(String clusterName, Admin admin, SslContext sslCtx) {
    this.sslCtx = sslCtx;
    this.clusterName = clusterName;
    this.admin = admin;
  }

  @Override
  public void initChannel(SocketChannel ch) {
    ChannelPipeline pipeline = ch.pipeline();

    if (sslCtx != null) {
      pipeline.addLast(sslCtx.newHandler(ch.alloc()));
    }

    pipeline.addLast(new HttpRequestDecoder());
    pipeline.addLast(new HttpResponseEncoder());

    // Remove the following line if you don't want automatic content compression.
    //pipeline.addLast(new HttpContentCompressor());

    pipeline.addLast(new Handler(clusterName, admin));
  }
}