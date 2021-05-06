package com.linkedin.davinci.ingestion.isolated;


import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;


public class IsolatedIngestionServerChannelInitializer extends ChannelInitializer<SocketChannel> {
  private final IsolatedIngestionServer isolatedIngestionServer;

  public IsolatedIngestionServerChannelInitializer(IsolatedIngestionServer isolatedIngestionServer) {
    this.isolatedIngestionServer = isolatedIngestionServer;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    ch.pipeline().addLast(new HttpRequestDecoder());
    ch.pipeline().addLast(new HttpObjectAggregator(1024 * 1024));
    ch.pipeline().addLast(new HttpResponseEncoder());
    ch.pipeline().addLast(new IsolatedIngestionServerHandler(isolatedIngestionServer));
  }
}