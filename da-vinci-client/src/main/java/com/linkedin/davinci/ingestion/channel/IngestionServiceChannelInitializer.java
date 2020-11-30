package com.linkedin.davinci.ingestion.channel;


import com.linkedin.davinci.ingestion.IngestionService;
import com.linkedin.davinci.ingestion.handler.IngestionServiceTaskHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;


public class IngestionServiceChannelInitializer extends ChannelInitializer<SocketChannel> {
  private final IngestionService ingestionService;

  public IngestionServiceChannelInitializer(IngestionService ingestionService) {
    this.ingestionService = ingestionService;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    ch.pipeline().addLast(new HttpRequestDecoder());
    ch.pipeline().addLast(new HttpObjectAggregator(1024 * 1024));
    ch.pipeline().addLast(new HttpResponseEncoder());
    ch.pipeline().addLast(new IngestionServiceTaskHandler(ingestionService));
  }
}