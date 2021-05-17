package com.linkedin.davinci.ingestion.main;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;


public class MainIngestionReportChannelInitializer extends ChannelInitializer<SocketChannel> {
  private final MainIngestionMonitorService mainIngestionMonitorService;

  public MainIngestionReportChannelInitializer(MainIngestionMonitorService mainIngestionMonitorService) {
    this.mainIngestionMonitorService = mainIngestionMonitorService;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    ch.pipeline().addLast(new HttpRequestDecoder());
    ch.pipeline().addLast(new HttpObjectAggregator(1024 * 1024));
    ch.pipeline().addLast(new HttpResponseEncoder());
    ch.pipeline().addLast(new MainIngestionReportHandler(mainIngestionMonitorService));
  }
}