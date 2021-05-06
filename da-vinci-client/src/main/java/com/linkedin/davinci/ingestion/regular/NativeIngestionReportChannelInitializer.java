package com.linkedin.davinci.ingestion.regular;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;


public class NativeIngestionReportChannelInitializer extends ChannelInitializer<SocketChannel> {
  private final NativeIngestionMonitorService nativeIngestionMonitorService;

  public NativeIngestionReportChannelInitializer(NativeIngestionMonitorService nativeIngestionMonitorService) {
    this.nativeIngestionMonitorService = nativeIngestionMonitorService;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    ch.pipeline().addLast(new HttpRequestDecoder());
    ch.pipeline().addLast(new HttpObjectAggregator(1024 * 1024));
    ch.pipeline().addLast(new HttpResponseEncoder());
    ch.pipeline().addLast(new NativeIngestionReportHandler(nativeIngestionMonitorService));
  }
}