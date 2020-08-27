package com.linkedin.venice.ingestion.channel;

import com.linkedin.venice.ingestion.handler.IngestionReportHandler;
import com.linkedin.venice.ingestion.IngestionReportListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;


public class IngestionReportChannelInitializer extends ChannelInitializer<SocketChannel> {
  private final IngestionReportListener ingestionReportListener;

  public IngestionReportChannelInitializer(IngestionReportListener ingestionReportListener) {
    this.ingestionReportListener = ingestionReportListener;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    ch.pipeline().addLast(new HttpRequestDecoder());
    ch.pipeline().addLast(new HttpObjectAggregator(1024 * 1024));
    ch.pipeline().addLast(new HttpResponseEncoder());
    ch.pipeline().addLast(new IngestionReportHandler(ingestionReportListener));
  }
}