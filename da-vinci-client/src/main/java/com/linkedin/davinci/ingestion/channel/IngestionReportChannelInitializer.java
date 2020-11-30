package com.linkedin.davinci.ingestion.channel;

import com.linkedin.davinci.ingestion.handler.IngestionReportHandler;
import com.linkedin.davinci.ingestion.IngestionReportListener;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;


public class IngestionReportChannelInitializer extends ChannelInitializer<SocketChannel> {
  private final IngestionReportListener ingestionReportListener;
  private final InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer;

  public IngestionReportChannelInitializer(IngestionReportListener ingestionReportListener,
                                           InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer) {
    this.ingestionReportListener = ingestionReportListener;
    this.partitionStateSerializer = partitionStateSerializer;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    ch.pipeline().addLast(new HttpRequestDecoder());
    ch.pipeline().addLast(new HttpObjectAggregator(1024 * 1024));
    ch.pipeline().addLast(new HttpResponseEncoder());
    ch.pipeline().addLast(new IngestionReportHandler(ingestionReportListener, partitionStateSerializer));
  }
}