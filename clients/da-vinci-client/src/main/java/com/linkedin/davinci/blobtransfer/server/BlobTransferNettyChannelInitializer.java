package com.linkedin.davinci.blobtransfer.server;

import com.linkedin.davinci.storage.StorageMetadataService;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleStateHandler;


public class BlobTransferNettyChannelInitializer extends ChannelInitializer<SocketChannel> {
  private final String baseDir;
  private StorageMetadataService storageMetadataService;

  public BlobTransferNettyChannelInitializer(String baseDir, StorageMetadataService storageMetadataService) {
    this.baseDir = baseDir;
    this.storageMetadataService = storageMetadataService;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    ChannelPipeline pipeline = ch.pipeline();

    pipeline
        // for http encoding/decoding.
        .addLast("codec", new HttpServerCodec())
        .addLast("aggregator", new HttpObjectAggregator(65536))
        // for detecting idle connections
        .addLast("idleStateHandler", new IdleStateHandler(0, 0, 300))
        // for safe writing of chunks for responses
        .addLast("chunker", new ChunkedWriteHandler())
        // for handling p2p file transfer
        .addLast("p2pFileTransferHandler", new P2PFileTransferServerHandler(baseDir, storageMetadataService));
  }
}
