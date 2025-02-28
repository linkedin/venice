package com.linkedin.davinci.blobtransfer.server;

import com.linkedin.davinci.blobtransfer.BlobSnapshotManager;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.handler.traffic.GlobalChannelTrafficShapingHandler;


public class BlobTransferNettyChannelInitializer extends ChannelInitializer<SocketChannel> {
  private final String baseDir;
  private final int blobTransferMaxTimeoutInMin;
  private BlobSnapshotManager blobSnapshotManager;

  private final GlobalChannelTrafficShapingHandler globalChannelTrafficShapingHandler;

  public BlobTransferNettyChannelInitializer(
      String baseDir,
      int blobTransferMaxTimeoutInMin,
      BlobSnapshotManager blobSnapshotManager,
      GlobalChannelTrafficShapingHandler globalChannelTrafficShapingHandler) {
    this.baseDir = baseDir;
    this.blobTransferMaxTimeoutInMin = blobTransferMaxTimeoutInMin;
    this.blobSnapshotManager = blobSnapshotManager;
    this.globalChannelTrafficShapingHandler = globalChannelTrafficShapingHandler;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    ChannelPipeline pipeline = ch.pipeline();

    pipeline.addLast("globalTrafficShaper", globalChannelTrafficShapingHandler)
        // for http encoding/decoding.
        .addLast("codec", new HttpServerCodec())
        .addLast("aggregator", new HttpObjectAggregator(65536))
        // for detecting idle connections
        .addLast("idleStateHandler", new IdleStateHandler(0, 0, 300))
        // for safe writing of chunks for responses
        .addLast("chunker", new ChunkedWriteHandler())
        // for handling p2p file transfer
        .addLast(
            "p2pFileTransferHandler",
            new P2PFileTransferServerHandler(baseDir, blobTransferMaxTimeoutInMin, blobSnapshotManager));
  }
}
