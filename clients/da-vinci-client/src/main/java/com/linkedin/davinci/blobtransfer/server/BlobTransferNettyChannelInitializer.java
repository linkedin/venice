package com.linkedin.davinci.blobtransfer.server;

import com.linkedin.davinci.blobtransfer.BlobSnapshotManager;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;


public class BlobTransferNettyChannelInitializer extends ChannelInitializer<SocketChannel> {
  private final String baseDir;
  private final int blobTransferMaxTimeoutInMin;
  private BlobSnapshotManager blobSnapshotManager;

  private final long blobTransferServiceWriteLimitBytesPerSec;
  private static final long READ_LIMIT_BYTES_PER_SEC = 20971520L; // 20 MB/s
  private static final long CHECK_INTERVAL_MS = 1000L;

  public BlobTransferNettyChannelInitializer(
      String baseDir,
      int blobTransferMaxTimeoutInMin,
      BlobSnapshotManager blobSnapshotManager,
      long blobTransferServiceWriteLimitBytesPerSec) {
    this.baseDir = baseDir;
    this.blobTransferMaxTimeoutInMin = blobTransferMaxTimeoutInMin;
    this.blobSnapshotManager = blobSnapshotManager;
    this.blobTransferServiceWriteLimitBytesPerSec = blobTransferServiceWriteLimitBytesPerSec;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    ChannelPipeline pipeline = ch.pipeline();

    pipeline
        .addLast(
            "trafficShaper",
            new ChannelTrafficShapingHandler(
                blobTransferServiceWriteLimitBytesPerSec,
                READ_LIMIT_BYTES_PER_SEC,
                CHECK_INTERVAL_MS))
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
