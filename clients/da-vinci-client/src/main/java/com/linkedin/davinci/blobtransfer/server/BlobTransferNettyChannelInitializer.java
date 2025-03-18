package com.linkedin.davinci.blobtransfer.server;

import com.linkedin.alpini.netty4.ssl.SslInitializer;
import com.linkedin.davinci.blobtransfer.BlobSnapshotManager;
import com.linkedin.davinci.blobtransfer.BlobTransferAclHandler;
import com.linkedin.venice.listener.VerifySslHandler;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.handler.traffic.GlobalChannelTrafficShapingHandler;
import java.util.Optional;


public class BlobTransferNettyChannelInitializer extends ChannelInitializer<SocketChannel> {
  private final String baseDir;
  private final int blobTransferMaxTimeoutInMin;
  private BlobSnapshotManager blobSnapshotManager;
  private Optional<SSLFactory> sslFactory;
  private Optional<BlobTransferAclHandler> aclHandler;

  private final GlobalChannelTrafficShapingHandler globalChannelTrafficShapingHandler;
  private final VerifySslHandler verifySsl = new VerifySslHandler();

  public BlobTransferNettyChannelInitializer(
      String baseDir,
      int blobTransferMaxTimeoutInMin,
      BlobSnapshotManager blobSnapshotManager,
      GlobalChannelTrafficShapingHandler globalChannelTrafficShapingHandler,
      Optional<SSLFactory> sslFactory,
      Optional<BlobTransferAclHandler> aclHandler) {
    this.baseDir = baseDir;
    this.blobTransferMaxTimeoutInMin = blobTransferMaxTimeoutInMin;
    this.blobSnapshotManager = blobSnapshotManager;
    this.globalChannelTrafficShapingHandler = globalChannelTrafficShapingHandler;
    this.sslFactory = sslFactory;
    this.aclHandler = aclHandler;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    ChannelPipeline pipeline = ch.pipeline();
    // The sslFactory is already converted to openssl factory
    sslFactory.ifPresent(
        sslFactory -> ch.pipeline().addLast(new SslInitializer(SslUtils.toAlpiniSSLFactory(sslFactory), false)));

    pipeline.addLast("globalTrafficShaper", globalChannelTrafficShapingHandler)
        .addLast("codec", new HttpServerCodec()) // for http encoding/decoding.
        .addLast("aggregator", new HttpObjectAggregator(65536));

    if (sslFactory.isPresent()) {
      pipeline.addLast(verifySsl);
      if (aclHandler.isPresent()) {
        pipeline.addLast(aclHandler.get());
      }
    }

    // for detecting idle connections
    pipeline.addLast("idleStateHandler", new IdleStateHandler(0, 0, 300))
        // for safe writing of chunks for responses
        .addLast("chunker", new ChunkedWriteHandler())
        // for handling p2p file transfer
        .addLast(
            "p2pFileTransferHandler",
            new P2PFileTransferServerHandler(baseDir, blobTransferMaxTimeoutInMin, blobSnapshotManager));
  }
}
