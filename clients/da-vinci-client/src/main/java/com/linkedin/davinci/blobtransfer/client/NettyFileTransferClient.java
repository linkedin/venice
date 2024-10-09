package com.linkedin.davinci.blobtransfer.client;

import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.venice.exceptions.VenicePeersConnectionException;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class NettyFileTransferClient {
  private static final Logger LOGGER = LogManager.getLogger(NettyFileTransferClient.class);
  private static final int MAX_METADATA_CONTENT_LENGTH = 1024 * 1024 * 100;
  EventLoopGroup workerGroup;
  Bootstrap clientBootstrap;
  private final String baseDir;
  private final int serverPort;
  private StorageMetadataService storageMetadataService;

  // TODO 1: move tunable configs to a config class
  // TODO 2: consider either increasing worker threads or have a dedicated thread pool to handle requests.
  public NettyFileTransferClient(int serverPort, String baseDir, StorageMetadataService storageMetadataService) {
    this.baseDir = baseDir;
    this.serverPort = serverPort;
    this.storageMetadataService = storageMetadataService;

    clientBootstrap = new Bootstrap();
    workerGroup = new NioEventLoopGroup();
    clientBootstrap.group(workerGroup);
    clientBootstrap.channel(NioSocketChannel.class);
    clientBootstrap.option(ChannelOption.SO_KEEPALIVE, true);
    clientBootstrap.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) {
        ch.pipeline().addLast(new HttpClientCodec());
      }
    });
  }

  public CompletionStage<InputStream> get(String host, String storeName, int version, int partition) {
    CompletionStage<InputStream> inputStream = new CompletableFuture<>();
    try {
      // Connects to the remote host
      Channel ch = connectToHost(host, storeName, version, partition);

      // Request to get the blob file and metadata
      // Attach the file handler to the pipeline
      // Attach the metadata handler to the pipeline
      ch.pipeline()
          .addLast(new MetadataAggregator(MAX_METADATA_CONTENT_LENGTH))
          .addLast(new P2PFileTransferClientHandler(baseDir, inputStream, storeName, version, partition))
          .addLast(new P2PMetadataTransferHandler(storageMetadataService, baseDir, storeName, version, partition));
      // Send a GET request
      ch.writeAndFlush(prepareRequest(storeName, version, partition));
    } catch (Exception e) {
      if (!inputStream.toCompletableFuture().isCompletedExceptionally()) {
        inputStream.toCompletableFuture().completeExceptionally(e);
      }
    }
    return inputStream;
  }

  public void close() {
    workerGroup.shutdownGracefully();
  }

  private FullHttpRequest prepareRequest(String storeName, int version, int partition) {
    return new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        String.format("/%s/%d/%d", storeName, version, partition));
  }

  /**
   * Connects to the host
   */
  private Channel connectToHost(String host, String storeName, int version, int partition) {
    try {
      return clientBootstrap.connect(host, serverPort).sync().channel();
    } catch (Exception e) {
      String errorMsg = String.format(
          "Failed to connect to the host: %s for blob transfer for store: %s, version: %d, partition: %d",
          host,
          storeName,
          version,
          partition);
      LOGGER.error(errorMsg, e);
      throw new VenicePeersConnectionException(errorMsg, e);
    }
  }
}
