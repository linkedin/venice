package com.linkedin.davinci.blobtransfer.client;

import com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferTableFormat;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.venice.exceptions.VenicePeersConnectionException;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
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
import io.netty.handler.timeout.IdleStateHandler;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class NettyFileTransferClient {
  private static final Logger LOGGER = LogManager.getLogger(NettyFileTransferClient.class);
  private static final int MAX_METADATA_CONTENT_LENGTH = 1024 * 1024 * 100;
  private static final int TIMEOUT_IN_MINUTES = 5;
  EventLoopGroup workerGroup;
  Bootstrap clientBootstrap;
  private final String baseDir;
  private final int serverPort;
  private StorageMetadataService storageMetadataService;
  private final ExecutorService executorService;
  private final ScheduledExecutorService scheduledExecutorService;
  // A set to contain the connectable and unconnectable hosts for saving effort on reconnection
  private Set<String> unconnectableHosts = VeniceConcurrentHashMap.newKeySet();
  private Set<String> connectedHosts = VeniceConcurrentHashMap.newKeySet();

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
    this.executorService = Executors.newCachedThreadPool();
    this.scheduledExecutorService = Executors.newScheduledThreadPool(1);
  }

  /**
   * A method to get the connectable hosts for the given store, version, and partition
   * This method is only used for checking connectivity to the hosts. Channel is closed after checking.
   * @param discoveredHosts the list of discovered hosts for the store, version, and partition, but not necessarily connectable
   * @param storeName the store name
   * @param version the version
   * @param partition the partition
   * @return the list of connectable hosts
   */
  public Set<String> getConnectableHosts(
      HashSet<String> discoveredHosts,
      String storeName,
      int version,
      int partition) {
    List<CompletableFuture<String>> futures = new ArrayList<>();
    Set<String> connectableHosts = new HashSet<>();

    discoveredHosts.removeAll(unconnectableHosts);
    for (String host: discoveredHosts) {
      CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
        try {
          if (connectableHosts.contains(host)) {
            return host;
          }
          // Check if the host is connectable
          Channel channel = connectToHost(host, storeName, version, partition);
          if (channel != null && channel.isActive()) {
            connectableHosts.add(host);
            channel.close(); // this is only for checking connectivity no need to open it.
            return host;
          } else {
            unconnectableHosts.add(host);
            return null;
          }
        } catch (Exception e) {
          unconnectableHosts.add(host);
          return null;
        }
      }, executorService);

      futures.add(future);
    }

    // Wait for all futures to complete
    CompletableFuture<Void> allConnections = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

    scheduledExecutorService.schedule(() -> {
      if (!allConnections.isDone()) {
        for (CompletableFuture<String> future: futures) {
          if (!future.isDone()) {
            future.complete(null);
          }
        }
        allConnections.complete(null);
      }
    }, TIMEOUT_IN_MINUTES, TimeUnit.MINUTES);

    allConnections.join();

    // Collect only the successfully connected hosts
    for (CompletableFuture<String> future: futures) {
      try {
        String host = future.get();
        if (host != null) {
          connectableHosts.add(host);
        }
      } catch (Exception e) {
        LOGGER.error("Error getting result from future", e);
      }
    }

    return connectableHosts;
  }

  public CompletionStage<InputStream> get(
      String host,
      String storeName,
      int version,
      int partition,
      BlobTransferTableFormat requestedTableFormat) {
    CompletionStage<InputStream> inputStream = new CompletableFuture<>();
    try {
      // Connects to the remote host
      // Must open a new connection for each request (per store per version per partition level),
      // Otherwise response will be mixed up
      Channel ch = connectToHost(host, storeName, version, partition);

      // Request to get the blob file and metadata
      // Attach the file handler to the pipeline
      // Attach the metadata handler to the pipeline
      ch.pipeline()
          .addLast(new IdleStateHandler(0, 0, 60))
          .addLast(new MetadataAggregator(MAX_METADATA_CONTENT_LENGTH))
          .addLast(
              new P2PFileTransferClientHandler(
                  baseDir,
                  inputStream,
                  storeName,
                  version,
                  partition,
                  requestedTableFormat))
          .addLast(
              new P2PMetadataTransferHandler(
                  storageMetadataService,
                  baseDir,
                  storeName,
                  version,
                  partition,
                  requestedTableFormat));
      // Send a GET request
      ch.writeAndFlush(prepareRequest(storeName, version, partition, requestedTableFormat));
      // Set a timeout, otherwise if the host is not responding, the future will never complete
      scheduledExecutorService.schedule(() -> {
        if (!inputStream.toCompletableFuture().isDone()) {
          inputStream.toCompletableFuture()
              .completeExceptionally(
                  new TimeoutException(
                      "Request timed out for store " + storeName + " version " + version + " partition " + partition
                          + " table format " + requestedTableFormat + " from host " + host));
        }
      }, TIMEOUT_IN_MINUTES, TimeUnit.MINUTES);
    } catch (Exception e) {
      if (!inputStream.toCompletableFuture().isCompletedExceptionally()) {
        inputStream.toCompletableFuture().completeExceptionally(e);
      }
    }
    return inputStream;
  }

  public void close() {
    workerGroup.shutdownGracefully();
    executorService.shutdown();
    scheduledExecutorService.shutdown();
    unconnectableHosts.clear();
    connectedHosts.clear();
  }

  private FullHttpRequest prepareRequest(
      String storeName,
      int version,
      int partition,
      BlobTransferTableFormat requestTableFormat) {
    return new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        String.format("/%s/%d/%d/%s", storeName, version, partition, requestTableFormat.name()));
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
