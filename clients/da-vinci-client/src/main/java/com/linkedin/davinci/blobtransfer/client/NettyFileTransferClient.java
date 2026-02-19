package com.linkedin.davinci.blobtransfer.client;

import com.linkedin.alpini.base.concurrency.Executors;
import com.linkedin.alpini.base.misc.ThreadPoolExecutor;
import com.linkedin.davinci.blobtransfer.BlobTransferUtils;
import com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferTableFormat;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.stats.AggBlobTransferStats;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.venice.exceptions.VenicePeersConnectionException;
import com.linkedin.venice.listener.VerifySslHandler;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.DaemonThreadFactory;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
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
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.handler.traffic.GlobalChannelTrafficShapingHandler;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class NettyFileTransferClient {
  private static final Logger LOGGER = LogManager.getLogger(NettyFileTransferClient.class);
  private static final int MAX_METADATA_CONTENT_LENGTH = 1024 * 1024 * 100;
  private static final int ALL_HOSTS_CONNECTION_TIMEOUT_IN_MINUTES = 1;
  // Total connection timeout (TCP connection and SSL handshake)
  private static final int PER_HOST_CONNECTION_TIMEOUT_MS = 50 * 1000;
  // Maximum time that Netty will wait to establish the initial connection before failing. (TCP connection)
  private static final int CONNECTION_ESTABLISHMENT_TIMEOUT_MS = 30 * 1000;
  // The default checksum threadpool size is the number of available processors.
  private static final int DEFAULT_CHECKSUM_VALIDATION_THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors();
  EventLoopGroup workerGroup;
  Bootstrap clientBootstrap;
  private final String baseDir;
  private final int serverPort;
  private final int blobReceiveTimeoutInMin; // the timeout for receiving the blob file in minutes
  private final int blobReceiveReaderIdleTimeInSeconds; // the reader idle timeout while receiving the blob file in
                                                        // seconds
  private final int peersConnectivityFreshnessInSeconds; // the freshness of the peers connectivity records
  private final StorageMetadataService storageMetadataService;
  private final ExecutorService hostConnectExecutorService;
  private final ScheduledExecutorService connectTimeoutScheduler;
  private final ExecutorService checksumValidationExecutorService;

  // A map to contain the connectable and unconnectable hosts for saving effort on reconnection
  // format: host -> timestamp of the last connection attempt
  private final VeniceConcurrentHashMap<String, Long> unconnectableHostsToTimestamp = new VeniceConcurrentHashMap<>();
  private final VeniceConcurrentHashMap<String, Long> connectedHostsToTimestamp = new VeniceConcurrentHashMap<>();
  private final Supplier<VeniceNotifier> notifierSupplier;
  private final AggBlobTransferStats aggBlobTransferStats;

  private final VerifySslHandler verifySsl = new VerifySslHandler();

  // Track active channels: <replica_id, active channel with ongoing transfer>
  private final VeniceConcurrentHashMap<String, Channel> activeChannels = new VeniceConcurrentHashMap<>();

  // TODO: consider either increasing worker threads or have a dedicated thread pool to handle requests.
  public NettyFileTransferClient(
      int serverPort,
      String baseDir,
      StorageMetadataService storageMetadataService,
      int peersConnectivityFreshnessInSeconds,
      int blobReceiveTimeoutInMin,
      int blobReceiveReaderIdleTimeInSeconds,
      GlobalChannelTrafficShapingHandler globalChannelTrafficShapingHandler,
      AggBlobTransferStats aggBlobTransferStats,
      Optional<SSLFactory> sslFactory,
      Supplier<VeniceNotifier> notifierSupplier) {
    this.baseDir = baseDir;
    this.serverPort = serverPort;
    this.storageMetadataService = storageMetadataService;
    this.notifierSupplier = notifierSupplier;
    this.peersConnectivityFreshnessInSeconds = peersConnectivityFreshnessInSeconds;
    this.blobReceiveTimeoutInMin = blobReceiveTimeoutInMin;
    this.blobReceiveReaderIdleTimeInSeconds = blobReceiveReaderIdleTimeInSeconds;
    this.aggBlobTransferStats = aggBlobTransferStats;

    clientBootstrap = new Bootstrap();
    workerGroup = new NioEventLoopGroup();
    clientBootstrap.group(workerGroup);
    clientBootstrap.channel(NioSocketChannel.class);
    clientBootstrap.option(ChannelOption.SO_KEEPALIVE, true);
    clientBootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECTION_ESTABLISHMENT_TIMEOUT_MS);
    // Increase the receiver buffer size to 1MB.
    clientBootstrap.option(ChannelOption.SO_RCVBUF, 1 << 20);
    // Use adaptive receiver buffer allocator to dynamically adjust the receiver buffer size.
    clientBootstrap
        .option(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator(64 * 1024, 512 * 1024, 1 << 20));
    clientBootstrap.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) {
        SslHandler sslHandler = BlobTransferUtils.createBlobTransferClientSslHandler(sslFactory);
        ch.pipeline().addLast("ssl", sslHandler);

        // globalChannelTrafficShapingHandler is shared across all network channels to enforce global rate limits
        ch.pipeline().addLast("globalTrafficShaper", globalChannelTrafficShapingHandler);
        ch.pipeline().addLast(new HttpClientCodec());

        if (sslHandler != null) {
          ch.pipeline().addLast(verifySsl);
        }
      }
    });
    this.hostConnectExecutorService =
        Executors.newCachedThreadPool(new DaemonThreadFactory("Venice-BlobTransfer-Host-Connect-Executor-Service"));
    this.connectTimeoutScheduler = Executors
        .newSingleThreadScheduledExecutor(new DaemonThreadFactory("Venice-BlobTransfer-Client-Timeout-Checker"));
    this.checksumValidationExecutorService = new ThreadPoolExecutor(
        DEFAULT_CHECKSUM_VALIDATION_THREAD_POOL_SIZE,
        DEFAULT_CHECKSUM_VALIDATION_THREAD_POOL_SIZE,
        60L,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        new DaemonThreadFactory("Venice-BlobTransfer-Checksum-Validation-Executor-Service"));
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

    // 1. Purge the host connectivity records that are stale
    purgeStaleConnectivityRecords(unconnectableHostsToTimestamp);
    purgeStaleConnectivityRecords(connectedHostsToTimestamp);

    // 2. Remove the hosts that are already marked as unconnectable
    discoveredHosts.removeAll(unconnectableHostsToTimestamp.keySet());

    // 3. Check if the discovered hosts are already connectable
    Set<String> connectableHostsResult = new HashSet<>();
    Iterator<String> discoveredHostsIterator = discoveredHosts.iterator();
    while (discoveredHostsIterator.hasNext()) {
      String host = discoveredHostsIterator.next();
      if (connectedHostsToTimestamp.containsKey(host)) {
        connectableHostsResult.add(host);
        discoveredHostsIterator.remove();
      }
    }

    // 4. Checking connectivity of remaining host via connectToHost
    for (String host: discoveredHosts) {
      CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
        try {
          // Check if the host is connectable
          Channel channel = connectToHost(host, storeName, version, partition);
          if (channel != null && channel.isActive()) {
            // Mark the host as connected
            connectedHostsToTimestamp.put(host, System.currentTimeMillis());
            // this is only for checking connectivity no need to open it.
            channel.close().addListener((ChannelFuture channelCloseFuture) -> {
              if (!channelCloseFuture.isSuccess()) {
                // this host connection is not closed properly due to unexpect error
                LOGGER.error("Failed to close the active channel for host: {}", host);
                connectedHostsToTimestamp.remove(host);
                unconnectableHostsToTimestamp.put(host, System.currentTimeMillis());
              }
            });
            return host;
          } else {
            LOGGER.warn(
                "Failed to connect to host: {} for store {} version {} partition {}",
                host,
                storeName,
                version,
                partition);
            unconnectableHostsToTimestamp.put(host, System.currentTimeMillis());
            return null;
          }
        } catch (Exception e) {
          LOGGER.warn(
              "Failed to connect to host: {} for store {} version {} partition {}",
              host,
              storeName,
              version,
              partition,
              e);
          unconnectableHostsToTimestamp.put(host, System.currentTimeMillis());
          return null;
        }
      }, hostConnectExecutorService);

      futures.add(future);
    }

    // Wait for all futures to complete
    CompletableFuture<Void> allConnections = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

    connectTimeoutScheduler.schedule(() -> {
      if (!allConnections.isDone()) {
        for (CompletableFuture<String> future: futures) {
          if (!future.isDone()) {
            future.complete(null);
          }
        }
        allConnections.complete(null);
      }
    }, ALL_HOSTS_CONNECTION_TIMEOUT_IN_MINUTES, TimeUnit.MINUTES);

    allConnections.join();

    // 5. Collect only the successfully connected hosts
    for (CompletableFuture<String> future: futures) {
      try {
        String host = future.get();
        if (host != null) {
          connectableHostsResult.add(host);
        }
      } catch (Exception e) {
        LOGGER.error("Error getting result from future", e);
      }
    }
    // 6. Removed the unconnectable hosts from the result again,
    // because some connectable host may fail to close the channel
    connectableHostsResult.removeAll(unconnectableHostsToTimestamp.keySet());

    return connectableHostsResult;
  }

  /**
   * Check the freshness of the connectivity records and purge the stale records
   * @param hostsToTimestamp the map of hosts to the timestamp of the last connection attempt
   */
  public void purgeStaleConnectivityRecords(VeniceConcurrentHashMap<String, Long> hostsToTimestamp) {
    Iterator<Map.Entry<String, Long>> iterator = hostsToTimestamp.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, Long> entry = iterator.next();
      if (entry.getValue() == null || System.currentTimeMillis() - entry.getValue() > TimeUnit.SECONDS
          .toMillis(peersConnectivityFreshnessInSeconds)) {
        iterator.remove();
      }
    }
  }

  public CompletionStage<InputStream> get(
      String host,
      String storeName,
      int version,
      int partition,
      BlobTransferTableFormat requestedTableFormat) {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic(storeName, version), partition);
    CompletionStage<InputStream> perHostTransferFuture = new CompletableFuture<>();
    try {
      // Connects to the remote host
      // Must open a new connection for each request (per store per version per partition level),
      // Otherwise response will be mixed up
      Channel ch = connectToHost(host, storeName, version, partition);

      // Track the active channel
      activeChannels.put(replicaId, ch);

      // Remove from tracking when transfer completes
      perHostTransferFuture.toCompletableFuture().whenComplete((result, throwable) -> {
        activeChannels.remove(replicaId);
        if (throwable != null) {
          LOGGER.info(
              "Removed active channel tracking for replica {} after transfer failure: {}",
              replicaId,
              throwable.getMessage());
        } else {
          LOGGER.info("Removed active channel tracking for replica {} after successful transfer", replicaId);
        }
      });

      // Check if the channel already has a P2PFileTransferClientHandler/P2PMetadataTransferHandler
      if (ch.pipeline().get(P2PFileTransferClientHandler.class) != null
          || ch.pipeline().get(P2PMetadataTransferHandler.class) != null) {
        perHostTransferFuture.toCompletableFuture()
            .completeExceptionally(
                new VenicePeersConnectionException(
                    "The host " + host
                        + " channel already have P2PFileTransferClientHandler/P2PMetadataTransferHandler for "
                        + storeName + " version " + version + " partition " + partition + " table format "
                        + requestedTableFormat));
        return perHostTransferFuture;
      }

      // Request to get the blob file and metadata
      // Attach the file handler to the pipeline
      // Attach the metadata handler to the pipeline
      ch.pipeline()
          .addLast(new IdleStateHandler(blobReceiveReaderIdleTimeInSeconds, 0, 0))
          .addLast(new MetadataAggregator(MAX_METADATA_CONTENT_LENGTH))
          .addLast(
              new P2PFileTransferClientHandler(
                  baseDir,
                  perHostTransferFuture,
                  storeName,
                  version,
                  partition,
                  requestedTableFormat,
                  aggBlobTransferStats,
                  checksumValidationExecutorService))
          .addLast(
              new P2PMetadataTransferHandler(
                  storageMetadataService,
                  baseDir,
                  storeName,
                  version,
                  partition,
                  requestedTableFormat,
                  notifierSupplier));
      // Send a GET request
      ChannelFuture requestFuture =
          ch.writeAndFlush(prepareRequest(storeName, version, partition, requestedTableFormat));

      requestFuture.addListener(f -> {
        if (f.isSuccess()) {
          LOGGER.info("Request successfully sent to the server for replica {} to remote host {}", replicaId, host);
        } else {
          LOGGER.error("Failed to send request for replica {} to host {}", replicaId, host, f.cause());
        }
      });

      // Set a timeout, otherwise if the host is not responding, the future will never complete
      connectTimeoutScheduler.schedule(() -> {
        if (!perHostTransferFuture.toCompletableFuture().isDone()) {
          String errorMsg = String.format(
              "Request timed out for store %s version %d partition %d table format %s from host %s after %d minutes",
              storeName,
              version,
              partition,
              requestedTableFormat,
              host,
              blobReceiveTimeoutInMin);
          LOGGER.error(errorMsg);
          ch.close();
        }
      }, blobReceiveTimeoutInMin, TimeUnit.MINUTES);
    } catch (Exception e) {
      if (!perHostTransferFuture.toCompletableFuture().isCompletedExceptionally()) {
        perHostTransferFuture.toCompletableFuture().completeExceptionally(e);
      }
    }
    return perHostTransferFuture;
  }

  /**
   * Get the active channel for a given transfer, if any.
   * @param replicaId the replica ID (format: storeName_vVersion-partition)
   * @return the active Channel, or null if no active transfer
   */
  public Channel getActiveChannel(String replicaId) {
    return activeChannels.get(replicaId);
  }

  public void close() {
    workerGroup.shutdownGracefully();
    hostConnectExecutorService.shutdown();
    connectTimeoutScheduler.shutdown();
    checksumValidationExecutorService.shutdown();
    unconnectableHostsToTimestamp.clear();
    connectedHostsToTimestamp.clear();
    activeChannels.clear();
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
   * Connects to the host with a timeout
   */
  private Channel connectToHost(String host, String storeName, int version, int partition) {
    ChannelFuture connectFuture = null;
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic(storeName, version), partition);
    try {
      connectFuture = clientBootstrap.connect(host, serverPort);

      // Wait at most PER_HOST_CONNECTION_TIMEOUT_MS for the connect to finish
      boolean completed = connectFuture.awaitUninterruptibly(PER_HOST_CONNECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);

      if (!completed) {
        if (!connectFuture.isDone()) {
          connectFuture.cancel(true);
        }
        String errorMsg = String.format(
            "Timed out after %d ms waiting to connect to host: %s for blob transfer for replica %s",
            PER_HOST_CONNECTION_TIMEOUT_MS,
            host,
            replicaId);
        LOGGER.error(errorMsg);
        throw new VenicePeersConnectionException(errorMsg);
      }

      if (!connectFuture.isSuccess()) {
        Throwable cause = connectFuture.cause();
        String errorMsg =
            String.format("Failed to connect to the host: %s for blob transfer for replica %s", host, replicaId);
        LOGGER.error(errorMsg, cause);
        throw new VenicePeersConnectionException(errorMsg, cause);
      }
      return connectFuture.channel();
    } catch (Exception e) {
      String errorMsg =
          String.format("Exception while connecting to host: %s for blob transfer for replica %s", host, replicaId);
      LOGGER.error(errorMsg, e);
      throw new VenicePeersConnectionException(errorMsg, e);
    }
  }
}
