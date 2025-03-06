package com.linkedin.davinci.blobtransfer;

import static com.linkedin.davinci.blobtransfer.BlobTransferGlobalTrafficShapingHandlerHolder.getGlobalChannelTrafficShapingHandlerInstance;

import com.linkedin.davinci.blobtransfer.client.NettyFileTransferClient;
import com.linkedin.davinci.blobtransfer.server.P2PBlobTransferService;
import com.linkedin.davinci.stats.AggVersionedBlobTransferStats;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.venice.blobtransfer.BlobFinder;
import com.linkedin.venice.blobtransfer.DaVinciBlobFinder;
import com.linkedin.venice.blobtransfer.ServerBlobFinder;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import io.netty.handler.traffic.GlobalChannelTrafficShapingHandler;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class P2PBlobTransferManagerFactory {
  private static final Logger LOGGER = LogManager.getLogger(P2PBlobTransferManagerFactory.class);
  private final P2PBlobTransferConfig blobTransferConfig;
  private final ClientConfig clientConfig;
  private final CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewFuture;
  private final StorageMetadataService storageMetadataService;
  private final ReadOnlyStoreRepository readOnlyStoreRepository;
  private final StorageEngineRepository storageEngineRepository;
  private final AggVersionedBlobTransferStats aggVersionedBlobTransferStats;
  private BlobTransferManager<Void> p2pBlobTransferManager;

  public P2PBlobTransferManagerFactory(
      P2PBlobTransferConfig blobTransferConfig,
      ClientConfig clientConfig,
      CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewFuture,
      StorageMetadataService storageMetadataService,
      ReadOnlyStoreRepository readOnlyStoreRepository,
      StorageEngineRepository storageEngineRepository,
      AggVersionedBlobTransferStats aggVersionedBlobTransferStats) {
    // the client config and customized view future must one of them be null,
    // because it either for DaVinci Client or Server blob finder
    if (clientConfig == null && customizedViewFuture == null) {
      throw new IllegalArgumentException("The client config and customized view future must one of them be null");
    }

    if (clientConfig != null && customizedViewFuture != null) {
      throw new IllegalArgumentException("The client config and customized view future must one of them be null");
    }

    if (blobTransferConfig == null || storageMetadataService == null || readOnlyStoreRepository == null
        || storageEngineRepository == null || aggVersionedBlobTransferStats == null) {
      throw new IllegalArgumentException(
          "The blob transfer config, storage metadata service, read only store repository, storage engine repository, "
              + "and agg versioned blob transfer stats must not be null");
    }

    this.blobTransferConfig = blobTransferConfig;
    this.clientConfig = clientConfig;
    this.customizedViewFuture = customizedViewFuture;
    this.storageMetadataService = storageMetadataService;
    this.readOnlyStoreRepository = readOnlyStoreRepository;
    this.storageEngineRepository = storageEngineRepository;
    this.aggVersionedBlobTransferStats = aggVersionedBlobTransferStats;

    // initialize the P2P blob transfer manager
    try {
      BlobFinder blobFinder = null;
      if (customizedViewFuture != null && clientConfig == null) {
        blobFinder = new ServerBlobFinder(customizedViewFuture);
      } else if (customizedViewFuture == null && clientConfig != null) {
        blobFinder = new DaVinciBlobFinder(clientConfig);
      } else {
        throw new IllegalArgumentException(
            "The client config and customized view future must one of them be null during the initialization for blob transfer manager.");
      }

      GlobalChannelTrafficShapingHandler globalTrafficHandler = getGlobalChannelTrafficShapingHandlerInstance(
          blobTransferConfig.getBlobTransferClientReadLimitBytesPerSec(),
          blobTransferConfig.getBlobTransferServiceWriteLimitBytesPerSec());

      BlobSnapshotManager blobSnapshotManager = new BlobSnapshotManager(
          readOnlyStoreRepository,
          storageEngineRepository,
          storageMetadataService,
          blobTransferConfig.getMaxConcurrentSnapshotUser(),
          blobTransferConfig.getSnapshotRetentionTimeInMin(),
          blobTransferConfig.getTransferSnapshotTableFormat());

      p2pBlobTransferManager = new NettyP2PBlobTransferManager(
          new P2PBlobTransferService(
              blobTransferConfig.getP2pTransferServerPort(),
              blobTransferConfig.getBaseDir(),
              blobTransferConfig.getBlobTransferMaxTimeoutInMin(),
              blobSnapshotManager,
              globalTrafficHandler),
          new NettyFileTransferClient(
              blobTransferConfig.getP2pTransferClientPort(),
              blobTransferConfig.getBaseDir(),
              storageMetadataService,
              blobTransferConfig.getPeersConnectivityFreshnessInSeconds(),
              globalTrafficHandler),
          blobFinder,
          blobTransferConfig.getBaseDir(),
          aggVersionedBlobTransferStats);

      // start the P2P blob transfer manager
      p2pBlobTransferManager.start();
    } catch (Exception e) {
      // swallow the exception and continue the consumption via pubsub system
      LOGGER.warn("Failed to start up the P2P blob transfer manager", e);
    }
  }

  /**
   * Builder for {@link P2PBlobTransferManagerFactory} to create a new instance.
   */
  public static class P2PBlobTransferManagerFactoryBuilder {
    private P2PBlobTransferConfig blobTransferConfig;
    private ClientConfig clientConfig;
    private CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewFuture;
    private StorageMetadataService storageMetadataService;
    private ReadOnlyStoreRepository readOnlyStoreRepository;
    private StorageEngineRepository storageEngineRepository;
    private AggVersionedBlobTransferStats aggVersionedBlobTransferStats;

    public P2PBlobTransferManagerFactoryBuilder setBlobTransferConfig(P2PBlobTransferConfig blobTransferConfig) {
      this.blobTransferConfig = blobTransferConfig;
      return this;
    }

    public P2PBlobTransferManagerFactoryBuilder setClientConfig(ClientConfig clientConfig) {
      this.clientConfig = clientConfig;
      return this;
    }

    public P2PBlobTransferManagerFactoryBuilder setCustomizedViewFuture(
        CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewFuture) {
      this.customizedViewFuture = customizedViewFuture;
      return this;
    }

    public P2PBlobTransferManagerFactoryBuilder setStorageMetadataService(
        StorageMetadataService storageMetadataService) {
      this.storageMetadataService = storageMetadataService;
      return this;
    }

    public P2PBlobTransferManagerFactoryBuilder setReadOnlyStoreRepository(
        ReadOnlyStoreRepository readOnlyStoreRepository) {
      this.readOnlyStoreRepository = readOnlyStoreRepository;
      return this;
    }

    public P2PBlobTransferManagerFactoryBuilder setStorageEngineRepository(
        StorageEngineRepository storageEngineRepository) {
      this.storageEngineRepository = storageEngineRepository;
      return this;
    }

    public P2PBlobTransferManagerFactoryBuilder setAggVersionedBlobTransferStats(
        AggVersionedBlobTransferStats aggVersionedBlobTransferStats) {
      this.aggVersionedBlobTransferStats = aggVersionedBlobTransferStats;
      return this;
    }

    public BlobTransferManager<Void> build() {
      P2PBlobTransferManagerFactory p2pBlobTransferManagerFactory = new P2PBlobTransferManagerFactory(
          blobTransferConfig,
          clientConfig,
          customizedViewFuture,
          storageMetadataService,
          readOnlyStoreRepository,
          storageEngineRepository,
          aggVersionedBlobTransferStats);

      return p2pBlobTransferManagerFactory.p2pBlobTransferManager;
    }
  }
}
