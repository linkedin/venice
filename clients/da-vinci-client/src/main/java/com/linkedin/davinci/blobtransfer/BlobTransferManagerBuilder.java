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
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import io.netty.handler.traffic.GlobalChannelTrafficShapingHandler;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A builder class to build the blob transfer manager.
 */
public class BlobTransferManagerBuilder {
  private static final Logger LOGGER = LogManager.getLogger(BlobTransferManagerBuilder.class);

  private P2PBlobTransferConfig blobTransferConfig;
  private ClientConfig clientConfig;
  private CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewFuture;
  private StorageMetadataService storageMetadataService;
  private ReadOnlyStoreRepository readOnlyStoreRepository;
  private StorageEngineRepository storageEngineRepository;
  private AggVersionedBlobTransferStats aggVersionedBlobTransferStats;
  private Optional<SSLFactory> sslFactory;
  private Optional<BlobTransferAclHandler> aclHandler;

  public BlobTransferManagerBuilder() {
  }

  public BlobTransferManagerBuilder setBlobTransferConfig(P2PBlobTransferConfig blobTransferConfig) {
    this.blobTransferConfig = blobTransferConfig;
    return this;
  }

  public BlobTransferManagerBuilder setClientConfig(ClientConfig clientConfig) {
    this.clientConfig = clientConfig;
    return this;
  }

  public BlobTransferManagerBuilder setCustomizedViewFuture(
      CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewFuture) {
    this.customizedViewFuture = customizedViewFuture;
    return this;
  }

  public BlobTransferManagerBuilder setStorageMetadataService(StorageMetadataService storageMetadataService) {
    this.storageMetadataService = storageMetadataService;
    return this;
  }

  public BlobTransferManagerBuilder setReadOnlyStoreRepository(ReadOnlyStoreRepository readOnlyStoreRepository) {
    this.readOnlyStoreRepository = readOnlyStoreRepository;
    return this;
  }

  public BlobTransferManagerBuilder setStorageEngineRepository(StorageEngineRepository storageEngineRepository) {
    this.storageEngineRepository = storageEngineRepository;
    return this;
  }

  public BlobTransferManagerBuilder setAggVersionedBlobTransferStats(
      AggVersionedBlobTransferStats aggVersionedBlobTransferStats) {
    this.aggVersionedBlobTransferStats = aggVersionedBlobTransferStats;
    return this;
  }

  public BlobTransferManagerBuilder setBlobTransferSSLFactory(Optional<SSLFactory> sslFactory) {
    // If sslFactory is present, convert it to one with OpenSSL support, if it's empty, just keep it as empty
    this.sslFactory =
        sslFactory.isPresent() ? Optional.of(SslUtils.toSSLFactoryWithOpenSSLSupport(sslFactory.get())) : sslFactory;
    return this;
  }

  public BlobTransferManagerBuilder setBlobTransferAclHandler(Optional<BlobTransferAclHandler> blobTransferAclHandler) {
    this.aclHandler = blobTransferAclHandler;
    return this;
  }

  public BlobTransferManager<Void> build() {
    try {
      validateFields();
      // initialize the P2P blob transfer manager
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

      BlobTransferManager<Void> blobTransferManager = new NettyP2PBlobTransferManager(
          new P2PBlobTransferService(
              blobTransferConfig.getP2pTransferServerPort(),
              blobTransferConfig.getBaseDir(),
              blobTransferConfig.getBlobTransferMaxTimeoutInMin(),
              blobSnapshotManager,
              globalTrafficHandler,
              sslFactory,
              aclHandler),
          new NettyFileTransferClient(
              blobTransferConfig.getP2pTransferClientPort(),
              blobTransferConfig.getBaseDir(),
              storageMetadataService,
              blobTransferConfig.getPeersConnectivityFreshnessInSeconds(),
              globalTrafficHandler,
              sslFactory),
          blobFinder,
          blobTransferConfig.getBaseDir(),
          aggVersionedBlobTransferStats);

      // start the P2P blob transfer manager
      blobTransferManager.start();

      return blobTransferManager;
    } catch (Exception e) {
      // swallow the exception and continue the consumption via pubsub system
      LOGGER.warn("Failed to start up the blob transfer manager", e);
      return null;
    }
  }

  private void validateFields() {
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

    if (sslFactory == null || aclHandler == null || !sslFactory.isPresent() || !aclHandler.isPresent()) {
      throw new IllegalArgumentException("The ssl factory and acl handler must not be null and must be present");
    }
  }
}
