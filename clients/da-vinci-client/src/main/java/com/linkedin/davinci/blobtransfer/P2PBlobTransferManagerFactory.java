package com.linkedin.davinci.blobtransfer;

import static com.linkedin.davinci.blobtransfer.BlobTransferGlobalTrafficShapingHandlerHolder.getGlobalChannelTrafficShapingHandlerInstance;

import com.linkedin.davinci.blobtransfer.client.NettyFileTransferClient;
import com.linkedin.davinci.blobtransfer.server.P2PBlobTransferService;
import com.linkedin.davinci.stats.AggVersionedBlobTransferStats;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
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

  /**
   * Get a P2P blob transfer manager for DaVinci Client and start it.
   * @param blobTransferConfig the P2P blob transfer config
   * @param clientConfig the client config
   * @param storageMetadataService the storage metadata service
   * @param readOnlyStoreRepository the read only store repository
   * @param storageEngineRepository the storage engine repository
   * @param aggVersionedBlobTransferStats the aggregated versioned blob transfer stats
   * @return the blob transfer manager for DaVinci Client
   * @throws Exception
   */
  public static BlobTransferManager<Void> getP2PBlobTransferManagerForDVCAndStart(
      P2PBlobTransferConfig blobTransferConfig,
      ClientConfig clientConfig,
      StorageMetadataService storageMetadataService,
      ReadOnlyStoreRepository readOnlyStoreRepository,
      StorageEngineRepository storageEngineRepository,
      AggVersionedBlobTransferStats aggVersionedBlobTransferStats) {
    try {
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

      BlobTransferManager<Void> manager = new NettyP2PBlobTransferManager(
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
          new DaVinciBlobFinder(clientConfig),
          blobTransferConfig.getBaseDir(),
          aggVersionedBlobTransferStats);

      manager.start();
      return manager;
    } catch (Exception e) {
      // swallow the exception and continue the consumption via pubsub system
      LOGGER.warn("Failed to start up the P2P blob transfer manager for DaVinci Client", e);
      return null;
    }
  }

  /**
   * Get a P2P blob transfer manager for Server and start it.
   * @param blobTransferConfig the P2P blob transfer config
   * @param customizedViewFuture the customized view future
   * @param storageMetadataService the storage metadata service
   * @param readOnlyStoreRepository the read only store repository
   * @param storageEngineRepository the storage engine repository
   * @param aggVersionedBlobTransferStats the aggregated versioned blob transfer stats
   * @return the blob transfer manager for server
   */
  public static BlobTransferManager<Void> getP2PBlobTransferManagerForServerAndStart(
      P2PBlobTransferConfig blobTransferConfig,
      CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewFuture,
      StorageMetadataService storageMetadataService,
      ReadOnlyStoreRepository readOnlyStoreRepository,
      StorageEngineRepository storageEngineRepository,
      AggVersionedBlobTransferStats aggVersionedBlobTransferStats) {
    try {
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

      BlobTransferManager<Void> manager = new NettyP2PBlobTransferManager(
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
          new ServerBlobFinder(customizedViewFuture),
          blobTransferConfig.getBaseDir(),
          aggVersionedBlobTransferStats);

      manager.start();
      return manager;
    } catch (Exception e) {
      // swallow the exception and continue the consumption via pubsub system
      LOGGER.warn("Failed to start up the P2P blob transfer manager for server", e);
      return null;
    }
  }
}
