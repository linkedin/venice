package com.linkedin.davinci.bootstrap;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.davinci.DaVinciBackend;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.venice.blobtransfer.BlobTransferManager;
import com.linkedin.venice.blobtransfer.DvcBlobFinder;
import com.linkedin.venice.blobtransfer.NettyP2PBlobTransferManager;
import com.linkedin.venice.blobtransfer.client.NettyFileTransferClient;
import com.linkedin.venice.blobtransfer.server.P2PBlobTransferService;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.exceptions.VeniceBootstrapException;
import java.io.InputStream;
import java.util.concurrent.CompletionStage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DvcP2PRocksDbBootstrapper extends AbstractBootstrapper {
  private static final Logger LOGGER = LogManager.getLogger(DvcP2PRocksDbBootstrapper.class);
  private final BlobTransferManager p2pBlobTransferManager;
  private final DaVinciBackend backend;
  private final VeniceStoreVersionConfig veniceStoreVersionConfig;
  private final String p2pBlobTransferBaseDir;
  private final Boolean isBlobTransferEnabled;
  private final Boolean isHybridStore;

  public DvcP2PRocksDbBootstrapper(
      DaVinciBackend backend,
      VeniceServerConfig serverConfig,
      VeniceStoreVersionConfig veniceStoreVersionConfig,
      Boolean isBlobTransferEnabled,
      Boolean isHybridStore) {
    this(
        backend,
        serverConfig,
        veniceStoreVersionConfig,
        isBlobTransferEnabled,
        isHybridStore,
        new NettyP2PBlobTransferManager(
            new P2PBlobTransferService(serverConfig.getDvcP2pBlobTransferPort(), serverConfig.getRocksDBPath()),
            new NettyFileTransferClient(serverConfig.getDvcP2pFileTransferPort(), serverConfig.getRocksDBPath()),
            new DvcBlobFinder(
                ClientFactory.getTransportClient(backend.getClientConfig()),
                backend.getClientConfig().getVeniceURL())));
  }

  @VisibleForTesting
  public DvcP2PRocksDbBootstrapper(
      DaVinciBackend backend,
      VeniceServerConfig serverConfig,
      VeniceStoreVersionConfig veniceStoreVersionConfig,
      Boolean isBlobTransferEnabled,
      Boolean isHybridStore,
      BlobTransferManager p2pBlobTransferManager) {
    this.p2pBlobTransferBaseDir = serverConfig.getRocksDBPath();
    this.backend = backend;
    this.veniceStoreVersionConfig = veniceStoreVersionConfig;
    this.isBlobTransferEnabled = isBlobTransferEnabled;
    this.isHybridStore = isHybridStore;
    this.p2pBlobTransferManager = p2pBlobTransferManager;
  }

  /**
   * Starts consuming data for the specified partition of the store and version.
   * First, it calls the blob manager to transfer blobs from a peer node to a given path
   * Then, it attempts to bootstrap the database from the given path
   * If it fails to bootstrap from blobs at anypoint, it falls back to consuming data from Kafka
   *
   * @param storeName - store Name
   * @param versionNumber - store version
   * @param partitionId - partition ID for the store & version
   * @throws VeniceBootstrapException
   */
  @Override
  public void bootstrapDatabase(String storeName, int versionNumber, int partitionId) throws VeniceBootstrapException {
    if (!isBlobTransferEnabled || isHybridStore) {
      bootstrapFromKafka(partitionId);
    } else {
      bootstrapFromBlobs(storeName, versionNumber, partitionId);
    }
  }

  @Override
  protected void bootstrapFromBlobs(String storeName, int versionNumber, int partitionId)
      throws VeniceBootstrapException {
    try {
      p2pBlobTransferManager.start();

      CompletionStage<InputStream> p2pTransfer = p2pBlobTransferManager.get(storeName, versionNumber, partitionId);
      p2pTransfer
          .whenComplete((inputStream, throwable) -> handleP2PTransferCompletion(inputStream, throwable, partitionId));
    } catch (Exception e) {
      LOGGER.error("Error occurred during bootstrap from blobs: {}", e.getMessage());
      bootstrapFromKafka(partitionId);
    }
  }

  private void handleP2PTransferCompletion(InputStream inputStream, Throwable throwable, int partitionId) {
    if (throwable != null) {
      LOGGER.error("Error occurred during p2p transfer: {}", throwable.getMessage());
      bootstrapFromKafka(partitionId);
      return;
    }

    try {
      // P2P transfer is successful, attempt to bootstrap from blobs
      verifyBootstrap(p2pBlobTransferBaseDir);
      p2pBlobTransferManager.close();
    } catch (Exception e) {
      throw new VeniceBootstrapException(e.getMessage());
    }
  }

  @Override
  protected void bootstrapFromKafka(int partitionId) {
    try {
      p2pBlobTransferManager.close();
    } catch (Exception e) {
      LOGGER.error("Error closing p2pBlobTransferManager ", e.getMessage());
    }

    LOGGER.info("Bootstrapping from Kafka");
    backend.getIngestionBackend().startConsumption(veniceStoreVersionConfig, partitionId);
  }

}
