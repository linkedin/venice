package com.linkedin.davinci.bootstrap;

import com.linkedin.davinci.DaVinciBackend;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.venice.blobtransfer.BlobFinder;
import com.linkedin.venice.blobtransfer.BlobTransferManager;
import com.linkedin.venice.blobtransfer.DvcBlobFinder;
import com.linkedin.venice.blobtransfer.NettyP2PBlobTransferManager;
import com.linkedin.venice.blobtransfer.client.NettyFileTransferClient;
import com.linkedin.venice.blobtransfer.server.P2PBlobTransferService;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.transport.TransportClient;
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
  private final Boolean isDvcP2pBlobTransferEnabled;
  private final Boolean isBlobTransferEnabled;
  private final Boolean isHybridStore;

  public DvcP2PRocksDbBootstrapper(
      DaVinciBackend backend,
      VeniceServerConfig serverConfig,
      VeniceStoreVersionConfig veniceStoreVersionConfig,
      Boolean isBlobTransferEnabled,
      Boolean isHybridStore) {
    int p2pBlobTransferPort = serverConfig.getDvcP2pBlobTransferPort();
    int p2pFileTransferPort = serverConfig.getDvcP2pFileTransferPort();
    String routerUrl = backend.getClientConfig().getVeniceURL();

    this.p2pBlobTransferBaseDir = serverConfig.getDvcP2pBlobTransferBaseDir();
    this.isDvcP2pBlobTransferEnabled = serverConfig.isDvcP2pBlobTransferEnabled();
    this.veniceStoreVersionConfig = veniceStoreVersionConfig;
    this.backend = backend;
    this.isBlobTransferEnabled = isBlobTransferEnabled;
    this.isHybridStore = isHybridStore;

    P2PBlobTransferService blobTransferService =
        new P2PBlobTransferService(p2pBlobTransferPort, p2pBlobTransferBaseDir);
    NettyFileTransferClient fileTransferClient =
        new NettyFileTransferClient(p2pFileTransferPort, p2pBlobTransferBaseDir);
    TransportClient transportClient = ClientFactory.getTransportClient(backend.getClientConfig());
    BlobFinder blobFinder = new DvcBlobFinder(transportClient, routerUrl);
    this.p2pBlobTransferManager = new NettyP2PBlobTransferManager(blobTransferService, fileTransferClient, blobFinder);

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
    if (!isDvcP2pBlobTransferEnabled || !isBlobTransferEnabled || isHybridStore) {
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
    LOGGER.info("Bootstrapping from Kafka");
    backend.getIngestionBackend().startConsumption(veniceStoreVersionConfig, partitionId);
  }

}
