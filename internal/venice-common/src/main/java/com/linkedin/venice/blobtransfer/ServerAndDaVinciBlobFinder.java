package com.linkedin.venice.blobtransfer;

import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.client.store.ClientConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Discovers Da Vinci peers first and lazily discovers Venice servers only after the primary peers are exhausted.
 */
public class ServerAndDaVinciBlobFinder implements BlobFinder {
  private static final Logger LOGGER = LogManager.getLogger(ServerAndDaVinciBlobFinder.class);

  private final BlobFinder daVinciBlobFinder;
  private final BlobFinder serverBlobFinder;

  public ServerAndDaVinciBlobFinder(ClientConfig clientConfig) {
    this(new DaVinciBlobFinder(clientConfig), new MetadataBasedServerBlobFinder(clientConfig));
  }

  @VisibleForTesting
  ServerAndDaVinciBlobFinder(BlobFinder daVinciBlobFinder, BlobFinder serverBlobFinder) {
    this.daVinciBlobFinder = daVinciBlobFinder;
    this.serverBlobFinder = serverBlobFinder;
  }

  @Override
  public BlobPeersDiscoveryResponse discoverBlobPeers(String storeName, int version, int partitionId) {
    BlobPeersDiscoveryResponse response = daVinciBlobFinder.discoverBlobPeers(storeName, version, partitionId);
    LOGGER.info(
        "Discovered {} Da Vinci peer(s) for store {} version {} partition {}.",
        getDiscoveredPeerCount(response),
        storeName,
        version,
        partitionId);
    return response;
  }

  @Override
  public boolean supportsFallback() {
    return true;
  }

  @Override
  public BlobPeersDiscoveryResponse discoverFallbackBlobPeers(String storeName, int version, int partitionId) {
    BlobPeersDiscoveryResponse response = serverBlobFinder.discoverBlobPeers(storeName, version, partitionId);
    LOGGER.info(
        "Discovered {} Venice server peer(s) for store {} version {} partition {}.",
        getDiscoveredPeerCount(response),
        storeName,
        version,
        partitionId);
    return response;
  }

  private static int getDiscoveredPeerCount(BlobPeersDiscoveryResponse response) {
    return response == null || response.getDiscoveryResult() == null ? 0 : response.getDiscoveryResult().size();
  }

  @Override
  public void close() throws Exception {
    try {
      daVinciBlobFinder.close();
    } finally {
      serverBlobFinder.close();
    }
  }
}
