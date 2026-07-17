package com.linkedin.venice.blobtransfer;

import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.client.store.ClientConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Discovers Da Vinci peers first and Venice servers second for client cold-start blob transfer.
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
    BlobPeersDiscoveryResponse daVinciResponse = daVinciBlobFinder.discoverBlobPeers(storeName, version, partitionId);
    BlobPeersDiscoveryResponse serverResponse = serverBlobFinder.discoverBlobPeers(storeName, version, partitionId);

    List<String> daVinciPeers = getDiscoveredPeers(daVinciResponse);
    List<String> serverPeers = getDiscoveredPeers(serverResponse);
    Collections.shuffle(daVinciPeers);
    Collections.shuffle(serverPeers);

    BlobPeersDiscoveryResponse response = new BlobPeersDiscoveryResponse();
    List<String> discoveredPeers = new ArrayList<>(daVinciPeers.size() + serverPeers.size());
    discoveredPeers.addAll(daVinciPeers);
    discoveredPeers.addAll(serverPeers);
    response.setDiscoveryResult(discoveredPeers);
    response.setServerHostNames(getNormalizedHostNames(serverPeers));

    if (discoveredPeers.isEmpty() && hasError(daVinciResponse) && hasError(serverResponse)) {
      response.setError(true);
      response.setErrorMessage(
          "Failed to discover both Da Vinci peers and Venice servers. Da Vinci error: "
              + getErrorMessage(daVinciResponse) + "; server error: " + getErrorMessage(serverResponse));
    }
    LOGGER.info(
        "Discovered {} Da Vinci peer(s) and {} Venice server peer(s) for store {} version {} partition {}.",
        daVinciPeers.size(),
        serverPeers.size(),
        storeName,
        version,
        partitionId);
    return response;
  }

  @Override
  public boolean shouldPreservePeerOrder() {
    return true;
  }

  private static List<String> getDiscoveredPeers(BlobPeersDiscoveryResponse response) {
    if (response == null || response.isError() || response.getDiscoveryResult() == null
        || response.getDiscoveryResult().isEmpty()) {
      return new ArrayList<>();
    }
    return new ArrayList<>(response.getDiscoveryResult());
  }

  private static boolean hasError(BlobPeersDiscoveryResponse response) {
    return response == null || response.isError();
  }

  private static Set<String> getNormalizedHostNames(List<String> peers) {
    Set<String> hostNames = new HashSet<>(peers.size());
    for (String peer: peers) {
      hostNames.add(peer.split("_")[0]);
    }
    return hostNames;
  }

  private static String getErrorMessage(BlobPeersDiscoveryResponse response) {
    return response == null ? "null response" : response.getErrorMessage();
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
