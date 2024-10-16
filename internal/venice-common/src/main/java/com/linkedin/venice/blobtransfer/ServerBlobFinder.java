package com.linkedin.venice.blobtransfer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Version;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ServerBlobFinder implements BlobFinder {
  private final CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewRepository;

  private static final Logger LOGGER = LogManager.getLogger(ServerBlobFinder.class);

  public ServerBlobFinder(CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewRepository) {
    this.customizedViewRepository = customizedViewRepository;
  }

  @Override
  public BlobPeersDiscoveryResponse discoverBlobPeers(String storeName, int version, int partitionId) {
    BlobPeersDiscoveryResponse response = new BlobPeersDiscoveryResponse();
    try {
      String currentVersionResource = Version.composeKafkaTopic(storeName, version);
      // Get the partition assignments for the specific partition and retrieve the host with COMPLETE status
      HelixCustomizedViewOfflinePushRepository customizedViewRepository = this.customizedViewRepository.get();

      List<String> hostNames = customizedViewRepository.getReadyToServeInstances(currentVersionResource, partitionId)
          .stream()
          .map(Instance::getHost)
          .collect(Collectors.toList());
      // Shuffle the list to avoid always picking the same host
      if (hostNames != null && !hostNames.isEmpty()) {
        Collections.shuffle(hostNames);
      }
      response.setDiscoveryResult(hostNames);
    } catch (VeniceException | InterruptedException | ExecutionException e) {
      response.setError(true);
      String errorMsg = String.format(
          "Error finding peers for blob transfer in store: %s, version: %d, partitionId: %d",
          storeName,
          version,
          partitionId);
      response.setErrorMessage(errorMsg + ".\n Error: " + e.getMessage());
      LOGGER.error(errorMsg, e);

    }

    return response;
  }

  @Override
  public void close() {
  }

}
