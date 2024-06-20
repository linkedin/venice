package com.linkedin.venice.blobtransfer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.Version;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ServerBlobFinder implements BlobFinder {
  private final HelixCustomizedViewOfflinePushRepository customizedViewRepository;

  private static final Logger LOGGER = LogManager.getLogger(ServerBlobFinder.class);

  public ServerBlobFinder(CompletableFuture<HelixCustomizedViewOfflinePushRepository> customizedViewRepository) {
    this.customizedViewRepository = customizedViewRepository.join();
  }

  @Override
  public BlobPeersDiscoveryResponse discoverBlobPeers(String storeName, int version, int partitionId) {
    BlobPeersDiscoveryResponse response = new BlobPeersDiscoveryResponse();
    try {
      String currentVersionResource = Version.composeKafkaTopic(storeName, version);
      // Get the partition assignments for the specific partition and retrieve the host names
      Partition partition =
          customizedViewRepository.getPartitionAssignments(currentVersionResource).getPartition(partitionId);
      if (partition != null) {
        response.addPartition(partition);
      }
    } catch (VeniceException e) {
      response.setError(true);
      String errorMsg = String
          .format("Error finding blob for store: %s, version: %d, partitionId: %d", storeName, version, partitionId);
      response.setErrorMessage(errorMsg + ".\n Error: " + e.getMessage());
      LOGGER.error(errorMsg, e);

    }

    return response;
  }
}
