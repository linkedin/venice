package com.linkedin.venice.meta;

import java.util.List;


/**
 * Interface defines all the operations related to transferring and managing blobs
 * between nodes.
 */
public interface BlobTransferManager {
  /**
   * Get live node hostnames that are ready to provide blobs
   * @return a list of live node's hostnames
   */
  List<String> getLiveNodeHostNamesForTransfer(String storeName, int version, int partitionID);
}
