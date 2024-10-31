package com.linkedin.davinci.blobtransfer;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.concurrent.CompletionStage;


/**
 * Peer-to-Peer (P2P) Blob Transfer Manager.
 * For P2P, the manager acts as both a client and the server to transfer blobs between nodes.
 */
public interface P2PBlobTransferManager<T> extends BlobTransferManager<T> {
  /**
   * For P2P blob transfer, only GET should be implemented and there's no need to implement PUT method by default. It's
   * subject to change depending on the use cases to use P2P blob transfer.
   *
   * @param storeName
   * @param version
   * @param partition
   * @return the type of the object returned from the underlying blob client to indicate the upload status
   */
  default CompletionStage<T> put(String storeName, int version, int partition) {
    throw new VeniceException("PUT method is not supported for P2P Blob Transfer");
  }
}
