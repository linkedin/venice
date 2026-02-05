package com.linkedin.davinci.blobtransfer;

import com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferTableFormat;
import com.linkedin.davinci.stats.AggVersionedBlobTransferStats;
import com.linkedin.venice.annotation.Experimental;
import com.linkedin.venice.exceptions.VenicePeersAllFailedException;
import com.linkedin.venice.exceptions.VenicePeersNotFoundException;
import java.io.InputStream;
import java.util.concurrent.CompletionStage;


/**
 *
 * A BlobTransferManager is responsible for transferring blobs between two entities, either that Peer to Peer or node to
 * a blob store and vice versa. The underlying blob client is responsible for the actual transfer of the blob.
 * @param <T> the type of the object from the underlying blob client to indicate the upload status. It can be a blob ID
 *           indicating the blob has been uploaded or an enum representing the status of the blob transfer.
 */
public interface BlobTransferManager<T> extends AutoCloseable {
  /**
   * Start the blob transfer manager and related resources
   * @throws Exception
   */
  void start() throws Exception;

  /**
   * Get the blobs for the given storeName and partition
   * @param storeName
   * @param version
   * @param partition
   * @param requestTableFormat the table format defined in config (PLAIN_TABLE or BLOCK_BASED_TABLE).
   * @return the InputStream of the blob. The return type is experimental and may change in the future.
   * @throws VenicePeersNotFoundException when no peers are found for the requested blob.
   * @throws VenicePeersAllFailedException when peers are found but all peers sequentially fail to complete the blob transfer.
   * Other exceptions may be thrown, but it's wrapped inside the CompletionStage.
   */
  @Experimental
  CompletionStage<? extends InputStream> get(
      String storeName,
      int version,
      int partition,
      BlobTransferTableFormat requestTableFormat) throws VenicePeersNotFoundException, VenicePeersAllFailedException;

  /**
   * Put the blob for the given storeName and partition
   * @param storeName
   * @param version
   * @param partition
   * @return the type of the object returned from the underlying blob client to indicate the upload status
   */
  CompletionStage<T> put(String storeName, int version, int partition);

  /**
   * Close the blob transfer manager and related resources
   */
  void close() throws Exception;

  /**
   * Get the blob transfer stats
   * @return the blob transfer stats
   */
  AggVersionedBlobTransferStats getAggVersionedBlobTransferStats();

  BlobTransferStatusTrackingManager getTransferStatusTrackingManager();
}
