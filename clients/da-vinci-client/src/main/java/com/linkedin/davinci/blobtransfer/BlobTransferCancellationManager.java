package com.linkedin.davinci.blobtransfer;

import com.linkedin.davinci.blobtransfer.client.NettyFileTransferClient;
import com.linkedin.venice.exceptions.VeniceBlobTransferCancelledException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.channel.Channel;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * BlobTransferCancellationManager is responsible for managing the cancellation of blob transfers.
 * It provides a centralized way to track ongoing transfers, request cancellations, and coordinate
 * the shutdown of active channels and peer chains.
 *
 * Cancellation workflow:
 *   - Set cancellation request flag to prevent new peer attempts in the chain
 *   - Close any active network channel to abort ongoing data transfer
 *   - Wait for the partition-level transfer future to complete with cancellation exception
 *   - The entire chain of peer attempts will terminate early due to the cancellation flag
 *
 */
public class BlobTransferCancellationManager {
  private static final Logger LOGGER = LogManager.getLogger(BlobTransferCancellationManager.class);

  // Track ongoing blob transfers: <replicaId, perPartitionTransferFuture>
  private final Map<String, CompletableFuture<InputStream>> partitionLevelTransferStatus =
      new VeniceConcurrentHashMap<>();

  // Track blob transfer cancellation requests: <replicaId, cancellation requested flag>
  private final Map<String, AtomicBoolean> partitionLevelCancellationFlag = new VeniceConcurrentHashMap<>();

  private final NettyFileTransferClient nettyClient;

  public BlobTransferCancellationManager(NettyFileTransferClient nettyClient) {
    this.nettyClient = nettyClient;
  }

  /**
   * Register a blob transfer for tracking. This should be called when a new blob transfer starts.
   *
   * @param storeName the name of the store
   * @param version the version of the store
   * @param partition the partition of the store
   * @param perPartitionTransferFuture the CompletableFuture representing the transfer operation
   */
  public void registerTransfer(
      String storeName,
      int version,
      int partition,
      CompletableFuture<InputStream> perPartitionTransferFuture) {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic(storeName, version), partition);

    // Initialize the partition level transfer future
    partitionLevelTransferStatus.put(replicaId, perPartitionTransferFuture);

    // Initialize cancellation request flag as false
    partitionLevelCancellationFlag.put(replicaId, new AtomicBoolean(false));

    // Remove from tracking when complete
    // NOTE: We do NOT remove cancellation flags here because we need to check it after transfer completes
    // to prevent the race where: transfer completes -> cancellation request arrives -> consumption starts
    // The cancellation boolean flag will be cleaned up later when new consumption starts or cancellation completes
    perPartitionTransferFuture.whenComplete((result, throwable) -> {
      partitionLevelTransferStatus.remove(replicaId);
    });
  }

  /**
   * Cancel an ongoing blob transfer for the given storeName, version and partition.
   * This method performs the following steps:
   * - Sets the cancellation request flag to stop new peer attempts in the chain
   * - Closes the active channel to abort ongoing data transfer and gracefully cleanup
   * - Waits for the partition-level transfer future to complete
   *
   *
   * @param storeName the name of the store
   * @param version the version of the store
   * @param partition the partition of the store
   * @param timeoutInSeconds maximum time to wait for cancellation to complete
   * @throws InterruptedException if interrupted while waiting
   * @throws TimeoutException if cancellation doesn't complete within timeout
   * @throws ExecutionException if the transfer completes with an unexpected exception
   */
  public void cancelTransfer(String storeName, int version, int partition, int timeoutInSeconds)
      throws InterruptedException, TimeoutException, ExecutionException {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic(storeName, version), partition);

    LOGGER.info("Cancellation request received for replica {}. Starting coordinated cancellation.", replicaId);

    // Step 1: Set the cancellation request flag FIRST
    // This prevents the peer chain from attempting more peers and causes early termination
    AtomicBoolean cancellationFlag = partitionLevelCancellationFlag.get(replicaId);
    if (cancellationFlag != null) {
      cancellationFlag.set(true);
      LOGGER.info(
          "Step 1: Set cancellation request flag for replica {}. "
              + "This will stop the entire chain of peer attempts.",
          replicaId);
    } else {
      LOGGER.info("No cancellation flag found for replica {}. Creating new flag.", replicaId);
      partitionLevelCancellationFlag.put(replicaId, new AtomicBoolean(true));
    }

    // Step 2: Check if there's an ongoing transfer
    CompletableFuture<InputStream> perPartitionTransferFuture = partitionLevelTransferStatus.get(replicaId);
    if (perPartitionTransferFuture == null || perPartitionTransferFuture.isDone()) {
      LOGGER.info(
          "No ongoing blob transfer found for replica {}. Cancellation flag is set to prevent future transfers.",
          replicaId);
      return;
    }

    LOGGER.info(
        "Step 2: Ongoing blob transfer detected for replica {}. Proceeding with active channel cancellation.",
        replicaId);

    // Step 3: Close the current ongoing channel to abort data transfer
    // This will cause the current peer channel to fail, and combined with the cancellation flag,
    // the entire chain of peer attempts will terminate
    Channel currentChannel = nettyClient.getActiveChannel(storeName, version, partition);
    if (currentChannel != null && currentChannel.isActive()) {
      LOGGER.info("Step 3: Closing active channel for replica {} to abort ongoing data transfer.", replicaId);
      try {
        currentChannel.close().syncUninterruptibly();
        LOGGER.info("Active channel closed successfully for replica {}", replicaId);
      } catch (Exception e) {
        LOGGER.warn("Exception while closing channel for replica {}. Continuing with cancellation.", replicaId, e);
      }
    } else {
      LOGGER.info("No active channel found for replica {}. Transfer may be between peer attempts.", replicaId);
    }

    // Step 4: Wait for perPartitionTransferFuture to complete
    // The future should complete with VeniceBlobTransferCancelledException due to:
    // - Channel closure triggering failure on current peer attempt
    // - Cancellation flag preventing new peer attempts
    // - chainOfPeersFuture detecting cancellation flag and completing with cancellation exception
    LOGGER.info(
        "Step 4: Waiting for partition-level transfer future to complete for replica {} (timeout: {} seconds).",
        replicaId,
        timeoutInSeconds);

    try {
      perPartitionTransferFuture.get(timeoutInSeconds, TimeUnit.SECONDS);
      LOGGER.info(
          "Blob transfer completed successfully for replica {} despite cancellation request. "
              + "This can happen if transfer completed just before cancellation.",
          replicaId);
    } catch (ExecutionException e) {
      // Check if it's the EXPECTED VeniceBlobTransferCancelledException
      if (e.getCause() instanceof VeniceBlobTransferCancelledException) {
        LOGGER.info(
            "Blob transfer cancelled successfully for replica {} with expected cancellation exception: {}",
            replicaId,
            e.getCause().getMessage());
      } else {
        LOGGER.warn(
            "Blob transfer for replica {} completed with unexpected exception: {}. "
                + "This may indicate a transfer failure rather than cancellation.",
            replicaId,
            e.getCause().getMessage());
        throw e;
      }
    }

    LOGGER.info("Cancellation coordination completed for replica {}", replicaId);
  }

  /**
   * Check if blob transfer cancellation was requested for the given partition.
   * This flag is used to:
   * - Skip starting consumption after successful blob transfer if cancellation was requested
   * - Terminate the peer chain early during blob transfer
   * @param storeName the name of the store
   * @param version the version of the store
   * @param partition the partition of the store
   * @return true if blob transfer cancellation was requested for this partition, false otherwise
   */
  public boolean isBlobTransferCancelled(String storeName, int version, int partition) {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic(storeName, version), partition);
    AtomicBoolean cancellationFlag = partitionLevelCancellationFlag.get(replicaId);
    boolean isCancelled = cancellationFlag != null && cancellationFlag.get();
    return isCancelled;
  }

  /**
   * Check if there's an ongoing blob transfer for the given partition.
   *
   * @param storeName the name of the store
   * @param version the version of the store
   * @param partition the partition of the store
   * @return true if blob transfer is in progress for this partition, false otherwise
   */
  public boolean isBlobTransferInProgress(String storeName, int version, int partition) {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic(storeName, version), partition);
    CompletableFuture<InputStream> transferFuture = partitionLevelTransferStatus.get(replicaId);
    boolean inProgress = transferFuture != null && !transferFuture.isDone();
    return inProgress;
  }

  /**
   * Get the cancellation request flag for a partition.
   * Used by the blob transfer manager to check cancellation during peer chain processing.
   *
   * @param storeName the name of the store
   * @param version the version of the store
   * @param partition the partition of the store
   * @return the cancellation flag, or null if not found
   */
  public AtomicBoolean getCancellationFlag(String storeName, int version, int partition) {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic(storeName, version), partition);
    return partitionLevelCancellationFlag.get(replicaId);
  }

  /**
   * Clear the cancellation request flag for a partition.
   * Should be called after consumption successfully starts or after cancellation completes.
   *
   * @param storeName the name of the store
   * @param version the version of the store
   * @param partition the partition of the store
   */
  public void clearCancellationRequest(String storeName, int version, int partition) {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic(storeName, version), partition);
    if (partitionLevelCancellationFlag.get(replicaId) != null) {
      partitionLevelCancellationFlag.remove(replicaId);
    }
  }
}
