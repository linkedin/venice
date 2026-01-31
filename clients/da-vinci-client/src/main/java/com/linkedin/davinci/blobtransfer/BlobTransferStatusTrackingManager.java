package com.linkedin.davinci.blobtransfer;

import com.linkedin.davinci.blobtransfer.client.NettyFileTransferClient;
import com.linkedin.venice.exceptions.VeniceBlobTransferCancelledException;
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
 * BlobTransferStatusTrackingManager is responsible for tracking and managing the status of blob transfers.
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
public class BlobTransferStatusTrackingManager {
  private static final Logger LOGGER = LogManager.getLogger(BlobTransferStatusTrackingManager.class);

  // Track ongoing blob transfers: <replicaId, perPartitionTransferFuture>
  private final Map<String, CompletableFuture<InputStream>> partitionLevelTransferStatus =
      new VeniceConcurrentHashMap<>();

  // Track blob transfer cancellation requests: <replicaId, cancellation requested flag>
  private final Map<String, AtomicBoolean> partitionLevelCancellationFlag = new VeniceConcurrentHashMap<>();

  private final NettyFileTransferClient nettyClient;

  public BlobTransferStatusTrackingManager(NettyFileTransferClient nettyClient) {
    this.nettyClient = nettyClient;
  }

  /**
   * Register a blob transfer for tracking. This should be called when a new blob transfer starts.
   *
   * @param replicaId the replica ID (format: storeName_vVersion-partition)
   * @param perPartitionTransferFuture the CompletableFuture representing the transfer operation
   */
  public void registerTransfer(String replicaId, CompletableFuture<InputStream> perPartitionTransferFuture) {

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
   * Cancel an ongoing blob transfer for the given replica.
   * This method performs the following steps:
   * - Sets the cancellation request flag to stop new peer attempts in the chain
   * - If transfer is in progress: closes the channel and waits for completion
   * - The flag remains set after this method completes
   *
   * This is a blocking behavior.
   *
   * @param replicaId the replica ID (format: storeName_vVersion-partition)
   * @param timeoutInSeconds maximum time to wait for cancellation to complete
   * @throws InterruptedException if interrupted while waiting
   * @throws TimeoutException if cancellation doesn't complete within timeout
   */
  public void cancelTransfer(String replicaId, int timeoutInSeconds) throws InterruptedException, TimeoutException {
    LOGGER.info("Cancellation request received for replica {}. Starting coordinated cancellation.", replicaId);

    // Step 1: Set the cancellation request flag FIRST
    // This prevents the peer chain from attempting more peers and causes early termination
    AtomicBoolean cancellationFlag =
        partitionLevelCancellationFlag.computeIfAbsent(replicaId, k -> new AtomicBoolean(false));
    cancellationFlag.set(true);
    LOGGER.info(
        "[Step 1 Flag Setting]: Set cancellation request flag for replica {}. "
            + "This will stop the entire chain of peer attempts.",
        replicaId);

    // Step 1.1: Check if there's an ongoing transfer
    CompletableFuture<InputStream> perPartitionTransferFuture = partitionLevelTransferStatus.get(replicaId);
    if (perPartitionTransferFuture == null) {
      LOGGER.info(
          "[Step 1.1 Flag Setting]: No ongoing blob transfer found for replica {}. Cancellation flag is no longer needed, removing cancellation flag.",
          replicaId);
      clearCancellationRequest(replicaId);
      return;
    } else if (perPartitionTransferFuture.isDone()) {
      LOGGER.info(
          "[Step 1.2 Flag Setting]: Having completed transfer for replica {}. Skipping closing channel and partition transfer.",
          replicaId);
      return;
    }

    // Step 2: Close the current ONGOING channel to abort data transfer
    // This will cause the current peer channel to fail, and combined with the cancellation flag,
    // the entire chain of peer attempts will terminate
    Channel currentChannel = nettyClient.getActiveChannel(replicaId);
    if (currentChannel != null && currentChannel.isActive()) {
      LOGGER.info(
          "[Step 2 Channel Close]: Starting close active channel for replica {} to abort ongoing data transfer.",
          replicaId);
      try {
        currentChannel.close().syncUninterruptibly();
        LOGGER.info("[Step 2 Channel Close]: Active channel closed successfully for replica {}", replicaId);
      } catch (Exception e) {
        LOGGER.warn(
            "[Step 2 Channel Close]: Exception while closing channel for replica {}. Continuing with cancellation.",
            replicaId,
            e);
      }
    } else {
      LOGGER.info(
          "[Step 2 Channel Close] : No active channel found for replica {}. Transfer may be between peer attempts.",
          replicaId);
    }

    // Step 3: Wait for perPartitionTransferFuture to complete
    // After setting cancellation flag and closing channel, the transfer should stop.
    LOGGER.info(
        "[Step 3 Transfer Close]: Waiting for partition-level transfer future to complete for replica {} (timeout: {} seconds).",
        replicaId,
        timeoutInSeconds);

    try {
      perPartitionTransferFuture.get(timeoutInSeconds, TimeUnit.SECONDS);
      LOGGER.info(
          "[Step 3 Transfer Close]: Blob transfer completed successfully for replica {} despite cancellation request. "
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
      }
    }
  }

  /**
   * Check if blob transfer cancellation was requested for the given replica.
   * This flag is used to:
   * - Skip starting consumption after successful blob transfer if cancellation was requested
   * - Terminate the peer chain early during blob transfer
   * @param replicaId the replica ID (format: storeName_vVersion-partition)
   * @return true if blob transfer cancellation was requested for this partition, false otherwise
   */
  public boolean isBlobTransferCancelled(String replicaId) {
    AtomicBoolean cancellationFlag = partitionLevelCancellationFlag.get(replicaId);
    boolean isCancelled = cancellationFlag != null && cancellationFlag.get();
    return isCancelled;
  }

  /**
   * Check if there's an ongoing blob transfer for the given replica.
   *
   * @param replicaId the replica ID (format: storeName_vVersion-partition)
   * @return true if blob transfer is in progress for this partition, false otherwise
   */
  public boolean isBlobTransferInProgress(String replicaId) {
    CompletableFuture<InputStream> transferFuture = partitionLevelTransferStatus.get(replicaId);
    boolean inProgress = transferFuture != null && !transferFuture.isDone();
    return inProgress;
  }

  /**
   * Get the cancellation request flag for a replica.
   * Used by the blob transfer manager to check cancellation during peer chain processing.
   *
   * @param replicaId the replica ID (format: storeName_vVersion-partition)
   * @return the cancellation flag, or null if not found
   */
  public AtomicBoolean getCancellationFlag(String replicaId) {
    return partitionLevelCancellationFlag.get(replicaId);
  }

  /**
   * Clear the cancellation request flag for a replica.
   * Should be called after consumption successfully starts or after cancellation completes.
   *
   * @param replicaId the replica ID (format: storeName_vVersion-partition)
   */
  public void clearCancellationRequest(String replicaId) {
    if (partitionLevelCancellationFlag.get(replicaId) != null) {
      partitionLevelCancellationFlag.remove(replicaId);
    }
  }
}
