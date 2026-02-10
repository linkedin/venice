package com.linkedin.davinci.blobtransfer;

import com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferStatus;
import com.linkedin.davinci.blobtransfer.client.NettyFileTransferClient;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.netty.channel.Channel;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * BlobTransferStatusTrackingManager is responsible for tracking and managing the status of blob transfers.
 * It provides a centralized way to track ongoing transfers, request cancellations, and coordinate
 * the shutdown of active channels and peer chains.
 *
 * Cancellation workflow:
 *   - Set status to TRANSFER_CANCEL_REQUESTED to prevent new peer attempts in the chain
 *   - Close any active network channel to abort ongoing data transfer
 *   - The entire chain of peer attempts will terminate early due to the cancellation status
 *
 */
public class BlobTransferStatusTrackingManager {
  private static final Logger LOGGER = LogManager.getLogger(BlobTransferStatusTrackingManager.class);

  // Track blob transfer status: <replicaId, transfer status>
  private final Map<String, AtomicReference<BlobTransferStatus>> partitionLevelTransferStatusEnum =
      new VeniceConcurrentHashMap<>();

  private final NettyFileTransferClient nettyClient;

  public BlobTransferStatusTrackingManager(NettyFileTransferClient nettyClient) {
    this.nettyClient = nettyClient;
  }

  public void initialTransfer(String replicaId) {
    partitionLevelTransferStatusEnum.put(replicaId, new AtomicReference<>(BlobTransferStatus.TRANSFER_NOT_STARTED));
  }

  public void startedTransfer(String replicaId) {
    partitionLevelTransferStatusEnum.put(replicaId, new AtomicReference<>(BlobTransferStatus.TRANSFER_STARTED));
  }

  public BlobTransferStatus getTransferStatus(String replicaId) {
    AtomicReference<BlobTransferStatus> statusRef = partitionLevelTransferStatusEnum.get(replicaId);
    return statusRef != null ? statusRef.get() : null;
  }

  public void clearTransferStatusEnum(String replicaId) {
    partitionLevelTransferStatusEnum.remove(replicaId);
  }

  public boolean isBlobTransferInProgress(String replicaId) {
    AtomicReference<BlobTransferStatus> statusRef = partitionLevelTransferStatusEnum.get(replicaId);
    if (statusRef == null) {
      return false;
    }
    BlobTransferStatus status = statusRef.get();
    boolean inProgress = status == BlobTransferStatus.TRANSFER_NOT_STARTED
        || status == BlobTransferStatus.TRANSFER_CANCEL_REQUESTED || status == BlobTransferStatus.TRANSFER_STARTED;
    return inProgress;
  }

  public boolean isBlobTransferCancelRequested(String replicaId) {
    AtomicReference<BlobTransferStatus> statusRef = partitionLevelTransferStatusEnum.get(replicaId);
    if (statusRef == null) {
      return false;
    }
    BlobTransferStatus status = statusRef.get();
    return status == BlobTransferStatus.TRANSFER_CANCEL_REQUESTED;
  }

  public boolean isBlobTransferCancelRequestSentBefore(String replicaId) {
    AtomicReference<BlobTransferStatus> statusRef = partitionLevelTransferStatusEnum.get(replicaId);
    if (statusRef == null) {
      return false;
    }
    BlobTransferStatus status = statusRef.get();
    return status == BlobTransferStatus.TRANSFER_CANCELLED || status == BlobTransferStatus.TRANSFER_CANCEL_REQUESTED;
  }

  public void markTransferCancelled(String replicaId) {
    AtomicReference<BlobTransferStatus> statusRef = partitionLevelTransferStatusEnum.get(replicaId);
    if (statusRef != null) {
      BlobTransferStatus currentStatus = statusRef.get();
      if (currentStatus == BlobTransferStatus.TRANSFER_CANCEL_REQUESTED) {
        statusRef.set(BlobTransferStatus.TRANSFER_CANCELLED);
        LOGGER.info("Marked transfer status as TRANSFER_CANCELLED for replica {} (was {})", replicaId, currentStatus);
      } else {
        LOGGER.info(
            "Not marking as TRANSFER_CANCELLED for replica {} because current status is {}",
            replicaId,
            currentStatus);
      }
    }
  }

  public void markTransferCompleted(String replicaId) {
    AtomicReference<BlobTransferStatus> statusRef = partitionLevelTransferStatusEnum.get(replicaId);
    if (statusRef != null) {
      BlobTransferStatus currentStatus = statusRef.get();
      if (currentStatus == BlobTransferStatus.TRANSFER_STARTED
          || currentStatus == BlobTransferStatus.TRANSFER_NOT_STARTED) {
        statusRef.set(BlobTransferStatus.TRANSFER_COMPLETED);
        LOGGER.info("Marked transfer status as TRANSFER_COMPLETED for replica {} (was {})", replicaId, currentStatus);
      } else {
        LOGGER.info(
            "Not marking as TRANSFER_COMPLETED for replica {} because current status is {}",
            replicaId,
            currentStatus);
      }
    }
  }

  public boolean isTransferInFinalState(String replicaId) {
    BlobTransferStatus status = getTransferStatus(replicaId);
    return status == null || status == BlobTransferStatus.TRANSFER_COMPLETED
        || status == BlobTransferStatus.TRANSFER_CANCELLED;
  }

  public void cancelTransfer(String replicaId) {
    if (isBlobTransferCancelRequestSentBefore(replicaId)) {
      LOGGER.warn("Cancellation flag already set for replica {}. No action needed.", replicaId);
      return;
    }

    if (isTransferInFinalState(replicaId)) {
      LOGGER
          .info("Skipping send cancelling request for replica {}, due to transfer already in final state.", replicaId);
      return;
    }

    LOGGER.info("Cancellation request received for replica {}. Starting cancellation.", replicaId);
    // Step 1: Set status to TRANSFER_CANCEL_REQUESTED to stop peer chain and signal bootstrap callback
    AtomicReference<BlobTransferStatus> statusRef = partitionLevelTransferStatusEnum
        .computeIfAbsent(replicaId, k -> new AtomicReference<>(BlobTransferStatus.TRANSFER_NOT_STARTED));

    BlobTransferStatus currentStatus = statusRef.get();
    LOGGER.info(
        "Current status for replica {} is {}, transitioning to TRANSFER_CANCEL_REQUESTED",
        replicaId,
        currentStatus);

    statusRef.set(BlobTransferStatus.TRANSFER_CANCEL_REQUESTED);
    LOGGER.info("Set transfer status to TRANSFER_CANCEL_REQUESTED for replica {}", replicaId);

    // Step 2: Close active channel to abort ongoing data transfer
    Channel currentChannel = nettyClient.getActiveChannel(replicaId);
    if (currentChannel != null && currentChannel.isActive()) {
      LOGGER.info("Closing active channel for replica {} to abort data transfer", replicaId);
      try {
        currentChannel.close().syncUninterruptibly();
        LOGGER.info("Successfully closed active channel for replica {}", replicaId);
      } catch (Exception e) {
        LOGGER.warn("Exception while closing channel for replica {}. Continuing with cancellation.", replicaId, e);
      }
    } else {
      LOGGER.info(
          "No active channel found for replica {}. Transfer may not be in progress or already completed.",
          replicaId);
    }
  }
}
