package com.linkedin.davinci.blobtransfer.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Host-level admission control for <b>client-origin</b> (Stateful CDC / external consumer) blob transfer requests.
 *
 * <p>This controller governs only the new client-request feature: it bounds concurrent client-origin transfers to
 * their own reserved cap, which is a percentage of the host blob-transfer budget. Server-to-server (and DVC-to-DVC)
 * transfers are <b>not</b> tracked here -- they keep using the pre-existing global concurrency counter in
 * {@code P2PFileTransferServerHandler}. The two budgets are intentionally independent so client traffic can never
 * perturb the tuned server-to-server path.
 *
 * <p>The client cap is decided atomically under a single lock. Admission frequency is low (a handful of concurrent
 * cold-start transfers, each lasting minutes), so {@code synchronized} carries no meaningful cost.
 */
public class BlobTransferAdmissionController {
  private static final Logger LOGGER = LogManager.getLogger(BlobTransferAdmissionController.class);

  /**
   * Upper bound on the configurable client capacity percentage. Client-origin transfers share the host's disk and
   * network bandwidth with server-to-server transfers.
   */
  static final int MAX_CLIENT_CAPACITY_PERCENT = 50;

  // Hard cap on concurrent client-origin transfers. Derived from the client capacity percentage of the host budget.
  private final int maxClientTransfers;

  private int clientInFlight = 0;

  /**
   * @param maxConcurrentTransfers the host-wide concurrency budget {@code N} (must be > 0) that the client cap is
   *        expressed as a percentage of.
   * @param clientCapacityPercent the percentage [0, {@value #MAX_CLIENT_CAPACITY_PERCENT}] of {@code N} reserved as the
   *        client-origin cap. {@code 0} disables client-origin transfers entirely; any positive percentage yields a cap
   *        of at least 1 so a small budget does not silently round the client reservation down to zero. Capped at
   *        {@value #MAX_CLIENT_CAPACITY_PERCENT}% so client transfers cannot dominate shared host I/O.
   */
  public BlobTransferAdmissionController(int maxConcurrentTransfers, int clientCapacityPercent) {
    if (maxConcurrentTransfers <= 0) {
      throw new IllegalArgumentException("maxConcurrentTransfers must be > 0, got " + maxConcurrentTransfers);
    }
    if (clientCapacityPercent < 0 || clientCapacityPercent > MAX_CLIENT_CAPACITY_PERCENT) {
      throw new IllegalArgumentException(
          "clientCapacityPercent must be in [0, " + MAX_CLIENT_CAPACITY_PERCENT + "], got " + clientCapacityPercent);
    }
    this.maxClientTransfers = clientCapacityPercent == 0
        ? 0
        : Math.max(1, (int) Math.floor(maxConcurrentTransfers * clientCapacityPercent / 100.0));
  }

  /**
   * Attempt to admit a client-origin transfer, incrementing the in-flight count on success. The caller must invoke
   * {@link #releaseClient()} exactly once for every {@code true} result, when the transfer finishes.
   *
   * @return {@code true} if admitted, {@code false} if the client-origin reservation is full.
   */
  public synchronized boolean tryAdmitClient() {
    if (clientInFlight >= maxClientTransfers) {
      return false;
    }
    clientInFlight++;
    return true;
  }

  /**
   * Release a previously admitted client-origin slot. Clamped at 0 to defend against a double release.
   */
  public synchronized void releaseClient() {
    if (clientInFlight <= 0) {
      LOGGER.warn("Attempted to release a client-origin blob transfer slot while none were in flight.");
      clientInFlight = 0;
    } else {
      clientInFlight--;
    }
  }

  public synchronized int getClientInFlight() {
    return clientInFlight;
  }

  public int getMaxClientTransfers() {
    return maxClientTransfers;
  }
}
