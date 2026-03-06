package com.linkedin.venice.pubsub;

/**
 * Listener for PubSub health status changes. Implementations are notified asynchronously
 * when a PubSub target transitions between health states.
 */
public interface PubSubHealthChangeListener {
  /**
   * Called when a PubSub target's health status changes. Implementations must be non-blocking.
   *
   * @param pubSubAddress the broker or metadata service address
   * @param category      whether this is a BROKER or METADATA_SERVICE status change
   * @param newStatus     the new health status
   */
  void onHealthStatusChanged(String pubSubAddress, PubSubHealthCategory category, PubSubHealthStatus newStatus);
}
