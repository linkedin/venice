package com.linkedin.venice.controller.placement;

/**
 * Optional controller-boundary admission policy for batch user version topics. Implementations are borrowed:
 * the controller does not own their clients. Never evaluate this policy from a pub/sub retry or data path.
 */
public interface TopicPlacementPolicy {
  enum Selection {
    ABSTAIN, ADAPTER_DEFAULT, REQUEST_ALTERNATIVE,
    /** Ensure the existing topic is online without issuing another create request. */
    PRESERVE_EXISTING
  }

  default boolean isEnabled(String clusterName) {
    return true;
  }

  /** Shadow policies must not add a broker read (or a new broker-read failure) to legacy creation. */
  default boolean requiresTopicExistenceCheck(String clusterName) {
    return false;
  }

  Selection select(TopicPlacementContext context);
}
