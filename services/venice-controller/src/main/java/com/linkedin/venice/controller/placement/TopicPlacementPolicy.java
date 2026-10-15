package com.linkedin.venice.controller.placement;

/** Chooses the backend for new batch user-store version topics. */
public interface TopicPlacementPolicy {
  enum Selection {
    /** Use the adapter's default backend. */
    CONTROL,
    /** Use the alternative backend if the cluster configuration allows it. */
    TREATMENT
  }

  /** Disabled policies leave the existing placement logic unchanged. */
  default boolean isEnabled(String clusterName) {
    return true;
  }

  /** Evaluate for comparison only; do not change placement or add topic-existence checks. */
  default boolean isShadowMode(String clusterName) {
    return false;
  }

  /** Called once per creation request, not on each retry. */
  Selection select(TopicPlacementContext context);
}
