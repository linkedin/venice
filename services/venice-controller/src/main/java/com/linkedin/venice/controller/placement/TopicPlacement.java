package com.linkedin.venice.controller.placement;

/** Chooses the backend for new batch user-store and push-status version topics. */
public interface TopicPlacement {
  enum Selection {
    /** Use the adapter's default backend. */
    CONTROL,
    /** Use the alternative backend. */
    TREATMENT
  }

  /** When disabled, use the adapter's default backend. */
  default boolean isEnabled(String clusterName) {
    return true;
  }

  /** Evaluate for comparison only; use the adapter's default backend. */
  default boolean isShadowMode(String clusterName) {
    return false;
  }

  /** Called once per creation request, not on each retry. */
  Selection select(TopicPlacementContext context);
}
