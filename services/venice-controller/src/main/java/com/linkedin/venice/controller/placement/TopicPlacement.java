package com.linkedin.venice.controller.placement;

/** Chooses the backend for new batch user-store and push-status version topics. */
public interface TopicPlacement {
  enum Selection {
    /** Use the adapter's default backend. */
    CONTROL,
    /** Use the alternative backend. */
    TREATMENT
  }

  /** When disabled, use static placement configuration. */
  default boolean isEnabled(String clusterName) {
    return true;
  }

  /** Evaluate for comparison only; keep static placement. */
  default boolean isShadowMode(String clusterName) {
    return false;
  }

  /** Called once per creation request, not on each retry. The implementation decides category eligibility. */
  Selection select(TopicPlacementContext context);
}
