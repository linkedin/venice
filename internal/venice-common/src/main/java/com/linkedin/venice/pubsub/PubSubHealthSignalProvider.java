package com.linkedin.venice.pubsub;

/**
 * A pluggable provider of health signals for PubSub targets. The {@link PubSubHealthMonitor}
 * aggregates signals from all registered providers to determine overall broker health.
 *
 * <p>The first implementation is exception-based (any PubSub exception marks the target unhealthy).
 * Future implementations can detect non-exception failures such as growing heartbeat lag or
 * elevated latency.
 */
public interface PubSubHealthSignalProvider {
  /**
   * @return a descriptive name for this provider, used in logging and metrics (e.g., "exception", "heartbeat-lag")
   */
  String getName();

  /**
   * @return true if this provider currently considers the given target unhealthy
   */
  boolean isUnhealthy(String pubSubAddress, PubSubHealthCategory category);

  /**
   * Called by the health monitor when a recovery probe succeeds for the given target.
   * Implementations should clear any failure state for this target.
   */
  default void onProbeSuccess(String pubSubAddress, PubSubHealthCategory category) {
  }

  /**
   * Called by the health monitor when a recovery probe fails for the given target.
   */
  default void onProbeFailure(String pubSubAddress, PubSubHealthCategory category) {
  }
}
