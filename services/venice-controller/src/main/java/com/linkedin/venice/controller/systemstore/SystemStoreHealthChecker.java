package com.linkedin.venice.controller.systemstore;

import java.util.Map;
import java.util.Set;


/**
 * Pluggable interface for checking the health of Venice system stores.
 *
 * The default implementation ({@link HeartbeatBasedSystemStoreHealthChecker}) uses the existing heartbeat write+read
 * cycle. Alternative implementations (e.g., metrics-based) can be plugged in via the
 * {@code controller.parent.system.store.health.check.override.class.name} config.
 *
 * <p>Custom implementations must provide a public constructor with the following signature:
 * <pre>{@code
 * public MyHealthChecker(VeniceControllerMultiClusterConfig config)
 * }</pre>
 * so they can be instantiated reflectively by the controller.
 *
 */
public interface SystemStoreHealthChecker extends AutoCloseable {
  /**
   * Result of a system store health check.
   */
  enum HealthCheckResult {
    /** Store is healthy and does not need repair. */
    HEALTHY,
    /** Store is unhealthy and should be repaired. */
    UNHEALTHY
  }

  /**
   * Check the health of the given system stores in the specified cluster.
   *
   * @param clusterName the Venice cluster name
   * @param systemStoreNames the set of system store names to check
   * @return a map from system store name to its health check result. Implementations should return an entry for
   *         every store they were able to check. Missing entries (e.g., when the checker aborts early due to
   *         leadership change or shutdown) are treated by the caller as "deferred to next round" — they are
   *         neither marked HEALTHY nor UNHEALTHY for this round, so a partial result will not inflate unhealthy
   *         counts. Implementations should therefore omit a store from the result map only when no decision was
   *         reached for it; an explicit UNHEALTHY entry should be returned for stores that were checked and found
   *         to be unhealthy.
   *
   *         <p>This method is invoked on the repair service's single-threaded scheduler, so a call that blocks
   *         indefinitely will stall every subsequent repair round. Implementations must bound their own execution
   *         time and honor thread interruption (the service calls {@code shutdownNow()} on shutdown) rather than
   *         relying on the caller to time them out.
   */
  Map<String, HealthCheckResult> checkHealth(String clusterName, Set<String> systemStoreNames);

  @Override
  default void close() {
  }
}
