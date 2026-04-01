package com.linkedin.venice.controller.systemstore;

import java.io.Closeable;
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
public interface SystemStoreHealthChecker extends Closeable {
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
   *         every store in {@code systemStoreNames}; missing entries are treated as UNHEALTHY by the caller.
   */
  Map<String, HealthCheckResult> checkHealth(String clusterName, Set<String> systemStoreNames);

  @Override
  default void close() {
  }
}
