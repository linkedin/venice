package com.linkedin.venice.controller.systemstore;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;


/**
 * Pluggable interface for checking the health of Venice system stores.
 *
 * The default implementation ({@link HeartbeatBasedSystemStoreHealthChecker}) uses the existing heartbeat write+read
 * cycle. Alternative implementations (e.g., metrics-based) can be plugged in via config to provide faster health
 * checks, with the heartbeat checker as a fallback for stores returning {@link HealthCheckResult#UNKNOWN}.
 */
public interface SystemStoreHealthChecker extends Closeable {
  /**
   * Result of a system store health check.
   */
  enum HealthCheckResult {
    /** Store is healthy and does not need repair. */
    HEALTHY,
    /** Store is unhealthy and should be repaired. */
    UNHEALTHY,
    /** Health could not be determined; caller should fall back to the default heartbeat checker. */
    UNKNOWN
  }

  /**
   * Check the health of the given system stores in the specified cluster.
   *
   * @param clusterName the Venice cluster name
   * @param systemStoreNames the set of system store names to check
   * @return a map from system store name to its health check result
   */
  Map<String, HealthCheckResult> checkHealth(String clusterName, Set<String> systemStoreNames);

  @Override
  default void close() {
  }
}
