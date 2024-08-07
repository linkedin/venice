package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.utils.lazy.Lazy;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Rate;


public class ServerMetadataServiceStats extends AbstractVeniceStats {
  /**
   * Measure the number of time request based metadata endpoint was invoked
   */
  private final Lazy<Sensor> requestBasedMetadataInvokeCount;

  /**
   * Measure the number of time request based metadata endpoint failed to respond
   */
  private final Lazy<Sensor> requestBasedMetadataFailureCount;

  public ServerMetadataServiceStats(MetricsRepository metricsRepository) {
    super(metricsRepository, "ServerMetadataStats");

    this.requestBasedMetadataInvokeCount =
        Lazy.of(() -> registerSensorIfAbsent("request_based_metadata_invoke_count", new Rate()));
    this.requestBasedMetadataFailureCount =
        Lazy.of(() -> registerSensorIfAbsent("request_based_metadata_failure_count", new Rate()));
  }

  public void recordRequestBasedMetadataInvokeCount() {
    requestBasedMetadataInvokeCount.get().record();
  }

  public void recordRequestBasedMetadataFailureCount() {
    requestBasedMetadataFailureCount.get().record();
  }
}
