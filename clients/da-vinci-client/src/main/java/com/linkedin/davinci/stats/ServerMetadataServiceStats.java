package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Rate;


public class ServerMetadataServiceStats extends AbstractVeniceStats {
  /**
   * Measure the number of time request based metadata endpoint was invoked
   */
  private final Sensor requestBasedMetadataInvokeCount;

  /**
   * Measure the number of time request based metadata endpoint failed to respond
   */
  private final Sensor requestBasedMetadataFailureCount;

  public ServerMetadataServiceStats(MetricsRepository metricsRepository) {
    super(metricsRepository, "ServerMetadataStats");

    this.requestBasedMetadataInvokeCount = registerSensorIfAbsent("request_based_metadata_invoke_count", new Rate());
    this.requestBasedMetadataFailureCount = registerSensorIfAbsent("request_based_metadata_failure_count", new Rate());
  }

  public void recordRequestBasedMetadataInvokeCount() {
    requestBasedMetadataInvokeCount.record();
  }

  public void recordRequestBasedMetadataFailureCount() {
    requestBasedMetadataFailureCount.record();
  }
}
