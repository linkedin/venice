package com.linkedin.venice.stats;

import io.tehuti.metrics.MetricsRepository;
import java.io.Closeable;


/** extends MetricsRepository to keep the changes to a minimum. Next step would be to create a MetricsRepository inside rather than extending it */
public class VeniceMetricsRepository extends MetricsRepository implements Closeable {
  private VeniceMetricsConfig veniceMetricsConfig;
  VeniceOpenTelemetryMetricsRepository openTelemetryMetricsRepository;

  public VeniceMetricsRepository() {
    super();
    this.veniceMetricsConfig = new VeniceMetricsConfig.Builder().build();
    this.openTelemetryMetricsRepository = new VeniceOpenTelemetryMetricsRepository(veniceMetricsConfig);
  }

  public VeniceMetricsRepository(VeniceMetricsConfig veniceMetricsConfig) {
    this(veniceMetricsConfig, new VeniceOpenTelemetryMetricsRepository(veniceMetricsConfig));
  }

  public VeniceMetricsRepository(
      VeniceMetricsConfig veniceMetricsConfig,
      VeniceOpenTelemetryMetricsRepository openTelemetryMetricsRepository) {
    super(veniceMetricsConfig.getTehutiMetricConfig());
    this.veniceMetricsConfig = veniceMetricsConfig;
    this.openTelemetryMetricsRepository = openTelemetryMetricsRepository;
  }

  public VeniceOpenTelemetryMetricsRepository getOpenTelemetryMetricsRepository() {
    return this.openTelemetryMetricsRepository;
  }

  public VeniceMetricsConfig getVeniceMetricsConfig() {
    return veniceMetricsConfig;
  }

  @Override
  public void close() {
    super.close();
    if (openTelemetryMetricsRepository != null) {
      openTelemetryMetricsRepository.close();
    }
  }
}
