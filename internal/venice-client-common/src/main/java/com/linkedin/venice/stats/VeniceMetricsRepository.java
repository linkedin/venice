package com.linkedin.venice.stats;

import com.linkedin.venice.stats.metrics.MetricEntity;
import io.tehuti.metrics.JmxReporter;
import io.tehuti.metrics.MetricsRepository;
import java.io.Closeable;
import java.util.Collection;
import java.util.Map;


/**
 * extends {@link MetricsRepository} to keep the changes to a minimum.
 * Next step would be to create a MetricsRepository inside rather than extending it
 */
public class VeniceMetricsRepository extends MetricsRepository implements Closeable {
  private final VeniceMetricsConfig veniceMetricsConfig;
  private final VeniceOpenTelemetryMetricsRepository openTelemetryMetricsRepository;

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

  public static VeniceMetricsRepository getVeniceMetricsRepository(
      String serviceName,
      String metricPrefix,
      Collection<MetricEntity> metricEntities,
      Map<String, String> configs) {
    VeniceMetricsRepository metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setServiceName(serviceName)
            .setMetricPrefix(metricPrefix)
            .setMetricEntities(metricEntities)
            .extractAndSetOtelConfigs(configs)
            .build());
    metricsRepository.addReporter(new JmxReporter(serviceName));
    return metricsRepository;
  }
}
