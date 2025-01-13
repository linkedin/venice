package com.linkedin.venice.stats;

import com.linkedin.venice.stats.metrics.MetricEntity;
import io.tehuti.metrics.JmxReporter;
import io.tehuti.metrics.MetricsRepository;
import java.io.Closeable;
import java.util.Collection;
import java.util.Map;


/**
 * Repository to hold both tehuti and OpenTelemetry metrics.
 * This class extends {@link MetricsRepository} to keep the changes to a minimum and
 * to avoid a breaking change.<br>
 * Once all components are migrated to use this class: make this class add {@link MetricsRepository}
 * as a member variable and delegate all tehuti calls to it.
 */
public class VeniceMetricsRepository extends MetricsRepository implements Closeable {
  private final VeniceMetricsConfig veniceMetricsConfig;
  private final VeniceOpenTelemetryMetricsRepository openTelemetryMetricsRepository;

  public VeniceMetricsRepository() {
    super();
    this.veniceMetricsConfig = new VeniceMetricsConfig.Builder().build();
    this.openTelemetryMetricsRepository =
        (veniceMetricsConfig.emitOtelMetrics() ? new VeniceOpenTelemetryMetricsRepository(veniceMetricsConfig) : null);
  }

  public VeniceMetricsRepository(VeniceMetricsConfig veniceMetricsConfig) {
    this(
        veniceMetricsConfig,
        veniceMetricsConfig.emitOtelMetrics() ? new VeniceOpenTelemetryMetricsRepository(veniceMetricsConfig) : null);
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
