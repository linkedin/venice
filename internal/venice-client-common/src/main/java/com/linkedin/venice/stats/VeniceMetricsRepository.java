package com.linkedin.venice.stats;

import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.opentelemetry.sdk.metrics.export.MetricReader;
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

  /**
   * Creates a child repository that shares the same OpenTelemetry SDK instance
   * but uses a different metric prefix. This is useful for emitting metrics with a
   * different prefix (e.g., "participant_store_client") without reinitializing OpenTelemetry.
   *
   * @param newMetricPrefix The metric prefix to use for the child repository
   * @return A new VeniceMetricsRepository instance with the specified prefix
   */
  public VeniceMetricsRepository cloneWithNewMetricPrefix(String newMetricPrefix) {
    return new VeniceMetricsRepository(
        veniceMetricsConfig,
        openTelemetryMetricsRepository != null
            ? openTelemetryMetricsRepository.cloneWithNewMetricPrefix(newMetricPrefix)
            : null);
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
    return getVeniceMetricsRepository(serviceName, metricPrefix, metricEntities, configs, false);
  }

  public static VeniceMetricsRepository getVeniceMetricsRepository(
      String serviceName,
      String metricPrefix,
      Collection<MetricEntity> metricEntities,
      Map<String, String> configs,
      boolean useSingleThreadedMetricsRepository) {
    VeniceMetricsConfig.Builder configBuilder = new VeniceMetricsConfig.Builder().setServiceName(serviceName)
        .setMetricPrefix(metricPrefix)
        .setMetricEntities(metricEntities)
        .extractAndSetOtelConfigs(configs);
    if (useSingleThreadedMetricsRepository) {
      configBuilder.setTehutiMetricConfig(MetricsRepositoryUtils.createDefaultSingleThreadedMetricConfig());
    }
    VeniceMetricsRepository metricsRepository = new VeniceMetricsRepository(configBuilder.build());
    metricsRepository.addReporter(new JmxReporter(serviceName));
    return metricsRepository;
  }

  public static VeniceMetricsRepository getVeniceMetricsRepository(
      ClientType clientType,
      Collection<MetricEntity> metricEntities,
      boolean emitOtelMetrics) {
    VeniceMetricsRepository metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setServiceName(clientType.getName())
            .setMetricPrefix(clientType.getMetricsPrefix())
            .setMetricEntities(metricEntities)
            .setEmitOtelMetrics(emitOtelMetrics)
            .build());
    metricsRepository.addReporter(new JmxReporter(clientType.getName()));
    return metricsRepository;
  }

  public static VeniceMetricsRepository getVeniceMetricsRepository(
      ClientType clientType,
      Collection<MetricEntity> metricEntities,
      boolean emitOtelMetrics,
      MetricReader additionalMetricReader) {
    return new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setServiceName(clientType.getName())
            .setMetricPrefix(clientType.getMetricsPrefix())
            .setMetricEntities(metricEntities)
            .setEmitOtelMetrics(emitOtelMetrics)
            .setOtelAdditionalMetricsReader(additionalMetricReader)
            .build());
  }
}
