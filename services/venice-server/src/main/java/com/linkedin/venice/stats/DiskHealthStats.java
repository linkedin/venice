package com.linkedin.venice.stats;

import static com.linkedin.davinci.stats.DiskHealthOtelMetricEntity.DISK_HEALTH_STATUS;

import com.linkedin.davinci.storage.DiskHealthCheckService;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.Map;
import java.util.function.LongSupplier;


/**
 * {@code DiskHealthStats} measures the disk health conditions based on the periodic tests ran by
 * the {@link DiskHealthCheckService}. Reports 1 if healthy, 0 if unhealthy.
 *
 * <p>Tehuti and OTel both poll the same {@link DiskHealthCheckService#isDiskHealthy()} method.
 * They cannot share a single registration because Tehuti uses {@link AsyncGauge} (polled by
 * Tehuti's async executor) while OTel uses {@link AsyncMetricEntityStateBase} (polled by the
 * OTel SDK's PeriodicMetricReader).
 */
public class DiskHealthStats extends AbstractVeniceStats {
  public DiskHealthStats(
      MetricsRepository metricsRepository,
      DiskHealthCheckService diskHealthCheckService,
      String name,
      String clusterName) {
    super(metricsRepository, name);

    // Shared callback: 1 = healthy, 0 = unhealthy
    LongSupplier healthCallback = () -> diskHealthCheckService.isDiskHealthy() ? 1 : 0;

    // Tehuti: AsyncGauge
    registerSensor(new AsyncGauge((ignored, ignored2) -> healthCallback.getAsLong(), "disk_healthy"));

    // OTel: ASYNC_GAUGE with CLUSTER_NAME
    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(clusterName).build();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();
    Attributes baseAttributes = otelData.getBaseAttributes();
    AsyncMetricEntityStateBase.create(
        DISK_HEALTH_STATUS.getMetricEntity(),
        otelData.getOtelRepository(),
        baseDimensionsMap,
        baseAttributes,
        healthCallback);
  }
}
