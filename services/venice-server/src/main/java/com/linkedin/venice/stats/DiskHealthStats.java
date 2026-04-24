package com.linkedin.venice.stats;

import static com.linkedin.davinci.stats.DiskHealthOtelMetricEntity.DISK_HEALTH_STATUS;

import com.linkedin.davinci.storage.DiskHealthCheckService;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.Collections;
import java.util.Map;
import java.util.function.LongSupplier;


/**
 * {@code DiskHealthStats} measures the disk health conditions based on the periodic tests ran by
 * the {@link DiskHealthCheckService}. Reports 1 if healthy, 0 if unhealthy.
 *
 * <p>Uses the joint Tehuti+OTel API: a single {@link AsyncMetricEntityStateBase} registration
 * binds both the Tehuti {@link AsyncGauge} and the OTel ASYNC_GAUGE to the same
 * {@link DiskHealthCheckService#isDiskHealthy()} callback.
 */
public class DiskHealthStats extends AbstractVeniceStats {
  /** Tehuti metric name for the disk health sensor. */
  enum TehutiMetricName implements TehutiMetricNameEnum {
    DISK_HEALTHY
  }

  public DiskHealthStats(
      MetricsRepository metricsRepository,
      DiskHealthCheckService diskHealthCheckService,
      String name,
      String clusterName) {
    super(metricsRepository, name);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(clusterName).build();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();
    Attributes baseAttributes = otelData.getBaseAttributes();

    LongSupplier healthCallback = () -> diskHealthCheckService.isDiskHealthy() ? 1 : 0;
    AsyncMetricEntityStateBase.create(
        DISK_HEALTH_STATUS.getMetricEntity(),
        otelData.getOtelRepository(),
        this::registerSensorIfAbsent,
        TehutiMetricName.DISK_HEALTHY,
        Collections.singletonList(
            new AsyncGauge((ig, ig2) -> healthCallback.getAsLong(), TehutiMetricName.DISK_HEALTHY.getMetricName())),
        baseDimensionsMap,
        baseAttributes,
        healthCallback);
  }
}
