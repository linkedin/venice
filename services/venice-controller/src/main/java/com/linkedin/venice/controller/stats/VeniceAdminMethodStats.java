package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntityStateTwoEnums;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;


/**
 * This class contains metrics for {@link com.linkedin.venice.controller.VeniceParentHelixAdmin} methods.
 */
public class VeniceAdminMethodStats extends AbstractVeniceStats {
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;
  private final MetricEntityStateTwoEnums<VeniceAdminMethod, VeniceAdminMethodStep> parentAdminMethodLatencyMetrics;

  public VeniceAdminMethodStats(MetricsRepository metricsRepository, String clusterName) {
    super(metricsRepository, clusterName);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository)
            // set all base dimensions for this stats class and build
            .setClusterName(clusterName)
            .build();

    this.otelRepository = otelData.getOtelRepository();
    this.baseDimensionsMap = otelData.getBaseDimensionsMap();

    /**
     * The metrics will track two dimensions: The method in the {@link com.linkedin.venice.controller.VeniceHelixAdmin}
     * and the step in the method. This helps find the bottleneck in a method for latency.
     */
    parentAdminMethodLatencyMetrics = MetricEntityStateTwoEnums.create(
        AdminBaseMetricEntity.PARENT_ADMIN_CALL_TIME.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        VeniceAdminMethod.class,
        VeniceAdminMethodStep.class);

  }

  public void recordParentAdminMethodStepLatency(VeniceAdminMethod method, VeniceAdminMethodStep step, long latency) {
    parentAdminMethodLatencyMetrics.record(latency, method, step);
  }

}
