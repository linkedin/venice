package com.linkedin.davinci.stats;

import com.linkedin.davinci.kafka.consumer.PubSubHealthMonitor;
import com.linkedin.venice.pubsub.PubSubHealthCategory;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import io.tehuti.metrics.MetricsRepository;
import java.util.Map;


/**
 * OTel metrics for the {@link PubSubHealthMonitor}. Reports the number of currently unhealthy
 * targets as async gauges, and probe/transition counts as counters, all dimensioned by
 * {@link PubSubHealthCategory}.
 */
public class PubSubHealthMonitorStats {
  private final MetricEntityStateOneEnum<PubSubHealthCategory> probeSuccessCount;
  private final MetricEntityStateOneEnum<PubSubHealthCategory> probeFailureCount;
  private final MetricEntityStateOneEnum<PubSubHealthCategory> stateTransitionCount;

  public PubSubHealthMonitorStats(
      MetricsRepository metricsRepository,
      PubSubHealthMonitor healthMonitor,
      String clusterName) {
    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelSetup =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(clusterName).build();

    VeniceOpenTelemetryMetricsRepository otelRepository = otelSetup.getOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelSetup.getBaseDimensionsMap();

    // Async gauge: count of unhealthy targets per category
    AsyncMetricEntityStateOneEnum.create(
        PubSubHealthOtelMetricEntity.PUBSUB_HEALTH_UNHEALTHY_COUNT.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        PubSubHealthCategory.class,
        category -> () -> (long) healthMonitor.getUnhealthyCount(category));

    // Counters for probe results and state transitions
    probeSuccessCount = MetricEntityStateOneEnum.create(
        PubSubHealthOtelMetricEntity.PUBSUB_HEALTH_PROBE_SUCCESS_COUNT.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        PubSubHealthCategory.class);

    probeFailureCount = MetricEntityStateOneEnum.create(
        PubSubHealthOtelMetricEntity.PUBSUB_HEALTH_PROBE_FAILURE_COUNT.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        PubSubHealthCategory.class);

    stateTransitionCount = MetricEntityStateOneEnum.create(
        PubSubHealthOtelMetricEntity.PUBSUB_HEALTH_STATE_TRANSITION_COUNT.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        PubSubHealthCategory.class);
  }

  public void recordProbeSuccess(PubSubHealthCategory category) {
    probeSuccessCount.record(1, category);
  }

  public void recordProbeFailure(PubSubHealthCategory category) {
    probeFailureCount.record(1, category);
  }

  public void recordStateTransition(PubSubHealthCategory category) {
    stateTransitionCount.record(1, category);
  }
}
