package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.StuckConsumerRepairOtelMetricEntity.STUCK_CONSUMER_DETECTED_COUNT;
import static com.linkedin.davinci.stats.StuckConsumerRepairOtelMetricEntity.STUCK_CONSUMER_TASK_REPAIRED_COUNT;
import static com.linkedin.davinci.stats.StuckConsumerRepairOtelMetricEntity.STUCK_CONSUMER_UNRESOLVED_COUNT;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.Collections;
import java.util.Map;


public class StuckConsumerRepairStats extends AbstractVeniceStats {
  /** Tehuti metric names for StuckConsumerRepairStats sensors. */
  enum TehutiMetricName implements TehutiMetricNameEnum {
    STUCK_CONSUMER_FOUND, INGESTION_TASK_REPAIR, REPAIR_FAILURE
  }

  private final MetricEntityStateBase stuckConsumerFoundOtel;
  private final MetricEntityStateBase ingestionTaskRepairOtel;
  private final MetricEntityStateBase repairFailureOtel;

  public StuckConsumerRepairStats(MetricsRepository metricsRepository, String clusterName) {
    super(metricsRepository, "StuckConsumerRepair");

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(clusterName).build();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();
    Attributes baseAttributes = otelData.getBaseAttributes();

    stuckConsumerFoundOtel = MetricEntityStateBase.create(
        STUCK_CONSUMER_DETECTED_COUNT.getMetricEntity(),
        otelData.getOtelRepository(),
        this::registerSensorIfAbsent,
        TehutiMetricName.STUCK_CONSUMER_FOUND,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        baseAttributes);

    ingestionTaskRepairOtel = MetricEntityStateBase.create(
        STUCK_CONSUMER_TASK_REPAIRED_COUNT.getMetricEntity(),
        otelData.getOtelRepository(),
        this::registerSensorIfAbsent,
        TehutiMetricName.INGESTION_TASK_REPAIR,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        baseAttributes);

    repairFailureOtel = MetricEntityStateBase.create(
        STUCK_CONSUMER_UNRESOLVED_COUNT.getMetricEntity(),
        otelData.getOtelRepository(),
        this::registerSensorIfAbsent,
        TehutiMetricName.REPAIR_FAILURE,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        baseAttributes);
  }

  public void recordStuckConsumerFound() {
    stuckConsumerFoundOtel.record(1);
  }

  public void recordIngestionTaskRepair() {
    ingestionTaskRepairOtel.record(1);
  }

  public void recordRepairFailure() {
    repairFailureOtel.record(1);
  }
}
