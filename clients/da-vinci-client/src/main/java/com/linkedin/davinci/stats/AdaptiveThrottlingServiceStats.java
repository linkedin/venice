package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceAdaptiveThrottlerType;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Rate;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;


public class AdaptiveThrottlingServiceStats extends AbstractVeniceStats {
  private static final String ADAPTIVE_THROTTLING_SERVICE_SUFFIX = "AdaptiveThrottlingService";

  enum TehutiMetricName implements TehutiMetricNameEnum {
    KAFKA_CONSUMPTION_RECORDS_COUNT, KAFKA_CONSUMPTION_BANDWIDTH, CURRENT_VERSION_AA_WC_LEADER_RECORDS_COUNT,
    CURRENT_VERSION_NON_AA_WC_LEADER_RECORDS_COUNT, NON_CURRENT_VERSION_AA_WC_LEADER_RECORDS_COUNT,
    NON_CURRENT_VERSION_NON_AA_WC_LEADER_RECORDS_COUNT;
  }

  /** One joint Tehuti+OTel metric state per throttler type. Record-count throttlers share one OTel metric; the
   *  bandwidth throttler uses a separate OTel metric with BYTES unit. */
  private final Map<VeniceAdaptiveThrottlerType, MetricEntityStateBase> rateMetrics;

  public AdaptiveThrottlingServiceStats(MetricsRepository metricsRepository, String clusterName) {
    super(metricsRepository, ADAPTIVE_THROTTLING_SERVICE_SUFFIX);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(clusterName).build();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();
    VeniceOpenTelemetryMetricsRepository otelRepository = otelData.getOtelRepository();

    EnumMap<VeniceAdaptiveThrottlerType, MetricEntityStateBase> metricMap =
        new EnumMap<>(VeniceAdaptiveThrottlerType.class);
    for (VeniceAdaptiveThrottlerType type: VeniceAdaptiveThrottlerType.values()) {
      MetricEntity metricEntity = getMetricEntityForType(type);
      Map<VeniceMetricsDimensions, String> dimensionsMap = new HashMap<>(baseDimensionsMap);
      dimensionsMap.put(VeniceMetricsDimensions.VENICE_ADAPTIVE_THROTTLER_TYPE, type.getDimensionValue());
      dimensionsMap = Collections.unmodifiableMap(dimensionsMap);
      Attributes attributes =
          otelRepository != null ? otelRepository.createAttributes(metricEntity, dimensionsMap) : null;

      metricMap.put(
          type,
          MetricEntityStateBase.create(
              metricEntity,
              otelRepository,
              this::registerSensorIfAbsent,
              getTehutiName(type),
              Collections.singletonList(new Rate()),
              dimensionsMap,
              attributes));
    }
    this.rateMetrics = Collections.unmodifiableMap(metricMap);
  }

  private static MetricEntity getMetricEntityForType(VeniceAdaptiveThrottlerType type) {
    switch (type.getMetricUnit()) {
      case BYTES:
        return AdaptiveThrottlingOtelMetricEntity.BYTE_COUNT.getMetricEntity();
      case NUMBER:
        return AdaptiveThrottlingOtelMetricEntity.RECORD_COUNT.getMetricEntity();
      default:
        throw new IllegalArgumentException("Unsupported metric unit: " + type.getMetricUnit() + " for " + type);
    }
  }

  /** Maps each throttler type to its Tehuti metric name. OTel uses pubsub-prefixed dimension values
   *  (from the enum constant name); Tehuti retains kafka-prefixed names for backward compatibility. */
  private static TehutiMetricName getTehutiName(VeniceAdaptiveThrottlerType type) {
    switch (type) {
      case PUBSUB_CONSUMPTION_RECORDS_COUNT:
        return TehutiMetricName.KAFKA_CONSUMPTION_RECORDS_COUNT;
      case PUBSUB_CONSUMPTION_BANDWIDTH:
        return TehutiMetricName.KAFKA_CONSUMPTION_BANDWIDTH;
      case CURRENT_VERSION_AA_WC_LEADER_RECORDS_COUNT:
        return TehutiMetricName.CURRENT_VERSION_AA_WC_LEADER_RECORDS_COUNT;
      case CURRENT_VERSION_NON_AA_WC_LEADER_RECORDS_COUNT:
        return TehutiMetricName.CURRENT_VERSION_NON_AA_WC_LEADER_RECORDS_COUNT;
      case NON_CURRENT_VERSION_AA_WC_LEADER_RECORDS_COUNT:
        return TehutiMetricName.NON_CURRENT_VERSION_AA_WC_LEADER_RECORDS_COUNT;
      case NON_CURRENT_VERSION_NON_AA_WC_LEADER_RECORDS_COUNT:
        return TehutiMetricName.NON_CURRENT_VERSION_NON_AA_WC_LEADER_RECORDS_COUNT;
      default:
        throw new IllegalArgumentException("No Tehuti metric name for: " + type);
    }
  }

  public void recordRateForAdaptiveThrottler(VeniceAdaptiveThrottlerType throttlerType, long rate) {
    rateMetrics.get(throttlerType).record(rate);
  }
}
