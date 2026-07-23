package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceAdaptiveThrottlerType;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Rate;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;


public class AdaptiveThrottlingServiceStats extends AbstractVeniceStats {
  private static final String ADAPTIVE_THROTTLING_SERVICE_SUFFIX = "AdaptiveThrottlingService";

  enum TehutiMetricName implements TehutiMetricNameEnum {
    KAFKA_CONSUMPTION_RECORDS_COUNT, KAFKA_CONSUMPTION_BANDWIDTH, CURRENT_VERSION_AA_WC_LEADER_RECORDS_COUNT,
    CURRENT_VERSION_NON_AA_WC_LEADER_RECORDS_COUNT, NON_CURRENT_VERSION_AA_WC_LEADER_RECORDS_COUNT,
    NON_CURRENT_VERSION_NON_AA_WC_LEADER_RECORDS_COUNT, BLOB_TRANSFER_WRITE_BANDWIDTH, BLOB_TRANSFER_READ_BANDWIDTH;
  }

  /**
   * Throttler types whose recorded value is a current bytes/sec limit — a level, not an additive count.
   * These are exposed as async gauges (last value) instead of the Rate/COUNTER path used by the ingestion
   * throttlers, which record observed counts. Routing the limit through Rate/COUNTER would scale it by the
   * signal-refresh interval (e.g. a 1000 B/s limit recorded every 30s would read as ~33).
   */
  static final Set<VeniceAdaptiveThrottlerType> GAUGE_TYPES = Collections.unmodifiableSet(
      EnumSet.of(
          VeniceAdaptiveThrottlerType.BLOB_TRANSFER_WRITE_BANDWIDTH,
          VeniceAdaptiveThrottlerType.BLOB_TRANSFER_READ_BANDWIDTH));

  /** Joint Tehuti+OTel Rate/COUNTER state per non-gauge throttler type, recorded via {@link MetricEntityStateBase#record}. */
  private final Map<VeniceAdaptiveThrottlerType, MetricEntityStateBase> rateMetrics;

  /** Last-pushed current limit per {@link #GAUGE_TYPES} throttler, read by the registered async gauges. */
  private final Map<VeniceAdaptiveThrottlerType, AtomicLong> currentLimitGauges;

  public AdaptiveThrottlingServiceStats(MetricsRepository metricsRepository, String clusterName) {
    super(metricsRepository, ADAPTIVE_THROTTLING_SERVICE_SUFFIX);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(clusterName).build();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();
    VeniceOpenTelemetryMetricsRepository otelRepository = otelData.getOtelRepository();

    EnumMap<VeniceAdaptiveThrottlerType, MetricEntityStateBase> metricMap =
        new EnumMap<>(VeniceAdaptiveThrottlerType.class);
    EnumMap<VeniceAdaptiveThrottlerType, AtomicLong> gaugeMap = new EnumMap<>(VeniceAdaptiveThrottlerType.class);
    for (VeniceAdaptiveThrottlerType type: VeniceAdaptiveThrottlerType.values()) {
      Map<VeniceMetricsDimensions, String> dimensionsMap = new HashMap<>(baseDimensionsMap);
      dimensionsMap.put(VeniceMetricsDimensions.VENICE_ADAPTIVE_THROTTLER_TYPE, type.getDimensionValue());
      dimensionsMap = Collections.unmodifiableMap(dimensionsMap);

      if (GAUGE_TYPES.contains(type)) {
        MetricEntity gaugeEntity = AdaptiveThrottlingOtelMetricEntity.CURRENT_LIMIT.getMetricEntity();
        Attributes attributes =
            otelRepository != null ? otelRepository.createAttributes(gaugeEntity, dimensionsMap) : null;
        AtomicLong currentLimit = new AtomicLong(0);
        gaugeMap.put(type, currentLimit);
        AsyncMetricEntityStateBase.create(
            gaugeEntity,
            otelRepository,
            this::registerSensorIfAbsent,
            getTehutiName(type),
            Collections.singletonList(
                new AsyncGauge((ignored1, ignored2) -> currentLimit.get(), getTehutiName(type).getMetricName())),
            dimensionsMap,
            attributes,
            currentLimit::get);
      } else {
        MetricEntity metricEntity = getMetricEntityForType(type);
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
    }
    this.rateMetrics = Collections.unmodifiableMap(metricMap);
    this.currentLimitGauges = Collections.unmodifiableMap(gaugeMap);
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
      case BLOB_TRANSFER_WRITE_BANDWIDTH:
        return TehutiMetricName.BLOB_TRANSFER_WRITE_BANDWIDTH;
      case BLOB_TRANSFER_READ_BANDWIDTH:
        return TehutiMetricName.BLOB_TRANSFER_READ_BANDWIDTH;
      default:
        throw new IllegalArgumentException("No Tehuti metric name for: " + type);
    }
  }

  /**
   * Publishes the latest value for a throttler type. For {@link #GAUGE_TYPES} this updates the current-limit
   * gauge (last value); for the ingestion throttlers it records into the Rate/COUNTER metric.
   */
  public void recordRateForAdaptiveThrottler(VeniceAdaptiveThrottlerType throttlerType, long rate) {
    AtomicLong currentLimit = currentLimitGauges.get(throttlerType);
    if (currentLimit != null) {
      currentLimit.set(rate);
    } else {
      rateMetrics.get(throttlerType).record(rate);
    }
  }
}
