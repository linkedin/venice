package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.controller.stats.ControllerStatsDimensionUtils.dimensionMapBuilder;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REGION_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateGeneric;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Gauge;
import io.tehuti.metrics.stats.Max;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;


/**
 * Metrics for degraded mode operations including recovery, push auto-conversion, and incremental push blocking.
 */
public class DegradedModeStats extends AbstractVeniceStats {
  private final MetricEntityStateGeneric recoveryStoreSuccessMetric;
  private final MetricEntityStateGeneric recoveryStoreFailureMetric;
  private final MetricEntityStateGeneric recoveryVersionTransitionedMetric;
  private final MetricEntityStateBase recoveryProgressMetric;
  private final MetricEntityStateGeneric pushAutoConvertedMetric;
  private final MetricEntityStateGeneric pushBlockedIncrementalMetric;
  private final MetricEntityStateBase degradedDcActiveMetric;
  private final MetricEntityStateGeneric degradedDcDurationMetric;
  private final MetricEntityStateGeneric recoveryStoreDurationMetric;

  public DegradedModeStats(MetricsRepository metricsRepository) {
    super(metricsRepository, "DegradedMode");

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).build();
    VeniceOpenTelemetryMetricsRepository otelRepository = otelData.getOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();
    Attributes baseAttributes = otelData.getBaseAttributes();

    recoveryStoreSuccessMetric = MetricEntityStateGeneric.create(
        DegradedModeOtelMetric.RECOVERY_STORE_SUCCESS_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        DegradedModeTehutiMetric.RECOVERY_STORE_SUCCESS,
        Collections.singletonList(new Count()),
        baseDimensionsMap);

    recoveryStoreFailureMetric = MetricEntityStateGeneric.create(
        DegradedModeOtelMetric.RECOVERY_STORE_FAILURE_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        DegradedModeTehutiMetric.RECOVERY_STORE_FAILURE,
        Collections.singletonList(new Count()),
        baseDimensionsMap);

    recoveryVersionTransitionedMetric = MetricEntityStateGeneric.create(
        DegradedModeOtelMetric.RECOVERY_VERSION_TRANSITIONED_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        DegradedModeTehutiMetric.RECOVERY_VERSION_TRANSITIONED,
        Collections.singletonList(new Count()),
        baseDimensionsMap);

    recoveryProgressMetric = MetricEntityStateBase.create(
        DegradedModeOtelMetric.RECOVERY_PROGRESS.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        DegradedModeTehutiMetric.RECOVERY_PROGRESS,
        Collections.singletonList(new Gauge()),
        baseDimensionsMap,
        baseAttributes);

    pushAutoConvertedMetric = MetricEntityStateGeneric.create(
        DegradedModeOtelMetric.PUSH_AUTO_CONVERTED_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        DegradedModeTehutiMetric.PUSH_AUTO_CONVERTED,
        Collections.singletonList(new Count()),
        baseDimensionsMap);

    pushBlockedIncrementalMetric = MetricEntityStateGeneric.create(
        DegradedModeOtelMetric.PUSH_BLOCKED_INCREMENTAL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        DegradedModeTehutiMetric.PUSH_BLOCKED_INCREMENTAL,
        Collections.singletonList(new Count()),
        baseDimensionsMap);

    degradedDcActiveMetric = MetricEntityStateBase.create(
        DegradedModeOtelMetric.DEGRADED_DC_ACTIVE_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        DegradedModeTehutiMetric.DEGRADED_DC_ACTIVE,
        Collections.singletonList(new Gauge()),
        baseDimensionsMap,
        baseAttributes);

    degradedDcDurationMetric = MetricEntityStateGeneric.create(
        DegradedModeOtelMetric.DEGRADED_DC_DURATION_MINUTES.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        DegradedModeTehutiMetric.DEGRADED_DC_DURATION,
        Collections.singletonList(new Gauge()),
        baseDimensionsMap);

    recoveryStoreDurationMetric = MetricEntityStateGeneric.create(
        DegradedModeOtelMetric.RECOVERY_STORE_DURATION_MS.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        DegradedModeTehutiMetric.RECOVERY_STORE_DURATION,
        Arrays.asList(new Avg(), new Max()),
        baseDimensionsMap);
  }

  public void recordRecoveryStoreSuccess(String clusterName, String storeName) {
    recoveryStoreSuccessMetric.record(1, dimensionMapBuilder().cluster(clusterName).store(storeName).build());
  }

  public void recordRecoveryStoreFailure(String clusterName, String storeName) {
    recoveryStoreFailureMetric.record(1, dimensionMapBuilder().cluster(clusterName).store(storeName).build());
  }

  public void recordRecoveryVersionTransitioned(String clusterName, String storeName) {
    recoveryVersionTransitionedMetric.record(1, dimensionMapBuilder().cluster(clusterName).store(storeName).build());
  }

  public void recordRecoveryProgress(double progress) {
    recoveryProgressMetric.record(progress);
  }

  public void recordPushAutoConverted(String clusterName, String storeName) {
    pushAutoConvertedMetric.record(1, dimensionMapBuilder().cluster(clusterName).store(storeName).build());
  }

  public void recordPushBlockedIncremental(String clusterName, String storeName) {
    pushBlockedIncrementalMetric.record(1, dimensionMapBuilder().cluster(clusterName).store(storeName).build());
  }

  public void recordDegradedDcActiveCount(double count) {
    degradedDcActiveMetric.record(count);
  }

  public void recordRecoveryStoreDurationMs(String clusterName, String storeName, double durationMs) {
    recoveryStoreDurationMetric.record(durationMs, dimensionMapBuilder().cluster(clusterName).store(storeName).build());
  }

  public void recordDegradedDcDurationMinutes(String clusterName, String datacenterName, double durationMinutes) {
    degradedDcDurationMetric.record(
        durationMinutes,
        dimensionMapBuilder().cluster(clusterName).add(VENICE_REGION_NAME, datacenterName).build());
  }

  enum DegradedModeTehutiMetric implements TehutiMetricNameEnum {
    RECOVERY_STORE_SUCCESS, RECOVERY_STORE_FAILURE, RECOVERY_VERSION_TRANSITIONED, RECOVERY_PROGRESS,
    RECOVERY_STORE_DURATION, PUSH_AUTO_CONVERTED, PUSH_BLOCKED_INCREMENTAL, DEGRADED_DC_ACTIVE, DEGRADED_DC_DURATION
  }

  public enum DegradedModeOtelMetric implements ModuleMetricEntityInterface {
    RECOVERY_STORE_SUCCESS_COUNT(
        "degraded_mode.recovery.store_success_count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Count of individual store recoveries completed successfully", setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)
    ),
    RECOVERY_STORE_FAILURE_COUNT(
        "degraded_mode.recovery.store_failure_count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Count of individual store recoveries that failed after all retries",
        setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)
    ),
    RECOVERY_VERSION_TRANSITIONED_COUNT(
        "degraded_mode.recovery.version_transitioned_count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Count of versions transitioned from PARTIALLY_ONLINE to ONLINE after recovery",
        setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)
    ),
    RECOVERY_PROGRESS(
        MetricEntity.createWithNoDimensions(
            "degraded_mode.recovery.progress",
            MetricType.GAUGE,
            MetricUnit.NUMBER,
            "Recovery progress (0.0 to 1.0) for the most recent recovery operation")
    ),
    PUSH_AUTO_CONVERTED_COUNT(
        "degraded_mode.push.auto_converted_count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Count of pushes auto-converted to targeted region push due to degraded DC",
        setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)
    ),
    PUSH_BLOCKED_INCREMENTAL_COUNT(
        "degraded_mode.push.blocked_incremental_count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Count of incremental pushes blocked due to degraded DC", setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)
    ),
    DEGRADED_DC_ACTIVE_COUNT(
        MetricEntity.createWithNoDimensions(
            "degraded_mode.dc.active_count",
            MetricType.GAUGE,
            MetricUnit.NUMBER,
            "Number of currently degraded DCs")
    ),
    DEGRADED_DC_DURATION_MINUTES(
        "degraded_mode.dc.duration_minutes", MetricType.GAUGE, MetricUnit.NUMBER,
        "How long each DC has been in degraded state, in minutes. Set alert on this metric.",
        setOf(VENICE_CLUSTER_NAME, VENICE_REGION_NAME)
    ),
    RECOVERY_STORE_DURATION_MS(
        "degraded_mode.recovery.store_duration_ms", MetricType.GAUGE, MetricUnit.MILLISECOND,
        "Duration of individual store recovery from initiation to completion, in milliseconds",
        setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)
    );

    private final MetricEntity metricEntity;

    DegradedModeOtelMetric(
        String metricName,
        MetricType metricType,
        MetricUnit unit,
        String description,
        Set<VeniceMetricsDimensions> dimensionsList) {
      this.metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensionsList);
    }

    DegradedModeOtelMetric(MetricEntity metricEntity) {
      this.metricEntity = metricEntity;
    }

    @Override
    public MetricEntity getMetricEntity() {
      return metricEntity;
    }
  }
}
