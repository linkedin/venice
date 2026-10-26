package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_SYSTEM_STORE_TYPE;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceSystemStoreType;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;


/**
 * This class is the metric class for {@link com.linkedin.venice.controller.systemstore.SystemStoreRepairService}.
 * OTel reports the counts that {@link #setMeasured} published for this controller's last completed round as the
 * cluster's leader; Tehuti keeps reporting the raw counters.
 */
public class SystemStoreHealthCheckStats extends AbstractVeniceStats {
  private final Sensor badMetaSystemStoreCountSensor;
  private final Sensor badPushStatusSystemStoreCountSensor;
  private final Sensor notRepairableSystemStoreCountSensor;
  private final Sensor systemStoreHealthCheckErrorCountSensor;
  private final AtomicLong badMetaSystemStoreCounter = new AtomicLong(0);
  private final AtomicLong badPushStatusSystemStoreCounter = new AtomicLong(0);
  private final AtomicLong notRepairableSystemStoreCounter = new AtomicLong(0);
  private final AtomicLong systemStoreHealthCheckErrorCounter = new AtomicLong(0);
  /** Null while this controller has no completed round to report. */
  private volatile RoundCounts publishedRound;

  public SystemStoreHealthCheckStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    // Tehuti and OTel are registered separately because: (1) multiple Tehuti sensors (bad_meta + bad_push_status)
    // map to a single OTel metric differentiated by dimension, and (2) AsyncMetricEntityStateOneEnum only supports
    // OTel registration, not Tehuti.
    badMetaSystemStoreCountSensor = registerSensorIfAbsent(
        new AsyncGauge(
            (ignored, ignored2) -> badMetaSystemStoreCounter.get(),
            SystemStoreHealthCheckTehutiMetricNameEnum.BAD_META_SYSTEM_STORE_COUNT.getMetricName()));
    badPushStatusSystemStoreCountSensor = registerSensorIfAbsent(
        new AsyncGauge(
            (ignored, ignored2) -> badPushStatusSystemStoreCounter.get(),
            SystemStoreHealthCheckTehutiMetricNameEnum.BAD_PUSH_STATUS_SYSTEM_STORE_COUNT.getMetricName()));
    notRepairableSystemStoreCountSensor = registerSensorIfAbsent(
        new AsyncGauge(
            (ignored, ignored2) -> notRepairableSystemStoreCounter.get(),
            SystemStoreHealthCheckTehutiMetricNameEnum.NOT_REPAIRABLE_SYSTEM_STORE_COUNT.getMetricName()));
    systemStoreHealthCheckErrorCountSensor = registerSensorIfAbsent(
        new AsyncGauge(
            (ignored, ignored2) -> systemStoreHealthCheckErrorCounter.get(),
            SystemStoreHealthCheckTehutiMetricNameEnum.SYSTEM_STORE_HEALTH_CHECK_ERROR_COUNT.getMetricName()));

    // OTel setup
    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(name).build();
    VeniceOpenTelemetryMetricsRepository otelRepository = otelData.getOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();
    Attributes baseAttributes = otelData.getBaseAttributes();

    // OTel async gauge. The liveStateResolver returns the published round for each mapped VeniceSystemStoreType value
    // (null while there is none or for any future enum additions, which skips emission); the valueResolver reads that
    // type's count from it.
    AsyncMetricEntityStateOneEnum.create(
        SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNHEALTHY_COUNT.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        VeniceSystemStoreType.class,
        getMetricScope(),
        type -> {
          switch (type) {
            case META_STORE:
            case DAVINCI_PUSH_STATUS_STORE:
              return publishedRound;
            default:
              /*
               * Return null (skip emission) rather than throw — throwing on every collection cycle
               * would spam failure metrics. Missing switch cases are caught by
               * SystemStoreHealthCheckStatsOtelTest#testEveryVeniceSystemStoreTypeEmitsADataPoint.
               */
              return null;
          }
        },
        (round, type) -> type == VeniceSystemStoreType.META_STORE
            ? round.badMetaSystemStores
            : round.badPushStatusSystemStores);

    AsyncMetricEntityStateBase.createWithState(
        SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNREPAIRABLE_COUNT.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        baseAttributes,
        getMetricScope(),
        () -> publishedRound,
        round -> round.notRepairableSystemStores);

    AsyncMetricEntityStateBase.createWithState(
        SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_HEALTH_CHECK_ERROR_COUNT.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        baseAttributes,
        getMetricScope(),
        () -> publishedRound,
        round -> round.healthCheckErrors);
  }

  /**
   * {@code true} publishes the current counters as this controller's last completed round as the cluster's leader,
   * which OTel reports until the next call, so a round in progress never exports a mix of old and new counts.
   * {@code false} stops OTel reporting.
   */
  public void setMeasured(boolean measured) {
    this.publishedRound = measured ? new RoundCounts(this) : null;
  }

  public AtomicLong getBadMetaSystemStoreCounter() {
    return badMetaSystemStoreCounter;
  }

  public AtomicLong getBadPushStatusSystemStoreCounter() {
    return badPushStatusSystemStoreCounter;
  }

  public AtomicLong getNotRepairableSystemStoreCounter() {
    return notRepairableSystemStoreCounter;
  }

  public AtomicLong getSystemStoreHealthCheckErrorCounter() {
    return systemStoreHealthCheckErrorCounter;
  }

  /** The counters as of a completed round. */
  private static final class RoundCounts {
    private final long badMetaSystemStores;
    private final long badPushStatusSystemStores;
    private final long notRepairableSystemStores;
    private final long healthCheckErrors;

    private RoundCounts(SystemStoreHealthCheckStats stats) {
      this.badMetaSystemStores = stats.badMetaSystemStoreCounter.get();
      this.badPushStatusSystemStores = stats.badPushStatusSystemStoreCounter.get();
      this.notRepairableSystemStores = stats.notRepairableSystemStoreCounter.get();
      this.healthCheckErrors = stats.systemStoreHealthCheckErrorCounter.get();
    }
  }

  enum SystemStoreHealthCheckTehutiMetricNameEnum implements TehutiMetricNameEnum {
    BAD_META_SYSTEM_STORE_COUNT, BAD_PUSH_STATUS_SYSTEM_STORE_COUNT, NOT_REPAIRABLE_SYSTEM_STORE_COUNT,
    SYSTEM_STORE_HEALTH_CHECK_ERROR_COUNT
  }

  public enum SystemStoreHealthCheckOtelMetricEntity implements ModuleMetricEntityInterface {
    SYSTEM_STORE_UNHEALTHY_COUNT(
        "system_store.health_check.unhealthy_count", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
        "Unhealthy system stores, differentiated by system store type",
        setOf(VENICE_CLUSTER_NAME, VENICE_SYSTEM_STORE_TYPE)
    ),
    SYSTEM_STORE_UNREPAIRABLE_COUNT(
        "system_store.health_check.unrepairable_count", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
        "System stores that cannot be repaired", setOf(VENICE_CLUSTER_NAME)
    ),
    SYSTEM_STORE_HEALTH_CHECK_ERROR_COUNT(
        "system_store.health_check.error_count", MetricType.ASYNC_GAUGE, MetricUnit.NUMBER,
        "Cumulative count of system store health-check invocations that failed by throwing or returning null",
        setOf(VENICE_CLUSTER_NAME)
    );

    private final MetricEntity metricEntity;

    SystemStoreHealthCheckOtelMetricEntity(
        String metricName,
        MetricType metricType,
        MetricUnit unit,
        String description,
        Set<VeniceMetricsDimensions> dimensionsList) {
      this.metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensionsList);
    }

    @Override
    public MetricEntity getMetricEntity() {
      return metricEntity;
    }
  }
}
