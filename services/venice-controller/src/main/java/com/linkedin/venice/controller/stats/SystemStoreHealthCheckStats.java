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
 * This class is the metric class for {@link com.linkedin.venice.controller.systemstore.SystemStoreRepairService}
 */
public class SystemStoreHealthCheckStats extends AbstractVeniceStats {
  private final Sensor badMetaSystemStoreCountSensor;
  private final Sensor badPushStatusSystemStoreCountSensor;
  private final Sensor notRepairableSystemStoreCountSensor;
  private final AtomicLong badMetaSystemStoreCounter = new AtomicLong(0);
  private final AtomicLong badPushStatusSystemStoreCounter = new AtomicLong(0);
  private final AtomicLong notRepairableSystemStoreCounter = new AtomicLong(0);

  public SystemStoreHealthCheckStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    // Tehuti and OTel are registered separately because: (1) multiple Tehuti sensors (bad_meta + bad_push_status)
    // map to a single OTel metric differentiated by dimension, and (2) AsyncGauge is callback-based, not a
    // MeasurableStat, so it cannot be passed to the combined MetricEntityStateBase.create() factory.
    badMetaSystemStoreCountSensor = registerSensorIfAbsent(
        new AsyncGauge((ignored, ignored2) -> badMetaSystemStoreCounter.get(), "bad_meta_system_store_count"));
    badPushStatusSystemStoreCountSensor = registerSensorIfAbsent(
        new AsyncGauge(
            (ignored, ignored2) -> badPushStatusSystemStoreCounter.get(),
            "bad_push_status_system_store_count"));
    notRepairableSystemStoreCountSensor = registerSensorIfAbsent(
        new AsyncGauge(
            (ignored, ignored2) -> notRepairableSystemStoreCounter.get(),
            "not_repairable_system_store_count"));

    // OTel setup
    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(name).build();
    VeniceOpenTelemetryMetricsRepository otelRepository = otelData.getOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();
    Attributes baseAttributes = otelData.getBaseAttributes();

    // OTel async gauges
    AsyncMetricEntityStateOneEnum.create(
        SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNHEALTHY_COUNT.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        VeniceSystemStoreType.class,
        type -> {
          switch (type) {
            case META_STORE:
              return badMetaSystemStoreCounter::get;
            case DAVINCI_PUSH_STATUS_STORE:
              return badPushStatusSystemStoreCounter::get;
            default:
              return () -> 0;
          }
        });

    AsyncMetricEntityStateBase.create(
        SystemStoreHealthCheckOtelMetricEntity.SYSTEM_STORE_UNREPAIRABLE_COUNT.getMetricEntity(),
        otelRepository,
        baseDimensionsMap,
        baseAttributes,
        notRepairableSystemStoreCounter::get);
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

  enum SystemStoreHealthCheckTehutiMetricNameEnum implements TehutiMetricNameEnum {
    BAD_META_SYSTEM_STORE_COUNT, BAD_PUSH_STATUS_SYSTEM_STORE_COUNT, NOT_REPAIRABLE_SYSTEM_STORE_COUNT
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
