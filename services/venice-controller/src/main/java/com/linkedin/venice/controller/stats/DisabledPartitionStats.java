package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateGeneric;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Total;
import java.util.Collections;
import java.util.Map;


public class DisabledPartitionStats extends AbstractVeniceStats {
  private final MetricEntityStateGeneric disabledPartitionCountMetric;

  public DisabledPartitionStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(name).build();
    VeniceOpenTelemetryMetricsRepository otelRepository = otelData.getOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();

    disabledPartitionCountMetric = MetricEntityStateGeneric.create(
        DisabledPartitionOtelMetricEntity.DISABLED_PARTITION_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        DisabledPartitionTehutiMetricNameEnum.DISABLED_PARTITION_COUNT,
        Collections.singletonList(new Total()),
        baseDimensionsMap);
  }

  public void recordDisabledPartition(String storeName) {
    disabledPartitionCountMetric.record(1, storeDimensions(storeName));
  }

  public void recordClearDisabledPartition(int count, String storeName) {
    disabledPartitionCountMetric.record(-count, storeDimensions(storeName));
  }

  private static Map<VeniceMetricsDimensions, String> storeDimensions(String storeName) {
    return Collections.singletonMap(VENICE_STORE_NAME, storeName);
  }

  enum DisabledPartitionTehutiMetricNameEnum implements TehutiMetricNameEnum {
    DISABLED_PARTITION_COUNT
  }

  public enum DisabledPartitionOtelMetricEntity implements ModuleMetricEntityInterface {
    DISABLED_PARTITION_COUNT(
        new MetricEntity(
            "partition.disabled_partition.count",
            MetricType.UP_DOWN_COUNTER,
            MetricUnit.NUMBER,
            "Partition replicas disabled in Helix due to ingestion errors",
            setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME))
    );

    private final MetricEntity metricEntity;

    DisabledPartitionOtelMetricEntity(MetricEntity metricEntity) {
      this.metricEntity = metricEntity;
    }

    @Override
    public MetricEntity getMetricEntity() {
      return metricEntity;
    }
  }
}
