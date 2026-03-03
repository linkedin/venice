package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.AbstractVeniceAggStats.STORE_NAME_FOR_TOTAL_STAT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.meta.Version;
import com.linkedin.venice.stats.AbstractVeniceAggStats;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Gauge;
import io.tehuti.metrics.stats.Max;
import java.util.Arrays;
import java.util.Set;


/**
 * Resource level partition health stats. Tracks under-replicated partitions for a given version topic.
 * Both Tehuti and OTel metrics are combined into a single {@link MetricEntityStateBase}, following
 * the same pattern used in the router (e.g., RouterHttpRequestStats).
 *
 * <p>For OTel, the store name dimension is derived from the version topic name at construction time.
 * Total stats instances (created by {@link AbstractVeniceAggStats}) have OTel disabled via
 * {@link OpenTelemetryMetricsSetup.Builder#isTotalStats(boolean)}.
 */
public class PartitionHealthStats extends AbstractVeniceStats {
  private final MetricEntityStateBase underReplicatedPartitionMetric;

  /**
   * Only for test usage.
   */
  public PartitionHealthStats(String resourceName) {
    super(null, resourceName);
    this.underReplicatedPartitionMetric = null;
  }

  public PartitionHealthStats(MetricsRepository metricsRepository, String name, String clusterName) {
    super(metricsRepository, name);

    // AbstractVeniceAggStats creates total stats with names like "total" (non-cluster)
    // or "total.clusterName" (per-cluster aggregate). isTotalStats() only matches the first
    // pattern, so we also check for the per-cluster pattern explicitly.
    boolean isTotal = isTotalStats() || name.startsWith(STORE_NAME_FOR_TOTAL_STAT + ".");
    String storeName = isTotal ? null : Version.parseStoreFromKafkaTopicName(name);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository)
            .isTotalStats(isTotal)
            .setClusterName(clusterName)
            .setStoreName(storeName)
            .build();

    underReplicatedPartitionMetric = MetricEntityStateBase.create(
        PartitionHealthOtelMetricEntity.PARTITION_UNDER_REPLICATED_COUNT.getMetricEntity(),
        otelData.getOtelRepository(),
        this::registerSensorIfAbsent,
        PartitionHealthTehutiMetricNameEnum.UNDER_REPLICATED_PARTITION,
        Arrays.asList(new Max(), new Gauge()),
        otelData.getBaseDimensionsMap(),
        otelData.getBaseAttributes());
  }

  public void recordUnderReplicatePartition(int num) {
    if (underReplicatedPartitionMetric != null) {
      underReplicatedPartitionMetric.record(num);
    }
  }

  enum PartitionHealthTehutiMetricNameEnum implements TehutiMetricNameEnum {
    UNDER_REPLICATED_PARTITION;

    @Override
    public String getMetricName() {
      // Preserve the original camelCase sensor name for backward compatibility with existing dashboards.
      return "underReplicatedPartition";
    }
  }

  public enum PartitionHealthOtelMetricEntity implements ModuleMetricEntityInterface {
    PARTITION_UNDER_REPLICATED_COUNT(
        "partition.under_replicated_count", MetricType.GAUGE, MetricUnit.NUMBER,
        "Partitions with fewer ready-to-serve replicas than the replication factor",
        setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)
    );

    private final MetricEntity metricEntity;

    PartitionHealthOtelMetricEntity(
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
