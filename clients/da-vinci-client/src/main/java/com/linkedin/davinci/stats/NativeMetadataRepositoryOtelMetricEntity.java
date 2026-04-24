package com.linkedin.davinci.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import java.util.Set;


/**
 * OTel metric entity definitions for {@link NativeMetadataRepositoryStats}.
 */
public enum NativeMetadataRepositoryOtelMetricEntity implements ModuleMetricEntityInterface {
  METADATA_CACHE_STALENESS(
      "metadata.staleness_duration", MetricType.ASYNC_DOUBLE_GAUGE, MetricUnit.MILLISECOND,
      "Per-store metadata staleness in ms since the store metadata was last fetched from the meta system store",
      setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME)
  );

  private final MetricEntity metricEntity;

  NativeMetadataRepositoryOtelMetricEntity(
      String metricName,
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensions) {
    this.metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensions);
  }

  @Override
  public MetricEntity getMetricEntity() {
    return metricEntity;
  }
}
