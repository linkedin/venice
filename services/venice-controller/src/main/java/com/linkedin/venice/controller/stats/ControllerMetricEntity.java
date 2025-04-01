package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import java.util.Set;


public enum ControllerMetricEntity {

  ;
  private final MetricEntity metricEntity;

  ControllerMetricEntity(
      MetricType metricType,
      MetricUnit unit,
      String description,
      Set<VeniceMetricsDimensions> dimensionsList) {
    this.metricEntity = new MetricEntity(this.name().toLowerCase(), metricType, unit, description, dimensionsList);
  }

  public MetricEntity getMetricEntity() {
    return metricEntity;
  }
}
