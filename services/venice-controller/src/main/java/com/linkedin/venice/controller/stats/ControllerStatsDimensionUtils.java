package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;

import com.google.common.collect.ImmutableMap;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import java.util.Map;


/**
 * Fluent builder for constructing dimension maps used by controller stats classes when recording
 * OTel metrics via {@link com.linkedin.venice.stats.metrics.MetricEntityStateGeneric}.
 */
class ControllerStatsDimensionUtils {
  private ControllerStatsDimensionUtils() {
  }

  static DimensionMapBuilder dimensionMapBuilder() {
    return new DimensionMapBuilder();
  }

  static class DimensionMapBuilder {
    private final ImmutableMap.Builder<VeniceMetricsDimensions, String> mapBuilder = ImmutableMap.builder();

    DimensionMapBuilder store(String storeName) {
      mapBuilder.put(VENICE_STORE_NAME, OpenTelemetryMetricsSetup.sanitizeStoreName(storeName));
      return this;
    }

    DimensionMapBuilder cluster(String clusterName) {
      mapBuilder.put(VENICE_CLUSTER_NAME, clusterName);
      return this;
    }

    DimensionMapBuilder add(VeniceMetricsDimensions dimension, String value) {
      mapBuilder.put(dimension, value);
      return this;
    }

    Map<VeniceMetricsDimensions, String> build() {
      return mapBuilder.build();
    }
  }
}
