package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.Map;


public class LogCompactionStats {
  private final MetricsRepository metricsRepository;
  private final boolean emitOpenTelemetryMetrics;
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Attributes baseAttributes;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;

  public LogCompactionStats(MetricsRepository metricsRepository, String clusterName) {
    this.metricsRepository = metricsRepository;
    if (metricsRepository instanceof VeniceMetricsRepository) {
      VeniceMetricsRepository veniceMetricsRepository = (VeniceMetricsRepository) metricsRepository;
      VeniceMetricsConfig veniceMetricsConfig = veniceMetricsRepository.getVeniceMetricsConfig();
      emitOpenTelemetryMetrics = veniceMetricsConfig.emitOtelMetrics();
      if (emitOpenTelemetryMetrics) {
        this.otelRepository = veniceMetricsRepository.getOpenTelemetryMetricsRepository();
        this.baseDimensionsMap = new HashMap<>();
        this.baseDimensionsMap.put(VENICE_CLUSTER_NAME, clusterName);
        AttributesBuilder baseAttributesBuilder = Attributes.builder();
        baseAttributesBuilder.put(this.otelRepository.getDimensionName(VENICE_CLUSTER_NAME), clusterName);
        this.baseAttributes = baseAttributesBuilder.build();
      } else {
        this.otelRepository = null;
        this.baseAttributes = null;
        this.baseDimensionsMap = null;
      }
    } else {
      this.emitOpenTelemetryMetrics = false;
      this.otelRepository = null;
      this.baseAttributes = null;
      this.baseDimensionsMap = null;
    }
  }
}
