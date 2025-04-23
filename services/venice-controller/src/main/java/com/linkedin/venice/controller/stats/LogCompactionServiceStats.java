package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.Map;


public class LogCompactionServiceStats {
  private final boolean emitOpenTelemetryMetrics;
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Attributes baseAttributes;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap = new HashMap<>();

  public LogCompactionServiceStats(
      MetricsRepository metricsRepository,
      boolean emitOpenTelemetryMetrics,
      VeniceOpenTelemetryMetricsRepository otelRepository,
      Attributes baseAttributes) {
    if (metricsRepository instanceof VeniceMetricsRepository) {
      VeniceMetricsRepository veniceMetricsRepository = (VeniceMetricsRepository) metricsRepository;
      VeniceMetricsConfig veniceMetricsConfig = veniceMetricsRepository.getVeniceMetricsConfig();
      emitOpenTelemetryMetrics = veniceMetricsConfig.emitOtelMetrics();
      if (emitOpenTelemetryMetrics) {
        otelRepository = veniceMetricsRepository.getOpenTelemetryMetricsRepository();
        // baseDimensionsMap = new HashMap<>();
        // baseDimensionsMap.put(VENICE_STORE_NAME, storeName);
        // baseDimensionsMap.put(VENICE_REQUEST_METHOD, requestType.getDimensionValue());
        // baseDimensionsMap.put(VENICE_CLUSTER_NAME, clusterName);
        // AttributesBuilder baseAttributesBuilder = Attributes.builder();
        // baseAttributesBuilder.put(otelRepository.getDimensionName(VENICE_STORE_NAME), storeName);
        // baseAttributesBuilder
        // .put(otelRepository.getDimensionName(VENICE_REQUEST_METHOD), requestType.getDimensionValue());
        // baseAttributesBuilder.put(otelRepository.getDimensionName(VENICE_CLUSTER_NAME), clusterName);
        // baseAttributes = baseAttributesBuilder.build();
      } else {
        otelRepository = null;
        baseAttributes = null;
      }
    } else {
      otelRepository = null;
      emitOpenTelemetryMetrics = false;
      baseAttributes = null;
    }

    this.emitOpenTelemetryMetrics = emitOpenTelemetryMetrics;
    this.otelRepository = otelRepository;
    this.baseAttributes = baseAttributes;
  }
}
