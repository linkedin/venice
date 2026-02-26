package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Gauge;
import java.util.Collections;
import java.util.Map;
import java.util.Set;


public class ProtocolVersionAutoDetectionStats extends AbstractVeniceStats {
  private static final String TEHUTI_PREFIX = "admin_operation_protocol_version_auto_detection_service_";

  private final MetricEntityStateBase consecutiveFailureMetric;
  private final MetricEntityStateBase detectionTimeMetric;

  public ProtocolVersionAutoDetectionStats(MetricsRepository metricsRepository, String clusterName) {
    super(metricsRepository, TEHUTI_PREFIX + clusterName);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(clusterName).build();
    VeniceOpenTelemetryMetricsRepository otelRepository = otelData.getOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();
    Attributes baseAttributes = otelData.getBaseAttributes();

    consecutiveFailureMetric = MetricEntityStateBase.create(
        ProtocolVersionAutoDetectionOtelMetricEntity.PROTOCOL_VERSION_AUTO_DETECTION_FAILURE_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        ProtocolVersionAutoDetectionTehutiMetricNameEnum.PROTOCOL_VERSION_AUTO_DETECTION_ERROR,
        Collections.singletonList(new Gauge()),
        baseDimensionsMap,
        baseAttributes);

    detectionTimeMetric = MetricEntityStateBase.create(
        ProtocolVersionAutoDetectionOtelMetricEntity.PROTOCOL_VERSION_AUTO_DETECTION_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        ProtocolVersionAutoDetectionTehutiMetricNameEnum.PROTOCOL_VERSION_AUTO_DETECTION_LATENCY,
        Collections.singletonList(new Avg()),
        baseDimensionsMap,
        baseAttributes);
  }

  public void recordProtocolVersionAutoDetectionErrorSensor(int count) {
    consecutiveFailureMetric.record(count);
  }

  public void recordProtocolVersionAutoDetectionLatencySensor(double latencyInMs) {
    detectionTimeMetric.record(latencyInMs);
  }

  enum ProtocolVersionAutoDetectionTehutiMetricNameEnum implements TehutiMetricNameEnum {
    PROTOCOL_VERSION_AUTO_DETECTION_ERROR, PROTOCOL_VERSION_AUTO_DETECTION_LATENCY
  }

  public enum ProtocolVersionAutoDetectionOtelMetricEntity implements ModuleMetricEntityInterface {
    PROTOCOL_VERSION_AUTO_DETECTION_FAILURE_COUNT(
        "protocol_version_auto_detection.consecutive_failure_count", MetricType.GAUGE, MetricUnit.NUMBER,
        "Consecutive failures in protocol version auto-detection", setOf(VENICE_CLUSTER_NAME)
    ),
    PROTOCOL_VERSION_AUTO_DETECTION_TIME(
        "protocol_version_auto_detection.time", MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
        "Latency of protocol version auto-detection", setOf(VENICE_CLUSTER_NAME)
    );

    private final MetricEntity metricEntity;

    ProtocolVersionAutoDetectionOtelMetricEntity(
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
