package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_REQUEST_URL;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;

import com.google.common.collect.ImmutableMap;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntityStateGeneric;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class SparkServerStats extends AbstractVeniceStats {
  private final boolean emitOpenTelemetryMetrics;
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;

  final private MetricEntityStateGeneric requestsCountMetric;
  final private MetricEntityStateGeneric finishedRequestsCountMetric;
  final private MetricEntityStateGeneric successfulRequestCountMetric;
  final private MetricEntityStateGeneric failedRequestCountMetric;
  final private MetricEntityStateGeneric currentInFlightRequestTotalCountMetric;
  final private MetricEntityStateGeneric successfulRequestLatencyHistogramMetric;
  final private MetricEntityStateGeneric failedRequestLatencyHistogramMetric;

  public SparkServerStats(MetricsRepository metricsRepository, String clusterName) {
    super(metricsRepository, clusterName);
    if (metricsRepository instanceof VeniceMetricsRepository) {
      VeniceMetricsRepository veniceMetricsRepository = (VeniceMetricsRepository) metricsRepository;
      VeniceMetricsConfig veniceMetricsConfig = veniceMetricsRepository.getVeniceMetricsConfig();
      emitOpenTelemetryMetrics = veniceMetricsConfig.emitOtelMetrics();
      if (emitOpenTelemetryMetrics) {
        this.otelRepository = veniceMetricsRepository.getOpenTelemetryMetricsRepository();
        this.baseDimensionsMap = new HashMap<>();
        this.baseDimensionsMap.put(VENICE_CLUSTER_NAME, clusterName);
      } else {
        this.otelRepository = null;
        this.baseDimensionsMap = null;
      }
    } else {
      this.emitOpenTelemetryMetrics = false;
      this.otelRepository = null;
      this.baseDimensionsMap = null;
    }

    requestsCountMetric = MetricEntityStateGeneric.create(
        ControllerMetricEntity.SPARK_SERVER_REQUESTS_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ControllerTehutiMetricNameEnum.REQUEST,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap);

    finishedRequestsCountMetric = MetricEntityStateGeneric.create(
        ControllerMetricEntity.SPARK_SERVER_FINISHED_REQUESTS_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ControllerTehutiMetricNameEnum.FINISHED_REQUEST,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap);

    successfulRequestCountMetric = MetricEntityStateGeneric.create(
        ControllerMetricEntity.SPARK_SERVER_SUCCESSFUL_REQUESTS_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ControllerTehutiMetricNameEnum.SUCCESSFUL_REQUEST,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap);

    failedRequestCountMetric = MetricEntityStateGeneric.create(
        ControllerMetricEntity.SPARK_SERVER_FAILED_REQUESTS_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ControllerTehutiMetricNameEnum.FAILED_REQUEST,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap);

    currentInFlightRequestTotalCountMetric = MetricEntityStateGeneric.create(
        ControllerMetricEntity.SPARK_SERVER_CURRENT_INFLIGHT_REQUESTS_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ControllerTehutiMetricNameEnum.CURRENT_IN_FLIGHT_REQUEST,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap);

    successfulRequestLatencyHistogramMetric = MetricEntityStateGeneric.create(
        ControllerMetricEntity.SPARK_SERVER_SUCCESSFUL_REQUESTS_LATENCY.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ControllerTehutiMetricNameEnum.SUCCESSFUL_REQUEST_LATENCY,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap);

    failedRequestLatencyHistogramMetric = MetricEntityStateGeneric.create(
        ControllerMetricEntity.SPARK_SERVER_FAILED_REQUESTS_LATENCY.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ControllerTehutiMetricNameEnum.FAILED_REQUEST_LATENCY,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap);
  }

  public void recordRequest(String url) {
    Map<VeniceMetricsDimensions, String> dimensions = getDimensionsBuilder().put(HTTP_REQUEST_URL, url).build();
    requestsCountMetric.record(1, dimensions);
    currentInFlightRequestTotalCountMetric.record(1, dimensions);
  }

  public void recordSuccessfulRequestLatency(String url, double latency) {
    Map<VeniceMetricsDimensions, String> dimensions = getDimensionsBuilder().put(HTTP_REQUEST_URL, url).build();
    finishRequest(url);
    successfulRequestCountMetric.record(1, dimensions);
    successfulRequestLatencyHistogramMetric.record(latency, dimensions);
  }

  public void recordFailedRequestLatency(String url, double latency) {
    Map<VeniceMetricsDimensions, String> dimensions = getDimensionsBuilder().put(HTTP_REQUEST_URL, url).build();
    finishRequest(url);
    failedRequestCountMetric.record(1, dimensions);
    failedRequestLatencyHistogramMetric.record(latency, dimensions);
  }

  private void finishRequest(String url) {
    Map<VeniceMetricsDimensions, String> dimensions = getDimensionsBuilder().put(HTTP_REQUEST_URL, url).build();
    finishedRequestsCountMetric.record(1, dimensions);
    currentInFlightRequestTotalCountMetric.record(-1, dimensions);
  }

  private ImmutableMap.Builder<VeniceMetricsDimensions, String> getDimensionsBuilder() {
    ImmutableMap.Builder<VeniceMetricsDimensions, String> builder = ImmutableMap.builder();

    if (baseDimensionsMap != null) {
      builder.putAll(baseDimensionsMap);
    }

    return builder;
  }

  enum ControllerTehutiMetricNameEnum implements TehutiMetricNameEnum {
    REQUEST, FINISHED_REQUEST, SUCCESSFUL_REQUEST, FAILED_REQUEST, CURRENT_IN_FLIGHT_REQUEST,
    SUCCESSFUL_REQUEST_LATENCY, FAILED_REQUEST_LATENCY;

    private final String metricName;

    ControllerTehutiMetricNameEnum() {
      this.metricName = name().toLowerCase();
    }

    @Override
    public String getMetricName() {
      return this.metricName;
    }
  }
}
