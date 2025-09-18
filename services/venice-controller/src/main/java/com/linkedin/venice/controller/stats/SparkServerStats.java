package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_REQUEST_URI;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;

import com.google.common.collect.ImmutableMap;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntityStateGeneric;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import spark.Request;
import spark.Response;


public class SparkServerStats extends AbstractVeniceStats {
  private final boolean emitOpenTelemetryMetrics;
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;

  private final Sensor requests;
  private final Sensor finishedRequests;

  private final MetricEntityStateGeneric successfulRequestCountMetric;
  private final MetricEntityStateGeneric failedRequestCountMetric;
  private final MetricEntityStateGeneric successfulRequestLatencyHistogramMetric;
  private final MetricEntityStateGeneric failedRequestLatencyHistogramMetric;

  private final MetricEntityStateGeneric inFlightRequestTotalCountMetric;

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

    requests =
        registerSensor(ControllerTehutiMetricNameEnum.REQUEST.getMetricName(), new Count(), new OccurrenceRate());
    finishedRequests = registerSensor(
        ControllerTehutiMetricNameEnum.FINISHED_REQUEST.getMetricName(),
        new Count(),
        new OccurrenceRate());

    successfulRequestCountMetric = MetricEntityStateGeneric.create(
        ControllerMetricEntity.CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ControllerTehutiMetricNameEnum.SUCCESSFUL_REQUEST,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap);

    failedRequestCountMetric = MetricEntityStateGeneric.create(
        ControllerMetricEntity.CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ControllerTehutiMetricNameEnum.FAILED_REQUEST,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap);

    successfulRequestLatencyHistogramMetric = MetricEntityStateGeneric.create(
        ControllerMetricEntity.REQUEST_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ControllerTehutiMetricNameEnum.SUCCESSFUL_REQUEST_LATENCY,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap);

    failedRequestLatencyHistogramMetric = MetricEntityStateGeneric.create(
        ControllerMetricEntity.REQUEST_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ControllerTehutiMetricNameEnum.FAILED_REQUEST_LATENCY,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap);

    inFlightRequestTotalCountMetric = MetricEntityStateGeneric.create(
        ControllerMetricEntity.IN_FLIGHT_CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ControllerTehutiMetricNameEnum.CURRENT_IN_FLIGHT_REQUEST,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap);
  }

  public void recordRequest(Request request) {
    Map<VeniceMetricsDimensions, String> dimensions = getDimensionsFromRequestAndResponse(request, null).build();
    requests.record(1);
    inFlightRequestTotalCountMetric.record(1, dimensions);
  }

  public void recordSuccessfulRequest(Request request, Response response, double latency) {
    Map<VeniceMetricsDimensions, String> dimensions = getDimensionsFromRequestAndResponse(request, response)
        .put(VENICE_RESPONSE_STATUS_CODE_CATEGORY, VeniceResponseStatusCategory.SUCCESS.getDimensionValue())
        .build();
    finishRequest(request);
    successfulRequestCountMetric.record(1, dimensions);
    successfulRequestLatencyHistogramMetric.record(latency, dimensions);
  }

  public void recordFailedRequest(Request request, Response response, double latency) {
    Map<VeniceMetricsDimensions, String> dimensions = getDimensionsFromRequestAndResponse(request, response)
        .put(VENICE_RESPONSE_STATUS_CODE_CATEGORY, VeniceResponseStatusCategory.FAIL.getDimensionValue())
        .build();
    finishRequest(request);
    failedRequestCountMetric.record(1, dimensions);
    failedRequestLatencyHistogramMetric.record(latency, dimensions);
  }

  private void finishRequest(Request request) {
    Map<VeniceMetricsDimensions, String> dimensions = getDimensionsFromRequestAndResponse(request, null).build();
    finishedRequests.record(1);
    inFlightRequestTotalCountMetric.record(-1, dimensions);
  }

  private ImmutableMap.Builder<VeniceMetricsDimensions, String> getDimensionsBuilder() {
    ImmutableMap.Builder<VeniceMetricsDimensions, String> builder = ImmutableMap.builder();

    if (baseDimensionsMap != null) {
      builder.putAll(baseDimensionsMap);
    }

    return builder;
  }

  private ImmutableMap.Builder<VeniceMetricsDimensions, String> getDimensionsFromRequestAndResponse(
      Request request,
      Response response) {
    ImmutableMap.Builder<VeniceMetricsDimensions, String> builder = getDimensionsBuilder();

    if (request != null) {
      ControllerRoute route = ControllerRoute.valueOfPath(request.uri());
      if (route != null) {
        builder = builder.put(HTTP_REQUEST_URI, route.getPath());
      }
      builder = builder.put(HTTP_REQUEST_METHOD, request.requestMethod());
    }

    if (response != null) {
      builder = builder
          .put(
              HTTP_RESPONSE_STATUS_CODE,
              HttpResponseStatusEnum.transformIntToHttpResponseStatusEnum(response.status()).getDimensionValue())
          .put(
              HTTP_RESPONSE_STATUS_CODE_CATEGORY,
              HttpResponseStatusCodeCategory.getVeniceHttpResponseStatusCodeCategory(response.status())
                  .getDimensionValue());
    }

    return builder;
  }

  enum ControllerTehutiMetricNameEnum implements TehutiMetricNameEnum {
    REQUEST, FINISHED_REQUEST, CURRENT_IN_FLIGHT_REQUEST, SUCCESSFUL_REQUEST, FAILED_REQUEST,
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
