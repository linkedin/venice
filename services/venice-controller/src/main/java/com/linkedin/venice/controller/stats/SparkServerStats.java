package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CONTROLLER_ENDPOINT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateFourEnums;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Total;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import spark.Request;
import spark.Response;


public class SparkServerStats extends AbstractVeniceStats {
  public static final String NON_CLUSTER_SPECIFIC_STAT_CLUSTER_NAME = "cluster_generic";

  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;

  private final Sensor requests;
  private final Sensor finishedRequests;

  private final MetricEntityStateOneEnum<ControllerRoute> inFlightRequestTotalCountMetric;
  private final MetricEntityStateFourEnums<ControllerRoute, HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> successfulRequestCountMetric;
  private final MetricEntityStateFourEnums<ControllerRoute, HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> failedRequestCountMetric;
  private final MetricEntityStateFourEnums<ControllerRoute, HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> successfulRequestLatencyHistogramMetric;
  private final MetricEntityStateFourEnums<ControllerRoute, HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> failedRequestLatencyHistogramMetric;

  public SparkServerStats(MetricsRepository metricsRepository, String statsPrefix, String clusterName) {
    super(
        metricsRepository,
        (clusterName.equals(NON_CLUSTER_SPECIFIC_STAT_CLUSTER_NAME)) ? statsPrefix : clusterName + "." + statsPrefix);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(clusterName).build();

    this.otelRepository = otelData.getOtelRepository();
    this.baseDimensionsMap = otelData.getBaseDimensionsMap();

    requests =
        registerSensor(ControllerTehutiMetricNameEnum.REQUEST.getMetricName(), new Count(), new OccurrenceRate());
    finishedRequests = registerSensor(
        ControllerTehutiMetricNameEnum.FINISHED_REQUEST.getMetricName(),
        new Count(),
        new OccurrenceRate());

    inFlightRequestTotalCountMetric = MetricEntityStateOneEnum.create(
        SparkServerOtelMetricEntity.INFLIGHT_CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ControllerTehutiMetricNameEnum.CURRENT_IN_FLIGHT_REQUEST,
        Collections.singletonList(new Total()),
        baseDimensionsMap,
        ControllerRoute.class);

    successfulRequestCountMetric = MetricEntityStateFourEnums.create(
        SparkServerOtelMetricEntity.CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ControllerTehutiMetricNameEnum.SUCCESSFUL_REQUEST,
        Collections.singletonList(new Count()),
        baseDimensionsMap,
        ControllerRoute.class,
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);

    failedRequestCountMetric = MetricEntityStateFourEnums.create(
        SparkServerOtelMetricEntity.CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ControllerTehutiMetricNameEnum.FAILED_REQUEST,
        Collections.singletonList(new Count()),
        baseDimensionsMap,
        ControllerRoute.class,
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);

    successfulRequestLatencyHistogramMetric = MetricEntityStateFourEnums.create(
        SparkServerOtelMetricEntity.CALL_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ControllerTehutiMetricNameEnum.SUCCESSFUL_REQUEST_LATENCY,
        Collections.singletonList(
            TehutiUtils.getPercentileStat(
                getName(),
                ControllerTehutiMetricNameEnum.SUCCESSFUL_REQUEST_LATENCY.getMetricName())),
        baseDimensionsMap,
        ControllerRoute.class,
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);

    failedRequestLatencyHistogramMetric = MetricEntityStateFourEnums.create(
        SparkServerOtelMetricEntity.CALL_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        ControllerTehutiMetricNameEnum.FAILED_REQUEST_LATENCY,
        Collections.singletonList(
            TehutiUtils
                .getPercentileStat(getName(), ControllerTehutiMetricNameEnum.FAILED_REQUEST_LATENCY.getMetricName())),
        baseDimensionsMap,
        ControllerRoute.class,
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);

  }

  public void recordRequest(Request request) {
    ControllerRoute route = ControllerRoute.valueOfPath(request.uri());
    requests.record(1);
    inFlightRequestTotalCountMetric.record(1, route);
  }

  public void recordSuccessfulRequest(Request request, Response response, double latency) {
    ControllerRoute route = ControllerRoute.valueOfPath(request.uri());
    HttpResponseStatusEnum httpResponseStatus =
        HttpResponseStatusEnum.transformIntToHttpResponseStatusEnum(response.status());
    HttpResponseStatusCodeCategory httpResponseStatusCodeCategory =
        HttpResponseStatusCodeCategory.getVeniceHttpResponseStatusCodeCategory(response.status());
    VeniceResponseStatusCategory veniceResponseStatusCategory = VeniceResponseStatusCategory.SUCCESS;

    finishRequest(request);
    successfulRequestCountMetric
        .record(1, route, httpResponseStatus, httpResponseStatusCodeCategory, veniceResponseStatusCategory);
    successfulRequestLatencyHistogramMetric
        .record(latency, route, httpResponseStatus, httpResponseStatusCodeCategory, veniceResponseStatusCategory);
  }

  public void recordFailedRequest(Request request, Response response, double latency) {
    ControllerRoute route = ControllerRoute.valueOfPath(request.uri());
    HttpResponseStatusEnum httpResponseStatus =
        HttpResponseStatusEnum.transformIntToHttpResponseStatusEnum(response.status());
    HttpResponseStatusCodeCategory httpResponseStatusCodeCategory =
        HttpResponseStatusCodeCategory.getVeniceHttpResponseStatusCodeCategory(response.status());
    VeniceResponseStatusCategory veniceResponseStatusCategory = VeniceResponseStatusCategory.FAIL;

    finishRequest(request);
    failedRequestCountMetric
        .record(1, route, httpResponseStatus, httpResponseStatusCodeCategory, veniceResponseStatusCategory);
    failedRequestLatencyHistogramMetric
        .record(latency, route, httpResponseStatus, httpResponseStatusCodeCategory, veniceResponseStatusCategory);
  }

  private void finishRequest(Request request) {
    ControllerRoute route = ControllerRoute.valueOfPath(request.uri());
    finishedRequests.record(1);
    inFlightRequestTotalCountMetric.record(-1, route);
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

  public enum SparkServerOtelMetricEntity implements ModuleMetricEntityInterface {
    /** Count of current in flight messages to AdminSparkServer */
    INFLIGHT_CALL_COUNT(
        "inflight_call_count", MetricType.UP_DOWN_COUNTER, MetricUnit.NUMBER,
        "Count of all current inflight calls to controller spark server",
        setOf(VENICE_CLUSTER_NAME, VENICE_CONTROLLER_ENDPOINT)
    ),
    /** Count of completed calls to AdminSparkServer */
    CALL_COUNT(
        "call_count", MetricType.COUNTER, MetricUnit.NUMBER, "Count of all calls to controller spark server",
        setOf(
            VENICE_CLUSTER_NAME,
            VENICE_CONTROLLER_ENDPOINT,
            HTTP_RESPONSE_STATUS_CODE,
            HTTP_RESPONSE_STATUS_CODE_CATEGORY,
            VENICE_RESPONSE_STATUS_CODE_CATEGORY)
    ),
    /** Histogram of call latency to AdminSparkServer */
    CALL_TIME(
        "call_time", MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
        "Latency histogram of all successful calls to controller spark server",
        setOf(
            VENICE_CLUSTER_NAME,
            VENICE_CONTROLLER_ENDPOINT,
            HTTP_RESPONSE_STATUS_CODE,
            HTTP_RESPONSE_STATUS_CODE_CATEGORY,
            VENICE_RESPONSE_STATUS_CODE_CATEGORY)
    );

    private final MetricEntity metricEntity;

    SparkServerOtelMetricEntity(
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
