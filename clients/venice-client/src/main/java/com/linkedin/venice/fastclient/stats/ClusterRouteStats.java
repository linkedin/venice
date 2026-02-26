package com.linkedin.venice.fastclient.stats;

import static com.linkedin.venice.client.stats.ClientMetricEntity.ROUTE_CALL_COUNT;
import static com.linkedin.venice.client.stats.ClientMetricEntity.ROUTE_CALL_TIME;
import static com.linkedin.venice.client.stats.ClientMetricEntity.ROUTE_REQUEST_PENDING_COUNT;
import static com.linkedin.venice.client.stats.ClientMetricEntity.ROUTE_REQUEST_REJECTION_RATIO;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum.UNKNOWN;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AbstractVeniceHttpStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.StatsUtils;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.dimensions.RejectionReason;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.MetricEntityStateThreeEnums;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ClusterRouteStats {
  private static final Logger LOGGER = LogManager.getLogger(ClusterRouteStats.class);

  private static final Map<String, ClusterRouteStats> perStoreClusterRouteStats = new VeniceConcurrentHashMap<>();

  private final Map<String, RouteStats> perRouteStatMap = new VeniceConcurrentHashMap<>();

  private final String storeName;

  // Obtaining the Singleton instance takes storeName as input.
  public static ClusterRouteStats getInstance(String storeName) {
    return perStoreClusterRouteStats.computeIfAbsent(storeName, ClusterRouteStats::new);
  }

  private ClusterRouteStats(String storeName) {
    this.storeName = storeName;
  }

  public RouteStats getRouteStats(
      MetricsRepository metricsRepository,
      String clusterName,
      String instanceUrl,
      RequestType requestType) {
    String combinedKey = clusterName + "-" + instanceUrl + "-" + requestType.toString();
    return perRouteStatMap.computeIfAbsent(combinedKey, ignored -> {
      String instanceName = instanceUrl;
      try {
        URL url = new URL(instanceUrl);
        instanceName = url.getHost();
      } catch (MalformedURLException e) {
        LOGGER.error("Invalid instance url: {}", instanceUrl);
      }
      return new RouteStats(metricsRepository, storeName, clusterName, instanceName, requestType);
    });
  }

  /**
   * Per-route request metrics.
   */
  public static class RouteStats extends AbstractVeniceHttpStats {
    private final Sensor requestCountSensor;

    // OpenTelemetry integration for pending request count
    private final MetricEntityStateBase pendingRequestCount;

    // OpenTelemetry integration for rejection ratio
    private final MetricEntityStateOneEnum<RejectionReason> rejectionRatio;

    // OpenTelemetry integration for healthy request count
    private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> healthyRequestCount;

    // OpenTelemetry integration for quota exceeded request count
    private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> quotaExceededRequestCount;

    // OpenTelemetry integration for internal server error request count
    private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> internalServerErrorRequestCount;

    // OpenTelemetry integration for leaked request count
    private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> leakedRequestCount;

    // OpenTelemetry integration for service unavailable request count
    private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> serviceUnavailableRequestCount;

    // OpenTelemetry integration for other error request count
    private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> otherErrorRequestCount;

    // OpenTelemetry integration for response waiting time
    private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> responseWaitingTime;

    // OTel support
    private final VeniceOpenTelemetryMetricsRepository otelRepository;
    private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;
    private final Attributes baseAttributes;

    public RouteStats(
        MetricsRepository metricsRepository,
        String storeName,
        String clusterName,
        String instanceName,
        RequestType requestType) {
      super(metricsRepository, clusterName + "." + StatsUtils.convertHostnameToMetricName(instanceName), requestType);

      String routeName = StatsUtils.convertHostnameToMetricName(instanceName);
      OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
          OpenTelemetryMetricsSetup.builder(metricsRepository)
              // set all base dimensions for this stats class and build
              .setStoreName(storeName)
              .setClusterName(clusterName)
              .setRequestType(requestType)
              .setRouteName(routeName)
              .build();

      this.otelRepository = otelData.getOtelRepository();
      this.baseDimensionsMap = otelData.getBaseDimensionsMap();
      this.baseAttributes = otelData.getBaseAttributes();

      // Initialize traditional Tehuti sensors
      this.requestCountSensor = registerSensor("request_count", new OccurrenceRate());

      // Initialize OpenTelemetry metric for pending request count using ROUTE_REQUEST_PENDING_COUNT
      this.pendingRequestCount = MetricEntityStateBase.create(
          ROUTE_REQUEST_PENDING_COUNT.getMetricEntity(),
          otelRepository,
          this::registerSensor,
          RouteTehutiMetricName.PENDING_REQUEST_COUNT,
          Arrays.asList(new Avg(), new Max()),
          baseDimensionsMap,
          baseAttributes);

      // Initialize OpenTelemetry metric for rejection ratio using ROUTE_REQUEST_REJECTION_RATIO
      this.rejectionRatio = MetricEntityStateOneEnum.create(
          ROUTE_REQUEST_REJECTION_RATIO.getMetricEntity(),
          otelRepository,
          this::registerSensor,
          RouteTehutiMetricName.REJECTION_RATIO,
          Arrays.asList(new Avg(), new Max()),
          baseDimensionsMap,
          RejectionReason.class);

      // Initialize OpenTelemetry metric for healthy request count using ROUTE_CALL_COUNT
      this.healthyRequestCount = MetricEntityStateThreeEnums.create(
          ROUTE_CALL_COUNT.getMetricEntity(),
          otelRepository,
          this::registerSensor,
          RouteTehutiMetricName.HEALTHY_REQUEST_COUNT,
          Collections.singletonList(new OccurrenceRate()),
          baseDimensionsMap,
          HttpResponseStatusEnum.class,
          HttpResponseStatusCodeCategory.class,
          VeniceResponseStatusCategory.class);

      // Initialize OpenTelemetry metric for quota exceeded request count using ROUTE_CALL_COUNT
      this.quotaExceededRequestCount = MetricEntityStateThreeEnums.create(
          ROUTE_CALL_COUNT.getMetricEntity(),
          otelRepository,
          this::registerSensor,
          RouteTehutiMetricName.QUOTA_EXCEEDED_REQUEST_COUNT,
          Collections.singletonList(new OccurrenceRate()),
          baseDimensionsMap,
          HttpResponseStatusEnum.class,
          HttpResponseStatusCodeCategory.class,
          VeniceResponseStatusCategory.class);

      // Initialize OpenTelemetry metric for internal server error request count using ROUTE_CALL_COUNT
      this.internalServerErrorRequestCount = MetricEntityStateThreeEnums.create(
          ROUTE_CALL_COUNT.getMetricEntity(),
          otelRepository,
          this::registerSensor,
          RouteTehutiMetricName.INTERNAL_SERVER_ERROR_REQUEST_COUNT,
          Collections.singletonList(new OccurrenceRate()),
          baseDimensionsMap,
          HttpResponseStatusEnum.class,
          HttpResponseStatusCodeCategory.class,
          VeniceResponseStatusCategory.class);

      // Initialize OpenTelemetry metric for leaked request count using ROUTE_CALL_COUNT
      this.leakedRequestCount = MetricEntityStateThreeEnums.create(
          ROUTE_CALL_COUNT.getMetricEntity(),
          otelRepository,
          this::registerSensor,
          RouteTehutiMetricName.LEAKED_REQUEST_COUNT,
          Collections.singletonList(new OccurrenceRate()),
          baseDimensionsMap,
          HttpResponseStatusEnum.class,
          HttpResponseStatusCodeCategory.class,
          VeniceResponseStatusCategory.class);

      // Initialize OpenTelemetry metric for service unavailable request count using ROUTE_CALL_COUNT
      this.serviceUnavailableRequestCount = MetricEntityStateThreeEnums.create(
          ROUTE_CALL_COUNT.getMetricEntity(),
          otelRepository,
          this::registerSensor,
          RouteTehutiMetricName.SERVICE_UNAVAILABLE_REQUEST_COUNT,
          Collections.singletonList(new OccurrenceRate()),
          baseDimensionsMap,
          HttpResponseStatusEnum.class,
          HttpResponseStatusCodeCategory.class,
          VeniceResponseStatusCategory.class);

      // Initialize OpenTelemetry metric for other error request count using ROUTE_CALL_COUNT
      this.otherErrorRequestCount = MetricEntityStateThreeEnums.create(
          ROUTE_CALL_COUNT.getMetricEntity(),
          otelRepository,
          this::registerSensor,
          RouteTehutiMetricName.OTHER_ERROR_REQUEST_COUNT,
          Collections.singletonList(new OccurrenceRate()),
          baseDimensionsMap,
          HttpResponseStatusEnum.class,
          HttpResponseStatusCodeCategory.class,
          VeniceResponseStatusCategory.class);

      // Initialize OpenTelemetry metric for response waiting time using ROUTE_CALL_TIME
      this.responseWaitingTime = MetricEntityStateThreeEnums.create(
          ROUTE_CALL_TIME.getMetricEntity(),
          otelRepository,
          this::registerSensor,
          RouteTehutiMetricName.RESPONSE_WAITING_TIME,
          Collections
              .singletonList(TehutiUtils.getPercentileStat(getName(), getFullMetricName("response_waiting_time"))),
          baseDimensionsMap,
          HttpResponseStatusEnum.class,
          HttpResponseStatusCodeCategory.class,
          VeniceResponseStatusCategory.class);
    }

    public void recordRequest() {
      requestCountSensor.record();
    }

    public void recordHealthyRequest(
        double latency,
        HttpResponseStatusEnum httpStatus,
        HttpResponseStatusCodeCategory codeCategory) {
      healthyRequestCount.record(1, httpStatus, codeCategory, VeniceResponseStatusCategory.SUCCESS);
      responseWaitingTime.record(latency, httpStatus, codeCategory, VeniceResponseStatusCategory.SUCCESS);
    }

    public void recordQuotaExceededRequest(
        double latency,
        HttpResponseStatusEnum httpStatus,
        HttpResponseStatusCodeCategory codeCategory) {
      quotaExceededRequestCount.record(1, httpStatus, codeCategory, VeniceResponseStatusCategory.FAIL);
      responseWaitingTime.record(latency, httpStatus, codeCategory, VeniceResponseStatusCategory.FAIL);
    }

    public void recordInternalServerErrorRequest(
        double latency,
        HttpResponseStatusEnum httpStatus,
        HttpResponseStatusCodeCategory codeCategory) {
      internalServerErrorRequestCount.record(1, httpStatus, codeCategory, VeniceResponseStatusCategory.FAIL);
      responseWaitingTime.record(latency, httpStatus, codeCategory, VeniceResponseStatusCategory.FAIL);
    }

    public void recordServiceUnavailableRequest(
        double latency,
        HttpResponseStatusEnum httpStatus,
        HttpResponseStatusCodeCategory codeCategory) {
      serviceUnavailableRequestCount.record(1, httpStatus, codeCategory, VeniceResponseStatusCategory.FAIL);
      responseWaitingTime.record(latency, httpStatus, codeCategory, VeniceResponseStatusCategory.FAIL);
    }

    public void recordLeakedRequest(
        double latency,
        HttpResponseStatusEnum httpStatus,
        HttpResponseStatusCodeCategory codeCategory) {
      leakedRequestCount.record(1, httpStatus, codeCategory, VeniceResponseStatusCategory.FAIL);
      responseWaitingTime.record(latency, httpStatus, codeCategory, VeniceResponseStatusCategory.FAIL);
    }

    public void recordOtherErrorRequest(double latency) {
      otherErrorRequestCount
          .record(1, UNKNOWN, HttpResponseStatusCodeCategory.UNKNOWN, VeniceResponseStatusCategory.FAIL);
      responseWaitingTime
          .record(latency, UNKNOWN, HttpResponseStatusCodeCategory.UNKNOWN, VeniceResponseStatusCategory.FAIL);
    }

    public void recordPendingRequestCount(int count) {
      pendingRequestCount.record(count);
    }

    public void recordRejectionRatio(double rejectionRatio, RejectionReason reason) {
      // Record to OpenTelemetry metric
      this.rejectionRatio.record(rejectionRatio, reason);
    }
  }

  /**
   * Metric names for tehuti metrics used in RouteStats.
   */
  public enum RouteTehutiMetricName implements TehutiMetricNameEnum {
    HEALTHY_REQUEST_COUNT, QUOTA_EXCEEDED_REQUEST_COUNT, INTERNAL_SERVER_ERROR_REQUEST_COUNT, LEAKED_REQUEST_COUNT,
    SERVICE_UNAVAILABLE_REQUEST_COUNT, OTHER_ERROR_REQUEST_COUNT, RESPONSE_WAITING_TIME, PENDING_REQUEST_COUNT,
    REJECTION_RATIO
  }
}
