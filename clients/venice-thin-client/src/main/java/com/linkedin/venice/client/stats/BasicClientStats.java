package com.linkedin.venice.client.stats;

import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.getVeniceHttpResponseStatusCodeCategory;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum.transformIntToHttpResponseStatusEnum;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLIENT_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.CollectionUtils.setOf;
import static org.apache.hc.core5.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.hc.core5.http.HttpStatus.SC_OK;

import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AbstractVeniceHttpStats;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.dimensions.VeniceClientType;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateThreeEnums;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Rate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.http.HttpStatus;


/**
 * This class offers some base metrics for clients currently used
 * by thin, fast and DaVinci clients.
 */
public class BasicClientStats extends AbstractVeniceHttpStats {
  public static final String CLIENT_SERVICE_NAME = "venice-client";
  public static final String CLIENT_METRIC_PREFIX = "client";
  public static final Collection<MetricEntity> CLIENT_METRIC_ENTITIES = Collections.unmodifiableList(
      Arrays.stream(BasicClientMetricEntity.values())
          .map(BasicClientMetricEntity::getMetricEntity)
          .collect(Collectors.toList()));

  private static final String SYSTEM_STORE_NAME_PREFIX = "venice_system_store_";

  private static final MetricsRepository dummySystemStoreMetricRepo = new MetricsRepository();

  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;

  private final Sensor requestSensor; // will be a derived metric in otel: healthy + unhealthy
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> healthyRequestMetric;
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> unhealthyRequestMetric;
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> healthyLatencyMetric;
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> unhealthyLatencyMetric;
  private final Sensor requestKeyCountSensor;
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> healthyKeyCountMetric;
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> unhealthyKeyCountMetric;
  private final Sensor successRequestRatioSensor;
  private final Sensor successRequestKeyRatioSensor;
  private final Rate requestRate = new OccurrenceRate();
  private final Rate successRequestKeyCountRate = new Rate();
  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final boolean emitOpenTelemetryMetrics;

  public static BasicClientStats getClientStats(
      MetricsRepository metricsRepository,
      String storeName,
      RequestType requestType,
      ClientConfig clientConfig,
      VeniceClientType clientType) {
    String prefix = clientConfig == null ? null : clientConfig.getStatsPrefix();
    String metricName = prefix == null || prefix.isEmpty() ? storeName : prefix + "." + storeName;
    return new BasicClientStats(metricsRepository, metricName, requestType, clientType);
  }

  protected BasicClientStats(
      MetricsRepository metricsRepository,
      String storeName,
      RequestType requestType,
      VeniceClientType clientType) {
    super(
        storeName.startsWith(SYSTEM_STORE_NAME_PREFIX) ? dummySystemStoreMetricRepo : metricsRepository,
        storeName,
        requestType);

    if (metricsRepository instanceof VeniceMetricsRepository) {
      VeniceMetricsRepository veniceMetricsRepository = (VeniceMetricsRepository) metricsRepository;
      VeniceMetricsConfig veniceMetricsConfig = veniceMetricsRepository.getVeniceMetricsConfig();
      emitOpenTelemetryMetrics = veniceMetricsConfig.emitOtelMetrics() && !isTotalStats();
      if (emitOpenTelemetryMetrics) {
        otelRepository = veniceMetricsRepository.getOpenTelemetryMetricsRepository();
        baseDimensionsMap = new HashMap<>();
        baseDimensionsMap.put(VENICE_STORE_NAME, storeName);
        baseDimensionsMap.put(VENICE_REQUEST_METHOD, requestType.getDimensionValue());
        baseDimensionsMap.put(VENICE_CLIENT_TYPE, clientType.getDimensionValue());
      } else {
        otelRepository = null;
        baseDimensionsMap = null;
      }
    } else {
      emitOpenTelemetryMetrics = false;
      otelRepository = null;
      baseDimensionsMap = null;
    }

    // QPS
    // requestSensor will be a derived metric in OTel
    requestSensor = registerSensor("request", requestRate);
    Rate healthyRequestRate = new OccurrenceRate();

    healthyRequestMetric = MetricEntityStateThreeEnums.create(
        BasicClientMetricEntity.CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        BasicClientTehutiMetricName.HEALTHY_REQUEST,
        Collections.singletonList(healthyRequestRate),
        baseDimensionsMap,
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);

    unhealthyRequestMetric = MetricEntityStateThreeEnums.create(
        BasicClientMetricEntity.CALL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        BasicClientTehutiMetricName.UNHEALTHY_REQUEST,
        Collections.singletonList(new OccurrenceRate()),
        baseDimensionsMap,
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);

    // successRequestRatioSensor will be a derived metric in OTel
    successRequestRatioSensor =
        registerSensor(new TehutiUtils.SimpleRatioStat(healthyRequestRate, requestRate, "success_request_ratio"));

    // latency
    healthyLatencyMetric = MetricEntityStateThreeEnums.create(
        BasicClientMetricEntity.CALL_TIME.getMetricEntity(),
        otelRepository,
        this::registerSensorWithDetailedPercentiles,
        BasicClientTehutiMetricName.HEALTHY_REQUEST_LATENCY,
        Arrays.asList(
            new Avg(),
            TehutiUtils.getPercentileStatForNetworkLatency(
                getName(),
                getFullMetricName(BasicClientTehutiMetricName.HEALTHY_REQUEST_LATENCY.getMetricName()))),
        baseDimensionsMap,
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);

    unhealthyLatencyMetric = MetricEntityStateThreeEnums.create(
        BasicClientMetricEntity.CALL_TIME.getMetricEntity(),
        getOtelRepository(),
        this::registerSensorWithDetailedPercentiles,
        BasicClientTehutiMetricName.UNHEALTHY_REQUEST_LATENCY,
        Arrays.asList(
            new Avg(),
            TehutiUtils.getPercentileStatForNetworkLatency(
                getName(),
                getFullMetricName(BasicClientTehutiMetricName.UNHEALTHY_REQUEST_LATENCY.getMetricName()))),
        getBaseDimensionsMap(),
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);

    // key count
    Rate requestKeyCountRate = new Rate();
    // requestKeyCountSensor will be a derived metric in OTel
    requestKeyCountSensor = registerSensor("request_key_count", requestKeyCountRate, new Avg(), new Max());

    healthyKeyCountMetric = MetricEntityStateThreeEnums.create(
        BasicClientMetricEntity.KEY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        BasicClientTehutiMetricName.SUCCESS_REQUEST_KEY_COUNT,
        Arrays.asList(successRequestKeyCountRate, new Avg(), new Max()),
        baseDimensionsMap,
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);
    unhealthyKeyCountMetric = MetricEntityStateThreeEnums.create(
        BasicClientMetricEntity.KEY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        BasicClientTehutiMetricName.FAILED_REQUEST_KEY_COUNT,
        Arrays.asList(new Rate(), new Avg(), new Max()),
        baseDimensionsMap,
        HttpResponseStatusEnum.class,
        HttpResponseStatusCodeCategory.class,
        VeniceResponseStatusCategory.class);

    successRequestKeyRatioSensor = registerSensor(
        new TehutiUtils.SimpleRatioStat(successRequestKeyCountRate, requestKeyCountRate, "success_request_key_ratio"));
  }

  private void recordRequest() {
    requestSensor.record();
  }

  public void recordHealthyRequest(int httpStatus) {
    recordRequest();
    healthyRequestMetric.record(
        1,
        transformIntToHttpResponseStatusEnum(httpStatus),
        getVeniceHttpResponseStatusCodeCategory(httpStatus),
        VeniceResponseStatusCategory.SUCCESS);
  }

  public void recordUnhealthyRequest(int httpStatus) {
    recordRequest();
    unhealthyRequestMetric.record(
        1,
        transformIntToHttpResponseStatusEnum(httpStatus),
        getVeniceHttpResponseStatusCodeCategory(httpStatus),
        VeniceResponseStatusCategory.FAIL);
  }

  public void recordHealthyLatency(double latency, int httpStatus) {
    healthyLatencyMetric.record(
        latency,
        transformIntToHttpResponseStatusEnum(httpStatus),
        getVeniceHttpResponseStatusCodeCategory(httpStatus),
        VeniceResponseStatusCategory.SUCCESS);
  }

  public void recordUnhealthyLatency(double latency, int httpStatus) {
    unhealthyLatencyMetric.record(
        latency,
        transformIntToHttpResponseStatusEnum(httpStatus),
        getVeniceHttpResponseStatusCodeCategory(httpStatus),
        VeniceResponseStatusCategory.FAIL);
  }

  public void recordRequestKeyCount(int keyCount) {
    requestKeyCountSensor.record(keyCount);
  }

  public void recordSuccessRequestKeyCount(int successKeyCount, int httpStatus) {
    healthyKeyCountMetric.record(
        successKeyCount,
        transformIntToHttpResponseStatusEnum(httpStatus),
        getVeniceHttpResponseStatusCodeCategory(httpStatus),
        VeniceResponseStatusCategory.SUCCESS);
  }

  public void recordFailedRequestKeyCount(int successKeyCount, int httpStatus) {
    unhealthyKeyCountMetric.record(
        successKeyCount,
        transformIntToHttpResponseStatusEnum(httpStatus),
        getVeniceHttpResponseStatusCodeCategory(httpStatus),
        VeniceResponseStatusCategory.FAIL);
  }

  protected final Rate getRequestRate() {
    return requestRate;
  }

  protected final Rate getSuccessRequestKeyCountRate() {
    return successRequestKeyCountRate;
  }

  /**
   * Get the HTTP status code for healthy requests based on the number of successful keys
   * as both null and valid values are considered healthy in Venice.
   *
   * @param numSuccessfulKeys
   * @return {@link HttpStatus}
   */
  public int getHealthyRequestHttpStatus(int numSuccessfulKeys) {
    return numSuccessfulKeys > 0 ? SC_OK : SC_NOT_FOUND;
  }

  /**
   * Get the HTTP status code for unhealthy requests based on the throwable.
   *
   * @param throwable
   * @return {@link HttpStatus}
   */
  public int getUnhealthyRequestHttpStatus(Throwable throwable) {
    if (throwable instanceof VeniceClientHttpException) {
      VeniceClientHttpException httpException = (VeniceClientHttpException) throwable;
      return httpException.getHttpStatus();
    } else {
      return HttpStatus.SC_INTERNAL_SERVER_ERROR;
    }
  }

  VeniceOpenTelemetryMetricsRepository getOtelRepository() {
    return otelRepository;
  }

  Map<VeniceMetricsDimensions, String> getBaseDimensionsMap() {
    return baseDimensionsMap;
  }

  /**
   * Metric names for tehuti metrics used in this class.
   */
  public enum BasicClientTehutiMetricName implements TehutiMetricNameEnum {
    HEALTHY_REQUEST, UNHEALTHY_REQUEST, HEALTHY_REQUEST_LATENCY, UNHEALTHY_REQUEST_LATENCY, SUCCESS_REQUEST_KEY_COUNT,
    FAILED_REQUEST_KEY_COUNT;

    private final String metricName;

    BasicClientTehutiMetricName() {
      this.metricName = name().toLowerCase();
    }

    @Override
    public String getMetricName() {
      return this.metricName;
    }
  }

  public enum BasicClientMetricEntity {
    /**
     * Count of all requests during response handling along with response codes
     */
    CALL_COUNT(
        MetricType.COUNTER, MetricUnit.NUMBER,
        "Count of all requests during response handling along with response codes",
        setOf(
            VENICE_STORE_NAME,
            VENICE_REQUEST_METHOD,
            VENICE_CLIENT_TYPE,
            HTTP_RESPONSE_STATUS_CODE,
            HTTP_RESPONSE_STATUS_CODE_CATEGORY,
            VENICE_RESPONSE_STATUS_CODE_CATEGORY)
    ),
    /**
     * Latency based on all responses
     */
    CALL_TIME(
        MetricType.HISTOGRAM, MetricUnit.MILLISECOND, "Latency based on all responses",
        setOf(
            VENICE_STORE_NAME,
            VENICE_REQUEST_METHOD,
            VENICE_CLIENT_TYPE,
            HTTP_RESPONSE_STATUS_CODE,
            HTTP_RESPONSE_STATUS_CODE_CATEGORY,
            VENICE_RESPONSE_STATUS_CODE_CATEGORY)
    ),
    /**
     * Count of keys during response handling along with response codes
     */
    KEY_COUNT(
        MetricType.HISTOGRAM, MetricUnit.NUMBER, "Count of keys during response handling along with response codes",
        setOf(
            VENICE_STORE_NAME,
            VENICE_REQUEST_METHOD,
            VENICE_CLIENT_TYPE,
            HTTP_RESPONSE_STATUS_CODE,
            HTTP_RESPONSE_STATUS_CODE_CATEGORY,
            VENICE_RESPONSE_STATUS_CODE_CATEGORY)
    );

    private final MetricEntity metricEntity;

    BasicClientMetricEntity(
        MetricType metricType,
        MetricUnit unit,
        String description,
        Set<VeniceMetricsDimensions> dimensions) {
      this.metricEntity = new MetricEntity(this.name().toLowerCase(), metricType, unit, description, dimensions);
    }

    public MetricEntity getMetricEntity() {
      return metricEntity;
    }
  }
}
