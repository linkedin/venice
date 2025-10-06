package com.linkedin.venice.client.stats;

import static com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory.getVeniceHttpResponseStatusCodeCategory;
import static com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum.transformIntToHttpResponseStatusEnum;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.FAIL;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.SUCCESS;
import static com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface.getUniqueMetricEntities;
import static com.linkedin.venice.utils.CollectionUtils.setOf;
import static org.apache.hc.core5.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.hc.core5.http.HttpStatus.SC_OK;

import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AbstractVeniceHttpStats;
import com.linkedin.venice.stats.ClientType;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.MetricEntityStateThreeEnums;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Rate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.http.HttpStatus;


/**
 * This class offers some base metrics for clients currently used
 * by thin, fast and DaVinci clients.
 */
public class BasicClientStats extends AbstractVeniceHttpStats {
  public static final Collection<MetricEntity> CLIENT_METRIC_ENTITIES =
      getUniqueMetricEntities(BasicClientMetricEntity.class, ClientMetricEntity.class);

  private static final String SYSTEM_STORE_NAME_PREFIX = "venice_system_store_";

  private static final MetricsRepository dummySystemStoreMetricRepo = new MetricsRepository();

  protected final Map<VeniceMetricsDimensions, String> baseDimensionsMap;
  protected final Attributes baseAttributes;

  private final Sensor requestSensor; // will be a derived metric in otel: healthy + unhealthy
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> healthyRequestMetric;
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> unhealthyRequestMetric;
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> healthyLatencyMetric;
  private final MetricEntityStateThreeEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, VeniceResponseStatusCategory> unhealthyLatencyMetric;
  private final MetricEntityStateOneEnum<VeniceResponseStatusCategory> healthyRequestMetricForDavinciClient;
  private final MetricEntityStateOneEnum<VeniceResponseStatusCategory> unhealthyRequestMetricForDavinciClient;
  private final MetricEntityStateOneEnum<VeniceResponseStatusCategory> healthyLatencyMetricForDavinciClient;
  private final MetricEntityStateOneEnum<VeniceResponseStatusCategory> unhealthyLatencyMetricForDavinciClient;
  private final MetricEntityStateBase requestKeyCount;
  private final MetricEntityStateBase successResponseKeyCount;
  private final Sensor successRequestRatioSensor;
  private final Sensor successRequestKeyRatioSensor;
  private final Rate requestRate = new OccurrenceRate();
  private final Rate successRequestKeyCountRate = new Rate();
  protected final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final boolean emitOpenTelemetryMetrics;
  protected final ClientType clientType;

  public static BasicClientStats getClientStats(
      MetricsRepository metricsRepository,
      String storeName,
      RequestType requestType,
      ClientConfig clientConfig,
      ClientType clientType) {
    String prefix = clientConfig == null ? null : clientConfig.getStatsPrefix();
    String metricName = prefix == null || prefix.isEmpty() ? storeName : prefix + "." + storeName;
    return new BasicClientStats(metricsRepository, metricName, requestType, clientType);
  }

  protected BasicClientStats(
      MetricsRepository metricsRepository,
      String storeName,
      RequestType requestType,
      ClientType clientType) {
    super(
        storeName.startsWith(SYSTEM_STORE_NAME_PREFIX) ? dummySystemStoreMetricRepo : metricsRepository,
        storeName,
        requestType);

    this.clientType = clientType;

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository)
            .isTotalStats(isTotalStats())
            // set all base dimensions for this stats class and build
            .setStoreName(storeName)
            .setRequestType(requestType)
            .build();

    this.emitOpenTelemetryMetrics = otelData.emitOpenTelemetryMetrics();
    this.otelRepository = otelData.getOtelRepository();
    this.baseDimensionsMap = otelData.getBaseDimensionsMap();
    this.baseAttributes = otelData.getBaseAttributes();

    // QPS
    // requestSensor will be a derived metric in OTel
    requestSensor = registerSensor("request", requestRate);
    Rate healthyRequestRate = new OccurrenceRate();
    Rate requestKeyCountRate = new Rate();

    if (ClientType.isDavinciClient(clientType)) {
      healthyRequestMetricForDavinciClient = MetricEntityStateOneEnum.create(
          BasicClientMetricEntity.CALL_COUNT_DVC.getMetricEntity(),
          otelRepository,
          this::registerSensor,
          BasicClientTehutiMetricName.HEALTHY_REQUEST,
          Collections.singletonList(healthyRequestRate),
          baseDimensionsMap,
          VeniceResponseStatusCategory.class);
      unhealthyRequestMetricForDavinciClient = MetricEntityStateOneEnum.create(
          BasicClientMetricEntity.CALL_COUNT_DVC.getMetricEntity(),
          otelRepository,
          this::registerSensor,
          BasicClientTehutiMetricName.UNHEALTHY_REQUEST,
          Collections.singletonList(new OccurrenceRate()),
          baseDimensionsMap,
          VeniceResponseStatusCategory.class);

      // latency
      healthyLatencyMetricForDavinciClient = MetricEntityStateOneEnum.create(
          BasicClientMetricEntity.CALL_TIME_DVC.getMetricEntity(),
          otelRepository,
          this::registerSensorWithDetailedPercentiles,
          BasicClientTehutiMetricName.HEALTHY_REQUEST_LATENCY,
          Arrays.asList(
              new Avg(),
              TehutiUtils.getPercentileStatForNetworkLatency(
                  getName(),
                  getFullMetricName(BasicClientTehutiMetricName.HEALTHY_REQUEST_LATENCY.getMetricName()))),
          baseDimensionsMap,
          VeniceResponseStatusCategory.class);
      unhealthyLatencyMetricForDavinciClient = MetricEntityStateOneEnum.create(
          BasicClientMetricEntity.CALL_TIME_DVC.getMetricEntity(),
          getOtelRepository(),
          this::registerSensorWithDetailedPercentiles,
          BasicClientTehutiMetricName.UNHEALTHY_REQUEST_LATENCY,
          Arrays.asList(
              new Avg(),
              TehutiUtils.getPercentileStatForNetworkLatency(
                  getName(),
                  getFullMetricName(BasicClientTehutiMetricName.UNHEALTHY_REQUEST_LATENCY.getMetricName()))),
          getBaseDimensionsMap(),
          VeniceResponseStatusCategory.class);

      healthyRequestMetric = null;
      unhealthyRequestMetric = null;
      healthyLatencyMetric = null;
      unhealthyLatencyMetric = null;
    } else {
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
      healthyRequestMetricForDavinciClient = null;
      unhealthyRequestMetricForDavinciClient = null;
      healthyLatencyMetricForDavinciClient = null;
      unhealthyLatencyMetricForDavinciClient = null;
    }

    // request key count
    requestKeyCount = MetricEntityStateBase.create(
        BasicClientMetricEntity.REQUEST_KEY_COUNT.getMetricEntity(),
        getOtelRepository(),
        this::registerSensor,
        BasicClientTehutiMetricName.REQUEST_KEY_COUNT,
        Arrays.asList(requestKeyCountRate, new Avg(), new Max()),
        baseDimensionsMap,
        baseAttributes);

    successResponseKeyCount = MetricEntityStateBase.create(
        BasicClientMetricEntity.RESPONSE_KEY_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        BasicClientTehutiMetricName.SUCCESS_REQUEST_KEY_COUNT,
        Arrays.asList(successRequestKeyCountRate, new Avg(), new Max()),
        baseDimensionsMap,
        baseAttributes);

    // successRequestRatioSensor will be a derived metric in OTel
    successRequestRatioSensor =
        registerSensor(new TehutiUtils.SimpleRatioStat(healthyRequestRate, requestRate, "success_request_ratio"));

    successRequestKeyRatioSensor = registerSensor(
        new TehutiUtils.SimpleRatioStat(successRequestKeyCountRate, requestKeyCountRate, "success_request_key_ratio"));
  }

  private void recordRequest() {
    requestSensor.record();
  }

  public void emitHealthyRequestMetrics(double latency, int keyCount) {
    if (!clientType.equals(ClientType.DAVINCI_CLIENT)) {
      recordRequest();
      int httpStatus = getHealthyRequestHttpStatus(keyCount);
      HttpResponseStatusEnum statusEnum = transformIntToHttpResponseStatusEnum(httpStatus);
      HttpResponseStatusCodeCategory httpCategory = getVeniceHttpResponseStatusCodeCategory(httpStatus);
      healthyRequestMetric.record(1, statusEnum, httpCategory, SUCCESS);
      healthyLatencyMetric.record(latency, statusEnum, httpCategory, SUCCESS);
    }
  }

  public void emitHealthyRequestMetrics(double latency, Object value) {
    emitHealthyRequestMetrics(latency, getSuccessfulKeyCount(value));
  }

  public void emitUnhealthyRequestMetrics(double latency, int httpStatus) {
    if (!clientType.equals(ClientType.DAVINCI_CLIENT)) {
      recordRequest();
      HttpResponseStatusEnum statusEnum = transformIntToHttpResponseStatusEnum(httpStatus);
      HttpResponseStatusCodeCategory httpCategory = getVeniceHttpResponseStatusCodeCategory(httpStatus);
      unhealthyRequestMetric.record(1, statusEnum, httpCategory, FAIL);
      unhealthyLatencyMetric.record(latency, statusEnum, httpCategory, FAIL);
    }
  }

  public void emitUnhealthyRequestMetrics(double latency, Throwable throwable) {
    emitUnhealthyRequestMetrics(latency, getUnhealthyRequestHttpStatus(throwable));
  }

  public void emitHealthyRequestMetricsForDavinciClient(double latency) {
    if (clientType.equals(ClientType.DAVINCI_CLIENT)) {
      recordRequest();
      healthyRequestMetricForDavinciClient.record(1, SUCCESS);
      healthyLatencyMetricForDavinciClient.record(latency, SUCCESS);
    }
  }

  public void emitUnhealthyRequestMetricsForDavinciClient(double latency) {
    if (clientType.equals(ClientType.DAVINCI_CLIENT)) {
      recordRequest();
      unhealthyRequestMetricForDavinciClient.record(1, FAIL);
      unhealthyLatencyMetricForDavinciClient.record(latency, FAIL);
    }
  }

  public void recordRequestKeyCount(int keyCount) {
    requestKeyCount.record(keyCount);
  }

  public void recordResponseKeyCount(int keyCount) {
    successResponseKeyCount.record(keyCount);
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
  public static int getHealthyRequestHttpStatus(int numSuccessfulKeys) {
    return numSuccessfulKeys > 0 ? SC_OK : SC_NOT_FOUND;
  }

  /**
   * Get the number of successful keys from the value.
   * @param value
   * @return The number of successful keys.
   */
  public static int getSuccessfulKeyCount(Object value) {
    if (value instanceof Map) {
      return ((Map) value).size();
    }
    return (value != null) ? 1 : 0;
  }

  /**
   * Get the HTTP status code for unhealthy requests based on the throwable.
   *
   * @param throwable
   * @return {@link HttpStatus}
   */
  public static int getUnhealthyRequestHttpStatus(Throwable throwable) {
    if (throwable instanceof VeniceClientHttpException) {
      VeniceClientHttpException httpException = (VeniceClientHttpException) throwable;
      return httpException.getHttpStatus();
    } else {
      return HttpStatus.SC_INTERNAL_SERVER_ERROR;
    }
  }

  private VeniceOpenTelemetryMetricsRepository getOtelRepository() {
    return otelRepository;
  }

  private Map<VeniceMetricsDimensions, String> getBaseDimensionsMap() {
    return baseDimensionsMap;
  }

  protected Attributes getBaseAttributes() {
    return baseAttributes;
  }

  protected boolean emitOpenTelemetryMetrics() {
    return emitOpenTelemetryMetrics;
  }

  /**
   * Metric names for tehuti metrics used in this class.
   */
  public enum BasicClientTehutiMetricName implements TehutiMetricNameEnum {
    HEALTHY_REQUEST, UNHEALTHY_REQUEST, HEALTHY_REQUEST_LATENCY, UNHEALTHY_REQUEST_LATENCY, REQUEST_KEY_COUNT,
    SUCCESS_REQUEST_KEY_COUNT;

    private final String metricName;

    BasicClientTehutiMetricName() {
      this.metricName = name().toLowerCase();
    }

    @Override
    public String getMetricName() {
      return this.metricName;
    }
  }

  public enum BasicClientMetricEntity implements ModuleMetricEntityInterface {
    /**
     * Count of all requests during response handling along with response codes
     */
    CALL_COUNT(
        MetricType.COUNTER, MetricUnit.NUMBER,
        "Count of all requests during response handling along with response codes",
        setOf(
            VENICE_STORE_NAME,
            VENICE_REQUEST_METHOD,
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
            HTTP_RESPONSE_STATUS_CODE,
            HTTP_RESPONSE_STATUS_CODE_CATEGORY,
            VENICE_RESPONSE_STATUS_CODE_CATEGORY)
    ),
    /**
     * Count of keys for venice client request and response.
     */
    REQUEST_KEY_COUNT(
        "request.key_count", MetricType.HISTOGRAM, MetricUnit.NUMBER, "Count of keys for venice client request",
        setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)
    ),

    RESPONSE_KEY_COUNT(
        "response.key_count", MetricType.HISTOGRAM, MetricUnit.NUMBER, "Count of keys for venice client response",
        setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD)
    ),
    /**
     * Count of all DaVinci requests: as DaVinci is local reads, we do not track HTTP response codes
     * But keeping the same name call_count across all clients for consistency
     */
    CALL_COUNT_DVC(
        CALL_COUNT.name().toLowerCase(), MetricType.COUNTER, MetricUnit.NUMBER, "Count of all DaVinci Client requests",
        setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_RESPONSE_STATUS_CODE_CATEGORY)
    ),
    /**
     * Latency for all DaVinci Client responses
     */
    CALL_TIME_DVC(
        CALL_TIME.name().toLowerCase(), MetricType.HISTOGRAM, MetricUnit.MILLISECOND,
        "Latency for all DaVinci Client responses",
        setOf(VENICE_STORE_NAME, VENICE_REQUEST_METHOD, VENICE_RESPONSE_STATUS_CODE_CATEGORY)
    );

    private final MetricEntity metricEntity;

    BasicClientMetricEntity(
        MetricType metricType,
        MetricUnit unit,
        String description,
        Set<VeniceMetricsDimensions> dimensions) {
      this.metricEntity = new MetricEntity(this.name().toLowerCase(), metricType, unit, description, dimensions);
    }

    BasicClientMetricEntity(
        String metricName,
        MetricType metricType,
        MetricUnit unit,
        String description,
        Set<VeniceMetricsDimensions> dimensions) {
      this.metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensions);
    }

    @Override
    public MetricEntity getMetricEntity() {
      return metricEntity;
    }
  }
}
