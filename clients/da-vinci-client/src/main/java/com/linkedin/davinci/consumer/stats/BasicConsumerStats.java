package com.linkedin.davinci.consumer.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory.SUCCESS;
import static com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface.getUniqueMetricEntities;
import static com.linkedin.venice.utils.CollectionUtils.setOf;

import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Gauge;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Rate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;


/**
 *
 */
public class BasicConsumerStats extends AbstractVeniceStats {
  public static final Collection<MetricEntity> CONSUMER_METRIC_ENTITIES =
      getUniqueMetricEntities(BasicConsumerMetricEntity.class);

  private final Attributes baseAttributes;

  private final MetricEntityStateBase heartBeatDelayMetric;
  private final MetricEntityStateBase minimumConsumingVersionMetric;
  private final MetricEntityStateBase maximumConsumingVersionMetric;
  private final MetricEntityStateBase recordsConsumedCountMetric;
  private final MetricEntityStateOneEnum<VeniceResponseStatusCategory> pollSuccessCountMetric;
  private final MetricEntityStateOneEnum<VeniceResponseStatusCategory> pollFailCountMetric;
  private final MetricEntityStateOneEnum<VeniceResponseStatusCategory> versionSwapSuccessCountMetric;
  private final MetricEntityStateOneEnum<VeniceResponseStatusCategory> versionSwapFailCountMetric;
  private final MetricEntityStateOneEnum<VeniceResponseStatusCategory> chunkedRecordSuccessCountMetric;
  private final MetricEntityStateOneEnum<VeniceResponseStatusCategory> chunkedRecordFailCountMetric;

  private final AtomicInteger versionSwapSuccessCount = new AtomicInteger();
  private final AtomicInteger versionSwapFailCount = new AtomicInteger();

  public BasicConsumerStats(MetricsRepository metricsRepository, String consumerName, String storeName) {
    super(metricsRepository, consumerName);

    boolean emitOpenTelemetryMetrics;
    VeniceOpenTelemetryMetricsRepository otelRepository;
    Map<VeniceMetricsDimensions, String> baseDimensionsMap;

    if (metricsRepository instanceof VeniceMetricsRepository) {
      VeniceMetricsRepository veniceMetricsRepository = (VeniceMetricsRepository) metricsRepository;
      VeniceMetricsConfig veniceMetricsConfig = veniceMetricsRepository.getVeniceMetricsConfig();
      emitOpenTelemetryMetrics = veniceMetricsConfig.emitOtelMetrics();
      if (emitOpenTelemetryMetrics) {
        otelRepository = veniceMetricsRepository.getOpenTelemetryMetricsRepository();
        baseDimensionsMap = new HashMap<>();
        baseDimensionsMap.put(VENICE_STORE_NAME, storeName);
        AttributesBuilder baseAttributesBuilder = Attributes.builder();
        baseAttributesBuilder.put(otelRepository.getDimensionName(VENICE_STORE_NAME), storeName);
        baseAttributes = baseAttributesBuilder.build();
      } else {
        otelRepository = null;
        baseDimensionsMap = null;
        baseAttributes = null;
      }
    } else {
      otelRepository = null;
      baseDimensionsMap = null;
      baseAttributes = null;
    }

    heartBeatDelayMetric = MetricEntityStateBase.create(
        BasicConsumerMetricEntity.HEART_BEAT_DELAY.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        BasicConsumerTehutiMetricName.MAX_PARTITION_LAG,
        Collections.singletonList(new Max()),
        baseDimensionsMap,
        baseAttributes);

    minimumConsumingVersionMetric = MetricEntityStateBase.create(
        BasicConsumerMetricEntity.CURRENT_CONSUMING_VERSION.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        BasicConsumerTehutiMetricName.MINIMUM_CONSUMING_VERSION,
        Collections.singletonList(new Gauge()),
        baseDimensionsMap,
        baseAttributes);

    maximumConsumingVersionMetric = MetricEntityStateBase.create(
        BasicConsumerMetricEntity.CURRENT_CONSUMING_VERSION.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        BasicConsumerTehutiMetricName.MAXIMUM_CONSUMING_VERSION,
        Collections.singletonList(new Gauge()),
        baseDimensionsMap,
        baseAttributes);

    recordsConsumedCountMetric = MetricEntityStateBase.create(
        BasicConsumerMetricEntity.RECORDS_CONSUMED_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        BasicConsumerTehutiMetricName.RECORDS_CONSUMED,
        Arrays.asList(new Avg(), new Max(), new Rate()),
        baseDimensionsMap,
        baseAttributes);

    pollSuccessCountMetric = MetricEntityStateOneEnum.create(
        BasicConsumerMetricEntity.POLL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        BasicConsumerTehutiMetricName.POLL_SUCCESS_COUNT,
        Collections.singletonList(new Rate()),
        baseDimensionsMap,
        VeniceResponseStatusCategory.class);

    pollFailCountMetric = MetricEntityStateOneEnum.create(
        BasicConsumerMetricEntity.POLL_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        BasicConsumerTehutiMetricName.POLL_FAIL_COUNT,
        Collections.singletonList(new Rate()),
        baseDimensionsMap,
        VeniceResponseStatusCategory.class);

    versionSwapSuccessCountMetric = MetricEntityStateOneEnum.create(
        BasicConsumerMetricEntity.VERSION_SWAP_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        BasicConsumerTehutiMetricName.VERSION_SWAP_SUCCESS_COUNT,
        Collections.singletonList(new Gauge()),
        baseDimensionsMap,
        VeniceResponseStatusCategory.class);

    versionSwapFailCountMetric = MetricEntityStateOneEnum.create(
        BasicConsumerMetricEntity.VERSION_SWAP_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        BasicConsumerTehutiMetricName.VERSION_SWAP_FAIL_COUNT,
        Collections.singletonList(new Gauge()),
        baseDimensionsMap,
        VeniceResponseStatusCategory.class);

    chunkedRecordSuccessCountMetric = MetricEntityStateOneEnum.create(
        BasicConsumerMetricEntity.CHUNKED_RECORD_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        BasicConsumerTehutiMetricName.CHUNKED_RECORD_SUCCESS_COUNT,
        Collections.singletonList(new Rate()),
        baseDimensionsMap,
        VeniceResponseStatusCategory.class);

    chunkedRecordFailCountMetric = MetricEntityStateOneEnum.create(
        BasicConsumerMetricEntity.CHUNKED_RECORD_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensor,
        BasicConsumerTehutiMetricName.CHUNKED_RECORD_FAIL_COUNT,
        Collections.singletonList(new Rate()),
        baseDimensionsMap,
        VeniceResponseStatusCategory.class);
  }

  public void emitCurrentConsumingVersionMetrics(int minVersion, int maxVersion) {
    minimumConsumingVersionMetric.record(minVersion);
    maximumConsumingVersionMetric.record(maxVersion);
  }

  /**
   * This won't be emitted by DVRT CDC, since it doesn't have context into heartbeat delay.
   */
  public void emitHeartBeatDelayMetrics(long heartBeatDelay) {
    heartBeatDelayMetric.record(heartBeatDelay);
  }

  public void emitRecordsConsumedCountMetrics(int count) {
    recordsConsumedCountMetric.record(count);
  }

  public void emitPollCountMetrics(VeniceResponseStatusCategory responseStatusCategory) {
    if (responseStatusCategory == SUCCESS) {
      pollSuccessCountMetric.record(1, responseStatusCategory);
    } else {
      pollFailCountMetric.record(1, responseStatusCategory);
    }
  }

  public void emitVersionSwapCountMetrics(VeniceResponseStatusCategory responseStatusCategory) {
    if (responseStatusCategory == SUCCESS) {
      versionSwapSuccessCountMetric.record(versionSwapSuccessCount.incrementAndGet(), responseStatusCategory);
    } else {
      versionSwapFailCountMetric.record(versionSwapFailCount.incrementAndGet(), responseStatusCategory);
    }
  }

  /**
   * This won't be emitted by DVRT CDC, since it doesn't have context into chunked records.
   */
  public void emitChunkedRecordCountMetrics(VeniceResponseStatusCategory responseStatusCategory) {
    if (responseStatusCategory == SUCCESS) {
      chunkedRecordSuccessCountMetric.record(1, responseStatusCategory);
    } else {
      chunkedRecordFailCountMetric.record(1, responseStatusCategory);
    }
  }

  @VisibleForTesting
  public Attributes getBaseAttributes() {
    return baseAttributes;
  }

  /**
   * Metric names for tehuti metrics used in this class.
   */
  public enum BasicConsumerTehutiMetricName implements TehutiMetricNameEnum {
    MAX_PARTITION_LAG, RECORDS_CONSUMED, MINIMUM_CONSUMING_VERSION, MAXIMUM_CONSUMING_VERSION, POLL_SUCCESS_COUNT,
    POLL_FAIL_COUNT, VERSION_SWAP_SUCCESS_COUNT, VERSION_SWAP_FAIL_COUNT, CHUNKED_RECORD_SUCCESS_COUNT,
    CHUNKED_RECORD_FAIL_COUNT;

    private final String metricName;

    BasicConsumerTehutiMetricName() {
      this.metricName = name().toLowerCase();
    }

    @Override
    public String getMetricName() {
      return this.metricName;
    }
  }

  public enum BasicConsumerMetricEntity implements ModuleMetricEntityInterface {
    /**
     * Measures the max heartbeat delay across all subscribed partitions
     */
    HEART_BEAT_DELAY(
        MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.MILLISECOND,
        "Measures the max heartbeat delay across all subscribed partitions", setOf(VENICE_STORE_NAME)
    ),
    /**
     * Measures the min/max consuming version across all subscribed partitions
     */
    CURRENT_CONSUMING_VERSION(
        MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER,
        "Measures the min/max consuming version across all subscribed partitions", setOf(VENICE_STORE_NAME)
    ),
    /**
     * Measures the count of records consumed
     */
    RECORDS_CONSUMED_COUNT(
        MetricType.COUNTER, MetricUnit.NUMBER, "Measures the count of records consumed", setOf(VENICE_STORE_NAME)
    ),
    /**
     * Measures the count of poll invocations
     */
    POLL_COUNT(
        MetricType.COUNTER, MetricUnit.NUMBER, "Measures the count of poll invocations",
        setOf(VENICE_STORE_NAME, VENICE_RESPONSE_STATUS_CODE_CATEGORY)
    ),
    /**
     * Measures the count of version swaps
     */
    VERSION_SWAP_COUNT(
        MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER, "Measures the count of version swaps",
        setOf(VENICE_STORE_NAME, VENICE_RESPONSE_STATUS_CODE_CATEGORY)
    ),
    /**
     * Measures the count of chunked records consumed
     */
    CHUNKED_RECORD_COUNT(
        MetricType.COUNTER, MetricUnit.NUMBER, "Measures the count of chunked records consumed",
        setOf(VENICE_STORE_NAME, VENICE_RESPONSE_STATUS_CODE_CATEGORY)
    );

    private final MetricEntity metricEntity;

    BasicConsumerMetricEntity(
        MetricType metricType,
        MetricUnit unit,
        String description,
        Set<VeniceMetricsDimensions> dimensions) {
      this.metricEntity = new MetricEntity(this.name().toLowerCase(), metricType, unit, description, dimensions);
    }

    @Override
    public MetricEntity getMetricEntity() {
      return metricEntity;
    }
  }
}
