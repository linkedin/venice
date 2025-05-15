package com.linkedin.davinci.consumer.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface.getUniqueMetricEntities;
import static com.linkedin.venice.utils.CollectionUtils.setOf;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Gauge;
import io.tehuti.metrics.stats.Max;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 *
 */
public class BasicConsumerStats extends AbstractVeniceStats {
  public static final Collection<MetricEntity> CONSUMER_METRIC_ENTITIES =
      getUniqueMetricEntities(BasicConsumerMetricEntity.class);

  private final Attributes baseAttributes;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;

  private final MetricEntityStateBase heartBeatDelayMetric;

  private final Sensor recordsConsumed;
  private final Sensor maximumConsumingVersion;

  private final Sensor minimumConsumingVersion;

  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final boolean emitOpenTelemetryMetrics;

  public BasicConsumerStats(MetricsRepository metricsRepository, String name, String storeName) {
    super(metricsRepository, name);

    if (metricsRepository instanceof VeniceMetricsRepository) {
      VeniceMetricsRepository veniceMetricsRepository = (VeniceMetricsRepository) metricsRepository;
      VeniceMetricsConfig veniceMetricsConfig = veniceMetricsRepository.getVeniceMetricsConfig();
      emitOpenTelemetryMetrics = veniceMetricsConfig.emitOtelMetrics() && !isTotalStats();
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
      emitOpenTelemetryMetrics = false;
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

    recordsConsumed = registerSensor("records_consumed", new Avg(), new Max());
    maximumConsumingVersion = registerSensor("maximum_consuming_version", new Gauge());
    minimumConsumingVersion = registerSensor("minimum_consuming_version", new Gauge());
  }

  public void recordMaximumConsumingVersion(int version) {
    maximumConsumingVersion.record(version);
  }

  public void recordMinimumConsumingVersion(int version) {
    minimumConsumingVersion.record(version);
  }

  public void emitHeartBeatDelayMetrics(Long heartBeatDelay) {
    heartBeatDelayMetric.record(heartBeatDelay);
  }

  public void recordRecordsConsumed(int count) {
    recordsConsumed.record(count);
  }

  /**
   * Metric names for tehuti metrics used in this class.
   */
  public enum BasicConsumerTehutiMetricName implements TehutiMetricNameEnum {
    MAX_PARTITION_LAG, RECORDS_CONSUMED, MAXIMUM_CONSUMING_VERSION, MINIMUM_CONSUMING_VERSION;

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
        MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS, MetricUnit.NUMBER,
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
    POLL_CALL_COUNT(
        MetricType.COUNTER, MetricUnit.NUMBER, "Measures the count of poll invocations",
        setOf(VENICE_STORE_NAME, VENICE_RESPONSE_STATUS_CODE_CATEGORY)
    ),
    /**
     * Measures to count of version swaps
     */
    VERSION_SWAP_COUNT(
        MetricType.COUNTER, MetricUnit.NUMBER, "Measures to count of version swaps",
        setOf(VENICE_STORE_NAME, VENICE_RESPONSE_STATUS_CODE_CATEGORY)
    ),
    /**
     * Measures the count of chunks consumed
     */
    RECORD_CHUNK_COUNT(
        MetricType.COUNTER, MetricUnit.NUMBER, "Measures the count of chunks consumed",
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
