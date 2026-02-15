package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
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
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Gauge;
import io.tehuti.metrics.stats.Rate;
import java.util.Arrays;
import java.util.Map;


public class TopicCleanupServiceStats extends AbstractVeniceStats {
  private final MetricEntityStateBase deletableTopicsCountMetric;
  private final MetricEntityStateOneEnum<VeniceResponseStatusCategory> topicsDeletedMetric;
  private final MetricEntityStateOneEnum<VeniceResponseStatusCategory> topicDeletionErrorMetric;

  public TopicCleanupServiceStats(MetricsRepository metricsRepository) {
    super(metricsRepository, "TopicCleanupService");

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).build();
    VeniceOpenTelemetryMetricsRepository otelRepository = otelData.getOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();
    Attributes baseAttributes = otelData.getBaseAttributes();

    deletableTopicsCountMetric = MetricEntityStateBase.create(
        TopicCleanupOtelMetricEntity.TOPIC_CLEANUP_DELETABLE_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        TopicCleanupTehutiMetricNameEnum.DELETABLE_TOPICS_COUNT,
        Arrays.asList(new Gauge()),
        baseDimensionsMap,
        baseAttributes);

    topicsDeletedMetric = MetricEntityStateOneEnum.create(
        TopicCleanupOtelMetricEntity.TOPIC_CLEANUP_DELETED_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        TopicCleanupTehutiMetricNameEnum.TOPICS_DELETED_RATE,
        Arrays.asList(new Rate()),
        baseDimensionsMap,
        VeniceResponseStatusCategory.class);

    topicDeletionErrorMetric = MetricEntityStateOneEnum.create(
        TopicCleanupOtelMetricEntity.TOPIC_CLEANUP_DELETED_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        TopicCleanupTehutiMetricNameEnum.TOPIC_DELETION_ERROR_RATE,
        Arrays.asList(new Rate()),
        baseDimensionsMap,
        VeniceResponseStatusCategory.class);
  }

  public void recordDeletableTopicsCount(int deletableTopicsCount) {
    deletableTopicsCountMetric.record(deletableTopicsCount);
  }

  public void recordTopicDeleted() {
    topicsDeletedMetric.record(1, VeniceResponseStatusCategory.SUCCESS);
  }

  public void recordTopicDeletionError() {
    topicDeletionErrorMetric.record(1, VeniceResponseStatusCategory.FAIL);
  }

  enum TopicCleanupTehutiMetricNameEnum implements TehutiMetricNameEnum {
    DELETABLE_TOPICS_COUNT("deletable_topics_count"), TOPICS_DELETED_RATE("topics_deleted_rate"),
    TOPIC_DELETION_ERROR_RATE("topic_deletion_error_rate");

    private final String metricName;

    TopicCleanupTehutiMetricNameEnum(String metricName) {
      this.metricName = metricName;
    }

    @Override
    public String getMetricName() {
      return this.metricName;
    }
  }

  public enum TopicCleanupOtelMetricEntity implements ModuleMetricEntityInterface {
    /** Gauge of topics currently eligible for deletion */
    TOPIC_CLEANUP_DELETABLE_COUNT(
        "topic_cleanup_service.topic.deletable_count", MetricType.GAUGE, MetricUnit.NUMBER,
        "Count of topics currently eligible for deletion"
    ),
    /** Count of topic deletion operations (success and failure) */
    TOPIC_CLEANUP_DELETED_COUNT(
        "topic_cleanup_service.topic.deleted_count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Count of topic deletion operations", setOf(VENICE_RESPONSE_STATUS_CODE_CATEGORY)
    );

    private final MetricEntity metricEntity;
    private final String metricName;

    TopicCleanupOtelMetricEntity(String metricName, MetricType metricType, MetricUnit unit, String description) {
      this.metricName = metricName;
      this.metricEntity = MetricEntity.createWithNoDimensions(metricName, metricType, unit, description);
    }

    TopicCleanupOtelMetricEntity(
        String metricName,
        MetricType metricType,
        MetricUnit unit,
        String description,
        java.util.Set<VeniceMetricsDimensions> dimensionsList) {
      this.metricName = metricName;
      this.metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensionsList);
    }

    @VisibleForTesting
    public String getMetricName() {
      return metricName;
    }

    public MetricEntity getMetricEntity() {
      return metricEntity;
    }
  }
}
