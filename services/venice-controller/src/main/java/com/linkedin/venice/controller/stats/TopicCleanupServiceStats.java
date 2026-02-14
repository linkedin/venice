package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
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
        ControllerMetricEntity.TOPIC_CLEANUP_DELETABLE_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        TopicCleanupTehutiMetricNameEnum.DELETABLE_TOPICS_COUNT,
        Arrays.asList(new Gauge()),
        baseDimensionsMap,
        baseAttributes);

    topicsDeletedMetric = MetricEntityStateOneEnum.create(
        ControllerMetricEntity.TOPIC_CLEANUP_DELETED_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        TopicCleanupTehutiMetricNameEnum.TOPICS_DELETED_RATE,
        Arrays.asList(new Rate()),
        baseDimensionsMap,
        VeniceResponseStatusCategory.class);

    topicDeletionErrorMetric = MetricEntityStateOneEnum.create(
        ControllerMetricEntity.TOPIC_CLEANUP_DELETED_COUNT.getMetricEntity(),
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
}
