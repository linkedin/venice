package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_TYPE;
import static com.linkedin.venice.utils.Utils.setOf;

import com.linkedin.venice.meta.Version.PushType;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntityStateOneEnum;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.opentelemetry.api.common.Attributes;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Count;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;


public class VeniceAdminStats extends AbstractVeniceStats {
  /**
   * A counter reporting errors due to absence of a kafka topic that is expected to be available. e.g. the version topic
   * is absent during an incremental push. Differentiated by push type.
   */
  private final MetricEntityStateOneEnum<PushType> unexpectedAbsenceMetric;

  /**
   * A counter reporting successfully started user batch pushes from the Venice parent admin's perspective.
   * i.e. this metric doesn't include version creation triggered by store migration or system stores. This metric is
   * used as another data point to validate/monitor push job details reporting.
   */
  private final MetricEntityStateOneEnum<PushType> batchPushStartedMetric;

  /**
   * A counter reporting successfully started user incremental pushes from the Venice parent admin's perspective.
   * This metric is used as another data point to validate/monitor push job details reporting.
   */
  private final MetricEntityStateOneEnum<PushType> incrementalPushStartedMetric;

  /**
   * A counter reporting the number of failed serialization attempts of admin operations.
   * This metric is used to monitor the health of serialization of admin operations in parent admin by using the dynamic
   * version in the serializer.
   */
  private final MetricEntityStateBase serializationFailureMetric;

  public VeniceAdminStats(MetricsRepository metricsRepository, String statsPrefix, String clusterName) {
    super(metricsRepository, statsPrefix + clusterName);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(clusterName).build();
    VeniceOpenTelemetryMetricsRepository otelRepository = otelData.getOtelRepository();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = otelData.getBaseDimensionsMap();
    Attributes baseAttributes = otelData.getBaseAttributes();

    unexpectedAbsenceMetric = MetricEntityStateOneEnum.create(
        VeniceAdminOtelMetricEntity.ADMIN_TOPIC_UNEXPECTED_ABSENCE_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        VeniceAdminTehutiMetricNameEnum.UNEXPECTED_TOPIC_ABSENCE_DURING_INCREMENTAL_PUSH_COUNT,
        Arrays.asList(new Count()),
        baseDimensionsMap,
        PushType.class);

    batchPushStartedMetric = MetricEntityStateOneEnum.create(
        VeniceAdminOtelMetricEntity.ADMIN_PUSH_STARTED_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        VeniceAdminTehutiMetricNameEnum.SUCCESSFULLY_STARTED_USER_BATCH_PUSH_PARENT_ADMIN_COUNT,
        Arrays.asList(new Count()),
        baseDimensionsMap,
        PushType.class);

    incrementalPushStartedMetric = MetricEntityStateOneEnum.create(
        VeniceAdminOtelMetricEntity.ADMIN_PUSH_STARTED_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        VeniceAdminTehutiMetricNameEnum.SUCCESSFUL_STARTED_USER_INCREMENTAL_PUSH_PARENT_ADMIN_COUNT,
        Arrays.asList(new Count()),
        baseDimensionsMap,
        PushType.class);

    serializationFailureMetric = MetricEntityStateBase.create(
        VeniceAdminOtelMetricEntity.ADMIN_OPERATION_SERIALIZATION_FAILURE_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        VeniceAdminTehutiMetricNameEnum.FAILED_SERIALIZING_ADMIN_OPERATION_MESSAGE_COUNT,
        Arrays.asList(new Count()),
        baseDimensionsMap,
        baseAttributes);
  }

  public void recordUnexpectedTopicAbsenceCount(PushType pushType) {
    unexpectedAbsenceMetric.record(1, pushType);
  }

  public void recordSuccessfullyStartedUserBatchPushParentAdminCount() {
    batchPushStartedMetric.record(1, PushType.BATCH);
  }

  public void recordSuccessfullyStartedUserIncrementalPushParentAdminCount() {
    incrementalPushStartedMetric.record(1, PushType.INCREMENTAL);
  }

  public void recordFailedSerializingAdminOperationMessageCount() {
    serializationFailureMetric.record(1);
  }

  enum VeniceAdminTehutiMetricNameEnum implements TehutiMetricNameEnum {
    UNEXPECTED_TOPIC_ABSENCE_DURING_INCREMENTAL_PUSH_COUNT, SUCCESSFULLY_STARTED_USER_BATCH_PUSH_PARENT_ADMIN_COUNT,
    SUCCESSFUL_STARTED_USER_INCREMENTAL_PUSH_PARENT_ADMIN_COUNT, FAILED_SERIALIZING_ADMIN_OPERATION_MESSAGE_COUNT
  }

  public enum VeniceAdminOtelMetricEntity implements ModuleMetricEntityInterface {
    /** Topics unexpectedly missing or truncated during push */
    ADMIN_TOPIC_UNEXPECTED_ABSENCE_COUNT(
        "admin.topic.unexpected_absence_count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Topics unexpectedly missing or truncated during push", setOf(VENICE_CLUSTER_NAME, VENICE_PUSH_JOB_TYPE)
    ),
    /** Successful push starts from parent admin */
    ADMIN_PUSH_STARTED_COUNT(
        "admin.push.started_count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Successful push starts from parent admin, differentiated by push type",
        setOf(VENICE_CLUSTER_NAME, VENICE_PUSH_JOB_TYPE)
    ),
    /** Failed admin operation serializations */
    ADMIN_OPERATION_SERIALIZATION_FAILURE_COUNT(
        "admin.operation.serialization.failure_count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Failed admin operation serializations", setOf(VENICE_CLUSTER_NAME)
    );

    private final MetricEntity metricEntity;

    VeniceAdminOtelMetricEntity(
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
