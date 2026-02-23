package com.linkedin.venice.controller.stats;

import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_STATUS;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_PUSH_JOB_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.Utils.setOf;

import com.google.common.collect.ImmutableMap;
import com.linkedin.venice.meta.Version.PushType;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VenicePushJobStatus;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityStateGeneric;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.CountSinceLastMeasurement;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;


public class PushJobStatusStats extends AbstractVeniceStats {
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;

  private final MetricEntityStateGeneric batchPushSuccessMetric;
  private final MetricEntityStateGeneric batchPushFailureDueToUserErrorMetric;
  private final MetricEntityStateGeneric batchPushFailureDueToNonUserErrorMetric;
  private final MetricEntityStateGeneric incrementalPushSuccessMetric;
  private final MetricEntityStateGeneric incrementalPushFailureDueToUserErrorMetric;
  private final MetricEntityStateGeneric incrementalPushFailureDueToNonUserErrorMetric;

  public PushJobStatusStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(name).build();
    VeniceOpenTelemetryMetricsRepository otelRepository = otelData.getOtelRepository();
    this.baseDimensionsMap = otelData.getBaseDimensionsMap();

    batchPushSuccessMetric = MetricEntityStateGeneric.create(
        PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        PushJobTehutiMetricNameEnum.BATCH_PUSH_JOB_SUCCESS,
        Arrays.asList(new Count(), new CountSinceLastMeasurement()),
        baseDimensionsMap);

    batchPushFailureDueToUserErrorMetric = MetricEntityStateGeneric.create(
        PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        PushJobTehutiMetricNameEnum.BATCH_PUSH_JOB_FAILED_USER_ERROR,
        Arrays.asList(new Count(), new CountSinceLastMeasurement()),
        baseDimensionsMap);

    batchPushFailureDueToNonUserErrorMetric = MetricEntityStateGeneric.create(
        PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        PushJobTehutiMetricNameEnum.BATCH_PUSH_JOB_FAILED_NON_USER_ERROR,
        Arrays.asList(new Count(), new CountSinceLastMeasurement()),
        baseDimensionsMap);

    incrementalPushSuccessMetric = MetricEntityStateGeneric.create(
        PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        PushJobTehutiMetricNameEnum.INCREMENTAL_PUSH_JOB_SUCCESS,
        Arrays.asList(new Count(), new CountSinceLastMeasurement()),
        baseDimensionsMap);

    incrementalPushFailureDueToUserErrorMetric = MetricEntityStateGeneric.create(
        PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        PushJobTehutiMetricNameEnum.INCREMENTAL_PUSH_JOB_FAILED_USER_ERROR,
        Arrays.asList(new Count(), new CountSinceLastMeasurement()),
        baseDimensionsMap);

    incrementalPushFailureDueToNonUserErrorMetric = MetricEntityStateGeneric.create(
        PushJobOtelMetricEntity.PUSH_JOB_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        PushJobTehutiMetricNameEnum.INCREMENTAL_PUSH_JOB_FAILED_NON_USER_ERROR,
        Arrays.asList(new Count(), new CountSinceLastMeasurement()),
        baseDimensionsMap);
  }

  public void recordBatchPushSuccessSensor(String storeName) {
    batchPushSuccessMetric.record(1, buildDimensions(storeName, PushType.BATCH, VenicePushJobStatus.SUCCESS));
  }

  public void recordBatchPushFailureDueToUserErrorSensor(String storeName) {
    batchPushFailureDueToUserErrorMetric
        .record(1, buildDimensions(storeName, PushType.BATCH, VenicePushJobStatus.USER_ERROR));
  }

  public void recordBatchPushFailureNotDueToUserErrorSensor(String storeName) {
    batchPushFailureDueToNonUserErrorMetric
        .record(1, buildDimensions(storeName, PushType.BATCH, VenicePushJobStatus.SYSTEM_ERROR));
  }

  public void recordIncrementalPushSuccessSensor(String storeName) {
    incrementalPushSuccessMetric
        .record(1, buildDimensions(storeName, PushType.INCREMENTAL, VenicePushJobStatus.SUCCESS));
  }

  public void recordIncrementalPushFailureDueToUserErrorSensor(String storeName) {
    incrementalPushFailureDueToUserErrorMetric
        .record(1, buildDimensions(storeName, PushType.INCREMENTAL, VenicePushJobStatus.USER_ERROR));
  }

  public void recordIncrementalPushFailureNotDueToUserErrorSensor(String storeName) {
    incrementalPushFailureDueToNonUserErrorMetric
        .record(1, buildDimensions(storeName, PushType.INCREMENTAL, VenicePushJobStatus.SYSTEM_ERROR));
  }

  private Map<VeniceMetricsDimensions, String> buildDimensions(
      String storeName,
      PushType pushType,
      VenicePushJobStatus status) {
    ImmutableMap.Builder<VeniceMetricsDimensions, String> builder = ImmutableMap.builder();
    if (baseDimensionsMap != null) {
      builder.putAll(baseDimensionsMap);
    }
    return builder.put(VeniceMetricsDimensions.VENICE_STORE_NAME, storeName)
        .put(VeniceMetricsDimensions.VENICE_PUSH_JOB_TYPE, pushType.getDimensionValue())
        .put(VeniceMetricsDimensions.VENICE_PUSH_JOB_STATUS, status.getDimensionValue())
        .build();
  }

  enum PushJobTehutiMetricNameEnum implements TehutiMetricNameEnum {
    BATCH_PUSH_JOB_SUCCESS, BATCH_PUSH_JOB_FAILED_USER_ERROR, BATCH_PUSH_JOB_FAILED_NON_USER_ERROR,
    INCREMENTAL_PUSH_JOB_SUCCESS, INCREMENTAL_PUSH_JOB_FAILED_USER_ERROR, INCREMENTAL_PUSH_JOB_FAILED_NON_USER_ERROR
  }

  public enum PushJobOtelMetricEntity implements ModuleMetricEntityInterface {
    /** PushJobStatusStats: Push job completions */
    PUSH_JOB_COUNT(
        "push_job.count", MetricType.COUNTER, MetricUnit.NUMBER,
        "Push job completions, differentiated by push type and status",
        setOf(VENICE_CLUSTER_NAME, VENICE_STORE_NAME, VENICE_PUSH_JOB_TYPE, VENICE_PUSH_JOB_STATUS)
    );

    private final MetricEntity metricEntity;

    PushJobOtelMetricEntity(
        String metricName,
        MetricType metricType,
        MetricUnit unit,
        String description,
        Set<VeniceMetricsDimensions> dimensionsList) {
      this.metricEntity = new MetricEntity(metricName, metricType, unit, description, dimensionsList);
    }

    public MetricEntity getMetricEntity() {
      return metricEntity;
    }
  }
}
