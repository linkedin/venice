package com.linkedin.venice.controller.stats;

import com.google.common.collect.ImmutableMap;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VenicePushJobStatus;
import com.linkedin.venice.stats.dimensions.VenicePushType;
import com.linkedin.venice.stats.metrics.MetricEntityStateGeneric;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.CountSinceLastMeasurement;
import java.util.Arrays;
import java.util.Map;


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
        ControllerMetricEntity.PUSH_JOB_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        PushJobTehutiMetricNameEnum.BATCH_PUSH_JOB_SUCCESS,
        Arrays.asList(new Count(), new CountSinceLastMeasurement()),
        baseDimensionsMap);

    batchPushFailureDueToUserErrorMetric = MetricEntityStateGeneric.create(
        ControllerMetricEntity.PUSH_JOB_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        PushJobTehutiMetricNameEnum.BATCH_PUSH_JOB_FAILED_USER_ERROR,
        Arrays.asList(new Count(), new CountSinceLastMeasurement()),
        baseDimensionsMap);

    batchPushFailureDueToNonUserErrorMetric = MetricEntityStateGeneric.create(
        ControllerMetricEntity.PUSH_JOB_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        PushJobTehutiMetricNameEnum.BATCH_PUSH_JOB_FAILED_NON_USER_ERROR,
        Arrays.asList(new Count(), new CountSinceLastMeasurement()),
        baseDimensionsMap);

    incrementalPushSuccessMetric = MetricEntityStateGeneric.create(
        ControllerMetricEntity.PUSH_JOB_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        PushJobTehutiMetricNameEnum.INCREMENTAL_PUSH_JOB_SUCCESS,
        Arrays.asList(new Count(), new CountSinceLastMeasurement()),
        baseDimensionsMap);

    incrementalPushFailureDueToUserErrorMetric = MetricEntityStateGeneric.create(
        ControllerMetricEntity.PUSH_JOB_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        PushJobTehutiMetricNameEnum.INCREMENTAL_PUSH_JOB_FAILED_USER_ERROR,
        Arrays.asList(new Count(), new CountSinceLastMeasurement()),
        baseDimensionsMap);

    incrementalPushFailureDueToNonUserErrorMetric = MetricEntityStateGeneric.create(
        ControllerMetricEntity.PUSH_JOB_COUNT.getMetricEntity(),
        otelRepository,
        this::registerSensorIfAbsent,
        PushJobTehutiMetricNameEnum.INCREMENTAL_PUSH_JOB_FAILED_NON_USER_ERROR,
        Arrays.asList(new Count(), new CountSinceLastMeasurement()),
        baseDimensionsMap);
  }

  public void recordBatchPushSuccessSensor(String storeName) {
    batchPushSuccessMetric.record(1, buildDimensions(storeName, VenicePushType.BATCH, VenicePushJobStatus.SUCCESS));
  }

  public void recordBatchPushFailureDueToUserErrorSensor(String storeName) {
    batchPushFailureDueToUserErrorMetric
        .record(1, buildDimensions(storeName, VenicePushType.BATCH, VenicePushJobStatus.USER_ERROR));
  }

  public void recordBatchPushFailureNotDueToUserErrorSensor(String storeName) {
    batchPushFailureDueToNonUserErrorMetric
        .record(1, buildDimensions(storeName, VenicePushType.BATCH, VenicePushJobStatus.SYSTEM_ERROR));
  }

  public void recordIncrementalPushSuccessSensor(String storeName) {
    incrementalPushSuccessMetric
        .record(1, buildDimensions(storeName, VenicePushType.INCREMENTAL, VenicePushJobStatus.SUCCESS));
  }

  public void recordIncrementalPushFailureDueToUserErrorSensor(String storeName) {
    incrementalPushFailureDueToUserErrorMetric
        .record(1, buildDimensions(storeName, VenicePushType.INCREMENTAL, VenicePushJobStatus.USER_ERROR));
  }

  public void recordIncrementalPushFailureNotDueToUserErrorSensor(String storeName) {
    incrementalPushFailureDueToNonUserErrorMetric
        .record(1, buildDimensions(storeName, VenicePushType.INCREMENTAL, VenicePushJobStatus.SYSTEM_ERROR));
  }

  private Map<VeniceMetricsDimensions, String> buildDimensions(
      String storeName,
      VenicePushType pushType,
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
    INCREMENTAL_PUSH_JOB_SUCCESS, INCREMENTAL_PUSH_JOB_FAILED_USER_ERROR, INCREMENTAL_PUSH_JOB_FAILED_NON_USER_ERROR;

    private final String metricName;

    PushJobTehutiMetricNameEnum() {
      this.metricName = name().toLowerCase();
    }

    @Override
    public String getMetricName() {
      return this.metricName;
    }
  }
}
