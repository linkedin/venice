package com.linkedin.venice.controller.stats;

import com.google.common.collect.ImmutableMap;
import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VenicePushJobStatus;
import com.linkedin.venice.stats.dimensions.VenicePushType;
import com.linkedin.venice.stats.metrics.MetricEntityStateGeneric;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.CountSinceLastMeasurement;
import java.util.Map;


public class PushJobStatusStats extends AbstractVeniceStats {
  private final Sensor batchPushSuccessSensor;
  private final Sensor batchPushFailureDueToUserErrorSensor;
  private final Sensor batchPushFailureDueToNonUserErrorSensor;
  private final Sensor incrementalPushSuccessSensor;
  private final Sensor incrementalPushFailureDueToUserErrorSensor;
  private final Sensor incrementalPushFailureDueToNonUserErrorSensor;

  private final VeniceOpenTelemetryMetricsRepository otelRepository;
  private final Map<VeniceMetricsDimensions, String> baseDimensionsMap;
  private final MetricEntityStateGeneric pushJobCountMetric;

  public PushJobStatusStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).setClusterName(name).build();
    this.otelRepository = otelData.getOtelRepository();
    this.baseDimensionsMap = otelData.getBaseDimensionsMap();

    pushJobCountMetric = MetricEntityStateGeneric
        .create(ControllerMetricEntity.PUSH_JOB_COUNT.getMetricEntity(), otelRepository, baseDimensionsMap);

    batchPushSuccessSensor =
        registerSensorIfAbsent("batch_push_job_success", new Count(), new CountSinceLastMeasurement());
    batchPushFailureDueToUserErrorSensor =
        registerSensorIfAbsent("batch_push_job_failed_user_error", new Count(), new CountSinceLastMeasurement());
    batchPushFailureDueToNonUserErrorSensor =
        registerSensorIfAbsent("batch_push_job_failed_non_user_error", new Count(), new CountSinceLastMeasurement());
    incrementalPushSuccessSensor =
        registerSensorIfAbsent("incremental_push_job_success", new Count(), new CountSinceLastMeasurement());
    incrementalPushFailureDueToUserErrorSensor =
        registerSensorIfAbsent("incremental_push_job_failed_user_error", new Count(), new CountSinceLastMeasurement());
    incrementalPushFailureDueToNonUserErrorSensor = registerSensorIfAbsent(
        "incremental_push_job_failed_non_user_error",
        new Count(),
        new CountSinceLastMeasurement());
  }

  public void recordBatchPushSuccessSensor(String storeName) {
    batchPushSuccessSensor.record();
    recordPushJobOtel(storeName, VenicePushType.BATCH, VenicePushJobStatus.SUCCESS);
  }

  public void recordBatchPushFailureDueToUserErrorSensor(String storeName) {
    batchPushFailureDueToUserErrorSensor.record();
    recordPushJobOtel(storeName, VenicePushType.BATCH, VenicePushJobStatus.USER_ERROR);
  }

  public void recordBatchPushFailureNotDueToUserErrorSensor(String storeName) {
    batchPushFailureDueToNonUserErrorSensor.record();
    recordPushJobOtel(storeName, VenicePushType.BATCH, VenicePushJobStatus.SYSTEM_ERROR);
  }

  public void recordIncrementalPushSuccessSensor(String storeName) {
    incrementalPushSuccessSensor.record();
    recordPushJobOtel(storeName, VenicePushType.INCREMENTAL, VenicePushJobStatus.SUCCESS);
  }

  public void recordIncrementalPushFailureDueToUserErrorSensor(String storeName) {
    incrementalPushFailureDueToUserErrorSensor.record();
    recordPushJobOtel(storeName, VenicePushType.INCREMENTAL, VenicePushJobStatus.USER_ERROR);
  }

  public void recordIncrementalPushFailureNotDueToUserErrorSensor(String storeName) {
    incrementalPushFailureDueToNonUserErrorSensor.record();
    recordPushJobOtel(storeName, VenicePushType.INCREMENTAL, VenicePushJobStatus.SYSTEM_ERROR);
  }

  private void recordPushJobOtel(String storeName, VenicePushType pushType, VenicePushJobStatus status) {
    pushJobCountMetric.record(
        1,
        getDimensionsBuilder().put(VeniceMetricsDimensions.VENICE_STORE_NAME, storeName)
            .put(VeniceMetricsDimensions.VENICE_PUSH_JOB_TYPE, pushType.getDimensionValue())
            .put(VeniceMetricsDimensions.VENICE_PUSH_JOB_STATUS, status.getDimensionValue())
            .build());
  }

  private ImmutableMap.Builder<VeniceMetricsDimensions, String> getDimensionsBuilder() {
    ImmutableMap.Builder<VeniceMetricsDimensions, String> builder = ImmutableMap.builder();

    if (baseDimensionsMap != null) {
      builder.putAll(baseDimensionsMap);
    }

    return builder;
  }
}
