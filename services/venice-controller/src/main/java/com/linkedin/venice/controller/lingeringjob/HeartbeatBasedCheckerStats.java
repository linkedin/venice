package com.linkedin.venice.controller.lingeringjob;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.stats.OpenTelemetryMetricsSetup;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.TehutiMetricNameEnum;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Count;
import java.util.Collections;


public class HeartbeatBasedCheckerStats extends AbstractVeniceStats {
  private static final String STATS_NAME = "controller-batch-job-heartbeat-checker";

  private final MetricEntityStateBase checkJobHasHeartbeatFailedState;
  private final MetricEntityStateBase timeoutHeartbeatCheckState;
  private final MetricEntityStateBase noTimeoutHeartbeatCheckState;

  public HeartbeatBasedCheckerStats(MetricsRepository metricsRepository) {
    super(metricsRepository, STATS_NAME);

    OpenTelemetryMetricsSetup.OpenTelemetryMetricsSetupInfo otelData =
        OpenTelemetryMetricsSetup.builder(metricsRepository).build();

    checkJobHasHeartbeatFailedState = MetricEntityStateBase.create(
        HeartbeatCheckerOtelMetricEntity.BATCH_JOB_HEARTBEAT_CHECK_FAILURE_COUNT.getMetricEntity(),
        otelData.getOtelRepository(),
        this::registerSensor,
        HeartbeatCheckerTehutiMetricNameEnum.CHECK_JOB_HAS_HEARTBEAT_FAILED,
        Collections.singletonList(new Count()),
        otelData.getBaseDimensionsMap(),
        otelData.getBaseAttributes());

    timeoutHeartbeatCheckState = MetricEntityStateBase.create(
        HeartbeatCheckerOtelMetricEntity.BATCH_JOB_HEARTBEAT_TIMEOUT_COUNT.getMetricEntity(),
        otelData.getOtelRepository(),
        this::registerSensor,
        HeartbeatCheckerTehutiMetricNameEnum.TIMEOUT_HEARTBEAT_CHECK,
        Collections.singletonList(new Count()),
        otelData.getBaseDimensionsMap(),
        otelData.getBaseAttributes());

    noTimeoutHeartbeatCheckState = MetricEntityStateBase.create(
        HeartbeatCheckerOtelMetricEntity.BATCH_JOB_HEARTBEAT_ACTIVE_COUNT.getMetricEntity(),
        otelData.getOtelRepository(),
        this::registerSensor,
        HeartbeatCheckerTehutiMetricNameEnum.NON_TIMEOUT_HEARTBEAT_CHECK,
        Collections.singletonList(new Count()),
        otelData.getBaseDimensionsMap(),
        otelData.getBaseAttributes());
  }

  void recordCheckJobHasHeartbeatFailed() {
    checkJobHasHeartbeatFailedState.record(1);
  }

  void recordTimeoutHeartbeatCheck() {
    timeoutHeartbeatCheckState.record(1);
  }

  void recordNoTimeoutHeartbeatCheck() {
    noTimeoutHeartbeatCheckState.record(1);
  }

  enum HeartbeatCheckerTehutiMetricNameEnum implements TehutiMetricNameEnum {
    CHECK_JOB_HAS_HEARTBEAT_FAILED, TIMEOUT_HEARTBEAT_CHECK, NON_TIMEOUT_HEARTBEAT_CHECK
  }
}
