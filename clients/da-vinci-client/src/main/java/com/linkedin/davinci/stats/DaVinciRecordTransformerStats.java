package com.linkedin.davinci.stats;

import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;


public class DaVinciRecordTransformerStats {
  private static final MetricConfig METRIC_CONFIG = new MetricConfig();
  private final MetricsRepository localMetricRepository;

  public static final String RECORD_TRANSFORMER_PUT_LATENCY = "record_transformer_put_latency";
  public static final String RECORD_TRANSFORMER_DELETE_LATENCY = "record_transformer_delete_latency";
  public static final String RECORD_TRANSFORMER_ON_RECOVERY_LATENCY = "record_transformer_on_recovery_latency";
  public static final String RECORD_TRANSFORMER_ON_START_VERSION_INGESTION_LATENCY =
      "record_transformer_on_start_version_ingestion_latency";
  public static final String RECORD_TRANSFORMER_ON_END_VERSION_INGESTION_LATENCY =
      "record_transformer_on_end_version_ingestion_latency";
  public static final String RECORD_TRANSFORMER_PUT_ERROR_COUNT = "record_transformer_put_error_count";
  public static final String RECORD_TRANSFORMER_DELETE_ERROR_COUNT = "record_transformer_delete_error_count";

  private final WritePathLatencySensor putLatencySensor;
  private final WritePathLatencySensor deleteLatencySensor;
  private final WritePathLatencySensor onRecoveryLatencySensor;
  private final WritePathLatencySensor onStartVersionIngestionLatencySensor;
  private final WritePathLatencySensor onEndVersionIngestionLatencySensor;
  private final Count putErrorCount = new Count();
  private final Sensor putErrorSensor;
  private final Count deleteErrorCount = new Count();
  private final Sensor deleteErrorSensor;

  public DaVinciRecordTransformerStats() {
    localMetricRepository = new MetricsRepository(METRIC_CONFIG);

    putLatencySensor = new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, RECORD_TRANSFORMER_PUT_LATENCY);
    deleteLatencySensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, RECORD_TRANSFORMER_DELETE_LATENCY);
    onRecoveryLatencySensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, RECORD_TRANSFORMER_ON_RECOVERY_LATENCY);
    onStartVersionIngestionLatencySensor = new WritePathLatencySensor(
        localMetricRepository,
        METRIC_CONFIG,
        RECORD_TRANSFORMER_ON_START_VERSION_INGESTION_LATENCY);
    onEndVersionIngestionLatencySensor = new WritePathLatencySensor(
        localMetricRepository,
        METRIC_CONFIG,
        RECORD_TRANSFORMER_ON_END_VERSION_INGESTION_LATENCY);
    putErrorSensor = localMetricRepository.sensor(RECORD_TRANSFORMER_PUT_ERROR_COUNT);
    putErrorSensor.add(RECORD_TRANSFORMER_PUT_ERROR_COUNT, putErrorCount);
    deleteErrorSensor = localMetricRepository.sensor(RECORD_TRANSFORMER_DELETE_ERROR_COUNT);
    deleteErrorSensor.add(RECORD_TRANSFORMER_DELETE_ERROR_COUNT, deleteErrorCount);
  }

  public void recordPutLatency(double latencyMs, long currentTimeMs) {
    putLatencySensor.record(latencyMs, currentTimeMs);
  }

  public WritePathLatencySensor getPutLatencySensor() {
    return putLatencySensor;
  }

  public void recordDeleteLatency(double latencyMs, long currentTimeMs) {
    deleteLatencySensor.record(latencyMs, currentTimeMs);
  }

  public WritePathLatencySensor getDeleteLatencySensor() {
    return deleteLatencySensor;
  }

  public void recordOnRecoveryLatency(double latencyMs, long currentTimeMs) {
    onRecoveryLatencySensor.record(latencyMs, currentTimeMs);
  }

  public WritePathLatencySensor getOnRecoveryLatencySensor() {
    return onRecoveryLatencySensor;
  }

  public void recordOnStartVersionIngestionLatency(double latencyMs, long currentTimeMs) {
    onStartVersionIngestionLatencySensor.record(latencyMs, currentTimeMs);
  }

  public WritePathLatencySensor getOnStartVersionIngestionLatencySensor() {
    return onStartVersionIngestionLatencySensor;
  }

  public void recordOnEndVersionIngestionLatency(double latencyMs, long currentTimeMs) {
    onEndVersionIngestionLatencySensor.record(latencyMs, currentTimeMs);
  }

  public WritePathLatencySensor getOnEndVersionIngestionLatencySensor() {
    return onEndVersionIngestionLatencySensor;
  }

  public void recordPutError(long currentTimeMs) {
    putErrorSensor.record(1, currentTimeMs);
  }

  public double getPutErrorCount() {
    return putErrorCount.measure(METRIC_CONFIG, System.currentTimeMillis());
  }

  public void recordDeleteError(long currentTimeMs) {
    deleteErrorSensor.record(1, currentTimeMs);
  }

  public double getDeleteErrorCount() {
    return deleteErrorCount.measure(METRIC_CONFIG, System.currentTimeMillis());
  }
}
