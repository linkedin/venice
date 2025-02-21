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

  private final WritePathLatencySensor recordTransformerPutLatencySensor;
  private final WritePathLatencySensor recordTransformerDeleteLatencySensor;
  private final WritePathLatencySensor recordTransformerOnRecoveryLatencySensor;
  private final WritePathLatencySensor recordTransformerOnStartVersionIngestionLatencySensor;
  private final WritePathLatencySensor recordTransformerOnEndVersionIngestionLatencySensor;
  private final Count recordTransformerPutErrorCount = new Count();
  private final Sensor recordTransformerPutErrorSensor;
  private final Count recordTransformerDeleteErrorCount = new Count();
  private final Sensor recordTransformerDeleteErrorSensor;

  public DaVinciRecordTransformerStats() {
    localMetricRepository = new MetricsRepository(METRIC_CONFIG);

    recordTransformerPutLatencySensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, RECORD_TRANSFORMER_PUT_LATENCY);
    recordTransformerDeleteLatencySensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, RECORD_TRANSFORMER_DELETE_LATENCY);
    recordTransformerOnRecoveryLatencySensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, RECORD_TRANSFORMER_ON_RECOVERY_LATENCY);
    recordTransformerOnStartVersionIngestionLatencySensor = new WritePathLatencySensor(
        localMetricRepository,
        METRIC_CONFIG,
        RECORD_TRANSFORMER_ON_START_VERSION_INGESTION_LATENCY);
    recordTransformerOnEndVersionIngestionLatencySensor = new WritePathLatencySensor(
        localMetricRepository,
        METRIC_CONFIG,
        RECORD_TRANSFORMER_ON_END_VERSION_INGESTION_LATENCY);
    recordTransformerPutErrorSensor = localMetricRepository.sensor(RECORD_TRANSFORMER_PUT_ERROR_COUNT);
    recordTransformerPutErrorSensor.add(RECORD_TRANSFORMER_PUT_ERROR_COUNT, recordTransformerPutErrorCount);
    recordTransformerDeleteErrorSensor = localMetricRepository.sensor(RECORD_TRANSFORMER_DELETE_ERROR_COUNT);
    recordTransformerDeleteErrorSensor.add(RECORD_TRANSFORMER_DELETE_ERROR_COUNT, recordTransformerDeleteErrorCount);
  }

  public void recordTransformerPutLatency(double value, long currentTimeMs) {
    recordTransformerPutLatencySensor.record(value, currentTimeMs);
  }

  public WritePathLatencySensor getRecordTransformerPutLatencySensor() {
    return recordTransformerPutLatencySensor;
  }

  public void recordTransformerDeleteLatency(double value, long currentTimeMs) {
    recordTransformerDeleteLatencySensor.record(value, currentTimeMs);
  }

  public WritePathLatencySensor getRecordTransformerDeleteLatencySensor() {
    return recordTransformerDeleteLatencySensor;
  }

  public void recordTransformerOnRecoveryLatency(double value, long currentTimeMs) {
    recordTransformerOnRecoveryLatencySensor.record(value, currentTimeMs);
  }

  public WritePathLatencySensor getRecordTransformerOnRecoveryLatencySensor() {
    return recordTransformerOnRecoveryLatencySensor;
  }

  public void recordTransformerOnStartVersionIngestionLatency(double value, long currentTimeMs) {
    recordTransformerOnStartVersionIngestionLatencySensor.record(value, currentTimeMs);
  }

  public WritePathLatencySensor getRecordTransformerOnStartVersionIngestionLatencySensor() {
    return recordTransformerOnStartVersionIngestionLatencySensor;
  }

  public void recordTransformerOnEndVersionIngestionLatency(double value, long currentTimeMs) {
    recordTransformerOnEndVersionIngestionLatencySensor.record(value, currentTimeMs);
  }

  public WritePathLatencySensor getRecordTransformerOnEndVersionIngestionLatencySensor() {
    return recordTransformerOnEndVersionIngestionLatencySensor;
  }

  public void recordTransformerPutError(double value, long currentTimeMs) {
    recordTransformerPutErrorSensor.record(value, currentTimeMs);
  }

  public double getRecordTransformerPutErrorCount() {
    return recordTransformerPutErrorCount.measure(METRIC_CONFIG, System.currentTimeMillis());
  }

  public void recordTransformerDeleteError(double value, long currentTimeMs) {
    recordTransformerDeleteErrorSensor.record(value, currentTimeMs);
  }

  public double getRecordTransformerDeleteErrorCount() {
    return recordTransformerDeleteErrorCount.measure(METRIC_CONFIG, System.currentTimeMillis());
  }
}
