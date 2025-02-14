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
  public static final String RECORD_TRANSFORMER_ERROR_COUNT = "record_transformer_error_count";

  private final WritePathLatencySensor recordTransformerPutLatencySensor;
  private final WritePathLatencySensor recordTransformerDeleteLatencySensor;
  private final WritePathLatencySensor recordTransformerOnRecoveryLatencySensor;
  private final WritePathLatencySensor recordTransformerOnStartVersionIngestionLatencySensor;
  private final WritePathLatencySensor recordTransformerOnEndVersionIngestionLatencySensor;
  private final Count recordTransformerErrorCount = new Count();
  private final Sensor recordTransformerErrorSensor;

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
    recordTransformerErrorSensor = localMetricRepository.sensor(RECORD_TRANSFORMER_ERROR_COUNT);
    recordTransformerErrorSensor.add(RECORD_TRANSFORMER_ERROR_COUNT, recordTransformerErrorCount);
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

  public void recordTransformerError(double value, long currentTimeMs) {
    recordTransformerErrorSensor.record(value, currentTimeMs);
  }

  public double getTransformerErrorCount() {
    return recordTransformerErrorCount.measure(METRIC_CONFIG, System.currentTimeMillis());
  }
}
