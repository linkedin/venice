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
  public static final String RECORD_TRANSFORMER_PUT_ERROR_COUNT = "record_transformer_put_error_count";
  public static final String RECORD_TRANSFORMER_DELETE_ERROR_COUNT = "record_transformer_delete_error_count";

  private final WritePathLatencySensor putLatencySensor;
  private final WritePathLatencySensor deleteLatencySensor;
  private final Count putErrorCount = new Count();
  private final Sensor putErrorSensor;
  private final Count deleteErrorCount = new Count();
  private final Sensor deleteErrorSensor;

  public DaVinciRecordTransformerStats() {
    localMetricRepository = new MetricsRepository(METRIC_CONFIG);

    putLatencySensor = new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, RECORD_TRANSFORMER_PUT_LATENCY);
    deleteLatencySensor =
        new WritePathLatencySensor(localMetricRepository, METRIC_CONFIG, RECORD_TRANSFORMER_DELETE_LATENCY);
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
