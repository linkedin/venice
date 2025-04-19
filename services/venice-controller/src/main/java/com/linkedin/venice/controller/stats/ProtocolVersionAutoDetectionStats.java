package com.linkedin.venice.controller.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Count;
import io.tehuti.metrics.stats.Max;


public class ProtocolVersionAutoDetectionStats extends AbstractVeniceStats {
  private final Sensor protocolVersionAutoDetectionErrorSensor;
  private final Sensor protocolVersionAutoDetectionLatencySensor;
  private final static String PROTOCOL_VERSION_AUTO_DETECTION_ERROR = "protocol_version_auto_detection_error";
  private final static String PROTOCOL_VERSION_AUTO_DETECTION_LATENCY = "protocol_version_auto_detection_latency";

  public ProtocolVersionAutoDetectionStats(MetricsRepository metricsRepository, String name) {
    super(metricsRepository, name);
    protocolVersionAutoDetectionErrorSensor =
        registerSensorIfAbsent(PROTOCOL_VERSION_AUTO_DETECTION_ERROR, new Count());
    protocolVersionAutoDetectionLatencySensor =
        registerSensorIfAbsent(PROTOCOL_VERSION_AUTO_DETECTION_LATENCY, new Avg(), new Max());
  }

  public void recordProtocolVersionAutoDetectionErrorSensor() {
    protocolVersionAutoDetectionErrorSensor.record();
  }

  public void recordProtocolVersionAutoDetectionLatencySensor(double latencyInMs) {
    protocolVersionAutoDetectionLatencySensor.record(latencyInMs);
  }
}
