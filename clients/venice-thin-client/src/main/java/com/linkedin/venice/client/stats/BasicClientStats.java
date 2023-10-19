package com.linkedin.venice.client.stats;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AbstractVeniceHttpStats;
import com.linkedin.venice.stats.TehutiUtils;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Rate;


/**
 * This class offers very basic metrics for client, and right now, it is directly used by DaVinci.
 */
public class BasicClientStats extends AbstractVeniceHttpStats {
  String SYSTEM_STORE_NAME_PREFIX = "venice_system_store_";

  private final Sensor requestSensor;
  private final Sensor healthySensor;
  private final Sensor unhealthySensor;
  private final Sensor healthyRequestLatencySensor;
  private final Sensor requestKeyCountSensor;
  private final Sensor successRequestKeyCountSensor;
  private final Sensor successRequestRatioSensor;
  private final Sensor successRequestKeyRatioSensor;

  private final Rate requestRate = new OccurrenceRate();
  private final Rate successRequestKeyCountRate = new Rate();

  private final boolean isMetaSystemStore;

  public static BasicClientStats getClientStats(
      MetricsRepository metricsRepository,
      String storeName,
      RequestType requestType,
      ClientConfig clientConfig) {
    String prefix = clientConfig == null ? null : clientConfig.getStatsPrefix();
    String metricName = prefix == null || prefix.isEmpty() ? storeName : prefix + "." + storeName;
    return new BasicClientStats(metricsRepository, metricName, requestType);
  }

  protected BasicClientStats(MetricsRepository metricsRepository, String storeName, RequestType requestType) {
    super(metricsRepository, storeName, requestType);
    this.isMetaSystemStore = storeName.startsWith(SYSTEM_STORE_NAME_PREFIX);
    requestSensor = registerSensor("request", requestRate);
    Rate healthyRequestRate = new OccurrenceRate();
    healthySensor = registerSensor("healthy_request", healthyRequestRate);
    unhealthySensor = registerSensor("unhealthy_request", new OccurrenceRate());
    healthyRequestLatencySensor = registerSensorWithDetailedPercentiles("healthy_request_latency", new Avg());
    successRequestRatioSensor =
        registerSensor("success_request_ratio", new TehutiUtils.SimpleRatioStat(healthyRequestRate, requestRate));
    Rate requestKeyCountRate = new Rate();
    requestKeyCountSensor = registerSensor("request_key_count", requestKeyCountRate, new Avg(), new Max());
    successRequestKeyCountSensor =
        registerSensor("success_request_key_count", successRequestKeyCountRate, new Avg(), new Max());

    successRequestKeyRatioSensor = registerSensor(
        "success_request_key_ratio",
        new TehutiUtils.SimpleRatioStat(successRequestKeyCountRate, requestKeyCountRate));
  }

  protected boolean isMetaSystemStore() {
    return isMetaSystemStore;
  }

  private void recordRequest() {
    if (isMetaSystemStore()) {
      return;
    }
    requestSensor.record();
  }

  public void recordHealthyRequest() {
    if (isMetaSystemStore()) {
      return;
    }
    recordRequest();
    healthySensor.record();
  }

  public void recordUnhealthyRequest() {
    if (isMetaSystemStore()) {
      return;
    }
    recordRequest();
    unhealthySensor.record();
  }

  public void recordHealthyLatency(double latency) {
    if (isMetaSystemStore()) {
      return;
    }
    healthyRequestLatencySensor.record(latency);
  }

  public void recordRequestKeyCount(int keyCount) {
    if (isMetaSystemStore()) {
      return;
    }
    requestKeyCountSensor.record(keyCount);
  }

  public void recordSuccessRequestKeyCount(int successKeyCount) {
    if (isMetaSystemStore()) {
      return;
    }
    successRequestKeyCountSensor.record(successKeyCount);
  }

  protected final Rate getRequestRate() {
    return requestRate;
  }

  @Override
  protected Sensor registerSensor(String sensorName, MeasurableStat... stats) {
    if (!isMetaSystemStore() || sensorName.equals("request")) {
      return super.registerSensor(getFullMetricName(sensorName), stats);
    }
    return null;
  }

  @Override
  protected Sensor registerSensorWithDetailedPercentiles(String sensorName, MeasurableStat... stats) {
    if (!isMetaSystemStore()) {
      return super.registerSensorWithDetailedPercentiles(sensorName, stats);
    }
    return null;
  }
}
