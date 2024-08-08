package com.linkedin.venice.client.stats;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.AbstractVeniceHttpStats;
import com.linkedin.venice.stats.TehutiUtils;
import com.linkedin.venice.utils.lazy.Lazy;
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
  private static final String SYSTEM_STORE_NAME_PREFIX = "venice_system_store_";

  private static final MetricsRepository dummySystemStoreMetricRepo = new MetricsRepository();

  private final Lazy<Sensor> requestSensor;
  private final Lazy<Sensor> healthySensor;
  private final Lazy<Sensor> unhealthySensor;
  private final Lazy<Sensor> healthyRequestLatencySensor;
  private final Lazy<Sensor> requestKeyCountSensor;
  private final Lazy<Sensor> successRequestKeyCountSensor;
  private final Lazy<Sensor> successRequestRatioSensor;
  private final Lazy<Sensor> successRequestKeyRatioSensor;
  private final Rate requestRate = new OccurrenceRate();
  private final Rate successRequestKeyCountRate = new Rate();

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
    super(
        storeName.startsWith(SYSTEM_STORE_NAME_PREFIX) ? dummySystemStoreMetricRepo : metricsRepository,
        storeName,
        requestType);
    Rate healthyRequestRate = new OccurrenceRate();
    successRequestRatioSensor = Lazy.of(
        () -> registerSensor(
            new TehutiUtils.SimpleRatioStat(healthyRequestRate, requestRate, "success_request_ratio")));
    requestSensor = Lazy.of(() -> {
      Sensor requestSensor = registerSensor("request", requestRate);
      successRequestRatioSensor.get();
      return requestSensor;
    });
    healthySensor = Lazy.of(() -> {
      Sensor healthySensor = registerSensor("healthy_request", healthyRequestRate);
      successRequestRatioSensor.get();
      return healthySensor;
    });
    unhealthySensor = Lazy.of(() -> registerSensor("unhealthy_request", new OccurrenceRate()));
    healthyRequestLatencySensor =
        Lazy.of(() -> registerSensorWithDetailedPercentiles("healthy_request_latency", new Avg()));

    Rate requestKeyCountRate = new Rate();
    successRequestKeyRatioSensor = Lazy.of(
        () -> registerSensor(
            new TehutiUtils.SimpleRatioStat(
                successRequestKeyCountRate,
                requestKeyCountRate,
                "success_request_key_ratio")));
    requestKeyCountSensor = Lazy.of(() -> {
      Sensor requestKeyCountSensor = registerSensor("request_key_count", requestKeyCountRate, new Avg(), new Max());
      successRequestKeyRatioSensor.get();
      return requestKeyCountSensor;
    });
    successRequestKeyCountSensor = Lazy.of(() -> {
      Sensor successRequestKeyCountSensor =
          registerSensor("success_request_key_count", successRequestKeyCountRate, new Avg(), new Max());
      successRequestKeyRatioSensor.get();
      return successRequestKeyCountSensor;
    });
  }

  private void recordRequest() {
    requestSensor.get().record();
  }

  public void recordHealthyRequest() {
    recordRequest();
    healthySensor.get().record();
  }

  public void recordUnhealthyRequest() {
    recordRequest();
    unhealthySensor.get().record();
  }

  public void recordHealthyLatency(double latency) {
    healthyRequestLatencySensor.get().record(latency);
  }

  public void recordRequestKeyCount(int keyCount) {
    requestKeyCountSensor.get().record(keyCount);
  }

  public void recordSuccessRequestKeyCount(int successKeyCount) {
    successRequestKeyCountSensor.get().record(successKeyCount);
  }

  protected final Rate getRequestRate() {
    return requestRate;
  }

  protected final Rate getSuccessRequestKeyCountRate() {
    return successRequestKeyCountRate;
  }
}
