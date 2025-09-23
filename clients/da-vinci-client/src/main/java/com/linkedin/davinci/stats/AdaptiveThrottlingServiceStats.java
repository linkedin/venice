package com.linkedin.davinci.stats;

import com.linkedin.venice.stats.AbstractVeniceStats;
import com.linkedin.venice.throttle.VeniceAdaptiveThrottler;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Rate;
import java.util.HashMap;
import java.util.Map;


public class AdaptiveThrottlingServiceStats extends AbstractVeniceStats {
  private static final String ADAPTIVE_THROTTLING_SERVICE_SUFFIX = "AdaptiveThrottlingService";
  private final Map<String, Sensor> sensors = new HashMap<>();

  public AdaptiveThrottlingServiceStats(MetricsRepository metricsRepository) {
    super(metricsRepository, ADAPTIVE_THROTTLING_SERVICE_SUFFIX);
  }

  public void registerSensorForThrottler(VeniceAdaptiveThrottler throttler) {
    String throttlerName = throttler.getThrottlerName();
    sensors.put(throttlerName, registerSensorIfAbsent(throttlerName, new Rate()));
  }

  public void recordRateForAdaptiveThrottler(VeniceAdaptiveThrottler throttler, int rate) {
    Sensor sensor = sensors.get(throttler.getThrottlerName());
    if (sensor != null) {
      sensor.record(rate);
    }
  }
}
