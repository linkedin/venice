package com.linkedin.davinci.stats;

import com.linkedin.davinci.kafka.consumer.VeniceAdaptiveIngestionThrottler;
import com.linkedin.venice.stats.AbstractVeniceStats;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Gauge;
import java.util.HashMap;
import java.util.Map;


public class AdaptiveThrottlingServiceStats extends AbstractVeniceStats {
  private static final String ADAPTIVE_THROTTLING_SERVICE_SUFFIX = "-AdaptiveThrottlingService";
  private final Map<String, Sensor> sensors = new HashMap<>();

  public AdaptiveThrottlingServiceStats(MetricsRepository metricsRepository) {
    super(metricsRepository, ADAPTIVE_THROTTLING_SERVICE_SUFFIX);
  }

  public void registerSensorForThrottler(VeniceAdaptiveIngestionThrottler throttler) {
    String throttlerName = throttler.getThrottlerName();
    sensors.put(throttlerName, registerSensorIfAbsent(throttlerName, new Gauge()));
  }

  public void recordThrottleLimitForThrottler(VeniceAdaptiveIngestionThrottler throttler) {
    Sensor sensor = sensors.get(throttler.getThrottlerName());
    if (sensor != null) {
      sensor.record(throttler.getCurrentThrottlerRate());
    }
  }
}
