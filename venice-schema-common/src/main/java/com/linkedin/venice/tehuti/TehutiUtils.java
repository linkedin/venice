package com.linkedin.venice.tehuti;

import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Percentile;
import io.tehuti.metrics.stats.Percentiles;

import javax.validation.constraints.NotNull;

public class TehutiUtils {
  //TODO: The granularity in Percentiles should be configurable in the future.
  public static Sensor getPercentileSensor(@NotNull MetricsRepository metricsRepository, String name) {
    Sensor sensor = metricsRepository.sensor(name);
    sensor.add(new Percentiles(40000, 10000, Percentiles.BucketSizing.CONSTANT,
      new Percentile(name + ".50thPercentile", 50),
      new Percentile(name + ".95thPercentile", 95),
      new Percentile(name + ".99thPercentile", 99)));

    return sensor;
  }
}
