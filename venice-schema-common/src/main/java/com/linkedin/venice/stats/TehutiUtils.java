package com.linkedin.venice.stats;

import io.tehuti.metrics.JmxReporter;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Percentile;
import io.tehuti.metrics.stats.Percentiles;

import javax.validation.constraints.NotNull;

public class TehutiUtils {
  public static Percentiles getPercentileStat(@NotNull String name) {
    return getPercentileStat(name, 40000, 10000);
  }

  public static Percentiles getPercentileStat(@NotNull String name, int sizeInBytes, double max) {
    return new Percentiles(sizeInBytes, max, Percentiles.BucketSizing.CONSTANT,
        new Percentile(name + ".50thPercentile", 50),
        new Percentile(name + ".95thPercentile", 95),
        new Percentile(name + ".99thPercentile", 99));
  }

  public static MetricsRepository getMetricsRepository(String serviceName) {
    MetricsRepository metricsRepository = new MetricsRepository();
    metricsRepository.addReporter(new JmxReporter(serviceName));
    return metricsRepository;
  }
}
