package com.linkedin.venice.stats;

import io.tehuti.metrics.*;
import io.tehuti.metrics.stats.Percentile;
import io.tehuti.metrics.stats.Percentiles;

import javax.validation.constraints.NotNull;

/**
 * Utils for venice metrics
 */
public class TehutiUtils {
  public static Percentiles getPercentileStat(@NotNull String name) {
    return getPercentileStat(name, 40000, 10000);
  }

  /**
   *Generate a histogram stat that emits P50, P95, and P99 values.
   * @param name
   * @param sizeInBytes Histogram's memory consumption
   * @param max Histogram's max value
   * @return 3 sub stats that emit p50, P95, and P99 values.
   */
  public static Percentiles getPercentileStat(@NotNull String name, int sizeInBytes, double max) {
    return new Percentiles(sizeInBytes, max, Percentiles.BucketSizing.CONSTANT,
        new Percentile(name + ".50thPercentile", 50),
        new Percentile(name + ".95thPercentile", 95),
        new Percentile(name + ".99thPercentile", 99));
  }

  /**
   * {@link TehutiUtils#getRatioStat(MeasurableStat, MetricConfig, MeasurableStat, MetricConfig)} with the
   * default MetricConfigs.
   */

  public static LambdaStat getRatioStat(MeasurableStat one, MeasurableStat two) {
    return getRatioStat(one, new MetricConfig(), two, new MetricConfig());
  }

  /**
   * Generate a ratio stat that is based on two arbitrary {@link MeasurableStat}. It calculates the proportion of one
   * Stat over the sum of two Stats. This method is mostly used to calculate the bad request ratio (bad / (good + bad))
   * @param one First stats, it is also the numerator
   * @param c1  MetricConfig of the first Stats
   * @param two Second stats.
   * @param c2 MetricConfig of the second Stats
   * @return
   */
  public static LambdaStat getRatioStat(MeasurableStat one, MetricConfig c1, MeasurableStat two, MetricConfig c2) {
    return new LambdaStat(() -> {
      double numerator = one.measure(c1, 0);
      double denominator = two.measure(c2, 0);

      if (numerator + denominator == 0) {
        return Double.NaN;
      }

      return numerator / (numerator + denominator);
    });
  }

  /**
   * Get default MetricRepository. It will emit metrics via JMX.
   * @param serviceName Prefix name of all emitted metrics
   * @return
   */

  public static MetricsRepository getMetricsRepository(String serviceName) {
    MetricsRepository metricsRepository = new MetricsRepository();
    metricsRepository.addReporter(new JmxReporter(serviceName));
    return metricsRepository;
  }
}
