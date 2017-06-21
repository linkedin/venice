package com.linkedin.venice.stats;

import io.tehuti.metrics.*;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Percentile;
import io.tehuti.metrics.stats.Percentiles;

import io.tehuti.metrics.stats.Rate;
import io.tehuti.metrics.stats.SampledCount;
import javax.validation.constraints.NotNull;

/**
 * Utils for venice metrics
 */
public class TehutiUtils {
  public static Percentiles getPercentileStat(String sensorName, String storeName) {
    return getPercentileStat(sensorName + AbstractVeniceStats.DELIMITER + storeName);

  }

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
    return new Percentiles(sizeInBytes, max, Percentiles.BucketSizing.LINEAR,
        new Percentile(name + ".50thPercentile", 50),
        new Percentile(name + ".95thPercentile", 95),
        new Percentile(name + ".99thPercentile", 99));
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

  /**
   * Generate a ratio stat that is based on two arbitrary {@link Rate}. It calculates the proportion of one
   * Stat over the sum of two Stats. This method is mostly used to calculate the bad request ratio (bad / (good + bad))
   *
   * Use {@link Rate} instead of {@link io.tehuti.metrics.stats.SampledStat} to capture the event rate, so that the result
   * won't get impacted by the un-aligned windows introduced by {@link io.tehuti.metrics.stats.SampledStat}.
   *
   * More background: {@link io.tehuti.metrics.stats.SampledStat} is not maintaining sliding windows based on absolute time, and the new window
   * creation is triggered by new event, considering the event for different metrics could happen at different rate/time,
   * the sliding-windows being used by different metrics will be different. In this case, if we want to emit the ratio
   * of two metrics, the result won't reflect the actual data since the nominator could use the window starting from time: t1,
   * and the denominator could use the window starting from time: t2.
   * By using {@link Rate}, the ratio metric will be more reasonable since it doesn't depend on the actual window size/starting time,
   * but the average event frequency. It is still not perfect, since both nominator and denominator metrics could still
   * use different window size/starting time.
   * The ultimate solution should be to use the consistent windowing (based on absolute time) across all the metrics.
   */
  public static class RatioStat extends LambdaStat {
    public RatioStat(Rate one, Rate two) {
      this(one, new MetricConfig(), two, new MetricConfig());
    }
    public RatioStat(Rate one, MetricConfig c1, Rate two, MetricConfig c2) {
      super(() -> {
        double numerator = one.measure(c1, 0);
        double denominator = two.measure(c2, 0);

        if (numerator + denominator == 0) {
          return Double.NaN;
        }

        return numerator / (numerator + denominator);
      });
    }
  }

  /**
   * Generate a ratio stat that is based on two arbitrary {@link MeasurableStat}. It calculates the proportion of
   * numerator stat over the denominator stat.
   */
  public static class SimpleRatioStat extends LambdaStat {
    public SimpleRatioStat(Rate numeratorStat, Rate denominatorStat) {
      this(numeratorStat, new MetricConfig(), denominatorStat, new MetricConfig());
    }
    public SimpleRatioStat(Rate numeratorStat, MetricConfig numeatorConfig,
        Rate denominatorStat, MetricConfig demoniatorConfig) {
      super(() -> {
        double numerator = numeratorStat.measure(numeatorConfig, 0);
        double denominator = denominatorStat.measure(demoniatorConfig, 0);

        if (denominator == 0) {
          return Double.NaN;
        }

        return numerator / denominator;
      });
    }
  }
}
