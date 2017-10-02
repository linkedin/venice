package com.linkedin.venice.stats;

import io.tehuti.metrics.*;
import io.tehuti.metrics.stats.OccurrenceRate;
import io.tehuti.metrics.stats.Percentile;
import io.tehuti.metrics.stats.Percentiles;

import io.tehuti.metrics.stats.Rate;
import io.tehuti.metrics.stats.SampledCount;
import java.util.Arrays;
import javax.validation.constraints.NotNull;

/**
 * Utils for venice metrics
 */
public class TehutiUtils {

  private static final int DEFAULT_HISTOGRAM_SIZE_IN_BYTES = 40000;
  private static final double DEFAULT_HISTOGRAM_MAX_VALUE = 10000;
  private static final double[] DEFAULT_HISTOGRAM_PERCENTILES = new double[]{50, 95, 99};
  private static final double[] HISTOGRAM_PERCENTILES_FOR_NETWORK_LATENCY = new double[]{50, 77, 90, 95, 99, 99.9};
  private static final String ROUND_NUMBER_SUFFIX = ".0";

  /**
   * TODO: need to investigate why percentiles with big values (> 10^6) won't show in InGraph.
   */
  public static Percentiles getPercentileStat(String sensorName, String storeName) {
    String name = sensorName + AbstractVeniceStats.DELIMITER + storeName;
    return getPercentileStat(name, DEFAULT_HISTOGRAM_SIZE_IN_BYTES, DEFAULT_HISTOGRAM_MAX_VALUE);
  }

  /**
   * Generate a histogram stat that emits P50, P77, P90, P95, P99 and P99.9 values.
   *
   * N.B.: These are useful percentiles to estimate the latency we would get with speculative queries:
   *
   * P77 latency with one query would become: P95 with the fastest of two queries, and P99 with the fastest of three
   * P90 latency with one query would become: P99 with the fastest of two queries, and P99.9 with the fastest of three
   */
  public static Percentiles getPercentileStatForNetworkLatency(String sensorName, String storeName) {
    String name = sensorName + AbstractVeniceStats.DELIMITER + storeName;
    return getPercentileStat(name, DEFAULT_HISTOGRAM_SIZE_IN_BYTES, DEFAULT_HISTOGRAM_MAX_VALUE, HISTOGRAM_PERCENTILES_FOR_NETWORK_LATENCY);
  }

  /**
   * Generate a histogram stat that emits P50, P95, and P99 values.
   * @param name
   * @param sizeInBytes Histogram's memory consumption
   * @param max Histogram's max value
   * @return 3 sub stats that emit p50, P95, and P99 values.
   */
  public static Percentiles getPercentileStat(@NotNull String name, int sizeInBytes, double max) {
    return getPercentileStat(name, sizeInBytes, max, DEFAULT_HISTOGRAM_PERCENTILES);
  }

  public static Percentiles getPercentileStat(@NotNull String name, int sizeInBytes, double max, double... percentiles) {
    Percentile[] percentileObjectsArray = Arrays.stream(percentiles)
        .mapToObj(percentile -> getPercentile(name, percentile))
        .toArray(value -> new Percentile[percentiles.length]);
    return new Percentiles(sizeInBytes, max, Percentiles.BucketSizing.LINEAR, percentileObjectsArray);
  }

  private static Percentile getPercentile(String name, double percentile) {
    String stringPercentile = Double.toString(percentile);
    // Clip decimals for round numbers.
    if (stringPercentile.endsWith(ROUND_NUMBER_SUFFIX)) {
      stringPercentile = stringPercentile.substring(0, stringPercentile.length() - ROUND_NUMBER_SUFFIX.length());
    }
    return new Percentile(name + "." + stringPercentile + "thPercentile", percentile);
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
