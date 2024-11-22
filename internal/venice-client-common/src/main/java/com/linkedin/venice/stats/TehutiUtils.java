package com.linkedin.venice.stats;

import io.tehuti.metrics.JmxReporter;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.stats.Avg;
import io.tehuti.metrics.stats.Max;
import io.tehuti.metrics.stats.Percentile;
import io.tehuti.metrics.stats.Percentiles;
import io.tehuti.metrics.stats.Rate;
import java.util.Arrays;


/**
 * Utils for venice metrics
 */
public class TehutiUtils {
  private static final int DEFAULT_HISTOGRAM_SIZE_IN_BYTES = 40000;
  private static final double DEFAULT_HISTOGRAM_MAX_VALUE = 10000;
  private static final double[] DEFAULT_HISTOGRAM_PERCENTILES = new double[] { 50, 95, 99 };

  // a fine grained percentiles. Please use it with cautions as it will emit more 20
  // metrics. It's likely to degrade critical path performance
  private static final double[] FINE_GRAINED_HISTOGRAM_PERCENTILES =
      new double[] { 0.01, 0.1, 1, 2, 3, 4, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 99, 99.9 };
  private static final double[] HISTOGRAM_PERCENTILES_FOR_NETWORK_LATENCY = new double[] { 50, 77, 90, 95, 99, 99.9 };
  private static final String ROUND_NUMBER_SUFFIX = ".0";

  /**
   * TODO: need to investigate why percentiles with big values (> 10^6) won't show in InGraph.
   */
  public static Percentiles getPercentileStat(String sensorName, String storeName) {
    String name = sensorName + AbstractVeniceStats.DELIMITER + storeName;
    return getPercentileStat(name, DEFAULT_HISTOGRAM_SIZE_IN_BYTES, DEFAULT_HISTOGRAM_MAX_VALUE);
  }

  public static MeasurableStat[] getPercentileStatWithAvgAndMax(String sensorName, String storeName) {
    String name = sensorName + AbstractVeniceStats.DELIMITER + storeName;
    return new MeasurableStat[] { getPercentileStat(name, DEFAULT_HISTOGRAM_SIZE_IN_BYTES, DEFAULT_HISTOGRAM_MAX_VALUE),
        new Avg(), new Max() };
  }

  public static MeasurableStat[] getFineGrainedPercentileStatWithAvgAndMax(String sensorName, String storeName) {
    String name = sensorName + AbstractVeniceStats.DELIMITER + storeName;
    return new MeasurableStat[] {
        getFineGrainedPercentileStat(name, DEFAULT_HISTOGRAM_SIZE_IN_BYTES, DEFAULT_HISTOGRAM_MAX_VALUE), new Avg(),
        new Max() };
  }

  public static Percentiles getFineGrainedPercentileStat(String sensorName, String storeName) {
    String name = sensorName + AbstractVeniceStats.DELIMITER + storeName;
    return getFineGrainedPercentileStat(name, DEFAULT_HISTOGRAM_SIZE_IN_BYTES, DEFAULT_HISTOGRAM_MAX_VALUE);
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
    return getPercentileStat(
        name,
        DEFAULT_HISTOGRAM_SIZE_IN_BYTES,
        DEFAULT_HISTOGRAM_MAX_VALUE,
        HISTOGRAM_PERCENTILES_FOR_NETWORK_LATENCY);
  }

  /**
   * Generate a histogram stat that emits P50, P95, and P99 values.
   * @param name
   * @return 3 sub stats that emit p50, P95, and P99 values.
   */
  public static Percentiles getPercentileStat(String name) {
    return getPercentileStat(
        name,
        DEFAULT_HISTOGRAM_SIZE_IN_BYTES,
        DEFAULT_HISTOGRAM_MAX_VALUE,
        DEFAULT_HISTOGRAM_PERCENTILES);
  }

  /**
   * Generate a histogram stat that emits P50, P95, and P99 values.
   * @param name
   * @param sizeInBytes Histogram's memory consumption
   * @param max Histogram's max value
   * @return 3 sub stats that emit p50, P95, and P99 values.
   */
  public static Percentiles getPercentileStat(String name, int sizeInBytes, double max) {
    return getPercentileStat(name, sizeInBytes, max, DEFAULT_HISTOGRAM_PERCENTILES);
  }

  public static Percentiles getFineGrainedPercentileStat(String name, int sizeInBytes, double max) {
    return getPercentileStat(name, sizeInBytes, max, FINE_GRAINED_HISTOGRAM_PERCENTILES);
  }

  public static Percentiles getPercentileStat(String name, int sizeInBytes, double max, double... percentiles) {
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

    // If there are still any dots left, replace them (i.e.: this is for the 99.9th percentile)
    if (stringPercentile.contains(".")) {
      stringPercentile = stringPercentile.replace(".", "_");
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
   * A valid metric name needs to pass the test in {@link javax.management.ObjectName}. This helper function will
   * try to fix all invalid character mentioned in the above function to avoid MalformedObjectNameException; besides,
   * dot(.) will also be replaced since dot is a separator used in InGraph.
   */
  public static String fixMalformedMetricName(String metricName) {
    return metricName.replaceAll("[\\*?=:\".]", "_");
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
    public RatioStat(Rate one, Rate two, String metricName) {
      super((config, now) -> {
        double numerator = one.measure(config, now);
        double denominator = two.measure(config, now);

        if (numerator + denominator == 0) {
          /** TODO: Consider whether we should use a {@link StatsErrorCode} here */
          return Double.NaN;
        }

        return numerator / (numerator + denominator);
      }, metricName);
    }
  }

  /**
   * Generate a ratio stat that is based on two arbitrary {@link MeasurableStat}. It calculates the proportion of
   * numerator stat over the denominator stat.
   */
  public static class SimpleRatioStat extends LambdaStat {
    public SimpleRatioStat(Rate numeratorStat, Rate denominatorStat, String metricName) {
      super((config, now) -> {
        double numerator = numeratorStat.measure(config, now);
        double denominator = denominatorStat.measure(config, now);

        if (denominator == 0) {
          /** TODO: Consider whether we should use a {@link StatsErrorCode} here */
          return Double.NaN;
        }

        return numerator / denominator;
      }, metricName);
    }
  }
}
