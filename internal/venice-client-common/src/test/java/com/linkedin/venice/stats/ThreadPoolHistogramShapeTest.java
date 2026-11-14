package com.linkedin.venice.stats;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.utils.metrics.MetricsRepositoryUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.sdk.metrics.data.ExponentialHistogramPointData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ThreadPoolHistogramShapeTest {
  @DataProvider
  public Object[][] aggregations() {
    return new Object[][] { { ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_DISTRIBUTION, false },
        { ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_DISTRIBUTION, true },
        { ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_REQUEST_DISTRIBUTION, false },
        { ThreadPoolOtelMetricEntity.THREAD_POOL_THREAD_ACTIVE_REQUEST_DISTRIBUTION, true } };
  }

  @Test(dataProvider = "aggregations")
  public void testRepresentativeRequestDistribution(ThreadPoolOtelMetricEntity definition, boolean exponential) {
    MetricEntity source = definition.getMetricEntity();
    // Model the percentile-only change without changing the production definitions in this test-only PR.
    MetricEntity histogram = new MetricEntity(
        source.getMetricName(),
        MetricType.HISTOGRAM,
        source.getUnit(),
        source.getDescription(),
        source.getDimensionsList());
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    VeniceMetricsConfig.Builder config = new VeniceMetricsConfig.Builder().setMetricPrefix("server")
        .setMetricEntities(Collections.singletonList(histogram))
        .setEmitOtelMetrics(true)
        .setOtelAdditionalMetricsReader(reader)
        .setTehutiMetricConfig(MetricsRepositoryUtils.createDefaultSingleThreadedMetricConfig());
    if (!exponential) {
      config.setUseOtelExponentialHistogram(false);
    }
    try (VeniceMetricsRepository repository = new VeniceMetricsRepository(config.build())) {
      assertEquals(repository.getVeniceMetricsConfig().useOtelExponentialHistogram(), exponential);
      List<Integer> samples = new ArrayList<>();
      samples.add(0);
      for (int value = 1; value <= 400; value++) {
        samples.addAll(Collections.nCopies(value <= 100 ? 90 : 1, value));
      }
      assertEquals(samples.size(), 9301);
      assertEquals(samples.get(0).intValue(), 0);
      assertEquals(samples.get(samples.size() - 1).intValue(), 400);
      DoubleHistogram instrument = repository.getOpenTelemetryMetricsRepository().createDoubleHistogram(histogram);
      samples.forEach(value -> instrument.record(value, Attributes.empty()));
      MetricData metric = reader.collectAllMetrics()
          .stream()
          .filter(data -> data.getName().equals("venice.server." + source.getMetricName()))
          .findFirst()
          .orElseThrow(() -> new AssertionError("Missing request histogram"));
      List<Double> upperBounds = new ArrayList<>();
      List<Long> counts = new ArrayList<>();
      if (exponential) {
        assertEquals(metric.getType(), MetricDataType.EXPONENTIAL_HISTOGRAM);
        ExponentialHistogramPointData point = metric.getExponentialHistogramData().getPoints().iterator().next();
        assertEquals(point.getCount(), samples.size());
        assertEquals(point.getScale(), 5);
        upperBounds.add(0.0);
        counts.add(point.getZeroCount());
        for (int i = 0; i < point.getPositiveBuckets().getBucketCounts().size(); i++) {
          int index = point.getPositiveBuckets().getOffset() + i;
          upperBounds.add(Math.pow(2, Math.scalb(index + 1.0, -point.getScale())));
          counts.add(point.getPositiveBuckets().getBucketCounts().get(i));
        }
      } else {
        assertEquals(metric.getType(), MetricDataType.HISTOGRAM);
        HistogramPointData point = metric.getHistogramData().getPoints().iterator().next();
        assertEquals(point.getCount(), samples.size());
        upperBounds.addAll(point.getBoundaries());
        upperBounds.add(Double.POSITIVE_INFINITY);
        counts.addAll(point.getCounts());
      }
      assertEquals(counts.stream().mapToLong(Long::longValue).sum(), samples.size());
      double[] quantiles = { 0.50, 0.95, 0.99 };
      int[] exactPercentiles = { 52, 99, 307 };
      double[] expectedEstimates =
          exponential ? new double[] { 52.288014, 98.881600, 307.294008 } : new double[] { 51.666667, 98.166667, 345 };
      double lower = 0;
      long cumulative = 0;
      int sampleIndex = 0;
      int checkedQuantiles = 0;
      for (int i = 0; i < counts.size(); i++) {
        double upper = upperBounds.get(i);
        int first = sampleIndex;
        while (sampleIndex < samples.size() && samples.get(sampleIndex) <= upper) {
          sampleIndex++;
        }
        assertEquals(counts.get(i).longValue(), sampleIndex - first, "Bucket ending at " + upper);
        for (int q = 0; q < quantiles.length; q++) {
          double quantile = quantiles[q];
          long rank = (long) Math.ceil(quantile * samples.size());
          if (cumulative < rank && rank <= cumulative + counts.get(i)) {
            int exact = samples.get((int) rank - 1);
            double estimate = lower + (upper - lower) * (rank - cumulative) / counts.get(i);
            assertEquals(exact, exactPercentiles[q]);
            assertTrue(lower <= exact && exact <= upper);
            assertTrue(Double.isFinite(estimate), "Quantile must not land in overflow");
            assertEquals(estimate, expectedEstimates[q], 0.000001, "Linear bucket interpolation at p" + quantile * 100);
            checkedQuantiles++;
            System.out.printf(
                "%s %s p%.0f exact=%d bucket=(%.6f,%.6f] estimate=%.6f error=%.6f%n",
                source.getMetricName(),
                exponential ? "exponential" : "explicit",
                quantile * 100,
                exact,
                lower,
                upper,
                estimate,
                estimate - exact);
          }
        }
        cumulative += counts.get(i);
        lower = upper;
      }
      assertEquals(checkedQuantiles, quantiles.length);
    }
  }
}
