package com.linkedin.venice.stats;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.export.MemoryMode;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


/**
 * A MetricExporter that splits the input metrics into batches and exports each batch separately.
 * maxBatchSize is the maximum number of metric points allowed in a single batch.
 *
 * For instance, the below metric has metric points and if maxBatchSize is 1, then this will be
 * split into 2 batches:
 * ....
 * name=venice.thin_client.call_time, description=Latency based on all responses, unit=MILLISECOND,
 * type=EXPONENTIAL_HISTOGRAM, data=ImmutableExponentialHistogramData{aggregationTemporality=DELTA,
 * points=[
 * ImmutableExponentialHistogramPointData{......},
 * ImmutableExponentialHistogramPointData{......}
 * ]}},
 * ....
 */
public class SplitOtlpHttpMetricExporter implements MetricExporter {
  private final MetricExporter delegate;
  private final int maxBatchSize;

  public SplitOtlpHttpMetricExporter(MetricExporter delegate, int maxBatchSize) {
    this.delegate = delegate;
    this.maxBatchSize = maxBatchSize;
  }

  /**
   * Export the input metrics in batches by leveraging the delegate exporter.
   */
  @Override
  public CompletableResultCode export(Collection<MetricData> metrics) {
    Collection<CompletableResultCode> results = new ArrayList<>();
    splitIntoBatches(metrics, maxBatchSize, results);
    return CompletableResultCode.ofAll(results);
  }

  @Override
  public CompletableResultCode flush() {
    return delegate.flush();
  }

  @Override
  public CompletableResultCode shutdown() {
    return delegate.shutdown();
  }

  @Override
  public MemoryMode getMemoryMode() {
    return delegate.getMemoryMode();
  }

  @Override
  public Aggregation getDefaultAggregation(InstrumentType instrumentType) {
    return delegate.getDefaultAggregation(instrumentType);
  }

  @Override
  public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
    return delegate.getAggregationTemporality(instrumentType);
  }

  /**
   * Splits a collection of {@link MetricData} into batches and exports them based on a specified maximum batch size.
   *
   * <p>The splitting logic follows these rules:
   * <ul>
   *   <li>Metrics are added to a batch until adding another would exceed {@code maxBatchSize}.</li>
   *   <li>If adding a metric would exceed the batch size, the current batch is exported first.</li>
   *   <li>Large metrics (where the number of points exceeds {@code maxBatchSize}) are split into chunks:
   *     <ul>
   *       <li>Full-sized chunks ({@code maxBatchSize}) are exported immediately.</li>
   *       <li>The last chunk (if smaller than {@code maxBatchSize}) is added to a new batch.</li>
   *     </ul>
   *   </li>
   *   <li>Before processing a large metric, any existing batch is exported to prevent unnecessary splits.</li>
   *   <li>Any remaining batch after iteration is exported at the end.</li>
   * </ul>
   * </p>
   *
   * @param metrics       The collection of {@link MetricData} to be batched and exported.
   * @param maxBatchSize  The maximum number of metric points allowed per batch.
   * @param results       A collection to store the result codes of each export operation.
   */
  private void splitIntoBatches(
      Collection<MetricData> metrics,
      int maxBatchSize,
      Collection<CompletableResultCode> results) {
    List<MetricData> currentBatch = new ArrayList<>();
    int currentBatchSize = 0;

    for (MetricData metricData: metrics) {
      int metricSize = metricData.getData().getPoints().size();

      // Export current batch before splitting a large MetricData to reduce number of split
      if (!currentBatch.isEmpty() && metricSize > maxBatchSize) {
        results.add(delegate.export(currentBatch));
        currentBatch = new ArrayList<>();
        currentBatchSize = 0;
      }

      if (metricSize > maxBatchSize) {
        int startIndex = 0;
        while (startIndex < metricSize) {
          int endIndex = Math.min(startIndex + maxBatchSize, metricSize);
          MetricData subMetricData = new MetricDataRangeDecorator<>(metricData, startIndex, endIndex);

          if (endIndex - startIndex == maxBatchSize) {
            // Export full-sized split immediately
            results.add(delegate.export(Collections.singletonList(subMetricData)));
          } else {
            currentBatch.add(subMetricData);
            currentBatchSize += endIndex - startIndex;
          }
          startIndex = endIndex;
        }
      } else {
        if (currentBatchSize + metricSize > maxBatchSize) {
          results.add(delegate.export(currentBatch));
          currentBatch = new ArrayList<>();
          currentBatchSize = 0;
        }

        currentBatch.add(metricData);
        currentBatchSize += metricSize;
      }
    }

    // Export any remaining batch
    if (!currentBatch.isEmpty()) {
      results.add(delegate.export(currentBatch));
    }
  }
}
