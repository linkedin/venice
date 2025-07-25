package com.linkedin.venice.stats.metrics;

import static com.linkedin.venice.utils.OpenTelemetryDataPointTestUtils.validateExponentialHistogramPointData;
import static com.linkedin.venice.utils.OpenTelemetryDataPointTestUtils.validateHistogramPointData;
import static com.linkedin.venice.utils.OpenTelemetryDataPointTestUtils.validateLongPointDataFromCounter;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricConfig;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;


public class MetricTypeTest {
  @Test
  public void testOTelRecordForDifferentTypes() {
    Set<VeniceMetricsDimensions> dimensionsSet = new HashSet<>();
    dimensionsSet.add(VeniceMetricsDimensions.VENICE_REQUEST_METHOD);
    MetricEntity metricEntityCounter = new MetricEntity(
        "test_metric_counter",
        MetricType.COUNTER,
        MetricUnit.NUMBER,
        "Test description",
        dimensionsSet);
    MetricEntity metricEntityHistogram = new MetricEntity(
        "test_metric_hist",
        MetricType.HISTOGRAM,
        MetricUnit.MILLISECOND,
        "Test description",
        dimensionsSet);
    MetricEntity metricEntityMinMaxCountSumAggregations = new MetricEntity(
        "test_metric_min_max_count_sum_aggregations",
        MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
        MetricUnit.NUMBER,
        "Test description",
        dimensionsSet);
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsConfig metricsConfig = new VeniceMetricsConfig.Builder().setEmitOtelMetrics(true)
        .setMetricPrefix("test_prefix")
        .setOtelAdditionalMetricsReader(inMemoryMetricReader)
        .setMetricEntities(
            Arrays.asList(metricEntityCounter, metricEntityHistogram, metricEntityMinMaxCountSumAggregations))
        .setTehutiMetricConfig(new MetricConfig())
        .build();
    VeniceOpenTelemetryMetricsRepository otelMetricsRepository =
        new VeniceOpenTelemetryMetricsRepository(metricsConfig);

    Map<VeniceMetricsDimensions, String> baseDimensionsMap = new HashMap<>();
    baseDimensionsMap
        .put(VeniceMetricsDimensions.VENICE_REQUEST_METHOD, RequestType.MULTI_GET_STREAMING.getDimensionValue());
    Attributes baseAttributes = Attributes.builder()
        .put(
            VeniceMetricsDimensions.VENICE_REQUEST_METHOD
                .getDimensionName(VeniceOpenTelemetryMetricNamingFormat.getDefaultFormat()),
            RequestType.MULTI_GET_STREAMING.getDimensionValue())
        .build();
    MetricEntityStateBase metricEntityStateBaseCounter =
        MetricEntityStateBase.create(metricEntityCounter, otelMetricsRepository, baseDimensionsMap, baseAttributes);
    MetricEntityStateBase metricEntityStateBaseHistogram =
        MetricEntityStateBase.create(metricEntityHistogram, otelMetricsRepository, baseDimensionsMap, baseAttributes);
    MetricEntityStateBase metricEntityStateBaseMinMaxCountSumAggregations = MetricEntityStateBase
        .create(metricEntityMinMaxCountSumAggregations, otelMetricsRepository, baseDimensionsMap, baseAttributes);

    // Record values for the metric
    int[] values = { 10, 20, 30, 40, 50 }; // total 150
    for (int value: values) {
      metricEntityStateBaseCounter.record(value);
      metricEntityStateBaseHistogram.record(value);
      metricEntityStateBaseMinMaxCountSumAggregations.record(value);
    }

    // Check if the metric is recorded in otel
    Collection<MetricData> metrics = inMemoryMetricReader.collectAllMetrics();

    // Assert recorded values
    assertFalse(metrics.isEmpty(), "Metrics should not be empty");
    assertEquals(metrics.size(), 3, "There should be three metrics recorded");

    validateLongPointDataFromCounter(inMemoryMetricReader, 150, baseAttributes, "test_metric_counter", "test_prefix");
    validateExponentialHistogramPointData(
        inMemoryMetricReader,
        10.0,
        50.0,
        5,
        150.0,
        baseAttributes,
        "test_metric_hist",
        "test_prefix");
    validateHistogramPointData(
        inMemoryMetricReader,
        10.0,
        50.0,
        5,
        150.0,
        baseAttributes,
        "test_metric_min_max_count_sum_aggregations",
        "test_prefix");
  }
}
