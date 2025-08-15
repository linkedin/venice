package com.linkedin.venice.stats.metrics;

import static com.linkedin.venice.stats.metrics.MetricType.HISTOGRAM;
import static com.linkedin.venice.utils.OpenTelemetryDataPointTestUtils.validateExponentialHistogramPointData;
import static com.linkedin.venice.utils.OpenTelemetryDataPointTestUtils.validateHistogramPointData;
import static com.linkedin.venice.utils.OpenTelemetryDataPointTestUtils.validateLongPointDataFromCounter;
import static com.linkedin.venice.utils.OpenTelemetryDataPointTestUtils.validateLongPointDataFromGauge;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.utils.DataProviderUtils;
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
import java.util.function.LongSupplier;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link MetricType}.
 */
public class MetricTypeTest {
  private static final String METRIC_PREFIX = "test_prefix";
  private static final String TEST_DESCRIPTION = "Test description";
  private static final VeniceMetricsDimensions DIMENSION = VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
  private static final String DIMENSION_VALUE = RequestType.MULTI_GET_STREAMING.getDimensionValue();

  private static Set<VeniceMetricsDimensions> getTestDimensions() {
    Set<VeniceMetricsDimensions> dimensionsSet = new HashSet<>();
    dimensionsSet.add(DIMENSION);
    return dimensionsSet;
  }

  private static Map<VeniceMetricsDimensions, String> getBaseDimensionsMap() {
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = new HashMap<>();
    baseDimensionsMap.put(DIMENSION, DIMENSION_VALUE);
    return baseDimensionsMap;
  }

  private static Attributes getBaseAttributes() {
    return Attributes.builder()
        .put(DIMENSION.getDimensionName(VeniceOpenTelemetryMetricNamingFormat.getDefaultFormat()), DIMENSION_VALUE)
        .build();
  }

  private static VeniceOpenTelemetryMetricsRepository createOtelRepo(
      MetricEntity metricEntity,
      InMemoryMetricReader inMemoryMetricReader) {
    VeniceMetricsConfig metricsConfig = new VeniceMetricsConfig.Builder().setEmitOtelMetrics(true)
        .setMetricPrefix(METRIC_PREFIX)
        .setOtelAdditionalMetricsReader(inMemoryMetricReader)
        .setMetricEntities(Arrays.asList(metricEntity))
        .setTehutiMetricConfig(new MetricConfig())
        .build();
    return new VeniceOpenTelemetryMetricsRepository(metricsConfig);
  }

  @Test
  public void testOTelRecordCounter() {
    MetricEntity metricEntityCounter = new MetricEntity(
        "test_metric_counter",
        MetricType.COUNTER,
        MetricUnit.NUMBER,
        TEST_DESCRIPTION,
        getTestDimensions());
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceOpenTelemetryMetricsRepository otelMetricsRepository =
        createOtelRepo(metricEntityCounter, inMemoryMetricReader);
    MetricEntityStateBase metricEntityStateBaseCounter = MetricEntityStateBase
        .create(metricEntityCounter, otelMetricsRepository, getBaseDimensionsMap(), getBaseAttributes());
    int[] values = { 10, 20, 30, 40, 50 };
    for (int value: values) {
      metricEntityStateBaseCounter.record(value);
    }
    Collection<MetricData> metrics = inMemoryMetricReader.collectAllMetrics();
    assertFalse(metrics.isEmpty(), "Metrics should not be empty");
    assertEquals(metrics.size(), 1, "There should be one metric recorded");
    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        150,
        getBaseAttributes(),
        "test_metric_counter",
        METRIC_PREFIX);
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testOTelRecordHistogram(boolean isExponentialHistogram) {
    MetricEntity metricEntityHistogram = new MetricEntity(
        "test_metric_hist",
        isExponentialHistogram ? HISTOGRAM : MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
        MetricUnit.MILLISECOND,
        TEST_DESCRIPTION,
        getTestDimensions());
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceOpenTelemetryMetricsRepository otelMetricsRepository =
        createOtelRepo(metricEntityHistogram, inMemoryMetricReader);
    MetricEntityStateBase metricEntityStateBaseHistogram = MetricEntityStateBase
        .create(metricEntityHistogram, otelMetricsRepository, getBaseDimensionsMap(), getBaseAttributes());
    int[] values = { 10, 20, 30, 40, 50 };
    for (int value: values) {
      metricEntityStateBaseHistogram.record(value);
    }
    Collection<MetricData> metrics = inMemoryMetricReader.collectAllMetrics();
    assertFalse(metrics.isEmpty(), "Metrics should not be empty");
    assertEquals(metrics.size(), 1, "There should be one metric recorded");
    if (isExponentialHistogram) {
      validateExponentialHistogramPointData(
          inMemoryMetricReader,
          10.0,
          50.0,
          5,
          150.0,
          getBaseAttributes(),
          "test_metric_hist",
          METRIC_PREFIX);
    } else {
      validateHistogramPointData(
          inMemoryMetricReader,
          10.0,
          50.0,
          5,
          150.0,
          getBaseAttributes(),
          "test_metric_hist",
          METRIC_PREFIX);
    }
  }

  @Test
  public void testOTelRecordGauge() {
    MetricEntity metricEntityGauge = new MetricEntity(
        "test_metric_gauge",
        MetricType.GAUGE,
        MetricUnit.NUMBER,
        TEST_DESCRIPTION,
        getTestDimensions());
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceOpenTelemetryMetricsRepository otelMetricsRepository =
        createOtelRepo(metricEntityGauge, inMemoryMetricReader);
    MetricEntityStateBase metricEntityStateBaseGauge = MetricEntityStateBase
        .create(metricEntityGauge, otelMetricsRepository, getBaseDimensionsMap(), getBaseAttributes());
    metricEntityStateBaseGauge.record(10L);
    metricEntityStateBaseGauge.record(20L);
    Collection<MetricData> metrics = inMemoryMetricReader.collectAllMetrics();
    assertFalse(metrics.isEmpty(), "Metrics should not be empty");
    assertEquals(metrics.size(), 1, "There should be one metric recorded");
    validateLongPointDataFromGauge(inMemoryMetricReader, 20L, getBaseAttributes(), "test_metric_gauge", METRIC_PREFIX);

    // record another value and validate again
    metricEntityStateBaseGauge.record(30L);
    validateLongPointDataFromGauge(inMemoryMetricReader, 30L, getBaseAttributes(), "test_metric_gauge", METRIC_PREFIX);
  }

  @Test
  public void testOTelRecordAsyncGauge() {
    MetricEntity metricEntityAsyncGauge = new MetricEntity(
        "test_metric_async_gauge",
        MetricType.ASYNC_GAUGE,
        MetricUnit.NUMBER,
        TEST_DESCRIPTION,
        getTestDimensions());
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceOpenTelemetryMetricsRepository otelMetricsRepository =
        createOtelRepo(metricEntityAsyncGauge, inMemoryMetricReader);

    // Use an array to allow mutation in lambda
    final long[] gaugeValue = { 100L };
    LongSupplier supplier = () -> gaugeValue[0];

    MetricEntityStateBase.createAsyncMetric(
        metricEntityAsyncGauge,
        otelMetricsRepository,
        getBaseDimensionsMap(),
        getBaseAttributes(),
        supplier);

    Collection<MetricData> metrics = inMemoryMetricReader.collectAllMetrics();
    assertFalse(metrics.isEmpty(), "Metrics should not be empty");
    assertEquals(metrics.size(), 1, "There should be one metric recorded");
    validateLongPointDataFromGauge(
        inMemoryMetricReader,
        100,
        getBaseAttributes(),
        "test_metric_async_gauge",
        METRIC_PREFIX);

    // Change the value in the source and validate again
    gaugeValue[0] = 555L;
    validateLongPointDataFromGauge(
        inMemoryMetricReader,
        555,
        getBaseAttributes(),
        "test_metric_async_gauge",
        METRIC_PREFIX);
  }

  @Test
  public void testOTelRecordAsyncMetrics() {
    for (MetricType metricType: MetricType.values()) {
      switch (metricType) {
        case HISTOGRAM:
        case MIN_MAX_COUNT_SUM_AGGREGATIONS:
        case COUNTER:
        case GAUGE:
          assertFalse(metricType.isAsyncMetric(), "MetricType " + metricType + " should not be async");
          break;
        case ASYNC_GAUGE:
          assertTrue(metricType.isAsyncMetric(), "MetricType " + metricType + " should be async");
          break;

        default:
          fail("Unknown MetricType " + metricType);
      }
    }
  }
}
