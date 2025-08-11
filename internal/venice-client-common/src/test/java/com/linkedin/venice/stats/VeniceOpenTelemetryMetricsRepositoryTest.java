package com.linkedin.venice.stats;

import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.SNAKE_CASE;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.transformMetricName;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.validateMetricName;
import static java.util.Collections.singletonList;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityState;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleGauge;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceOpenTelemetryMetricsRepositoryTest {
  private VeniceOpenTelemetryMetricsRepository metricsRepository;
  private static final String TEST_PREFIX = "test_prefix";

  private VeniceMetricsConfig mockMetricsConfig;

  @BeforeMethod
  public void setUp() {
    mockMetricsConfig = Mockito.mock(VeniceMetricsConfig.class);
    when(mockMetricsConfig.emitOtelMetrics()).thenReturn(true);
    when(mockMetricsConfig.getMetricNamingFormat()).thenReturn(SNAKE_CASE);
    when(mockMetricsConfig.getMetricPrefix()).thenReturn(TEST_PREFIX);
    when(mockMetricsConfig.getServiceName()).thenReturn("test_service");
    when(mockMetricsConfig.exportOtelMetricsToEndpoint()).thenReturn(true);
    when(mockMetricsConfig.getOtelEndpoint()).thenReturn("http://localhost:4318");
    when(mockMetricsConfig.getExportOtelMetricsIntervalInSeconds()).thenReturn(60);

    metricsRepository = new VeniceOpenTelemetryMetricsRepository(mockMetricsConfig);
  }

  @AfterMethod
  public void tearDown() {
    metricsRepository.close();
  }

  @Test
  public void testConstructorInitialize() {
    // Check if OpenTelemetry and SdkMeterProvider are initialized correctly
    assertNotNull(metricsRepository.getSdkMeterProvider());
    assertNotNull(metricsRepository.getMeter());
  }

  @Test
  public void testConstructorWithEmitDisabled() {
    when(mockMetricsConfig.emitOtelMetrics()).thenReturn(false);
    VeniceOpenTelemetryMetricsRepository metricsRepository =
        new VeniceOpenTelemetryMetricsRepository(mockMetricsConfig);

    // Verify that metrics-related fields are null when metrics are disabled
    assertNull(metricsRepository.getSdkMeterProvider());
    assertNull(metricsRepository.getMeter());
    Set<VeniceMetricsDimensions> dimensionsSet = new HashSet<>();
    dimensionsSet.add(VeniceMetricsDimensions.VENICE_REQUEST_METHOD); // dummy
    assertNull(
        metricsRepository.createInstrument(
            new MetricEntity("test", MetricType.HISTOGRAM, MetricUnit.NUMBER, "desc", dimensionsSet)));
    assertNull(
        metricsRepository
            .createInstrument(new MetricEntity("test", MetricType.COUNTER, MetricUnit.NUMBER, "desc", dimensionsSet)));
  }

  @Test
  public void testGetOtlpHttpMetricExporterWithValidConfig() {
    MetricExporter exporter = metricsRepository.getOtlpHttpMetricExporter(mockMetricsConfig);

    // Verify that the exporter is not null
    assertNotNull(exporter);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testValidateMetricNameWithNullName() {
    validateMetricName(null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testValidateMetricNameWithEmptyName() {
    validateMetricName("");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testValidateMetricNameWithInvalidName() {
    validateMetricName("Invalid Name!");
  }

  @Test
  public void testTransformMetricName() {
    when(mockMetricsConfig.getMetricNamingFormat()).thenReturn(SNAKE_CASE);
    String testMetricName = "test_metric_name";
    MetricEntity metricEntity = new MetricEntity(
        testMetricName,
        MetricType.COUNTER,
        MetricUnit.NUMBER,
        "Test metric",
        new HashSet<>(singletonList(VeniceMetricsDimensions.VENICE_REQUEST_METHOD)));
    assertEquals(
        metricsRepository.getFullMetricName(metricEntity),
        String.format("%s%s.%s", "venice.", TEST_PREFIX, testMetricName));

    String transformedName =
        transformMetricName("test.test_metric_name", VeniceOpenTelemetryMetricNamingFormat.PASCAL_CASE);
    assertEquals(transformedName, "Test.TestMetricName");

    transformedName = transformMetricName("test.test_metric_name", VeniceOpenTelemetryMetricNamingFormat.CAMEL_CASE);
    assertEquals(transformedName, "test.testMetricName");
  }

  /**
   * This test verifies that the {@link VeniceOpenTelemetryMetricsRepository#createInstrument} creates the correct instrument
   * type and {@link MetricEntityState#recordOtelMetric} casts it to the same instrument type.
   */
  @Test
  public void testCreateAndRecordMetricsForAllMetricTypes() {
    for (MetricType metricType: MetricType.values()) {
      MetricEntity metricEntity = new MetricEntity(
          "test_metric_" + metricType.name().toLowerCase(),
          metricType,
          MetricUnit.NUMBER,
          "desc",
          new HashSet<>(singletonList(VeniceMetricsDimensions.VENICE_REQUEST_METHOD)));

      Object instrument = metricsRepository.createInstrument(metricEntity);
      assertNotNull(instrument, "Instrument should not be null for metric type: " + metricType);

      Map<VeniceMetricsDimensions, String> baseDimensionsMap = new HashMap<>();
      baseDimensionsMap
          .put(VeniceMetricsDimensions.VENICE_REQUEST_METHOD, RequestType.MULTI_GET_STREAMING.getDimensionValue());
      Attributes baseAttributes = Attributes.builder()
          .put(
              VeniceMetricsDimensions.VENICE_REQUEST_METHOD
                  .getDimensionName(VeniceOpenTelemetryMetricNamingFormat.getDefaultFormat()),
              RequestType.MULTI_GET_STREAMING.getDimensionValue())
          .build();

      MetricEntityState metricEntityState =
          MetricEntityStateBase.create(metricEntity, metricsRepository, baseDimensionsMap, baseAttributes);
      metricEntityState.setOtelMetric(instrument);

      Attributes attributes = Attributes.builder().put("key", "value").build();
      double value = 10.0;

      switch (metricType) {
        case HISTOGRAM:
        case MIN_MAX_COUNT_SUM_AGGREGATIONS:
          assertTrue(
              instrument instanceof DoubleHistogram,
              "Instrument should be a DoubleHistogram for metric type: " + metricType);
          metricEntityState.recordOtelMetric(value, attributes);
          break;
        case COUNTER:
          assertTrue(
              instrument instanceof LongCounter,
              "Instrument should be a LongCounter for metric type: " + metricType);
          metricEntityState.recordOtelMetric(value, attributes);
          break;
        case GAUGE:
          assertTrue(
              instrument instanceof DoubleGauge,
              "Instrument should be a DoubleGauge for metric type: " + metricType);
          metricEntityState.recordOtelMetric(value, attributes);
          break;
        default:
          fail("Unsupported metric type: " + metricType);
      }
    }
  }

  @Test
  public void testCreateTwoHistograms() {
    Set<VeniceMetricsDimensions> dimensionsSet = new HashSet<>();
    dimensionsSet.add(VeniceMetricsDimensions.VENICE_REQUEST_METHOD); // dummy
    Object instrument1 = metricsRepository.createInstrument(
        new MetricEntity("test_histogram", MetricType.HISTOGRAM, MetricUnit.NUMBER, "desc", dimensionsSet));
    Object instrument2 = metricsRepository.createInstrument(
        new MetricEntity("test_histogram", MetricType.HISTOGRAM, MetricUnit.NUMBER, "desc", dimensionsSet));
    assertNotNull(instrument1);
    assertNotNull(instrument2);
    assertTrue(instrument1 instanceof DoubleHistogram);
    assertTrue(instrument2 instanceof DoubleHistogram);
    assertSame(instrument1, instrument2, "Should return the same instance for the same histogram name.");
  }

  @Test
  public void testCreateTwoHistogramsWithMinMaxCountAggregations() {
    Set<VeniceMetricsDimensions> dimensionsSet = new HashSet<>();
    dimensionsSet.add(VeniceMetricsDimensions.VENICE_REQUEST_METHOD); // dummy
    Object instrument1 = metricsRepository.createInstrument(
        new MetricEntity(
            "test_histogram",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.NUMBER,
            "desc",
            dimensionsSet));
    Object instrument2 = metricsRepository.createInstrument(
        new MetricEntity(
            "test_histogram",
            MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
            MetricUnit.NUMBER,
            "desc",
            dimensionsSet));
    assertNotNull(instrument1);
    assertNotNull(instrument2);
    assertTrue(instrument1 instanceof DoubleHistogram);
    assertTrue(instrument2 instanceof DoubleHistogram);
    assertSame(instrument1, instrument2, "Should return the same instance for the same histogram name.");
  }

  @Test
  public void testCreateTwoCounters() {
    Set<VeniceMetricsDimensions> dimensionsSet = new HashSet<>();
    dimensionsSet.add(VeniceMetricsDimensions.VENICE_REQUEST_METHOD); // dummy
    Object instrument1 = metricsRepository.createInstrument(
        new MetricEntity("test_counter", MetricType.COUNTER, MetricUnit.NUMBER, "desc", dimensionsSet));
    Object instrument2 = metricsRepository.createInstrument(
        new MetricEntity("test_counter", MetricType.COUNTER, MetricUnit.NUMBER, "desc", dimensionsSet));
    assertNotNull(instrument1);
    assertNotNull(instrument2);
    assertTrue(instrument1 instanceof LongCounter);
    assertTrue(instrument2 instanceof LongCounter);
    assertSame(instrument1, instrument2, "Should return the same instance for the same counter name.");
  }

  @Test
  public void testRepositoryCreationWithoutSetMetricEntities() {
    when(mockMetricsConfig.useOtelExponentialHistogram()).thenReturn(true);
    when(mockMetricsConfig.getMetricEntities()).thenReturn(null);
    try {
      new VeniceOpenTelemetryMetricsRepository(mockMetricsConfig);
      fail();
    } catch (VeniceException e) {
      // Verify that the exception message is correct
      assertEquals(
          e.getCause().getMessage(),
          "metricEntities cannot be empty if exponential Histogram is enabled, List all the metrics used in this service using setMetricEntities method");
    }

    when(mockMetricsConfig.getMetricEntities()).thenReturn(new ArrayList<>());
    try {
      new VeniceOpenTelemetryMetricsRepository(mockMetricsConfig);
      fail();
    } catch (VeniceException e) {
      // Verify that the exception message is correct
      assertEquals(
          e.getCause().getMessage(),
          "metricEntities cannot be empty if exponential Histogram is enabled, List all the metrics used in this service using setMetricEntities method");
    }

    when(mockMetricsConfig.useOtelExponentialHistogram()).thenReturn(false);
    new VeniceOpenTelemetryMetricsRepository(mockMetricsConfig);
  }

  @Test
  public void testGetMetricPrefix() {
    String metricPrefix = metricsRepository.getMetricPrefix();
    assertNotNull(metricPrefix, "Metric prefix should not be null");
    assertEquals(metricPrefix, "venice.test_prefix", "Metric prefix should match the configured value");

    MetricEntity metricEntity = MetricEntity.createInternalMetricEntityWithoutDimensions(
        "test_metric",
        MetricType.COUNTER,
        MetricUnit.NUMBER,
        "Test metric",
        "test_custom_prefix");
    metricPrefix = metricsRepository.getMetricPrefix(metricEntity);
    assertNotNull(metricPrefix, "Metric prefix should not be null");
    assertEquals(metricPrefix, "venice.test_custom_prefix", "Metric prefix should match the configured value");
  }

  @Test
  public void testOtelCustomDescription() {
    String metricName = "test_metric";
    String specificMetricDescription = "This is a specific metric description";
    String customDescriptionInConfig = "Custom description from config";

    assertEquals(
        VeniceOpenTelemetryMetricsRepository.getMetricDescription(
            new MetricEntity(
                metricName,
                MetricType.HISTOGRAM,
                MetricUnit.NUMBER,
                specificMetricDescription,
                new HashSet<>(singletonList(VeniceMetricsDimensions.VENICE_REQUEST_METHOD))),
            mockMetricsConfig),
        specificMetricDescription);

    assertEquals(
        VeniceOpenTelemetryMetricsRepository.getMetricDescription(
            new MetricEntity(
                metricName,
                MetricType.COUNTER,
                MetricUnit.NUMBER,
                specificMetricDescription,
                new HashSet<>(singletonList(VeniceMetricsDimensions.VENICE_REQUEST_METHOD))),
            mockMetricsConfig),
        specificMetricDescription);

    when(mockMetricsConfig.getOtelCustomDescriptionForHistogramMetrics()).thenReturn(customDescriptionInConfig);

    assertEquals(
        VeniceOpenTelemetryMetricsRepository.getMetricDescription(
            new MetricEntity(
                metricName,
                MetricType.HISTOGRAM,
                MetricUnit.NUMBER,
                specificMetricDescription,
                new HashSet<>(singletonList(VeniceMetricsDimensions.VENICE_REQUEST_METHOD))),
            mockMetricsConfig),
        customDescriptionInConfig);

    // Non HISTOGRAM should still be the metric specific description
    assertEquals(
        VeniceOpenTelemetryMetricsRepository.getMetricDescription(
            new MetricEntity(
                metricName,
                MetricType.COUNTER,
                MetricUnit.NUMBER,
                specificMetricDescription,
                new HashSet<>(singletonList(VeniceMetricsDimensions.VENICE_REQUEST_METHOD))),
            mockMetricsConfig),
        specificMetricDescription);

    // reset
    when(mockMetricsConfig.getOtelCustomDescriptionForHistogramMetrics()).thenReturn(null);
  }

  @Test
  public void testOpenTelemetryCreation() {
    // case 1: No global set and useOpenTelemetryInitializedByApplication is false: Use newly created one
    VeniceOpenTelemetryMetricsRepository otelMetricsRepositoryCase1 =
        new VeniceOpenTelemetryMetricsRepository(mockMetricsConfig);

    assertNotNull(otelMetricsRepositoryCase1.getSdkMeterProvider(), "SdkMeterProvider should not be null");
    assertNotNull(otelMetricsRepositoryCase1.getOpenTelemetry(), "OpenTelemetry should not be null");

    // case 2: Global is set from case 1 and useOpenTelemetryInitializedByApplication is true: Use the global
    // OpenTelemetry
    GlobalOpenTelemetry.set(otelMetricsRepositoryCase1.getOpenTelemetry());
    when(mockMetricsConfig.useOpenTelemetryInitializedByApplication()).thenReturn(true);
    VeniceOpenTelemetryMetricsRepository otelMetricsRepositoryCase2 =
        new VeniceOpenTelemetryMetricsRepository(mockMetricsConfig);

    assertNull(otelMetricsRepositoryCase2.getSdkMeterProvider(), "SdkMeterProvider should be null");
    // comparing the meter provider rather than comparing the OpenTelemetry instance as setting the GlobalOpenTelemetry
    // copies the OpenTelemetry instance as a new ObfuscatedOpenTelemetry instance.
    assertEquals(
        otelMetricsRepositoryCase2.getOpenTelemetry().getMeterProvider(),
        otelMetricsRepositoryCase1.getOpenTelemetry().getMeterProvider());

    // case 3: Global is not set and useOpenTelemetryInitializedByApplication is true: exception should be thrown
    GlobalOpenTelemetry.resetForTest();
    try {
      new VeniceOpenTelemetryMetricsRepository(mockMetricsConfig);
      fail();
    } catch (VeniceException e) {
      assertTrue(
          e.getMessage().contains("OpenTelemetry is not initialized globally by the application"),
          e.getMessage());
    }

    // reset
    when(mockMetricsConfig.useOpenTelemetryInitializedByApplication()).thenReturn(false);
  }
}
