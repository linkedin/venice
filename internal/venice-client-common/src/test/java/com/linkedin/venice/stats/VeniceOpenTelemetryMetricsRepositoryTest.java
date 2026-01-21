package com.linkedin.venice.stats;

import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.SNAKE_CASE;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.transformMetricName;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.validateMetricName;
import static java.util.Collections.singletonList;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityState;
import com.linkedin.venice.stats.metrics.AsyncMetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricEntityState;
import com.linkedin.venice.stats.metrics.MetricEntityStateBase;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongGauge;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link VeniceOpenTelemetryMetricsRepository}.
 */
public class VeniceOpenTelemetryMetricsRepositoryTest {
  private VeniceOpenTelemetryMetricsRepository metricsRepository;
  private static final String TEST_PREFIX = "test_prefix";

  private VeniceMetricsConfig mockMetricsConfig;

  @BeforeMethod
  public void setUp() {
    mockMetricsConfig = Mockito.mock(VeniceMetricsConfig.class);
    when(mockMetricsConfig.emitOtelMetrics()).thenReturn(true);
    when(mockMetricsConfig.emitTehutiMetrics()).thenReturn(true);
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
    when(mockMetricsConfig.emitTehutiMetrics()).thenReturn(true);
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
    assertTrue(metricsRepository.emitTehutiMetrics(), "Tehuti metrics should still be enabled");
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

      Map<VeniceMetricsDimensions, String> baseDimensionsMap = new HashMap<>();
      baseDimensionsMap
          .put(VeniceMetricsDimensions.VENICE_REQUEST_METHOD, RequestType.MULTI_GET_STREAMING.getDimensionValue());
      Attributes baseAttributes = Attributes.builder()
          .put(
              VeniceMetricsDimensions.VENICE_REQUEST_METHOD
                  .getDimensionName(VeniceOpenTelemetryMetricNamingFormat.getDefaultFormat()),
              RequestType.MULTI_GET_STREAMING.getDimensionValue())
          .build();

      Object instrument = metricsRepository.createInstrument(metricEntity, () -> 10, baseAttributes);

      assertNotNull(instrument, "Instrument should not be null for metric type: " + metricType);

      AsyncMetricEntityState metricEntityState;
      if (metricType.isAsyncMetric()) {
        metricEntityState = AsyncMetricEntityStateBase
            .create(metricEntity, metricsRepository, baseDimensionsMap, baseAttributes, () -> 10);
      } else {
        metricEntityState =
            MetricEntityStateBase.create(metricEntity, metricsRepository, baseDimensionsMap, baseAttributes);
      }

      metricEntityState.setOtelMetric(instrument);

      Attributes attributes = Attributes.builder().put("key", "value").build();
      double value = 10.0;

      switch (metricType) {
        case HISTOGRAM:
        case MIN_MAX_COUNT_SUM_AGGREGATIONS:
          MetricEntityStateBase metricEntityStateBase = (MetricEntityStateBase) metricEntityState;
          assertTrue(
              instrument instanceof DoubleHistogram,
              "Instrument should be a DoubleHistogram for metric type: " + metricType);
          metricEntityStateBase.recordOtelMetric(value, attributes);
          break;
        case COUNTER:
          metricEntityStateBase = (MetricEntityStateBase) metricEntityState;
          assertTrue(
              instrument instanceof LongCounter,
              "Instrument should be a LongCounter for metric type: " + metricType);
          metricEntityStateBase.recordOtelMetric(value, attributes);
          break;
        case UP_DOWN_COUNTER:
          metricEntityStateBase = (MetricEntityStateBase) metricEntityState;
          assertTrue(
              instrument instanceof LongUpDownCounter,
              "Instrument should be a LongUpDownCounter for metric type: " + metricType);
          metricEntityStateBase.recordOtelMetric(value, attributes);
          break;
        case GAUGE:
          metricEntityStateBase = (MetricEntityStateBase) metricEntityState;
          assertTrue(
              instrument instanceof LongGauge,
              "Instrument should be a LongGauge for metric type: " + metricType);
          metricEntityStateBase.recordOtelMetric(value, attributes);
          break;

        case ASYNC_GAUGE:
          assertTrue(
              instrument instanceof ObservableLongGauge,
              "Instrument should be a ObservableLongGauge for metric type: " + metricType);
          // async metrics should not be recorded directly
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

    // case 3: Global is not set and useOpenTelemetryInitializedByApplication is true: fall back to local
    // initialization
    GlobalOpenTelemetry.resetForTest();
    VeniceOpenTelemetryMetricsRepository otelMetricsRepositoryCase3 =
        new VeniceOpenTelemetryMetricsRepository(mockMetricsConfig);
    assertNotNull(
        otelMetricsRepositoryCase3.getSdkMeterProvider(),
        "SdkMeterProvider should not be null when falling back to local initialization");
    assertNotNull(
        otelMetricsRepositoryCase3.getOpenTelemetry(),
        "OpenTelemetry should not be null when falling back to local initialization");

    // reset
    when(mockMetricsConfig.useOpenTelemetryInitializedByApplication()).thenReturn(false);
  }

  @Test
  public void testEmitTehutiMetrics() {
    // Test when Tehuti metrics are enabled
    when(mockMetricsConfig.emitTehutiMetrics()).thenReturn(true);
    VeniceOpenTelemetryMetricsRepository repository = new VeniceOpenTelemetryMetricsRepository(mockMetricsConfig);
    assertTrue(repository.emitTehutiMetrics(), "Should return true when Tehuti metrics are enabled");

    // Test when Tehuti metrics are disabled
    when(mockMetricsConfig.emitTehutiMetrics()).thenReturn(false);
    repository = new VeniceOpenTelemetryMetricsRepository(mockMetricsConfig);
    assertFalse(repository.emitTehutiMetrics(), "Should return false when Tehuti metrics are disabled");
  }

  @Test
  public void testEmitTehutiMetricsWithOtelDisabled() {
    // Test that Tehuti metrics can be enabled even when OTel is disabled
    when(mockMetricsConfig.emitOtelMetrics()).thenReturn(false);
    when(mockMetricsConfig.emitTehutiMetrics()).thenReturn(true);

    VeniceOpenTelemetryMetricsRepository repository = new VeniceOpenTelemetryMetricsRepository(mockMetricsConfig);

    assertFalse(repository.emitOpenTelemetryMetrics(), "OTel metrics should be disabled");
    assertTrue(repository.emitTehutiMetrics(), "Tehuti metrics should be enabled independently");
  }

  /**
   * This test uses reflection to verify that all non-static fields in VeniceOpenTelemetryMetricsRepository
   * are properly initialized in the child constructor. This helps catch cases where new fields are added
   * but the child constructor is not updated.
   *
   * <p>When this test fails after adding a new field, you need to:</p>
   * <ol>
   *   <li>Update the child constructor to properly initialize the new field</li>
   *   <li>If the field should intentionally be different in child (like instrument maps),
   *       add it to {@code FIELDS_EXPECTED_TO_DIFFER}</li>
   *   <li>If the field is intentionally null in child (like sdkMeterProvider),
   *       add it to {@code FIELDS_EXPECTED_NULL_IN_CHILD}</li>
   * </ol>
   */
  @Test
  public void testCloneWithNewMetricPrefixCopiesAllRequiredFields() throws IllegalAccessException {
    // Fields that are expected to have different values in child vs parent
    Set<String> FIELDS_EXPECTED_TO_DIFFER = new HashSet<>(
        Arrays.asList(
            "metricPrefix", // Child has different prefix
            "meter", // Child has its own meter
            "recordFailureMetric", // Child creates its own failure metric
            "histogramMap", // Child has its own instrument maps
            "counterMap",
            "upDownCounterMap",
            "gaugeMap",
            "asyncGaugeMap"));

    // Fields that are expected to be null in child
    Set<String> FIELDS_EXPECTED_NULL_IN_CHILD = new HashSet<>(Arrays.asList("sdkMeterProvider"));

    // Get all declared fields (including private)
    Field[] fields = VeniceOpenTelemetryMetricsRepository.class.getDeclaredFields();

    // Modify parent's primitive values so child won't accidentally match it
    for (Field field: fields) {
      if (Modifier.isStatic(field.getModifiers()) || Modifier.isFinal(field.getModifiers())) {
        continue;
      }
      field.setAccessible(true);
      String fieldName = field.getName();

      // Skip fields that are expected to differ or be null in child
      if (FIELDS_EXPECTED_TO_DIFFER.contains(fieldName) || FIELDS_EXPECTED_NULL_IN_CHILD.contains(fieldName)) {
        continue;
      }

      Class<?> type = field.getType();
      if (type == boolean.class) {
        field.setBoolean(metricsRepository, !field.getBoolean(metricsRepository));
      } else if (type == int.class) {
        field.setInt(metricsRepository, field.getInt(metricsRepository) + 1);
      } else if (type == long.class) {
        field.setLong(metricsRepository, field.getLong(metricsRepository) + 1);
      } else if (type == double.class) {
        field.setDouble(metricsRepository, field.getDouble(metricsRepository) + 1.0);
      }
    }

    String childPrefix = "reflection_test_prefix";
    VeniceOpenTelemetryMetricsRepository childRepository = metricsRepository.cloneWithNewMetricPrefix(childPrefix);

    for (Field field: fields) {
      if (Modifier.isStatic(field.getModifiers())) {
        continue;
      }

      field.setAccessible(true);
      String fieldName = field.getName();
      Object parentValue = field.get(metricsRepository);
      Object childValue = field.get(childRepository);

      if (FIELDS_EXPECTED_NULL_IN_CHILD.contains(fieldName)) {
        assertNull(childValue, "Field '" + fieldName + "' should be null in child repository");
      } else if (FIELDS_EXPECTED_TO_DIFFER.contains(fieldName)) {
        if (parentValue != null && childValue != null) {
          assertNotSame(parentValue, childValue, "Field '" + fieldName + "' should be different in child");
        }
      } else {
        // All other fields should be copied from parent
        if (parentValue == null) {
          assertNull(childValue, "Field '" + fieldName + "' should be null in child when parent is null");
        } else if (field.getType().isPrimitive()) {
          assertEquals(
              childValue,
              parentValue,
              "Primitive field '" + fieldName + "' should have same value in child as parent");
        } else {
          assertTrue(
              childValue == parentValue || childValue.equals(parentValue),
              "Field '" + fieldName + "' should be shared or equal between parent and child. ");
        }
      }
    }

    childRepository.close();
  }

  /**
   * Test that child repository works correctly when OTel metrics are disabled.
   */
  @Test
  public void testCloneWithNewMetricPrefixWhenOtelDisabled() {
    when(mockMetricsConfig.emitOtelMetrics()).thenReturn(false);
    VeniceOpenTelemetryMetricsRepository parentWithOtelDisabled =
        new VeniceOpenTelemetryMetricsRepository(mockMetricsConfig);

    assertNull(parentWithOtelDisabled.getMeter(), "Parent meter should be null when OTel disabled");

    VeniceOpenTelemetryMetricsRepository childRepository =
        parentWithOtelDisabled.cloneWithNewMetricPrefix("disabled_child");

    assertNotNull(childRepository, "Child repository should not be null even when OTel disabled");
    assertNull(childRepository.getMeter(), "Child meter should be null when OTel disabled");
    assertFalse(childRepository.emitOpenTelemetryMetrics(), "Child should have OTel disabled");
    assertEquals(childRepository.emitTehutiMetrics(), parentWithOtelDisabled.emitTehutiMetrics());

    parentWithOtelDisabled.close();
  }

  /**
   * Test that closing a child repository doesn't affect the parent.
   */
  @Test
  public void testChildCloseDoesNotAffectParent() {
    VeniceOpenTelemetryMetricsRepository childRepository = metricsRepository.cloneWithNewMetricPrefix("child");
    childRepository.close();
    assertNotNull(metricsRepository.getSdkMeterProvider(), "Parent should still be functional after child close");
  }

}
