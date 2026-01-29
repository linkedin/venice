package com.linkedin.venice.stats.metrics;

import static com.linkedin.venice.read.RequestType.MULTI_GET_STREAMING;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.getDefaultFormat;
import static com.linkedin.venice.stats.dimensions.RequestRetryAbortReason.SLOW_ROUTE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_ABORT_REASON;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.metrics.MetricType.HISTOGRAM;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.tehuti.metrics.Sensor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class MetricEntityStateGenericTest {
  private VeniceOpenTelemetryMetricsRepository mockOtelRepository;
  private MetricEntity mockMetricEntity;
  private Sensor mockSensor;
  private Map<VeniceMetricsDimensions, String> baseDimensionsMap;
  private Map<VeniceMetricsDimensions, String> testInputDimensions;
  private Attributes expectedAttributesForTestInputDimensions;
  private MetricEntityStateBase recordFailureMetric;

  @BeforeMethod
  public void setUp() {
    mockOtelRepository = Mockito.mock(VeniceOpenTelemetryMetricsRepository.class);
    when(mockOtelRepository.emitOpenTelemetryMetrics()).thenReturn(true);
    when(mockOtelRepository.getMetricFormat()).thenReturn(getDefaultFormat());
    when(mockOtelRepository.getDimensionName(any())).thenCallRealMethod();
    when(mockOtelRepository.createAttributes(any(), any(), (Map) any())).thenCallRealMethod();
    doCallRealMethod().when(mockOtelRepository).recordFailureMetric(any(), any(Exception.class));
    recordFailureMetric = Mockito.mock(MetricEntityStateBase.class);
    when(mockOtelRepository.getRecordFailureMetric()).thenReturn(recordFailureMetric);
    VeniceMetricsConfig mockMetricsConfig = Mockito.mock(VeniceMetricsConfig.class);
    when(mockMetricsConfig.getOtelCustomDimensionsMap()).thenReturn(new HashMap<>());
    when(mockOtelRepository.getMetricsConfig()).thenReturn(mockMetricsConfig);
    mockMetricEntity = mock(MetricEntity.class);
    doReturn(HISTOGRAM).when(mockMetricEntity).getMetricType();
    Set<VeniceMetricsDimensions> dimensionsSet = new HashSet<>();
    dimensionsSet.add(VENICE_REQUEST_METHOD); // passed as base
    dimensionsSet.add(VENICE_STORE_NAME); // dynamically passed
    dimensionsSet.add(VENICE_CLUSTER_NAME); // dynamically passed
    doReturn(dimensionsSet).when(mockMetricEntity).getDimensionsList();
    mockSensor = mock(Sensor.class);
    baseDimensionsMap = new HashMap<>();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());

    // build a map of inputDimensions
    testInputDimensions = new HashMap<>();
    testInputDimensions.put(VENICE_STORE_NAME, "store1");
    testInputDimensions.put(VENICE_CLUSTER_NAME, "cluster1");

    AttributesBuilder attributesBuilder = Attributes.builder();
    for (Map.Entry<VeniceMetricsDimensions, String> entry: testInputDimensions.entrySet()) {
      attributesBuilder.put(mockOtelRepository.getDimensionName(entry.getKey()), entry.getValue()).build();
    }
    for (Map.Entry<VeniceMetricsDimensions, String> entry: baseDimensionsMap.entrySet()) {
      attributesBuilder.put(mockOtelRepository.getDimensionName(entry.getKey()), entry.getValue()).build();
    }
    expectedAttributesForTestInputDimensions = attributesBuilder.build();
  }

  @Test
  public void testRecordOtelMetricCounter() {
    LongCounter longCounter = mock(LongCounter.class);
    when(mockMetricEntity.getMetricType()).thenReturn(MetricType.COUNTER);

    MetricEntityState metricEntityState =
        MetricEntityStateGeneric.create(mockMetricEntity, mockOtelRepository, baseDimensionsMap);
    metricEntityState.setOtelMetric(longCounter);

    Attributes attributes = Attributes.builder().put("key", "value").build();
    metricEntityState.recordOtelMetric(10, new MetricAttributesData(attributes));

    verify(longCounter, times(1)).add(10, attributes);
  }

  @Test
  public void testRecordMetricsWithBothOtelAndTehuti() {
    DoubleHistogram doubleHistogram = mock(DoubleHistogram.class);
    when(mockMetricEntity.getMetricType()).thenReturn(HISTOGRAM);

    MetricEntityStateGeneric metricEntityState =
        MetricEntityStateGeneric.create(mockMetricEntity, mockOtelRepository, baseDimensionsMap);
    metricEntityState.setOtelMetric(doubleHistogram);
    metricEntityState.setTehutiSensor(mockSensor);

    // make a copy of the testInputDimensions as it will be modified in the test
    Map<VeniceMetricsDimensions, String> testInputDimensionsCopy = new HashMap<>(this.testInputDimensions);

    // called 0 times
    verify(doubleHistogram, times(0)).record(20.0, expectedAttributesForTestInputDimensions);
    verify(mockSensor, times(0)).record(20.0);

    // called 1 time
    metricEntityState.record(20.0, testInputDimensionsCopy);
    verify(doubleHistogram, times(1)).record(20.0, expectedAttributesForTestInputDimensions);
    verify(mockSensor, times(1)).record(20.0);

    // called 2 times
    metricEntityState.record(20.0, testInputDimensionsCopy);
    verify(doubleHistogram, times(2)).record(20.0, expectedAttributesForTestInputDimensions);
    verify(mockSensor, times(2)).record(20.0);

    // test without full dimensions
    verify(recordFailureMetric, never()).record(1);
    testInputDimensionsCopy.remove(VENICE_CLUSTER_NAME);
    metricEntityState.record(20.0, testInputDimensionsCopy);
    verify(doubleHistogram, times(2)).record(20.0, expectedAttributesForTestInputDimensions);
    verify(mockSensor, times(2)).record(20.0);
    verify(recordFailureMetric, times(1)).record(1);
  }

  @Test
  public void testGetAttributes() {
    DoubleHistogram doubleHistogram = mock(DoubleHistogram.class);
    when(mockMetricEntity.getMetricType()).thenReturn(HISTOGRAM);

    MetricEntityStateGeneric metricEntityState =
        MetricEntityStateGeneric.create(mockMetricEntity, mockOtelRepository, baseDimensionsMap);
    metricEntityState.setOtelMetric(doubleHistogram);
    metricEntityState.setTehutiSensor(mockSensor);

    // make a copy of the testInputDimensions as it will be modified in the test
    Map<VeniceMetricsDimensions, String> testInputDimensionsCopy = new HashMap<>(this.testInputDimensions);

    // case 1: valid attributes
    Attributes actualAttributes = metricEntityState.getAttributes(testInputDimensionsCopy);
    assertEquals(actualAttributes, expectedAttributesForTestInputDimensions);

    // case 2: less number of Attributes
    testInputDimensionsCopy.remove(VENICE_CLUSTER_NAME);
    try {
      metricEntityState.getAttributes(testInputDimensionsCopy);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("doesn't match with the required dimensions"), e.getMessage());
    }

    // case 3: extra number of Attributes
    testInputDimensionsCopy.put(VENICE_CLUSTER_NAME, "cluster1");
    testInputDimensionsCopy.put(VENICE_REQUEST_RETRY_TYPE, "test");
    try {
      metricEntityState.getAttributes(testInputDimensionsCopy);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("doesn't match with the required dimensions"), e.getMessage());
    }

    // case 4: empty dimension value
    testInputDimensionsCopy.put(VENICE_CLUSTER_NAME, "");
    testInputDimensionsCopy.remove(VENICE_REQUEST_RETRY_TYPE); // remove the extra dimension from case 3
    try {
      metricEntityState.getAttributes(testInputDimensionsCopy);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Dimension value cannot be null or empty for key"), e.getMessage());
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*does not support ASYNC_COUNTER_FOR_HIGH_PERF_CASES.*")
  public void testAsyncCounterNotSupported() {
    when(mockMetricEntity.getMetricType()).thenReturn(MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES);
    MetricEntityStateGeneric.create(mockMetricEntity, mockOtelRepository, baseDimensionsMap);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*does not support ASYNC_UP_DOWN_COUNTER_FOR_HIGH_PERF_CASES.*")
  public void testAsyncUpDownCounterNotSupported() {
    when(mockMetricEntity.getMetricType()).thenReturn(MetricType.ASYNC_UP_DOWN_COUNTER_FOR_HIGH_PERF_CASES);
    MetricEntityStateGeneric.create(mockMetricEntity, mockOtelRepository, baseDimensionsMap);
  }

  @Test
  public void testValidateRequiredDimensions() {
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = new HashMap<>();
    // case 1: right values
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
    MetricEntityStateGeneric metricEntityState =
        MetricEntityStateGeneric.create(mockMetricEntity, mockOtelRepository, baseDimensionsMap);
    assertNotNull(metricEntityState);

    // case 2: baseDimensionsMap has extra values
    baseDimensionsMap.clear();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
    baseDimensionsMap.put(VENICE_REQUEST_RETRY_ABORT_REASON, SLOW_ROUTE.getDimensionValue());

    try {
      MetricEntityStateGeneric.create(mockMetricEntity, mockOtelRepository, baseDimensionsMap);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("contains invalid dimension"), e.getMessage());
    }

    // case 3: baseDimensionsMap has less values
    baseDimensionsMap.clear();
    metricEntityState = MetricEntityStateGeneric.create(mockMetricEntity, mockOtelRepository, baseDimensionsMap);
    assertNotNull(metricEntityState);

    // case 4: baseDimensionsMap has same count, but different dimensions
    baseDimensionsMap.clear();
    baseDimensionsMap.put(VENICE_REQUEST_RETRY_ABORT_REASON, SLOW_ROUTE.getDimensionValue());
    try {
      MetricEntityStateGeneric.create(mockMetricEntity, mockOtelRepository, baseDimensionsMap);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("contains invalid dimension"), e.getMessage());
    }

    // case 5: baseDimensionsMap has null value
    baseDimensionsMap.clear();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, null);
    try {
      MetricEntityStateGeneric.create(mockMetricEntity, mockOtelRepository, baseDimensionsMap);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("contains a null or empty value for dimension"), e.getMessage());
    }

    // case 6: baseDimensionsMap has empty value
    baseDimensionsMap.clear();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, "");
    try {
      MetricEntityStateGeneric.create(mockMetricEntity, mockOtelRepository, baseDimensionsMap);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("contains a null or empty value for dimension"), e.getMessage());
    }

    // case 7: baseDimensionsMap has all keys
    baseDimensionsMap.clear();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
    baseDimensionsMap.put(VENICE_STORE_NAME, "store1");
    baseDimensionsMap.put(VENICE_CLUSTER_NAME, "cluster1");
    try {
      MetricEntityStateGeneric.create(mockMetricEntity, mockOtelRepository, baseDimensionsMap);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("contains all or more dimensions than required"), e.getMessage());
    }

    // case 8: baseDimensionsMap has more keys
    baseDimensionsMap.put(VENICE_REQUEST_RETRY_ABORT_REASON, SLOW_ROUTE.getDimensionValue());
    try {
      MetricEntityStateGeneric.create(mockMetricEntity, mockOtelRepository, baseDimensionsMap);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("contains all or more dimensions than required"), e.getMessage());
    }
  }
}
