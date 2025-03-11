package com.linkedin.venice.stats.metrics;

import static com.linkedin.venice.read.RequestType.MULTI_GET_STREAMING;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.getDefaultFormat;
import static com.linkedin.venice.stats.dimensions.RequestRetryAbortReason.SLOW_ROUTE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_ABORT_REASON;
import static com.linkedin.venice.stats.metrics.MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class MetricEntityStateOneEnumTest {
  private VeniceOpenTelemetryMetricsRepository mockOtelRepository;
  private MetricEntity mockMetricEntity;
  private Map<VeniceMetricsDimensions, String> baseDimensionsMap;
  private Attributes attributesDimensionOne;
  private Attributes attributesDimensionTwo;

  @BeforeMethod
  public void setUp() {
    mockOtelRepository = Mockito.mock(VeniceOpenTelemetryMetricsRepository.class);
    when(mockOtelRepository.emitOpenTelemetryMetrics()).thenReturn(true);
    when(mockOtelRepository.getMetricFormat()).thenReturn(getDefaultFormat());
    when(mockOtelRepository.getDimensionName(any())).thenCallRealMethod();
    when(mockOtelRepository.createAttributes(any(), any(), any())).thenCallRealMethod();
    VeniceMetricsConfig mockMetricsConfig = Mockito.mock(VeniceMetricsConfig.class);
    when(mockMetricsConfig.getOtelCustomDimensionsMap()).thenReturn(new HashMap<>());
    when(mockOtelRepository.getMetricsConfig()).thenReturn(mockMetricsConfig);
    mockMetricEntity = Mockito.mock(MetricEntity.class);
    when(mockMetricEntity.getMetricName()).thenReturn("test_metric");
    Set<VeniceMetricsDimensions> dimensionsSet = new HashSet<>();
    dimensionsSet.add(VENICE_REQUEST_METHOD);
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE.getDimensionName());
    // Duplicate: ignored
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum1Duplicate.DIMENSION_ONE.getDimensionName());
    doReturn(dimensionsSet).when(mockMetricEntity).getDimensionsList();
    baseDimensionsMap = new HashMap<>();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
    AttributesBuilder attributesBuilder = Attributes.builder();
    for (Map.Entry<VeniceMetricsDimensions, String> entry: baseDimensionsMap.entrySet()) {
      attributesBuilder.put(mockOtelRepository.getDimensionName(entry.getKey()), entry.getValue());
    }
    attributesBuilder.put(
        mockOtelRepository.getDimensionName(MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE.getDimensionName()),
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE.getDimensionValue());
    attributesDimensionOne = attributesBuilder.build();

    attributesBuilder = Attributes.builder();
    for (Map.Entry<VeniceMetricsDimensions, String> entry: baseDimensionsMap.entrySet()) {
      attributesBuilder.put(mockOtelRepository.getDimensionName(entry.getKey()), entry.getValue());
    }
    attributesBuilder.put(
        mockOtelRepository.getDimensionName(MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE.getDimensionName()),
        MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO.getDimensionValue());
    attributesDimensionTwo = attributesBuilder.build();
  }

  @Test
  public void testConstructorWithoutOtelRepo() {
    MetricEntityStateOneEnum<MetricEntityStateTest.DimensionEnum1> metricEntityState = MetricEntityStateOneEnum
        .create(mockMetricEntity, null, baseDimensionsMap, MetricEntityStateTest.DimensionEnum1.class);
    assertNotNull(metricEntityState);
    assertNull(metricEntityState.getAttributesEnumMap());
    assertNull(metricEntityState.getAttributes(MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE));
    assertNull(metricEntityState.getAttributes(MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO));
  }

  @Test
  public void testConstructorWithOtelRepo() {
    MetricEntityStateOneEnum<MetricEntityStateTest.DimensionEnum1> metricEntityState = MetricEntityStateOneEnum
        .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, MetricEntityStateTest.DimensionEnum1.class);
    assertNotNull(metricEntityState);
    assertEquals(metricEntityState.getAttributesEnumMap().size(), 2); // MetricEntityStateTest.DimensionEnum1 length
    Attributes attributes = metricEntityState.getAttributes(MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE);
    assertNotNull(attributes);
    assertEquals(attributes.size(), 2);
    assertEquals(attributes, attributesDimensionOne);

    attributes = metricEntityState.getAttributes(MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO);
    assertNotNull(attributes);
    assertEquals(attributes.size(), 2);
    assertEquals(attributes, attributesDimensionTwo);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*has no constants.*")
  public void testCreateAttributesEnumMapWithEmptyEnum() {
    MetricEntityStateOneEnum.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        MetricEntityStateTest.EmptyDimensionEnum.class);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "The key for otel dimension cannot be null.*")
  public void testGetAttributesWithNullKey() {
    MetricEntityStateOneEnum<MetricEntityStateTest.DimensionEnum1> metricEntityState = MetricEntityStateOneEnum
        .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, MetricEntityStateTest.DimensionEnum1.class);
    metricEntityState.getAttributes(null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "The key for otel dimension is not of the correct type.*")
  public void testGetAttributesWithInvalidKeyType() {
    MetricEntityStateOneEnum metricEntityState = MetricEntityStateOneEnum
        .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, MetricEntityStateTest.DimensionEnum1.class);
    metricEntityState.getAttributes(MULTI_GET_STREAMING);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*doesn't match with the required dimensions.*")
  public void testConstructorWithDuplicateBaseDimension() {
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = new HashMap<>();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
    baseDimensionsMap.put(DIMENSION_ONE.getDimensionName(), DIMENSION_ONE.getDimensionValue()); // duplicate
    MetricEntityStateOneEnum
        .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, MetricEntityStateTest.DimensionEnum1.class);
  }

  @Test
  public void testRecordWithValidKey() {
    MetricEntityStateOneEnum<MetricEntityStateTest.DimensionEnum1> metricEntityState = MetricEntityStateOneEnum
        .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, MetricEntityStateTest.DimensionEnum1.class);
    metricEntityState.record(100L, MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE);
    metricEntityState.record(100.5, MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO);
    // No exception expected
  }

  @Test
  public void testRecordWithNullKey() {
    MetricEntityStateOneEnum<MetricEntityStateTest.DimensionEnum1> metricEntityState = MetricEntityStateOneEnum
        .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, MetricEntityStateTest.DimensionEnum1.class);
    // Null key will cause IllegalArgumentException in getDimension, record should catch it.
    metricEntityState.record(100L, (MetricEntityStateTest.DimensionEnum1) null);
    metricEntityState.record(100.5, (MetricEntityStateTest.DimensionEnum1) null);
  }

  @Test
  public void testValidateRequiredDimensions() {
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = new HashMap<>();
    // case 1: right values
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
    MetricEntityStateOneEnum<MetricEntityStateTest.DimensionEnum1> metricEntityState = MetricEntityStateOneEnum
        .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, MetricEntityStateTest.DimensionEnum1.class);
    assertNotNull(metricEntityState);

    // case 2: baseDimensionsMap has extra values
    baseDimensionsMap.clear();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
    baseDimensionsMap.put(VENICE_REQUEST_RETRY_ABORT_REASON, SLOW_ROUTE.getDimensionValue());

    try {
      MetricEntityStateOneEnum
          .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, MetricEntityStateTest.DimensionEnum1.class);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("doesn't match with the required dimensions"));
    }

    // case 3: baseDimensionsMap has less values
    baseDimensionsMap.clear();
    try {
      MetricEntityStateOneEnum
          .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, MetricEntityStateTest.DimensionEnum1.class);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("doesn't match with the required dimensions"));
    }

    // case 4: baseDimensionsMap has same count, but different dimensions
    baseDimensionsMap.clear();
    baseDimensionsMap.put(VENICE_REQUEST_RETRY_ABORT_REASON, SLOW_ROUTE.getDimensionValue());
    try {
      MetricEntityStateOneEnum
          .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, MetricEntityStateTest.DimensionEnum1.class);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("doesn't match with the required dimensions"));
    }
  }
}
