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
import java.util.EnumMap;
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
    EnumMap<MetricEntityStateTest.DimensionEnum1, Attributes> attributesEnumMap =
        metricEntityState.getAttributesEnumMap();
    assertEquals(attributesEnumMap.size(), 0);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*has no constants.*")
  public void testCreateAttributesEnumMapWithEmptyEnum() {
    MetricEntityStateOneEnum.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        MetricEntityStateTest.EmptyDimensionEnum.class);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "The input Otel dimension cannot be null.*")
  public void testGetAttributesWithNullDimension() {
    MetricEntityStateOneEnum<MetricEntityStateTest.DimensionEnum1> metricEntityState = MetricEntityStateOneEnum
        .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, MetricEntityStateTest.DimensionEnum1.class);
    metricEntityState.getAttributes(null);
  }

  @Test(expectedExceptions = ClassCastException.class)
  public void testGetAttributesWithInvalidDimensionType() {
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
  public void testGetAttributesWithValidDimension() {
    MetricEntityStateOneEnum<MetricEntityStateTest.DimensionEnum1> metricEntityState = MetricEntityStateOneEnum
        .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, MetricEntityStateTest.DimensionEnum1.class);

    // getAttributes will work similarly for all cases as the attributes are either pre created
    // or on demand
    Attributes attributes = metricEntityState.getAttributes(MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE);
    assertNotNull(attributes);
    assertEquals(attributes.size(), 2);
    assertEquals(attributes, attributesDimensionOne);

    validateAttributesEnumMap(
        metricEntityState,
        1,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        attributesDimensionOne);

    attributes = metricEntityState.getAttributes(MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO);
    assertNotNull(attributes);
    assertEquals(attributes.size(), 2);
    assertEquals(attributes, attributesDimensionTwo);

    validateAttributesEnumMap(
        metricEntityState,
        2,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO,
        attributesDimensionTwo);
  }

  @Test
  public void testRecordWithValidDimension() {
    MetricEntityStateOneEnum<MetricEntityStateTest.DimensionEnum1> metricEntityState = MetricEntityStateOneEnum
        .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, MetricEntityStateTest.DimensionEnum1.class);

    // record attempt 1
    metricEntityState.record(100L, MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE);
    validateAttributesEnumMap(
        metricEntityState,
        1,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        attributesDimensionOne);

    // record attempt 2: same as last one with double instead of long
    metricEntityState.record(100.5, MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE);
    validateAttributesEnumMap(
        metricEntityState,
        1,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        attributesDimensionOne);

    // record attempt 3: different dimension
    metricEntityState.record(100.5, MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO);
    validateAttributesEnumMap(
        metricEntityState,
        2,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO,
        attributesDimensionTwo);

    // record attempt 3: same as last one but with double instead of long
    metricEntityState.record(100L, MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO);
    validateAttributesEnumMap(
        metricEntityState,
        2,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO,
        attributesDimensionTwo);
  }

  private void validateAttributesEnumMap(
      MetricEntityStateOneEnum metricEntityState,
      int attributesEnumMapSize,
      MetricEntityStateTest.DimensionEnum1 dimension,
      Attributes attributes) {
    EnumMap<MetricEntityStateTest.DimensionEnum1, Attributes> attributesEnumMap =
        metricEntityState.getAttributesEnumMap();
    // verify whether the attributes are cached
    assertNotNull(attributesEnumMap);
    assertEquals(metricEntityState.getAttributesEnumMap().size(), attributesEnumMapSize);
    assertEquals(attributesEnumMap.get(dimension), attributes);
  }

  @Test
  public void testRecordWithNullDimension() {
    MetricEntityStateOneEnum<MetricEntityStateTest.DimensionEnum1> metricEntityState = MetricEntityStateOneEnum
        .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, MetricEntityStateTest.DimensionEnum1.class);
    // Null dimension will cause IllegalArgumentException in getDimension, record should catch it.
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
