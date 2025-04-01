package com.linkedin.venice.stats.metrics;

import static com.linkedin.venice.read.RequestType.MULTI_GET_STREAMING;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.getDefaultFormat;
import static com.linkedin.venice.stats.dimensions.RequestRetryAbortReason.SLOW_ROUTE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_ABORT_REASON;
import static com.linkedin.venice.stats.metrics.MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE;
import static org.mockito.Mockito.any;
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


public class MetricEntityStateTwoEnumTest {
  private VeniceOpenTelemetryMetricsRepository mockOtelRepository;
  private MetricEntity mockMetricEntity;
  private Map<VeniceMetricsDimensions, String> baseDimensionsMap;
  private final Map<String, Attributes> attributesMap = new HashMap<>();

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
    Set<VeniceMetricsDimensions> dimensionsSet = new HashSet<>();
    dimensionsSet.add(VENICE_REQUEST_METHOD);
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE.getDimensionName());
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE.getDimensionName());
    // Duplicate: ignored
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum1Duplicate.DIMENSION_ONE.getDimensionName());
    doReturn(dimensionsSet).when(mockMetricEntity).getDimensionsList();
    baseDimensionsMap = new HashMap<>();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());

    MetricEntityStateTest.DimensionEnum1[] enum1Values = MetricEntityStateTest.DimensionEnum1.values();
    MetricEntityStateTest.DimensionEnum2[] enum2Values = MetricEntityStateTest.DimensionEnum2.values();

    for (MetricEntityStateTest.DimensionEnum1 enum1: enum1Values) {
      for (MetricEntityStateTest.DimensionEnum2 enum2: enum2Values) {
        AttributesBuilder attributesBuilder = Attributes.builder();
        for (Map.Entry<VeniceMetricsDimensions, String> entry: baseDimensionsMap.entrySet()) {
          attributesBuilder.put(mockOtelRepository.getDimensionName(entry.getKey()), entry.getValue());
        }
        attributesBuilder.put(mockOtelRepository.getDimensionName(enum1.getDimensionName()), enum1.getDimensionValue());
        attributesBuilder.put(mockOtelRepository.getDimensionName(enum2.getDimensionName()), enum2.getDimensionValue());
        Attributes attributes = attributesBuilder.build();
        String attributeName = String.format("attributesDimensionEnum1%sEnum2%s", enum1.name(), enum2.name());
        attributesMap.put(attributeName, attributes);
      }
    }
  }

  @Test
  public void testConstructorWithoutOtelRepo() {
    MetricEntityStateTwoEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2> metricEntityState =
        MetricEntityStateTwoEnums.create(
            mockMetricEntity,
            null,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class);
    assertNotNull(metricEntityState);
    assertNull(metricEntityState.getAttributesEnumMap());
    for (MetricEntityStateTest.DimensionEnum1 enum1: MetricEntityStateTest.DimensionEnum1.values()) {
      for (MetricEntityStateTest.DimensionEnum2 enum2: MetricEntityStateTest.DimensionEnum2.values()) {
        assertNull(metricEntityState.getAttributes(enum1, enum2));
      }
    }
  }

  @Test
  public void testConstructorWithOtelRepo() {
    MetricEntityStateTwoEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2> metricEntityState =
        MetricEntityStateTwoEnums.create(
            mockMetricEntity,
            mockOtelRepository,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class);
    assertNotNull(metricEntityState);
    EnumMap<MetricEntityStateTest.DimensionEnum1, EnumMap<MetricEntityStateTest.DimensionEnum2, Attributes>> attributesEnumMap =
        metricEntityState.getAttributesEnumMap();
    assertEquals(attributesEnumMap.size(), 0);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*has no constants.*")
  public void testCreateAttributesEnumMapWithEmptyEnum() {
    MetricEntityStateTwoEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        MetricEntityStateTest.EmptyDimensionEnum.class,
        MetricEntityStateTest.EmptyDimensionEnum.class);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "The input Otel dimension cannot be null.*")
  public void testGetAttributesWithNullDimension() {
    MetricEntityStateTwoEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2> metricEntityState =
        MetricEntityStateTwoEnums.create(
            mockMetricEntity,
            mockOtelRepository,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class);
    metricEntityState.getAttributes(null, null);
  }

  @Test(expectedExceptions = ClassCastException.class)
  public void testGetAttributesWithInvalidDimensionType() {
    MetricEntityStateTwoEnums metricEntityState = MetricEntityStateTwoEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        MetricEntityStateTest.DimensionEnum1.class,
        MetricEntityStateTest.DimensionEnum2.class);
    metricEntityState.getAttributes(MULTI_GET_STREAMING, MULTI_GET_STREAMING);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*has duplicate dimensions for MetricEntity.*")
  public void testConstructorWithDuplicateClasses() {
    MetricEntity mockMetricEntity = Mockito.mock(MetricEntity.class);
    Set<VeniceMetricsDimensions> dimensionsSet = new HashSet<>();
    dimensionsSet.add(VENICE_REQUEST_METHOD); // part of baseDimensionsMap
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE.getDimensionName());
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum1Duplicate.DIMENSION_ONE.getDimensionName());
    doReturn(dimensionsSet).when(mockMetricEntity).getDimensionsList();
    MetricEntityStateTwoEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        MetricEntityStateTest.DimensionEnum1.class,
        MetricEntityStateTest.DimensionEnum1Duplicate.class); // duplicate
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*doesn't match with the required dimensions.*")
  public void testConstructorWithDuplicateBaseDimension() {
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = new HashMap<>();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
    baseDimensionsMap.put(DIMENSION_ONE.getDimensionName(), DIMENSION_ONE.getDimensionValue()); // duplicate
    MetricEntityStateTwoEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        MetricEntityStateTest.DimensionEnum1.class,
        MetricEntityStateTest.DimensionEnum2.class);
  }

  @Test
  public void testGetAttributesWithValidDimension() {
    MetricEntityStateTwoEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2> metricEntityState =
        MetricEntityStateTwoEnums.create(
            mockMetricEntity,
            mockOtelRepository,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class);

    // getAttributes will work similarly for all cases as the attributes are either pre created
    // or on demand
    Attributes attributes = metricEntityState.getAttributes(
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE);
    assertNotNull(attributes);
    assertEquals(attributes.size(), 3);
    assertEquals(attributes, attributesMap.get("attributesDimensionEnum1DIMENSION_ONEEnum2DIMENSION_ONE"));

    validateAttributesEnumMap(
        metricEntityState,
        1,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        attributes);

    attributes = metricEntityState.getAttributes(
        MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO);
    assertNotNull(attributes);
    assertEquals(attributes.size(), 3);
    assertEquals(attributes, attributesMap.get("attributesDimensionEnum1DIMENSION_TWOEnum2DIMENSION_TWO"));

    validateAttributesEnumMap(
        metricEntityState,
        2,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO,
        attributes);
  }

  @Test
  public void testRecordWithValidDimension() {
    MetricEntityStateTwoEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2> metricEntityState =
        MetricEntityStateTwoEnums.create(
            mockMetricEntity,
            mockOtelRepository,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class);
    metricEntityState.record(
        100L,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE);
    validateAttributesEnumMap(
        metricEntityState,
        1,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        attributesMap.get("attributesDimensionEnum1DIMENSION_ONEEnum2DIMENSION_ONE"));

    metricEntityState.record(
        100.5,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE);
    validateAttributesEnumMap(
        metricEntityState,
        1,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        attributesMap.get("attributesDimensionEnum1DIMENSION_ONEEnum2DIMENSION_ONE"));

    metricEntityState.record(
        100L,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO);
    validateAttributesEnumMap(
        metricEntityState,
        2,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO,
        attributesMap.get("attributesDimensionEnum1DIMENSION_TWOEnum2DIMENSION_TWO"));

    metricEntityState.record(
        100.5,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO);
    validateAttributesEnumMap(
        metricEntityState,
        2,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO,
        attributesMap.get("attributesDimensionEnum1DIMENSION_TWOEnum2DIMENSION_TWO"));
  }

  private void validateAttributesEnumMap(
      MetricEntityStateTwoEnums metricEntityState,
      int attributesEnumMapSize,
      MetricEntityStateTest.DimensionEnum1 dimension1,
      MetricEntityStateTest.DimensionEnum2 dimension2,
      Attributes attributes) {
    EnumMap<MetricEntityStateTest.DimensionEnum1, EnumMap<MetricEntityStateTest.DimensionEnum2, Attributes>> attributesEnumMap =
        metricEntityState.getAttributesEnumMap();
    // verify whether the attributes are cached
    assertNotNull(attributesEnumMap);
    assertEquals(metricEntityState.getAttributesEnumMap().size(), attributesEnumMapSize);
    EnumMap<MetricEntityStateTest.DimensionEnum2, Attributes> attributesEnumMap2 = attributesEnumMap.get(dimension1);
    assertNotNull(attributesEnumMap2);
    assertEquals(attributesEnumMap2.size(), 1);
    assertEquals(attributesEnumMap2.get(dimension2), attributes);
  }

  @Test
  public void testRecordWithNullDimension() {
    MetricEntityStateTwoEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2> metricEntityState =
        MetricEntityStateTwoEnums.create(
            mockMetricEntity,
            mockOtelRepository,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class);
    // Null dimension will cause IllegalArgumentException in getDimension, record should catch it.
    metricEntityState.record(100L, null, null);
    metricEntityState.record(100.5, null, null);
  }

  @Test
  public void testValidateRequiredDimensions() {
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = new HashMap<>();
    // case 1: right values
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
    MetricEntityStateTwoEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2> metricEntityState =
        MetricEntityStateTwoEnums.create(
            mockMetricEntity,
            mockOtelRepository,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class);
    assertNotNull(metricEntityState);

    // case 2: baseDimensionsMap has extra values
    baseDimensionsMap.clear();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
    baseDimensionsMap.put(VENICE_REQUEST_RETRY_ABORT_REASON, SLOW_ROUTE.getDimensionValue());
    try {
      MetricEntityStateTwoEnums.create(
          mockMetricEntity,
          mockOtelRepository,
          baseDimensionsMap,
          MetricEntityStateTest.DimensionEnum1.class,
          MetricEntityStateTest.DimensionEnum2.class);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("doesn't match with the required dimensions"));
    }

    // case 3: baseDimensionsMap has less values
    baseDimensionsMap.clear();
    try {
      MetricEntityStateTwoEnums.create(
          mockMetricEntity,
          mockOtelRepository,
          baseDimensionsMap,
          MetricEntityStateTest.DimensionEnum1.class,
          MetricEntityStateTest.DimensionEnum2.class);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("doesn't match with the required dimensions"));
    }

    // case 4: baseDimensionsMap has same count, but different dimensions
    baseDimensionsMap.clear();
    baseDimensionsMap.put(VENICE_REQUEST_RETRY_ABORT_REASON, SLOW_ROUTE.getDimensionValue());
    try {
      MetricEntityStateTwoEnums.create(
          mockMetricEntity,
          mockOtelRepository,
          baseDimensionsMap,
          MetricEntityStateTest.DimensionEnum1.class,
          MetricEntityStateTest.DimensionEnum2.class);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("doesn't match with the required dimensions"));
    }
  }
}
