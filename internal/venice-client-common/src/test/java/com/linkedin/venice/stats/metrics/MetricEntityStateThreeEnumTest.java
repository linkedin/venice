package com.linkedin.venice.stats.metrics;

import static com.linkedin.venice.read.RequestType.MULTI_GET_STREAMING;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.getDefaultFormat;
import static com.linkedin.venice.stats.dimensions.RequestRetryAbortReason.SLOW_ROUTE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_ABORT_REASON;
import static com.linkedin.venice.stats.metrics.MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE;
import static com.linkedin.venice.stats.metrics.MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO;
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
import com.linkedin.venice.utils.DataProviderUtils;
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


public class MetricEntityStateThreeEnumTest {
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
    dimensionsSet.add(DIMENSION_ONE.getDimensionName());
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE.getDimensionName());
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE.getDimensionName());
    // Duplicate: ignored
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum1Duplicate.DIMENSION_ONE.getDimensionName());
    doReturn(dimensionsSet).when(mockMetricEntity).getDimensionsList();
    baseDimensionsMap = new HashMap<>();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());

    for (MetricEntityStateTest.DimensionEnum1 enum1: MetricEntityStateTest.DimensionEnum1.values()) {
      for (MetricEntityStateTest.DimensionEnum2 enum2: MetricEntityStateTest.DimensionEnum2.values()) {
        for (MetricEntityStateTest.DimensionEnum3 enum3: MetricEntityStateTest.DimensionEnum3.values()) {
          AttributesBuilder attributesBuilder = Attributes.builder();
          for (Map.Entry<VeniceMetricsDimensions, String> entry: baseDimensionsMap.entrySet()) {
            attributesBuilder.put(mockOtelRepository.getDimensionName(entry.getKey()), entry.getValue());
          }
          attributesBuilder
              .put(mockOtelRepository.getDimensionName(enum1.getDimensionName()), enum1.getDimensionValue());
          attributesBuilder
              .put(mockOtelRepository.getDimensionName(enum2.getDimensionName()), enum2.getDimensionValue());
          attributesBuilder
              .put(mockOtelRepository.getDimensionName(enum3.getDimensionName()), enum3.getDimensionValue());
          Attributes attributes = attributesBuilder.build();
          String attributeName =
              String.format("attributesDimensionEnum1%sEnum2%sEnum3%s", enum1.name(), enum2.name(), enum3.name());
          attributesMap.put(attributeName, attributes);
        }
      }
    }
  }

  @Test
  public void testConstructorWithoutOtelRepo() {
    MetricEntityStateThreeEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3> metricEntityState =
        MetricEntityStateThreeEnums.create(
            mockMetricEntity,
            null,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class,
            MetricEntityStateTest.DimensionEnum3.class);
    assertNotNull(metricEntityState);
    assertNull(metricEntityState.getAttributesEnumMap());
    for (MetricEntityStateTest.DimensionEnum1 enum1: MetricEntityStateTest.DimensionEnum1.values()) {
      for (MetricEntityStateTest.DimensionEnum2 enum2: MetricEntityStateTest.DimensionEnum2.values()) {
        for (MetricEntityStateTest.DimensionEnum3 enum3: MetricEntityStateTest.DimensionEnum3.values()) {
          assertNull(metricEntityState.getAttributes(enum1, enum2, enum3));
        }
      }
    }
  }

  @Test(dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testConstructorWithOtelRepo(boolean preCreateAttributes, boolean lazyInitializeAttributes) {
    when(mockOtelRepository.preCreateAttributes()).thenReturn(preCreateAttributes);
    when(mockOtelRepository.lazyInitializeAttributes()).thenReturn(lazyInitializeAttributes);
    MetricEntityStateThreeEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3> metricEntityState =
        MetricEntityStateThreeEnums.create(
            mockMetricEntity,
            mockOtelRepository,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class,
            MetricEntityStateTest.DimensionEnum3.class);
    assertNotNull(metricEntityState);
    if (!preCreateAttributes && !lazyInitializeAttributes) {
      assertNull(metricEntityState.getAttributesEnumMap());
    } else if (!preCreateAttributes) {
      assertEquals(metricEntityState.getAttributesEnumMap().size(), 0);
    } else {
      // MetricEntityStateTest.DimensionEnum1 length
      assertEquals(metricEntityState.getAttributesEnumMap().size(), 2);
      // loop through all 3 enum constants and verify whether the attributes are created correctly
      for (MetricEntityStateTest.DimensionEnum1 enum1: MetricEntityStateTest.DimensionEnum1.values()) {
        for (MetricEntityStateTest.DimensionEnum2 enum2: MetricEntityStateTest.DimensionEnum2.values()) {
          for (MetricEntityStateTest.DimensionEnum3 enum3: MetricEntityStateTest.DimensionEnum3.values()) {
            String attributeName =
                String.format("attributesDimensionEnum1%sEnum2%sEnum3%s", enum1.name(), enum2.name(), enum3.name());
            Attributes expectedAttributes = attributesMap.get(attributeName);
            Attributes actualAttributes = metricEntityState.getAttributes(enum1, enum2, enum3);
            assertNotNull(actualAttributes);
            assertEquals(actualAttributes.size(), 4);
            assertEquals(actualAttributes, expectedAttributes);
          }
        }
      }
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*has no constants.*")
  public void testCreateAttributesEnumMapWithEmptyEnum() {
    MetricEntityStateThreeEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        MetricEntityStateTest.EmptyDimensionEnum.class,
        MetricEntityStateTest.EmptyDimensionEnum.class,
        MetricEntityStateTest.EmptyDimensionEnum.class);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "The key for otel dimension cannot be null.*")
  public void testGetAttributesWithNullKey() {
    MetricEntityStateThreeEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3> metricEntityState =
        MetricEntityStateThreeEnums.create(
            mockMetricEntity,
            mockOtelRepository,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class,
            MetricEntityStateTest.DimensionEnum3.class);
    metricEntityState.getAttributes(null, null, null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "The key for otel dimension is not of the correct type.*")
  public void testGetAttributesWithInvalidKeyType() {
    MetricEntityStateThreeEnums metricEntityState = MetricEntityStateThreeEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        MetricEntityStateTest.DimensionEnum1.class,
        MetricEntityStateTest.DimensionEnum2.class,
        MetricEntityStateTest.DimensionEnum3.class);
    metricEntityState.getAttributes(MULTI_GET_STREAMING, MULTI_GET_STREAMING, MULTI_GET_STREAMING);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*has duplicate dimensions for MetricEntity.*")
  public void testConstructorWithDuplicateClasses() {
    MetricEntity mockMetricEntity = Mockito.mock(MetricEntity.class);
    Set<VeniceMetricsDimensions> dimensionsSet = new HashSet<>();
    dimensionsSet.add(VENICE_REQUEST_METHOD); // part of baseDimensionsMap
    dimensionsSet.add(DIMENSION_ONE.getDimensionName());
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE.getDimensionName());
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum1Duplicate.DIMENSION_ONE.getDimensionName());
    doReturn(dimensionsSet).when(mockMetricEntity).getDimensionsList();
    MetricEntityStateThreeEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        MetricEntityStateTest.DimensionEnum1.class,
        MetricEntityStateTest.DimensionEnum2.class,
        MetricEntityStateTest.DimensionEnum1Duplicate.class); // duplicate
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*doesn't match with the required dimensions.*")
  public void testConstructorWithDuplicateBaseDimension() {
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = new HashMap<>();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
    baseDimensionsMap.put(DIMENSION_ONE.getDimensionName(), DIMENSION_ONE.getDimensionValue()); // duplicate
    MetricEntityStateThreeEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        MetricEntityStateTest.DimensionEnum1.class,
        MetricEntityStateTest.DimensionEnum2.class,
        MetricEntityStateTest.DimensionEnum3.class);
  }

  @Test(dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testGetAttributesWithValidKey(boolean preCreateAttributes, boolean lazyInitializeAttributes) {
    when(mockOtelRepository.preCreateAttributes()).thenReturn(preCreateAttributes);
    when(mockOtelRepository.lazyInitializeAttributes()).thenReturn(lazyInitializeAttributes);
    MetricEntityStateThreeEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3> metricEntityState =
        MetricEntityStateThreeEnums.create(
            mockMetricEntity,
            mockOtelRepository,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class,
            MetricEntityStateTest.DimensionEnum3.class);

    // getAttributes will work similarly for all cases as the attributes are either pre created
    // or on demand
    Attributes attributes = metricEntityState.getAttributes(
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE);
    assertNotNull(attributes);
    assertEquals(attributes.size(), 4);
    assertEquals(
        attributes,
        attributesMap.get("attributesDimensionEnum1DIMENSION_ONEEnum2DIMENSION_ONEEnum3DIMENSION_ONE"));

    validateAttributesEnumMap(
        metricEntityState,
        preCreateAttributes,
        lazyInitializeAttributes,
        1,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE,
        attributes);

    attributes = metricEntityState.getAttributes(
        MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_TWO);
    assertNotNull(attributes);
    assertEquals(attributes.size(), 4);
    assertEquals(
        attributes,
        attributesMap.get("attributesDimensionEnum1DIMENSION_TWOEnum2DIMENSION_TWOEnum3DIMENSION_TWO"));

    validateAttributesEnumMap(
        metricEntityState,
        preCreateAttributes,
        lazyInitializeAttributes,
        2,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_TWO,
        attributes);
  }

  @Test(dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testRecordWithValidKey(boolean preCreateAttributes, boolean lazyInitializeAttributes) {
    when(mockOtelRepository.preCreateAttributes()).thenReturn(preCreateAttributes);
    when(mockOtelRepository.lazyInitializeAttributes()).thenReturn(lazyInitializeAttributes);
    MetricEntityStateThreeEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3> metricEntityState =
        MetricEntityStateThreeEnums.create(
            mockMetricEntity,
            mockOtelRepository,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class,
            MetricEntityStateTest.DimensionEnum3.class);
    metricEntityState.record(
        100L,
        DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE);
    validateAttributesEnumMap(
        metricEntityState,
        preCreateAttributes,
        lazyInitializeAttributes,
        1,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE,
        attributesMap.get("attributesDimensionEnum1DIMENSION_ONEEnum2DIMENSION_ONEEnum3DIMENSION_ONE"));
    metricEntityState.record(
        100.5,
        DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE);
    validateAttributesEnumMap(
        metricEntityState,
        preCreateAttributes,
        lazyInitializeAttributes,
        1,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE,
        attributesMap.get("attributesDimensionEnum1DIMENSION_ONEEnum2DIMENSION_ONEEnum3DIMENSION_ONE"));
    metricEntityState.record(
        100L,
        DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_TWO);
    validateAttributesEnumMap(
        metricEntityState,
        preCreateAttributes,
        lazyInitializeAttributes,
        2,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_TWO,
        attributesMap.get("attributesDimensionEnum1DIMENSION_TWOEnum2DIMENSION_TWOEnum3DIMENSION_TWO"));
    metricEntityState.record(
        100.5,
        DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_TWO);
    validateAttributesEnumMap(
        metricEntityState,
        preCreateAttributes,
        lazyInitializeAttributes,
        2,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_TWO,
        attributesMap.get("attributesDimensionEnum1DIMENSION_TWOEnum2DIMENSION_TWOEnum3DIMENSION_TWO"));
  }

  private void validateAttributesEnumMap(
      MetricEntityStateThreeEnums metricEntityState,
      boolean preCreateAttributes,
      boolean lazyInitializeAttributes,
      int attributesEnumMapSize,
      MetricEntityStateTest.DimensionEnum1 key1,
      MetricEntityStateTest.DimensionEnum2 key2,
      MetricEntityStateTest.DimensionEnum3 key3,
      Attributes attributes) {
    if (!preCreateAttributes) {
      EnumMap<MetricEntityStateTest.DimensionEnum1, EnumMap<MetricEntityStateTest.DimensionEnum2, EnumMap<MetricEntityStateTest.DimensionEnum3, Attributes>>> attributesEnumMap =
          metricEntityState.getAttributesEnumMap();
      if (lazyInitializeAttributes) {
        // verify whether the attributes are cached
        assertNotNull(attributesEnumMap);
        assertEquals(attributesEnumMap.size(), attributesEnumMapSize);
        EnumMap<MetricEntityStateTest.DimensionEnum2, EnumMap<MetricEntityStateTest.DimensionEnum3, Attributes>> mapE2 =
            attributesEnumMap.get(key1);
        assertNotNull(mapE2);
        assertEquals(mapE2.size(), 1);
        EnumMap<MetricEntityStateTest.DimensionEnum3, Attributes> mapE3 = mapE2.get(key2);
        assertNotNull(mapE3);
        assertEquals(mapE3.size(), 1);
        assertEquals(mapE3.get(key3), attributes);
      } else {
        assertNull(metricEntityState.getAttributesEnumMap());
      }
    }
  }

  @Test
  public void testRecordWithNullKey() {
    MetricEntityStateThreeEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3> metricEntityState =
        MetricEntityStateThreeEnums.create(
            mockMetricEntity,
            mockOtelRepository,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class,
            MetricEntityStateTest.DimensionEnum3.class);
    // Null key will cause IllegalArgumentException in getDimension, record should catch it.
    metricEntityState.record(100L, null, null, null);
    metricEntityState.record(100.5, null, null, null);
  }

  @Test(dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testValidateRequiredDimensions(boolean preCreateAttributes, boolean lazyInitializeAttributes) {
    when(mockOtelRepository.preCreateAttributes()).thenReturn(preCreateAttributes);
    when(mockOtelRepository.lazyInitializeAttributes()).thenReturn(lazyInitializeAttributes);
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = new HashMap<>();
    // case 1: right values
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
    MetricEntityStateThreeEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3> metricEntityState =
        MetricEntityStateThreeEnums.create(
            mockMetricEntity,
            mockOtelRepository,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class,
            MetricEntityStateTest.DimensionEnum3.class);
    assertNotNull(metricEntityState);

    // case 2: baseDimensionsMap has extra values
    baseDimensionsMap.clear();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
    baseDimensionsMap.put(VENICE_REQUEST_RETRY_ABORT_REASON, SLOW_ROUTE.getDimensionValue());
    try {
      MetricEntityStateThreeEnums.create(
          mockMetricEntity,
          mockOtelRepository,
          baseDimensionsMap,
          MetricEntityStateTest.DimensionEnum1.class,
          MetricEntityStateTest.DimensionEnum2.class,
          MetricEntityStateTest.DimensionEnum3.class);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("doesn't match with the required dimensions"));
    }

    // case 3: baseDimensionsMap has less values
    baseDimensionsMap.clear();
    try {
      MetricEntityStateThreeEnums.create(
          mockMetricEntity,
          mockOtelRepository,
          baseDimensionsMap,
          MetricEntityStateTest.DimensionEnum1.class,
          MetricEntityStateTest.DimensionEnum2.class,
          MetricEntityStateTest.DimensionEnum3.class);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("doesn't match with the required dimensions"));
    }

    // case 4: baseDimensionsMap has same count, but different dimensions
    baseDimensionsMap.clear();
    baseDimensionsMap.put(VENICE_REQUEST_RETRY_ABORT_REASON, SLOW_ROUTE.getDimensionValue());
    try {
      MetricEntityStateThreeEnums.create(
          mockMetricEntity,
          mockOtelRepository,
          baseDimensionsMap,
          MetricEntityStateTest.DimensionEnum1.class,
          MetricEntityStateTest.DimensionEnum2.class,
          MetricEntityStateTest.DimensionEnum3.class);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("doesn't match with the required dimensions"));
    }
  }
}
