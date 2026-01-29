package com.linkedin.venice.stats.metrics;

import static com.linkedin.venice.read.RequestType.MULTI_GET_STREAMING;
import static com.linkedin.venice.stats.dimensions.RequestRetryAbortReason.SLOW_ROUTE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_ABORT_REASON;
import static com.linkedin.venice.stats.metrics.MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE;
import static com.linkedin.venice.stats.metrics.MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO;
import static com.linkedin.venice.stats.metrics.MetricType.COUNTER;
import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test class for {@link MetricEntityStateFourEnums}.
 */
public class MetricEntityStateFourEnumsTest extends MetricEntityStateEnumTestBase {
  private final Map<String, Attributes> attributesMap = new HashMap<>();

  @BeforeMethod
  public void setUp() {
    setUpCommonMocks();

    Set<VeniceMetricsDimensions> dimensionsSet = createDimensionSet(
        DIMENSION_ONE.getDimensionName(),
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE.getDimensionName(),
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE.getDimensionName(),
        MetricEntityStateTest.DimensionEnum4.DIMENSION_ONE.getDimensionName(),
        MetricEntityStateTest.DimensionEnum1Duplicate.DIMENSION_ONE.getDimensionName() // Duplicate: ignored
    );
    setupMockMetricEntity(dimensionsSet);

    // Pre-build attributes for all enum combinations
    for (MetricEntityStateTest.DimensionEnum1 enum1: MetricEntityStateTest.DimensionEnum1.values()) {
      for (MetricEntityStateTest.DimensionEnum2 enum2: MetricEntityStateTest.DimensionEnum2.values()) {
        for (MetricEntityStateTest.DimensionEnum3 enum3: MetricEntityStateTest.DimensionEnum3.values()) {
          for (MetricEntityStateTest.DimensionEnum4 enum4: MetricEntityStateTest.DimensionEnum4.values()) {
            Attributes attributes = buildAttributes(enum1, enum2, enum3, enum4);
            String attributeName = createAttributeName("attributesDimension", enum1, enum2, enum3, enum4);
            attributesMap.put(attributeName, attributes);
          }
        }
      }
    }
  }

  @Test
  public void testConstructorWithoutOtelRepo() {
    MetricEntityStateFourEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3, MetricEntityStateTest.DimensionEnum4> metricEntityState =
        MetricEntityStateFourEnums.create(
            mockMetricEntity,
            null,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class,
            MetricEntityStateTest.DimensionEnum3.class,
            MetricEntityStateTest.DimensionEnum4.class);
    assertNotNull(metricEntityState);
    assertNull(metricEntityState.getMetricAttributesDataEnumMap());
    for (MetricEntityStateTest.DimensionEnum1 enum1: MetricEntityStateTest.DimensionEnum1.values()) {
      for (MetricEntityStateTest.DimensionEnum2 enum2: MetricEntityStateTest.DimensionEnum2.values()) {
        for (MetricEntityStateTest.DimensionEnum3 enum3: MetricEntityStateTest.DimensionEnum3.values()) {
          for (MetricEntityStateTest.DimensionEnum4 enum4: MetricEntityStateTest.DimensionEnum4.values()) {
            assertNull(metricEntityState.getAttributes(enum1, enum2, enum3, enum4));
          }
        }
      }
    }
  }

  @Test
  public void testConstructorWithOtelRepo() {
    MetricEntityStateFourEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3, MetricEntityStateTest.DimensionEnum4> metricEntityState =
        MetricEntityStateFourEnums.create(
            mockMetricEntity,
            mockOtelRepository,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class,
            MetricEntityStateTest.DimensionEnum3.class,
            MetricEntityStateTest.DimensionEnum4.class);
    assertNotNull(metricEntityState);
    assertEquals(metricEntityState.getMetricAttributesDataEnumMap().size(), 0);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*has no constants.*")
  public void testCreateAttributesEnumMapWithEmptyEnum() {
    MetricEntityStateFourEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        MetricEntityStateTest.EmptyDimensionEnum.class,
        MetricEntityStateTest.EmptyDimensionEnum.class,
        MetricEntityStateTest.EmptyDimensionEnum.class,
        MetricEntityStateTest.EmptyDimensionEnum.class);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "The input Otel dimension cannot be null.*")
  public void testGetAttributesWithNullDimension() {
    MetricEntityStateFourEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3, MetricEntityStateTest.DimensionEnum4> metricEntityState =
        MetricEntityStateFourEnums.create(
            mockMetricEntity,
            mockOtelRepository,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class,
            MetricEntityStateTest.DimensionEnum3.class,
            MetricEntityStateTest.DimensionEnum4.class);
    metricEntityState.getAttributes(null, null, null, null);
  }

  @Test(expectedExceptions = ClassCastException.class)
  public void testGetAttributesWithInvalidDimensionType() {
    MetricEntityStateFourEnums metricEntityState = MetricEntityStateFourEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        MetricEntityStateTest.DimensionEnum1.class,
        MetricEntityStateTest.DimensionEnum2.class,
        MetricEntityStateTest.DimensionEnum3.class,
        MetricEntityStateTest.DimensionEnum4.class);
    metricEntityState.getAttributes(MULTI_GET_STREAMING, MULTI_GET_STREAMING, MULTI_GET_STREAMING, MULTI_GET_STREAMING);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*has duplicate dimensions for MetricEntity.*")
  public void testConstructorWithDuplicateClasses() {
    MetricEntity mockMetricEntity = Mockito.mock(MetricEntity.class);
    Set<VeniceMetricsDimensions> dimensionsSet = new HashSet<>();
    dimensionsSet.add(VENICE_REQUEST_METHOD); // part of baseDimensionsMap
    dimensionsSet.add(DIMENSION_ONE.getDimensionName());
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE.getDimensionName());
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE.getDimensionName());
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum1Duplicate.DIMENSION_ONE.getDimensionName());
    doReturn(dimensionsSet).when(mockMetricEntity).getDimensionsList();
    doReturn(COUNTER).when(mockMetricEntity).getMetricType();
    MetricEntityStateFourEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        MetricEntityStateTest.DimensionEnum1.class,
        MetricEntityStateTest.DimensionEnum2.class,
        MetricEntityStateTest.DimensionEnum1Duplicate.class, // duplicate
        MetricEntityStateTest.DimensionEnum3.class);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*doesn't match with the required dimensions.*")
  public void testConstructorWithDuplicateBaseDimension() {
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = new HashMap<>();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
    baseDimensionsMap.put(DIMENSION_ONE.getDimensionName(), DIMENSION_ONE.getDimensionValue()); // duplicate
    MetricEntityStateFourEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        MetricEntityStateTest.DimensionEnum1.class,
        MetricEntityStateTest.DimensionEnum2.class,
        MetricEntityStateTest.DimensionEnum3.class,
        MetricEntityStateTest.DimensionEnum4.class);
  }

  @Test
  public void testGetAttributesWithValidDimension() {
    MetricEntityStateFourEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3, MetricEntityStateTest.DimensionEnum4> metricEntityState =
        MetricEntityStateFourEnums.create(
            mockMetricEntity,
            mockOtelRepository,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class,
            MetricEntityStateTest.DimensionEnum3.class,
            MetricEntityStateTest.DimensionEnum4.class);

    // getAttributes will work similarly for all cases as the attributes are either pre created
    // or on demand
    Attributes attributes = metricEntityState.getAttributes(
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_ONE);
    assertNotNull(attributes);
    assertEquals(attributes.size(), 5);
    assertEquals(
        attributes,
        attributesMap
            .get("attributesDimensionEnum1DIMENSION_ONEEnum2DIMENSION_ONEEnum3DIMENSION_ONEEnum4DIMENSION_ONE"));

    validateAttributesEnumMap(
        metricEntityState,
        1,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_ONE,
        attributes);

    attributes = metricEntityState.getAttributes(
        MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_TWO);
    assertNotNull(attributes);
    assertEquals(attributes.size(), 5);
    assertEquals(
        attributes,
        attributesMap
            .get("attributesDimensionEnum1DIMENSION_TWOEnum2DIMENSION_TWOEnum3DIMENSION_TWOEnum4DIMENSION_TWO"));

    validateAttributesEnumMap(
        metricEntityState,
        2,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_TWO,
        attributes);
  }

  @Test
  public void testRecordWithValidDimension() {
    MetricEntityStateFourEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3, MetricEntityStateTest.DimensionEnum4> metricEntityState =
        MetricEntityStateFourEnums.create(
            mockMetricEntity,
            mockOtelRepository,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class,
            MetricEntityStateTest.DimensionEnum3.class,
            MetricEntityStateTest.DimensionEnum4.class);
    metricEntityState.record(
        100L,
        DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_ONE);
    validateAttributesEnumMap(
        metricEntityState,
        1,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_ONE,
        attributesMap
            .get("attributesDimensionEnum1DIMENSION_ONEEnum2DIMENSION_ONEEnum3DIMENSION_ONEEnum4DIMENSION_ONE"));
    metricEntityState.record(
        100.5,
        DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_ONE);
    validateAttributesEnumMap(
        metricEntityState,
        1,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_ONE,
        attributesMap
            .get("attributesDimensionEnum1DIMENSION_ONEEnum2DIMENSION_ONEEnum3DIMENSION_ONEEnum4DIMENSION_ONE"));
    metricEntityState.record(
        100L,
        DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_TWO);
    validateAttributesEnumMap(
        metricEntityState,
        2,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_TWO,
        attributesMap
            .get("attributesDimensionEnum1DIMENSION_TWOEnum2DIMENSION_TWOEnum3DIMENSION_TWOEnum4DIMENSION_TWO"));
    metricEntityState.record(
        100.5,
        DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_TWO);
    validateAttributesEnumMap(
        metricEntityState,
        2,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_TWO,
        attributesMap
            .get("attributesDimensionEnum1DIMENSION_TWOEnum2DIMENSION_TWOEnum3DIMENSION_TWOEnum4DIMENSION_TWO"));
  }

  private void validateAttributesEnumMap(
      MetricEntityStateFourEnums metricEntityState,
      int attributesEnumMapSize,
      MetricEntityStateTest.DimensionEnum1 dimension1,
      MetricEntityStateTest.DimensionEnum2 dimension2,
      MetricEntityStateTest.DimensionEnum3 dimension3,
      MetricEntityStateTest.DimensionEnum4 dimension4,
      Attributes attributes) {
    EnumMap<MetricEntityStateTest.DimensionEnum1, EnumMap<MetricEntityStateTest.DimensionEnum2, EnumMap<MetricEntityStateTest.DimensionEnum3, EnumMap<MetricEntityStateTest.DimensionEnum4, MetricAttributesData>>>> metricAttributesDataEnumMap =
        metricEntityState.getMetricAttributesDataEnumMap();
    // verify whether the attributes are cached
    assertNotNull(metricAttributesDataEnumMap);
    assertEquals(metricAttributesDataEnumMap.size(), attributesEnumMapSize);

    EnumMap<MetricEntityStateTest.DimensionEnum2, EnumMap<MetricEntityStateTest.DimensionEnum3, EnumMap<MetricEntityStateTest.DimensionEnum4, MetricAttributesData>>> mapE2 =
        metricAttributesDataEnumMap.get(dimension1);
    assertNotNull(mapE2);
    assertEquals(mapE2.size(), 1);
    EnumMap<MetricEntityStateTest.DimensionEnum3, EnumMap<MetricEntityStateTest.DimensionEnum4, MetricAttributesData>> mapE3 =
        mapE2.get(dimension2);
    assertNotNull(mapE3);
    assertEquals(mapE3.size(), 1);
    EnumMap<MetricEntityStateTest.DimensionEnum4, MetricAttributesData> mapE4 = mapE3.get(dimension3);
    assertNotNull(mapE4);
    assertEquals(mapE4.size(), 1);
    assertEquals(mapE4.get(dimension4).getAttributes(), attributes);
  }

  @Test
  public void testValidateRequiredDimensions() {
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = new HashMap<>();
    // case 1: right values
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
    MetricEntityStateFourEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3, MetricEntityStateTest.DimensionEnum4> metricEntityState =
        MetricEntityStateFourEnums.create(
            mockMetricEntity,
            mockOtelRepository,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class,
            MetricEntityStateTest.DimensionEnum3.class,
            MetricEntityStateTest.DimensionEnum4.class);
    assertNotNull(metricEntityState);

    // case 2: baseDimensionsMap has extra values
    baseDimensionsMap.clear();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
    baseDimensionsMap.put(VENICE_REQUEST_RETRY_ABORT_REASON, SLOW_ROUTE.getDimensionValue());
    try {
      MetricEntityStateFourEnums.create(
          mockMetricEntity,
          mockOtelRepository,
          baseDimensionsMap,
          MetricEntityStateTest.DimensionEnum1.class,
          MetricEntityStateTest.DimensionEnum2.class,
          MetricEntityStateTest.DimensionEnum3.class,
          MetricEntityStateTest.DimensionEnum4.class);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("doesn't match with the required dimensions"));
    }

    // case 3: baseDimensionsMap has less values
    baseDimensionsMap.clear();
    try {
      MetricEntityStateFourEnums.create(
          mockMetricEntity,
          mockOtelRepository,
          baseDimensionsMap,
          MetricEntityStateTest.DimensionEnum1.class,
          MetricEntityStateTest.DimensionEnum2.class,
          MetricEntityStateTest.DimensionEnum3.class,
          MetricEntityStateTest.DimensionEnum4.class);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("doesn't match with the required dimensions"));
    }

    // case 4: baseDimensionsMap has same count, but different dimensions
    baseDimensionsMap.clear();
    baseDimensionsMap.put(VENICE_REQUEST_RETRY_ABORT_REASON, SLOW_ROUTE.getDimensionValue());
    try {
      MetricEntityStateFourEnums.create(
          mockMetricEntity,
          mockOtelRepository,
          baseDimensionsMap,
          MetricEntityStateTest.DimensionEnum1.class,
          MetricEntityStateTest.DimensionEnum2.class,
          MetricEntityStateTest.DimensionEnum3.class,
          MetricEntityStateTest.DimensionEnum4.class);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("doesn't match with the required dimensions"));
    }
  }
}
