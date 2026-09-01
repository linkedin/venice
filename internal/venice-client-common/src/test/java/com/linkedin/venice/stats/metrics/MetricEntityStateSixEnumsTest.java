package com.linkedin.venice.stats.metrics;

import static com.linkedin.venice.stats.metrics.MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE;
import static com.linkedin.venice.stats.metrics.MetricType.COUNTER;
import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import java.util.HashSet;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test class for {@link MetricEntityStateSixEnums}. Mirrors the coverage of
 * {@link MetricEntityStateFiveEnumsTest}, exercised with a 6th dynamic dimension.
 */
public class MetricEntityStateSixEnumsTest extends MetricEntityStateEnumTestBase {
  private static final Class<MetricEntityStateTest.DimensionEnum1> E1 = MetricEntityStateTest.DimensionEnum1.class;
  private static final Class<MetricEntityStateTest.DimensionEnum2> E2 = MetricEntityStateTest.DimensionEnum2.class;
  private static final Class<MetricEntityStateTest.DimensionEnum3> E3 = MetricEntityStateTest.DimensionEnum3.class;
  private static final Class<MetricEntityStateTest.DimensionEnum4> E4 = MetricEntityStateTest.DimensionEnum4.class;
  private static final Class<MetricEntityStateTest.DimensionEnum5> E5 = MetricEntityStateTest.DimensionEnum5.class;
  private static final Class<MetricEntityStateTest.DimensionEnum6> E6 = MetricEntityStateTest.DimensionEnum6.class;

  @BeforeMethod
  public void setUp() {
    setUpCommonMocks();

    Set<VeniceMetricsDimensions> dimensionsSet = createDimensionSet(
        DIMENSION_ONE.getDimensionName(),
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE.getDimensionName(),
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE.getDimensionName(),
        MetricEntityStateTest.DimensionEnum4.DIMENSION_ONE.getDimensionName(),
        MetricEntityStateTest.DimensionEnum5.DIMENSION_ONE.getDimensionName(),
        MetricEntityStateTest.DimensionEnum6.DIMENSION_ONE.getDimensionName());
    setupMockMetricEntity(dimensionsSet);
  }

  @Test
  public void testConstructorWithoutOtelRepo() {
    MetricEntityStateSixEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3, MetricEntityStateTest.DimensionEnum4, MetricEntityStateTest.DimensionEnum5, MetricEntityStateTest.DimensionEnum6> metricEntityState =
        MetricEntityStateSixEnums.create(mockMetricEntity, null, baseDimensionsMap, E1, E2, E3, E4, E5, E6);
    assertNotNull(metricEntityState);
    assertNull(metricEntityState.getMetricAttributesDataEnumMap());
    assertNull(
        metricEntityState.getAttributes(
            MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
            MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
            MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE,
            MetricEntityStateTest.DimensionEnum4.DIMENSION_ONE,
            MetricEntityStateTest.DimensionEnum5.DIMENSION_ONE,
            MetricEntityStateTest.DimensionEnum6.DIMENSION_ONE));
  }

  @Test
  public void testConstructorWithOtelRepo() {
    MetricEntityStateSixEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3, MetricEntityStateTest.DimensionEnum4, MetricEntityStateTest.DimensionEnum5, MetricEntityStateTest.DimensionEnum6> metricEntityState =
        MetricEntityStateSixEnums
            .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, E1, E2, E3, E4, E5, E6);
    assertNotNull(metricEntityState);
    assertEquals(metricEntityState.getMetricAttributesDataEnumMap().size(), 0);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*has no constants.*")
  public void testCreateAttributesEnumMapWithEmptyEnum() {
    MetricEntityStateSixEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        MetricEntityStateTest.EmptyDimensionEnum.class,
        MetricEntityStateTest.EmptyDimensionEnum.class,
        MetricEntityStateTest.EmptyDimensionEnum.class,
        MetricEntityStateTest.EmptyDimensionEnum.class,
        MetricEntityStateTest.EmptyDimensionEnum.class,
        MetricEntityStateTest.EmptyDimensionEnum.class);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "The input Otel dimension cannot be null.*")
  public void testGetAttributesWithNullDimension() {
    MetricEntityStateSixEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3, MetricEntityStateTest.DimensionEnum4, MetricEntityStateTest.DimensionEnum5, MetricEntityStateTest.DimensionEnum6> metricEntityState =
        MetricEntityStateSixEnums
            .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, E1, E2, E3, E4, E5, E6);
    metricEntityState.getAttributes(null, null, null, null, null, null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*has duplicate dimensions for MetricEntity.*")
  public void testConstructorWithDuplicateClasses() {
    MetricEntity mockMetricEntity = Mockito.mock(MetricEntity.class);
    Set<VeniceMetricsDimensions> dimensionsSet = new HashSet<>();
    dimensionsSet.add(VeniceMetricsDimensions.VENICE_REQUEST_METHOD); // part of baseDimensionsMap
    dimensionsSet.add(DIMENSION_ONE.getDimensionName());
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE.getDimensionName());
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE.getDimensionName());
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum4.DIMENSION_ONE.getDimensionName());
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum1Duplicate.DIMENSION_ONE.getDimensionName());
    doReturn(dimensionsSet).when(mockMetricEntity).getDimensionsList();
    doReturn(COUNTER).when(mockMetricEntity).getMetricType();
    MetricEntityStateSixEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        E1,
        E2,
        MetricEntityStateTest.DimensionEnum1Duplicate.class, // duplicate
        E3,
        E4,
        E5);
  }

  @Test
  public void testGetAttributesWithValidDimension() {
    MetricEntityStateSixEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3, MetricEntityStateTest.DimensionEnum4, MetricEntityStateTest.DimensionEnum5, MetricEntityStateTest.DimensionEnum6> metricEntityState =
        MetricEntityStateSixEnums
            .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, E1, E2, E3, E4, E5, E6);

    Attributes attributes = metricEntityState.getAttributes(
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum5.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum6.DIMENSION_ONE);
    assertNotNull(attributes);
    assertEquals(attributes.size(), 7);

    // Same combo must be cached/reused.
    Attributes again = metricEntityState.getAttributes(
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum5.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum6.DIMENSION_ONE);
    assertEquals(attributes, again);
  }

  /**
   * Records a "diagonal" combo where consecutive dimensions alternate ONE/TWO, including the new
   * 6th dimension. If the computeIfAbsent chain ever nested an EnumMap of the wrong type at one
   * level, retrieving the resulting Attributes would produce the wrong attribute set or collide
   * with a different combo.
   */
  @Test
  public void testGetAttributesWithDiagonalDimensionCombo() {
    MetricEntityStateSixEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3, MetricEntityStateTest.DimensionEnum4, MetricEntityStateTest.DimensionEnum5, MetricEntityStateTest.DimensionEnum6> metricEntityState =
        MetricEntityStateSixEnums
            .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, E1, E2, E3, E4, E5, E6);

    Attributes diag1 = metricEntityState.getAttributes(
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum5.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum6.DIMENSION_TWO);
    assertNotNull(diag1);
    assertEquals(diag1.size(), 7);

    Attributes diag2 = metricEntityState.getAttributes(
        MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum5.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum6.DIMENSION_ONE);
    assertNotNull(diag2);
    assertNotEquals(diag1, diag2);
  }

  @Test
  public void testRecordWithValidDimension() {
    MetricEntityStateSixEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3, MetricEntityStateTest.DimensionEnum4, MetricEntityStateTest.DimensionEnum5, MetricEntityStateTest.DimensionEnum6> metricEntityState =
        MetricEntityStateSixEnums
            .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, E1, E2, E3, E4, E5, E6);
    metricEntityState.record(
        100L,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum5.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum6.DIMENSION_ONE);
    assertEquals(metricEntityState.getMetricAttributesDataEnumMap().size(), 1);

    metricEntityState.record(
        100.5,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum5.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum6.DIMENSION_ONE);
    // Still just one cached combo — same dimensions reused, no new EnumMap entries created.
    assertEquals(metricEntityState.getMetricAttributesDataEnumMap().size(), 1);
  }

  @Test
  public void testGetAllMetricAttributesData() {
    MetricEntityStateSixEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3, MetricEntityStateTest.DimensionEnum4, MetricEntityStateTest.DimensionEnum5, MetricEntityStateTest.DimensionEnum6> metricEntityState =
        MetricEntityStateSixEnums
            .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, E1, E2, E3, E4, E5, E6);
    // Populate the 6-level EnumMap with 2 distinct entries spanning all enum levels
    metricEntityState.record(
        100L,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum5.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum6.DIMENSION_ONE);
    metricEntityState.record(
        200L,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum5.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum6.DIMENSION_TWO);

    // Verify iteration walks all 6 nested EnumMap levels and surfaces both entries.
    int count = 0;
    for (com.linkedin.venice.stats.metrics.MetricAttributesData md: metricEntityState.getAllMetricAttributesData()) {
      assertNotNull(md);
      count++;
    }
    assertEquals(count, 2);

    // Otel disabled → null sentinel branch
    MetricEntityStateSixEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3, MetricEntityStateTest.DimensionEnum4, MetricEntityStateTest.DimensionEnum5, MetricEntityStateTest.DimensionEnum6> disabled =
        MetricEntityStateSixEnums.create(mockMetricEntity, null, baseDimensionsMap, E1, E2, E3, E4, E5, E6);
    assertNull(disabled.getAllMetricAttributesData());
  }
}
