package com.linkedin.venice.stats.metrics;

import static com.linkedin.venice.read.RequestType.MULTI_GET_STREAMING;
import static com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat.getDefaultFormat;
import static com.linkedin.venice.stats.dimensions.RequestRetryAbortReason.SLOW_ROUTE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_ABORT_REASON;
import static com.linkedin.venice.stats.metrics.MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE;
import static com.linkedin.venice.stats.metrics.MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO;
import static com.linkedin.venice.stats.metrics.MetricType.COUNTER;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusCodeCategory;
import com.linkedin.venice.stats.dimensions.HttpResponseStatusEnum;
import com.linkedin.venice.stats.dimensions.MessageType;
import com.linkedin.venice.stats.dimensions.RepushStoreTriggerSource;
import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.utils.Time;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class MetricEntityStateFiveEnumsTest {
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
    when(mockOtelRepository.createAttributes(any(), any(), (VeniceDimensionInterface) any())).thenCallRealMethod();
    VeniceMetricsConfig mockMetricsConfig = Mockito.mock(VeniceMetricsConfig.class);
    when(mockMetricsConfig.getOtelCustomDimensionsMap()).thenReturn(new HashMap<>());
    when(mockOtelRepository.getMetricsConfig()).thenReturn(mockMetricsConfig);
    mockMetricEntity = Mockito.mock(MetricEntity.class);
    Set<VeniceMetricsDimensions> dimensionsSet = new HashSet<>();
    doReturn(COUNTER).when(mockMetricEntity).getMetricType();
    dimensionsSet.add(VENICE_REQUEST_METHOD);
    dimensionsSet.add(DIMENSION_ONE.getDimensionName());
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE.getDimensionName());
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE.getDimensionName());
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum4.DIMENSION_ONE.getDimensionName());
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum5.DIMENSION_ONE.getDimensionName());
    // Duplicate: ignored
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum1Duplicate.DIMENSION_ONE.getDimensionName());
    doReturn(dimensionsSet).when(mockMetricEntity).getDimensionsList();
    baseDimensionsMap = new HashMap<>();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());

    for (MetricEntityStateTest.DimensionEnum1 enum1: MetricEntityStateTest.DimensionEnum1.values()) {
      for (MetricEntityStateTest.DimensionEnum2 enum2: MetricEntityStateTest.DimensionEnum2.values()) {
        for (MetricEntityStateTest.DimensionEnum3 enum3: MetricEntityStateTest.DimensionEnum3.values()) {
          for (MetricEntityStateTest.DimensionEnum4 enum4: MetricEntityStateTest.DimensionEnum4.values()) {
            for (MetricEntityStateTest.DimensionEnum5 enum5: MetricEntityStateTest.DimensionEnum5.values()) {
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
              attributesBuilder
                  .put(mockOtelRepository.getDimensionName(enum4.getDimensionName()), enum4.getDimensionValue());
              attributesBuilder
                  .put(mockOtelRepository.getDimensionName(enum5.getDimensionName()), enum5.getDimensionValue());
              Attributes attributes = attributesBuilder.build();
              String attributeName = String.format(
                  "attributesDimensionEnum1%sEnum2%sEnum3%sEnum4%sEnum5%s",
                  enum1.name(),
                  enum2.name(),
                  enum3.name(),
                  enum4.name(),
                  enum5.name());
              attributesMap.put(attributeName, attributes);
            }
          }
        }
      }
    }
  }

  @Test
  public void testConstructorWithoutOtelRepo() {
    MetricEntityStateFiveEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3, MetricEntityStateTest.DimensionEnum4, MetricEntityStateTest.DimensionEnum5> metricEntityState =
        MetricEntityStateFiveEnums.create(
            mockMetricEntity,
            null,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class,
            MetricEntityStateTest.DimensionEnum3.class,
            MetricEntityStateTest.DimensionEnum4.class,
            MetricEntityStateTest.DimensionEnum5.class);
    assertNotNull(metricEntityState);
    assertNull(metricEntityState.getAttributesEnumMap());
    for (MetricEntityStateTest.DimensionEnum1 enum1: MetricEntityStateTest.DimensionEnum1.values()) {
      for (MetricEntityStateTest.DimensionEnum2 enum2: MetricEntityStateTest.DimensionEnum2.values()) {
        for (MetricEntityStateTest.DimensionEnum3 enum3: MetricEntityStateTest.DimensionEnum3.values()) {
          for (MetricEntityStateTest.DimensionEnum4 enum4: MetricEntityStateTest.DimensionEnum4.values()) {
            for (MetricEntityStateTest.DimensionEnum5 enum5: MetricEntityStateTest.DimensionEnum5.values()) {
              assertNull(metricEntityState.getAttributes(enum1, enum2, enum3, enum4, enum5));
            }
          }
        }
      }
    }
  }

  @Test
  public void testConstructorWithOtelRepo() {
    MetricEntityStateFiveEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3, MetricEntityStateTest.DimensionEnum4, MetricEntityStateTest.DimensionEnum5> metricEntityState =
        MetricEntityStateFiveEnums.create(
            mockMetricEntity,
            mockOtelRepository,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class,
            MetricEntityStateTest.DimensionEnum3.class,
            MetricEntityStateTest.DimensionEnum4.class,
            MetricEntityStateTest.DimensionEnum5.class);
    assertNotNull(metricEntityState);
    assertEquals(metricEntityState.getAttributesEnumMap().size(), 0);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*has no constants.*")
  public void testCreateAttributesEnumMapWithEmptyEnum() {
    MetricEntityStateFiveEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        MetricEntityStateTest.EmptyDimensionEnum.class,
        MetricEntityStateTest.EmptyDimensionEnum.class,
        MetricEntityStateTest.EmptyDimensionEnum.class,
        MetricEntityStateTest.EmptyDimensionEnum.class,
        MetricEntityStateTest.EmptyDimensionEnum.class);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "The input Otel dimension cannot be null.*")
  public void testGetAttributesWithNullDimension() {
    MetricEntityStateFiveEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3, MetricEntityStateTest.DimensionEnum4, MetricEntityStateTest.DimensionEnum5> metricEntityState =
        MetricEntityStateFiveEnums.create(
            mockMetricEntity,
            mockOtelRepository,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class,
            MetricEntityStateTest.DimensionEnum3.class,
            MetricEntityStateTest.DimensionEnum4.class,
            MetricEntityStateTest.DimensionEnum5.class);
    metricEntityState.getAttributes(null, null, null, null, null);
  }

  @Test(expectedExceptions = ClassCastException.class)
  public void testGetAttributesWithInvalidDimensionType() {
    MetricEntityStateFiveEnums metricEntityState = MetricEntityStateFiveEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        MetricEntityStateTest.DimensionEnum1.class,
        MetricEntityStateTest.DimensionEnum2.class,
        MetricEntityStateTest.DimensionEnum3.class,
        MetricEntityStateTest.DimensionEnum4.class,
        MetricEntityStateTest.DimensionEnum5.class);
    metricEntityState.getAttributes(
        MULTI_GET_STREAMING,
        MULTI_GET_STREAMING,
        MULTI_GET_STREAMING,
        MULTI_GET_STREAMING,
        MULTI_GET_STREAMING);
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*has duplicate dimensions for MetricEntity.*")
  public void testConstructorWithDuplicateClasses() {
    MetricEntity mockMetricEntity = Mockito.mock(MetricEntity.class);
    Set<VeniceMetricsDimensions> dimensionsSet = new HashSet<>();
    dimensionsSet.add(VENICE_REQUEST_METHOD); // part of baseDimensionsMap
    dimensionsSet.add(DIMENSION_ONE.getDimensionName());
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE.getDimensionName());
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE.getDimensionName());
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum4.DIMENSION_ONE.getDimensionName());
    dimensionsSet.add(MetricEntityStateTest.DimensionEnum1Duplicate.DIMENSION_ONE.getDimensionName());
    doReturn(dimensionsSet).when(mockMetricEntity).getDimensionsList();
    doReturn(COUNTER).when(mockMetricEntity).getMetricType();
    MetricEntityStateFiveEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        MetricEntityStateTest.DimensionEnum1.class,
        MetricEntityStateTest.DimensionEnum2.class,
        MetricEntityStateTest.DimensionEnum1Duplicate.class,
        MetricEntityStateTest.DimensionEnum3.class,
        MetricEntityStateTest.DimensionEnum4.class); // duplicate
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*doesn't match with the required dimensions.*")
  public void testConstructorWithDuplicateBaseDimension() {
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = new HashMap<>();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
    baseDimensionsMap.put(DIMENSION_ONE.getDimensionName(), DIMENSION_ONE.getDimensionValue()); // duplicate
    MetricEntityStateFiveEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        MetricEntityStateTest.DimensionEnum1.class,
        MetricEntityStateTest.DimensionEnum2.class,
        MetricEntityStateTest.DimensionEnum3.class,
        MetricEntityStateTest.DimensionEnum4.class,
        MetricEntityStateTest.DimensionEnum5.class);
  }

  @Test
  public void testGetAttributesWithValidDimension() {
    MetricEntityStateFiveEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3, MetricEntityStateTest.DimensionEnum4, MetricEntityStateTest.DimensionEnum5> metricEntityState =
        MetricEntityStateFiveEnums.create(
            mockMetricEntity,
            mockOtelRepository,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class,
            MetricEntityStateTest.DimensionEnum3.class,
            MetricEntityStateTest.DimensionEnum4.class,
            MetricEntityStateTest.DimensionEnum5.class);

    // getAttributes will work similarly for all cases as the attributes are either pre created
    // or on demand
    Attributes attributes = metricEntityState.getAttributes(
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum5.DIMENSION_ONE);
    assertNotNull(attributes);
    assertEquals(attributes.size(), 6);
    assertEquals(
        attributes,
        attributesMap.get(
            "attributesDimensionEnum1DIMENSION_ONEEnum2DIMENSION_ONEEnum3DIMENSION_ONEEnum4DIMENSION_ONEEnum5DIMENSION_ONE"));

    validateAttributesEnumMap(
        metricEntityState,
        1,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum5.DIMENSION_ONE,
        attributes);

    attributes = metricEntityState.getAttributes(
        MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum5.DIMENSION_TWO);
    assertNotNull(attributes);
    assertEquals(attributes.size(), 6);
    assertEquals(
        attributes,
        attributesMap.get(
            "attributesDimensionEnum1DIMENSION_TWOEnum2DIMENSION_TWOEnum3DIMENSION_TWOEnum4DIMENSION_TWOEnum5DIMENSION_TWO"));

    validateAttributesEnumMap(
        metricEntityState,
        2,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum5.DIMENSION_TWO,
        attributes);
  }

  @Test
  public void testRecordWithValidDimension() {
    MetricEntityStateFiveEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3, MetricEntityStateTest.DimensionEnum4, MetricEntityStateTest.DimensionEnum5> metricEntityState =
        MetricEntityStateFiveEnums.create(
            mockMetricEntity,
            mockOtelRepository,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class,
            MetricEntityStateTest.DimensionEnum3.class,
            MetricEntityStateTest.DimensionEnum4.class,
            MetricEntityStateTest.DimensionEnum5.class);
    metricEntityState.record(
        100L,
        DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum5.DIMENSION_ONE);
    validateAttributesEnumMap(
        metricEntityState,
        1,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum5.DIMENSION_ONE,
        attributesMap.get(
            "attributesDimensionEnum1DIMENSION_ONEEnum2DIMENSION_ONEEnum3DIMENSION_ONEEnum4DIMENSION_ONEEnum5DIMENSION_ONE"));
    metricEntityState.record(
        100.5,
        DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum5.DIMENSION_ONE);
    validateAttributesEnumMap(
        metricEntityState,
        1,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_ONE,
        MetricEntityStateTest.DimensionEnum5.DIMENSION_ONE,
        attributesMap.get(
            "attributesDimensionEnum1DIMENSION_ONEEnum2DIMENSION_ONEEnum3DIMENSION_ONEEnum4DIMENSION_ONEEnum5DIMENSION_ONE"));
    metricEntityState.record(
        100L,
        DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum5.DIMENSION_TWO);
    validateAttributesEnumMap(
        metricEntityState,
        2,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum5.DIMENSION_TWO,
        attributesMap.get(
            "attributesDimensionEnum1DIMENSION_TWOEnum2DIMENSION_TWOEnum3DIMENSION_TWOEnum4DIMENSION_TWOEnum5DIMENSION_TWO"));
    metricEntityState.record(
        100.5,
        DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum5.DIMENSION_TWO);
    validateAttributesEnumMap(
        metricEntityState,
        2,
        MetricEntityStateTest.DimensionEnum1.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum2.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum3.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum4.DIMENSION_TWO,
        MetricEntityStateTest.DimensionEnum5.DIMENSION_TWO,
        attributesMap.get(
            "attributesDimensionEnum1DIMENSION_TWOEnum2DIMENSION_TWOEnum3DIMENSION_TWOEnum4DIMENSION_TWOEnum5DIMENSION_TWO"));
  }

  private void validateAttributesEnumMap(
      MetricEntityStateFiveEnums metricEntityState,
      int attributesEnumMapSize,
      MetricEntityStateTest.DimensionEnum1 dimension1,
      MetricEntityStateTest.DimensionEnum2 dimension2,
      MetricEntityStateTest.DimensionEnum3 dimension3,
      MetricEntityStateTest.DimensionEnum4 dimension4,
      MetricEntityStateTest.DimensionEnum5 dimension5,
      Attributes attributes) {
    EnumMap<MetricEntityStateTest.DimensionEnum1, EnumMap<MetricEntityStateTest.DimensionEnum2, EnumMap<MetricEntityStateTest.DimensionEnum3, EnumMap<MetricEntityStateTest.DimensionEnum4, EnumMap<MetricEntityStateTest.DimensionEnum5, Attributes>>>>> attributesEnumMap =
        metricEntityState.getAttributesEnumMap();
    // verify whether the attributes are cached
    assertNotNull(attributesEnumMap);
    assertEquals(attributesEnumMap.size(), attributesEnumMapSize);

    EnumMap<MetricEntityStateTest.DimensionEnum2, EnumMap<MetricEntityStateTest.DimensionEnum3, EnumMap<MetricEntityStateTest.DimensionEnum4, EnumMap<MetricEntityStateTest.DimensionEnum5, Attributes>>>> mapE4 =
        attributesEnumMap.get(dimension1);
    assertNotNull(mapE4);
    assertEquals(mapE4.size(), 1);
    EnumMap<MetricEntityStateTest.DimensionEnum3, EnumMap<MetricEntityStateTest.DimensionEnum4, EnumMap<MetricEntityStateTest.DimensionEnum5, Attributes>>> mapE3 =
        mapE4.get(dimension2);
    assertNotNull(mapE3);
    assertEquals(mapE3.size(), 1);
    EnumMap<MetricEntityStateTest.DimensionEnum4, EnumMap<MetricEntityStateTest.DimensionEnum5, Attributes>> mapE4Level =
        mapE3.get(dimension3);
    assertNotNull(mapE4Level);
    assertEquals(mapE4Level.size(), 1);
    EnumMap<MetricEntityStateTest.DimensionEnum5, Attributes> mapE5 = mapE4Level.get(dimension4);
    assertNotNull(mapE5);
    assertEquals(mapE5.size(), 1);
    assertEquals(mapE5.get(dimension5), attributes);
  }

  @Test
  public void testValidateRequiredDimensions() {
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = new HashMap<>();
    // case 1: right values
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
    MetricEntityStateFiveEnums<MetricEntityStateTest.DimensionEnum1, MetricEntityStateTest.DimensionEnum2, MetricEntityStateTest.DimensionEnum3, MetricEntityStateTest.DimensionEnum4, MetricEntityStateTest.DimensionEnum5> metricEntityState =
        MetricEntityStateFiveEnums.create(
            mockMetricEntity,
            mockOtelRepository,
            baseDimensionsMap,
            MetricEntityStateTest.DimensionEnum1.class,
            MetricEntityStateTest.DimensionEnum2.class,
            MetricEntityStateTest.DimensionEnum3.class,
            MetricEntityStateTest.DimensionEnum4.class,
            MetricEntityStateTest.DimensionEnum5.class);
    assertNotNull(metricEntityState);

    // case 2: baseDimensionsMap has extra values
    baseDimensionsMap.clear();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
    baseDimensionsMap.put(VENICE_REQUEST_RETRY_ABORT_REASON, SLOW_ROUTE.getDimensionValue());
    try {
      MetricEntityStateFiveEnums.create(
          mockMetricEntity,
          mockOtelRepository,
          baseDimensionsMap,
          MetricEntityStateTest.DimensionEnum1.class,
          MetricEntityStateTest.DimensionEnum2.class,
          MetricEntityStateTest.DimensionEnum3.class,
          MetricEntityStateTest.DimensionEnum4.class,
          MetricEntityStateTest.DimensionEnum5.class);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("doesn't match with the required dimensions"));
    }

    // case 3: baseDimensionsMap has less values
    baseDimensionsMap.clear();
    try {
      MetricEntityStateFiveEnums.create(
          mockMetricEntity,
          mockOtelRepository,
          baseDimensionsMap,
          MetricEntityStateTest.DimensionEnum1.class,
          MetricEntityStateTest.DimensionEnum2.class,
          MetricEntityStateTest.DimensionEnum3.class,
          MetricEntityStateTest.DimensionEnum4.class,
          MetricEntityStateTest.DimensionEnum5.class);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("doesn't match with the required dimensions"));
    }

    // case 4: baseDimensionsMap has same count, but different dimensions
    baseDimensionsMap.clear();
    baseDimensionsMap.put(VENICE_REQUEST_RETRY_ABORT_REASON, SLOW_ROUTE.getDimensionValue());
    try {
      MetricEntityStateFiveEnums.create(
          mockMetricEntity,
          mockOtelRepository,
          baseDimensionsMap,
          MetricEntityStateTest.DimensionEnum1.class,
          MetricEntityStateTest.DimensionEnum2.class,
          MetricEntityStateTest.DimensionEnum3.class,
          MetricEntityStateTest.DimensionEnum4.class,
          MetricEntityStateTest.DimensionEnum5.class);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("doesn't match with the required dimensions"));
    }
  }

  /**
   * Test concurrent access to MetricEntityStateFiveEnums to ensure thread safety and correctness and the idempotent
   * nature of the result stored in cache: by creating multiple threads that concurrently write and read to the
   * MetricEntityStateFiveEnums instance. It verifies that the attributes are correctly retrieved everytime while
   * there are concurrent writes and the values are always the same for each set of enums.
   */
  @Test(timeOut = 20 * Time.MS_PER_SECOND, invocationCount = 10)
  public void testConcurrentAccess() throws InterruptedException {
    MetricEntity mockMetricEntity = Mockito.mock(MetricEntity.class);
    when(mockMetricEntity.getMetricType()).thenReturn(MetricType.COUNTER);
    Set<VeniceMetricsDimensions> dimensionsSet = new HashSet<>();
    dimensionsSet.add(VENICE_REQUEST_RETRY_ABORT_REASON);
    dimensionsSet.add(HttpResponseStatusEnum.CONTINUE.getDimensionName());
    dimensionsSet.add(HttpResponseStatusCodeCategory.UNKNOWN.getDimensionName());
    dimensionsSet.add(MULTI_GET_STREAMING.getDimensionName());
    dimensionsSet.add(MessageType.REQUEST.getDimensionName());
    dimensionsSet.add(RepushStoreTriggerSource.MANUAL.getDimensionName());

    doReturn(dimensionsSet).when(mockMetricEntity).getDimensionsList();
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = new HashMap<>();
    baseDimensionsMap.put(VENICE_REQUEST_RETRY_ABORT_REASON, SLOW_ROUTE.getDimensionValue());

    // use a metric and populate dimensions to use as a base for comparison
    MetricEntityStateFiveEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, RequestType, MessageType, RepushStoreTriggerSource> metricEntityStateForComparison =
        MetricEntityStateFiveEnums.create(
            mockMetricEntity,
            mockOtelRepository,
            baseDimensionsMap,
            HttpResponseStatusEnum.class,
            HttpResponseStatusCodeCategory.class,
            RequestType.class,
            MessageType.class,
            RepushStoreTriggerSource.class);
    for (HttpResponseStatusEnum enum1: HttpResponseStatusEnum.values()) {
      for (HttpResponseStatusCodeCategory enum2: HttpResponseStatusCodeCategory.values()) {
        for (RequestType enum3: RequestType.values()) {
          for (MessageType enum4: MessageType.values()) {
            for (RepushStoreTriggerSource enum5: RepushStoreTriggerSource.values()) {
              Attributes attributes = metricEntityStateForComparison.getAttributes(enum1, enum2, enum3, enum4, enum5);
              assertNotNull(attributes);
            }
          }
        }
      }
    }
    EnumMap<HttpResponseStatusEnum, EnumMap<HttpResponseStatusCodeCategory, EnumMap<RequestType, EnumMap<MessageType, EnumMap<RepushStoreTriggerSource, Attributes>>>>> attributesEnumMapForComparison =
        metricEntityStateForComparison.getAttributesEnumMap();

    // create a new metric keeping everything the same to concurrently access for testing
    MetricEntityStateFiveEnums<HttpResponseStatusEnum, HttpResponseStatusCodeCategory, RequestType, MessageType, RepushStoreTriggerSource> metricEntityStateForTest =
        MetricEntityStateFiveEnums.create(
            mockMetricEntity,
            mockOtelRepository,
            baseDimensionsMap,
            HttpResponseStatusEnum.class,
            HttpResponseStatusCodeCategory.class,
            RequestType.class,
            MessageType.class,
            RepushStoreTriggerSource.class);

    int writerThreads = 10;
    int readerThreads = 10;
    int totalThreads = writerThreads + readerThreads;
    int iterations = 100;

    ExecutorService executor = Executors.newFixedThreadPool(totalThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch finishLatch = new CountDownLatch(totalThreads);

    // Writer threads
    for (int threadNum = 0; threadNum < writerThreads; threadNum++) {
      executor.submit(() -> {
        try {
          startLatch.await();
          // Random sleep of 0-2 ms to randomize the order of execution
          Thread.sleep(ThreadLocalRandom.current().nextInt(3));
          for (HttpResponseStatusEnum enum1: HttpResponseStatusEnum.values()) {
            for (HttpResponseStatusCodeCategory enum2: HttpResponseStatusCodeCategory.values()) {
              for (RequestType enum3: RequestType.values()) {
                for (MessageType enum4: MessageType.values()) {
                  for (RepushStoreTriggerSource enum5: RepushStoreTriggerSource.values()) {
                    metricEntityStateForTest.record(1L, enum1, enum2, enum3, enum4, enum5);
                    metricEntityStateForTest.record(1.0, enum1, enum2, enum3, enum4, enum5);
                    metricEntityStateForTest.record(100.0, enum1, enum2, enum3, enum4, enum5);
                  }
                }
              }
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          finishLatch.countDown();
        }
      });
    }

    // Reader threads
    for (int threadNum = 0; threadNum < readerThreads; threadNum++) {
      executor.submit(() -> {
        try {
          startLatch.await();
          for (int j = 0; j < iterations; j++) {
            for (HttpResponseStatusEnum enum1: HttpResponseStatusEnum.values()) {
              for (HttpResponseStatusCodeCategory enum2: HttpResponseStatusCodeCategory.values()) {
                for (RequestType enum3: RequestType.values()) {
                  for (MessageType enum4: MessageType.values()) {
                    for (RepushStoreTriggerSource enum5: RepushStoreTriggerSource.values()) {
                      Attributes attributes = metricEntityStateForTest.getAttributes(enum1, enum2, enum3, enum4, enum5);
                      assertNotNull(attributes);
                      assertEquals(
                          attributes,
                          attributesEnumMapForComparison.get(enum1).get(enum2).get(enum3).get(enum4).get(enum5));
                    }
                  }
                }
              }
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          finishLatch.countDown();
        }
      });
    }

    // start executing all threads
    startLatch.countDown();
    // wait to finish
    finishLatch.await();

    executor.shutdownNow();
    executor.awaitTermination(5, TimeUnit.SECONDS);

    // Verify the end result
    EnumMap<HttpResponseStatusEnum, EnumMap<HttpResponseStatusCodeCategory, EnumMap<RequestType, EnumMap<MessageType, EnumMap<RepushStoreTriggerSource, Attributes>>>>> attributesEnumMapForTest =
        metricEntityStateForTest.getAttributesEnumMap();

    for (HttpResponseStatusEnum enum1: HttpResponseStatusEnum.values()) {
      for (HttpResponseStatusCodeCategory enum2: HttpResponseStatusCodeCategory.values()) {
        for (RequestType enum3: RequestType.values()) {
          for (MessageType enum4: MessageType.values()) {
            for (RepushStoreTriggerSource enum5: RepushStoreTriggerSource.values()) {
              assertEquals(
                  attributesEnumMapForTest.get(enum1).get(enum2).get(enum3).get(enum4).get(enum5),
                  attributesEnumMapForComparison.get(enum1).get(enum2).get(enum3).get(enum4).get(enum5));
            }
          }
        }
      }
    }
  }
}
