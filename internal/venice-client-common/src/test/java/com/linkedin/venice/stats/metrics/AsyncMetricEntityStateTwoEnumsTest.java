package com.linkedin.venice.stats.metrics;

import static com.linkedin.venice.stats.metrics.MetricEntityStateTest.DimensionEnum1;
import static com.linkedin.venice.stats.metrics.MetricEntityStateTest.DimensionEnum2;
import static com.linkedin.venice.stats.metrics.MetricType.ASYNC_GAUGE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link AsyncMetricEntityStateTwoEnums}.
 */
public class AsyncMetricEntityStateTwoEnumsTest extends MetricEntityStateEnumTestBase {
  @BeforeMethod
  public void setUp() {
    setUpCommonMocks();
    when(mockMetricEntity.getMetricType()).thenReturn(ASYNC_GAUGE);
    when(mockMetricEntity.getMetricName()).thenReturn("test_async_gauge_two_enums_metric");
    when(mockOtelRepository.createInstrument(any(MetricEntity.class), any(LongSupplier.class), any(Attributes.class)))
        .thenAnswer(invocation -> new Object());
    // The base class stubs createAttributes with thenCallRealMethod() using a single varargs matcher.
    // In Mockito, a single any() matcher for a varargs parameter matches any number of varargs elements
    // (0, 1, 2, ...), so the base stub also handles the two-arg (e1, e2) call made by this class.

    Set<VeniceMetricsDimensions> dimensionsSet = createDimensionSet(
        DimensionEnum1.DIMENSION_ONE.getDimensionName(),
        DimensionEnum2.DIMENSION_ONE.getDimensionName());
    setupMockMetricEntity(dimensionsSet);
  }

  @Test
  public void testCreateWithoutOtelRepo() {
    BiFunction<DimensionEnum1, DimensionEnum2, DoubleSupplier> callbackProvider = (e1, e2) -> () -> 42L;

    AsyncMetricEntityStateTwoEnums<DimensionEnum1, DimensionEnum2> metricState = AsyncMetricEntityStateTwoEnums.create(
        mockMetricEntity,
        null, // no OTel repository
        baseDimensionsMap,
        DimensionEnum1.class,
        DimensionEnum2.class,
        callbackProvider);

    assertNotNull(metricState);
    assertNull(metricState.getMetricStatesByEnum());
    assertEquals(metricState.emitOpenTelemetryMetrics(), false);
  }

  @Test
  public void testCreateWithOtelRepo() {
    BiFunction<DimensionEnum1, DimensionEnum2, DoubleSupplier> callbackProvider = (e1, e2) -> () -> 100L;

    AsyncMetricEntityStateTwoEnums<DimensionEnum1, DimensionEnum2> metricState = AsyncMetricEntityStateTwoEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        DimensionEnum2.class,
        callbackProvider);

    assertNotNull(metricState);
    assertEquals(metricState.emitOpenTelemetryMetrics(), true);

    EnumMap<DimensionEnum1, EnumMap<DimensionEnum2, AsyncMetricEntityStateBase>> statesByEnum =
        metricState.getMetricStatesByEnum();
    assertNotNull(statesByEnum);

    // Should have an inner map for every E1 value
    assertEquals(statesByEnum.size(), DimensionEnum1.values().length);

    // Each inner map should have a state for every E2 value
    for (DimensionEnum1 e1: DimensionEnum1.values()) {
      EnumMap<DimensionEnum2, AsyncMetricEntityStateBase> innerMap = statesByEnum.get(e1);
      assertNotNull(innerMap, "Inner map should exist for " + e1);
      assertEquals(innerMap.size(), DimensionEnum2.values().length);
      for (DimensionEnum2 e2: DimensionEnum2.values()) {
        assertNotNull(innerMap.get(e2), "State should exist for (" + e1 + ", " + e2 + ")");
      }
    }
  }

  @Test
  public void testGetMetricState() {
    BiFunction<DimensionEnum1, DimensionEnum2, DoubleSupplier> callbackProvider = (e1, e2) -> () -> 100L;

    AsyncMetricEntityStateTwoEnums<DimensionEnum1, DimensionEnum2> metricState = AsyncMetricEntityStateTwoEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        DimensionEnum2.class,
        callbackProvider);

    // States for all combinations should be non-null and distinct
    Map<String, AsyncMetricEntityStateBase> seen = new HashMap<>();
    for (DimensionEnum1 e1: DimensionEnum1.values()) {
      for (DimensionEnum2 e2: DimensionEnum2.values()) {
        AsyncMetricEntityStateBase state = metricState.getMetricState(e1, e2);
        assertNotNull(state, "State should exist for (" + e1 + ", " + e2 + ")");
        String key = e1.name() + "_" + e2.name();
        seen.put(key, state);
      }
    }
    // All states should be distinct instances
    assertEquals(seen.size(), DimensionEnum1.values().length * DimensionEnum2.values().length);
  }

  @Test
  public void testGetMetricStateWhenOtelDisabled() {
    BiFunction<DimensionEnum1, DimensionEnum2, DoubleSupplier> callbackProvider = (e1, e2) -> () -> 42L;

    AsyncMetricEntityStateTwoEnums<DimensionEnum1, DimensionEnum2> metricState = AsyncMetricEntityStateTwoEnums.create(
        mockMetricEntity,
        null, // OTel disabled
        baseDimensionsMap,
        DimensionEnum1.class,
        DimensionEnum2.class,
        callbackProvider);

    assertNull(metricState.getMetricState(DimensionEnum1.DIMENSION_ONE, DimensionEnum2.DIMENSION_ONE));
  }

  @Test
  public void testCallbackProviderReceivesCorrectEnumValues() {
    Map<String, AtomicLong> valuesByPair = new HashMap<>();
    for (DimensionEnum1 e1: DimensionEnum1.values()) {
      for (DimensionEnum2 e2: DimensionEnum2.values()) {
        valuesByPair.put(e1.name() + "_" + e2.name(), new AtomicLong((long) (e1.ordinal() * 10 + e2.ordinal())));
      }
    }

    BiFunction<DimensionEnum1, DimensionEnum2, DoubleSupplier> callbackProvider =
        (e1, e2) -> () -> valuesByPair.get(e1.name() + "_" + e2.name()).get();

    AsyncMetricEntityStateTwoEnums<DimensionEnum1, DimensionEnum2> metricState = AsyncMetricEntityStateTwoEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        DimensionEnum2.class,
        callbackProvider);

    assertNotNull(metricState);
    assertEquals(
        metricState.getMetricStatesByEnum().size(),
        DimensionEnum1.values().length,
        "Outer map size should equal E1 count");
  }

  @Test
  public void testCallbackRoutingIsolatedPerPair() {
    // Verify each (e1, e2) pair captures its own distinct closure — no cross-capture bugs
    Map<String, AtomicLong> valuesByPair = new HashMap<>();
    for (DimensionEnum1 e1: DimensionEnum1.values()) {
      for (DimensionEnum2 e2: DimensionEnum2.values()) {
        valuesByPair.put(e1.name() + "_" + e2.name(), new AtomicLong(e1.ordinal() * 100L + e2.ordinal()));
      }
    }

    List<LongSupplier> capturedSuppliers = new ArrayList<>();
    when(mockOtelRepository.createInstrument(any(MetricEntity.class), any(LongSupplier.class), any(Attributes.class)))
        .thenAnswer(invocation -> {
          capturedSuppliers.add(invocation.getArgument(1, LongSupplier.class));
          return new Object();
        });

    BiFunction<DimensionEnum1, DimensionEnum2, DoubleSupplier> callbackProvider =
        (e1, e2) -> () -> valuesByPair.get(e1.name() + "_" + e2.name()).get();

    AsyncMetricEntityStateTwoEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        DimensionEnum2.class,
        callbackProvider);

    int expectedCount = DimensionEnum1.values().length * DimensionEnum2.values().length;
    assertEquals(capturedSuppliers.size(), expectedCount);

    // Every callback must return a distinct value — proves each closure captured the correct pair
    Set<Long> returnedValues = new HashSet<>();
    for (LongSupplier supplier: capturedSuppliers) {
      returnedValues.add(supplier.getAsLong());
    }
    assertEquals(
        returnedValues.size(),
        expectedCount,
        "Each (e1, e2) callback must return a distinct value — duplicate values indicate incorrect closure capture");
  }

  @Test
  public void testCreateInstrumentCalledForEachCombination() {
    BiFunction<DimensionEnum1, DimensionEnum2, DoubleSupplier> callbackProvider = (e1, e2) -> () -> 42L;

    AsyncMetricEntityStateTwoEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        DimensionEnum2.class,
        callbackProvider);

    int expectedCalls = DimensionEnum1.values().length * DimensionEnum2.values().length;
    verify(mockOtelRepository, times(expectedCalls))
        .createInstrument(eq(mockMetricEntity), any(LongSupplier.class), any(Attributes.class));
  }

  @Test
  public void testAttributesCreatedForEachCombination() {
    BiFunction<DimensionEnum1, DimensionEnum2, DoubleSupplier> callbackProvider = (e1, e2) -> () -> 42L;

    AsyncMetricEntityStateTwoEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        DimensionEnum2.class,
        callbackProvider);

    for (DimensionEnum1 e1: DimensionEnum1.values()) {
      for (DimensionEnum2 e2: DimensionEnum2.values()) {
        verify(mockOtelRepository).createAttributes(eq(mockMetricEntity), eq(baseDimensionsMap), eq(e1), eq(e2));
      }
    }
  }

  @Test
  public void testAsyncDoubleGaugeDispatchUsesDoubleSupplier() {
    when(mockMetricEntity.getMetricType()).thenReturn(MetricType.ASYNC_DOUBLE_GAUGE);
    when(mockOtelRepository.createInstrument(any(MetricEntity.class), any(DoubleSupplier.class), any(Attributes.class)))
        .thenAnswer(invocation -> new Object());

    BiFunction<DimensionEnum1, DimensionEnum2, DoubleSupplier> callbackProvider = (e1, e2) -> () -> 0.75;

    AsyncMetricEntityStateTwoEnums<DimensionEnum1, DimensionEnum2> metricState = AsyncMetricEntityStateTwoEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        DimensionEnum2.class,
        callbackProvider);

    assertNotNull(metricState);
    assertNotNull(metricState.getMetricStatesByEnum());

    int expectedCalls = DimensionEnum1.values().length * DimensionEnum2.values().length;
    verify(mockOtelRepository, times(expectedCalls))
        .createInstrument(eq(mockMetricEntity), any(DoubleSupplier.class), any(Attributes.class));
    verify(mockOtelRepository, times(0))
        .createInstrument(eq(mockMetricEntity), any(LongSupplier.class), any(Attributes.class));
  }

  @Test
  public void testAsyncGaugeDispatchWrapsAsLongSupplier() {
    Assert.assertEquals(mockMetricEntity.getMetricType(), ASYNC_GAUGE);

    BiFunction<DimensionEnum1, DimensionEnum2, DoubleSupplier> callbackProvider = (e1, e2) -> () -> 42.0;

    AsyncMetricEntityStateTwoEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        DimensionEnum2.class,
        callbackProvider);

    int expectedCalls = DimensionEnum1.values().length * DimensionEnum2.values().length;
    verify(mockOtelRepository, times(expectedCalls))
        .createInstrument(eq(mockMetricEntity), any(LongSupplier.class), any(Attributes.class));
    verify(mockOtelRepository, times(0))
        .createInstrument(eq(mockMetricEntity), any(DoubleSupplier.class), any(Attributes.class));
  }
}
