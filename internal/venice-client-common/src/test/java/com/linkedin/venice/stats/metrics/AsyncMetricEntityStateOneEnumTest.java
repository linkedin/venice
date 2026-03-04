package com.linkedin.venice.stats.metrics;

import static com.linkedin.venice.stats.metrics.MetricEntityStateTest.DimensionEnum1;
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
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link AsyncMetricEntityStateOneEnum}.
 */
public class AsyncMetricEntityStateOneEnumTest extends MetricEntityStateEnumTestBase {
  @BeforeMethod
  public void setUp() {
    setUpCommonMocks();
    when(mockMetricEntity.getMetricType()).thenReturn(ASYNC_GAUGE);
    when(mockMetricEntity.getMetricName()).thenReturn("test_async_gauge_metric");
    // Stub createInstrument to return a mock object for each call
    when(mockOtelRepository.createInstrument(any(MetricEntity.class), any(LongSupplier.class), any(Attributes.class)))
        .thenAnswer(invocation -> new Object());

    Set<VeniceMetricsDimensions> dimensionsSet = createDimensionSet(DimensionEnum1.DIMENSION_ONE.getDimensionName());
    setupMockMetricEntity(dimensionsSet);
  }

  @Test
  public void testCreateWithoutOtelRepo() {
    Function<DimensionEnum1, LongSupplier> callbackProvider = role -> () -> 42L;

    AsyncMetricEntityStateOneEnum<DimensionEnum1> metricState = AsyncMetricEntityStateOneEnum.create(
        mockMetricEntity,
        null, // no OTel repository
        baseDimensionsMap,
        DimensionEnum1.class,
        callbackProvider);

    assertNotNull(metricState);
    assertNull(metricState.getMetricStatesByEnum());
    assertEquals(metricState.emitOpenTelemetryMetrics(), false);
  }

  @Test
  public void testCreateWithOtelRepo() {
    AtomicLong callbackValue = new AtomicLong(100L);
    Function<DimensionEnum1, LongSupplier> callbackProvider = role -> callbackValue::get;

    AsyncMetricEntityStateOneEnum<DimensionEnum1> metricState = AsyncMetricEntityStateOneEnum
        .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, DimensionEnum1.class, callbackProvider);

    assertNotNull(metricState);
    assertEquals(metricState.emitOpenTelemetryMetrics(), true);

    EnumMap<DimensionEnum1, AsyncMetricEntityStateBase> statesByEnum = metricState.getMetricStatesByEnum();
    assertNotNull(statesByEnum);

    // Should have created states for all enum values
    assertEquals(statesByEnum.size(), DimensionEnum1.values().length);

    // Verify each enum value has a state
    for (DimensionEnum1 enumValue: DimensionEnum1.values()) {
      AsyncMetricEntityStateBase state = statesByEnum.get(enumValue);
      assertNotNull(state, "State should exist for " + enumValue);
    }
  }

  @Test
  public void testGetMetricState() {
    AtomicLong callbackValue = new AtomicLong(100L);
    Function<DimensionEnum1, LongSupplier> callbackProvider = role -> callbackValue::get;

    AsyncMetricEntityStateOneEnum<DimensionEnum1> metricState = AsyncMetricEntityStateOneEnum
        .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, DimensionEnum1.class, callbackProvider);

    // Get state for specific enum value
    AsyncMetricEntityStateBase state = metricState.getMetricState(DimensionEnum1.DIMENSION_ONE);
    assertNotNull(state);

    // Get state for another enum value
    AsyncMetricEntityStateBase state2 = metricState.getMetricState(DimensionEnum1.DIMENSION_TWO);
    assertNotNull(state2);

    // States should be different instances
    assert state != state2;
  }

  @Test
  public void testGetMetricStateWhenOtelDisabled() {
    Function<DimensionEnum1, LongSupplier> callbackProvider = role -> () -> 42L;

    AsyncMetricEntityStateOneEnum<DimensionEnum1> metricState = AsyncMetricEntityStateOneEnum.create(
        mockMetricEntity,
        null, // OTel disabled
        baseDimensionsMap,
        DimensionEnum1.class,
        callbackProvider);

    // Should return null when OTel is disabled
    assertNull(metricState.getMetricState(DimensionEnum1.DIMENSION_ONE));
  }

  @Test
  public void testCallbackProviderIsInvokedForEachEnumValue() {
    Map<DimensionEnum1, AtomicLong> valuesByRole = new HashMap<>();
    for (DimensionEnum1 role: DimensionEnum1.values()) {
      valuesByRole.put(role, new AtomicLong(role.ordinal() * 10L));
    }

    Function<DimensionEnum1, LongSupplier> callbackProvider = role -> () -> valuesByRole.get(role).get();

    AsyncMetricEntityStateOneEnum<DimensionEnum1> metricState = AsyncMetricEntityStateOneEnum
        .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, DimensionEnum1.class, callbackProvider);

    assertNotNull(metricState);

    // Verify states are created for all enum values
    EnumMap<DimensionEnum1, AsyncMetricEntityStateBase> statesByEnum = metricState.getMetricStatesByEnum();
    assertEquals(statesByEnum.size(), DimensionEnum1.values().length);
  }

  @Test
  public void testCreateInstrumentIsCalledForEachEnumValue() {
    Function<DimensionEnum1, LongSupplier> callbackProvider = role -> () -> 42L;

    AsyncMetricEntityStateOneEnum
        .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, DimensionEnum1.class, callbackProvider);

    // Verify createInstrument was called for each enum value
    verify(mockOtelRepository, times(DimensionEnum1.values().length))
        .createInstrument(eq(mockMetricEntity), any(LongSupplier.class), any(Attributes.class));
  }

  @Test
  public void testAttributesCreatedForEachEnumValue() {
    Function<DimensionEnum1, LongSupplier> callbackProvider = role -> () -> 42L;

    AsyncMetricEntityStateOneEnum
        .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, DimensionEnum1.class, callbackProvider);

    // Verify createAttributes was called for each enum value
    for (DimensionEnum1 enumValue: DimensionEnum1.values()) {
      verify(mockOtelRepository).createAttributes(eq(mockMetricEntity), eq(baseDimensionsMap), eq(enumValue));
    }
  }
}
