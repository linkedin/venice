package com.linkedin.venice.stats.metrics;

import static com.linkedin.venice.stats.metrics.MetricEntityStateTest.DimensionEnum1;
import static com.linkedin.venice.stats.metrics.MetricType.ASYNC_GAUGE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToDoubleBiFunction;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link AsyncMetricEntityStateOneEnum}. One OTel observable gauge is registered
 * per metric entity; its callback iterates enum values, calls {@code liveStateResolver} to
 * resolve the backing state (or skip if {@code null}), and emits via {@code valueResolver}.
 */
public class AsyncMetricEntityStateOneEnumTest extends MetricEntityStateEnumTestBase {
  @BeforeMethod
  public void setUp() {
    setUpCommonMocks();
    when(mockMetricEntity.getMetricType()).thenReturn(ASYNC_GAUGE);
    when(mockMetricEntity.getMetricName()).thenReturn("test_async_gauge_metric");

    Set<VeniceMetricsDimensions> dimensionsSet = createDimensionSet(DimensionEnum1.DIMENSION_ONE.getDimensionName());
    setupMockMetricEntity(dimensionsSet);
  }

  @Test
  public void testCreateWithoutOtelRepo() {
    AsyncMetricEntityStateOneEnum<DimensionEnum1> metricState = AsyncMetricEntityStateOneEnum.create(
        mockMetricEntity,
        null /* no OTel repository */,
        baseDimensionsMap,
        DimensionEnum1.class,
        e -> e,
        (state, e) -> 1L);

    assertNotNull(metricState);
    assertFalse(metricState.emitOpenTelemetryMetrics());
    assertNull(metricState.getAttributesByEnum());
    assertNull(metricState.getInstrument());
  }

  @Test
  public void testCreateRegistersExactlyOneObservableGauge() {
    AsyncMetricEntityStateOneEnum<DimensionEnum1> metricState = AsyncMetricEntityStateOneEnum.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        e -> e,
        (state, e) -> 7L);

    assertTrue(metricState.emitOpenTelemetryMetrics());
    // Exactly one SDK instrument registered, regardless of |E|.
    verify(mockOtelRepository, times(1)).registerObservableLongGauge(eq(mockMetricEntity), any());
    verify(mockOtelRepository, times(0)).registerObservableDoubleGauge(eq(mockMetricEntity), any());
    // Attributes precomputed for every enum value.
    assertEquals(metricState.getAttributesByEnum().size(), DimensionEnum1.values().length);
    for (DimensionEnum1 v: DimensionEnum1.values()) {
      assertNotNull(metricState.getAttributesByEnum().get(v));
    }
  }

  @Test
  public void testCallbackEmitsOnlyWhenLiveStateResolverReturnsNonNull() {
    // liveStateResolver returns state for DIMENSION_ONE only; DIMENSION_TWO is dormant.
    Function<DimensionEnum1, Object> liveStateResolver = e -> e == DimensionEnum1.DIMENSION_ONE ? "live" : null;
    ToDoubleBiFunction<Object, DimensionEnum1> valueResolver = (state, e) -> 42L;

    ArgumentCaptor<Consumer<ObservableLongMeasurement>> callbackCaptor = captureLongCallback();
    AsyncMetricEntityStateOneEnum.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        liveStateResolver,
        valueResolver);
    Consumer<ObservableLongMeasurement> callback = callbackCaptor.getValue();

    ObservableLongMeasurement measurement = mock(ObservableLongMeasurement.class);
    callback.accept(measurement);

    // Only DIMENSION_ONE should emit.
    verify(measurement, times(1)).record(eq(42L), any(Attributes.class));
  }

  /**
   * A throwing {@code liveStateResolver} or {@code valueResolver} for one enum value must NOT
   * prevent subsequent enum values from emitting in the same collection cycle. Without per-combo
   * exception isolation, a single bad combo would silently drop every combo iterated after it
   * for every cycle until the bug is fixed.
   */
  @Test
  public void testThrowingResolverForOneComboDoesNotSkipOthers() {
    // DIMENSION_ONE throws; DIMENSION_TWO must still emit.
    Function<DimensionEnum1, DimensionEnum1> liveStateResolver = e -> {
      if (e == DimensionEnum1.DIMENSION_ONE) {
        throw new RuntimeException("boom");
      }
      return e;
    };

    ArgumentCaptor<Consumer<ObservableLongMeasurement>> callbackCaptor = captureLongCallback();
    AsyncMetricEntityStateOneEnum.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        liveStateResolver,
        (state, e) -> 7L);

    ObservableLongMeasurement measurement = mock(ObservableLongMeasurement.class);
    // If per-combo isolation is missing, the whole callback throws and this would propagate.
    callbackCaptor.getValue().accept(measurement);

    // DIMENSION_TWO must still emit despite DIMENSION_ONE's throw.
    verify(measurement, times(1)).record(eq(7L), any(Attributes.class));
    // Failure was routed through the repository's failure metric (no-op on the mock; we only
    // verify the call happened rather than its side-effect).
    verify(mockOtelRepository, times(1)).recordFailureMetric(eq(mockMetricEntity), any(Exception.class));
  }

  @Test
  public void testValueResolverNotInvokedForDormantCombos() {
    // Sharp enforcement test: valueResolver must NOT be called when liveStateResolver returns null.
    ToDoubleBiFunction<Object, DimensionEnum1> valueResolver = mock(ToDoubleBiFunction.class);
    when(valueResolver.applyAsDouble(any(), any())).thenReturn(1.0);

    ArgumentCaptor<Consumer<ObservableLongMeasurement>> callbackCaptor = captureLongCallback();
    AsyncMetricEntityStateOneEnum.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        e -> null, // always dormant
        valueResolver);
    callbackCaptor.getValue().accept(mock(ObservableLongMeasurement.class));

    verifyNoInteractions(valueResolver);
  }

  @Test
  public void testCallbackIsInvokedFreshOnEachCollection() {
    // Predicate-backed liveStateResolver: flip between calls to simulate state changes.
    AtomicReference<EnumSet<DimensionEnum1>> live = new AtomicReference<>(EnumSet.noneOf(DimensionEnum1.class));
    Function<DimensionEnum1, DimensionEnum1> liveStateResolver = e -> live.get().contains(e) ? e : null;

    ArgumentCaptor<Consumer<ObservableLongMeasurement>> callbackCaptor = captureLongCallback();
    AsyncMetricEntityStateOneEnum.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        liveStateResolver,
        (state, e) -> 1L);
    Consumer<ObservableLongMeasurement> callback = callbackCaptor.getValue();

    // First collection: nothing live, no records emitted.
    ObservableLongMeasurement m1 = mock(ObservableLongMeasurement.class);
    callback.accept(m1);
    verifyNoInteractions(m1);

    // Transition DIMENSION_ONE to live.
    live.set(EnumSet.of(DimensionEnum1.DIMENSION_ONE));
    ObservableLongMeasurement m2 = mock(ObservableLongMeasurement.class);
    callback.accept(m2);
    verify(m2, times(1)).record(eq(1L), any(Attributes.class));

    // Everything live — all values emit.
    live.set(EnumSet.allOf(DimensionEnum1.class));
    ObservableLongMeasurement m3 = mock(ObservableLongMeasurement.class);
    callback.accept(m3);
    verify(m3, times(DimensionEnum1.values().length)).record(eq(1L), any(Attributes.class));

    // Back to dormant — nothing emits in the next collection.
    live.set(EnumSet.noneOf(DimensionEnum1.class));
    ObservableLongMeasurement m4 = mock(ObservableLongMeasurement.class);
    callback.accept(m4);
    verifyNoInteractions(m4);
  }

  @Test
  public void testAsyncGaugeEmitsTruncatedLong() {
    // valueResolver returns fractional; ASYNC_GAUGE truncates to long.
    ArgumentCaptor<Consumer<ObservableLongMeasurement>> callbackCaptor = captureLongCallback();
    AsyncMetricEntityStateOneEnum.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        e -> e,
        (state, e) -> 42.9);
    Consumer<ObservableLongMeasurement> callback = callbackCaptor.getValue();

    ObservableLongMeasurement measurement = mock(ObservableLongMeasurement.class);
    callback.accept(measurement);

    verify(measurement, times(DimensionEnum1.values().length)).record(eq(42L), any(Attributes.class));
  }

  @Test
  public void testAsyncDoubleGaugeEmitsDoubleDirectly() {
    when(mockMetricEntity.getMetricType()).thenReturn(MetricType.ASYNC_DOUBLE_GAUGE);

    ArgumentCaptor<Consumer<ObservableDoubleMeasurement>> callbackCaptor = captureDoubleCallback();
    AsyncMetricEntityStateOneEnum.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        e -> e,
        (state, e) -> 0.75);
    Consumer<ObservableDoubleMeasurement> callback = callbackCaptor.getValue();

    // ASYNC_DOUBLE_GAUGE uses registerObservableDoubleGauge, not the Long variant.
    verify(mockOtelRepository, times(0)).registerObservableLongGauge(eq(mockMetricEntity), any());

    ObservableDoubleMeasurement measurement = mock(ObservableDoubleMeasurement.class);
    callback.accept(measurement);

    verify(measurement, times(DimensionEnum1.values().length)).record(eq(0.75), any(Attributes.class));
  }

  /** Non-null repo whose {@code emitOpenTelemetryMetrics()} returns false must also yield a disabled instance. */
  @Test
  public void testRepoPresentButEmissionDisabledYieldsDisabledInstance() {
    when(mockOtelRepository.emitOpenTelemetryMetrics()).thenReturn(false);

    AsyncMetricEntityStateOneEnum<DimensionEnum1> metricState = AsyncMetricEntityStateOneEnum.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        e -> e,
        (state, e) -> 1L);

    assertFalse(metricState.emitOpenTelemetryMetrics());
    assertNull(metricState.getAttributesByEnum());
    assertNull(metricState.getInstrument());
    verify(mockOtelRepository, times(0)).registerObservableLongGauge(eq(mockMetricEntity), any());
    verify(mockOtelRepository, times(0)).registerObservableDoubleGauge(eq(mockMetricEntity), any());
  }

  /**
   * Each enum value's emission must use its own precomputed {@link Attributes} — no cross-combo
   * attribute leakage.
   */
  @Test
  public void testEachComboEmitsWithItsOwnPrecomputedAttributes() {
    // valueResolver returns a distinct value per enum ordinal, so we can match (value, attrs) pairs.
    ArgumentCaptor<Consumer<ObservableLongMeasurement>> callbackCaptor = captureLongCallback();
    AsyncMetricEntityStateOneEnum<DimensionEnum1> metricState = AsyncMetricEntityStateOneEnum.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        e -> e,
        (state, e) -> e.ordinal() * 10L);

    ObservableLongMeasurement measurement = mock(ObservableLongMeasurement.class);
    callbackCaptor.getValue().accept(measurement);

    for (DimensionEnum1 v: DimensionEnum1.values()) {
      Attributes expected = metricState.getAttributesByEnum().get(v);
      verify(measurement, times(1)).record(eq(v.ordinal() * 10L), eq(expected));
    }
  }

  /**
   * The object returned by {@code liveStateResolver} for enum value {@code e} must be the same
   * object handed to {@code valueResolver} for that same {@code e} in the same collection cycle.
   * Guards against wiring bugs that cross state between combos.
   */
  @Test
  public void testStateFromLiveStateResolverFlowsIntoValueResolver() {
    // Unique state per enum value — object identity check in the valueResolver.
    EnumMap<DimensionEnum1, Object> stateByEnum = new EnumMap<>(DimensionEnum1.class);
    for (DimensionEnum1 e: DimensionEnum1.values()) {
      stateByEnum.put(e, new Object());
    }

    ArgumentCaptor<Consumer<ObservableLongMeasurement>> callbackCaptor = captureLongCallback();
    AsyncMetricEntityStateOneEnum.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        stateByEnum::get,
        (state, e) -> {
          if (state != stateByEnum.get(e)) {
            throw new AssertionError("state mismatch for " + e + ": got " + state + ", expected " + stateByEnum.get(e));
          }
          return 1L;
        });

    // If any combo crosses state, the valueResolver throws -> callback propagates -> test fails.
    callbackCaptor.getValue().accept(mock(ObservableLongMeasurement.class));
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*ASYNC_GAUGE.*ASYNC_DOUBLE_GAUGE.*COUNTER.*")
  public void testCreateThrowsOnNonAsyncGaugeMetricType() {
    when(mockMetricEntity.getMetricType()).thenReturn(MetricType.COUNTER);
    when(mockMetricEntity.getMetricName()).thenReturn("test_counter_metric");
    AsyncMetricEntityStateOneEnum.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        e -> e,
        (state, e) -> 1L);
  }

  /** Captures the {@code Consumer<ObservableLongMeasurement>} passed to {@code registerObservableLongGauge}. */
  @SuppressWarnings("unchecked")
  private ArgumentCaptor<Consumer<ObservableLongMeasurement>> captureLongCallback() {
    ArgumentCaptor<Consumer<ObservableLongMeasurement>> captor = ArgumentCaptor.forClass(Consumer.class);
    when(mockOtelRepository.registerObservableLongGauge(eq(mockMetricEntity), captor.capture())).thenReturn(null);
    return captor;
  }

  /** Captures the {@code Consumer<ObservableDoubleMeasurement>} passed to {@code registerObservableDoubleGauge}. */
  @SuppressWarnings("unchecked")
  private ArgumentCaptor<Consumer<ObservableDoubleMeasurement>> captureDoubleCallback() {
    ArgumentCaptor<Consumer<ObservableDoubleMeasurement>> captor = ArgumentCaptor.forClass(Consumer.class);
    when(mockOtelRepository.registerObservableDoubleGauge(eq(mockMetricEntity), captor.capture())).thenReturn(null);
    return captor;
  }
}
