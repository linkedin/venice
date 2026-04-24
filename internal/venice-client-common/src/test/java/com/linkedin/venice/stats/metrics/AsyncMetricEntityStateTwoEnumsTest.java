package com.linkedin.venice.stats.metrics;

import static com.linkedin.venice.stats.metrics.MetricEntityStateTest.DimensionEnum1;
import static com.linkedin.venice.stats.metrics.MetricEntityStateTest.DimensionEnum2;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link AsyncMetricEntityStateTwoEnums}. One OTel observable gauge is registered
 * per metric entity; its callback iterates the {@code (E1, E2)} product, calls
 * {@code liveStateResolver} to resolve the backing state (or skip if {@code null}), and emits via
 * {@code valueResolver}.
 */
public class AsyncMetricEntityStateTwoEnumsTest extends MetricEntityStateEnumTestBase {
  @BeforeMethod
  public void setUp() {
    setUpCommonMocks();
    when(mockMetricEntity.getMetricType()).thenReturn(ASYNC_GAUGE);
    when(mockMetricEntity.getMetricName()).thenReturn("test_async_gauge_two_enums_metric");

    Set<VeniceMetricsDimensions> dimensionsSet = createDimensionSet(
        DimensionEnum1.DIMENSION_ONE.getDimensionName(),
        DimensionEnum2.DIMENSION_ONE.getDimensionName());
    setupMockMetricEntity(dimensionsSet);
  }

  @Test
  public void testCreateWithoutOtelRepo() {
    AsyncMetricEntityStateTwoEnums<DimensionEnum1, DimensionEnum2> metricState = AsyncMetricEntityStateTwoEnums.create(
        mockMetricEntity,
        null,
        baseDimensionsMap,
        DimensionEnum1.class,
        DimensionEnum2.class,
        (e1, e2) -> e1,
        (state, e1, e2) -> 1L);

    assertNotNull(metricState);
    assertFalse(metricState.emitOpenTelemetryMetrics());
    assertNull(metricState.getAttributesByEnum());
    assertNull(metricState.getInstrument());
  }

  @Test
  public void testCreateRegistersExactlyOneObservableGauge() {
    AsyncMetricEntityStateTwoEnums<DimensionEnum1, DimensionEnum2> metricState = AsyncMetricEntityStateTwoEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        DimensionEnum2.class,
        (e1, e2) -> e1,
        (state, e1, e2) -> 1L);

    assertTrue(metricState.emitOpenTelemetryMetrics());
    verify(mockOtelRepository, times(1)).registerObservableLongGauge(eq(mockMetricEntity), any());
    verify(mockOtelRepository, times(0)).registerObservableDoubleGauge(eq(mockMetricEntity), any());

    int leafCount = 0;
    for (DimensionEnum1 e1: DimensionEnum1.values()) {
      for (DimensionEnum2 e2: DimensionEnum2.values()) {
        assertNotNull(metricState.getAttributesByEnum().get(e1).get(e2));
        leafCount++;
      }
    }
    assertEquals(leafCount, DimensionEnum1.values().length * DimensionEnum2.values().length);
  }

  @Test
  public void testCallbackEmitsOnlyWhenLiveStateResolverReturnsNonNull() {
    BiFunction<DimensionEnum1, DimensionEnum2, Object> liveStateResolver = (e1, e2) -> {
      if (e1 == DimensionEnum1.DIMENSION_ONE && e2 == DimensionEnum2.DIMENSION_ONE) {
        return "live";
      }
      return null;
    };

    ArgumentCaptor<Consumer<ObservableLongMeasurement>> callbackCaptor = captureLongCallback();
    AsyncMetricEntityStateTwoEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        DimensionEnum2.class,
        liveStateResolver,
        (state, e1, e2) -> 42L);
    Consumer<ObservableLongMeasurement> callback = callbackCaptor.getValue();

    ObservableLongMeasurement measurement = mock(ObservableLongMeasurement.class);
    callback.accept(measurement);

    verify(measurement, times(1)).record(eq(42L), any(Attributes.class));
  }

  /**
   * A throwing resolver for one {@code (e1, e2)} pair must NOT prevent emission for remaining
   * pairs in the same collection cycle.
   */
  @Test
  public void testThrowingResolverForOnePairDoesNotSkipOthers() {
    // Exactly (DIMENSION_ONE, DIMENSION_ONE) throws; every other pair must still emit.
    BiFunction<DimensionEnum1, DimensionEnum2, String> liveStateResolver = (e1, e2) -> {
      if (e1 == DimensionEnum1.DIMENSION_ONE && e2 == DimensionEnum2.DIMENSION_ONE) {
        throw new RuntimeException("boom");
      }
      return "live";
    };

    ArgumentCaptor<Consumer<ObservableLongMeasurement>> callbackCaptor = captureLongCallback();
    AsyncMetricEntityStateTwoEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        DimensionEnum2.class,
        liveStateResolver,
        (state, e1, e2) -> 3L);

    ObservableLongMeasurement measurement = mock(ObservableLongMeasurement.class);
    callbackCaptor.getValue().accept(measurement);

    // 1 pair threw; the remaining (|E1| * |E2| - 1) must still have emitted.
    int expected = DimensionEnum1.values().length * DimensionEnum2.values().length - 1;
    verify(measurement, times(expected)).record(eq(3L), any(Attributes.class));
    verify(mockOtelRepository, times(1)).recordFailureMetric(eq(mockMetricEntity), any(Exception.class));
  }

  @Test
  public void testValueResolverNotInvokedForDormantPairs() {
    ToDoubleTriFunction<Object, DimensionEnum1, DimensionEnum2> valueResolver = mock(ToDoubleTriFunction.class);
    when(valueResolver.applyAsDouble(any(), any(), any())).thenReturn(1.0);

    ArgumentCaptor<Consumer<ObservableLongMeasurement>> callbackCaptor = captureLongCallback();
    AsyncMetricEntityStateTwoEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        DimensionEnum2.class,
        (e1, e2) -> null, // always dormant
        valueResolver);
    callbackCaptor.getValue().accept(mock(ObservableLongMeasurement.class));

    verifyNoInteractions(valueResolver);
  }

  @Test
  public void testCallbackIsInvokedFreshOnEachCollection() {
    AtomicReference<Set<String>> live = new AtomicReference<>(new HashSet<>());
    BiFunction<DimensionEnum1, DimensionEnum2, String> liveStateResolver =
        (e1, e2) -> live.get().contains(e1.name() + "_" + e2.name()) ? "live" : null;

    ArgumentCaptor<Consumer<ObservableLongMeasurement>> callbackCaptor = captureLongCallback();
    AsyncMetricEntityStateTwoEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        DimensionEnum2.class,
        liveStateResolver,
        (state, e1, e2) -> 1L);
    Consumer<ObservableLongMeasurement> callback = callbackCaptor.getValue();

    // First collection: nothing live.
    ObservableLongMeasurement m1 = mock(ObservableLongMeasurement.class);
    callback.accept(m1);
    verifyNoInteractions(m1);

    // Transition one pair to live.
    Set<String> afterOne = new HashSet<>();
    afterOne.add(DimensionEnum1.DIMENSION_ONE.name() + "_" + DimensionEnum2.DIMENSION_TWO.name());
    live.set(afterOne);
    ObservableLongMeasurement m2 = mock(ObservableLongMeasurement.class);
    callback.accept(m2);
    verify(m2, times(1)).record(eq(1L), any(Attributes.class));

    // Back to dormant.
    live.set(new HashSet<>());
    ObservableLongMeasurement m3 = mock(ObservableLongMeasurement.class);
    callback.accept(m3);
    verifyNoInteractions(m3);
  }

  @Test
  public void testAsyncGaugeEmitsTruncatedLong() {
    ArgumentCaptor<Consumer<ObservableLongMeasurement>> callbackCaptor = captureLongCallback();
    AsyncMetricEntityStateTwoEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        DimensionEnum2.class,
        (e1, e2) -> e1,
        (state, e1, e2) -> 42.9);
    Consumer<ObservableLongMeasurement> callback = callbackCaptor.getValue();

    ObservableLongMeasurement measurement = mock(ObservableLongMeasurement.class);
    callback.accept(measurement);

    int expected = DimensionEnum1.values().length * DimensionEnum2.values().length;
    verify(measurement, times(expected)).record(eq(42L), any(Attributes.class));
  }

  @Test
  public void testAsyncDoubleGaugeEmitsDoubleDirectly() {
    when(mockMetricEntity.getMetricType()).thenReturn(MetricType.ASYNC_DOUBLE_GAUGE);

    ArgumentCaptor<Consumer<ObservableDoubleMeasurement>> callbackCaptor = captureDoubleCallback();
    AsyncMetricEntityStateTwoEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        DimensionEnum2.class,
        (e1, e2) -> e1,
        (state, e1, e2) -> 0.75);
    Consumer<ObservableDoubleMeasurement> callback = callbackCaptor.getValue();

    verify(mockOtelRepository, times(0)).registerObservableLongGauge(eq(mockMetricEntity), any());

    ObservableDoubleMeasurement measurement = mock(ObservableDoubleMeasurement.class);
    callback.accept(measurement);

    int expected = DimensionEnum1.values().length * DimensionEnum2.values().length;
    verify(measurement, times(expected)).record(eq(0.75), any(Attributes.class));
  }

  /** Non-null repo whose {@code emitOpenTelemetryMetrics()} returns false must also yield a disabled instance. */
  @Test
  public void testRepoPresentButEmissionDisabledYieldsDisabledInstance() {
    when(mockOtelRepository.emitOpenTelemetryMetrics()).thenReturn(false);

    AsyncMetricEntityStateTwoEnums<DimensionEnum1, DimensionEnum2> metricState = AsyncMetricEntityStateTwoEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        DimensionEnum2.class,
        (e1, e2) -> e1,
        (state, e1, e2) -> 1L);

    assertFalse(metricState.emitOpenTelemetryMetrics());
    assertNull(metricState.getAttributesByEnum());
    assertNull(metricState.getInstrument());
    verify(mockOtelRepository, times(0)).registerObservableLongGauge(eq(mockMetricEntity), any());
    verify(mockOtelRepository, times(0)).registerObservableDoubleGauge(eq(mockMetricEntity), any());
  }

  /**
   * Each {@code (e1, e2)} pair's emission must use its own precomputed {@link Attributes} — no
   * cross-pair attribute leakage.
   */
  @Test
  public void testEachPairEmitsWithItsOwnPrecomputedAttributes() {
    // valueResolver returns a distinct value per (e1, e2), so we can match (value, attrs) pairs.
    ArgumentCaptor<Consumer<ObservableLongMeasurement>> callbackCaptor = captureLongCallback();
    AsyncMetricEntityStateTwoEnums<DimensionEnum1, DimensionEnum2> metricState = AsyncMetricEntityStateTwoEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        DimensionEnum2.class,
        (e1, e2) -> e1,
        (state, e1, e2) -> e1.ordinal() * 100L + e2.ordinal());

    ObservableLongMeasurement measurement = mock(ObservableLongMeasurement.class);
    callbackCaptor.getValue().accept(measurement);

    for (DimensionEnum1 e1: DimensionEnum1.values()) {
      for (DimensionEnum2 e2: DimensionEnum2.values()) {
        Attributes expected = metricState.getAttributesByEnum().get(e1).get(e2);
        verify(measurement, times(1)).record(eq(e1.ordinal() * 100L + e2.ordinal()), eq(expected));
      }
    }
  }

  /**
   * The object returned by {@code liveStateResolver} for pair {@code (e1, e2)} must be the same
   * object handed to {@code valueResolver} for that same pair. Guards against wiring bugs that
   * cross state between pairs.
   */
  @Test
  public void testStateFromLiveStateResolverFlowsIntoValueResolver() {
    // Unique state per (e1, e2) — object identity check in the valueResolver.
    Map<String, Object> stateByPair = new HashMap<>();
    for (DimensionEnum1 e1: DimensionEnum1.values()) {
      for (DimensionEnum2 e2: DimensionEnum2.values()) {
        stateByPair.put(e1.name() + "_" + e2.name(), new Object());
      }
    }

    ArgumentCaptor<Consumer<ObservableLongMeasurement>> callbackCaptor = captureLongCallback();
    AsyncMetricEntityStateTwoEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        DimensionEnum2.class,
        (e1, e2) -> stateByPair.get(e1.name() + "_" + e2.name()),
        (state, e1, e2) -> {
          Object expected = stateByPair.get(e1.name() + "_" + e2.name());
          if (state != expected) {
            throw new AssertionError(
                "state mismatch for (" + e1 + "," + e2 + "): got " + state + ", expected " + expected);
          }
          return 1L;
        });

    // If any pair crosses state, the valueResolver throws -> callback propagates -> test fails.
    callbackCaptor.getValue().accept(mock(ObservableLongMeasurement.class));
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*ASYNC_GAUGE.*ASYNC_DOUBLE_GAUGE.*COUNTER.*")
  public void testCreateThrowsOnNonAsyncGaugeMetricType() {
    when(mockMetricEntity.getMetricType()).thenReturn(MetricType.COUNTER);
    when(mockMetricEntity.getMetricName()).thenReturn("test_counter_metric");
    AsyncMetricEntityStateTwoEnums.create(
        mockMetricEntity,
        mockOtelRepository,
        baseDimensionsMap,
        DimensionEnum1.class,
        DimensionEnum2.class,
        (e1, e2) -> e1,
        (state, e1, e2) -> 1L);
  }

  @SuppressWarnings("unchecked")
  private ArgumentCaptor<Consumer<ObservableLongMeasurement>> captureLongCallback() {
    ArgumentCaptor<Consumer<ObservableLongMeasurement>> captor = ArgumentCaptor.forClass(Consumer.class);
    when(mockOtelRepository.registerObservableLongGauge(eq(mockMetricEntity), captor.capture())).thenReturn(null);
    return captor;
  }

  @SuppressWarnings("unchecked")
  private ArgumentCaptor<Consumer<ObservableDoubleMeasurement>> captureDoubleCallback() {
    ArgumentCaptor<Consumer<ObservableDoubleMeasurement>> captor = ArgumentCaptor.forClass(Consumer.class);
    when(mockOtelRepository.registerObservableDoubleGauge(eq(mockMetricEntity), captor.capture())).thenReturn(null);
    return captor;
  }
}
