package com.linkedin.venice.stats.metrics;

import static com.linkedin.venice.read.RequestType.MULTI_GET_STREAMING;
import static com.linkedin.venice.stats.dimensions.RequestRetryType.ERROR_RETRY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_TYPE;
import static com.linkedin.venice.stats.metrics.MetricType.ASYNC_GAUGE;
import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.utils.Utils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import io.tehuti.metrics.stats.Count;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit test for {@link AsyncMetricEntityState}.
 */
public class AsyncMetricEntityStateTest {
  private VeniceOpenTelemetryMetricsRepository mockOtelRepository;
  private MetricEntity mockMetricEntity;
  private MetricEntityState.TehutiSensorRegistrationFunction sensorRegistrationFunction;
  private Map<VeniceMetricsDimensions, String> baseDimensionsMap;
  private Attributes baseAttributes;
  private MetricEntityStateBase recordFailureMetric;

  private enum TestTehutiMetricNameEnum implements TehutiMetricNameEnum {
    TEST_METRIC;

    private final String metricName;

    TestTehutiMetricNameEnum() {
      this.metricName = this.name().toLowerCase();
    }

    @Override
    public String getMetricName() {
      return this.metricName;
    }
  }

  @BeforeMethod
  public void setUp() {
    mockOtelRepository = mock(VeniceOpenTelemetryMetricsRepository.class);
    when(mockOtelRepository.emitOpenTelemetryMetrics()).thenReturn(true);
    when(mockOtelRepository.emitTehutiMetrics()).thenReturn(true);
    when(mockOtelRepository.getMetricFormat()).thenReturn(VeniceOpenTelemetryMetricNamingFormat.getDefaultFormat());
    when(mockOtelRepository.getDimensionName(any())).thenCallRealMethod();
    doCallRealMethod().when(mockOtelRepository).recordFailureMetric(any(), any(String.class));
    recordFailureMetric = Mockito.mock(MetricEntityStateBase.class);
    when(mockOtelRepository.getRecordFailureMetric()).thenReturn(recordFailureMetric);
    mockMetricEntity = mock(MetricEntity.class);
    doReturn(ASYNC_GAUGE).when(mockMetricEntity).getMetricType();
    Set<VeniceMetricsDimensions> dimensionsSet = new HashSet<>();
    dimensionsSet.add(VENICE_REQUEST_METHOD);
    doReturn(dimensionsSet).when(mockMetricEntity).getDimensionsList();
    sensorRegistrationFunction = (name, stats) -> mock(Sensor.class);
    baseDimensionsMap = new HashMap<>();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
    baseAttributes = Attributes.builder()
        .put(
            VENICE_REQUEST_METHOD.getDimensionName(VeniceOpenTelemetryMetricNamingFormat.getDefaultFormat()),
            MULTI_GET_STREAMING.getDimensionValue())
        .build();
  }

  @Test
  public void testCreateMetricWithOtelDisabled() {
    when(mockMetricEntity.getMetricType()).thenReturn(ASYNC_GAUGE);
    LongCounter longCounter = mock(LongCounter.class);
    when(mockOtelRepository.createInstrument(mockMetricEntity)).thenReturn(longCounter);

    // without tehuti sensor
    AsyncMetricEntityState metricEntityState =
        AsyncMetricEntityStateBase.create(mockMetricEntity, null, baseDimensionsMap, baseAttributes, () -> 0L);
    Assert.assertNotNull(metricEntityState);
    Assert.assertNull(metricEntityState.getOtelMetric());
    Assert.assertNull(metricEntityState.getTehutiSensor()); // No Tehuti sensors added

    // with tehuti sensor
    metricEntityState = AsyncMetricEntityStateBase.create(
        mockMetricEntity,
        null,
        sensorRegistrationFunction,
        TestTehutiMetricNameEnum.TEST_METRIC,
        singletonList(new AsyncGauge((ignored1, ignored2) -> 0, "test")),
        baseDimensionsMap,
        baseAttributes,
        () -> 0L);
    Assert.assertNotNull(metricEntityState);
    Assert.assertNull(metricEntityState.getOtelMetric());
    Assert.assertNotNull(metricEntityState.getTehutiSensor());
  }

  @Test
  public void testCreateMetricWithOtelEnabled() {
    when(mockMetricEntity.getMetricType()).thenReturn(ASYNC_GAUGE);
    LongCounter longCounter = mock(LongCounter.class);
    when(mockOtelRepository.createInstrument(any(MetricEntity.class), any(LongSupplier.class), any(Attributes.class)))
        .thenReturn(longCounter);

    // without tehuti sensor
    AsyncMetricEntityState metricEntityState = AsyncMetricEntityStateBase
        .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, baseAttributes, () -> 0L);
    Assert.assertNotNull(metricEntityState);
    Assert.assertNotNull(metricEntityState.getOtelMetric());
    Assert.assertNull(metricEntityState.getTehutiSensor()); // No Tehuti sensors added

    // with tehuti sensor
    metricEntityState = AsyncMetricEntityStateBase.create(
        mockMetricEntity,
        mockOtelRepository,
        sensorRegistrationFunction,
        TestTehutiMetricNameEnum.TEST_METRIC,
        singletonList(new AsyncGauge((ignored1, ignored2) -> 0, "test")),
        baseDimensionsMap,
        baseAttributes,
        () -> 0L);
    Assert.assertNotNull(metricEntityState);
    Assert.assertNotNull(metricEntityState.getOtelMetric());
    Assert.assertNotNull(metricEntityState.getTehutiSensor());
  }

  @Test
  public void testValidateRequiredDimensions() {
    Map<VeniceMetricsDimensions, String> baseDimensionsMap = new HashMap<>();
    // case 1: right values
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
    Attributes baseAttributes1 = getBaseAttributes(baseDimensionsMap);
    AsyncMetricEntityState metricEntityState = AsyncMetricEntityStateBase
        .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, baseAttributes1, () -> 0L);
    assertNotNull(metricEntityState);

    // case 2: baseAttributes have different count than baseDimensionsMap
    Attributes baseAttributes2 = Attributes.builder().build();
    try {
      AsyncMetricEntityStateBase
          .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, baseAttributes2, () -> 0L);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("should have the same size and values"));
    }

    // case 3: baseAttributes have same count as baseDimensionsMap but different content
    Map<VeniceMetricsDimensions, String> baseAttributes3Map = new HashMap<>();
    baseAttributes3Map.put(VENICE_REQUEST_RETRY_TYPE, ERROR_RETRY.getDimensionValue());
    Attributes baseAttributes3 = getBaseAttributes(baseAttributes3Map);
    try {
      AsyncMetricEntityStateBase
          .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, baseAttributes3, () -> 0L);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("should contain all the keys and same values as in baseDimensionsMap"));
    }

    // case 4: baseDimensionsMap has extra values
    baseDimensionsMap.clear();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
    baseDimensionsMap.put(VENICE_REQUEST_RETRY_TYPE, ERROR_RETRY.getDimensionValue());
    Attributes baseAttributes4 = getBaseAttributes(baseDimensionsMap);
    try {
      AsyncMetricEntityStateBase
          .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, baseAttributes4, () -> 0L);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("doesn't match with the required dimensions"));
    }

    // case 5: baseDimensionsMap has less values
    baseDimensionsMap.clear();
    Attributes baseAttributes5 = Attributes.builder().build();
    try {
      AsyncMetricEntityStateBase
          .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, baseAttributes5, () -> 0L);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("doesn't match with the required dimensions"));
    }

    // case 6: baseDimensionsMap has same count, but different dimensions
    baseDimensionsMap.clear();
    baseDimensionsMap.put(VENICE_REQUEST_RETRY_TYPE, ERROR_RETRY.getDimensionValue());
    Attributes baseAttributes6 = getBaseAttributes(baseDimensionsMap);
    try {
      AsyncMetricEntityStateBase
          .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, baseAttributes6, () -> 0L);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("doesn't match with the required dimensions"));
    }

    // case 7: baseAttributes has empty values
    baseDimensionsMap.clear();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, null);
    Attributes baseAttributes7 = getBaseAttributes(baseDimensionsMap);
    baseDimensionsMap.clear();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
    try {
      AsyncMetricEntityStateBase
          .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, baseAttributes7, () -> 0L);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("should have the same size and values"));
    }

    // case 8: baseDimensionsMap has empty values
    Attributes baseAttributes8 = getBaseAttributes(baseDimensionsMap);
    baseDimensionsMap.clear();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, null);
    try {
      AsyncMetricEntityStateBase
          .create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, baseAttributes8, () -> 0L);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("should contain all the keys and same values as in baseDimensionsMap"));
    }

    // case 9: baseAttributes is null. This is fine as long as emitting OTel metrics is disabled.
    when(mockOtelRepository.emitOpenTelemetryMetrics()).thenReturn(false);
    baseDimensionsMap.clear();
    baseDimensionsMap.put(VENICE_REQUEST_METHOD, MULTI_GET_STREAMING.getDimensionValue());
    try {
      AsyncMetricEntityStateBase.create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, null, () -> 0L);
    } catch (IllegalArgumentException e) {
      fail("baseAttributes can be null when emitting OTel metrics is disabled");
    }
    // Set it back to true for other tests.
    when(mockOtelRepository.emitOpenTelemetryMetrics()).thenReturn(true);

    // case 10: baseAttributes is null but emitting OTel metrics is enabled. This should throw exception.
    try {
      AsyncMetricEntityStateBase.create(mockMetricEntity, mockOtelRepository, baseDimensionsMap, null, () -> 0L);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Base attributes cannot be null"));
    }
  }

  @Test
  public void testValidateMetric() {
    // case 1: MetricType is ASYNC_GAUGE, but tehuti does not have AsyncGauge() in stats
    MetricEntity metricEntity = new MetricEntity(
        "test_metric",
        ASYNC_GAUGE,
        MetricUnit.NUMBER,
        "Test description",
        Utils.setOf(VENICE_REQUEST_METHOD));
    try {
      AsyncMetricEntityStateBase.create(
          metricEntity,
          mockOtelRepository,
          sensorRegistrationFunction,
          AsyncMetricEntityStateTest.TestTehutiMetricNameEnum.TEST_METRIC,
          singletonList(new Count()), // No AsyncGauge in stats
          baseDimensionsMap,
          baseAttributes,
          () -> 0L);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(
          e.getMessage()
              .contains(
                  "Tehuti metric stats does not contain AsyncGauge, but the otel metric type is ASYNC_GAUGE for metric"),
          e.getMessage());
    }

    // case 2: MetricType is ASYNC_GAUGE and tehuti stats have AsyncGauge() but have other stats as well
    metricEntity = new MetricEntity(
        "test_metric",
        ASYNC_GAUGE,
        MetricUnit.NUMBER,
        "Test description",
        Utils.setOf(VENICE_REQUEST_METHOD));
    try {
      AsyncMetricEntityStateBase.create(
          metricEntity,
          mockOtelRepository,
          sensorRegistrationFunction,
          AsyncMetricEntityStateTest.TestTehutiMetricNameEnum.TEST_METRIC,
          Arrays.asList(new AsyncGauge((ignored, ignored2) -> 0, "test"), new Count()),
          baseDimensionsMap,
          baseAttributes,
          () -> 0L);
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(
          e.getMessage().contains("Tehuti metric stats contains AsyncGauge, but it should be the only stat for metric"),
          e.getMessage());
    }

    // case 3: MetricType is ASYNC_GAUGE and tehuti stats have AsyncGauge(): No exception
    metricEntity = new MetricEntity(
        "test_metric",
        ASYNC_GAUGE,
        MetricUnit.NUMBER,
        "Test description",
        Utils.setOf(VENICE_REQUEST_METHOD));
    AsyncMetricEntityStateBase metricEntityState = AsyncMetricEntityStateBase.create(
        metricEntity,
        mockOtelRepository,
        sensorRegistrationFunction,
        AsyncMetricEntityStateTest.TestTehutiMetricNameEnum.TEST_METRIC,
        Arrays.asList(new AsyncGauge((ignored, ignored2) -> 0, "test")),
        baseDimensionsMap,
        baseAttributes,
        () -> 0L);
    assertNotNull(metricEntityState);
  }

  @Test
  public void testEmitTehutiMetricsEnabled() {
    when(mockOtelRepository.emitTehutiMetrics()).thenReturn(true);

    AsyncMetricEntityState metricEntityState = AsyncMetricEntityStateBase.create(
        mockMetricEntity,
        mockOtelRepository,
        sensorRegistrationFunction,
        TestTehutiMetricNameEnum.TEST_METRIC,
        singletonList(new AsyncGauge((ignored1, ignored2) -> 0, "test")),
        baseDimensionsMap,
        baseAttributes,
        () -> 0L);

    assertTrue(metricEntityState.emitTehutiMetrics(), "Should emit Tehuti metrics when enabled");
    assertNotNull(metricEntityState.getTehutiSensor(), "Tehuti sensor should be created when enabled");
  }

  @Test
  public void testEmitTehutiMetricsDisabled() {
    when(mockOtelRepository.emitTehutiMetrics()).thenReturn(false);

    AsyncMetricEntityState metricEntityState = AsyncMetricEntityStateBase.create(
        mockMetricEntity,
        mockOtelRepository,
        sensorRegistrationFunction,
        TestTehutiMetricNameEnum.TEST_METRIC,
        singletonList(new AsyncGauge((ignored1, ignored2) -> 0, "test")),
        baseDimensionsMap,
        baseAttributes,
        () -> 0L);

    assertFalse(metricEntityState.emitTehutiMetrics(), "Should not emit Tehuti metrics when disabled");
    Assert.assertNull(metricEntityState.getTehutiSensor(), "Tehuti sensor should not be created when disabled");
  }

  @Test
  public void testEmitTehutiMetricsWithNullRepository() {
    // When repository is null, Tehuti metrics should be disabled
    AsyncMetricEntityState metricEntityState = AsyncMetricEntityStateBase.create(
        mockMetricEntity,
        null,
        sensorRegistrationFunction,
        TestTehutiMetricNameEnum.TEST_METRIC,
        singletonList(new AsyncGauge((ignored1, ignored2) -> 0, "test")),
        baseDimensionsMap,
        baseAttributes,
        () -> 0L);

    assertTrue(metricEntityState.emitTehutiMetrics(), "Should emit Tehuti metrics when repository is null");
    assertNotNull(
        metricEntityState.getTehutiSensor(),
        "Tehuti sensor should still be created when registration function is provided");
  }

  @Test
  public void testEmitTehutiMetricsWithNullRegistrationFunction() {
    // When registration function is null, Tehuti metrics should be disabled
    AsyncMetricEntityState metricEntityState = AsyncMetricEntityStateBase.create(
        mockMetricEntity,
        mockOtelRepository,
        null,
        TestTehutiMetricNameEnum.TEST_METRIC,
        singletonList(new AsyncGauge((ignored1, ignored2) -> 0, "test")),
        baseDimensionsMap,
        baseAttributes,
        () -> 0L);

    assertFalse(
        metricEntityState.emitTehutiMetrics(),
        "Should not emit Tehuti metrics when registration function is null");
    Assert.assertNull(
        metricEntityState.getTehutiSensor(),
        "Tehuti sensor should not be created when registration function is null");
  }

  @Test
  public void testEmitTehutiMetricsWithEmptyStats() {
    // When Tehuti stats are empty, Tehuti metrics should be disabled
    AsyncMetricEntityState metricEntityState = AsyncMetricEntityStateBase.create(
        mockMetricEntity,
        mockOtelRepository,
        sensorRegistrationFunction,
        TestTehutiMetricNameEnum.TEST_METRIC,
        new ArrayList<>(), // Empty stats list
        baseDimensionsMap,
        baseAttributes,
        () -> 0L);

    assertFalse(metricEntityState.emitTehutiMetrics(), "Should not emit Tehuti metrics when stats are empty");
    Assert.assertNull(metricEntityState.getTehutiSensor(), "Tehuti sensor should not be created when stats are empty");
  }

  @Test
  public void testEmitTehutiMetricsIndependentOfOtel() {
    // Test that Tehuti metrics can be enabled independently of OTel metrics
    when(mockOtelRepository.emitOpenTelemetryMetrics()).thenReturn(false);
    when(mockOtelRepository.emitTehutiMetrics()).thenReturn(true);

    AsyncMetricEntityState metricEntityState = AsyncMetricEntityStateBase.create(
        mockMetricEntity,
        mockOtelRepository,
        sensorRegistrationFunction,
        TestTehutiMetricNameEnum.TEST_METRIC,
        singletonList(new AsyncGauge((ignored1, ignored2) -> 0, "test")),
        baseDimensionsMap,
        baseAttributes,
        () -> 0L);

    assertFalse(metricEntityState.emitOpenTelemetryMetrics(), "OTel metrics should be disabled");
    assertTrue(metricEntityState.emitTehutiMetrics(), "Tehuti metrics should be enabled independently");
    Assert.assertNull(metricEntityState.getOtelMetric(), "OTel metric should not be created");
    assertNotNull(metricEntityState.getTehutiSensor(), "Tehuti sensor should be created");
  }

  private Attributes getBaseAttributes(Map<VeniceMetricsDimensions, String> inputMap) {
    AttributesBuilder builder = Attributes.builder();
    for (Map.Entry<VeniceMetricsDimensions, String> entry: inputMap.entrySet()) {
      builder.put(
          entry.getKey().getDimensionName(VeniceOpenTelemetryMetricNamingFormat.getDefaultFormat()),
          entry.getValue());
    }
    return builder.build();
  }
}
