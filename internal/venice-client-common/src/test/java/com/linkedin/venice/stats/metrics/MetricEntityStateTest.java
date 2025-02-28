package com.linkedin.venice.stats.metrics;

import static com.linkedin.venice.stats.metrics.MetricType.HISTOGRAM;
import static java.util.Collections.singletonList;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceDimensionInterface;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class MetricEntityStateTest {
  private VeniceOpenTelemetryMetricsRepository mockOtelRepository;
  private MetricEntity mockMetricEntity;
  private MetricEntityState.TehutiSensorRegistrationFunction sensorRegistrationFunction;
  private Sensor mockSensor;
  Map<VeniceMetricsDimensions, String> baseDimensionsMap;
  private Attributes baseAttributes;

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
    mockMetricEntity = mock(MetricEntity.class);
    doReturn(HISTOGRAM).when(mockMetricEntity).getMetricType();
    Set<VeniceMetricsDimensions> dimensionsSet = new HashSet<>();
    dimensionsSet.add(VeniceMetricsDimensions.VENICE_REQUEST_METHOD);
    doReturn(dimensionsSet).when(mockMetricEntity).getDimensionsList();
    sensorRegistrationFunction = (name, stats) -> mock(Sensor.class);
    mockSensor = mock(Sensor.class);
    baseDimensionsMap = new HashMap<>();
    baseDimensionsMap
        .put(VeniceMetricsDimensions.VENICE_REQUEST_METHOD, RequestType.MULTI_GET_STREAMING.getDimensionValue());
    baseAttributes = Attributes.builder()
        .put(
            VeniceMetricsDimensions.VENICE_REQUEST_METHOD
                .getDimensionName(VeniceOpenTelemetryMetricNamingFormat.getDefaultFormat()),
            RequestType.MULTI_GET_STREAMING.getDimensionValue())
        .build();
  }

  @Test
  public void testCreateMetricWithOtelEnabled() {
    when(mockMetricEntity.getMetricType()).thenReturn(MetricType.COUNTER);
    LongCounter longCounter = mock(LongCounter.class);
    when(mockOtelRepository.createInstrument(mockMetricEntity)).thenReturn(longCounter);

    // without tehuti sensor
    MetricEntityState metricEntityState =
        new MetricEntityStateBase(mockMetricEntity, mockOtelRepository, baseDimensionsMap, baseAttributes);
    Assert.assertNotNull(metricEntityState);
    Assert.assertNotNull(metricEntityState.getOtelMetric());
    Assert.assertNull(metricEntityState.getTehutiSensor()); // No Tehuti sensors added
    Assert.assertEquals(((MetricEntityStateBase) metricEntityState).getAttributes(), baseAttributes);

    // with tehuti sensor
    metricEntityState = new MetricEntityStateBase(
        mockMetricEntity,
        mockOtelRepository,
        sensorRegistrationFunction,
        TestTehutiMetricNameEnum.TEST_METRIC,
        singletonList(new Count()),
        baseDimensionsMap,
        baseAttributes);
    Assert.assertNotNull(metricEntityState);
    Assert.assertNotNull(metricEntityState.getOtelMetric());
    Assert.assertNotNull(metricEntityState.getTehutiSensor());
    Assert.assertEquals(((MetricEntityStateBase) metricEntityState).getAttributes(), baseAttributes);
  }

  @Test
  public void testRecordOtelMetricHistogram() {
    DoubleHistogram doubleHistogram = mock(DoubleHistogram.class);
    when(mockMetricEntity.getMetricType()).thenReturn(HISTOGRAM);

    MetricEntityState metricEntityState =
        new MetricEntityStateBase(mockMetricEntity, mockOtelRepository, baseDimensionsMap, baseAttributes);
    metricEntityState.setOtelMetric(doubleHistogram);

    Attributes attributes = Attributes.builder().put("key", "value").build();
    metricEntityState.recordOtelMetric(5.5, attributes);

    verify(doubleHistogram, times(1)).record(5.5, attributes);
  }

  @Test
  public void testRecordOtelMetricCounter() {
    LongCounter longCounter = mock(LongCounter.class);
    when(mockMetricEntity.getMetricType()).thenReturn(MetricType.COUNTER);

    MetricEntityState metricEntityState =
        new MetricEntityStateBase(mockMetricEntity, mockOtelRepository, baseDimensionsMap, baseAttributes);
    metricEntityState.setOtelMetric(longCounter);

    Attributes attributes = Attributes.builder().put("key", "value").build();
    metricEntityState.recordOtelMetric(10, attributes);

    verify(longCounter, times(1)).add(10, attributes);
  }

  @Test
  public void testRecordTehutiMetric() {
    MetricEntityState metricEntityState =
        new MetricEntityStateBase(mockMetricEntity, mockOtelRepository, baseDimensionsMap, baseAttributes);
    metricEntityState.setTehutiSensor(mockSensor);
    metricEntityState.recordTehutiMetric(15.0);
    verify(mockSensor, times(1)).record(15.0);
  }

  @Test
  public void testRecordMetricsWithBothOtelAndTehuti() {
    DoubleHistogram doubleHistogram = mock(DoubleHistogram.class);
    when(mockMetricEntity.getMetricType()).thenReturn(HISTOGRAM);

    MetricEntityState metricEntityState =
        new MetricEntityStateBase(mockMetricEntity, mockOtelRepository, baseDimensionsMap, baseAttributes);
    metricEntityState.setOtelMetric(doubleHistogram);
    metricEntityState.setTehutiSensor(mockSensor);

    Attributes attributes = Attributes.builder().put("key", "value").build();

    // called 0 times
    verify(doubleHistogram, times(0)).record(20.0, attributes);
    verify(mockSensor, times(0)).record(20.0);

    // called 1 time
    metricEntityState.record(20.0, attributes);
    verify(doubleHistogram, times(1)).record(20.0, attributes);
    verify(mockSensor, times(1)).record(20.0);

    // called 2 times
    metricEntityState.record(20.0, attributes);
    verify(doubleHistogram, times(2)).record(20.0, attributes);
    verify(mockSensor, times(2)).record(20.0);
  }

  enum DimensionEnum1 implements VeniceDimensionInterface {
    DIMENSION_ONE(), DIMENSION_TWO();

    private final String dimensionValue;

    DimensionEnum1() {
      this.dimensionValue = "value_" + name().toLowerCase();
    }

    @Override
    public VeniceMetricsDimensions getDimensionName() {
      return VeniceMetricsDimensions.VENICE_STORE_NAME; // Dummy dimension
    }

    @Override
    public String getDimensionValue() {
      return dimensionValue;
    }
  }

  enum DimensionEnum2 implements VeniceDimensionInterface {
    DIMENSION_ONE(), DIMENSION_TWO();

    private final String dimensionValue;

    DimensionEnum2() {
      this.dimensionValue = "value_" + name().toLowerCase();
    }

    @Override
    public VeniceMetricsDimensions getDimensionName() {
      return VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE; // Dummy dimension
    }

    @Override
    public String getDimensionValue() {
      return dimensionValue;
    }
  }

  enum DimensionEnum3 implements VeniceDimensionInterface {
    DIMENSION_ONE(), DIMENSION_TWO();

    private final String dimensionValue;

    DimensionEnum3() {
      this.dimensionValue = "value_" + name().toLowerCase();
    }

    @Override
    public VeniceMetricsDimensions getDimensionName() {
      return VeniceMetricsDimensions.HTTP_RESPONSE_STATUS_CODE_CATEGORY; // Dummy dimension
    }

    @Override
    public String getDimensionValue() {
      return dimensionValue;
    }
  }

  enum EmptyDimensionEnum implements VeniceDimensionInterface {
    ; // Empty enum
    @Override
    public VeniceMetricsDimensions getDimensionName() {
      return VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
    }

    @Override
    public String getDimensionValue() {
      throw new UnsupportedOperationException(); // Should not be called in this test context
    }
  }
}
