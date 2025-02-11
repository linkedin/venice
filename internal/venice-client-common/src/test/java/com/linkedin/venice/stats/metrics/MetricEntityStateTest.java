package com.linkedin.venice.stats.metrics;

import static com.linkedin.venice.stats.metrics.MetricType.HISTOGRAM;
import static java.util.Collections.singletonList;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Count;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class MetricEntityStateTest {
  private VeniceOpenTelemetryMetricsRepository mockOtelRepository;
  private MetricEntity mockMetricEntity;
  private MetricEntityState.TehutiSensorRegistrationFunction sensorRegistrationFunction;
  private Sensor mockSensor;

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
    mockMetricEntity = mock(MetricEntity.class);
    doReturn(HISTOGRAM).when(mockMetricEntity).getMetricType();
    sensorRegistrationFunction = (name, stats) -> mock(Sensor.class);
    mockSensor = mock(Sensor.class);
  }

  @Test
  public void testCreateMetricWithOtelEnabled() {
    when(mockMetricEntity.getMetricType()).thenReturn(MetricType.COUNTER);
    LongCounter longCounter = mock(LongCounter.class);
    when(mockOtelRepository.createInstrument(mockMetricEntity)).thenReturn(longCounter);

    // without tehuti sensor
    MetricEntityState metricEntityState = new MetricEntityState(mockMetricEntity, mockOtelRepository);
    Assert.assertNotNull(metricEntityState);
    Assert.assertNotNull(metricEntityState.getOtelMetric());
    Assert.assertNull(metricEntityState.getTehutiSensor()); // No Tehuti sensors added

    // with tehuti sensor
    metricEntityState = new MetricEntityState(
        mockMetricEntity,
        mockOtelRepository,
        sensorRegistrationFunction,
        TestTehutiMetricNameEnum.TEST_METRIC,
        singletonList(new Count()));
    Assert.assertNotNull(metricEntityState);
    Assert.assertNotNull(metricEntityState.getOtelMetric());
    Assert.assertNotNull(metricEntityState.getTehutiSensor());
  }

  @Test
  public void testRecordOtelMetricHistogram() {
    DoubleHistogram doubleHistogram = mock(DoubleHistogram.class);
    when(mockMetricEntity.getMetricType()).thenReturn(HISTOGRAM);

    MetricEntityState metricEntityState = new MetricEntityState(mockMetricEntity, mockOtelRepository);
    metricEntityState.setOtelMetric(doubleHistogram);

    Attributes attributes = Attributes.builder().put("key", "value").build();
    metricEntityState.recordOtelMetric(5.5, attributes);

    verify(doubleHistogram, times(1)).record(5.5, attributes);
  }

  @Test
  public void testRecordOtelMetricCounter() {
    LongCounter longCounter = mock(LongCounter.class);
    when(mockMetricEntity.getMetricType()).thenReturn(MetricType.COUNTER);

    MetricEntityState metricEntityState = new MetricEntityState(mockMetricEntity, mockOtelRepository);
    metricEntityState.setOtelMetric(longCounter);

    Attributes attributes = Attributes.builder().put("key", "value").build();
    metricEntityState.recordOtelMetric(10, attributes);

    verify(longCounter, times(1)).add(10, attributes);
  }

  @Test
  public void testRecordTehutiMetric() {
    MetricEntityState metricEntityState = new MetricEntityState(mockMetricEntity, mockOtelRepository);
    metricEntityState.setTehutiSensor(mockSensor);
    metricEntityState.recordTehutiMetric(15.0);
    verify(mockSensor, times(1)).record(15.0);
  }

  @Test
  public void testRecordMetricsWithBothOtelAndTehuti() {
    DoubleHistogram doubleHistogram = mock(DoubleHistogram.class);
    when(mockMetricEntity.getMetricType()).thenReturn(HISTOGRAM);

    MetricEntityState metricEntityState = new MetricEntityState(mockMetricEntity, mockOtelRepository);
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
}
