package com.linkedin.venice.stats.metrics;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.Sensor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    sensorRegistrationFunction = (name, stats) -> mock(Sensor.class);
    mockSensor = mock(Sensor.class);
  }

  @Test
  public void testCreateMetricWithOtelEnabled() {
    when(mockMetricEntity.getMetricType()).thenReturn(MetricType.COUNTER);
    LongCounter longCounter = mock(LongCounter.class);
    when(mockOtelRepository.createInstrument(mockMetricEntity)).thenReturn(longCounter);

    Map<TehutiMetricNameEnum, List<MeasurableStat>> tehutiMetricInput = new HashMap<>();
    MetricEntityState metricEntityState =
        new MetricEntityState(mockMetricEntity, mockOtelRepository, sensorRegistrationFunction, tehutiMetricInput);

    Assert.assertNotNull(metricEntityState);
    Assert.assertNull(metricEntityState.getTehutiSensors()); // No Tehuti sensors added
  }

  @Test
  public void testAddTehutiSensorsSuccessfully() {
    MetricEntityState metricEntityState = new MetricEntityState(mockMetricEntity, mockOtelRepository);
    metricEntityState.addTehutiSensors(TestTehutiMetricNameEnum.TEST_METRIC, mockSensor);

    Assert.assertNotNull(metricEntityState.getTehutiSensors());
    Assert.assertTrue(metricEntityState.getTehutiSensors().containsKey(TestTehutiMetricNameEnum.TEST_METRIC));
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*Sensor with name 'TEST_METRIC' already exists.*")
  public void testAddTehutiSensorThrowsExceptionOnDuplicate() {
    MetricEntityState metricEntityState = new MetricEntityState(mockMetricEntity, mockOtelRepository);
    metricEntityState.addTehutiSensors(TestTehutiMetricNameEnum.TEST_METRIC, mockSensor);

    // Adding the same sensor name again should throw an exception
    metricEntityState.addTehutiSensors(TestTehutiMetricNameEnum.TEST_METRIC, mockSensor);
  }

  @Test
  public void testRecordOtelMetricHistogram() {
    DoubleHistogram doubleHistogram = mock(DoubleHistogram.class);
    when(mockMetricEntity.getMetricType()).thenReturn(MetricType.HISTOGRAM);

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
    metricEntityState.addTehutiSensors(TestTehutiMetricNameEnum.TEST_METRIC, mockSensor);

    metricEntityState.recordTehutiMetric(TestTehutiMetricNameEnum.TEST_METRIC, 15.0);

    verify(mockSensor, times(1)).record(15.0);
  }

  @Test
  public void testRecordMetricsWithBothOtelAndTehuti() {
    DoubleHistogram doubleHistogram = mock(DoubleHistogram.class);
    when(mockMetricEntity.getMetricType()).thenReturn(MetricType.HISTOGRAM);

    MetricEntityState metricEntityState = new MetricEntityState(mockMetricEntity, mockOtelRepository);
    metricEntityState.setOtelMetric(doubleHistogram);
    metricEntityState.addTehutiSensors(TestTehutiMetricNameEnum.TEST_METRIC, mockSensor);

    Attributes attributes = Attributes.builder().put("key", "value").build();
    metricEntityState.record(TestTehutiMetricNameEnum.TEST_METRIC, 20.0, attributes);

    verify(doubleHistogram, times(1)).record(20.0, attributes);
    verify(mockSensor, times(1)).record(20.0);
  }
}
