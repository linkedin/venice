package com.linkedin.venice.stats.metrics;

import static org.mockito.Mockito.*;

import com.linkedin.venice.stats.VeniceOpenTelemetryMetricsRepository;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.tehuti.metrics.MeasurableStat;
import io.tehuti.metrics.Sensor;
import java.util.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class MetricEntityStateTest {
  private VeniceOpenTelemetryMetricsRepository mockOtelRepository;
  private MetricEntity mockMetricEntity;
  private MetricEntityState.TehutiSensorRegistrationFunction sensorRegistrationFunction;
  private Sensor mockSensor;

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

    Map<String, List<MeasurableStat>> tehutiMetricInput = new HashMap<>();
    MetricEntityState metricEntityState =
        new MetricEntityState(mockMetricEntity, mockOtelRepository, sensorRegistrationFunction, tehutiMetricInput);

    Assert.assertNotNull(metricEntityState);
    Assert.assertNull(metricEntityState.getTehutiSensors()); // No Tehuti sensors added
  }

  @Test
  public void testAddTehutiSensorsSuccessfully() {
    MetricEntityState metricEntityState = new MetricEntityState(mockMetricEntity, mockOtelRepository);
    metricEntityState.addTehutiSensors("testSensor", mockSensor);

    Assert.assertNotNull(metricEntityState.getTehutiSensors());
    Assert.assertTrue(metricEntityState.getTehutiSensors().containsKey("testSensor"));
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*Sensor with name 'testSensor' already exists.*")
  public void testAddTehutiSensorThrowsExceptionOnDuplicate() {
    MetricEntityState metricEntityState = new MetricEntityState(mockMetricEntity, mockOtelRepository);
    metricEntityState.addTehutiSensors("testSensor", mockSensor);

    // Adding the same sensor name again should throw an exception
    metricEntityState.addTehutiSensors("testSensor", mockSensor);
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
    metricEntityState.addTehutiSensors("testSensor", mockSensor);

    metricEntityState.recordTehutiMetric("testSensor", 15.0);

    verify(mockSensor, times(1)).record(15.0);
  }

  @Test
  public void testRecordMetricsWithBothOtelAndTehuti() {
    DoubleHistogram doubleHistogram = mock(DoubleHistogram.class);
    when(mockMetricEntity.getMetricType()).thenReturn(MetricType.HISTOGRAM);

    MetricEntityState metricEntityState = new MetricEntityState(mockMetricEntity, mockOtelRepository);
    metricEntityState.setOtelMetric(doubleHistogram);
    metricEntityState.addTehutiSensors("testSensor", mockSensor);

    Attributes attributes = Attributes.builder().put("key", "value").build();
    metricEntityState.record("testSensor", 20.0, attributes);

    verify(doubleHistogram, times(1)).record(20.0, attributes);
    verify(mockSensor, times(1)).record(20.0);
  }
}
