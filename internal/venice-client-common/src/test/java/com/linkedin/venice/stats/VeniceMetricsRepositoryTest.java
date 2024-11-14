package com.linkedin.venice.stats;

import static org.testng.Assert.assertEquals;

import io.tehuti.Metric;
import io.tehuti.metrics.Measurable;
import io.tehuti.metrics.MetricsReporter;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.AsyncGauge;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VeniceMetricsRepositoryTest {
  private VeniceMetricsRepository metricsRepository;
  private VeniceOpenTelemetryMetricsRepository mockOpenTelemetryMetricsRepository;
  private MetricsRepository mockDelegate;

  @BeforeMethod
  public void setUp() {
    VeniceMetricsConfig config = new VeniceMetricsConfig.VeniceMetricsConfigBuilder().build();
    mockOpenTelemetryMetricsRepository = Mockito.mock(VeniceOpenTelemetryMetricsRepository.class);
    mockDelegate = Mockito.mock(MetricsRepository.class);
    metricsRepository = new VeniceMetricsRepository(mockDelegate, config, mockOpenTelemetryMetricsRepository);
  }

  @AfterMethod
  public void tearDown() {
    metricsRepository.close();
  }

  @Test
  public void testConstructorWithDelegateAndConfig() {
    VeniceMetricsConfig config = new VeniceMetricsConfig.VeniceMetricsConfigBuilder().build();
    VeniceMetricsRepository repo =
        new VeniceMetricsRepository(mockDelegate, config, mockOpenTelemetryMetricsRepository);

    assertEquals(repo.getVeniceMetricsConfig(), config);
    assertEquals(repo.getOpenTelemetryMetricsRepository(), mockOpenTelemetryMetricsRepository);
  }

  @Test
  public void testCloseWithDelegate() {
    metricsRepository.close();
    Mockito.verify(mockDelegate, Mockito.times(1)).close();
    Mockito.verify(mockOpenTelemetryMetricsRepository, Mockito.times(1)).close();
  }

  @Test
  public void testAddMetricDelegation() {
    Measurable measurable = Mockito.mock(Measurable.class);
    Metric metric = Mockito.mock(Metric.class);

    Mockito.when(mockDelegate.addMetric("testMetric", measurable)).thenReturn(metric);

    Metric returnedMetric = metricsRepository.addMetric("testMetric", measurable);
    assertEquals(returnedMetric, metric);

    Mockito.verify(mockDelegate, Mockito.times(1)).addMetric("testMetric", measurable);
  }

  @Test
  public void testGetSensorDelegation() {
    Sensor sensor = Mockito.mock(Sensor.class);
    Mockito.when(mockDelegate.getSensor("testSensor")).thenReturn(sensor);

    Sensor returnedSensor = metricsRepository.getSensor("testSensor");
    assertEquals(returnedSensor, sensor);

    Mockito.verify(mockDelegate, Mockito.times(1)).getSensor("testSensor");
  }

  @Test
  public void testMetricsRetrieval() {
    Map<String, Metric> mockMetrics = Mockito.mock(Map.class);
    Mockito.doReturn(mockMetrics).when(mockDelegate).metrics();

    Map<String, ? extends Metric> retrievedMetrics = metricsRepository.metrics();
    assertEquals(retrievedMetrics, mockMetrics);

    Mockito.verify(mockDelegate, Mockito.times(1)).metrics();
  }

  @Test
  public void testGetMetricDelegation() {
    Metric metric = Mockito.mock(Metric.class);
    Mockito.when(mockDelegate.getMetric("testMetric")).thenReturn(metric);

    Metric retrievedMetric = metricsRepository.getMetric("testMetric");
    assertEquals(retrievedMetric, metric);

    Mockito.verify(mockDelegate, Mockito.times(1)).getMetric("testMetric");
  }

  @Test
  public void testAddReporterDelegation() {
    MetricsReporter mockReporter = Mockito.mock(MetricsReporter.class);

    metricsRepository.addReporter(mockReporter);
    Mockito.verify(mockDelegate, Mockito.times(1)).addReporter(mockReporter);
  }

  @Test
  public void testAsyncGaugeExecutorDelegation() {
    AsyncGauge.AsyncGaugeExecutor asyncGaugeExecutor = Mockito.mock(AsyncGauge.AsyncGaugeExecutor.class);
    Mockito.when(mockDelegate.getAsyncGaugeExecutor()).thenReturn(asyncGaugeExecutor);

    AsyncGauge.AsyncGaugeExecutor executor = metricsRepository.getAsyncGaugeExecutor();
    assertEquals(executor, asyncGaugeExecutor);

    Mockito.verify(mockDelegate, Mockito.times(1)).getAsyncGaugeExecutor();
  }

  @Test
  public void testSensorCreationAndDeletionWithDelegate() {
    Sensor mockSensor = Mockito.mock(Sensor.class);
    Mockito.when(mockDelegate.sensor("testSensor")).thenReturn(mockSensor);

    Sensor sensor = metricsRepository.sensor("testSensor");
    assertEquals(sensor, mockSensor);

    metricsRepository.removeSensor("testSensor");
    Mockito.verify(mockDelegate, Mockito.times(1)).sensor("testSensor");
    Mockito.verify(mockDelegate, Mockito.times(1)).removeSensor("testSensor");
  }
}
