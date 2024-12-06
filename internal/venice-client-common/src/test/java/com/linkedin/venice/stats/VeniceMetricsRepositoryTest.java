package com.linkedin.venice.stats;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import io.tehuti.metrics.MetricConfig;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class VeniceMetricsRepositoryTest {
  @Test
  public void testDefaultConstructor() {
    VeniceMetricsRepository repository = new VeniceMetricsRepository();
    assertNotNull(repository.getVeniceMetricsConfig(), "VeniceMetricsConfig should not be null.");
    assertFalse(repository.getVeniceMetricsConfig().emitOtelMetrics());
    assertNull(
        repository.getOpenTelemetryMetricsRepository(),
        "OpenTelemetryMetricsRepository should be null if not enabled explicitly");
    repository.close();
  }

  @Test
  public void testConstructorWithMetricConfig() {
    VeniceMetricsConfig metricsConfig = new VeniceMetricsConfig.Builder().build();
    VeniceMetricsRepository repository = new VeniceMetricsRepository(metricsConfig);
    assertFalse(metricsConfig.emitOtelMetrics());

    assertEquals(
        repository.getVeniceMetricsConfig(),
        metricsConfig,
        "VeniceMetricsConfig should match the provided config.");
    assertNull(
        repository.getOpenTelemetryMetricsRepository(),
        "OpenTelemetryMetricsRepository should be null if not enabled explicitly");
    repository.close();
  }

  @Test
  public void testConstructorWithMetricConfigAndOtelEnabled() {
    VeniceMetricsConfig metricsConfig = new VeniceMetricsConfig.Builder().setEmitOtelMetrics(true).build();
    VeniceMetricsRepository repository = new VeniceMetricsRepository(metricsConfig);

    assertEquals(
        repository.getVeniceMetricsConfig(),
        metricsConfig,
        "VeniceMetricsConfig should match the provided config.");
    assertTrue(metricsConfig.emitOtelMetrics());
    assertNotNull(
        repository.getOpenTelemetryMetricsRepository(),
        "OpenTelemetryMetricsRepository should not be null if enabled explicitly");
    repository.close();
  }

  @Test
  public void testConstructorWithAllParameters() {
    VeniceMetricsConfig metricsConfig = new VeniceMetricsConfig.Builder().build();
    VeniceOpenTelemetryMetricsRepository openTelemetryMetricsRepository =
        new VeniceOpenTelemetryMetricsRepository(metricsConfig);
    VeniceMetricsRepository repository = new VeniceMetricsRepository(metricsConfig, openTelemetryMetricsRepository);

    assertEquals(
        repository.getVeniceMetricsConfig(),
        metricsConfig,
        "VeniceMetricsConfig should match the provided config.");
    assertEquals(
        repository.getOpenTelemetryMetricsRepository(),
        openTelemetryMetricsRepository,
        "OpenTelemetryMetricsRepository should match the provided instance.");
    repository.close();
  }

  @Test
  public void testCloseMethod() {
    VeniceMetricsConfig mockConfig = Mockito.mock(VeniceMetricsConfig.class);
    VeniceOpenTelemetryMetricsRepository mockOpenTelemetryRepository =
        Mockito.mock(VeniceOpenTelemetryMetricsRepository.class);
    Mockito.when(mockConfig.getTehutiMetricConfig()).thenReturn(new MetricConfig());

    VeniceMetricsRepository repository = new VeniceMetricsRepository(mockConfig, mockOpenTelemetryRepository);
    repository.close();

    // Verify that close methods are called
    Mockito.verify(mockOpenTelemetryRepository).close();
  }
}
