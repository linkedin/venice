package com.linkedin.venice.stats;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import io.tehuti.metrics.MetricConfig;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class VeniceMetricsRepositoryTest {
  @Test
  public void testDefaultConstructor() throws Exception {
    VeniceMetricsRepository repository = new VeniceMetricsRepository();
    assertNotNull(repository.getVeniceMetricsConfig(), "VeniceMetricsConfig should not be null.");
    assertNotNull(repository.getOpenTelemetryMetricsRepository(), "OpenTelemetryMetricsRepository should not be null.");
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
