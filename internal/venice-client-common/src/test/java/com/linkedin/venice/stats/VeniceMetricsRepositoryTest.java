package com.linkedin.venice.stats;

import static io.tehuti.metrics.stats.AsyncGauge.DEFAULT_ASYNC_GAUGE_EXECUTOR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import com.linkedin.venice.utils.DataProviderUtils;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsReporter;
import io.tehuti.metrics.TehutiMetric;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
    Collection<MetricEntity> metricEntities = new ArrayList<>();
    Set<VeniceMetricsDimensions> dimensionsSet = new HashSet<>();
    dimensionsSet.add(VeniceMetricsDimensions.VENICE_REQUEST_METHOD); // dummy
    metricEntities.add(
        new MetricEntity(
            "test_metric",
            MetricType.HISTOGRAM,
            MetricUnit.MILLISECOND,
            "Test description",
            dimensionsSet));
    VeniceMetricsConfig metricsConfig =
        new VeniceMetricsConfig.Builder().setEmitOtelMetrics(true).setMetricEntities(metricEntities).build();
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

  /**
   * Regression test for the try-finally contract in {@link VeniceMetricsRepository#close()}: if Tehuti's
   * {@code super.close()} throws (e.g., a misbehaving reporter), the OTel repository's {@code close()} MUST
   * still run so the SDK MeterProvider is shut down. Pre-fix (without try-finally) this would leak the OTel
   * exporter thread and could prevent JVM exit.
   */
  @Test
  public void testCloseRunsOtelCloseEvenIfSuperCloseThrows() {
    VeniceMetricsConfig mockConfig = Mockito.mock(VeniceMetricsConfig.class);
    VeniceOpenTelemetryMetricsRepository mockOpenTelemetryRepository =
        Mockito.mock(VeniceOpenTelemetryMetricsRepository.class);
    Mockito.when(mockConfig.getTehutiMetricConfig()).thenReturn(new MetricConfig());

    VeniceMetricsRepository repository = new VeniceMetricsRepository(mockConfig, mockOpenTelemetryRepository);
    // Add a Tehuti reporter that throws on close — this makes super.close() throw, exercising the try-finally.
    repository.addReporter(new MetricsReporter() {
      @Override
      public void init(List<TehutiMetric> metrics) {
      }

      @Override
      public void metricChange(TehutiMetric metric) {
      }

      @Override
      public void addMetric(TehutiMetric metric) {
      }

      @Override
      public void removeMetric(TehutiMetric metric) {
      }

      @Override
      public void close() {
        throw new RuntimeException("simulated Tehuti close failure");
      }

      @Override
      public void configure(java.util.Map<String, ?> configs) {
      }
    });

    try {
      repository.close();
    } catch (RuntimeException expected) {
      // Expected — close() rethrows after the finally block runs.
    }

    // The contract: OTel close MUST have been invoked even though super.close() threw.
    Mockito.verify(mockOpenTelemetryRepository).close();
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testGetVeniceMetricsRepositoryWithSingleThreadedConfig(boolean useSingleThreadedMetricsRepository) {
    VeniceMetricsRepository repository = VeniceMetricsRepository.getVeniceMetricsRepository(
        "test_service",
        "test_prefix",
        Collections.emptyList(),
        Collections.emptyMap(),
        useSingleThreadedMetricsRepository);

    assertNotNull(repository, "Repository should not be null");
    assertNotNull(repository.getVeniceMetricsConfig(), "VeniceMetricsConfig should not be null");

    MetricConfig tehutiConfig = repository.getVeniceMetricsConfig().getTehutiMetricConfig();
    assertNotNull(tehutiConfig, "TehutiMetricConfig should not be null");

    assertEquals(
        tehutiConfig.getAsyncGaugeExecutor().equals(DEFAULT_ASYNC_GAUGE_EXECUTOR),
        !useSingleThreadedMetricsRepository);
    repository.close();
  }
}
