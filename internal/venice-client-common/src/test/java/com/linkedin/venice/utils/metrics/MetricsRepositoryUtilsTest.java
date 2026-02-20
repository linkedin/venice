package com.linkedin.venice.utils.metrics;

import static com.linkedin.venice.stats.metrics.ModuleMetricEntityInterface.getUniqueMetricEntities;
import static org.testng.Assert.*;

import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.VeniceOpenTelemetryMetricNamingFormat;
import com.linkedin.venice.stats.metrics.ModuleMetricEntityInterfaceTest;
import com.linkedin.venice.utils.DataProviderUtils;
import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;


public class MetricsRepositoryUtilsTest {
  @Test
  public void testCreateSingleThreadedMetricsRepositoryWithCustomTimeouts() {
    long maxTimeout = TimeUnit.MINUTES.toMillis(2);
    long initialTimeout = 200;
    assertNotNull(MetricsRepositoryUtils.createSingleThreadedMetricsRepository(maxTimeout, initialTimeout));
  }

  @Test
  public void testCreateSingleThreadedVeniceMetricsRepository() {
    MetricsRepository repository = MetricsRepositoryUtils.createSingleThreadedVeniceMetricsRepository();

    assertNotNull(repository, "Repository should not be null");
    assertTrue(repository instanceof VeniceMetricsRepository, "Should return a VeniceMetricsRepository instance");

    VeniceMetricsRepository veniceRepo = (VeniceMetricsRepository) repository;
    assertFalse(veniceRepo.getVeniceMetricsConfig().emitOtelMetrics(), "OTel should be disabled by default");
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testCreateSingleThreadedVeniceMetricsRepositoryWithAllParameters(boolean otelEnabled) {
    long maxTimeout = TimeUnit.MINUTES.toMillis(5);
    long initialTimeout = 500;

    try {
      MetricsRepositoryUtils.createSingleThreadedVeniceMetricsRepository(
          maxTimeout,
          initialTimeout,
          otelEnabled,
          VeniceOpenTelemetryMetricNamingFormat.SNAKE_CASE,
          Collections.emptyList());
      if (otelEnabled) {
        fail();
      }
    } catch (Exception e) {
      if (!otelEnabled) {
        fail();
      }
      assertTrue(e.getCause() instanceof IllegalArgumentException);
      assertTrue(e.getCause().getMessage().contains("metricEntities cannot be empty"));
    }

    // with valid MetricEntities
    MetricsRepository repository = MetricsRepositoryUtils.createSingleThreadedVeniceMetricsRepository(
        maxTimeout,
        initialTimeout,
        otelEnabled,
        VeniceOpenTelemetryMetricNamingFormat.SNAKE_CASE,
        getUniqueMetricEntities(ModuleMetricEntityInterfaceTest.SingleEnumForTest.class));

    assertNotNull(repository, "Repository should not be null");
    assertTrue(repository instanceof VeniceMetricsRepository, "Should return a VeniceMetricsRepository instance");

    VeniceMetricsRepository veniceRepo = (VeniceMetricsRepository) repository;
    assertEquals(veniceRepo.getVeniceMetricsConfig().emitOtelMetrics(), otelEnabled);
  }

  @Test
  public void testCreateDefaultSingleThreadedMetricConfig() {
    MetricConfig config = MetricsRepositoryUtils.createDefaultSingleThreadedMetricConfig();
    assertNotNull(config, "Config should not be null");
    assertNotNull(config.getAsyncGaugeExecutor(), "AsyncGaugeExecutor should not be null");
  }
}
