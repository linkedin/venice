package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.DaVinciRecordTransformerOtelMetricEntity.RECORD_TRANSFORMER_ERROR_COUNT;
import static com.linkedin.davinci.stats.DaVinciRecordTransformerOtelMetricEntity.RECORD_TRANSFORMER_LATENCY;
import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RECORD_TRANSFORMER_OPERATION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceRecordTransformerOperation;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class AggVersionedDaVinciRecordTransformerStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "davinci_client";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_STORE_NAME = "test-store";

  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private AggVersionedDaVinciRecordTransformerStats aggStats;

  @BeforeMethod
  public void setUp() {
    inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());
    aggStats = createAggStats(metricsRepository);
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
  }

  @Test
  public void testRecordPutLatency() {
    long timestamp = System.currentTimeMillis();
    aggStats.recordPutLatency(TEST_STORE_NAME, 1, 50.0, timestamp);
    aggStats.recordPutLatency(TEST_STORE_NAME, 1, 100.0, timestamp);

    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        inMemoryMetricReader,
        50.0,
        100.0,
        2,
        150.0,
        buildAttributes(VeniceRecordTransformerOperation.PUT),
        RECORD_TRANSFORMER_LATENCY.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testRecordDeleteLatency() {
    long timestamp = System.currentTimeMillis();
    aggStats.recordDeleteLatency(TEST_STORE_NAME, 1, 25.0, timestamp);

    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        inMemoryMetricReader,
        25.0,
        25.0,
        1,
        25.0,
        buildAttributes(VeniceRecordTransformerOperation.DELETE),
        RECORD_TRANSFORMER_LATENCY.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testRecordPutError() {
    long timestamp = System.currentTimeMillis();
    aggStats.recordPutError(TEST_STORE_NAME, 1, timestamp);
    aggStats.recordPutError(TEST_STORE_NAME, 1, timestamp);

    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        2,
        buildAttributes(VeniceRecordTransformerOperation.PUT),
        RECORD_TRANSFORMER_ERROR_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testRecordDeleteError() {
    long timestamp = System.currentTimeMillis();
    aggStats.recordDeleteError(TEST_STORE_NAME, 1, timestamp);

    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        buildAttributes(VeniceRecordTransformerOperation.DELETE),
        RECORD_TRANSFORMER_ERROR_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testOperationDimensionIsolation() {
    long timestamp = System.currentTimeMillis();
    aggStats.recordPutError(TEST_STORE_NAME, 1, timestamp);
    aggStats.recordPutError(TEST_STORE_NAME, 1, timestamp);
    aggStats.recordDeleteError(TEST_STORE_NAME, 1, timestamp);

    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        2,
        buildAttributes(VeniceRecordTransformerOperation.PUT),
        RECORD_TRANSFORMER_ERROR_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);

    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        buildAttributes(VeniceRecordTransformerOperation.DELETE),
        RECORD_TRANSFORMER_ERROR_COUNT.getMetricEntity().getMetricName(),
        TEST_METRIC_PREFIX);
  }

  // --- NPE prevention tests ---

  @Test
  public void testNoNpeWhenOtelDisabled() {
    try (VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build())) {
      exerciseAllRecordingPaths(disabledRepo);
    }
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    exerciseAllRecordingPaths(new MetricsRepository());
  }

  private void exerciseAllRecordingPaths(MetricsRepository repo) {
    AggVersionedDaVinciRecordTransformerStats safeStats = createAggStats(repo);
    long ts = System.currentTimeMillis();
    safeStats.recordPutLatency(TEST_STORE_NAME, 1, 10.0, ts);
    safeStats.recordDeleteLatency(TEST_STORE_NAME, 1, 10.0, ts);
    safeStats.recordPutError(TEST_STORE_NAME, 1, ts);
    safeStats.recordDeleteError(TEST_STORE_NAME, 1, ts);
  }

  // --- Helpers ---

  private static AggVersionedDaVinciRecordTransformerStats createAggStats(MetricsRepository repo) {
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = mock(Store.class);
    doReturn(TEST_STORE_NAME).when(mockStore).getName();
    doReturn(Collections.emptyList()).when(mockStore).getVersions();
    doReturn(0).when(mockStore).getCurrentVersion();
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(anyString());

    VeniceServerConfig serverConfig = mock(VeniceServerConfig.class);
    doReturn(false).when(serverConfig).isUnregisterMetricForDeletedStoreEnabled();
    doReturn(TEST_CLUSTER_NAME).when(serverConfig).getClusterName();

    return new AggVersionedDaVinciRecordTransformerStats(repo, metadataRepository, serverConfig);
  }

  private Attributes buildAttributes(VeniceRecordTransformerOperation operation) {
    return Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
        .put(VENICE_RECORD_TRANSFORMER_OPERATION.getDimensionNameInDefaultFormat(), operation.getDimensionValue())
        .build();
  }
}
