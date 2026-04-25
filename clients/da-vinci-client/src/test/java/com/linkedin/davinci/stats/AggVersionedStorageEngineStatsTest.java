package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.StorageEngineStats;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceRecordType;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.Metric;
import io.tehuti.metrics.MetricsRepository;
import java.util.Arrays;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AggVersionedStorageEngineStatsTest {
  @Test
  public void testVersionedStats() {
    String storeName = "testStore";
    MetricsRepository metricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = mock(Store.class);
    doReturn(storeName).when(mockStore).getName();
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(anyString());
    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(metricsRepository, metadataRepository, false, "test-cluster");
    stats.addStore(mockStore);
    stats.getStats(storeName, 1).getKeyCountEstimate();

    // Verify all 4 Tehuti sensors are registered on total stats
    assertNotNull(metricsRepository.getMetric(".testStore_total--rocksdb_key_count_estimate.Gauge"));
    assertEquals(metricsRepository.getMetric(".testStore_total--rocksdb_key_count_estimate.Gauge").value(), 0.0);
    assertNotNull(metricsRepository.getMetric(".testStore_total--disk_usage_in_bytes.Gauge"));
    assertNotNull(metricsRepository.getMetric(".testStore_total--rmd_disk_usage_in_bytes.Gauge"));
    assertNotNull(metricsRepository.getMetric(".testStore_total--rocksdb_open_failure_count.Gauge"));
  }

  @Test
  public void testSetStorageEngineWiresOtelStats() {
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    try (VeniceMetricsRepository veniceRepo = createOtelMetricsRepository(reader)) {
      OtelTestContext ctx = createOtelTestContext(veniceRepo);

      // setStorageEngine should wire the wrapper into OTel stats so ASYNC_GAUGE callbacks work
      ctx.stats.setStorageEngine(ctx.topicName, null); // null engine -> getDiskUsageInBytes() returns 0

      // Verify OTel stats were wired -- gauge returns 0 (storage engine is null, wrapper still registered)
      Attributes attrs = buildDiskUsageDataCurrentAttrs(ctx.clusterName, ctx.storeName);
      OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
          reader,
          0,
          attrs,
          StorageEngineOtelMetricEntity.DISK_USAGE.getMetricEntity().getMetricName(),
          OTEL_PREFIX);
    }
  }

  @Test
  public void testRecordRocksDBOpenFailureWiresBothTehutiAndOtel() {
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    try (VeniceMetricsRepository veniceRepo = createOtelMetricsRepository(reader)) {
      OtelTestContext ctx = createOtelTestContext(veniceRepo);

      // Record failure -- should increment both Tehuti AtomicInteger and OTel COUNTER
      ctx.stats.recordRocksDBOpenFailure(ctx.topicName);
      ctx.stats.recordRocksDBOpenFailure(ctx.topicName);

      // Verify Tehuti AtomicInteger was incremented
      assertEquals(ctx.stats.getStats(ctx.storeName, 1).getRocksDBOpenFailureCount(), 2);

      // Verify OTel COUNTER — version 1 with no current/future -> BACKUP
      Attributes attrs = buildVersionRoleAttributes(ctx.clusterName, ctx.storeName, VersionRole.BACKUP);

      OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
          reader,
          2,
          attrs,
          StorageEngineOtelMetricEntity.ROCKSDB_OPEN_FAILURE_COUNT.getMetricEntity().getMetricName(),
          OTEL_PREFIX);
    }
  }

  @Test
  public void testHandleStoreDeletedClearsOtelStats() {
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    try (VeniceMetricsRepository veniceRepo = createOtelMetricsRepository(reader)) {
      OtelTestContext ctx = createOtelTestContext(veniceRepo);
      ctx.stats.setStorageEngine(ctx.topicName, null);

      // After deletion, OTel gauges should return 0 (wrappers cleared by close())
      ctx.stats.handleStoreDeleted(ctx.storeName);

      Attributes attrs = buildDiskUsageDataCurrentAttrs(ctx.clusterName, ctx.storeName);
      OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
          reader,
          0,
          attrs,
          StorageEngineOtelMetricEntity.DISK_USAGE.getMetricEntity().getMetricName(),
          OTEL_PREFIX);

      // Re-adding the store should create fresh OTel stats without error
      ctx.stats.addStore(ctx.mockStore);
      ctx.stats.setStorageEngine(ctx.topicName, null);

      OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
          reader,
          0,
          attrs,
          StorageEngineOtelMetricEntity.DISK_USAGE.getMetricEntity().getMetricName(),
          OTEL_PREFIX);
    }
  }

  @Test
  public void testVersionInfoPropagatedOnStoreChanged() {
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    try (VeniceMetricsRepository veniceRepo = createOtelMetricsRepository(reader)) {
      OtelTestContext ctx = createOtelTestContext(veniceRepo);

      // Initially currentVersion=0, so version 1 is BACKUP
      ctx.stats.recordRocksDBOpenFailure(ctx.topicName);

      // Simulate version promotion: version 1 becomes current
      doReturn(1).when(ctx.mockStore).getCurrentVersion();
      ctx.stats.handleStoreChanged(ctx.mockStore);

      // Now version 1 is CURRENT — record another failure
      ctx.stats.recordRocksDBOpenFailure(ctx.topicName);

      // Verify OTel COUNTER recorded 1 for CURRENT (the post-promotion recording)
      Attributes currentAttrs = buildVersionRoleAttributes(ctx.clusterName, ctx.storeName, VersionRole.CURRENT);
      OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
          reader,
          1,
          currentAttrs,
          StorageEngineOtelMetricEntity.ROCKSDB_OPEN_FAILURE_COUNT.getMetricEntity().getMetricName(),
          OTEL_PREFIX);
    }
  }

  @Test
  public void testInvalidTopicNameIsIgnored() {
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    try (VeniceMetricsRepository veniceRepo = createOtelMetricsRepository(reader)) {
      OtelTestContext ctx = createOtelTestContext(veniceRepo);
      // Non-version topic names must be silently ignored — no exception, no state created
      ctx.stats.setStorageEngine("notAVersionTopic", null);
      ctx.stats.recordRocksDBOpenFailure("notAVersionTopic");
    }
  }

  @Test
  public void testMultiStoreIsolation() {
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    try (VeniceMetricsRepository veniceRepo = createOtelMetricsRepository(reader)) {
      String clusterName = "test-cluster";
      ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);

      Store storeA = createMockStore("storeA", metadataRepository);
      Store storeB = createMockStore("storeB", metadataRepository);

      AggVersionedStorageEngineStats stats =
          new AggVersionedStorageEngineStats(veniceRepo, metadataRepository, false, clusterName);
      stats.addStore(storeA);
      stats.addStore(storeB);

      // 2 failures for storeA, 1 for storeB — counts must be independent
      stats.recordRocksDBOpenFailure("storeA_v1");
      stats.recordRocksDBOpenFailure("storeA_v1");
      stats.recordRocksDBOpenFailure("storeB_v1");

      String metricName = StorageEngineOtelMetricEntity.ROCKSDB_OPEN_FAILURE_COUNT.getMetricEntity().getMetricName();

      Attributes attrsA = buildVersionRoleAttributes(clusterName, "storeA", VersionRole.BACKUP);
      Attributes attrsB = buildVersionRoleAttributes(clusterName, "storeB", VersionRole.BACKUP);

      OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(reader, 2, attrsA, metricName, OTEL_PREFIX);
      OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(reader, 1, attrsB, metricName, OTEL_PREFIX);
    }
  }

  @Test
  public void testNoNpeWithPlainMetricsRepositoryAtAggLayer() {
    // Production path: plain MetricsRepository (OTel disabled). All public recording methods must
    // be safe — this is the path that runs in production when OTel is not configured.
    MetricsRepository plainRepo = new MetricsRepository();
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = createMockStore("testStore", metadataRepository);
    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(plainRepo, metadataRepository, false, "test-cluster");
    stats.addStore(mockStore);
    stats.setStorageEngine("testStore_v1", null);
    stats.recordRocksDBOpenFailure("testStore_v1");
    stats.handleStoreDeleted("testStore");
  }

  @Test
  public void testCleanupVersionResourcesPropagatesOnVersionRemoved() {
    // cleanupVersionResources is called by AbstractVeniceAggVersionedStats when a version is
    // cleaned up. Verify it propagates onVersionRemoved to the OTel stats wrapper map so the
    // ASYNC_GAUGE callback returns 0 after the version is removed.
    InMemoryMetricReader reader = InMemoryMetricReader.create();
    try (VeniceMetricsRepository veniceRepo = createOtelMetricsRepository(reader)) {
      OtelTestContext ctx = createOtelTestContext(veniceRepo);

      // Wire version 1 with a non-zero disk usage. currentVersion=0 in the mock, so v1 is BACKUP.
      StorageEngineStats mockEngineStats = mock(StorageEngineStats.class);
      doReturn(5000L).when(mockEngineStats).getStoreSizeInBytes();
      StorageEngine mockEngine = mock(StorageEngine.class);
      doReturn(mockEngineStats).when(mockEngine).getStats();
      ctx.stats.setStorageEngine(ctx.topicName, mockEngine);

      // Confirm the BACKUP gauge returns 5000 before cleanup
      String diskMetric = StorageEngineOtelMetricEntity.DISK_USAGE.getMetricEntity().getMetricName();
      Attributes backupDataAttrs = Attributes.builder()
          .put(VeniceMetricsDimensions.VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), ctx.clusterName)
          .put(VeniceMetricsDimensions.VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), ctx.storeName)
          .put(
              VeniceMetricsDimensions.VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(),
              VersionRole.BACKUP.getDimensionValue())
          .put(
              VeniceMetricsDimensions.VENICE_RECORD_TYPE.getDimensionNameInDefaultFormat(),
              VeniceRecordType.DATA.getDimensionValue())
          .build();
      OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(reader, 5000, backupDataAttrs, diskMetric, OTEL_PREFIX);

      // AbstractVeniceAggVersionedStats.cleanupVersionResources is protected and in the same package
      // as this test, so it is directly accessible. This exercises the OTel propagation path
      // (otelStatsMap.computeIfPresent → stats.onVersionRemoved) without going through the full
      // AbstractVeniceAggVersionedStats version-lifecycle machinery.
      ctx.stats.cleanupVersionResources(ctx.storeName, 1);

      // Wrapper removed — BACKUP gauge must now return 0
      OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(reader, 0, backupDataAttrs, diskMetric, OTEL_PREFIX);
    }
  }

  // --- OTel test helpers ---

  private static final String OTEL_PREFIX = "server";

  private static VeniceMetricsRepository createOtelMetricsRepository(InMemoryMetricReader reader) {
    return new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(OTEL_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(reader)
            .build());
  }

  private static OtelTestContext createOtelTestContext(VeniceMetricsRepository veniceRepo) {
    String storeName = "testStore";
    String clusterName = "test-cluster";

    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = mock(Store.class);
    doReturn(storeName).when(mockStore).getName();
    doReturn(Collections.emptyList()).when(mockStore).getVersions();
    doReturn(0).when(mockStore).getCurrentVersion();
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(anyString());

    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(veniceRepo, metadataRepository, false, clusterName);
    stats.addStore(mockStore);

    return new OtelTestContext(storeName, clusterName, storeName + "_v1", stats, mockStore);
  }

  private static Store createMockStore(String name, ReadOnlyStoreRepository metadataRepository) {
    Store store = mock(Store.class);
    doReturn(name).when(store).getName();
    doReturn(Collections.emptyList()).when(store).getVersions();
    doReturn(0).when(store).getCurrentVersion();
    doReturn(store).when(metadataRepository).getStoreOrThrow(name);
    return store;
  }

  private static Attributes buildVersionRoleAttributes(String clusterName, String storeName, VersionRole role) {
    return Attributes.builder()
        .put(VeniceMetricsDimensions.VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), clusterName)
        .put(VeniceMetricsDimensions.VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VeniceMetricsDimensions.VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), role.getDimensionValue())
        .build();
  }

  private static Attributes buildDiskUsageDataCurrentAttrs(String clusterName, String storeName) {
    return Attributes.builder()
        .put(VeniceMetricsDimensions.VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), clusterName)
        .put(VeniceMetricsDimensions.VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(
            VeniceMetricsDimensions.VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(),
            VersionRole.CURRENT.getDimensionValue())
        .put(
            VeniceMetricsDimensions.VENICE_RECORD_TYPE.getDimensionNameInDefaultFormat(),
            VeniceRecordType.DATA.getDimensionValue())
        .build();
  }

  private static class OtelTestContext {
    final String storeName;
    final String clusterName;
    final String topicName;
    final AggVersionedStorageEngineStats stats;
    final Store mockStore;

    OtelTestContext(
        String storeName,
        String clusterName,
        String topicName,
        AggVersionedStorageEngineStats stats,
        Store mockStore) {
      this.storeName = storeName;
      this.clusterName = clusterName;
      this.topicName = topicName;
      this.stats = stats;
      this.mockStore = mockStore;
    }
  }

  @Test
  public void testDiskSizeDropAlertFiresOnVersionSwap() {
    String storeName = "testStore";
    MetricsRepository metricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = createMockStoreWithVersions(storeName, 1, 2);
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(anyString());

    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(metricsRepository, metadataRepository, false, 0.5, "test-cluster");
    stats.addStore(mockStore);

    // Set up storage engines: current version = 40GB, future version = 1MB (huge drop)
    setStorageEngineSize(stats, storeName, 1, 40L * 1024 * 1024 * 1024);
    setStorageEngineSize(stats, storeName, 2, 1L * 1024 * 1024);

    // Simulate version swap event - this is where the check happens
    stats.handleStoreChanged(mockStore);

    Metric alertMetric = metricsRepository.getMetric(".testStore--version_swap_disk_size_drop_alert.Gauge");
    Assert.assertNotNull(alertMetric, "Alert gauge should be registered");
    Assert.assertEquals(alertMetric.value(), 1.0, "Alert should record 1 when future version is much smaller");
  }

  @Test
  public void testDiskSizeDropAlertResetsWhenSizesSimilar() {
    String storeName = "testStore";
    MetricsRepository metricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = createMockStoreWithVersions(storeName, 1, 2);
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(anyString());

    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(metricsRepository, metadataRepository, false, 0.5, "test-cluster");
    stats.addStore(mockStore);

    // Set up storage engines: current version = 40GB, future version = 38GB (similar)
    setStorageEngineSize(stats, storeName, 1, 40L * 1024 * 1024 * 1024);
    setStorageEngineSize(stats, storeName, 2, 38L * 1024 * 1024 * 1024);

    stats.handleStoreChanged(mockStore);

    Metric alertMetric = metricsRepository.getMetric(".testStore--version_swap_disk_size_drop_alert.Gauge");
    Assert.assertNotNull(alertMetric);
    Assert.assertEquals(alertMetric.value(), 0.0, "Alert should record 0 when sizes are similar");
  }

  @Test
  public void testDiskSizeDropAlertRecordsZeroWithNoFutureVersion() {
    String storeName = "testStore";
    MetricsRepository metricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = mock(Store.class);
    doReturn(storeName).when(mockStore).getName();
    doReturn(1).when(mockStore).getCurrentVersion();
    // Explicitly return empty versions list — no future version exists
    doReturn(Collections.emptyList()).when(mockStore).getVersions();
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(anyString());

    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(metricsRepository, metadataRepository, false, 0.5, "test-cluster");
    stats.addStore(mockStore);

    // Simulate store change with no future version
    stats.handleStoreChanged(mockStore);

    Metric alertMetric = metricsRepository.getMetric(".testStore--version_swap_disk_size_drop_alert.Gauge");
    Assert.assertNotNull(alertMetric);
    Assert.assertEquals(alertMetric.value(), 0.0, "Alert should record 0 without future version");
  }

  @Test
  public void testDiskSizeDropAlertFiresWhenFutureSizeIsZero() {
    String storeName = "testStore";
    MetricsRepository metricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = createMockStoreWithVersions(storeName, 1, 2);
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(anyString());

    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(metricsRepository, metadataRepository, false, 0.5, "test-cluster");
    stats.addStore(mockStore);

    // Current version has data, future version has 0 bytes — total data loss.
    // Since future version is PUSHED (ingestion complete), this is a real alert.
    setStorageEngineSize(stats, storeName, 1, 40L * 1024 * 1024 * 1024);

    stats.handleStoreChanged(mockStore);

    Metric alertMetric = metricsRepository.getMetric(".testStore--version_swap_disk_size_drop_alert.Gauge");
    Assert.assertNotNull(alertMetric);
    Assert
        .assertEquals(alertMetric.value(), 1.0, "Alert should fire when PUSHED future version has 0 bytes (data loss)");
  }

  @Test
  public void testDiskSizeDropAlertRecordsZeroWhenCurrentSizeIsZero() {
    String storeName = "testStore";
    MetricsRepository metricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = createMockStoreWithVersions(storeName, 1, 2);
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(anyString());

    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(metricsRepository, metadataRepository, false, 0.5, "test-cluster");
    stats.addStore(mockStore);

    // Current version has no data (e.g., first version of a store), future has data
    setStorageEngineSize(stats, storeName, 2, 40L * 1024 * 1024 * 1024);

    stats.handleStoreChanged(mockStore);

    Metric alertMetric = metricsRepository.getMetric(".testStore--version_swap_disk_size_drop_alert.Gauge");
    Assert.assertNotNull(alertMetric);
    Assert.assertEquals(alertMetric.value(), 0.0, "Alert should record 0 when current version has no data");
  }

  @Test
  public void testDiskSizeDropAlertSkipsStartedVersion() {
    String storeName = "testStore";
    MetricsRepository metricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    // Future version is STARTED (still ingesting), not PUSHED
    Store mockStore = createMockStoreWithVersions(storeName, 1, 2, VersionStatus.STARTED);
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(anyString());

    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(metricsRepository, metadataRepository, false, 0.5, "test-cluster");
    stats.addStore(mockStore);

    // Even though sizes show a huge drop, the STARTED guard should prevent the alert
    setStorageEngineSize(stats, storeName, 1, 40L * 1024 * 1024 * 1024);
    setStorageEngineSize(stats, storeName, 2, 1L * 1024 * 1024);

    stats.handleStoreChanged(mockStore);

    Metric alertMetric = metricsRepository.getMetric(".testStore--version_swap_disk_size_drop_alert.Gauge");
    Assert.assertNotNull(alertMetric);
    Assert.assertEquals(alertMetric.value(), 0.0, "Alert should not fire when future version is still STARTED");
  }

  @Test
  public void testDiskSizeDropAlertSensorCleanedUpOnStoreDelete() {
    String storeName = "testStore";
    MetricsRepository metricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = createMockStoreWithVersions(storeName, 1, 2);
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(anyString());

    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(metricsRepository, metadataRepository, true, 0.5, "test-cluster");
    stats.addStore(mockStore);

    setStorageEngineSize(stats, storeName, 1, 40L * 1024 * 1024 * 1024);
    setStorageEngineSize(stats, storeName, 2, 1L * 1024 * 1024);

    // Trigger alert to create the sensor
    stats.handleStoreChanged(mockStore);
    Metric alertMetric = metricsRepository.getMetric(".testStore--version_swap_disk_size_drop_alert.Gauge");
    Assert.assertNotNull(alertMetric, "Sensor should exist before deletion");

    // Delete the store
    stats.handleStoreDeleted(storeName);

    // Verify the sensor is removed from MetricsRepository
    Metric removedMetric = metricsRepository.getMetric(".testStore--version_swap_disk_size_drop_alert.Gauge");
    Assert.assertNull(removedMetric, "Sensor should be removed from MetricsRepository after store deletion");
  }

  @Test
  public void testDiskSizeDropAlertRecordsZeroOnException() {
    String storeName = "testStore";
    MetricsRepository metricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);
    Store mockStore = createMockStoreWithVersions(storeName, 1, 2);
    doReturn(mockStore).when(metadataRepository).getStoreOrThrow(anyString());

    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(metricsRepository, metadataRepository, false, 0.5, "test-cluster");
    stats.addStore(mockStore);

    // Set current version with a storage engine that throws on getStats()
    StorageEngine throwingEngine = mock(StorageEngine.class);
    StorageEngineStats throwingStats = mock(StorageEngineStats.class);
    when(throwingEngine.getStats()).thenReturn(throwingStats);
    when(throwingStats.getStoreSizeInBytes()).thenThrow(new RuntimeException("RocksDB error"));
    stats.getStats(storeName, 1).setStorageEngine(throwingEngine);

    setStorageEngineSize(stats, storeName, 2, 1L * 1024 * 1024);

    stats.handleStoreChanged(mockStore);

    Metric alertMetric = metricsRepository.getMetric(".testStore--version_swap_disk_size_drop_alert.Gauge");
    Assert.assertNotNull(alertMetric);
    Assert.assertEquals(alertMetric.value(), 0.0, "Alert should record 0 when exception occurs");
  }

  @Test
  public void testDiskSizeDropAlertFiresThenResetsOnNextVersionSwap() {
    String storeName = "testStore";
    MetricsRepository metricsRepository = new MetricsRepository();
    ReadOnlyStoreRepository metadataRepository = mock(ReadOnlyStoreRepository.class);

    // First: version 2 is PUSHED with a huge drop — alert should fire
    Store mockStore1 = createMockStoreWithVersions(storeName, 1, 2);
    doReturn(mockStore1).when(metadataRepository).getStoreOrThrow(anyString());

    AggVersionedStorageEngineStats stats =
        new AggVersionedStorageEngineStats(metricsRepository, metadataRepository, false, 0.5, "test-cluster");
    stats.addStore(mockStore1);

    setStorageEngineSize(stats, storeName, 1, 40L * 1024 * 1024 * 1024);
    setStorageEngineSize(stats, storeName, 2, 1L * 1024 * 1024);

    stats.handleStoreChanged(mockStore1);
    Metric alertMetric = metricsRepository.getMetric(".testStore--version_swap_disk_size_drop_alert.Gauge");
    Assert.assertEquals(alertMetric.value(), 1.0, "Alert should fire on bad version swap");

    // Second: version swap completes — version 2 is now current, no future version
    Store mockStore2 = mock(Store.class);
    doReturn(storeName).when(mockStore2).getName();
    doReturn(2).when(mockStore2).getCurrentVersion();
    Version onlineVersion = new VersionImpl(storeName, 2, "push-job-2");
    onlineVersion.setStatus(VersionStatus.ONLINE);
    doReturn(Collections.singletonList(onlineVersion)).when(mockStore2).getVersions();
    doReturn(mockStore2).when(metadataRepository).getStoreOrThrow(anyString());

    stats.handleStoreChanged(mockStore2);
    Assert.assertEquals(alertMetric.value(), 0.0, "Alert should reset to 0 after version swap completes");
  }

  private Store createMockStoreWithVersions(String storeName, int currentVersionNum, int futureVersionNum) {
    return createMockStoreWithVersions(storeName, currentVersionNum, futureVersionNum, VersionStatus.PUSHED);
  }

  private Store createMockStoreWithVersions(
      String storeName,
      int currentVersionNum,
      int futureVersionNum,
      VersionStatus futureStatus) {
    Store mockStore = mock(Store.class);
    doReturn(storeName).when(mockStore).getName();
    doReturn(currentVersionNum).when(mockStore).getCurrentVersion();

    Version currentVersion = new VersionImpl(storeName, currentVersionNum, "push-job-1");
    currentVersion.setStatus(VersionStatus.ONLINE);

    Version futureVersion = new VersionImpl(storeName, futureVersionNum, "push-job-2");
    futureVersion.setStatus(futureStatus);

    doReturn(Arrays.asList(currentVersion, futureVersion)).when(mockStore).getVersions();
    doReturn(currentVersion).when(mockStore).getVersion(currentVersionNum);
    doReturn(futureVersion).when(mockStore).getVersion(futureVersionNum);
    return mockStore;
  }

  private void setStorageEngineSize(
      AggVersionedStorageEngineStats stats,
      String storeName,
      int version,
      long sizeInBytes) {
    StorageEngine mockEngine = mock(StorageEngine.class);
    StorageEngineStats mockEngineStats = mock(StorageEngineStats.class);
    when(mockEngine.getStats()).thenReturn(mockEngineStats);
    when(mockEngineStats.getStoreSizeInBytes()).thenReturn(sizeInBytes);
    stats.getStats(storeName, version).setStorageEngine(mockEngine);
  }
}
