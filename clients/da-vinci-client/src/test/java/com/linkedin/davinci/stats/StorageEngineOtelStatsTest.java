package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RECORD_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceRecordType;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collection;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StorageEngineOtelStatsTest {
  private static final String STORE_NAME = "test-store";
  private static final String CLUSTER_NAME = "test-cluster";
  private static final String METRIC_PREFIX = "server";

  private static final String DISK_USAGE_METRIC =
      StorageEngineOtelMetricEntity.DISK_USAGE.getMetricEntity().getMetricName();
  private static final String KEY_COUNT_METRIC =
      StorageEngineOtelMetricEntity.KEY_COUNT_ESTIMATE.getMetricEntity().getMetricName();
  private static final String OPEN_FAILURE_METRIC =
      StorageEngineOtelMetricEntity.ROCKSDB_OPEN_FAILURE_COUNT.getMetricEntity().getMetricName();

  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private StorageEngineOtelStats stats;

  @BeforeMethod
  public void setUp() {
    inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());
    stats = new StorageEngineOtelStats(metricsRepository, STORE_NAME, CLUSTER_NAME);
    stats.updateVersionInfo(1, 2);
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
  }

  // --- ASYNC_GAUGE tests for disk usage ---

  @Test
  public void testDiskUsageDataCurrentVersion() {
    AggVersionedStorageEngineStats.StorageEngineStatsWrapper wrapper = new MockWrapper(1000, 200, 50);
    stats.setStatsWrapper(1, wrapper);

    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        1000,
        buildDiskUsageAttributes(VersionRole.CURRENT, VeniceRecordType.DATA),
        DISK_USAGE_METRIC,
        METRIC_PREFIX);
  }

  @Test
  public void testDiskUsageRmdCurrentVersion() {
    AggVersionedStorageEngineStats.StorageEngineStatsWrapper wrapper = new MockWrapper(1000, 200, 50);
    stats.setStatsWrapper(1, wrapper);

    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        200,
        buildDiskUsageAttributes(VersionRole.CURRENT, VeniceRecordType.REPLICATION_METADATA),
        DISK_USAGE_METRIC,
        METRIC_PREFIX);
  }

  @Test
  public void testDiskUsageFutureVersion() {
    AggVersionedStorageEngineStats.StorageEngineStatsWrapper wrapper = new MockWrapper(2000, 300, 100);
    stats.setStatsWrapper(2, wrapper);

    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        2000,
        buildDiskUsageAttributes(VersionRole.FUTURE, VeniceRecordType.DATA),
        DISK_USAGE_METRIC,
        METRIC_PREFIX);
  }

  @Test
  public void testDiskUsageRmdFutureVersion() {
    AggVersionedStorageEngineStats.StorageEngineStatsWrapper wrapper = new MockWrapper(2000, 300, 100);
    stats.setStatsWrapper(2, wrapper);

    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        300,
        buildDiskUsageAttributes(VersionRole.FUTURE, VeniceRecordType.REPLICATION_METADATA),
        DISK_USAGE_METRIC,
        METRIC_PREFIX);
  }

  @Test
  public void testDiskUsageBackupVersion() {
    // current=1, future=2, version 3 is backup
    AggVersionedStorageEngineStats.StorageEngineStatsWrapper wrapper = new MockWrapper(3000, 500, 200);
    stats.setStatsWrapper(3, wrapper);

    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        3000,
        buildDiskUsageAttributes(VersionRole.BACKUP, VeniceRecordType.DATA),
        DISK_USAGE_METRIC,
        METRIC_PREFIX);
  }

  @Test
  public void testDiskUsageRmdBackupVersion() {
    // current=1, future=2, version 3 is backup
    AggVersionedStorageEngineStats.StorageEngineStatsWrapper wrapper = new MockWrapper(3000, 500, 200);
    stats.setStatsWrapper(3, wrapper);

    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        500,
        buildDiskUsageAttributes(VersionRole.BACKUP, VeniceRecordType.REPLICATION_METADATA),
        DISK_USAGE_METRIC,
        METRIC_PREFIX);
  }

  @Test
  public void testDiskUsageBackupSelectsSmallestVersion() {
    // current=1, future=2. Versions 3 and 5 are both backups — should select version 3 (smallest)
    stats.setStatsWrapper(3, new MockWrapper(3000, 300, 30));
    stats.setStatsWrapper(5, new MockWrapper(5000, 500, 50));

    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        3000,
        buildDiskUsageAttributes(VersionRole.BACKUP, VeniceRecordType.DATA),
        DISK_USAGE_METRIC,
        METRIC_PREFIX);
  }

  @Test
  public void testDiskUsageNoDataPointEmittedWhenNoWrapper() {
    // No wrapper set -> liveStateResolver returns null for all (role, recordType) pairs -> no
    // data point emitted. The instrument itself may or may not be in the output depending on SDK
    // behaviour; what matters is that no matching attribute set is present.
    Collection<MetricData> metrics = inMemoryMetricReader.collectAllMetrics();
    LongPointData point = OpenTelemetryDataTestUtils.getLongPointDataFromGaugeIfPresent(
        metrics,
        DISK_USAGE_METRIC,
        METRIC_PREFIX,
        buildDiskUsageAttributes(VersionRole.CURRENT, VeniceRecordType.DATA));
    assertNull(point);
  }

  // --- ASYNC_GAUGE tests for key count ---

  @Test
  public void testKeyCountCurrentVersion() {
    stats.setStatsWrapper(1, new MockWrapper(0, 0, 50));

    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        50,
        buildVersionRoleAttributes(VersionRole.CURRENT),
        KEY_COUNT_METRIC,
        METRIC_PREFIX);
  }

  @Test
  public void testKeyCountFutureVersion() {
    stats.setStatsWrapper(2, new MockWrapper(0, 0, 100));

    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        100,
        buildVersionRoleAttributes(VersionRole.FUTURE),
        KEY_COUNT_METRIC,
        METRIC_PREFIX);
  }

  @Test
  public void testKeyCountBackupVersion() {
    stats.setStatsWrapper(3, new MockWrapper(0, 0, 75));

    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        75,
        buildVersionRoleAttributes(VersionRole.BACKUP),
        KEY_COUNT_METRIC,
        METRIC_PREFIX);
  }

  @Test
  public void testKeyCountNoDataPointEmittedWhenNoWrapper() {
    Collection<MetricData> metrics = inMemoryMetricReader.collectAllMetrics();
    LongPointData point = OpenTelemetryDataTestUtils.getLongPointDataFromGaugeIfPresent(
        metrics,
        KEY_COUNT_METRIC,
        METRIC_PREFIX,
        buildVersionRoleAttributes(VersionRole.CURRENT));
    assertNull(point);
  }

  // --- COUNTER tests for RocksDB open failure ---

  @Test
  public void testRecordRocksDBOpenFailureCurrentVersion() {
    stats.recordRocksDBOpenFailure(1);

    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        buildVersionRoleAttributes(VersionRole.CURRENT),
        OPEN_FAILURE_METRIC,
        METRIC_PREFIX);
  }

  @Test
  public void testRecordRocksDBOpenFailureFutureVersion() {
    stats.recordRocksDBOpenFailure(2);

    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        buildVersionRoleAttributes(VersionRole.FUTURE),
        OPEN_FAILURE_METRIC,
        METRIC_PREFIX);
  }

  @Test
  public void testRecordRocksDBOpenFailureBackupVersion() {
    // current=1, future=2, version 3 is backup
    stats.recordRocksDBOpenFailure(3);

    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1,
        buildVersionRoleAttributes(VersionRole.BACKUP),
        OPEN_FAILURE_METRIC,
        METRIC_PREFIX);
  }

  @Test
  public void testRecordRocksDBOpenFailureVersionRoleIsolation() {
    // Record only for CURRENT (version 1) — FUTURE and BACKUP should not be incremented
    stats.recordRocksDBOpenFailure(1);
    stats.recordRocksDBOpenFailure(1);

    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    // CURRENT should have count 2
    LongPointData currentData = OpenTelemetryDataTestUtils.getLongPointDataFromSum(
        metricsData,
        OPEN_FAILURE_METRIC,
        METRIC_PREFIX,
        buildVersionRoleAttributes(VersionRole.CURRENT));
    assertNotNull(currentData, "CURRENT counter should exist");
    assertEquals(currentData.getValue(), 2);

    // FUTURE should have no data (never recorded)
    LongPointData futureData = OpenTelemetryDataTestUtils.getLongPointDataFromSum(
        metricsData,
        OPEN_FAILURE_METRIC,
        METRIC_PREFIX,
        buildVersionRoleAttributes(VersionRole.FUTURE));
    assertNull(futureData, "FUTURE counter should not exist — only CURRENT was recorded");
  }

  @Test
  public void testRecordRocksDBOpenFailureAccumulation() {
    stats.recordRocksDBOpenFailure(1);
    stats.recordRocksDBOpenFailure(1);
    stats.recordRocksDBOpenFailure(1);

    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        3,
        buildVersionRoleAttributes(VersionRole.CURRENT),
        OPEN_FAILURE_METRIC,
        METRIC_PREFIX);
  }

  // --- Version lifecycle tests ---

  @Test
  public void testLiveValueUpdatesAfterVersionInfoChange() {
    AggVersionedStorageEngineStats.StorageEngineStatsWrapper wrapper1 = new MockWrapper(1000, 100, 10);
    AggVersionedStorageEngineStats.StorageEngineStatsWrapper wrapper2 = new MockWrapper(2000, 200, 20);
    stats.setStatsWrapper(1, wrapper1);
    stats.setStatsWrapper(2, wrapper2);

    // Version 1 = CURRENT, version 2 = FUTURE
    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        1000,
        buildDiskUsageAttributes(VersionRole.CURRENT, VeniceRecordType.DATA),
        DISK_USAGE_METRIC,
        METRIC_PREFIX);

    // Swap: version 2 becomes CURRENT, version 3 becomes FUTURE
    stats.updateVersionInfo(2, 3);

    OpenTelemetryDataTestUtils.validateLongPointDataFromGauge(
        inMemoryMetricReader,
        2000,
        buildDiskUsageAttributes(VersionRole.CURRENT, VeniceRecordType.DATA),
        DISK_USAGE_METRIC,
        METRIC_PREFIX);
  }

  @Test
  public void testRemoveVersionClearsWrapper() {
    AggVersionedStorageEngineStats.StorageEngineStatsWrapper wrapper = new MockWrapper(1000, 100, 10);
    stats.setStatsWrapper(1, wrapper);
    stats.onVersionRemoved(1);

    // Wrapper removed -> liveStateResolver returns null -> no data point emitted for that combo.
    Collection<MetricData> metrics = inMemoryMetricReader.collectAllMetrics();
    LongPointData point = OpenTelemetryDataTestUtils.getLongPointDataFromGaugeIfPresent(
        metrics,
        DISK_USAGE_METRIC,
        METRIC_PREFIX,
        buildDiskUsageAttributes(VersionRole.CURRENT, VeniceRecordType.DATA));
    assertNull(point);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testSetStatsWrapperNullThrows() {
    stats.setStatsWrapper(1, null);
  }

  // --- NPE prevention tests ---

  @Test
  public void testNoNpeWhenOtelDisabled() {
    try (VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(false)
            .build())) {
      exerciseAllRecordingPaths(disabledRepo);
    }
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    exerciseAllRecordingPaths(new MetricsRepository());
  }

  // --- Helper methods ---

  /** Exercises all recording and lifecycle methods on a stats instance to verify no NPE. */
  private static void exerciseAllRecordingPaths(MetricsRepository repo) {
    StorageEngineOtelStats testStats = new StorageEngineOtelStats(repo, STORE_NAME, CLUSTER_NAME);
    testStats.updateVersionInfo(1, 2);
    testStats.recordRocksDBOpenFailure(1);
    testStats.setStatsWrapper(1, new MockWrapper(100, 10, 5));
    testStats.onVersionRemoved(1);
    testStats.close();
  }

  private static Attributes buildDiskUsageAttributes(VersionRole role, VeniceRecordType recordType) {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), role.getDimensionValue())
        .put(VENICE_RECORD_TYPE.getDimensionNameInDefaultFormat(), recordType.getDimensionValue())
        .build();
  }

  private static Attributes buildVersionRoleAttributes(VersionRole role) {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), role.getDimensionValue())
        .build();
  }

  @Test
  public void testVersionRoleEnumCount() {
    // getVersionForRole returns NON_EXISTING_VERSION for unknown roles — but a new VersionRole
    // value means the switch should be updated to handle it explicitly.
    assertEquals(
        VersionRole.values().length,
        3,
        "New VersionRole value added — update getVersionForRole switch in OtelVersionedStatsUtils");
  }

  @Test
  public void testVeniceRecordTypeEnumCount() {
    // getDiskUsageForRole has a default: throw for unknown VeniceRecordType values.
    // This guard catches new enum values that would throw at OTel collection time.
    assertEquals(
        VeniceRecordType.values().length,
        2,
        "New VeniceRecordType value added — update getDiskUsageForRole switch in StorageEngineOtelStats");
  }

  /**
   * Test wrapper that returns fixed values for disk usage, RMD usage, and key count.
   */
  private static class MockWrapper extends AggVersionedStorageEngineStats.StorageEngineStatsWrapper {
    private final long diskUsage;
    private final long rmdUsage;
    private final long keyCount;

    MockWrapper(long diskUsage, long rmdUsage, long keyCount) {
      this.diskUsage = diskUsage;
      this.rmdUsage = rmdUsage;
      this.keyCount = keyCount;
    }

    @Override
    public long getDiskUsageInBytes() {
      return diskUsage;
    }

    @Override
    public long getRMDDiskUsageInBytes() {
      return rmdUsage;
    }

    @Override
    public long getKeyCountEstimate() {
      return keyCount;
    }
  }
}
