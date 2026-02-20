package com.linkedin.davinci.stats.ingestion;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.BATCH_PROCESSING_REQUEST_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.BATCH_PROCESSING_REQUEST_ERROR_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.BATCH_PROCESSING_REQUEST_RECORD_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.BATCH_PROCESSING_REQUEST_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.DCR_EVENT_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.DCR_TOTAL_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.DUPLICATE_KEY_UPDATE_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_BYTES_CONSUMED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_BYTES_PRODUCED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_PREPROCESSING_INTERNAL_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_PREPROCESSING_LEADER_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_PRODUCER_CALLBACK_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_PRODUCER_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_RECORDS_CONSUMED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_RECORDS_PRODUCED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_SUBSCRIBE_PREP_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelStats.IngestionOtelMetricEntity.INGESTION_TIME_BETWEEN_COMPONENTS;
import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DCR_EVENT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_INGESTION_DESTINATION_COMPONENT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_INGESTION_SOURCE_COMPONENT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REPLICA_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateHistogramPointData;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateObservableCounterValue;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.ReplicaType;
import com.linkedin.venice.stats.dimensions.VeniceDCREvent;
import com.linkedin.venice.stats.dimensions.VeniceIngestionDestinationComponent;
import com.linkedin.venice.stats.dimensions.VeniceIngestionSourceComponent;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class IngestionOtelStatsTest {
  private static final String STORE_NAME = "test_store";
  private static final String CLUSTER_NAME = "test_cluster";
  private static final int BACKUP_VERSION = 1;
  private static final int CURRENT_VERSION = 2;
  private static final int FUTURE_VERSION = 3;
  private static final String TEST_PREFIX = "test_prefix";

  private InMemoryMetricReader inMemoryMetricReader;
  private IngestionOtelStats ingestionOtelStats;

  @BeforeMethod
  public void setUp() {
    inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricEntities(SERVER_METRIC_ENTITIES)
            .setMetricPrefix(TEST_PREFIX)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());
    ingestionOtelStats = new IngestionOtelStats(metricsRepository, STORE_NAME, CLUSTER_NAME);
  }

  @Test
  public void testConstructorWithOtelEnabled() {
    assertTrue(ingestionOtelStats.emitOtelMetrics(), "OTel metrics should be enabled");
  }

  @Test
  public void testConstructorWithOtelDisabled() {
    VeniceMetricsRepository disabledMetricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(false)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());

    IngestionOtelStats stats = new IngestionOtelStats(disabledMetricsRepository, STORE_NAME, CLUSTER_NAME);
    assertFalse(stats.emitOtelMetrics(), "OTel metrics should be disabled");
  }

  @Test
  public void testConstructorWithNonVeniceMetricsRepository() {
    MetricsRepository regularRepository = new MetricsRepository();
    IngestionOtelStats stats = new IngestionOtelStats(regularRepository, STORE_NAME, CLUSTER_NAME);
    assertFalse(stats.emitOtelMetrics(), "OTel metrics should be disabled for non-Venice repository");
  }

  @Test
  public void testUpdateVersionInfo() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    assertEquals(ingestionOtelStats.getVersionInfo().getCurrentVersion(), CURRENT_VERSION);
    assertEquals(ingestionOtelStats.getVersionInfo().getFutureVersion(), FUTURE_VERSION);
  }

  @DataProvider(name = "versionRoleProvider")
  public Object[][] versionRoleProvider() {
    return new Object[][] { { VersionRole.CURRENT, CURRENT_VERSION }, { VersionRole.FUTURE, FUTURE_VERSION },
        { VersionRole.BACKUP, BACKUP_VERSION } };
  }

  @Test(dataProvider = "versionRoleProvider")
  public void testClassifyVersion(VersionRole expectedRole, int version) {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    assertSame(IngestionOtelStats.classifyVersion(version, ingestionOtelStats.getVersionInfo()), expectedRole);
  }

  @Test
  public void testClassifyVersionWithNonExistingVersion() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    assertSame(
        IngestionOtelStats.classifyVersion(NON_EXISTING_VERSION, ingestionOtelStats.getVersionInfo()),
        VersionRole.BACKUP);
  }

  // Counter metrics with ReplicaType

  @Test
  public void testRecordRecordsConsumed() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.recordRecordsConsumed(CURRENT_VERSION, ReplicaType.LEADER, 10);
    validateObservableCounterValue(
        inMemoryMetricReader,
        10,
        buildAttributesWithVersionRoleAndReplicaType(VersionRole.CURRENT, ReplicaType.LEADER),
        INGESTION_RECORDS_CONSUMED.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @Test
  public void testRecordBytesConsumed() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.recordBytesConsumed(CURRENT_VERSION, ReplicaType.FOLLOWER, 1024);
    validateObservableCounterValue(
        inMemoryMetricReader,
        1024,
        buildAttributesWithVersionRoleAndReplicaType(VersionRole.CURRENT, ReplicaType.FOLLOWER),
        INGESTION_BYTES_CONSUMED.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @Test
  public void testRecordRecordsProduced() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.recordRecordsProduced(FUTURE_VERSION, ReplicaType.LEADER, 5);
    validateObservableCounterValue(
        inMemoryMetricReader,
        5,
        buildAttributesWithVersionRoleAndReplicaType(VersionRole.FUTURE, ReplicaType.LEADER),
        INGESTION_RECORDS_PRODUCED.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @Test
  public void testRecordBytesProduced() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.recordBytesProduced(BACKUP_VERSION, ReplicaType.LEADER, 2048);
    validateObservableCounterValue(
        inMemoryMetricReader,
        2048,
        buildAttributesWithVersionRoleAndReplicaType(VersionRole.BACKUP, ReplicaType.LEADER),
        INGESTION_BYTES_PRODUCED.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  // Histogram metrics

  @Test
  public void testRecordSubscribePrepTime() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.recordSubscribePrepTime(CURRENT_VERSION, 100.0);
    validateHistogramPointData(
        inMemoryMetricReader,
        100.0,
        100.0,
        1,
        100.0,
        buildAttributesWithVersionRole(VersionRole.CURRENT),
        INGESTION_SUBSCRIBE_PREP_TIME.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @Test
  public void testRecordIngestionTime() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.recordIngestionTime(CURRENT_VERSION, 50.0);
    validateHistogramPointData(
        inMemoryMetricReader,
        50.0,
        50.0,
        1,
        50.0,
        buildAttributesWithVersionRole(VersionRole.CURRENT),
        INGESTION_TIME.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @Test
  public void testRecordProducerCallbackTime() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.recordProducerCallbackTime(CURRENT_VERSION, ReplicaType.LEADER, 25.0);
    validateHistogramPointData(
        inMemoryMetricReader,
        25.0,
        25.0,
        1,
        25.0,
        buildAttributesWithVersionRoleAndReplicaType(VersionRole.CURRENT, ReplicaType.LEADER),
        INGESTION_PRODUCER_CALLBACK_TIME.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @Test
  public void testRecordPreprocessingLeaderTime() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.recordPreprocessingLeaderTime(CURRENT_VERSION, 30.0);
    validateHistogramPointData(
        inMemoryMetricReader,
        30.0,
        30.0,
        1,
        30.0,
        buildAttributesWithVersionRole(VersionRole.CURRENT),
        INGESTION_PREPROCESSING_LEADER_TIME.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @Test
  public void testRecordPreprocessingInternalTime() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.recordPreprocessingInternalTime(CURRENT_VERSION, 20.0);
    validateHistogramPointData(
        inMemoryMetricReader,
        20.0,
        20.0,
        1,
        20.0,
        buildAttributesWithVersionRole(VersionRole.CURRENT),
        INGESTION_PREPROCESSING_INTERNAL_TIME.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @Test
  public void testRecordProducerTime() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.recordProducerTime(CURRENT_VERSION, 40.0);
    validateHistogramPointData(
        inMemoryMetricReader,
        40.0,
        40.0,
        1,
        40.0,
        buildAttributesWithVersionRole(VersionRole.CURRENT),
        INGESTION_PRODUCER_TIME.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  // DCR metrics

  @Test
  public void testRecordDcrEventCount() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.recordDcrEventCount(CURRENT_VERSION, VeniceDCREvent.UPDATE_IGNORED, 1);
    validateObservableCounterValue(
        inMemoryMetricReader,
        1,
        buildAttributesWithVersionRoleAndDcrEvent(VersionRole.CURRENT, VeniceDCREvent.UPDATE_IGNORED),
        DCR_EVENT_COUNT.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @DataProvider(name = "dcrEventProvider")
  public Object[][] dcrEventProvider() {
    return new Object[][] { { VeniceDCREvent.UPDATE_IGNORED }, { VeniceDCREvent.TOMBSTONE_CREATION },
        { VeniceDCREvent.TIMESTAMP_REGRESSION_ERROR }, { VeniceDCREvent.OFFSET_REGRESSION_ERROR } };
  }

  @Test(dataProvider = "dcrEventProvider")
  public void testAllDcrEvents(VeniceDCREvent event) {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.recordDcrEventCount(CURRENT_VERSION, event, 1);
    validateObservableCounterValue(
        inMemoryMetricReader,
        1,
        buildAttributesWithVersionRoleAndDcrEvent(VersionRole.CURRENT, event),
        DCR_EVENT_COUNT.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @Test
  public void testRecordDcrTotalCount() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.recordDcrTotalCount(CURRENT_VERSION, 1);
    validateObservableCounterValue(
        inMemoryMetricReader,
        1,
        buildAttributesWithVersionRole(VersionRole.CURRENT),
        DCR_TOTAL_COUNT.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @Test
  public void testRecordDuplicateKeyUpdateCount() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.recordDuplicateKeyUpdateCount(CURRENT_VERSION, 1);
    validateObservableCounterValue(
        inMemoryMetricReader,
        1,
        buildAttributesWithVersionRole(VersionRole.CURRENT),
        DUPLICATE_KEY_UPDATE_COUNT.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  // Batch processing metrics

  @Test
  public void testRecordBatchProcessingRequestCount() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.recordBatchProcessingRequestCount(CURRENT_VERSION, 1);
    validateObservableCounterValue(
        inMemoryMetricReader,
        1,
        buildAttributesWithVersionRole(VersionRole.CURRENT),
        BATCH_PROCESSING_REQUEST_COUNT.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @Test
  public void testRecordBatchProcessingRequestRecordCount() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.recordBatchProcessingRequestRecordCount(CURRENT_VERSION, 100);
    validateObservableCounterValue(
        inMemoryMetricReader,
        100,
        buildAttributesWithVersionRole(VersionRole.CURRENT),
        BATCH_PROCESSING_REQUEST_RECORD_COUNT.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @Test
  public void testRecordBatchProcessingRequestErrorCount() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.recordBatchProcessingRequestErrorCount(CURRENT_VERSION, 1);
    validateObservableCounterValue(
        inMemoryMetricReader,
        1,
        buildAttributesWithVersionRole(VersionRole.CURRENT),
        BATCH_PROCESSING_REQUEST_ERROR_COUNT.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @Test
  public void testRecordBatchProcessingRequestTime() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.recordBatchProcessingRequestTime(CURRENT_VERSION, 150.0);
    validateHistogramPointData(
        inMemoryMetricReader,
        150.0,
        150.0,
        1,
        150.0,
        buildAttributesWithVersionRole(VersionRole.CURRENT),
        BATCH_PROCESSING_REQUEST_TIME.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  // Time between components

  @Test
  public void testRecordTimeBetweenComponents() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.recordTimeBetweenComponents(
        CURRENT_VERSION,
        VeniceIngestionSourceComponent.PRODUCER,
        VeniceIngestionDestinationComponent.SOURCE_BROKER,
        50.0);
    validateHistogramPointData(
        inMemoryMetricReader,
        50.0,
        50.0,
        1,
        50.0,
        buildAttributesWithComponents(
            VersionRole.CURRENT,
            VeniceIngestionSourceComponent.PRODUCER,
            VeniceIngestionDestinationComponent.SOURCE_BROKER),
        INGESTION_TIME_BETWEEN_COMPONENTS.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @Test
  public void testRecordTimeBetweenComponentsMultipleCombinations() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.recordTimeBetweenComponents(
        CURRENT_VERSION,
        VeniceIngestionSourceComponent.PRODUCER,
        VeniceIngestionDestinationComponent.SOURCE_BROKER,
        50.0);
    ingestionOtelStats.recordTimeBetweenComponents(
        CURRENT_VERSION,
        VeniceIngestionSourceComponent.SOURCE_BROKER,
        VeniceIngestionDestinationComponent.LEADER_CONSUMER,
        30.0);

    validateHistogramPointData(
        inMemoryMetricReader,
        50.0,
        50.0,
        1,
        50.0,
        buildAttributesWithComponents(
            VersionRole.CURRENT,
            VeniceIngestionSourceComponent.PRODUCER,
            VeniceIngestionDestinationComponent.SOURCE_BROKER),
        INGESTION_TIME_BETWEEN_COMPONENTS.getMetricEntity().getMetricName(),
        TEST_PREFIX);
    validateHistogramPointData(
        inMemoryMetricReader,
        30.0,
        30.0,
        1,
        30.0,
        buildAttributesWithComponents(
            VersionRole.CURRENT,
            VeniceIngestionSourceComponent.SOURCE_BROKER,
            VeniceIngestionDestinationComponent.LEADER_CONSUMER),
        INGESTION_TIME_BETWEEN_COMPONENTS.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  // OTel disabled

  @Test
  public void testNoMetricsRecordedWhenOtelDisabled() {
    InMemoryMetricReader disabledMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository disabledMetricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(false)
            .setOtelAdditionalMetricsReader(disabledMetricReader)
            .build());
    IngestionOtelStats stats = new IngestionOtelStats(disabledMetricsRepository, STORE_NAME, CLUSTER_NAME);
    stats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    stats.recordRecordsConsumed(CURRENT_VERSION, ReplicaType.LEADER, 10);
    stats.recordIngestionTime(CURRENT_VERSION, 50.0);
    assertEquals(disabledMetricReader.collectAllMetrics().size(), 0, "No metrics when OTel disabled");
  }

  // ASYNC_GAUGE state management

  @Test
  public void testSetAndRemoveIngestionTask() {
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.setIngestionTask(CURRENT_VERSION, mockTask);
    ingestionOtelStats.removeIngestionTask(CURRENT_VERSION);
    ingestionOtelStats.setIngestionTask(CURRENT_VERSION, null); // null ignored, no exception
  }

  @Test
  public void testSetIngestionTaskForMultipleVersions() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.setIngestionTask(CURRENT_VERSION, mock(StoreIngestionTask.class));
    ingestionOtelStats.setIngestionTask(FUTURE_VERSION, mock(StoreIngestionTask.class));
    ingestionOtelStats.setIngestionTask(BACKUP_VERSION, mock(StoreIngestionTask.class));
    ingestionOtelStats.removeIngestionTask(BACKUP_VERSION);
    ingestionOtelStats.removeIngestionTask(CURRENT_VERSION);
    ingestionOtelStats.removeIngestionTask(FUTURE_VERSION);
  }

  @Test
  public void testPushTimeoutGaugeStateManagement() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.setIngestionTaskPushTimeoutGauge(CURRENT_VERSION, 1);
    ingestionOtelStats.setIngestionTaskPushTimeoutGauge(CURRENT_VERSION, 0);
    ingestionOtelStats.setIngestionTaskPushTimeoutGauge(FUTURE_VERSION, 1);
    ingestionOtelStats.removeIngestionTask(FUTURE_VERSION);
  }

  @Test
  public void testIdleTimeStateManagement() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.recordIdleTime(CURRENT_VERSION, 5000);
    ingestionOtelStats.recordIdleTime(CURRENT_VERSION, 10000);
    ingestionOtelStats.recordIdleTime(FUTURE_VERSION, 3000);
    ingestionOtelStats.removeIngestionTask(CURRENT_VERSION);
  }

  @Test
  public void testRemoveIngestionTaskCleansUpAllState() {
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.setIngestionTask(CURRENT_VERSION, mockTask);
    ingestionOtelStats.setIngestionTaskPushTimeoutGauge(CURRENT_VERSION, 1);
    ingestionOtelStats.recordIdleTime(CURRENT_VERSION, 5000);
    ingestionOtelStats.removeIngestionTask(CURRENT_VERSION);
    // Can re-add state after removal
    ingestionOtelStats.setIngestionTask(CURRENT_VERSION, mockTask);
    ingestionOtelStats.setIngestionTaskPushTimeoutGauge(CURRENT_VERSION, 0);
    ingestionOtelStats.recordIdleTime(CURRENT_VERSION, 0);
  }

  @Test
  public void testVersionRoleClassificationForAsyncGauges() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    assertEquals(
        IngestionOtelStats.classifyVersion(CURRENT_VERSION, ingestionOtelStats.getVersionInfo()),
        VersionRole.CURRENT);
    assertEquals(
        IngestionOtelStats.classifyVersion(FUTURE_VERSION, ingestionOtelStats.getVersionInfo()),
        VersionRole.FUTURE);
    assertEquals(
        IngestionOtelStats.classifyVersion(BACKUP_VERSION, ingestionOtelStats.getVersionInfo()),
        VersionRole.BACKUP);
    assertEquals(
        IngestionOtelStats.classifyVersion(NON_EXISTING_VERSION, ingestionOtelStats.getVersionInfo()),
        VersionRole.BACKUP);
  }

  @Test
  public void testMultipleIdleTimeUpdates() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    for (int i = 1; i <= 10; i++) {
      ingestionOtelStats.recordIdleTime(CURRENT_VERSION, i * 1000L);
    }
    ingestionOtelStats.recordIdleTime(CURRENT_VERSION, 15000);
    ingestionOtelStats.recordIdleTime(FUTURE_VERSION, 8000);
    ingestionOtelStats.recordIdleTime(BACKUP_VERSION, 12000);
  }

  @Test
  public void testPushTimeoutGaugeToggle() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    for (int i = 0; i < 5; i++) {
      ingestionOtelStats.setIngestionTaskPushTimeoutGauge(CURRENT_VERSION, 1);
      ingestionOtelStats.setIngestionTaskPushTimeoutGauge(CURRENT_VERSION, 0);
    }
  }

  @Test
  public void testCloseMethod() throws Exception {
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.setIngestionTask(CURRENT_VERSION, mockTask);
    ingestionOtelStats.setIngestionTask(FUTURE_VERSION, mockTask);
    ingestionOtelStats.setIngestionTaskPushTimeoutGauge(CURRENT_VERSION, 1);
    ingestionOtelStats.recordIdleTime(CURRENT_VERSION, 5000);

    assertTrue(getIngestionTasksByVersion(ingestionOtelStats).containsKey(CURRENT_VERSION));
    assertTrue(getPushTimeoutByVersion(ingestionOtelStats).containsKey(CURRENT_VERSION));
    assertTrue(getIdleTimeByVersion(ingestionOtelStats).containsKey(CURRENT_VERSION));

    ingestionOtelStats.close();

    assertTrue(getIngestionTasksByVersion(ingestionOtelStats).isEmpty(), "ingestionTasksByVersion should be cleared");
    assertTrue(getPushTimeoutByVersion(ingestionOtelStats).isEmpty(), "pushTimeoutByVersion should be cleared");
    assertTrue(getIdleTimeByVersion(ingestionOtelStats).isEmpty(), "idleTimeByVersion should be cleared");

    // After close, can set new state
    ingestionOtelStats.setIngestionTask(CURRENT_VERSION, mockTask);
    ingestionOtelStats.setIngestionTaskPushTimeoutGauge(CURRENT_VERSION, 0);
    ingestionOtelStats.recordIdleTime(CURRENT_VERSION, 0);
  }

  @Test
  public void testDeterministicBackupVersionSelection() throws Exception {
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    int currentVersion = 5;
    int futureVersion = 6;
    int[] backupVersions = { 4, 1, 3 };

    ingestionOtelStats.updateVersionInfo(currentVersion, futureVersion);
    ingestionOtelStats.setIngestionTask(currentVersion, mockTask);
    ingestionOtelStats.setIngestionTask(futureVersion, mockTask);
    for (int version: backupVersions) {
      ingestionOtelStats.setIngestionTask(version, mockTask);
    }

    Method method = IngestionOtelStats.class.getDeclaredMethod("getVersionForRole", VersionRole.class);
    method.setAccessible(true);

    assertEquals((int) method.invoke(ingestionOtelStats, VersionRole.BACKUP), 1, "Should return smallest backup (1)");

    ingestionOtelStats.removeIngestionTask(1);
    assertEquals((int) method.invoke(ingestionOtelStats, VersionRole.BACKUP), 3, "After removing 1, should return 3");

    ingestionOtelStats.removeIngestionTask(3);
    assertEquals((int) method.invoke(ingestionOtelStats, VersionRole.BACKUP), 4, "After removing 3, should return 4");

    ingestionOtelStats.removeIngestionTask(4);
    assertEquals(
        (int) method.invoke(ingestionOtelStats, VersionRole.BACKUP),
        NON_EXISTING_VERSION,
        "No backups -> NON_EXISTING_VERSION");
  }

  @Test
  public void testGetPushTimeoutCountForRoleCallback() throws Exception {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    Method method = IngestionOtelStats.class.getDeclaredMethod("getPushTimeoutCountForRole", VersionRole.class);
    method.setAccessible(true);

    assertEquals((long) method.invoke(ingestionOtelStats, VersionRole.CURRENT), 0L);
    assertEquals((long) method.invoke(ingestionOtelStats, VersionRole.FUTURE), 0L);

    ingestionOtelStats.setIngestionTaskPushTimeoutGauge(CURRENT_VERSION, 1);
    assertEquals((long) method.invoke(ingestionOtelStats, VersionRole.CURRENT), 1L);

    ingestionOtelStats.setIngestionTaskPushTimeoutGauge(FUTURE_VERSION, 1);
    assertEquals((long) method.invoke(ingestionOtelStats, VersionRole.FUTURE), 1L);

    ingestionOtelStats.setIngestionTaskPushTimeoutGauge(CURRENT_VERSION, 0);
    assertEquals((long) method.invoke(ingestionOtelStats, VersionRole.CURRENT), 0L);

    assertEquals((long) method.invoke(ingestionOtelStats, VersionRole.BACKUP), 0L, "No backup -> 0");
  }

  @Test
  public void testGetIdleTimeForRoleCallback() throws Exception {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    Method method = IngestionOtelStats.class.getDeclaredMethod("getIdleTimeForRole", VersionRole.class);
    method.setAccessible(true);

    assertEquals((long) method.invoke(ingestionOtelStats, VersionRole.CURRENT), 0L);

    ingestionOtelStats.recordIdleTime(CURRENT_VERSION, 5000L);
    assertEquals((long) method.invoke(ingestionOtelStats, VersionRole.CURRENT), 5000L);

    ingestionOtelStats.recordIdleTime(CURRENT_VERSION, 10000L);
    assertEquals((long) method.invoke(ingestionOtelStats, VersionRole.CURRENT), 10000L);

    ingestionOtelStats.recordIdleTime(FUTURE_VERSION, 3000L);
    assertEquals((long) method.invoke(ingestionOtelStats, VersionRole.FUTURE), 3000L);

    assertEquals((long) method.invoke(ingestionOtelStats, VersionRole.BACKUP), 0L, "No backup -> 0");
  }

  @Test
  public void testGetIdleTimeForBackupRole() throws Exception {
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);
    int currentVersion = 5;
    int futureVersion = 6;
    int backupVersion = 1;

    ingestionOtelStats.updateVersionInfo(currentVersion, futureVersion);
    ingestionOtelStats.setIngestionTask(backupVersion, mockTask);

    Method method = IngestionOtelStats.class.getDeclaredMethod("getIdleTimeForRole", VersionRole.class);
    method.setAccessible(true);

    assertEquals((long) method.invoke(ingestionOtelStats, VersionRole.BACKUP), 0L);

    ingestionOtelStats.recordIdleTime(backupVersion, 7000L);
    assertEquals((long) method.invoke(ingestionOtelStats, VersionRole.BACKUP), 7000L);
  }

  // Attribute builders

  private Attributes buildAttributesWithVersionRole(VersionRole versionRole) {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), versionRole.getDimensionValue())
        .build();
  }

  private Attributes buildAttributesWithVersionRoleAndReplicaType(VersionRole versionRole, ReplicaType replicaType) {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), versionRole.getDimensionValue())
        .put(VENICE_REPLICA_TYPE.getDimensionNameInDefaultFormat(), replicaType.getDimensionValue())
        .build();
  }

  private Attributes buildAttributesWithVersionRoleAndDcrEvent(VersionRole versionRole, VeniceDCREvent event) {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), versionRole.getDimensionValue())
        .put(VENICE_DCR_EVENT.getDimensionNameInDefaultFormat(), event.getDimensionValue())
        .build();
  }

  private Attributes buildAttributesWithComponents(
      VersionRole versionRole,
      VeniceIngestionSourceComponent sourceComponent,
      VeniceIngestionDestinationComponent destComponent) {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), versionRole.getDimensionValue())
        .put(VENICE_INGESTION_SOURCE_COMPONENT.getDimensionNameInDefaultFormat(), sourceComponent.getDimensionValue())
        .put(
            VENICE_INGESTION_DESTINATION_COMPONENT.getDimensionNameInDefaultFormat(),
            destComponent.getDimensionValue())
        .build();
  }

  // Reflection helpers

  @SuppressWarnings("unchecked")
  private Map<Integer, StoreIngestionTask> getIngestionTasksByVersion(IngestionOtelStats stats) throws Exception {
    Field field = IngestionOtelStats.class.getDeclaredField("ingestionTasksByVersion");
    field.setAccessible(true);
    return (Map<Integer, StoreIngestionTask>) field.get(stats);
  }

  @SuppressWarnings("unchecked")
  private Map<Integer, Integer> getPushTimeoutByVersion(IngestionOtelStats stats) throws Exception {
    Field field = IngestionOtelStats.class.getDeclaredField("pushTimeoutByVersion");
    field.setAccessible(true);
    return (Map<Integer, Integer>) field.get(stats);
  }

  @SuppressWarnings("unchecked")
  private Map<Integer, ?> getIdleTimeByVersion(IngestionOtelStats stats) throws Exception {
    Field field = IngestionOtelStats.class.getDeclaredField("idleTimeByVersion");
    field.setAccessible(true);
    return (Map<Integer, ?>) field.get(stats);
  }

}
