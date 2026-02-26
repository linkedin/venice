package com.linkedin.davinci.stats.ingestion;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.BATCH_PROCESSING_REQUEST_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.BATCH_PROCESSING_REQUEST_ERROR_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.BATCH_PROCESSING_REQUEST_RECORD_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.BATCH_PROCESSING_REQUEST_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.CONSUMER_IDLE_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.DCR_EVENT_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.DCR_TOTAL_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.DISK_QUOTA_USED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.DUPLICATE_KEY_UPDATE_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_BYTES_CONSUMED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_BYTES_PRODUCED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_PREPROCESSING_INTERNAL_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_PREPROCESSING_LEADER_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_PRODUCER_CALLBACK_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_PRODUCER_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_RECORDS_CONSUMED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_RECORDS_PRODUCED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_SUBSCRIBE_PREP_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_TASK_ERROR_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_TASK_PUSH_TIMEOUT_COUNT;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_TIME;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.INGESTION_TIME_BETWEEN_COMPONENTS;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.RT_BYTES_CONSUMED;
import static com.linkedin.davinci.stats.ingestion.IngestionOtelMetricEntity.RT_RECORDS_CONSUMED;
import static com.linkedin.venice.meta.Store.NON_EXISTING_VERSION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DCR_EVENT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DESTINATION_REGION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_INGESTION_DESTINATION_COMPONENT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_INGESTION_SOURCE_COMPONENT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REGION_LOCALITY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REPLICA_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_SOURCE_REGION;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateHistogramPointData;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateLongPointDataFromCounter;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateObservableCounterValue;
import static com.linkedin.venice.utils.Utils.setOf;
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
import com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions;
import com.linkedin.venice.stats.dimensions.VeniceRegionLocality;
import com.linkedin.venice.stats.metrics.MetricEntity;
import com.linkedin.venice.stats.metrics.MetricType;
import com.linkedin.venice.stats.metrics.MetricUnit;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
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
  private static final String LOCAL_REGION = "dc-1";
  private static final String REMOTE_REGION = "dc-2";

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
    ingestionOtelStats = new IngestionOtelStats(metricsRepository, STORE_NAME, CLUSTER_NAME, LOCAL_REGION);
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

    IngestionOtelStats stats =
        new IngestionOtelStats(disabledMetricsRepository, STORE_NAME, CLUSTER_NAME, LOCAL_REGION);
    assertFalse(stats.emitOtelMetrics(), "OTel metrics should be disabled");
  }

  @Test
  public void testConstructorWithNonVeniceMetricsRepository() {
    MetricsRepository regularRepository = new MetricsRepository();
    IngestionOtelStats stats = new IngestionOtelStats(regularRepository, STORE_NAME, CLUSTER_NAME, LOCAL_REGION);
    assertFalse(stats.emitOtelMetrics(), "OTel metrics should be disabled for non-Venice repository");

    // RT recording methods should not throw when baseDimensionsMap is null
    stats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    stats.recordRtRecordsConsumed(CURRENT_VERSION, REMOTE_REGION, VeniceRegionLocality.REMOTE, 5);
    stats.recordRtBytesConsumed(CURRENT_VERSION, REMOTE_REGION, VeniceRegionLocality.REMOTE, 1024);
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
    IngestionOtelStats stats =
        new IngestionOtelStats(disabledMetricsRepository, STORE_NAME, CLUSTER_NAME, LOCAL_REGION);
    stats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    stats.recordRecordsConsumed(CURRENT_VERSION, ReplicaType.LEADER, 10);
    stats.recordIngestionTime(CURRENT_VERSION, 50.0);
    stats.recordRtRecordsConsumed(CURRENT_VERSION, REMOTE_REGION, VeniceRegionLocality.REMOTE, 5);
    stats.recordRtBytesConsumed(CURRENT_VERSION, REMOTE_REGION, VeniceRegionLocality.REMOTE, 1024);
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
    ingestionOtelStats.recordRtRecordsConsumed(CURRENT_VERSION, REMOTE_REGION, VeniceRegionLocality.REMOTE, 5);
    ingestionOtelStats.recordRtBytesConsumed(CURRENT_VERSION, REMOTE_REGION, VeniceRegionLocality.REMOTE, 512);

    assertTrue(getIngestionTasksByVersion(ingestionOtelStats).containsKey(CURRENT_VERSION));
    assertTrue(getPushTimeoutByVersion(ingestionOtelStats).containsKey(CURRENT_VERSION));
    assertTrue(getIdleTimeByVersion(ingestionOtelStats).containsKey(CURRENT_VERSION));
    assertFalse(getRtRecordsConsumedByRegion(ingestionOtelStats).isEmpty());
    assertFalse(getRtBytesConsumedByRegion(ingestionOtelStats).isEmpty());

    ingestionOtelStats.close();

    assertTrue(getIngestionTasksByVersion(ingestionOtelStats).isEmpty(), "ingestionTasksByVersion should be cleared");
    assertTrue(getPushTimeoutByVersion(ingestionOtelStats).isEmpty(), "pushTimeoutByVersion should be cleared");
    assertTrue(getIdleTimeByVersion(ingestionOtelStats).isEmpty(), "idleTimeByVersion should be cleared");
    assertTrue(
        getRtRecordsConsumedByRegion(ingestionOtelStats).isEmpty(),
        "rtRecordsConsumedByRegion should be cleared");
    assertTrue(getRtBytesConsumedByRegion(ingestionOtelStats).isEmpty(), "rtBytesConsumedByRegion should be cleared");

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

  // RT region metrics

  @Test
  public void testRecordRtRecordsConsumed() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.recordRtRecordsConsumed(CURRENT_VERSION, REMOTE_REGION, VeniceRegionLocality.REMOTE, 10);
    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        10,
        buildAttributesWithRegion(VersionRole.CURRENT, REMOTE_REGION, LOCAL_REGION, VeniceRegionLocality.REMOTE),
        RT_RECORDS_CONSUMED.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @Test
  public void testRecordRtBytesConsumed() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.recordRtBytesConsumed(CURRENT_VERSION, REMOTE_REGION, VeniceRegionLocality.REMOTE, 2048);
    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        2048,
        buildAttributesWithRegion(VersionRole.CURRENT, REMOTE_REGION, LOCAL_REGION, VeniceRegionLocality.REMOTE),
        RT_BYTES_CONSUMED.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @Test
  public void testRtMetricsMultipleRegionCombinations() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    // Record from local region
    ingestionOtelStats.recordRtRecordsConsumed(CURRENT_VERSION, LOCAL_REGION, VeniceRegionLocality.LOCAL, 5);
    ingestionOtelStats.recordRtBytesConsumed(CURRENT_VERSION, LOCAL_REGION, VeniceRegionLocality.LOCAL, 512);
    // Record from remote region
    ingestionOtelStats.recordRtRecordsConsumed(CURRENT_VERSION, REMOTE_REGION, VeniceRegionLocality.REMOTE, 3);
    ingestionOtelStats.recordRtBytesConsumed(CURRENT_VERSION, REMOTE_REGION, VeniceRegionLocality.REMOTE, 1024);

    // Validate local region data points
    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        5,
        buildAttributesWithRegion(VersionRole.CURRENT, LOCAL_REGION, LOCAL_REGION, VeniceRegionLocality.LOCAL),
        RT_RECORDS_CONSUMED.getMetricEntity().getMetricName(),
        TEST_PREFIX);
    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        512,
        buildAttributesWithRegion(VersionRole.CURRENT, LOCAL_REGION, LOCAL_REGION, VeniceRegionLocality.LOCAL),
        RT_BYTES_CONSUMED.getMetricEntity().getMetricName(),
        TEST_PREFIX);

    // Validate remote region data points
    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        3,
        buildAttributesWithRegion(VersionRole.CURRENT, REMOTE_REGION, LOCAL_REGION, VeniceRegionLocality.REMOTE),
        RT_RECORDS_CONSUMED.getMetricEntity().getMetricName(),
        TEST_PREFIX);
    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1024,
        buildAttributesWithRegion(VersionRole.CURRENT, REMOTE_REGION, LOCAL_REGION, VeniceRegionLocality.REMOTE),
        RT_BYTES_CONSUMED.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @Test
  public void testRtMetricsNoNpeWhenOtelDisabled() {
    NoOpIngestionOtelStats noOpStats = NoOpIngestionOtelStats.INSTANCE;
    // Should not throw NPE
    noOpStats.recordRtRecordsConsumed(CURRENT_VERSION, REMOTE_REGION, VeniceRegionLocality.REMOTE, 10);
    noOpStats.recordRtBytesConsumed(CURRENT_VERSION, REMOTE_REGION, VeniceRegionLocality.REMOTE, 1024);
  }

  @Test
  public void testRtMetricsAccumulation() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    // Record from the same region multiple times — counter should accumulate
    ingestionOtelStats.recordRtRecordsConsumed(CURRENT_VERSION, REMOTE_REGION, VeniceRegionLocality.REMOTE, 3);
    ingestionOtelStats.recordRtRecordsConsumed(CURRENT_VERSION, REMOTE_REGION, VeniceRegionLocality.REMOTE, 7);
    ingestionOtelStats.recordRtBytesConsumed(CURRENT_VERSION, REMOTE_REGION, VeniceRegionLocality.REMOTE, 100);
    ingestionOtelStats.recordRtBytesConsumed(CURRENT_VERSION, REMOTE_REGION, VeniceRegionLocality.REMOTE, 200);

    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        10,
        buildAttributesWithRegion(VersionRole.CURRENT, REMOTE_REGION, LOCAL_REGION, VeniceRegionLocality.REMOTE),
        RT_RECORDS_CONSUMED.getMetricEntity().getMetricName(),
        TEST_PREFIX);
    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        300,
        buildAttributesWithRegion(VersionRole.CURRENT, REMOTE_REGION, LOCAL_REGION, VeniceRegionLocality.REMOTE),
        RT_BYTES_CONSUMED.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @Test
  public void testRtMetricsFutureVersionRole() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.recordRtRecordsConsumed(FUTURE_VERSION, REMOTE_REGION, VeniceRegionLocality.REMOTE, 5);
    ingestionOtelStats.recordRtBytesConsumed(FUTURE_VERSION, REMOTE_REGION, VeniceRegionLocality.REMOTE, 1024);

    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        5,
        buildAttributesWithRegion(VersionRole.FUTURE, REMOTE_REGION, LOCAL_REGION, VeniceRegionLocality.REMOTE),
        RT_RECORDS_CONSUMED.getMetricEntity().getMetricName(),
        TEST_PREFIX);
    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        1024,
        buildAttributesWithRegion(VersionRole.FUTURE, REMOTE_REGION, LOCAL_REGION, VeniceRegionLocality.REMOTE),
        RT_BYTES_CONSUMED.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  // Metric entity definition validation

  @Test
  public void testMetricEntityDefinitions() {
    Set<VeniceMetricsDimensions> storeClusterVersion =
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE);
    Set<VeniceMetricsDimensions> storeClusterVersionReplica =
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_REPLICA_TYPE);
    Set<VeniceMetricsDimensions> storeClusterVersionDcr =
        setOf(VENICE_STORE_NAME, VENICE_CLUSTER_NAME, VENICE_VERSION_ROLE, VENICE_DCR_EVENT);
    Set<VeniceMetricsDimensions> storeClusterVersionComponents = setOf(
        VENICE_STORE_NAME,
        VENICE_CLUSTER_NAME,
        VENICE_VERSION_ROLE,
        VENICE_INGESTION_SOURCE_COMPONENT,
        VENICE_INGESTION_DESTINATION_COMPONENT);
    Set<VeniceMetricsDimensions> storeClusterVersionRegion = setOf(
        VENICE_STORE_NAME,
        VENICE_CLUSTER_NAME,
        VENICE_VERSION_ROLE,
        VENICE_SOURCE_REGION,
        VENICE_DESTINATION_REGION,
        VENICE_REGION_LOCALITY);

    // ASYNC_GAUGE metrics
    assertMetricEntity(
        INGESTION_TASK_ERROR_COUNT.getMetricEntity(),
        "ingestion.task.error_count",
        MetricType.ASYNC_GAUGE,
        MetricUnit.NUMBER,
        "Count of ingestion tasks in error state",
        storeClusterVersion);
    assertMetricEntity(
        INGESTION_TASK_PUSH_TIMEOUT_COUNT.getMetricEntity(),
        "ingestion.task.push_timeout_count",
        MetricType.ASYNC_GAUGE,
        MetricUnit.NUMBER,
        "Count of ingestion tasks timed out during push operation",
        storeClusterVersion);
    assertMetricEntity(
        DISK_QUOTA_USED.getMetricEntity(),
        "ingestion.disk_quota.used",
        MetricType.ASYNC_GAUGE,
        MetricUnit.RATIO,
        "Disk quota used for the store version",
        storeClusterVersion);
    assertMetricEntity(
        CONSUMER_IDLE_TIME.getMetricEntity(),
        "ingestion.consumer.idle_time",
        MetricType.ASYNC_GAUGE,
        MetricUnit.MILLISECOND,
        "Time the ingestion consumer has been idle without polling records",
        storeClusterVersion);

    // ASYNC_COUNTER_FOR_HIGH_PERF_CASES metrics with VersionRole + ReplicaType
    assertMetricEntity(
        INGESTION_RECORDS_CONSUMED.getMetricEntity(),
        "ingestion.records.consumed",
        MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
        MetricUnit.NUMBER,
        "Records consumed from remote/local topic",
        storeClusterVersionReplica);
    assertMetricEntity(
        INGESTION_RECORDS_PRODUCED.getMetricEntity(),
        "ingestion.records.produced",
        MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
        MetricUnit.NUMBER,
        "Records produced to local topic",
        storeClusterVersionReplica);
    assertMetricEntity(
        INGESTION_BYTES_CONSUMED.getMetricEntity(),
        "ingestion.bytes.consumed",
        MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
        MetricUnit.BYTES,
        "Bytes consumed from remote/local topic that are successfully processed excluding control/DIV messages",
        storeClusterVersionReplica);
    assertMetricEntity(
        INGESTION_BYTES_PRODUCED.getMetricEntity(),
        "ingestion.bytes.produced",
        MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
        MetricUnit.BYTES,
        "Bytes produced to local topic",
        storeClusterVersionReplica);

    // MIN_MAX_COUNT_SUM_AGGREGATIONS metrics with VersionRole only
    assertMetricEntity(
        INGESTION_SUBSCRIBE_PREP_TIME.getMetricEntity(),
        "ingestion.subscribe.prep.time",
        MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
        MetricUnit.MILLISECOND,
        "Subscription preparation latency",
        storeClusterVersion);
    assertMetricEntity(
        INGESTION_TIME.getMetricEntity(),
        "ingestion.time",
        MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
        MetricUnit.MILLISECOND,
        "End-to-end processing time from topic poll to storage write",
        storeClusterVersion);
    assertMetricEntity(
        INGESTION_PREPROCESSING_LEADER_TIME.getMetricEntity(),
        "ingestion.preprocessing.leader.time",
        MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
        MetricUnit.MILLISECOND,
        "Leader-side preprocessing latency during ingestion",
        storeClusterVersion);
    assertMetricEntity(
        INGESTION_PREPROCESSING_INTERNAL_TIME.getMetricEntity(),
        "ingestion.preprocessing.internal.time",
        MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
        MetricUnit.MILLISECOND,
        "Internal preprocessing latency during ingestion",
        storeClusterVersion);
    assertMetricEntity(
        INGESTION_PRODUCER_TIME.getMetricEntity(),
        "ingestion.producer.time",
        MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
        MetricUnit.MILLISECOND,
        "Latency from leader producing to producer completion",
        storeClusterVersion);
    assertMetricEntity(
        BATCH_PROCESSING_REQUEST_TIME.getMetricEntity(),
        "ingestion.batch_processing.request.time",
        MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
        MetricUnit.MILLISECOND,
        "Batch processing latency",
        storeClusterVersion);

    // MIN_MAX_COUNT_SUM_AGGREGATIONS metric with VersionRole + ReplicaType
    assertMetricEntity(
        INGESTION_PRODUCER_CALLBACK_TIME.getMetricEntity(),
        "ingestion.producer.callback.time",
        MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
        MetricUnit.MILLISECOND,
        "Producer callback latency (ack wait time)",
        storeClusterVersionReplica);

    // MIN_MAX_COUNT_SUM_AGGREGATIONS metric with VersionRole + components
    assertMetricEntity(
        INGESTION_TIME_BETWEEN_COMPONENTS.getMetricEntity(),
        "ingestion.time_between_components",
        MetricType.MIN_MAX_COUNT_SUM_AGGREGATIONS,
        MetricUnit.MILLISECOND,
        "Ingestion latency between different components of the flow",
        storeClusterVersionComponents);

    // ASYNC_COUNTER_FOR_HIGH_PERF_CASES metrics with VersionRole only
    assertMetricEntity(
        BATCH_PROCESSING_REQUEST_COUNT.getMetricEntity(),
        "ingestion.batch_processing.request.count",
        MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
        MetricUnit.NUMBER,
        "Count of batch processing requests during ingestion",
        storeClusterVersion);
    assertMetricEntity(
        BATCH_PROCESSING_REQUEST_RECORD_COUNT.getMetricEntity(),
        "ingestion.batch_processing.request.record.count",
        MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
        MetricUnit.NUMBER,
        "Total records across batch-processing requests",
        storeClusterVersion);
    assertMetricEntity(
        BATCH_PROCESSING_REQUEST_ERROR_COUNT.getMetricEntity(),
        "ingestion.batch_processing.request.error_count",
        MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
        MetricUnit.NUMBER,
        "Count of failed batch processing requests during ingestion",
        storeClusterVersion);
    assertMetricEntity(
        DCR_EVENT_COUNT.getMetricEntity(),
        "ingestion.dcr.event_count",
        MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
        MetricUnit.NUMBER,
        "Count of DCR outcomes per event type (e.g., PUT, DELETE, UPDATE)",
        storeClusterVersionDcr);
    assertMetricEntity(
        DCR_TOTAL_COUNT.getMetricEntity(),
        "ingestion.dcr.total_count",
        MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
        MetricUnit.NUMBER,
        "Deterministic Conflict Resolution (DCR) total count",
        storeClusterVersion);
    assertMetricEntity(
        DUPLICATE_KEY_UPDATE_COUNT.getMetricEntity(),
        "ingestion.key.update.duplicate_count",
        MetricType.ASYNC_COUNTER_FOR_HIGH_PERF_CASES,
        MetricUnit.NUMBER,
        "Count of duplicate-key updates during ingestion",
        storeClusterVersion);

    // COUNTER metrics with region dimensions
    assertMetricEntity(
        RT_RECORDS_CONSUMED.getMetricEntity(),
        "ingestion.records.consumed_from_real_time_topic",
        MetricType.COUNTER,
        MetricUnit.NUMBER,
        "Records consumed from local/remote region real-time topics",
        storeClusterVersionRegion);
    assertMetricEntity(
        RT_BYTES_CONSUMED.getMetricEntity(),
        "ingestion.bytes.consumed_from_real_time_topic",
        MetricType.COUNTER,
        MetricUnit.BYTES,
        "Bytes consumed from local/remote region real-time topics",
        storeClusterVersionRegion);

    // Verify total count
    assertEquals(IngestionOtelMetricEntity.values().length, 24, "Expected 24 metric entities");
  }

  private static void assertMetricEntity(
      MetricEntity entity,
      String expectedName,
      MetricType expectedType,
      MetricUnit expectedUnit,
      String expectedDescription,
      Set<VeniceMetricsDimensions> expectedDimensions) {
    assertEquals(entity.getMetricName(), expectedName, "metric name mismatch");
    assertEquals(entity.getMetricType(), expectedType, expectedName + " metric type mismatch");
    assertEquals(entity.getUnit(), expectedUnit, expectedName + " unit mismatch");
    assertEquals(entity.getDescription(), expectedDescription, expectedName + " description mismatch");
    assertEquals(entity.getDimensionsList(), expectedDimensions, expectedName + " dimensions mismatch");
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

  private Attributes buildAttributesWithRegion(
      VersionRole versionRole,
      String sourceRegion,
      String destRegion,
      VeniceRegionLocality regionLocality) {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_SOURCE_REGION.getDimensionNameInDefaultFormat(), sourceRegion)
        .put(VENICE_DESTINATION_REGION.getDimensionNameInDefaultFormat(), destRegion)
        .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), versionRole.getDimensionValue())
        .put(VENICE_REGION_LOCALITY.getDimensionNameInDefaultFormat(), regionLocality.getDimensionValue())
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

  @SuppressWarnings("unchecked")
  private Map<String, ?> getRtRecordsConsumedByRegion(IngestionOtelStats stats) throws Exception {
    Field field = IngestionOtelStats.class.getDeclaredField("rtRecordsConsumedByRegion");
    field.setAccessible(true);
    return (Map<String, ?>) field.get(stats);
  }

  @SuppressWarnings("unchecked")
  private Map<String, ?> getRtBytesConsumedByRegion(IngestionOtelStats stats) throws Exception {
    Field field = IngestionOtelStats.class.getDeclaredField("rtBytesConsumedByRegion");
    field.setAccessible(true);
    return (Map<String, ?>) field.get(stats);
  }

}
