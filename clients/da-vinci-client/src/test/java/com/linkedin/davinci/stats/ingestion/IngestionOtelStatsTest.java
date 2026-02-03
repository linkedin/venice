package com.linkedin.davinci.stats.ingestion;

import static com.linkedin.davinci.stats.ServerMetricEntity.BATCH_PROCESSING_REQUEST_COUNT;
import static com.linkedin.davinci.stats.ServerMetricEntity.BATCH_PROCESSING_REQUEST_ERROR_COUNT;
import static com.linkedin.davinci.stats.ServerMetricEntity.BATCH_PROCESSING_REQUEST_RECORD_COUNT;
import static com.linkedin.davinci.stats.ServerMetricEntity.BATCH_PROCESSING_REQUEST_TIME;
import static com.linkedin.davinci.stats.ServerMetricEntity.DCR_EVENT_COUNT;
import static com.linkedin.davinci.stats.ServerMetricEntity.DCR_TOTAL_COUNT;
import static com.linkedin.davinci.stats.ServerMetricEntity.DUPLICATE_KEY_UPDATE_COUNT;
import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_BYTES_CONSUMED;
import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_BYTES_PRODUCED;
import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_PREPROCESSING_INTERNAL_TIME;
import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_PREPROCESSING_LEADER_TIME;
import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_PRODUCER_CALLBACK_TIME;
import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_PRODUCER_TIME;
import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_RECORDS_CONSUMED;
import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_RECORDS_PRODUCED;
import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_SUBSCRIBE_PREP_TIME;
import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_TIME;
import static com.linkedin.davinci.stats.ServerMetricEntity.INGESTION_TIME_BETWEEN_COMPONENTS;
import static com.linkedin.davinci.stats.ServerMetricEntity.RT_BYTES_CONSUMED;
import static com.linkedin.davinci.stats.ServerMetricEntity.RT_RECORDS_CONSUMED;
import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
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
import com.linkedin.venice.stats.dimensions.VeniceDestinationIngestionComponent;
import com.linkedin.venice.stats.dimensions.VeniceIngestionSourceComponent;
import com.linkedin.venice.stats.dimensions.VeniceRegionLocality;
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
  private static final String REGION_US_WEST = "us-west";
  private static final String REGION_US_EAST = "us-east";
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

  // Tests for records/bytes consumed and produced with ReplicaType dimension

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

  // Tests for histogram metrics

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

  // Tests for DCR metrics

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

  // Tests for batch processing metrics

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

  // Tests for time between components

  @Test
  public void testRecordTimeBetweenComponents() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    ingestionOtelStats.recordTimeBetweenComponents(
        CURRENT_VERSION,
        VeniceIngestionSourceComponent.PRODUCER,
        VeniceDestinationIngestionComponent.SOURCE_BROKER,
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
            VeniceDestinationIngestionComponent.SOURCE_BROKER),
        INGESTION_TIME_BETWEEN_COMPONENTS.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @Test
  public void testRecordTimeBetweenComponentsMultipleCombinations() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Producer to source broker
    ingestionOtelStats.recordTimeBetweenComponents(
        CURRENT_VERSION,
        VeniceIngestionSourceComponent.PRODUCER,
        VeniceDestinationIngestionComponent.SOURCE_BROKER,
        50.0);

    // Source broker to leader consumer
    ingestionOtelStats.recordTimeBetweenComponents(
        CURRENT_VERSION,
        VeniceIngestionSourceComponent.SOURCE_BROKER,
        VeniceDestinationIngestionComponent.LEADER_CONSUMER,
        30.0);

    // Validate both combinations
    validateHistogramPointData(
        inMemoryMetricReader,
        50.0,
        50.0,
        1,
        50.0,
        buildAttributesWithComponents(
            VersionRole.CURRENT,
            VeniceIngestionSourceComponent.PRODUCER,
            VeniceDestinationIngestionComponent.SOURCE_BROKER),
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
            VeniceDestinationIngestionComponent.LEADER_CONSUMER),
        INGESTION_TIME_BETWEEN_COMPONENTS.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  // Tests for RT region-specific metrics

  @Test
  public void testRecordRtRecordsConsumed() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    ingestionOtelStats
        .recordRtRecordsConsumed(CURRENT_VERSION, REGION_US_WEST, REGION_US_EAST, VeniceRegionLocality.LOCAL, 10);

    validateObservableCounterValue(
        inMemoryMetricReader,
        10,
        buildAttributesWithRegions(VersionRole.CURRENT, REGION_US_WEST, REGION_US_EAST, VeniceRegionLocality.LOCAL),
        RT_RECORDS_CONSUMED.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @Test
  public void testRecordRtBytesConsumed() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    ingestionOtelStats
        .recordRtBytesConsumed(CURRENT_VERSION, REGION_US_WEST, REGION_US_EAST, VeniceRegionLocality.REMOTE, 1024);

    validateObservableCounterValue(
        inMemoryMetricReader,
        1024,
        buildAttributesWithRegions(VersionRole.CURRENT, REGION_US_WEST, REGION_US_EAST, VeniceRegionLocality.REMOTE),
        RT_BYTES_CONSUMED.getMetricEntity().getMetricName(),
        TEST_PREFIX);
  }

  @Test
  public void testNoMetricsRecordedWhenOtelDisabled() {
    // Use a fresh metric reader for this test to avoid interference from setUp's async gauges
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

    assertEquals(
        disabledMetricReader.collectAllMetrics().size(),
        0,
        "No metrics should be recorded when OTel disabled");
  }

  /**
   * Tests that RT metrics recording does not throw NPE when OTel is disabled.
   * This is a regression test for a bug where recordRtRecordsConsumed and recordRtBytesConsumed
   * would throw NullPointerException because baseDimensionsMap is null when OTel is disabled.
   */
  @Test
  public void testRtMetricsNoNpeWhenOtelDisabled() {
    // Use a fresh metric reader for this test
    InMemoryMetricReader disabledMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository disabledMetricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(false)
            .setOtelAdditionalMetricsReader(disabledMetricReader)
            .build());
    IngestionOtelStats stats = new IngestionOtelStats(disabledMetricsRepository, STORE_NAME, CLUSTER_NAME);

    assertFalse(stats.emitOtelMetrics(), "OTel should be disabled");

    stats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // These calls should NOT throw NPE even when OTel is disabled
    stats.recordRtRecordsConsumed(CURRENT_VERSION, REGION_US_WEST, REGION_US_EAST, VeniceRegionLocality.LOCAL, 100);
    stats.recordRtBytesConsumed(CURRENT_VERSION, REGION_US_WEST, REGION_US_EAST, VeniceRegionLocality.REMOTE, 1024);

    // Verify no metrics were recorded
    assertEquals(
        disabledMetricReader.collectAllMetrics().size(),
        0,
        "No RT metrics should be recorded when OTel disabled");
  }

  // Helper methods for building attributes

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
      VeniceDestinationIngestionComponent destComponent) {
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

  private Attributes buildAttributesWithRegions(
      VersionRole versionRole,
      String sourceRegion,
      String destRegion,
      VeniceRegionLocality locality) {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), CLUSTER_NAME)
        .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), versionRole.getDimensionValue())
        .put(VENICE_SOURCE_REGION.getDimensionNameInDefaultFormat(), sourceRegion)
        .put(VENICE_DESTINATION_REGION.getDimensionNameInDefaultFormat(), destRegion)
        .put(VENICE_REGION_LOCALITY.getDimensionNameInDefaultFormat(), locality.getDimensionValue())
        .build();
  }

  // ==================== Tests for ASYNC_GAUGE state management and callbacks ====================
  // Note: ASYNC_GAUGE metrics use callbacks that are invoked during metric collection.
  // These tests verify the state management methods that support those callbacks.

  @Test
  public void testSetAndRemoveIngestionTask() {
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);

    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Set ingestion task
    ingestionOtelStats.setIngestionTask(CURRENT_VERSION, mockTask);

    // Remove the task and verify no exceptions
    ingestionOtelStats.removeIngestionTask(CURRENT_VERSION);

    // Setting null task should be ignored (no exception)
    ingestionOtelStats.setIngestionTask(CURRENT_VERSION, null);
  }

  @Test
  public void testSetIngestionTaskForMultipleVersions() {
    StoreIngestionTask currentTask = mock(StoreIngestionTask.class);
    StoreIngestionTask futureTask = mock(StoreIngestionTask.class);
    StoreIngestionTask backupTask = mock(StoreIngestionTask.class);

    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.setIngestionTask(CURRENT_VERSION, currentTask);
    ingestionOtelStats.setIngestionTask(FUTURE_VERSION, futureTask);
    ingestionOtelStats.setIngestionTask(BACKUP_VERSION, backupTask);

    // All three tasks should be set without error
    // Remove them in sequence
    ingestionOtelStats.removeIngestionTask(BACKUP_VERSION);
    ingestionOtelStats.removeIngestionTask(CURRENT_VERSION);
    ingestionOtelStats.removeIngestionTask(FUTURE_VERSION);
  }

  @Test
  public void testPushTimeoutGaugeStateManagement() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Set push timeout for current version
    ingestionOtelStats.setIngestionTaskPushTimeoutGauge(CURRENT_VERSION, 1);

    // Reset push timeout
    ingestionOtelStats.setIngestionTaskPushTimeoutGauge(CURRENT_VERSION, 0);

    // Set for future version
    ingestionOtelStats.setIngestionTaskPushTimeoutGauge(FUTURE_VERSION, 1);

    // Remove task should clean up push timeout state
    ingestionOtelStats.removeIngestionTask(FUTURE_VERSION);
  }

  @Test
  public void testIdleTimeStateManagement() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Record idle time for current version
    ingestionOtelStats.recordIdleTime(CURRENT_VERSION, 5000);

    // Update idle time
    ingestionOtelStats.recordIdleTime(CURRENT_VERSION, 10000);

    // Record for future version
    ingestionOtelStats.recordIdleTime(FUTURE_VERSION, 3000);

    // Remove ingestion task should clean up idle time state
    ingestionOtelStats.removeIngestionTask(CURRENT_VERSION);
  }

  @Test
  public void testRemoveIngestionTaskCleansUpAllState() {
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);

    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.setIngestionTask(CURRENT_VERSION, mockTask);
    ingestionOtelStats.setIngestionTaskPushTimeoutGauge(CURRENT_VERSION, 1);
    ingestionOtelStats.recordIdleTime(CURRENT_VERSION, 5000);

    // Remove should clean up all state for this version
    ingestionOtelStats.removeIngestionTask(CURRENT_VERSION);

    // Setting new values after removal should work
    ingestionOtelStats.setIngestionTask(CURRENT_VERSION, mockTask);
    ingestionOtelStats.setIngestionTaskPushTimeoutGauge(CURRENT_VERSION, 0);
    ingestionOtelStats.recordIdleTime(CURRENT_VERSION, 0);
  }

  @Test
  public void testVersionRoleClassificationForAsyncGauges() {
    // Verify version classification works correctly for gauge callbacks
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Current version should be classified as CURRENT
    assertEquals(
        IngestionOtelStats.classifyVersion(CURRENT_VERSION, ingestionOtelStats.getVersionInfo()),
        VersionRole.CURRENT);

    // Future version should be classified as FUTURE
    assertEquals(
        IngestionOtelStats.classifyVersion(FUTURE_VERSION, ingestionOtelStats.getVersionInfo()),
        VersionRole.FUTURE);

    // Backup version should be classified as BACKUP
    assertEquals(
        IngestionOtelStats.classifyVersion(BACKUP_VERSION, ingestionOtelStats.getVersionInfo()),
        VersionRole.BACKUP);

    // Non-existing version should be classified as BACKUP
    assertEquals(
        IngestionOtelStats.classifyVersion(NON_EXISTING_VERSION, ingestionOtelStats.getVersionInfo()),
        VersionRole.BACKUP);
  }

  @Test
  public void testMultipleIdleTimeUpdates() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Record multiple idle time updates for the same version
    for (int i = 1; i <= 10; i++) {
      ingestionOtelStats.recordIdleTime(CURRENT_VERSION, i * 1000L);
    }

    // Record for different versions concurrently
    ingestionOtelStats.recordIdleTime(CURRENT_VERSION, 15000);
    ingestionOtelStats.recordIdleTime(FUTURE_VERSION, 8000);
    ingestionOtelStats.recordIdleTime(BACKUP_VERSION, 12000);
  }

  @Test
  public void testPushTimeoutGaugeToggle() {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Toggle push timeout on and off multiple times
    for (int i = 0; i < 5; i++) {
      ingestionOtelStats.setIngestionTaskPushTimeoutGauge(CURRENT_VERSION, 1);
      ingestionOtelStats.setIngestionTaskPushTimeoutGauge(CURRENT_VERSION, 0);
    }

    // Final state should be 0
    ingestionOtelStats.setIngestionTaskPushTimeoutGauge(CURRENT_VERSION, 0);
  }

  @Test
  public void testCloseMethod() throws Exception {
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);

    // Set up some state
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.setIngestionTask(CURRENT_VERSION, mockTask);
    ingestionOtelStats.setIngestionTask(FUTURE_VERSION, mockTask);
    ingestionOtelStats.setIngestionTaskPushTimeoutGauge(CURRENT_VERSION, 1);
    ingestionOtelStats.recordIdleTime(CURRENT_VERSION, 5000);

    // Record some RT metrics to populate the RT metrics maps
    ingestionOtelStats
        .recordRtRecordsConsumed(CURRENT_VERSION, REGION_US_WEST, REGION_US_EAST, VeniceRegionLocality.REMOTE, 100);
    ingestionOtelStats
        .recordRtBytesConsumed(CURRENT_VERSION, REGION_US_WEST, REGION_US_EAST, VeniceRegionLocality.REMOTE, 1000);

    // Verify state exists before close
    assertTrue(getIngestionTasksByVersion(ingestionOtelStats).containsKey(CURRENT_VERSION));
    assertTrue(getPushTimeoutByVersion(ingestionOtelStats).containsKey(CURRENT_VERSION));
    assertTrue(getIdleTimeByVersion(ingestionOtelStats).containsKey(CURRENT_VERSION));
    assertFalse(getRtRecordsConsumedMetrics(ingestionOtelStats).isEmpty());
    assertFalse(getRtBytesConsumedMetrics(ingestionOtelStats).isEmpty());

    // Close should clean up all state
    ingestionOtelStats.close();

    // Verify all maps were cleared
    assertTrue(getIngestionTasksByVersion(ingestionOtelStats).isEmpty(), "ingestionTasksByVersion should be cleared");
    assertTrue(getPushTimeoutByVersion(ingestionOtelStats).isEmpty(), "pushTimeoutByVersion should be cleared");
    assertTrue(getIdleTimeByVersion(ingestionOtelStats).isEmpty(), "idleTimeByVersion should be cleared");
    assertTrue(getRtRecordsConsumedMetrics(ingestionOtelStats).isEmpty(), "rtRecordsConsumedMetrics should be cleared");
    assertTrue(getRtBytesConsumedMetrics(ingestionOtelStats).isEmpty(), "rtBytesConsumedMetrics should be cleared");

    // After close, we should be able to set new state without issues
    ingestionOtelStats.setIngestionTask(CURRENT_VERSION, mockTask);
    ingestionOtelStats.setIngestionTaskPushTimeoutGauge(CURRENT_VERSION, 0);
    ingestionOtelStats.recordIdleTime(CURRENT_VERSION, 0);
  }

  /**
   * Tests that BACKUP version selection is deterministic - always returns the smallest
   * backup version when multiple backup versions exist.
   */
  @Test
  public void testDeterministicBackupVersionSelection() throws Exception {
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);

    // Set up with CURRENT=5, FUTURE=6
    // Backup versions will be 1, 3, 4
    int currentVersion = 5;
    int futureVersion = 6;
    int[] backupVersions = { 4, 1, 3 }; // Added in non-sorted order

    ingestionOtelStats.updateVersionInfo(currentVersion, futureVersion);

    // Add current and future tasks
    ingestionOtelStats.setIngestionTask(currentVersion, mockTask);
    ingestionOtelStats.setIngestionTask(futureVersion, mockTask);

    // Add backup tasks in non-sorted order
    for (int version: backupVersions) {
      ingestionOtelStats.setIngestionTask(version, mockTask);
    }

    // The BACKUP role should always return version 1 (smallest backup)
    // We test this by invoking the private getVersionForRole method via reflection
    Method method = IngestionOtelStats.class.getDeclaredMethod("getVersionForRole", VersionRole.class);
    method.setAccessible(true);

    int backupVersion = (int) method.invoke(ingestionOtelStats, VersionRole.BACKUP);
    assertEquals(backupVersion, 1, "BACKUP should return the smallest backup version (1)");

    // Remove version 1 and verify version 3 is now selected
    ingestionOtelStats.removeIngestionTask(1);
    backupVersion = (int) method.invoke(ingestionOtelStats, VersionRole.BACKUP);
    assertEquals(backupVersion, 3, "After removing 1, BACKUP should return version 3");

    // Remove version 3 and verify version 4 is now selected
    ingestionOtelStats.removeIngestionTask(3);
    backupVersion = (int) method.invoke(ingestionOtelStats, VersionRole.BACKUP);
    assertEquals(backupVersion, 4, "After removing 3, BACKUP should return version 4");

    // Remove version 4 and verify NON_EXISTING_VERSION is returned
    ingestionOtelStats.removeIngestionTask(4);
    backupVersion = (int) method.invoke(ingestionOtelStats, VersionRole.BACKUP);
    assertEquals(backupVersion, NON_EXISTING_VERSION, "With no backups, should return NON_EXISTING_VERSION");
  }

  /**
   * Tests the ASYNC_GAUGE callback for push timeout count.
   * Verifies that getPushTimeoutCountForRole returns the correct value.
   */
  @Test
  public void testGetPushTimeoutCountForRoleCallback() throws Exception {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    // Initially, push timeout should be 0 for all roles
    Method method = IngestionOtelStats.class.getDeclaredMethod("getPushTimeoutCountForRole", VersionRole.class);
    method.setAccessible(true);

    assertEquals(
        (long) method.invoke(ingestionOtelStats, VersionRole.CURRENT),
        0L,
        "Initial push timeout for CURRENT should be 0");
    assertEquals(
        (long) method.invoke(ingestionOtelStats, VersionRole.FUTURE),
        0L,
        "Initial push timeout for FUTURE should be 0");

    // Set push timeout for current version
    ingestionOtelStats.setIngestionTaskPushTimeoutGauge(CURRENT_VERSION, 1);
    assertEquals(
        (long) method.invoke(ingestionOtelStats, VersionRole.CURRENT),
        1L,
        "Push timeout for CURRENT should be 1 after setting");

    // Set push timeout for future version
    ingestionOtelStats.setIngestionTaskPushTimeoutGauge(FUTURE_VERSION, 1);
    assertEquals(
        (long) method.invoke(ingestionOtelStats, VersionRole.FUTURE),
        1L,
        "Push timeout for FUTURE should be 1 after setting");

    // Reset push timeout for current version
    ingestionOtelStats.setIngestionTaskPushTimeoutGauge(CURRENT_VERSION, 0);
    assertEquals(
        (long) method.invoke(ingestionOtelStats, VersionRole.CURRENT),
        0L,
        "Push timeout for CURRENT should be 0 after reset");

    // BACKUP with no backup version should return 0
    assertEquals(
        (long) method.invoke(ingestionOtelStats, VersionRole.BACKUP),
        0L,
        "Push timeout for BACKUP (no backup version) should be 0");
  }

  /**
   * Tests the ASYNC_GAUGE callback for idle time.
   * Verifies that getIdleTimeForRole returns the correct value.
   */
  @Test
  public void testGetIdleTimeForRoleCallback() throws Exception {
    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);

    Method method = IngestionOtelStats.class.getDeclaredMethod("getIdleTimeForRole", VersionRole.class);
    method.setAccessible(true);

    // Initially, idle time should be 0 for all roles
    assertEquals(
        (long) method.invoke(ingestionOtelStats, VersionRole.CURRENT),
        0L,
        "Initial idle time for CURRENT should be 0");

    // Record idle time for current version
    ingestionOtelStats.recordIdleTime(CURRENT_VERSION, 5000L);
    assertEquals(
        (long) method.invoke(ingestionOtelStats, VersionRole.CURRENT),
        5000L,
        "Idle time for CURRENT should be 5000 after recording");

    // Update idle time
    ingestionOtelStats.recordIdleTime(CURRENT_VERSION, 10000L);
    assertEquals(
        (long) method.invoke(ingestionOtelStats, VersionRole.CURRENT),
        10000L,
        "Idle time for CURRENT should be 10000 after update");

    // Record idle time for future version
    ingestionOtelStats.recordIdleTime(FUTURE_VERSION, 3000L);
    assertEquals(
        (long) method.invoke(ingestionOtelStats, VersionRole.FUTURE),
        3000L,
        "Idle time for FUTURE should be 3000");

    // BACKUP with no backup version should return 0
    assertEquals(
        (long) method.invoke(ingestionOtelStats, VersionRole.BACKUP),
        0L,
        "Idle time for BACKUP (no backup version) should be 0");
  }

  /**
   * Tests the ASYNC_GAUGE callback for idle time with backup versions.
   */
  @Test
  public void testGetIdleTimeForBackupRole() throws Exception {
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);

    // CURRENT=5, FUTURE=6, BACKUP will be 1
    int currentVersion = 5;
    int futureVersion = 6;
    int backupVersion = 1;

    ingestionOtelStats.updateVersionInfo(currentVersion, futureVersion);
    ingestionOtelStats.setIngestionTask(backupVersion, mockTask);

    Method method = IngestionOtelStats.class.getDeclaredMethod("getIdleTimeForRole", VersionRole.class);
    method.setAccessible(true);

    // Initially backup idle time should be 0
    assertEquals(
        (long) method.invoke(ingestionOtelStats, VersionRole.BACKUP),
        0L,
        "Initial idle time for BACKUP should be 0");

    // Record idle time for backup version
    ingestionOtelStats.recordIdleTime(backupVersion, 7000L);
    assertEquals(
        (long) method.invoke(ingestionOtelStats, VersionRole.BACKUP),
        7000L,
        "Idle time for BACKUP should be 7000 after recording");
  }

  /**
   * Tests that RT metrics (rtRecordsConsumedMetrics, rtBytesConsumedMetrics) are NOT cleaned
   * during removeIngestionTask. This is by design - RT metrics are keyed by region combinations,
   * not versions. The version is only used to classify into VersionRole at recording time.
   * These maps are only cleared when the entire store is deleted via close().
   */
  @Test
  public void testRtMetricsNotCleanedOnVersionRemoval() throws Exception {
    StoreIngestionTask mockTask = mock(StoreIngestionTask.class);

    ingestionOtelStats.updateVersionInfo(CURRENT_VERSION, FUTURE_VERSION);
    ingestionOtelStats.setIngestionTask(CURRENT_VERSION, mockTask);

    // Record RT metrics for the current version
    ingestionOtelStats
        .recordRtRecordsConsumed(CURRENT_VERSION, REGION_US_WEST, REGION_US_EAST, VeniceRegionLocality.LOCAL, 100);
    ingestionOtelStats
        .recordRtBytesConsumed(CURRENT_VERSION, REGION_US_WEST, REGION_US_EAST, VeniceRegionLocality.LOCAL, 1000);

    // Verify RT metrics were recorded
    assertFalse(
        getRtRecordsConsumedMetrics(ingestionOtelStats).isEmpty(),
        "RT records metrics should exist after recording");
    assertFalse(
        getRtBytesConsumedMetrics(ingestionOtelStats).isEmpty(),
        "RT bytes metrics should exist after recording");

    // Remove the version - this should NOT clean up RT metrics
    ingestionOtelStats.removeIngestionTask(CURRENT_VERSION);

    // Verify version-specific state was cleaned
    assertFalse(
        getIngestionTasksByVersion(ingestionOtelStats).containsKey(CURRENT_VERSION),
        "Task should be removed for the version");

    // Verify RT metrics are still present (by design, they're keyed by region, not version)
    assertFalse(
        getRtRecordsConsumedMetrics(ingestionOtelStats).isEmpty(),
        "RT records metrics should NOT be cleaned on version removal");
    assertFalse(
        getRtBytesConsumedMetrics(ingestionOtelStats).isEmpty(),
        "RT bytes metrics should NOT be cleaned on version removal");

    // Only close() should clean up RT metrics
    ingestionOtelStats.close();
    assertTrue(
        getRtRecordsConsumedMetrics(ingestionOtelStats).isEmpty(),
        "RT records metrics should be cleaned after close()");
    assertTrue(
        getRtBytesConsumedMetrics(ingestionOtelStats).isEmpty(),
        "RT bytes metrics should be cleaned after close()");
  }

  // ==================== Reflection helper methods ====================

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
  private Map<String, ?> getRtRecordsConsumedMetrics(IngestionOtelStats stats) throws Exception {
    Field field = IngestionOtelStats.class.getDeclaredField("rtRecordsConsumedMetrics");
    field.setAccessible(true);
    return (Map<String, ?>) field.get(stats);
  }

  @SuppressWarnings("unchecked")
  private Map<String, ?> getRtBytesConsumedMetrics(IngestionOtelStats stats) throws Exception {
    Field field = IngestionOtelStats.class.getDeclaredField("rtBytesConsumedMetrics");
    field.setAccessible(true);
    return (Map<String, ?>) field.get(stats);
  }
}
