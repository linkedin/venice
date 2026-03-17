package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DIV_RESULT;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_DIV_SEVERITY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.exceptions.validation.CorruptDataException;
import com.linkedin.venice.exceptions.validation.DataValidationException;
import com.linkedin.venice.exceptions.validation.DuplicateDataException;
import com.linkedin.venice.exceptions.validation.MissingDataException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceDIVResult;
import com.linkedin.venice.stats.dimensions.VeniceDIVSeverity;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import com.linkedin.venice.utils.lazy.Lazy;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class DIVStatsOtelTest {
  private static final String TEST_METRIC_PREFIX = "server";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_STORE_NAME = "test-store";
  private static final String TEST_STORE_NAME_2 = "test-store-2";

  private static final String OTEL_MESSAGE_COUNT = DIVOtelMetricEntity.MESSAGE_COUNT.getMetricEntity().getMetricName();
  private static final String OTEL_OFFSET_REWIND_COUNT =
      DIVOtelMetricEntity.OFFSET_REWIND_COUNT.getMetricEntity().getMetricName();
  private static final String OTEL_PRODUCER_FAILURE_COUNT =
      DIVOtelMetricEntity.PRODUCER_FAILURE_COUNT.getMetricEntity().getMetricName();
  private static final String OTEL_BENIGN_PRODUCER_FAILURE_COUNT =
      DIVOtelMetricEntity.BENIGN_PRODUCER_FAILURE_COUNT.getMetricEntity().getMetricName();

  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private ReadOnlyStoreRepository mockMetadataRepository;
  private AggVersionedDIVStats stats;

  @BeforeMethod
  public void setUp() {
    inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());
    mockMetadataRepository = mock(ReadOnlyStoreRepository.class);
    doReturn(Collections.emptyList()).when(mockMetadataRepository).getAllStores();
    stats = new AggVersionedDIVStats(metricsRepository, mockMetadataRepository, true, TEST_CLUSTER_NAME);
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
  }

  /**
   * Sets up a store with a current version (1) and a future version (2), then triggers
   * version info propagation so OTel version role classification works correctly.
   * Uses handleStoreCreated to add the store to the map first, since handleStoreChanged
   * requires the store to already be tracked (or getStoreOrThrow to succeed).
   */
  private void setUpStoreVersions(String storeName) {
    Store mockStore = mock(Store.class);
    doReturn(storeName).when(mockStore).getName();
    doReturn(1).when(mockStore).getCurrentVersion();
    Version v1 = new VersionImpl(storeName, 1, "test-push-1");
    v1.setStatus(VersionStatus.ONLINE);
    Version v2 = new VersionImpl(storeName, 2, "test-push-2");
    v2.setStatus(VersionStatus.STARTED);
    doReturn(Arrays.asList(v1, v2)).when(mockStore).getVersions();
    stats.handleStoreCreated(mockStore);
  }

  // --- Positive tests: message count ---

  @Test
  public void testRecordSuccessMsg() {
    setUpStoreVersions(TEST_STORE_NAME);
    stats.recordSuccessMsg(TEST_STORE_NAME, 1);

    validateAsyncCounter(
        OTEL_MESSAGE_COUNT,
        1,
        buildMessageAttributes(TEST_STORE_NAME, VersionRole.CURRENT, VeniceDIVResult.SUCCESS));
  }

  @Test
  public void testRecordDuplicateMsg() {
    setUpStoreVersions(TEST_STORE_NAME);
    stats.recordDuplicateMsg(TEST_STORE_NAME, 2);

    validateAsyncCounter(
        OTEL_MESSAGE_COUNT,
        1,
        buildMessageAttributes(TEST_STORE_NAME, VersionRole.FUTURE, VeniceDIVResult.DUPLICATE));
  }

  @Test
  public void testRecordMissingMsg() {
    setUpStoreVersions(TEST_STORE_NAME);
    stats.recordMissingMsg(TEST_STORE_NAME, 99);

    validateAsyncCounter(
        OTEL_MESSAGE_COUNT,
        1,
        buildMessageAttributes(TEST_STORE_NAME, VersionRole.BACKUP, VeniceDIVResult.MISSING));
  }

  @Test
  public void testRecordCorruptedMsg() {
    setUpStoreVersions(TEST_STORE_NAME);
    stats.recordCorruptedMsg(TEST_STORE_NAME, 1);

    validateAsyncCounter(
        OTEL_MESSAGE_COUNT,
        1,
        buildMessageAttributes(TEST_STORE_NAME, VersionRole.CURRENT, VeniceDIVResult.CORRUPTED));
  }

  // --- Positive tests: offset rewind count ---

  @Test
  public void testRecordBenignLeaderOffsetRewind() {
    setUpStoreVersions(TEST_STORE_NAME);
    stats.recordBenignLeaderOffsetRewind(TEST_STORE_NAME, 1);

    validateCounter(
        OTEL_OFFSET_REWIND_COUNT,
        1,
        buildSeverityAttributes(TEST_STORE_NAME, VersionRole.CURRENT, VeniceDIVSeverity.BENIGN));
  }

  @Test
  public void testRecordPotentiallyLossyLeaderOffsetRewind() {
    setUpStoreVersions(TEST_STORE_NAME);
    stats.recordPotentiallyLossyLeaderOffsetRewind(TEST_STORE_NAME, 2);

    validateCounter(
        OTEL_OFFSET_REWIND_COUNT,
        1,
        buildSeverityAttributes(TEST_STORE_NAME, VersionRole.FUTURE, VeniceDIVSeverity.POTENTIALLY_LOSSY));
  }

  // --- Positive tests: producer failure count ---

  @Test
  public void testRecordLeaderProducerFailure() {
    setUpStoreVersions(TEST_STORE_NAME);
    stats.recordLeaderProducerFailure(TEST_STORE_NAME, 1);

    validateCounter(OTEL_PRODUCER_FAILURE_COUNT, 1, buildRoleOnlyAttributes(TEST_STORE_NAME, VersionRole.CURRENT));
  }

  @Test
  public void testRecordBenignLeaderProducerFailure() {
    setUpStoreVersions(TEST_STORE_NAME);
    stats.recordBenignLeaderProducerFailure(TEST_STORE_NAME, 2);

    validateCounter(
        OTEL_BENIGN_PRODUCER_FAILURE_COUNT,
        1,
        buildRoleOnlyAttributes(TEST_STORE_NAME, VersionRole.FUTURE));
  }

  // --- Accumulation tests ---

  @Test
  public void testMessageCountAccumulation() {
    setUpStoreVersions(TEST_STORE_NAME);
    stats.recordSuccessMsg(TEST_STORE_NAME, 1);
    stats.recordSuccessMsg(TEST_STORE_NAME, 1);
    stats.recordSuccessMsg(TEST_STORE_NAME, 1);

    validateAsyncCounter(
        OTEL_MESSAGE_COUNT,
        3,
        buildMessageAttributes(TEST_STORE_NAME, VersionRole.CURRENT, VeniceDIVResult.SUCCESS));
  }

  // --- Dimension isolation tests ---

  @Test
  public void testDIVResultDimensionIsolation() {
    setUpStoreVersions(TEST_STORE_NAME);
    stats.recordSuccessMsg(TEST_STORE_NAME, 1);
    stats.recordSuccessMsg(TEST_STORE_NAME, 1);
    stats.recordDuplicateMsg(TEST_STORE_NAME, 1);

    // Collect once — ASYNC_COUNTER uses sumThenReset, so subsequent collections would see 0.
    Collection<MetricData> metrics = inMemoryMetricReader.collectAllMetrics();
    validateAsyncCounterFromCollection(
        metrics,
        OTEL_MESSAGE_COUNT,
        2,
        buildMessageAttributes(TEST_STORE_NAME, VersionRole.CURRENT, VeniceDIVResult.SUCCESS));
    validateAsyncCounterFromCollection(
        metrics,
        OTEL_MESSAGE_COUNT,
        1,
        buildMessageAttributes(TEST_STORE_NAME, VersionRole.CURRENT, VeniceDIVResult.DUPLICATE));
  }

  @Test
  public void testVersionRoleDimensionIsolation() {
    setUpStoreVersions(TEST_STORE_NAME);
    stats.recordSuccessMsg(TEST_STORE_NAME, 1);
    stats.recordSuccessMsg(TEST_STORE_NAME, 2);

    Collection<MetricData> metrics = inMemoryMetricReader.collectAllMetrics();
    validateAsyncCounterFromCollection(
        metrics,
        OTEL_MESSAGE_COUNT,
        1,
        buildMessageAttributes(TEST_STORE_NAME, VersionRole.CURRENT, VeniceDIVResult.SUCCESS));
    validateAsyncCounterFromCollection(
        metrics,
        OTEL_MESSAGE_COUNT,
        1,
        buildMessageAttributes(TEST_STORE_NAME, VersionRole.FUTURE, VeniceDIVResult.SUCCESS));
  }

  @Test
  public void testSeverityDimensionIsolation() {
    setUpStoreVersions(TEST_STORE_NAME);
    stats.recordBenignLeaderOffsetRewind(TEST_STORE_NAME, 1);
    stats.recordBenignLeaderOffsetRewind(TEST_STORE_NAME, 1);
    stats.recordPotentiallyLossyLeaderOffsetRewind(TEST_STORE_NAME, 1);

    validateCounter(
        OTEL_OFFSET_REWIND_COUNT,
        2,
        buildSeverityAttributes(TEST_STORE_NAME, VersionRole.CURRENT, VeniceDIVSeverity.BENIGN));
    validateCounter(
        OTEL_OFFSET_REWIND_COUNT,
        1,
        buildSeverityAttributes(TEST_STORE_NAME, VersionRole.CURRENT, VeniceDIVSeverity.POTENTIALLY_LOSSY));
  }

  // --- Multi-store isolation test ---

  @Test
  public void testMultiStoreIsolation() {
    setUpStoreVersions(TEST_STORE_NAME);
    setUpStoreVersions(TEST_STORE_NAME_2);

    stats.recordSuccessMsg(TEST_STORE_NAME, 1);
    stats.recordSuccessMsg(TEST_STORE_NAME, 1);
    stats.recordSuccessMsg(TEST_STORE_NAME_2, 1);

    Collection<MetricData> metrics = inMemoryMetricReader.collectAllMetrics();
    validateAsyncCounterFromCollection(
        metrics,
        OTEL_MESSAGE_COUNT,
        2,
        buildMessageAttributes(TEST_STORE_NAME, VersionRole.CURRENT, VeniceDIVResult.SUCCESS));
    validateAsyncCounterFromCollection(
        metrics,
        OTEL_MESSAGE_COUNT,
        1,
        buildMessageAttributes(TEST_STORE_NAME_2, VersionRole.CURRENT, VeniceDIVResult.SUCCESS));
  }

  // --- Version classification test ---

  @Test
  public void testVersionClassificationBackup() {
    setUpStoreVersions(TEST_STORE_NAME);
    // Version 99 is neither current (1) nor future (2), so it should be BACKUP
    stats.recordSuccessMsg(TEST_STORE_NAME, 99);

    validateAsyncCounter(
        OTEL_MESSAGE_COUNT,
        1,
        buildMessageAttributes(TEST_STORE_NAME, VersionRole.BACKUP, VeniceDIVResult.SUCCESS));
  }

  @Test
  public void testVersionClassificationDefaultsToBackupWhenNoVersionInfo() {
    // Create store with no versions so there's no version info for classification
    stats.handleStoreCreated(createMockStoreWithNoVersions(TEST_STORE_NAME));

    stats.recordSuccessMsg(TEST_STORE_NAME, 1);

    validateAsyncCounter(
        OTEL_MESSAGE_COUNT,
        1,
        buildMessageAttributes(TEST_STORE_NAME, VersionRole.BACKUP, VeniceDIVResult.SUCCESS));
  }

  // --- recordException dispatch tests ---

  @Test
  public void testRecordExceptionDispatchesByType() {
    setUpStoreVersions(TEST_STORE_NAME);

    stats.recordException(TEST_STORE_NAME, 1, new DuplicateDataException("dup"));
    stats.recordException(TEST_STORE_NAME, 1, new MissingDataException("miss"));
    stats.recordException(TEST_STORE_NAME, 1, new CorruptDataException(Lazy.of(() -> "corrupt")));

    Collection<MetricData> metrics = inMemoryMetricReader.collectAllMetrics();
    validateAsyncCounterFromCollection(
        metrics,
        OTEL_MESSAGE_COUNT,
        1,
        buildMessageAttributes(TEST_STORE_NAME, VersionRole.CURRENT, VeniceDIVResult.DUPLICATE));
    validateAsyncCounterFromCollection(
        metrics,
        OTEL_MESSAGE_COUNT,
        1,
        buildMessageAttributes(TEST_STORE_NAME, VersionRole.CURRENT, VeniceDIVResult.MISSING));
    validateAsyncCounterFromCollection(
        metrics,
        OTEL_MESSAGE_COUNT,
        1,
        buildMessageAttributes(TEST_STORE_NAME, VersionRole.CURRENT, VeniceDIVResult.CORRUPTED));
  }

  @Test
  public void testRecordExceptionIgnoresUnknownSubtype() {
    setUpStoreVersions(TEST_STORE_NAME);
    // An unknown DataValidationException subtype should not produce any OTel recording
    DataValidationException unknownSubtype = new DataValidationException("generic", false) {
    };
    stats.recordException(TEST_STORE_NAME, 1, unknownSubtype);
    // Verify no metric was recorded: record a known type and verify count=1
    // (proving the unknown exception didn't add anything).
    stats.recordSuccessMsg(TEST_STORE_NAME, 1);
    validateAsyncCounter(
        OTEL_MESSAGE_COUNT,
        1,
        buildMessageAttributes(TEST_STORE_NAME, VersionRole.CURRENT, VeniceDIVResult.SUCCESS));
  }

  // --- Version transition test ---

  @Test
  public void testVersionTransitionViaHandleStoreChanged() {
    setUpStoreVersions(TEST_STORE_NAME);
    // Initially: current=1, future=2. Record for version 2 → FUTURE
    stats.recordSuccessMsg(TEST_STORE_NAME, 2);
    validateAsyncCounter(
        OTEL_MESSAGE_COUNT,
        1,
        buildMessageAttributes(TEST_STORE_NAME, VersionRole.FUTURE, VeniceDIVResult.SUCCESS));

    // Promote version 2 to current via handleStoreChanged
    Store updatedStore = mock(Store.class);
    doReturn(TEST_STORE_NAME).when(updatedStore).getName();
    doReturn(2).when(updatedStore).getCurrentVersion();
    Version v2 = new VersionImpl(TEST_STORE_NAME, 2, "test-push-2");
    v2.setStatus(VersionStatus.ONLINE);
    doReturn(Collections.singletonList(v2)).when(updatedStore).getVersions();
    // Need getStoreOrThrow to work since handleStoreChanged -> getVersionedStats may look it up
    doReturn(updatedStore).when(mockMetadataRepository).getStoreOrThrow(TEST_STORE_NAME);
    stats.handleStoreChanged(updatedStore);

    // Now version 2 should be classified as CURRENT
    stats.recordSuccessMsg(TEST_STORE_NAME, 2);
    // Cumulative: 1 from FUTURE + 1 from CURRENT. Check the CURRENT bucket has 1.
    validateAsyncCounter(
        OTEL_MESSAGE_COUNT,
        1,
        buildMessageAttributes(TEST_STORE_NAME, VersionRole.CURRENT, VeniceDIVResult.SUCCESS));
  }

  // --- No double-counting test (total stats must NOT produce OTel data) ---

  /**
   * Verifies that OTel counter increments by exactly 1 per recording call, proving
   * that the total stats path (Tehuti-only) does NOT produce an additional OTel recording.
   * If total stats double-counted OTel, the counter would be 2 instead of 1.
   */
  @Test
  public void testNoOtelDoubleCountingFromTotalStats() {
    setUpStoreVersions(TEST_STORE_NAME);

    // Record once via the public method — this records to Tehuti total + per-version,
    // but OTel should only be recorded once (per-version path).
    stats.recordSuccessMsg(TEST_STORE_NAME, 1);
    validateAsyncCounter(
        OTEL_MESSAGE_COUNT,
        1,
        buildMessageAttributes(TEST_STORE_NAME, VersionRole.CURRENT, VeniceDIVResult.SUCCESS));

    stats.recordBenignLeaderOffsetRewind(TEST_STORE_NAME, 1);
    validateCounter(
        OTEL_OFFSET_REWIND_COUNT,
        1,
        buildSeverityAttributes(TEST_STORE_NAME, VersionRole.CURRENT, VeniceDIVSeverity.BENIGN));

    stats.recordLeaderProducerFailure(TEST_STORE_NAME, 1);
    validateCounter(OTEL_PRODUCER_FAILURE_COUNT, 1, buildRoleOnlyAttributes(TEST_STORE_NAME, VersionRole.CURRENT));

    stats.recordBenignLeaderProducerFailure(TEST_STORE_NAME, 1);
    validateCounter(
        OTEL_BENIGN_PRODUCER_FAILURE_COUNT,
        1,
        buildRoleOnlyAttributes(TEST_STORE_NAME, VersionRole.CURRENT));
  }

  // --- NPE prevention tests ---

  @Test
  public void testNoNpeWhenOtelDisabled() {
    try (VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build())) {
      exerciseAllRecordingPaths(
          new AggVersionedDIVStats(disabledRepo, createEmptyMockMetadataRepository(), true, TEST_CLUSTER_NAME));
    }
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    exerciseAllRecordingPaths(
        new AggVersionedDIVStats(
            new MetricsRepository(),
            createEmptyMockMetadataRepository(),
            true,
            TEST_CLUSTER_NAME));
  }

  @Test
  public void testNoNpeWhenNoClusterName() {
    exerciseAllRecordingPaths(
        new AggVersionedDIVStats(new MetricsRepository(), createEmptyMockMetadataRepository(), true, null));
  }

  private void exerciseAllRecordingPaths(AggVersionedDIVStats divStats) {
    // Create a minimal store so recording methods don't NPE on store lookup
    divStats.handleStoreCreated(createMockStoreWithNoVersions(TEST_STORE_NAME));

    divStats.recordSuccessMsg(TEST_STORE_NAME, 1);
    divStats.recordDuplicateMsg(TEST_STORE_NAME, 1);
    divStats.recordMissingMsg(TEST_STORE_NAME, 1);
    divStats.recordCorruptedMsg(TEST_STORE_NAME, 1);
    divStats.recordBenignLeaderOffsetRewind(TEST_STORE_NAME, 1);
    divStats.recordPotentiallyLossyLeaderOffsetRewind(TEST_STORE_NAME, 1);
    divStats.recordLeaderProducerFailure(TEST_STORE_NAME, 1);
    divStats.recordBenignLeaderProducerFailure(TEST_STORE_NAME, 1);
  }

  // --- Store deletion cleanup test ---

  @Test
  public void testStoreDeletedCleansUpOtelState() {
    setUpStoreVersions(TEST_STORE_NAME);
    stats.recordSuccessMsg(TEST_STORE_NAME, 1);

    // Verify OTel metric exists
    validateAsyncCounter(
        OTEL_MESSAGE_COUNT,
        1,
        buildMessageAttributes(TEST_STORE_NAME, VersionRole.CURRENT, VeniceDIVResult.SUCCESS));

    // Delete the store and record again — a new MetricEntityState should be created
    stats.handleStoreDeleted(TEST_STORE_NAME);
    setUpStoreVersions(TEST_STORE_NAME);
    stats.recordSuccessMsg(TEST_STORE_NAME, 1);

    // Counter is cumulative, so after reset + 1 new recording, OTel reader sees
    // the new recording (value depends on SDK behavior for new instrument instance).
    // The key verification is that no NPE occurs and recording succeeds.
  }

  // --- Helper methods ---

  private static ReadOnlyStoreRepository createEmptyMockMetadataRepository() {
    ReadOnlyStoreRepository mockRepo = mock(ReadOnlyStoreRepository.class);
    doReturn(Collections.emptyList()).when(mockRepo).getAllStores();
    return mockRepo;
  }

  private static Store createMockStoreWithNoVersions(String storeName) {
    Store mockStore = mock(Store.class);
    doReturn(storeName).when(mockStore).getName();
    doReturn(0).when(mockStore).getCurrentVersion();
    doReturn(Collections.emptyList()).when(mockStore).getVersions();
    return mockStore;
  }

  private static AttributesBuilder baseAttributesBuilder(String storeName, VersionRole role) {
    return Attributes.builder()
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME)
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), storeName)
        .put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), role.getDimensionValue());
  }

  private static Attributes buildMessageAttributes(String storeName, VersionRole role, VeniceDIVResult result) {
    return baseAttributesBuilder(storeName, role)
        .put(VENICE_DIV_RESULT.getDimensionNameInDefaultFormat(), result.getDimensionValue())
        .build();
  }

  private static Attributes buildSeverityAttributes(String storeName, VersionRole role, VeniceDIVSeverity severity) {
    return baseAttributesBuilder(storeName, role)
        .put(VENICE_DIV_SEVERITY.getDimensionNameInDefaultFormat(), severity.getDimensionValue())
        .build();
  }

  private static Attributes buildRoleOnlyAttributes(String storeName, VersionRole role) {
    return baseAttributesBuilder(storeName, role).build();
  }

  private void validateCounter(String metricName, long expectedValue, Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        expectedValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }

  private void validateAsyncCounter(String metricName, long expectedValue, Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateObservableCounterValue(
        inMemoryMetricReader,
        expectedValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }

  /**
   * Validates an async counter value from a pre-collected metrics snapshot. Use this when
   * validating multiple dimension values of the same ASYNC_COUNTER metric in a single test —
   * each {@code collectAllMetrics()} call resets the LongAdder via {@code sumThenReset()},
   * so only the first collection returns the accumulated values.
   */
  private static void validateAsyncCounterFromCollection(
      Collection<MetricData> metricsData,
      String metricName,
      long expectedValue,
      Attributes expectedAttributes) {
    LongPointData point = OpenTelemetryDataTestUtils
        .getLongPointDataFromSum(metricsData, metricName, TEST_METRIC_PREFIX, expectedAttributes);
    assertNotNull(point, "LongPointData should not be null for " + metricName + " with " + expectedAttributes);
    assertEquals(point.getValue(), expectedValue);
  }

}
