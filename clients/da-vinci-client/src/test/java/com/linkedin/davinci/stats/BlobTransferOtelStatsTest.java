package com.linkedin.davinci.stats;

import static com.linkedin.davinci.stats.ServerMetricEntity.SERVER_METRIC_ENTITIES;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_CLUSTER_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_RESPONSE_STATUS_CODE_CATEGORY;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_VERSION_ROLE;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.server.VersionRole;
import com.linkedin.venice.stats.VeniceMetricsConfig;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.VeniceResponseStatusCategory;
import com.linkedin.venice.utils.OpenTelemetryDataTestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collection;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class BlobTransferOtelStatsTest {
  private static final String TEST_METRIC_PREFIX = "server";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_STORE_NAME = "test-store";

  private static final String OTEL_RESPONSE_COUNT =
      BlobTransferOtelMetricEntity.RESPONSE_COUNT.getMetricEntity().getMetricName();
  private static final String OTEL_TIME = BlobTransferOtelMetricEntity.TIME.getMetricEntity().getMetricName();
  private static final String OTEL_BYTES_RECEIVED =
      BlobTransferOtelMetricEntity.BYTES_RECEIVED.getMetricEntity().getMetricName();
  private static final String OTEL_BYTES_SENT =
      BlobTransferOtelMetricEntity.BYTES_SENT.getMetricEntity().getMetricName();

  private InMemoryMetricReader inMemoryMetricReader;
  private VeniceMetricsRepository metricsRepository;
  private BlobTransferOtelStats stats;

  @BeforeMethod
  public void setUp() {
    inMemoryMetricReader = InMemoryMetricReader.create();
    metricsRepository = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX)
            .setMetricEntities(SERVER_METRIC_ENTITIES)
            .setEmitOtelMetrics(true)
            .setOtelAdditionalMetricsReader(inMemoryMetricReader)
            .build());
    stats = new BlobTransferOtelStats(metricsRepository, TEST_STORE_NAME, TEST_CLUSTER_NAME);
    // Set version info: version 1 = CURRENT, version 2 = FUTURE
    stats.updateVersionInfo(1, 2);
  }

  @AfterMethod
  public void tearDown() {
    if (metricsRepository != null) {
      metricsRepository.close();
    }
  }

  // --- Recording tests ---

  @Test
  public void testRecordResponseCountSuccess() {
    stats.recordResponseCount(1, VeniceResponseStatusCategory.SUCCESS);

    validateCounter(
        OTEL_RESPONSE_COUNT,
        1,
        buildResponseCountAttributes(VersionRole.CURRENT, VeniceResponseStatusCategory.SUCCESS));
  }

  @Test
  public void testRecordResponseCountFail() {
    stats.recordResponseCount(1, VeniceResponseStatusCategory.FAIL);

    validateCounter(
        OTEL_RESPONSE_COUNT,
        1,
        buildResponseCountAttributes(VersionRole.CURRENT, VeniceResponseStatusCategory.FAIL));
  }

  @Test
  public void testRecordTime() {
    double timeInSec = 5.5;
    stats.recordTime(1, timeInSec);

    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        inMemoryMetricReader,
        timeInSec,
        timeInSec,
        1,
        timeInSec,
        buildVersionRoleAttributes(VersionRole.CURRENT),
        OTEL_TIME,
        TEST_METRIC_PREFIX);
  }

  @Test
  public void testRecordBytesReceived() {
    stats.recordBytesReceived(1, 1024);

    validateCounter(OTEL_BYTES_RECEIVED, 1024, buildVersionRoleAttributes(VersionRole.CURRENT));
  }

  @Test
  public void testRecordBytesSent() {
    stats.recordBytesSent(1, 2048);

    validateCounter(OTEL_BYTES_SENT, 2048, buildVersionRoleAttributes(VersionRole.CURRENT));
  }

  // --- Version classification tests ---

  @Test
  public void testFutureVersionClassification() {
    stats.recordBytesReceived(2, 200);

    validateCounter(OTEL_BYTES_RECEIVED, 200, buildVersionRoleAttributes(VersionRole.FUTURE));
  }

  @Test
  public void testBackupVersionClassification() {
    stats.recordBytesReceived(99, 300);

    validateCounter(OTEL_BYTES_RECEIVED, 300, buildVersionRoleAttributes(VersionRole.BACKUP));
  }

  // --- Accumulation tests ---

  @Test
  public void testResponseCountAccumulation() {
    stats.recordResponseCount(1, VeniceResponseStatusCategory.SUCCESS);
    stats.recordResponseCount(1, VeniceResponseStatusCategory.SUCCESS);
    stats.recordResponseCount(1, VeniceResponseStatusCategory.SUCCESS);

    validateCounter(
        OTEL_RESPONSE_COUNT,
        3,
        buildResponseCountAttributes(VersionRole.CURRENT, VeniceResponseStatusCategory.SUCCESS));
  }

  @Test
  public void testBytesReceivedAccumulation() {
    stats.recordBytesReceived(1, 100);
    stats.recordBytesReceived(1, 200);

    validateCounter(OTEL_BYTES_RECEIVED, 300, buildVersionRoleAttributes(VersionRole.CURRENT));
  }

  @Test
  public void testTimeHistogramMultipleRecordings() {
    stats.recordTime(1, 3.0);
    stats.recordTime(1, 7.0);

    OpenTelemetryDataTestUtils.validateExponentialHistogramPointData(
        inMemoryMetricReader,
        3.0,
        7.0,
        2,
        10.0,
        buildVersionRoleAttributes(VersionRole.CURRENT),
        OTEL_TIME,
        TEST_METRIC_PREFIX);
  }

  // --- Dimension isolation tests ---

  @Test
  public void testSuccessAndFailAreIndependent() {
    stats.recordResponseCount(1, VeniceResponseStatusCategory.SUCCESS);
    stats.recordResponseCount(1, VeniceResponseStatusCategory.SUCCESS);
    stats.recordResponseCount(1, VeniceResponseStatusCategory.FAIL);

    validateCounter(
        OTEL_RESPONSE_COUNT,
        2,
        buildResponseCountAttributes(VersionRole.CURRENT, VeniceResponseStatusCategory.SUCCESS));
    validateCounter(
        OTEL_RESPONSE_COUNT,
        1,
        buildResponseCountAttributes(VersionRole.CURRENT, VeniceResponseStatusCategory.FAIL));
  }

  @Test
  public void testVersionRolesAreIndependent() {
    stats.recordBytesReceived(1, 100); // CURRENT
    stats.recordBytesReceived(2, 200); // FUTURE
    stats.recordBytesReceived(99, 300); // BACKUP

    validateCounter(OTEL_BYTES_RECEIVED, 100, buildVersionRoleAttributes(VersionRole.CURRENT));
    validateCounter(OTEL_BYTES_RECEIVED, 200, buildVersionRoleAttributes(VersionRole.FUTURE));
    validateCounter(OTEL_BYTES_RECEIVED, 300, buildVersionRoleAttributes(VersionRole.BACKUP));
  }

  // --- Cross-metric isolation tests ---

  @Test
  public void testRecordTimeDoesNotAffectCounters() {
    stats.recordTime(1, 5.0);

    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    Attributes currentAttrs = buildVersionRoleAttributes(VersionRole.CURRENT);

    OpenTelemetryDataTestUtils
        .assertNoLongSumDataForAttributes(metricsData, OTEL_BYTES_RECEIVED, TEST_METRIC_PREFIX, currentAttrs);
    OpenTelemetryDataTestUtils
        .assertNoLongSumDataForAttributes(metricsData, OTEL_BYTES_SENT, TEST_METRIC_PREFIX, currentAttrs);
    OpenTelemetryDataTestUtils.assertNoLongSumDataForAttributes(
        metricsData,
        OTEL_RESPONSE_COUNT,
        TEST_METRIC_PREFIX,
        buildResponseCountAttributes(VersionRole.CURRENT, VeniceResponseStatusCategory.SUCCESS));
  }

  @Test
  public void testRecordBytesSentDoesNotAffectOtherMetrics() {
    stats.recordBytesSent(1, 2048);

    Collection<MetricData> metricsData = inMemoryMetricReader.collectAllMetrics();
    Attributes currentAttrs = buildVersionRoleAttributes(VersionRole.CURRENT);

    OpenTelemetryDataTestUtils
        .assertNoLongSumDataForAttributes(metricsData, OTEL_BYTES_RECEIVED, TEST_METRIC_PREFIX, currentAttrs);
    OpenTelemetryDataTestUtils.assertNoLongSumDataForAttributes(
        metricsData,
        OTEL_RESPONSE_COUNT,
        TEST_METRIC_PREFIX,
        buildResponseCountAttributes(VersionRole.CURRENT, VeniceResponseStatusCategory.SUCCESS));
  }

  // --- NPE prevention tests ---

  @Test
  public void testNoNpeWhenOtelDisabled() {
    try (VeniceMetricsRepository disabledRepo = new VeniceMetricsRepository(
        new VeniceMetricsConfig.Builder().setMetricPrefix(TEST_METRIC_PREFIX).setEmitOtelMetrics(false).build())) {
      BlobTransferOtelStats disabledStats = new BlobTransferOtelStats(disabledRepo, TEST_STORE_NAME, TEST_CLUSTER_NAME);
      assertFalse(disabledStats.emitOtelMetrics());
      assertAllMethodsSafe(disabledStats);
    }
  }

  @Test
  public void testNoNpeWhenPlainMetricsRepository() {
    BlobTransferOtelStats plainStats =
        new BlobTransferOtelStats(new MetricsRepository(), TEST_STORE_NAME, TEST_CLUSTER_NAME);
    assertFalse(plainStats.emitOtelMetrics());
    assertAllMethodsSafe(plainStats);
  }

  @Test
  public void testEmitOtelMetricsWhenEnabled() {
    assertTrue(stats.emitOtelMetrics());
  }

  private void assertAllMethodsSafe(BlobTransferOtelStats safeStats) {
    safeStats.updateVersionInfo(1, 2);
    safeStats.recordResponseCount(1, VeniceResponseStatusCategory.SUCCESS);
    safeStats.recordResponseCount(1, VeniceResponseStatusCategory.FAIL);
    safeStats.recordTime(1, 5.0);
    safeStats.recordBytesReceived(1, 1024);
    safeStats.recordBytesSent(1, 2048);
  }

  // --- Helper methods ---

  private Attributes buildVersionRoleAttributes(VersionRole role) {
    return baseAttributesBuilder().put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), role.getDimensionValue())
        .build();
  }

  private Attributes buildResponseCountAttributes(VersionRole role, VeniceResponseStatusCategory status) {
    return baseAttributesBuilder().put(VENICE_VERSION_ROLE.getDimensionNameInDefaultFormat(), role.getDimensionValue())
        .put(VENICE_RESPONSE_STATUS_CODE_CATEGORY.getDimensionNameInDefaultFormat(), status.getDimensionValue())
        .build();
  }

  private AttributesBuilder baseAttributesBuilder() {
    return Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), TEST_STORE_NAME)
        .put(VENICE_CLUSTER_NAME.getDimensionNameInDefaultFormat(), TEST_CLUSTER_NAME);
  }

  private void validateCounter(String metricName, long expectedValue, Attributes expectedAttributes) {
    OpenTelemetryDataTestUtils.validateLongPointDataFromCounter(
        inMemoryMetricReader,
        expectedValue,
        expectedAttributes,
        metricName,
        TEST_METRIC_PREFIX);
  }

}
