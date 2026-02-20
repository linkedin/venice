package com.linkedin.venice.meta;

import static com.linkedin.venice.read.RequestType.SINGLE_GET;
import static com.linkedin.venice.stats.RetryManagerMetricEntity.RETRY_RATE_LIMIT_REJECTION_COUNT;
import static com.linkedin.venice.stats.RetryManagerMetricEntity.RETRY_RATE_LIMIT_REMAINING_TOKENS;
import static com.linkedin.venice.stats.RetryManagerMetricEntity.RETRY_RATE_LIMIT_TARGET_TOKENS;
import static com.linkedin.venice.stats.VeniceMetricsRepository.getVeniceMetricsRepository;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_METHOD;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_REQUEST_RETRY_TYPE;
import static com.linkedin.venice.stats.dimensions.VeniceMetricsDimensions.VENICE_STORE_NAME;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateLongPointDataFromCounter;
import static com.linkedin.venice.utils.OpenTelemetryDataTestUtils.validateLongPointDataFromGauge;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.stats.ClientType;
import com.linkedin.venice.stats.RetryManagerStats;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.stats.dimensions.RequestRetryType;
import com.linkedin.venice.utils.TestUtils;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.tehuti.metrics.MetricsRepository;
import java.time.Clock;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;


public class RetryManagerTest {
  private static final long TEST_TIMEOUT_IN_MS = 10000;
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  @AfterClass
  public void cleanUp() {
    scheduler.shutdownNow();
  }

  @Test(timeOut = TEST_TIMEOUT_IN_MS)
  public void testRetryManagerDisabled() {
    Clock mockClock = mock(Clock.class);
    long start = System.currentTimeMillis();
    doReturn(start).when(mockClock).millis();
    MetricsRepository metricsRepository = new MetricsRepository();
    RetryManager retryManager =
        new RetryManager(metricsRepository, "test-retry-manager", "test-store", null, 0, 0.1d, mockClock, scheduler);
    retryManager.recordRequest();
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(retryManager.isRetryAllowed());
    }
    Assert.assertNull(retryManager.getRetryTokenBucket());
    Assert.assertNull(metricsRepository.getMetric(".test-retry-manager--retry_limit_per_seconds.Gauge"));
  }

  @Test(timeOut = TEST_TIMEOUT_IN_MS)
  public void testRetryManager() {
    Clock mockClock = mock(Clock.class);
    long start = System.currentTimeMillis();
    doReturn(start).when(mockClock).millis();
    MetricsRepository metricsRepository = new MetricsRepository();
    RetryManager retryManager =
        new RetryManager(metricsRepository, "test-retry-manager", "test-store", null, 1000, 0.1d, mockClock, scheduler);
    doReturn(start + 1000).when(mockClock).millis();
    for (int i = 0; i < 50; i++) {
      retryManager.recordRequest();
    }
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        () -> Assert.assertNotNull(retryManager.getRetryTokenBucket()));
    // The retry budget should be set to 50 * 0.1 = 5
    // With refill interval of 1s and capacity multiple of 5 that makes the token bucket capacity of 25
    Assert.assertEquals(metricsRepository.getMetric(".test-retry-manager--retry_limit_per_seconds.Gauge").value(), 5d);
    Assert.assertEquals(metricsRepository.getMetric(".test-retry-manager--retries_remaining.Gauge").value(), 25d);
    for (int i = 0; i < 25; i++) {
      Assert.assertTrue(retryManager.isRetryAllowed());
    }
    Assert.assertFalse(retryManager.isRetryAllowed());
    Assert.assertEquals(metricsRepository.getMetric(".test-retry-manager--retries_remaining.Gauge").value(), 0d);
    Assert.assertTrue(metricsRepository.getMetric(".test-retry-manager--rejected_retry.OccurrenceRate").value() > 0);
    doReturn(start + 2001).when(mockClock).millis();
    // We should eventually be able to perform retries again
    TestUtils
        .waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> Assert.assertTrue(retryManager.isRetryAllowed()));

    // Make sure the empty request count kicks in
    doReturn(start + 3001).when(mockClock).millis();
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      verify(mockClock, atLeast(10)).millis();
    });

    // Send more traffic
    doReturn(start + 4001).when(mockClock).millis();
    retryManager.recordRequests(100);
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        () -> Assert.assertTrue(retryManager.isRetryAllowed(40)));
  }

  @Test(timeOut = TEST_TIMEOUT_IN_MS)
  public void testRetryManagerWithMulti() {
    // Verify RetryManager works in low QPS high KPS scenarios
    Clock mockClock = mock(Clock.class);
    long start = System.currentTimeMillis();
    doReturn(start).when(mockClock).millis();
    MetricsRepository metricsRepository = new MetricsRepository();
    RetryManager retryManager =
        new RetryManager(metricsRepository, "test-retry-manager", "test-store", null, 1000, 0.1d, mockClock, scheduler);
    doReturn(start + 1000).when(mockClock).millis();
    int multiKeySize = 500;
    retryManager.recordRequests(multiKeySize);
    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        () -> Assert.assertNotNull(retryManager.getRetryTokenBucket()));
    // Retry budget KPS should be set to 500 * 0.1 = 50
    // Token bucket capacity should be 50 * 5 = 250
    Assert.assertEquals(metricsRepository.getMetric(".test-retry-manager--retry_limit_per_seconds.Gauge").value(), 50d);
    Assert.assertEquals(metricsRepository.getMetric(".test-retry-manager--retries_remaining.Gauge").value(), 250d);
    Assert.assertFalse(retryManager.isRetryAllowed(multiKeySize));
    Assert.assertTrue(retryManager.isRetryAllowed(30));
    Assert.assertEquals(metricsRepository.getMetric(".test-retry-manager--retries_remaining.Gauge").value(), 220d);
    Assert.assertTrue(metricsRepository.getMetric(".test-retry-manager--rejected_retry.OccurrenceRate").value() > 0);
  }

  @Test(timeOut = TEST_TIMEOUT_IN_MS)
  public void testRetryManagerOtelStats() {
    Clock mockClock = mock(Clock.class);
    long start = System.currentTimeMillis();
    doReturn(start).when(mockClock).millis();

    // Create OTel-enabled metrics repository
    InMemoryMetricReader inMemoryMetricReader = InMemoryMetricReader.create();
    VeniceMetricsRepository metricsRepository = getVeniceMetricsRepository(
        ClientType.FAST_CLIENT,
        Arrays.asList(
            RETRY_RATE_LIMIT_REMAINING_TOKENS.getMetricEntity(),
            RETRY_RATE_LIMIT_TARGET_TOKENS.getMetricEntity(),
            RETRY_RATE_LIMIT_REJECTION_COUNT.getMetricEntity()),
        true,
        inMemoryMetricReader);

    RetryManager retryManager =
        new RetryManager(metricsRepository, "test-retry-manager", "test-store", null, 1000, 0.1d, mockClock, scheduler);

    // Create RetryManagerStats with RequestType to enable OTel
    RetryManagerStats retryStats =
        new RetryManagerStats(metricsRepository, "test-retry-manager", "test-store", SINGLE_GET, retryManager);

    doReturn(start + 1000).when(mockClock).millis();

    // Record requests to initialize token bucket
    for (int i = 0; i < 50; i++) {
      retryManager.recordRequest();
    }

    TestUtils.waitForNonDeterministicAssertion(
        5,
        TimeUnit.SECONDS,
        () -> Assert.assertNotNull(retryManager.getRetryTokenBucket()));

    // Expected attributes for OTel metrics
    Attributes expectedAttributes = Attributes.builder()
        .put(VENICE_STORE_NAME.getDimensionNameInDefaultFormat(), "test-store")
        .put(VENICE_REQUEST_METHOD.getDimensionNameInDefaultFormat(), SINGLE_GET.getDimensionValue())
        .put(
            VENICE_REQUEST_RETRY_TYPE.getDimensionNameInDefaultFormat(),
            RequestRetryType.LONG_TAIL_RETRY.getDimensionValue())
        .build();

    // Force OTel metric collection
    inMemoryMetricReader.forceFlush();

    // Test retryLimitPerSecondOtel - should be 50 * 0.1 = 5
    validateLongPointDataFromGauge(
        inMemoryMetricReader,
        5L,
        expectedAttributes,
        RETRY_RATE_LIMIT_TARGET_TOKENS.getMetricEntity().getMetricName(),
        ClientType.FAST_CLIENT.getMetricsPrefix());

    // Test retriesRemainingOtel - should be 5 * 5 = 25 (capacity multiple)
    validateLongPointDataFromGauge(
        inMemoryMetricReader,
        25L,
        expectedAttributes,
        RETRY_RATE_LIMIT_REMAINING_TOKENS.getMetricEntity().getMetricName(),
        ClientType.FAST_CLIENT.getMetricsPrefix());

    // Consume all retries to trigger rejections
    for (int i = 0; i < 25; i++) {
      Assert.assertTrue(retryManager.isRetryAllowed());
    }

    // Record rejected retries
    retryStats.recordRejectedRetry(1);
    retryStats.recordRejectedRetry(2);

    // Force OTel metric collection again
    inMemoryMetricReader.forceFlush();

    // Test rejectedRetrySensorOtel - should be 3 (1 + 2)
    validateLongPointDataFromCounter(
        inMemoryMetricReader,
        3L,
        expectedAttributes,
        RETRY_RATE_LIMIT_REJECTION_COUNT.getMetricEntity().getMetricName(),
        ClientType.FAST_CLIENT.getMetricsPrefix());

    // Verify retries remaining is now 0
    validateLongPointDataFromGauge(
        inMemoryMetricReader,
        0L,
        expectedAttributes,
        RETRY_RATE_LIMIT_REMAINING_TOKENS.getMetricEntity().getMetricName(),
        ClientType.FAST_CLIENT.getMetricsPrefix());
  }
}
