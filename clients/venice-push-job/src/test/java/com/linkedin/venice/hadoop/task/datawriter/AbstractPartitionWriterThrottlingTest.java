package com.linkedin.venice.hadoop.task.datawriter;

import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH_RATE_LIMITER_TYPE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH_WRITE_QUOTA_RECORDS_PER_SECOND_PER_PARTITION;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH_WRITE_QUOTA_TIME_WINDOW_MS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PARTITION_COUNT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TELEMETRY_MESSAGE_INTERVAL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TOPIC_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_SCHEMA_ID_PROP;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.engine.EngineTaskConfigProvider;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.throttle.GuavaRateLimiter;
import com.linkedin.venice.throttle.TokenBucket;
import com.linkedin.venice.throttle.VeniceRateLimiter;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Unit tests for throttling functionality in AbstractPartitionWriter: incremental-push write-quota
 * throttling and external-storage dual-write per-region throttler wiring.
 */
public class AbstractPartitionWriterThrottlingTest {
  private TestablePartitionWriter partitionWriter;
  private AbstractVeniceWriter<byte[], byte[], byte[]> mockVeniceWriter;
  private EngineTaskConfigProvider mockConfigProvider;

  @BeforeMethod
  public void setUp() {
    mockVeniceWriter = mock(AbstractVeniceWriter.class);
    mockConfigProvider = mock(EngineTaskConfigProvider.class);

    // Setup mock config provider with required properties
    Properties jobProps = new Properties();
    jobProps.setProperty(PARTITION_COUNT, "1");
    when(mockConfigProvider.getJobProps()).thenReturn(jobProps);
  }

  @Test
  public void testThrottlingDisabledWhenNoPerPartitionQuotaForwarded() {
    // Batch pushes and separate-RT pushes lead the driver to forward a disabled (<= 0) per-partition quota.
    Properties props = createBaseProperties();
    props.setProperty(INCREMENTAL_PUSH, "false");
    setupMockConfigProvider(props);

    partitionWriter = new TestablePartitionWriter(mockConfigProvider, mockVeniceWriter);
    partitionWriter.configure(mockConfigProvider);

    assertEquals(
        partitionWriter.isIncrementalPushThrottlingEnabled(),
        false,
        "Throttling should not be enabled when no per-partition quota is forwarded");
    assertNull(partitionWriter.getRecordsThrottler(), "Records throttler should not be initialized");
  }

  // forwarded per-partition quota, expected throttling enabled
  @DataProvider(name = "perPartitionQuotaValues")
  public Object[][] perPartitionQuotaValues() {
    return new Object[][] { { "-1", false }, { "0", false }, { "1", true }, { "250", true }, { "1000", true } };
  }

  @Test(dataProvider = "perPartitionQuotaValues")
  public void testThrottlingByPerPartitionQuota(String perPartitionQuota, boolean expectedEnabled) {
    Properties props = createBaseProperties();
    props.setProperty(INCREMENTAL_PUSH, "true");
    props.setProperty(INCREMENTAL_PUSH_WRITE_QUOTA_RECORDS_PER_SECOND_PER_PARTITION, perPartitionQuota);
    setupMockConfigProvider(props);

    partitionWriter = new TestablePartitionWriter(mockConfigProvider, mockVeniceWriter);
    partitionWriter.configure(mockConfigProvider);

    assertEquals(
        partitionWriter.isIncrementalPushThrottlingEnabled(),
        expectedEnabled,
        "Throttling enabled mismatch for perPartitionQuota=" + perPartitionQuota);
    if (expectedEnabled) {
      assertNotNull(
          partitionWriter.getRecordsThrottler(),
          "Throttler should be initialized for quota " + perPartitionQuota);
      assertEquals(
          partitionWriter.getRecordsThrottler().getQuota(),
          Long.parseLong(perPartitionQuota),
          "Throttler should enforce the forwarded per-partition quota directly (no re-derivation)");
    } else {
      assertNull(
          partitionWriter.getRecordsThrottler(),
          "Throttler should not be initialized for quota " + perPartitionQuota);
    }
  }

  // rate limiter type config, expected throttler class
  @DataProvider(name = "rateLimiterTypes")
  public Object[][] rateLimiterTypes() {
    return new Object[][] { { null, GuavaRateLimiter.class }, // default
        { "GUAVA_RATE_LIMITER", GuavaRateLimiter.class },
        { "EVENT_THROTTLER_WITH_SILENT_REJECTION", EventThrottler.class },
        { "TOKEN_BUCKET_INCREMENTAL_REFILL", TokenBucket.class }, { "INVALID_TYPE", GuavaRateLimiter.class }, // fallback
    };
  }

  @Test(dataProvider = "rateLimiterTypes")
  public void testRateLimiterTypeSelection(String rateLimiterType, Class<?> expectedClass) {
    Properties props = createBaseProperties();
    props.setProperty(INCREMENTAL_PUSH, "true");
    props.setProperty(INCREMENTAL_PUSH_WRITE_QUOTA_RECORDS_PER_SECOND_PER_PARTITION, "1000");
    if (rateLimiterType != null) {
      props.setProperty(INCREMENTAL_PUSH_RATE_LIMITER_TYPE, rateLimiterType);
    }
    setupMockConfigProvider(props);

    partitionWriter = new TestablePartitionWriter(mockConfigProvider, mockVeniceWriter);
    partitionWriter.configure(mockConfigProvider);

    VeniceRateLimiter throttler = partitionWriter.getRecordsThrottler();
    assertNotNull(throttler, "Records throttler should be initialized");
    assertTrue(
        expectedClass.isInstance(throttler),
        "Expected " + expectedClass.getSimpleName() + " but got " + throttler.getClass().getSimpleName() + " for type="
            + rateLimiterType);
  }

  @Test
  public void testThrottlingDoesNotBlockBelowQuota() {
    Properties props = createBaseProperties();
    props.setProperty(INCREMENTAL_PUSH, "true");
    props.setProperty(INCREMENTAL_PUSH_WRITE_QUOTA_RECORDS_PER_SECOND_PER_PARTITION, "100000"); // Very high quota
    setupMockConfigProvider(props);

    partitionWriter = new TestablePartitionWriter(mockConfigProvider, mockVeniceWriter);
    partitionWriter.configure(mockConfigProvider);

    assertTrue(partitionWriter.isIncrementalPushThrottlingEnabled(), "Throttling should be enabled");

    DataWriterTaskTracker mockTracker = mock(DataWriterTaskTracker.class);
    // Warm up to avoid JIT compilation overhead in measurement
    for (int i = 0; i < 10; i++) {
      partitionWriter.invokeThrottleForTesting(mockTracker);
    }
    long startTime = System.nanoTime();
    for (int i = 0; i < 10; i++) {
      partitionWriter.invokeThrottleForTesting(mockTracker);
    }
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);

    assertTrue(elapsedMs < 400, "Messages should not be throttled when below quota, elapsed: " + elapsedMs + "ms");
  }

  @Test
  public void testTokenBucketWithSubSecondTimeWindow() {
    Properties props = createBaseProperties();
    props.setProperty(INCREMENTAL_PUSH, "true");
    props.setProperty(INCREMENTAL_PUSH_WRITE_QUOTA_RECORDS_PER_SECOND_PER_PARTITION, "1000");
    props.setProperty(INCREMENTAL_PUSH_RATE_LIMITER_TYPE, "TOKEN_BUCKET_INCREMENTAL_REFILL");
    props.setProperty(INCREMENTAL_PUSH_WRITE_QUOTA_TIME_WINDOW_MS, "100");
    setupMockConfigProvider(props);

    partitionWriter = new TestablePartitionWriter(mockConfigProvider, mockVeniceWriter);
    partitionWriter.configure(mockConfigProvider);

    VeniceRateLimiter throttler = partitionWriter.getRecordsThrottler();
    assertNotNull(throttler, "Records throttler should be initialized");
    assertTrue(throttler instanceof TokenBucket, "Should be TokenBucket");
    // With 1000 rec/s and 100ms window, refillAmount should be ~100 (not 1000).
    // Verify the effective rate: refillAmount / enforcementInterval * 1000 should approximate 1000 rec/s.
    TokenBucket tokenBucket = (TokenBucket) throttler;
    long effectiveRatePerSec = tokenBucket.getRefillAmount() * 1000 / tokenBucket.getEnforcementInterval();
    assertTrue(
        effectiveRatePerSec <= 1100 && effectiveRatePerSec >= 900,
        "TokenBucket effective rate should be ~1000 rec/s, got: " + effectiveRatePerSec);
  }

  @Test
  public void testThrottlingDisabledForZeroQuota() {
    Properties props = createBaseProperties();
    props.setProperty(INCREMENTAL_PUSH, "true");
    props.setProperty(INCREMENTAL_PUSH_WRITE_QUOTA_RECORDS_PER_SECOND_PER_PARTITION, "0");
    setupMockConfigProvider(props);

    partitionWriter = new TestablePartitionWriter(mockConfigProvider, mockVeniceWriter);
    partitionWriter.configure(mockConfigProvider);

    assertNull(partitionWriter.getRecordsThrottler(), "Throttler should not be created for quota=0");
  }

  @Test
  public void testThrottlingDisabledForNegativeQuota() {
    Properties props = createBaseProperties();
    props.setProperty(INCREMENTAL_PUSH, "true");
    props.setProperty(INCREMENTAL_PUSH_WRITE_QUOTA_RECORDS_PER_SECOND_PER_PARTITION, "-1");
    setupMockConfigProvider(props);

    partitionWriter = new TestablePartitionWriter(mockConfigProvider, mockVeniceWriter);
    partitionWriter.configure(mockConfigProvider);

    assertNull(partitionWriter.getRecordsThrottler(), "Throttler should not be created for quota=-1");
  }

  @Test
  public void testMinimumValidQuota() {
    Properties props = createBaseProperties();
    props.setProperty(INCREMENTAL_PUSH, "true");
    props.setProperty(INCREMENTAL_PUSH_WRITE_QUOTA_RECORDS_PER_SECOND_PER_PARTITION, "1");
    setupMockConfigProvider(props);

    partitionWriter = new TestablePartitionWriter(mockConfigProvider, mockVeniceWriter);
    partitionWriter.configure(mockConfigProvider);

    assertNotNull(partitionWriter.getRecordsThrottler(), "Throttler should be created for quota=1");
    assertTrue(partitionWriter.isIncrementalPushThrottlingEnabled(), "Throttling should be enabled for quota=1");
  }

  @Test
  public void testInvalidRateLimiterTypeFallsBackToGuava() {
    Properties props = createBaseProperties();
    props.setProperty(INCREMENTAL_PUSH, "true");
    props.setProperty(INCREMENTAL_PUSH_WRITE_QUOTA_RECORDS_PER_SECOND_PER_PARTITION, "1000");
    props.setProperty(INCREMENTAL_PUSH_RATE_LIMITER_TYPE, "COMPLETELY_BOGUS_TYPE");
    setupMockConfigProvider(props);

    partitionWriter = new TestablePartitionWriter(mockConfigProvider, mockVeniceWriter);
    partitionWriter.configure(mockConfigProvider);

    VeniceRateLimiter throttler = partitionWriter.getRecordsThrottler();
    assertNotNull(throttler, "Throttler should be created with fallback");
    assertTrue(
        throttler instanceof GuavaRateLimiter,
        "Invalid type should fall back to GuavaRateLimiter, got: " + throttler.getClass().getSimpleName());
  }

  @Test
  public void testTokenBucketWithInvalidTimeWindowFallsBackToDefault() {
    Properties props = createBaseProperties();
    props.setProperty(INCREMENTAL_PUSH, "true");
    props.setProperty(INCREMENTAL_PUSH_WRITE_QUOTA_RECORDS_PER_SECOND_PER_PARTITION, "1000");
    props.setProperty(INCREMENTAL_PUSH_RATE_LIMITER_TYPE, "TOKEN_BUCKET_INCREMENTAL_REFILL");
    props.setProperty(INCREMENTAL_PUSH_WRITE_QUOTA_TIME_WINDOW_MS, "0");
    setupMockConfigProvider(props);

    partitionWriter = new TestablePartitionWriter(mockConfigProvider, mockVeniceWriter);
    partitionWriter.configure(mockConfigProvider);

    VeniceRateLimiter throttler = partitionWriter.getRecordsThrottler();
    assertNotNull(throttler, "Throttler should be created despite invalid time window");
    assertTrue(throttler instanceof TokenBucket, "Should still be TokenBucket with fallback time window");
  }

  // --- External-storage dual-write throttler wiring -----------------------------------------------

  @Test
  public void testExternalStorageThrottlersDisabledWhenNoQuotaConfigured() {
    Properties props = createBaseProperties();
    setupMockConfigProvider(props);
    partitionWriter = new TestablePartitionWriter(mockConfigProvider, mockVeniceWriter);
    partitionWriter.configure(mockConfigProvider);

    assertNull(
        partitionWriter.buildExternalStorageThrottlers(-1, -1, 2),
        "No external-storage throttlers when neither record nor byte quota is configured");
  }

  @Test
  public void testExternalStorageThrottlersOnePerRegionSplitAcrossPartitions() {
    Properties props = createBaseProperties();
    props.setProperty(PARTITION_COUNT, "4");
    setupMockConfigProvider(props);
    partitionWriter = new TestablePartitionWriter(mockConfigProvider, mockVeniceWriter);
    partitionWriter.configure(mockConfigProvider);

    List<ExternalStorageWriteThrottler> throttlers = partitionWriter.buildExternalStorageThrottlers(1000, 8000, 3);
    assertNotNull(throttlers, "Throttlers should be built when a quota is configured");
    assertEquals(throttlers.size(), 3, "One throttler per region");
    for (int i = 0; i < throttlers.size(); i++) {
      assertNotNull(throttlers.get(i), "Region " + i + " should have a throttler");
      assertEquals(
          throttlers.get(i).getRecordRateLimiter().getQuota(),
          250L,
          "1000 records/sec split across 4 partition-writer tasks -> 250/sec each");
      assertEquals(
          throttlers.get(i).getByteRateLimiter().getQuota(),
          2000L,
          "8000 bytes/sec split across 4 partition-writer tasks -> 2000/sec each");
    }
    // Independent instances per region so each region keeps its full per-region budget (separate buckets).
    assertTrue(throttlers.get(0) != throttlers.get(1), "Per-region throttler instances must be independent");
    assertTrue(throttlers.get(1) != throttlers.get(2), "Per-region throttler instances must be independent");
  }

  @Test
  public void testExternalStorageThrottlersWithOnlyByteQuotaConfigured() {
    Properties props = createBaseProperties();
    props.setProperty(PARTITION_COUNT, "2");
    setupMockConfigProvider(props);
    partitionWriter = new TestablePartitionWriter(mockConfigProvider, mockVeniceWriter);
    partitionWriter.configure(mockConfigProvider);

    List<ExternalStorageWriteThrottler> throttlers = partitionWriter.buildExternalStorageThrottlers(-1, 8000, 2);
    assertNotNull(throttlers);
    assertEquals(throttlers.size(), 2);
    assertNull(throttlers.get(0).getRecordRateLimiter(), "Record dimension disabled");
    assertEquals(throttlers.get(0).getByteRateLimiter().getQuota(), 4000L, "8000/sec across 2 tasks -> 4000/sec");
  }

  @Test
  public void testExternalStorageThrottlersFailFastWhenQuotaBelowPartitionCount() {
    Properties props = createBaseProperties();
    props.setProperty(PARTITION_COUNT, "4");
    setupMockConfigProvider(props);
    partitionWriter = new TestablePartitionWriter(mockConfigProvider, mockVeniceWriter);
    partitionWriter.configure(mockConfigProvider);

    // 2 records/sec cannot be split across 4 tasks without giving someone 0/sec -> fail fast.
    assertThrows(VeniceException.class, () -> partitionWriter.buildExternalStorageThrottlers(2, -1, 3));
  }

  private void setupMockConfigProvider(Properties props) {
    when(mockConfigProvider.getJobProps()).thenReturn(props);
  }

  private Properties createBaseProperties() {
    Properties props = new Properties();
    props.setProperty(VALUE_SCHEMA_ID_PROP, "1");
    props.setProperty(TELEMETRY_MESSAGE_INTERVAL, "10000");
    props.setProperty(PARTITION_COUNT, "1");
    props.setProperty(TOPIC_PROP, "testStore_v1");
    return props;
  }

  /**
   * Testable subclass of AbstractPartitionWriter that exposes internal state for testing
   * via the @VisibleForTesting methods on the parent class.
   */
  private static class TestablePartitionWriter extends AbstractPartitionWriter {
    private final EngineTaskConfigProvider configProvider;
    private final AbstractVeniceWriter<byte[], byte[], byte[]> veniceWriter;

    public TestablePartitionWriter(
        EngineTaskConfigProvider configProvider,
        AbstractVeniceWriter<byte[], byte[], byte[]> veniceWriter) {
      this.configProvider = configProvider;
      this.veniceWriter = veniceWriter;
    }

    @Override
    protected EngineTaskConfigProvider getEngineTaskConfigProvider() {
      return configProvider;
    }

    @Override
    protected AbstractVeniceWriter<byte[], byte[], byte[]> createBasicVeniceWriter() {
      return veniceWriter;
    }
  }
}
