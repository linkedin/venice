package com.linkedin.venice.hadoop.task.datawriter;

import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH_RATE_LIMITER_TYPE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH_WRITE_QUOTA_RECORDS_PER_SECOND;
import static com.linkedin.venice.vpj.VenicePushJobConstants.INCREMENTAL_PUSH_WRITE_QUOTA_TIME_WINDOW_MS;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PARTITION_COUNT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PUSH_TO_SEPARATE_REALTIME_TOPIC;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TELEMETRY_MESSAGE_INTERVAL;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TOPIC_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_SCHEMA_ID_PROP;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.hadoop.engine.EngineTaskConfigProvider;
import com.linkedin.venice.throttle.EventThrottler;
import com.linkedin.venice.throttle.GuavaRateLimiter;
import com.linkedin.venice.throttle.TokenBucket;
import com.linkedin.venice.throttle.VeniceRateLimiter;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import java.util.Properties;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Unit tests for incremental push throttling functionality in AbstractPartitionWriter.
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
  public void testThrottlingDisabledForBatchPush() {
    Properties props = createBaseProperties();
    props.setProperty(INCREMENTAL_PUSH, "false");
    setupMockConfigProvider(props);

    partitionWriter = new TestablePartitionWriter(mockConfigProvider, mockVeniceWriter);
    partitionWriter.configure(mockConfigProvider);

    assertEquals(
        partitionWriter.isIncrementalPushThrottlingEnabled(),
        false,
        "Throttling should not be enabled for batch push");
    assertNull(partitionWriter.getRecordsThrottler(), "Records throttler should not be initialized for batch push");
  }

  @Test
  public void testThrottlingDisabledForSeparateRealtimeTopic() {
    Properties props = createBaseProperties();
    props.setProperty(INCREMENTAL_PUSH, "true");
    props.setProperty(INCREMENTAL_PUSH_WRITE_QUOTA_RECORDS_PER_SECOND, "1000");
    props.setProperty(PUSH_TO_SEPARATE_REALTIME_TOPIC, "true");
    setupMockConfigProvider(props);

    partitionWriter = new TestablePartitionWriter(mockConfigProvider, mockVeniceWriter);
    partitionWriter.configure(mockConfigProvider);

    assertEquals(
        partitionWriter.isIncrementalPushThrottlingEnabled(),
        false,
        "Throttling should not be enabled for separate RT topic");
    assertNull(
        partitionWriter.getRecordsThrottler(),
        "Records throttler should not be initialized for separate RT topic");
  }

  // quota value, expected throttling enabled
  @DataProvider(name = "quotaValues")
  public Object[][] quotaValues() {
    return new Object[][] { { "-1", false }, { "0", false }, { "1", true }, { "1000", true }, { "5000", true } };
  }

  @Test(dataProvider = "quotaValues")
  public void testThrottlingByQuotaValue(String recordsPerSecond, boolean expectedEnabled) {
    Properties props = createBaseProperties();
    props.setProperty(INCREMENTAL_PUSH, "true");
    props.setProperty(INCREMENTAL_PUSH_WRITE_QUOTA_RECORDS_PER_SECOND, recordsPerSecond);
    setupMockConfigProvider(props);

    partitionWriter = new TestablePartitionWriter(mockConfigProvider, mockVeniceWriter);
    partitionWriter.configure(mockConfigProvider);

    assertEquals(
        partitionWriter.isIncrementalPushThrottlingEnabled(),
        expectedEnabled,
        "Throttling enabled mismatch for recordsPerSecond=" + recordsPerSecond);
    if (expectedEnabled) {
      assertNotNull(
          partitionWriter.getRecordsThrottler(),
          "Throttler should be initialized for quota " + recordsPerSecond);
    } else {
      assertNull(
          partitionWriter.getRecordsThrottler(),
          "Throttler should not be initialized for quota " + recordsPerSecond);
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
    props.setProperty(INCREMENTAL_PUSH_WRITE_QUOTA_RECORDS_PER_SECOND, "1000");
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
    props.setProperty(INCREMENTAL_PUSH_WRITE_QUOTA_RECORDS_PER_SECOND, "100000"); // Very high quota
    setupMockConfigProvider(props);

    partitionWriter = new TestablePartitionWriter(mockConfigProvider, mockVeniceWriter);
    partitionWriter.configure(mockConfigProvider);

    assertTrue(partitionWriter.isIncrementalPushThrottlingEnabled(), "Throttling should be enabled");

    long startTime = System.currentTimeMillis();
    DataWriterTaskTracker mockTracker = mock(DataWriterTaskTracker.class);
    for (int i = 0; i < 10; i++) {
      partitionWriter.invokeThrottleForTesting(mockTracker);
    }
    long elapsed = System.currentTimeMillis() - startTime;

    assertTrue(elapsed < 200, "Messages should not be throttled when below quota, elapsed: " + elapsed + "ms");
  }

  @Test
  public void testTokenBucketWithSubSecondTimeWindow() {
    Properties props = createBaseProperties();
    props.setProperty(INCREMENTAL_PUSH, "true");
    props.setProperty(INCREMENTAL_PUSH_WRITE_QUOTA_RECORDS_PER_SECOND, "1000");
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
