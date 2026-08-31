package com.linkedin.venice.hadoop.task.datawriter;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.throttle.VeniceRateLimiter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;


public class ExternalStorageWriteThrottlerTest {
  @Test
  public void throttleAcquiresBothLimitersWithRecordAndByteUnits() {
    RecordingRateLimiter records = new RecordingRateLimiter();
    RecordingRateLimiter bytes = new RecordingRateLimiter();
    ExternalStorageWriteThrottler throttler = new ExternalStorageWriteThrottler(records, bytes);

    throttler.throttle(3, 4096L);

    assertEquals(records.acquired, Arrays.asList(3), "record limiter should be charged the record count");
    assertEquals(bytes.acquired, Arrays.asList(4096), "byte limiter should be charged the byte count");
  }

  @Test
  public void throttleChargesOnlyRecordLimiterWhenByteDimensionDisabled() {
    RecordingRateLimiter records = new RecordingRateLimiter();
    ExternalStorageWriteThrottler throttler = new ExternalStorageWriteThrottler(records, null);

    throttler.throttle(5, 9999L);

    assertEquals(records.acquired, Arrays.asList(5));
  }

  @Test
  public void throttleChargesOnlyByteLimiterWhenRecordDimensionDisabled() {
    RecordingRateLimiter bytes = new RecordingRateLimiter();
    ExternalStorageWriteThrottler throttler = new ExternalStorageWriteThrottler(null, bytes);

    throttler.throttle(5, 256L);

    assertEquals(bytes.acquired, Arrays.asList(256));
  }

  @Test
  public void throttleChargesByteCountAboveIntMaxInChunks() {
    RecordingRateLimiter bytes = new RecordingRateLimiter();
    ExternalStorageWriteThrottler throttler = new ExternalStorageWriteThrottler(null, bytes);

    long byteCount = (long) Integer.MAX_VALUE + 100L;
    throttler.throttle(1, byteCount);

    // Charged in full, split into Integer.MAX_VALUE-sized chunks (no silent clamp / under-charge).
    assertEquals(bytes.acquired, Arrays.asList(Integer.MAX_VALUE, 100), "large byte count charged across chunks");
    long total = 0;
    for (int units: bytes.acquired) {
      total += units;
    }
    assertEquals(total, byteCount, "total permits charged must equal the batch byte count");
  }

  @Test
  public void constructorRejectsBothLimitersNull() {
    assertThrows(IllegalArgumentException.class, () -> new ExternalStorageWriteThrottler(null, null));
  }

  @Test
  public void createReturnsNullWhenBothRatesDisabled() {
    assertNull(ExternalStorageWriteThrottler.create(-1, -1, 4), "both dimensions disabled -> no throttler");
    assertNull(ExternalStorageWriteThrottler.create(0, 0, 4), "zero is also disabled");
  }

  @Test
  public void createSplitsGlobalRecordRateEvenlyAcrossPartitions() {
    ExternalStorageWriteThrottler throttler = ExternalStorageWriteThrottler.create(1000, -1, 4);
    assertNotNull(throttler);
    assertEquals(throttler.getRecordRateLimiter().getQuota(), 250L, "1000/sec across 4 tasks -> 250/sec each");
    assertNull(throttler.getByteRateLimiter(), "byte dimension disabled");
  }

  @Test
  public void createSplitsGlobalByteRateEvenlyAcrossPartitions() {
    ExternalStorageWriteThrottler throttler = ExternalStorageWriteThrottler.create(-1, 8000, 4);
    assertNotNull(throttler);
    assertNull(throttler.getRecordRateLimiter(), "record dimension disabled");
    assertEquals(throttler.getByteRateLimiter().getQuota(), 2000L, "8000/sec across 4 tasks -> 2000/sec each");
  }

  @Test
  public void createSplitsBothDimensionsWhenBothConfigured() {
    ExternalStorageWriteThrottler throttler = ExternalStorageWriteThrottler.create(1000, 8000, 4);
    assertNotNull(throttler);
    assertEquals(throttler.getRecordRateLimiter().getQuota(), 250L);
    assertEquals(throttler.getByteRateLimiter().getQuota(), 2000L);
  }

  @Test
  public void createWithSinglePartitionGivesTaskFullGlobalRate() {
    ExternalStorageWriteThrottler throttler = ExternalStorageWriteThrottler.create(1000, -1, 1);
    assertNotNull(throttler);
    assertEquals(throttler.getRecordRateLimiter().getQuota(), 1000L);
  }

  @Test
  public void createFloorsTheEvenSplitOfTheGlobalRate() {
    // Integer division floors, keeping the aggregate at or below the global budget.
    ExternalStorageWriteThrottler throttler = ExternalStorageWriteThrottler.create(1000, 2000, 3);
    assertNotNull(throttler);
    assertEquals(throttler.getRecordRateLimiter().getQuota(), 333L, "1000/sec across 3 tasks floors to 333/sec each");
    assertEquals(throttler.getByteRateLimiter().getQuota(), 666L, "2000/sec across 3 tasks floors to 666/sec each");
  }

  @Test
  public void validateQuotaPassesWhenSplittable() {
    ExternalStorageWriteThrottler.validateQuota(1000, 8000, 4); // both dimensions split cleanly -> no throw
  }

  @Test
  public void validateQuotaIsNoOpWhenDimensionsDisabled() {
    ExternalStorageWriteThrottler.validateQuota(-1, 0, 4); // nothing configured -> no throw
  }

  @Test
  public void validateQuotaThrowsWhenRecordRateBelowPartitionCount() {
    assertThrows(VeniceException.class, () -> ExternalStorageWriteThrottler.validateQuota(2, -1, 4));
  }

  @Test
  public void validateQuotaThrowsWhenByteRateBelowPartitionCount() {
    assertThrows(VeniceException.class, () -> ExternalStorageWriteThrottler.validateQuota(-1, 3, 4));
  }

  @Test
  public void createRejectsGlobalRecordRateBelowPartitionCount() {
    // 2 records/sec cannot be split across 4 tasks without giving someone 0/sec.
    assertThrows(VeniceException.class, () -> ExternalStorageWriteThrottler.create(2, -1, 4));
  }

  @Test
  public void createRejectsGlobalByteRateBelowPartitionCount() {
    assertThrows(VeniceException.class, () -> ExternalStorageWriteThrottler.create(-1, 3, 4));
  }

  @Test
  public void createRejectsNonPositivePartitionCountWhenADimensionIsEnabled() {
    assertThrows(VeniceException.class, () -> ExternalStorageWriteThrottler.create(1000, -1, 0));
  }

  /**
   * Records the units passed to {@code acquirePermit}/{@code tryAcquirePermit} so tests can assert the
   * throttler charges each dimension the right amount, without depending on real (timing-based) rate limiting.
   */
  private static final class RecordingRateLimiter implements VeniceRateLimiter {
    final List<Integer> acquired = new ArrayList<>();
    private long quota;

    @Override
    public boolean tryAcquirePermit(int units) {
      acquired.add(units);
      return true;
    }

    @Override
    public void acquirePermit(int units) {
      acquired.add(units);
    }

    @Override
    public void setQuota(long quota) {
      this.quota = quota;
    }

    @Override
    public long getQuota() {
      return quota;
    }
  }
}
