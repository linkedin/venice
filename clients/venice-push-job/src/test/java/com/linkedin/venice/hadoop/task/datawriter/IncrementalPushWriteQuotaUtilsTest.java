package com.linkedin.venice.hadoop.task.datawriter;

import static com.linkedin.venice.hadoop.task.datawriter.IncrementalPushWriteQuotaUtils.THROTTLING_DISABLED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.exceptions.VeniceException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link IncrementalPushWriteQuotaUtils#perPartitionQuotaForPush}, the single driver-side computation
 * that folds in the incremental-push / separate-real-time-topic / disabled decision and the global-quota split.
 */
public class IncrementalPushWriteQuotaUtilsTest {
  // isIncrementalPush, pushToSeparateRT, versionSeparateRT, globalQuota, partitionCount, expectedPerPartition
  @DataProvider(name = "disabledOrSplitCases")
  public Object[][] disabledOrSplitCases() {
    return new Object[][] {
        // Batch push: throttling never applies, even with an otherwise-valid quota.
        { false, false, false, 1000L, 4, THROTTLING_DISABLED },
        // Separate RT (both configs true): skipped even with an otherwise-unsplittable quota.
        { true, true, true, 2L, 4, THROTTLING_DISABLED },
        // Only the push-job separate-RT config: still throttles to the regular RT topic.
        { true, true, false, 1000L, 4, 250L },
        // Only the store separate-RT config: still throttles to the regular RT topic.
        { true, false, true, 1000L, 4, 250L },
        // Regular RT, evenly divisible.
        { true, false, false, 1000L, 4, 250L },
        // Regular RT, non-divisible: floor so the aggregate stays at or below the global quota.
        { true, false, false, 1000L, 3, 333L },
        // Minimum valid quota: exactly one per partition writer.
        { true, false, false, 4L, 4, 1L },
        // Disabled quota sentinels.
        { true, false, false, -1L, 4, THROTTLING_DISABLED }, { true, false, false, 0L, 4, THROTTLING_DISABLED } };
  }

  @Test(dataProvider = "disabledOrSplitCases")
  public void testPerPartitionQuotaForPush(
      boolean isIncrementalPush,
      boolean pushToSeparateRT,
      boolean versionSeparateRT,
      long globalQuota,
      int partitionCount,
      long expectedPerPartition) {
    assertEquals(
        IncrementalPushWriteQuotaUtils.perPartitionQuotaForPush(
            isIncrementalPush,
            pushToSeparateRT,
            versionSeparateRT,
            globalQuota,
            partitionCount),
        expectedPerPartition);
  }

  @Test
  public void testFailsFastWhenQuotaBelowPartitionCount() {
    // 2 records/sec cannot be split across 4 partition writers without giving someone 0/sec -> fail fast.
    assertThrows(
        VeniceException.class,
        () -> IncrementalPushWriteQuotaUtils.perPartitionQuotaForPush(true, false, false, 2L, 4));
  }

  @Test
  public void testFailsFastWhenPartitionCountNonPositive() {
    assertThrows(
        VeniceException.class,
        () -> IncrementalPushWriteQuotaUtils.perPartitionQuotaForPush(true, false, false, 100L, 0));
    assertThrows(
        VeniceException.class,
        () -> IncrementalPushWriteQuotaUtils.perPartitionQuotaForPush(true, false, false, 100L, -3));
  }
}
