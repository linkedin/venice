package com.linkedin.venice.utils.consistency;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DiffValidationUtilsTest {
  @Test
  public void testDoRecordsDiverge() {
    List<Long> firstValueOffsetRecord = new ArrayList<>();
    List<Long> secondValueOffsetRecord = new ArrayList<>();
    List<Long> firstPartitionHighWaterMark = new ArrayList<>();
    List<Long> secondPartitionHighWatermark = new ArrayList<>();

    // metadata isn't populated in both colo's
    Assert.assertFalse(
        DiffValidationUtils.doRecordsDiverge(
            "foo",
            "bar",
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST));

    // metadata isn't populated in first colo
    Collections.addAll(secondPartitionHighWatermark, 10L, 20L, 1500L);
    Collections.addAll(secondValueOffsetRecord, 3L, 0L);
    Assert.assertFalse(
        DiffValidationUtils.doRecordsDiverge(
            "foo",
            "bar",
            Collections.EMPTY_LIST,
            secondPartitionHighWatermark,
            Collections.EMPTY_LIST,
            secondValueOffsetRecord));

    // metadata isn't populated in second colo
    Collections.addAll(firstPartitionHighWaterMark, 10L, 20L, 1500L);
    Collections.addAll(firstValueOffsetRecord, 3L, 0L);
    Assert.assertFalse(
        DiffValidationUtils.doRecordsDiverge(
            "foo",
            "bar",
            firstPartitionHighWaterMark,
            Collections.EMPTY_LIST,
            firstValueOffsetRecord,
            Collections.EMPTY_LIST));

    // values are the same
    Assert.assertFalse(
        DiffValidationUtils.doRecordsDiverge(
            "foo",
            "foo",
            firstPartitionHighWaterMark,
            secondPartitionHighWatermark,
            firstValueOffsetRecord,
            secondValueOffsetRecord));

    // Clean up
    firstPartitionHighWaterMark.clear();
    firstValueOffsetRecord.clear();
    secondPartitionHighWatermark.clear();
    firstPartitionHighWaterMark.clear();

    // first colo is ahead completely
    Collections.addAll(firstPartitionHighWaterMark, 20L, 40L, 1600L);
    Collections.addAll(firstValueOffsetRecord, 20L, 40L);
    Collections.addAll(secondPartitionHighWatermark, 10L, 20L, 1500L);
    Collections.addAll(secondValueOffsetRecord, 3L, 0L);
    Assert.assertFalse(
        DiffValidationUtils.doRecordsDiverge(
            "foo",
            "bar",
            firstPartitionHighWaterMark,
            secondPartitionHighWatermark,
            firstValueOffsetRecord,
            secondValueOffsetRecord));
    firstPartitionHighWaterMark.clear();
    firstValueOffsetRecord.clear();
    secondPartitionHighWatermark.clear();
    secondValueOffsetRecord.clear();

    // second colo is ahead completely
    Collections.addAll(firstPartitionHighWaterMark, 10L, 20L, 1500L);
    Collections.addAll(firstValueOffsetRecord, 3L, 0L);
    Collections.addAll(secondPartitionHighWatermark, 20L, 40L, 1600L);
    Collections.addAll(secondValueOffsetRecord, 20L, 39L);
    Assert.assertFalse(
        DiffValidationUtils.doRecordsDiverge(
            "foo",
            "bar",
            firstPartitionHighWaterMark,
            secondPartitionHighWatermark,
            firstValueOffsetRecord,
            secondValueOffsetRecord));
    firstPartitionHighWaterMark.clear();
    firstValueOffsetRecord.clear();
    secondPartitionHighWatermark.clear();
    secondValueOffsetRecord.clear();

    // fist colo has a lagging colo
    Collections.addAll(firstPartitionHighWaterMark, 10L, 20L, 1500L);
    Collections.addAll(firstValueOffsetRecord, 3L, 0L);
    Collections.addAll(secondPartitionHighWatermark, 10L, 40L, 1500L);
    Collections.addAll(secondValueOffsetRecord, 10L, 25L);
    Assert.assertFalse(
        DiffValidationUtils.doRecordsDiverge(
            "foo",
            "bar",
            firstPartitionHighWaterMark,
            secondPartitionHighWatermark,
            firstValueOffsetRecord,
            secondValueOffsetRecord));
    firstPartitionHighWaterMark.clear();
    firstValueOffsetRecord.clear();
    secondPartitionHighWatermark.clear();
    secondValueOffsetRecord.clear();

    // second colo has a lagging colo
    Collections.addAll(firstPartitionHighWaterMark, 10L, 40L, 1500L);
    Collections.addAll(firstValueOffsetRecord, 3L, 25L);
    Collections.addAll(secondPartitionHighWatermark, 10L, 20L, 1500L);
    Collections.addAll(secondValueOffsetRecord, 10L, 19L);
    Assert.assertFalse(
        DiffValidationUtils.doRecordsDiverge(
            "foo",
            "bar",
            firstPartitionHighWaterMark,
            secondPartitionHighWatermark,
            firstValueOffsetRecord,
            secondValueOffsetRecord));
    firstPartitionHighWaterMark.clear();
    firstValueOffsetRecord.clear();
    secondPartitionHighWatermark.clear();
    secondValueOffsetRecord.clear();

    // records diverge
    Collections.addAll(firstPartitionHighWaterMark, 10L, 40L, 1505L);
    Collections.addAll(firstValueOffsetRecord, 3L, 25L);
    Collections.addAll(secondPartitionHighWatermark, 10L, 40L, 1500L);
    Collections.addAll(secondValueOffsetRecord, 10L, 19L);
    Assert.assertTrue(
        DiffValidationUtils.doRecordsDiverge(
            "foo",
            "bar",
            firstPartitionHighWaterMark,
            secondPartitionHighWatermark,
            firstValueOffsetRecord,
            secondValueOffsetRecord));

  }

  // ── Generic Map-based hasOffsetAdvanced ──────────────────────────────────────

  /**
   * Advanced offset covers base offset for all regions → returns true.
   */
  @Test
  public void testGenericHasOffsetAdvancedReturnsTrue() {
    Assert.assertTrue(DiffValidationUtils.hasOffsetAdvanced(Arrays.asList(5L, 10L), Arrays.asList(10L, 20L)));
  }

  /**
   * Advanced offset is behind base for one region → returns false.
   */
  @Test
  public void testGenericHasOffsetAdvancedReturnsFalseWhenBehind() {
    Assert.assertFalse(DiffValidationUtils.hasOffsetAdvanced(Arrays.asList(5L, 10L), Arrays.asList(10L, 8L)));
  }

  /**
   * Base has more entries than advanced → returns false.
   */
  @Test
  public void testGenericHasOffsetAdvancedReturnsFalseWhenBaseLarger() {
    Assert.assertFalse(DiffValidationUtils.hasOffsetAdvanced(Arrays.asList(5L, 10L, 3L), Arrays.asList(10L, 20L)));
  }

  /**
   * Null in base (region not yet seen) → trivially covered, should not block.
   */
  @Test
  public void testGenericHasOffsetAdvancedNullBaseIsTriviallyCovered() {
    Assert.assertTrue(DiffValidationUtils.hasOffsetAdvanced(Arrays.asList(null, 10L), Arrays.asList(5L, 20L)));
  }

  /**
   * Null in advanced but non-null in base → not yet covered, returns false.
   */
  @Test
  public void testGenericHasOffsetAdvancedNullAdvancedReturnsFalse() {
    Assert.assertFalse(DiffValidationUtils.hasOffsetAdvanced(Arrays.asList(5L, 10L), Arrays.asList(10L, null)));
  }

  /**
   * Partition HW covers the record's OV → record is missing.
   */
  @Test
  public void testGenericIsRecordMissingReturnsTrueWhenCovered() {
    Assert.assertTrue(DiffValidationUtils.isRecordMissing(Arrays.asList(5L, 10L), Arrays.asList(20L, 30L)));
  }

  /**
   * Partition HW does not cover the record's OV → not missing (just lag).
   */
  @Test
  public void testGenericIsRecordMissingReturnsFalseWhenNotCovered() {
    Assert.assertFalse(DiffValidationUtils.isRecordMissing(Arrays.asList(5L, 10L), Arrays.asList(20L, 8L)));
  }
}
