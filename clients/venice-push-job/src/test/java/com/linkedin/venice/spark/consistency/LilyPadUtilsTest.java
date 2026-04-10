package com.linkedin.venice.spark.consistency;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


public class LilyPadUtilsTest {
  /**
   * The core scenario: two DCs both had full information (each DC's high-watermark covers
   * the other DC's per-key offset vector) yet ended up with different values.
   *
   * <p>DC-0: wolf valueHash=100, OV=[5,10], HW=[50,60], logicalTs=200
   * <p>DC-1: wolf valueHash=200, OV=[10,15], HW=[20,30], logicalTs=180
   *
   * <p>Both records are comparable → value mismatch is a real inconsistency.
   * hawk has matching value hashes → should NOT appear as inconsistency.
   */
  @Test
  public void testFindInconsistenciesDetectsValueMismatchWhenBothDCsHadFullInfo() {
    long wolfKey = 1L;
    long hawkKey = 2L;

    LilyPadUtils.KeyRecord<Long> dc0Wolf =
        new LilyPadUtils.KeyRecord<>(100, Arrays.asList(5L, 10L), Arrays.asList(50L, 60L), 200L, 42L);
    LilyPadUtils.KeyRecord<Long> dc1Wolf =
        new LilyPadUtils.KeyRecord<>(200, Arrays.asList(10L, 15L), Arrays.asList(20L, 30L), 180L, 17L);

    LilyPadUtils.KeyRecord<Long> dc0Hawk =
        new LilyPadUtils.KeyRecord<>(999, Arrays.asList(3L, 7L), Arrays.asList(50L, 60L), 100L, 10L);
    LilyPadUtils.KeyRecord<Long> dc1Hawk =
        new LilyPadUtils.KeyRecord<>(999, Arrays.asList(3L, 7L), Arrays.asList(20L, 30L), 100L, 5L);

    Map<Long, List<LilyPadUtils.KeyRecord<Long>>> dc0Map = new HashMap<>();
    dc0Map.put(wolfKey, Collections.singletonList(dc0Wolf));
    dc0Map.put(hawkKey, Collections.singletonList(dc0Hawk));
    Map<Long, List<LilyPadUtils.KeyRecord<Long>>> dc1Map = new HashMap<>();
    dc1Map.put(wolfKey, Collections.singletonList(dc1Wolf));
    dc1Map.put(hawkKey, Collections.singletonList(dc1Hawk));

    LilyPadUtils.Snapshot<Long> dc0Snapshot = new LilyPadUtils.Snapshot<>(dc0Map, dc0Wolf.highWatermark);
    LilyPadUtils.Snapshot<Long> dc1Snapshot = new LilyPadUtils.Snapshot<>(dc1Map, dc1Wolf.highWatermark);

    List<LilyPadUtils.Inconsistency<Long>> result = LilyPadUtils.findInconsistencies(dc0Snapshot, dc1Snapshot);

    assertEquals(result.size(), 1, "Expected exactly one inconsistency (hawk should match, only wolf diverges)");
    LilyPadUtils.Inconsistency<Long> inc = result.get(0);
    assertEquals(inc.type, LilyPadUtils.InconsistencyType.VALUE_MISMATCH);
    assertEquals(inc.keyHash, wolfKey);
    assertEquals(inc.dc0Record.valueHash, Integer.valueOf(100));
    assertEquals(inc.dc1Record.valueHash, Integer.valueOf(200));
    assertTrue(
        inc.dc0Record.logicalTimestamp > inc.dc1Record.logicalTimestamp,
        "DC-0 should have the higher logicalTimestamp (the correct winner)");
  }

  /**
   * PUT in DC-0 vs DELETE (tombstone) in DC-1 for the same key. Both DCs had full info
   * (HWs cover each other's OVs), so this is a real inconsistency: one DC has a value,
   * the other deleted it. Objects.equals(100, null) = false → VALUE_MISMATCH.
   */
  @Test
  public void testFindInconsistencyForKeyDetectsPutVsDeleteMismatch() {
    long wolfKey = 1L;

    LilyPadUtils.KeyRecord<Long> dc0Wolf =
        new LilyPadUtils.KeyRecord<>(100, Arrays.asList(5L, 10L), Arrays.asList(50L, 60L), 200L, 42L);
    LilyPadUtils.KeyRecord<Long> dc1Wolf =
        new LilyPadUtils.KeyRecord<>(null, Arrays.asList(10L, 15L), Arrays.asList(20L, 30L), 180L, 17L);

    Map<Long, List<LilyPadUtils.KeyRecord<Long>>> dc0Map = new HashMap<>();
    dc0Map.put(wolfKey, Collections.singletonList(dc0Wolf));
    Map<Long, List<LilyPadUtils.KeyRecord<Long>>> dc1Map = new HashMap<>();
    dc1Map.put(wolfKey, Collections.singletonList(dc1Wolf));

    LilyPadUtils.Snapshot<Long> dc0Snapshot = new LilyPadUtils.Snapshot<>(dc0Map, dc0Wolf.highWatermark);
    LilyPadUtils.Snapshot<Long> dc1Snapshot = new LilyPadUtils.Snapshot<>(dc1Map, dc1Wolf.highWatermark);

    List<LilyPadUtils.Inconsistency<Long>> result = LilyPadUtils.findInconsistencies(dc0Snapshot, dc1Snapshot);

    assertEquals(result.size(), 1);
    LilyPadUtils.Inconsistency<Long> inc = result.get(0);
    assertEquals(inc.type, LilyPadUtils.InconsistencyType.VALUE_MISMATCH);
    assertEquals(inc.dc0Record.valueHash, Integer.valueOf(100));
    assertNull(inc.dc1Record.valueHash, "DELETE should have null valueHash");
  }

  /**
   * Replication lag should NOT be reported as an inconsistency.
   * DC-1's HW[1]=8 < DC-0's OV[1]=10 → DC-1 hadn't seen DC-0's colo-1 offset yet.
   */
  @Test
  public void testFindInconsistenciesSkipsRecordsWhenOneDCHadIncompleteInfo() {
    long wolfKey = 1L;

    LilyPadUtils.KeyRecord<Long> dc0Wolf =
        new LilyPadUtils.KeyRecord<>(100, Arrays.asList(5L, 10L), Arrays.asList(50L, 60L), 200L, 42L);
    LilyPadUtils.KeyRecord<Long> dc1Wolf =
        new LilyPadUtils.KeyRecord<>(200, Arrays.asList(5L, 3L), Arrays.asList(20L, 8L), 180L, 17L);

    Map<Long, List<LilyPadUtils.KeyRecord<Long>>> dc0Map = new HashMap<>();
    dc0Map.put(wolfKey, Collections.singletonList(dc0Wolf));
    Map<Long, List<LilyPadUtils.KeyRecord<Long>>> dc1Map = new HashMap<>();
    dc1Map.put(wolfKey, Collections.singletonList(dc1Wolf));

    LilyPadUtils.Snapshot<Long> dc0Snapshot = new LilyPadUtils.Snapshot<>(dc0Map, dc0Wolf.highWatermark);
    LilyPadUtils.Snapshot<Long> dc1Snapshot = new LilyPadUtils.Snapshot<>(dc1Map, dc1Wolf.highWatermark);

    List<LilyPadUtils.Inconsistency<Long>> result = LilyPadUtils.findInconsistencies(dc0Snapshot, dc1Snapshot);

    assertTrue(
        result.isEmpty(),
        "Replication lag (DC-1's HW didn't cover DC-0's offset) must not be reported as an inconsistency");
  }

  /**
   * MISSING_IN_DC0: key exists in DC-1 but not DC-0, and DC-0's partition HW covers
   * DC-1's per-key OV → genuine missing key.
   */
  @Test
  public void testFindInconsistencyForKeyDetectsMissingInDC0() {
    long wolfKey = 1L;
    LilyPadUtils.KeyRecord<Long> dc1Rec =
        new LilyPadUtils.KeyRecord<>(200, Arrays.asList(5L, 10L), Arrays.asList(20L, 30L), 180L, 17L);

    List<Long> dc0PartitionHW = Arrays.asList(100L, 100L);

    LilyPadUtils.Inconsistency<Long> inc = LilyPadUtils.findInconsistencyForKey(
        wolfKey,
        Collections.emptyList(),
        Collections.singletonList(dc1Rec),
        dc0PartitionHW,
        dc1Rec.highWatermark,
        new long[2]);

    assertEquals(inc.type, LilyPadUtils.InconsistencyType.MISSING_IN_DC0);
    assertEquals(inc.keyHash, wolfKey);
    assertNull(inc.dc0Record);
    assertEquals(inc.dc1Record.valueHash, Integer.valueOf(200));
  }

  /**
   * MISSING_IN_DC1: key exists in DC-0 but not DC-1, and DC-1's partition HW covers
   * DC-0's per-key OV → genuine missing key.
   */
  @Test
  public void testFindInconsistencyForKeyDetectsMissingInDC1() {
    long wolfKey = 1L;
    LilyPadUtils.KeyRecord<Long> dc0Rec =
        new LilyPadUtils.KeyRecord<>(100, Arrays.asList(5L, 10L), Arrays.asList(50L, 60L), 200L, 42L);

    List<Long> dc1PartitionHW = Arrays.asList(100L, 100L);

    LilyPadUtils.Inconsistency<Long> inc = LilyPadUtils.findInconsistencyForKey(
        wolfKey,
        Collections.singletonList(dc0Rec),
        Collections.emptyList(),
        dc0Rec.highWatermark,
        dc1PartitionHW,
        new long[2]);

    assertEquals(inc.type, LilyPadUtils.InconsistencyType.MISSING_IN_DC1);
    assertEquals(inc.keyHash, wolfKey);
    assertEquals(inc.dc0Record.valueHash, Integer.valueOf(100));
    assertNull(inc.dc1Record);
  }

  /**
   * Key missing in DC-0 but DC-0's partition HW doesn't cover DC-1's OV → replication lag,
   * not a real missing key. Should return null.
   */
  @Test
  public void testFindInconsistencyForKeySkipsMissingWhenHWDoesNotCover() {
    long wolfKey = 1L;
    LilyPadUtils.KeyRecord<Long> dc1Rec =
        new LilyPadUtils.KeyRecord<>(200, Arrays.asList(5L, 10L), Arrays.asList(20L, 30L), 180L, 17L);

    List<Long> dc0PartitionHW = Arrays.asList(2L, 3L);

    LilyPadUtils.Inconsistency<Long> inc = LilyPadUtils.findInconsistencyForKey(
        wolfKey,
        Collections.emptyList(),
        Collections.singletonList(dc1Rec),
        dc0PartitionHW,
        dc1Rec.highWatermark,
        new long[2]);

    assertEquals(inc, null, "Missing key with lagging HW must not be reported");
  }
}
