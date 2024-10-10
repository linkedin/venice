package com.linkedin.venice.spark.input.pubsub;

import static org.testng.Assert.*;

import java.util.*;
import org.testng.annotations.Test;


public class PartitionSplittersTest {
  @Test
  void fullPartitionSplitter_ReturnsCorrectSplits() {
    Map<Integer, List<Long>> partitionOffsetsMap = new HashMap<>();
    partitionOffsetsMap.put(0, Arrays.asList(0L, 100L));
    partitionOffsetsMap.put(1, Arrays.asList(100L, 200L));
    partitionOffsetsMap.put(2, Arrays.asList(0L, 20_000_000L));

    Map<Integer, List<List<Long>>> result = PartitionSplitters.fullPartitionSplitter(partitionOffsetsMap);

    // result is a map of partition number to list of segments
    // need to assert value and keys are correct based on input data
    assertEquals(3, result.size());
    assertEquals(Arrays.asList(Arrays.asList(0L, 100L)), result.get(0));
    assertEquals(Arrays.asList(Arrays.asList(100L, 200L)), result.get(1));
    assertEquals(Arrays.asList(Arrays.asList(0L, 20_000_000L)), result.get(2));

  }

  @Test
  void segmentCountSplitter_SplitsCorrectly_CommonCase() {
    Map<Integer, List<Long>> partitionOffsetsMap = new HashMap<>();
    partitionOffsetsMap.put(0, Arrays.asList(20_000L, 145_408L));
    partitionOffsetsMap.put(1, Arrays.asList(25_000L, 170_067L));
    partitionOffsetsMap.put(2, Arrays.asList(30_000L, 150_022L));
    partitionOffsetsMap.put(3, Arrays.asList(21_000L, 190_031L));
    partitionOffsetsMap.put(4, Arrays.asList(22_000L, 145_343L));

    final int intendedSplitCount = 40;

    Map<Integer, List<List<Long>>> result =
        PartitionSplitters.segmentCountSplitter(partitionOffsetsMap, intendedSplitCount);

    int totalPartitionsPresent = result.size();
    assertEquals(5, totalPartitionsPresent); // right number of partitions present in the result

    // count all the segments
    int totalSegments = 0;
    for (List<List<Long>> segments: result.values()) {
      totalSegments += segments.size();
    }
    assertTrue(intendedSplitCount <= totalSegments); // at least as many segments as intended
    assertTrue(intendedSplitCount + totalPartitionsPresent > totalSegments); // at most intended + partition count

    assertEquals(8, result.get(0).size());
    assertEquals(9, result.get(1).size());

    assertEquals(Arrays.asList(20_000L, 35_675L), result.get(0).get(0)); // spot check first segment
    assertEquals(Arrays.asList(37_904L, 54_807L), result.get(3).get(1)); // spot check middle segments
    assertEquals(Arrays.asList(25_000L, 41_118L), result.get(1).get(0)); // spot check middle segment
    assertEquals(Arrays.asList(129_926L, 145_343L), result.get(4).get(7)); // spot check first segment

  }

  @Test
  void segmentCountSplitter_SplitsCorrectly_EdgeCase1() {
    Map<Integer, List<Long>> partitionOffsetsMap = new HashMap<>();
    partitionOffsetsMap.put(0, Arrays.asList(0L, 150L));
    partitionOffsetsMap.put(3, Arrays.asList(110L, 200L));
    partitionOffsetsMap.put(2, Arrays.asList(50L, 70L));
    partitionOffsetsMap.put(1, Arrays.asList(20L, 23L));
    partitionOffsetsMap.put(5, Arrays.asList(20L, 400L));

    final int intendedSplitCount = 9;

    Map<Integer, List<List<Long>>> result =
        PartitionSplitters.segmentCountSplitter(partitionOffsetsMap, intendedSplitCount);

    int totalPartitionsPresent = result.size();
    assertEquals(5, totalPartitionsPresent); // right number of partitions present in the result

    // count all the segments
    int totalSegments = 0;
    for (List<List<Long>> segments: result.values()) {
      totalSegments += segments.size();
    }
    assertTrue(intendedSplitCount <= totalSegments); // at least as many segments as intended
    assertTrue(intendedSplitCount + totalPartitionsPresent > totalSegments); // at most intended + partition count

    assertEquals(3, result.get(0).size());
    assertEquals(1, result.get(1).size());

    assertEquals(Arrays.asList(155L, 200L), result.get(3).get(1));
    assertEquals(Arrays.asList(20L, 23L), result.get(1).get(0));

  }

  @Test
  void segmentCountSplitter_HandlesSingleSegment() {
    Map<Integer, List<Long>> partitionOffsetsMap = new HashMap<>();
    partitionOffsetsMap.put(0, Arrays.asList(0L, 50L));

    Map<Integer, List<List<Long>>> result = PartitionSplitters.segmentCountSplitter(partitionOffsetsMap, 1);

    assertEquals(1, result.size());
    assertEquals(1, result.get(0).size());
  }

  @Test
  void computeIntendedSplitLengthBasedOnCount_CalculatesCorrectly() {
    Map<Integer, List<Long>> partitionOffsetsMap = new HashMap<>();
    partitionOffsetsMap.put(0, Arrays.asList(0L, 97L));
    partitionOffsetsMap.put(1, Arrays.asList(100L, 903L));
    partitionOffsetsMap.put(2, Arrays.asList(0L, 2022L));

    long result = PartitionSplitters.computeIntendedSplitLengthBasedOnCount(partitionOffsetsMap, 29);

    assertEquals(101L, result);
  }

  @Test
  void computeIntendedSplitCountBasedOnOffset_CalculatesCorrectly() {
    Map<Integer, List<Long>> partitionOffsetsMap = new HashMap<>();
    partitionOffsetsMap.put(0, Arrays.asList(0L, 100L));
    partitionOffsetsMap.put(1, Arrays.asList(100L, 200L));

    long result = PartitionSplitters.computeIntendedSplitCountBasedOnOffset(partitionOffsetsMap, 50L);

    assertEquals(4, result);
  }

  @Test
  void computeTopicLengthInOffsets_CalculatesCorrectly() {
    Map<Integer, List<Long>> partitionOffsetsMap = new HashMap<>();
    partitionOffsetsMap.put(0, Arrays.asList(0L, 101L));
    partitionOffsetsMap.put(1, Arrays.asList(99L, 203L));

    Double result = PartitionSplitters.computeTopicLengthInOffsets(partitionOffsetsMap);

    assertEquals(205D, result);
  }

}
