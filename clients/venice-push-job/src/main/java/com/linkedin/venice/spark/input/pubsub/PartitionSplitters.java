package com.linkedin.venice.spark.input.pubsub;

import com.linkedin.venice.spark.input.pubsub.table.VenicePubsubInputPartition;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class PartitionSplitters {
  // need a method called fullPartitionSplitter, takes in list of partition start and end offsets
  // and returns a list of VenicePubsubInputPartition splits
  public static Map<Integer, List<List<Long>>> fullPartitionSplitter(Map<Integer, List<Long>> partitionOffsetsMap) {
    Map<Integer, List<List<Long>>> splits = new HashMap<>();
    for (Map.Entry<Integer, List<Long>> entry: partitionOffsetsMap.entrySet()) {
      int partitionNumber = entry.getKey();
      List<Long> offsets = entry.getValue(); // start and end offsets
      List<List<Long>> split = new ArrayList<>();
      split.add(assembleSegment(offsets.get(0), offsets.get(1)));
      splits.put(partitionNumber, split);
    }
    return splits;
  }

  // this one splits the partitions into equal length segments based on the total number of segments intended
  // the final segment count is going to be less than desired count + partition count to accomodate for partial segments
  // and division remainders. This method prioritizes paralellism over segment count.
  public static Map<Integer, List<List<Long>>> segmentCountSplitter(
      Map<Integer, List<Long>> partitionOffsetsMap,
      int totalSegments) {
    Map<Integer, List<List<Long>>> splits = new HashMap<>();
    long intendedSplitLength = computeIntendedSplitLengthBasedOnCount(partitionOffsetsMap, totalSegments);
    for (Map.Entry<Integer, List<Long>> entry: partitionOffsetsMap.entrySet()) {
      int partitionNumber = entry.getKey();
      List<Long> offsets = entry.getValue();
      long startOffset = offsets.get(0);
      long endOffset = offsets.get(1);
      long partitionLength = (endOffset - startOffset);
      if (intendedSplitLength >= partitionLength) {
        // this whole partition fits nicely in one chunk
        List<List<Long>> split = new ArrayList<>();
        split.add(assembleSegment(startOffset, endOffset));
        splits.put(partitionNumber, split); // this partition is going to be consumed by a single task
      } else {
        // this partition needs to be split into multiple segments
        // first lets see how many segments we can get out of this partition
        long count = (long) Math.ceil((double) partitionLength / intendedSplitLength);
        long segmentLength = (long) Math.ceil((double) partitionLength / count);
        List<List<Long>> split = new ArrayList<>();

        // generate the segments
        for (int i = 0; i < count; i++) {
          List<Long> segment = new ArrayList<>();
          segment.add(startOffset + i * segmentLength);
          if (i == count - 1) {
            segment.add(endOffset); // last segment
          } else {
            segment.add(startOffset + (i + 1) * segmentLength - 1);
          }
          split.add(segment);
        }
        splits.put(partitionNumber, split); // Multiple splits for this partition
      }
    }
    return splits;
  }

  // and a method that cuts the partitions into segments based on the total number of messages
  public static Map<Integer, List<List<Long>>> messageCountSplitter(
      Map<Integer, List<Long>> partitionOffsetsMap,
      long chunkOffsetSize) {
    Map<Integer, List<List<Long>>> splits = new HashMap<>();
    for (Map.Entry<Integer, List<Long>> entry: partitionOffsetsMap.entrySet()) {
      int partitionNumber = entry.getKey();
      List<Long> offsets = entry.getValue();
      long startOffset = offsets.get(0);
      long endOffset = offsets.get(1);
      long partitionLength = (endOffset - startOffset);
      if (chunkOffsetSize <= partitionLength) {
        // this whole partition fits nicely in one chunk
        List<List<Long>> split = new ArrayList<>();
        split.add(assembleSegment(startOffset, endOffset));
        splits.put(partitionNumber, split); // this partition is going to be consumed by a single task
      } else {
        // this partition needs to be split into multiple segments
        // first lets see how many segments we can get out of this partition
        long count = (long) Math.ceil((double) partitionLength / chunkOffsetSize);
        long segmentLength = (long) Math.ceil((double) partitionLength / count);
        List<List<Long>> split = new ArrayList<>();

        // generate the segments
        for (int i = 0; i < count; i++) {
          List<Long> segment = new ArrayList<>();
          segment.add(startOffset + i * segmentLength);
          segment.add(startOffset + (i + 1) * segmentLength);
          split.add(segment);
        }
        splits.put(partitionNumber, split); // Multiple splits for this partition
      }
    }
    return splits;
  }

  // assemble and wrap the splits into VenicePubsubInputPartition objects.
  public static List<VenicePubsubInputPartition> convertToInputPartitions(
      String region,
      String topicName,
      Map<Integer, List<List<Long>>> splits) {
    List<VenicePubsubInputPartition> veniceInputPartitions = new ArrayList<>();
    for (Map.Entry<Integer, List<List<Long>>> entry: splits.entrySet()) {
      int partitionNumber = entry.getKey();
      List<List<Long>> segments = entry.getValue();
      for (List<Long> segment: segments) {
        long startOffset = segment.get(0);
        long endOffset = segment.get(1);
        VenicePubsubInputPartition partition =
            new VenicePubsubInputPartition(region, topicName, partitionNumber, startOffset, endOffset);
        veniceInputPartitions.add(partition);
      }
    }
    return veniceInputPartitions;
  }

  // utility methods
  private static List<Long> assembleSegment(long startOffset, long endOffset) {
    List<Long> split;
    List<Long> segment = new ArrayList<>();
    segment.add(startOffset);
    segment.add(endOffset);
    return segment;
  }

  static long computeIntendedSplitLengthBasedOnCount(Map<Integer, List<Long>> partitionOffsetsMap, int totalSegments) {
    Double totalLength = computeTopicLengthInOffsets(partitionOffsetsMap);
    return (long) Math.ceil(totalLength / totalSegments);
  }

  static long computeIntendedSplitCountBasedOnOffset(Map<Integer, List<Long>> partitionOffsetsMap, long offsetCount) {
    Double totalLength = computeTopicLengthInOffsets(partitionOffsetsMap);
    return (long) Math.ceil(totalLength / offsetCount);
  }

  protected static Double computeTopicLengthInOffsets(Map<Integer, List<Long>> partitionOffsetsMap) {
    Double totalLength = 0.0;
    for (Map.Entry<Integer, List<Long>> entry: partitionOffsetsMap.entrySet()) {
      List<Long> offsets = entry.getValue();
      totalLength += offsets.get(1) - offsets.get(0);
    }
    return totalLength;
  }
}
