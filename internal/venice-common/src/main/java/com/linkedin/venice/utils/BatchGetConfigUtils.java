package com.linkedin.venice.utils;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;


public class BatchGetConfigUtils {
  /**
   * The expected config format is like the following:
   * "1-10:20,11-50:50,51-200:80,201-:1000"
   */
  public static TreeMap<Integer, Integer> parseRetryThresholdForBatchGet(String retryThresholdStr) {
    final String retryThresholdListSeparator = ",\\s*";
    final String retryThresholdSeparator = ":\\s*";
    final String keyRangeSeparator = "-\\s*";
    String[] retryThresholds = retryThresholdStr.split(retryThresholdListSeparator);
    List<String> retryThresholdList = Arrays.asList(retryThresholds);
    // Sort by the lower bound of the key ranges.
    retryThresholdList.sort((range1, range2) -> {
      // Find the lower bound of the key ranges
      String[] keyRange1 = range1.split(keyRangeSeparator);
      String[] keyRange2 = range2.split(keyRangeSeparator);
      if (keyRange1.length != 2) {
        throw new VeniceException(
            "Invalid single retry threshold config: " + range1 + ", which contains two parts separated by '"
                + keyRangeSeparator + "'");
      }
      if (keyRange2.length != 2) {
        throw new VeniceException(
            "Invalid single retry threshold config: " + range2 + ", which should contain two parts separated by '"
                + keyRangeSeparator + "'");
      }
      return Integer.parseInt(keyRange1[0]) - Integer.parseInt(keyRange2[0]);
    });
    TreeMap<Integer, Integer> retryThresholdMap = new TreeMap<>();
    // Check whether the key ranges are continuous, and store the mapping if everything is good
    int previousUpperBound = 0;
    final int MAX_KEY_COUNT = Integer.MAX_VALUE;
    for (String singleRetryThreshold: retryThresholdList) {
      // parse the range and retry threshold
      String[] singleRetryThresholdParts = singleRetryThreshold.split(retryThresholdSeparator);
      if (singleRetryThresholdParts.length != 2) {
        throw new VeniceException(
            "Invalid single retry threshold config: " + singleRetryThreshold + ", which"
                + " should contain two parts separated by '" + retryThresholdSeparator + "'");
      }
      Integer threshold = Integer.parseInt(singleRetryThresholdParts[1]);
      String[] keyCountRange = singleRetryThresholdParts[0].split(keyRangeSeparator);
      int upperBoundKeyCount = MAX_KEY_COUNT;
      if (keyCountRange.length > 2) {
        throw new VeniceException(
            "Invalid single retry threshold config: " + singleRetryThreshold + ", which"
                + " should contain only lower bound and upper bound of key count range");
      }
      int lowerBoundKeyCount = Integer.parseInt(keyCountRange[0]);
      if (keyCountRange.length == 2) {
        upperBoundKeyCount = keyCountRange[1].isEmpty() ? MAX_KEY_COUNT : Integer.parseInt(keyCountRange[1]);
      }
      if (lowerBoundKeyCount < 0 || upperBoundKeyCount < 0 || lowerBoundKeyCount > upperBoundKeyCount) {
        throw new VeniceException("Invalid single retry threshold config: " + singleRetryThreshold);
      }
      if (lowerBoundKeyCount != previousUpperBound + 1) {
        throw new VeniceException(
            "Current retry threshold config: " + retryThresholdStr + " is not continuous according to key count range");
      }
      retryThresholdMap.put(lowerBoundKeyCount, threshold);
      previousUpperBound = upperBoundKeyCount;
    }
    if (!retryThresholdMap.containsKey(1)) {
      throw new VeniceException(
          "Retry threshold for batch-get: " + retryThresholdStr + " should be setup starting from 1");
    }
    if (previousUpperBound != MAX_KEY_COUNT) {
      throw new VeniceException(
          " Retry threshold for batch-get: " + retryThresholdStr + " doesn't cover unlimited key count");
    }

    return retryThresholdMap;
  }
}
