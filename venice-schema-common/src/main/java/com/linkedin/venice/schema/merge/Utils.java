package com.linkedin.venice.schema.merge;

import com.linkedin.venice.utils.IndexedHashMap;
import java.util.List;


class Utils {
  private Utils() {
    // Utility class
  }

  static <T> IndexedHashMap<T, Long> createElementToActiveTsMap(
      List<T> existingElements,
      List<Long> activeTimestamps,
      final long topLevelTimestamp,
      final long minTimestamp, // Any timestamp smaller than or equal to this one will not be included in the result
                               // map.
      final int putOnlyPartLength) {
    IndexedHashMap<T, Long> activeElementToTsMap = new IndexedHashMap<>(existingElements.size());
    int idx = 0;
    for (T existingElement: existingElements) {
      final long activeTimestamp;
      if (idx < putOnlyPartLength) {
        activeTimestamp = topLevelTimestamp;
      } else {
        activeTimestamp = activeTimestamps.get(idx - putOnlyPartLength);
      }
      if (activeTimestamp > minTimestamp) {
        activeElementToTsMap.put(existingElement, activeTimestamp);
      }
      idx++;
    }
    return activeElementToTsMap;
  }

  static <T> IndexedHashMap<T, Long> createDeletedElementToTsMap(
      List<T> deletedElements,
      List<Long> deletedTimestamps,
      final long minTimestamp // Any deletion timestamp strictly smaller than this one will not be included in the
                              // result map.
  ) {
    IndexedHashMap<T, Long> elementToTimestampMap = new IndexedHashMap<>();
    int idx = 0;
    for (long deletedTimestamp: deletedTimestamps) {
      if (deletedTimestamp >= minTimestamp) {
        elementToTimestampMap.put(deletedElements.get(idx), deletedTimestamp);
      }
      idx++;
    }
    return elementToTimestampMap;
  }
}
