package com.linkedin.davinci.replication.merge;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;


public class MergeUtils {
  private MergeUtils() {
    // Utility class
  }

  /**
   * @return the object with a higher hash code.
   */
  public static Object compareAndReturn(Object o1, Object o2) {
    // nulls win comparison on the basis that we prefer deletes to win in the case
    // where there is a tie on RMD timestamp comparison.
    if (o1 == null) {
      return o1;
    } else if (o2 == null) {
      return o2;
    }
    // for same object always return first object o1.
    // TODO: note that hashCode based compare and return is not deterministic when hash collision happens. So, it should
    // be migrated to use {@link AvroCollectionElementComparator} later.
    if (o1.hashCode() >= o2.hashCode()) {
      return o1;
    } else {
      return o2;
    }
  }

  /**
   * @return If the input {@param oldOffsetVector} is null, the returned value could be null.
   */
  static @Nullable List<Long> mergeOffsetVectors(List<Long> oldOffsetVector, Long newOffset, int sourceBrokerID) {
    if (sourceBrokerID < 0) {
      // Can happen if we could not deduce the sourceBrokerID (can happen due to a misconfiguration)
      // in such cases, we will not try to alter the existing offsetVector, instead just returning it.
      return oldOffsetVector;
    }
    final List<Long> offsetVector = oldOffsetVector == null ? new ArrayList<>(sourceBrokerID) : oldOffsetVector;

    // Making sure there is room available for the insertion (fastserde LongList can't be cast to arraylist)
    // Lists in java require that gaps be filled, so first we fill any gaps by adding some initial offset values
    int i = offsetVector.size();
    for (; i <= sourceBrokerID; i++) {
      offsetVector.add(i, 0L);
    }
    offsetVector.set(sourceBrokerID, newOffset);
    return offsetVector;
  }
}
