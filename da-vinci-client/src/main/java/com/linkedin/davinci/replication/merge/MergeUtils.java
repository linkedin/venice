package com.linkedin.davinci.replication.merge;

import java.util.ArrayList;
import java.util.List;


public class MergeUtils {

  private MergeUtils() {
    // Utility class
  }

  /**
   * Returns the type of union record given tsObject is. Right now it will be either root level long or
   * generic record of per field timestamp.
   * @param tsObject
   * @return
   */
  public static Merge.ReplicationMetadataType getReplicationMetadataType(Object tsObject) {
    if (tsObject instanceof Long) {
      return Merge.ReplicationMetadataType.ROOT_LEVEL_TIMESTAMP;
    } else {
      return Merge.ReplicationMetadataType.PER_FIELD_TIMESTAMP;
    }
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


  static List<Long> mergeOffsetVectors(List<Long> oldOffsetVector, Long newOffset, int sourceBrokerID) {
    if (sourceBrokerID < 0) {
      // Can happen if we could not deduce the sourceBrokerID (can happen due to a misconfiguration)
      // in such cases, we will not try to alter the existing offsetVector, instead just returning it.
      return oldOffsetVector;
    }
    if (oldOffsetVector == null) {
      oldOffsetVector = new ArrayList<>(sourceBrokerID);
    }
    // Making sure there is room available for the insertion (fastserde LongList can't be cast to arraylist)
    // Lists in java require that gaps be filled, so first we fill any gaps by adding some initial offset values
    for(int i = oldOffsetVector.size(); i <= sourceBrokerID; i++) {
      oldOffsetVector.add(i, 0L);
    }
    oldOffsetVector.set(sourceBrokerID, newOffset);
    return oldOffsetVector;
  }

  /**
   * Returns a summation of all component parts to an offsetVector for vector comparison
   * @param offsetVector offsetVector to be summed
   * @return the sum of all offset vectors
   */
  static long sumOffsetVector(Object offsetVector) {
    if (offsetVector == null) {
      return 0L;
    }
    return ((List<Long>)offsetVector).stream().reduce(0L, Long::sum);
  }
}
