package com.linkedin.davinci.replication.merge;

import static com.linkedin.venice.schema.rmd.RmdConstants.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericRecord;


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
  public static RmdTimestampType getRmdTimestampType(Object tsObject) {
    if (tsObject instanceof Long) {
      return RmdTimestampType.VALUE_LEVEL_TIMESTAMP;
    } else if (tsObject instanceof GenericRecord) {
      return RmdTimestampType.PER_FIELD_TIMESTAMP;
    } else {
      throw new IllegalStateException("Unexpected type of timestamp object. Got timestamp object: " + tsObject);
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

  public static long extractOffsetVectorSumFromRmd(GenericRecord replicationMetadataRecord) {
    Object offsetVectorObject = replicationMetadataRecord.get(REPLICATION_CHECKPOINT_VECTOR_FIELD);
    return MergeUtils.sumOffsetVector(offsetVectorObject);
  }

  public static List<Long> extractTimestampFromRmd(GenericRecord replicationMetadataRecord) {
    // TODO: This function needs a heuristic to work on field level timestamps. At time of writing, this function
    // is only for recording the previous value of a record's timestamp, so we could consider specifying the incoming
    // operation to identify if we care about the record level timestamp, or, certain fields and then returning an
    // ordered
    // list of those timestamps to compare post resolution. I hesitate to commit to an implementation here prior to
    // putting
    // the full write compute resolution into uncommented fleshed out glory. So we'll effectively ignore operations
    // that aren't root level until then.
    if (replicationMetadataRecord == null) {
      return Collections.singletonList(0L);
    }
    Object timestampObject = replicationMetadataRecord.get(TIMESTAMP_FIELD_NAME);
    RmdTimestampType rmdTimestampType = MergeUtils.getRmdTimestampType(timestampObject);

    if (rmdTimestampType == RmdTimestampType.VALUE_LEVEL_TIMESTAMP) {
      return Collections.singletonList((long) timestampObject);
    } else {
      // not supported yet so ignore it
      // TODO Must clone the results when PER_FIELD_TIMESTAMP mode is enabled to return the list.
      return Collections.singletonList(0L);
    }
  }

  /**
   * @return If the input {@param oldOffsetVector} is {@link Optional#empty()}, the returned value could be null.
   */
  static @Nullable List<Long> mergeOffsetVectors(
      Optional<List<Long>> oldOffsetVector,
      Long newOffset,
      int sourceBrokerID) {
    if (sourceBrokerID < 0) {
      // Can happen if we could not deduce the sourceBrokerID (can happen due to a misconfiguration)
      // in such cases, we will not try to alter the existing offsetVector, instead just returning it.
      return oldOffsetVector.orElse(null);
    }
    final List<Long> offsetVector = oldOffsetVector.orElse(new ArrayList<>(sourceBrokerID));

    // Making sure there is room available for the insertion (fastserde LongList can't be cast to arraylist)
    // Lists in java require that gaps be filled, so first we fill any gaps by adding some initial offset values
    for (int i = offsetVector.size(); i <= sourceBrokerID; i++) {
      offsetVector.add(i, 0L);
    }
    offsetVector.set(sourceBrokerID, newOffset);
    return offsetVector;
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
    return ((List<Long>) offsetVector).stream().reduce(0L, Long::sum);
  }
}
