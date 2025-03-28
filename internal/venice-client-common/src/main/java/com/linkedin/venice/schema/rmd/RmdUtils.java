package com.linkedin.venice.schema.rmd;

import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_POS;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_POS;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.ACTIVE_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.DELETED_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_TS_FIELD_NAME;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.antlr.v4.runtime.misc.NotNull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * Ths class is responsible for deserializing RMDs and extract some information from it.
 *
 * It borrows some methods from {@link com.linkedin.davinci.replication.merge.RmdSerDe}.
 */
public class RmdUtils {
  /**
   * Returns the type of union record given tsObject is. Right now it will be either root level long or
   * generic record of per field timestamp.
   * @param tsObject
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

  public static long extractOffsetVectorSumFromRmd(GenericRecord replicationMetadataRecord) {
    Object offsetVectorObject = replicationMetadataRecord.get(REPLICATION_CHECKPOINT_VECTOR_FIELD_POS);
    return sumOffsetVector(offsetVectorObject);
  }

  public static List<Long> extractOffsetVectorFromRmd(GenericRecord replicationMetadataRecord) {
    Object offsetVector = replicationMetadataRecord.get(REPLICATION_CHECKPOINT_VECTOR_FIELD_POS);
    if (offsetVector == null) {
      return Collections.emptyList();
    }
    return (List<Long>) offsetVector;
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
    Object timestampObject = replicationMetadataRecord.get(TIMESTAMP_FIELD_POS);
    RmdTimestampType rmdTimestampType = getRmdTimestampType(timestampObject);

    if (rmdTimestampType == RmdTimestampType.VALUE_LEVEL_TIMESTAMP) {
      return Collections.singletonList((Long) timestampObject);
    } else {
      // not supported yet so ignore it
      // TODO Must clone the results when PER_FIELD_TIMESTAMP mode is enabled to return the list.
      return Collections.singletonList(0L);
    }
  }

  /**
   * Returns a summation of all component parts to an offsetVector for vector comparison
   * @param offsetVector offsetVector to be summed
   * @return the sum of all offset vectors
   */
  public static long sumOffsetVector(Object offsetVector) {
    if (offsetVector == null) {
      return 0L;
    }
    return ((List<Long>) offsetVector).stream().reduce(0L, Long::sum);
  }

  /**
   * Checks to see if an offset vector has advanced completely beyond some base offset vector or not.
   *
   * @param baseOffset      The vector to compare against.
   * @param advancedOffset  The vector has should be advanced along.
   * @return                True if the advancedOffset vector has grown beyond the baseOffset
   */
  static public boolean hasOffsetAdvanced(@NotNull List<Long> baseOffset, @NotNull List<Long> advancedOffset) {
    for (int i = 0; i < baseOffset.size(); i++) {
      if (advancedOffset.size() - 1 < i) {
        if (baseOffset.get(i) > 0) {
          return false;
        }
        continue;
      }
      if (advancedOffset.get(i) < baseOffset.get(i)) {
        return false;
      } else if (advancedOffset.get(i) > baseOffset.get(i)) {
        return true;
      }
    }
    return true;
  }

  static public List<Long> mergeOffsetVectors(@NotNull List<Long> baseOffset, @NotNull List<Long> advancedOffset) {
    List<Long> shortVector;
    List<Long> longVector;

    if (baseOffset.size() > advancedOffset.size()) {
      shortVector = advancedOffset;
      longVector = baseOffset;
    } else {
      shortVector = baseOffset;
      longVector = advancedOffset;
    }

    List<Long> mergedVector = new ArrayList<>(longVector.size());

    for (int i = 0; i < shortVector.size(); i++) {
      mergedVector.add(Math.max(shortVector.get(i), longVector.get(i)));
    }

    if (longVector.size() != shortVector.size()) {
      mergedVector.addAll(longVector.subList(shortVector.size(), longVector.size()));
    }

    return mergedVector;
  }

  static public long getLastUpdateTimestamp(Object object) {
    // The replication metadata object contains a single field called "timestamp" which is a union of a long and a
    // record.
    // if the type is a long, we just return that
    Object timestampRecord = ((GenericRecord) object).get(TIMESTAMP_FIELD_NAME);
    if (RmdUtils.getRmdTimestampType(timestampRecord).equals(RmdTimestampType.VALUE_LEVEL_TIMESTAMP)) {
      // return early
      return (Long) timestampRecord;
    }

    // If the type is a record, then we need to iterate over the fields and find the latest timestamp of any of the
    // fields
    // the field types we're interested will either be of type long, or another record. Record is used to bookkeeping
    // operations
    // on fields which are collection types like arrays or maps.
    Long lastUpdatedTimestamp = -1L;
    // iterate through the fields, this is only a two level deep structure, so a loop with a single embedded loop will
    // fit the bill
    for (Schema.Field field: ((GenericRecord) timestampRecord).getSchema().getFields()) {
      // if the field is a record, then we need to iterate through the fields of the record
      if (field.schema().getType().equals(Schema.Type.RECORD)) {
        lastUpdatedTimestamp = Math.max(
            lastUpdatedTimestamp,
            (Long) ((GenericRecord) ((GenericRecord) timestampRecord).get(field.name())).get(TOP_LEVEL_TS_FIELD_NAME));
        for (long timestamp: (long[]) ((GenericRecord) ((GenericRecord) timestampRecord).get(field.name()))
            .get(DELETED_ELEM_TS_FIELD_NAME)) {
          lastUpdatedTimestamp = Math.max(lastUpdatedTimestamp, timestamp);
        }
        for (long timestamp: (long[]) ((GenericRecord) ((GenericRecord) timestampRecord).get(field.name()))
            .get(ACTIVE_ELEM_TS_FIELD_NAME)) {
          lastUpdatedTimestamp = Math.max(lastUpdatedTimestamp, timestamp);
        }
      } else if (field.schema().getType().equals(Schema.Type.LONG)) {
        lastUpdatedTimestamp =
            Math.max(lastUpdatedTimestamp, (Long) ((GenericRecord) timestampRecord).get(field.name()));
      }
    }
    return lastUpdatedTimestamp;
  }
}
