package com.linkedin.venice.schema.rmd;

import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_POS;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.ACTIVE_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.DELETED_ELEM_TS_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp.TOP_LEVEL_TS_FIELD_NAME;

import java.util.Collections;
import java.util.List;
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

  public static List<Long> extractOffsetVectorFromRmd(GenericRecord replicationMetadataRecord) {
    Object offsetVector = replicationMetadataRecord.get(REPLICATION_CHECKPOINT_VECTOR_FIELD_POS);
    if (offsetVector == null) {
      return Collections.emptyList();
    }
    return (List<Long>) offsetVector;
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
