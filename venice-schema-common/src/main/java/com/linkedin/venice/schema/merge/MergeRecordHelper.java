package com.linkedin.venice.schema.merge;

import javax.annotation.concurrent.ThreadSafe;
import javax.validation.constraints.NotNull;
import org.apache.avro.generic.GenericRecord;


/**
 * The purpose of this interface is to extract common merge logic. For example,
 * {@link #putOnField(GenericRecord, GenericRecord, String, Object, long, int)} utOnField} can be used in below cases:
 *    1. Put a new record on an existing record.
 *    2. Partial update in write compute. Specifically, partial update tries to overrides specific fields in a record.
 */
@ThreadSafe
public interface MergeRecordHelper {

  UpdateResultStatus putOnField(
      @NotNull GenericRecord currRecord,
      @NotNull GenericRecord currTimestampRecord,
      String fieldName,
      Object newFieldValue,
      long putTimestamp,
      int putOperationColoID
  );

  UpdateResultStatus deleteRecord(
      @NotNull GenericRecord currRecord,
      @NotNull GenericRecord currTimestampRecord,
      long deleteTimestamp,
      int deleteOperationColoID
  );
}
