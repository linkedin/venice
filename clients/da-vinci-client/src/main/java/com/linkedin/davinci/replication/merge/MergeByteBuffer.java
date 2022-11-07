package com.linkedin.davinci.replication.merge;

import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;

import com.linkedin.venice.schema.merge.ValueAndRmd;
import com.linkedin.venice.schema.rmd.RmdTimestampType;
import com.linkedin.venice.schema.rmd.RmdUtils;
import com.linkedin.venice.utils.lazy.Lazy;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * This class handles byte-level merge. Each byte buffer has only one (value-level) timestamp. DCR happens only at the
 * whole byte buffer level. Specifically, given 2 byte buffers to merge, only one of them will win completely.
 */
public class MergeByteBuffer extends AbstractMerge<ByteBuffer> {
  @Override
  public ValueAndRmd<ByteBuffer> put(
      ValueAndRmd<ByteBuffer> oldValueAndRmd,
      ByteBuffer newValue,
      long putOperationTimestamp,
      int writeOperationColoID,
      long sourceOffsetOfNewValue,
      int newValueSourceBrokerID) {
    final GenericRecord oldReplicationMetadata = oldValueAndRmd.getRmd();
    final Object tsObject = oldReplicationMetadata.get(TIMESTAMP_FIELD_NAME);
    RmdTimestampType rmdTimestampType = RmdUtils.getRmdTimestampType(tsObject);

    if (rmdTimestampType == RmdTimestampType.VALUE_LEVEL_TIMESTAMP) {
      return putWithRecordLevelTimestamp(
          (long) tsObject,
          oldValueAndRmd,
          putOperationTimestamp,
          sourceOffsetOfNewValue,
          newValueSourceBrokerID,
          newValue);
    } else {
      throw new IllegalArgumentException("Only handle record-level timestamp. Got: " + rmdTimestampType);
    }
  }

  @Override
  public ValueAndRmd<ByteBuffer> delete(
      ValueAndRmd<ByteBuffer> oldValueAndRmd,
      long deleteOperationTimestamp,
      int deleteOperationColoID,
      long newValueSourceOffset,
      int newValueSourceBrokerID) {
    final GenericRecord oldReplicationMetadata = oldValueAndRmd.getRmd();
    final Object tsObject = oldReplicationMetadata.get(TIMESTAMP_FIELD_NAME);
    RmdTimestampType rmdTimestampType = RmdUtils.getRmdTimestampType(tsObject);
    if (rmdTimestampType == RmdTimestampType.VALUE_LEVEL_TIMESTAMP) {
      return deleteWithValueLevelTimestamp(
          (long) tsObject,
          deleteOperationTimestamp,
          newValueSourceOffset,
          newValueSourceBrokerID,
          oldValueAndRmd);
    } else {
      throw new IllegalArgumentException("Only handle record-level timestamp. Got: " + rmdTimestampType);
    }
  }

  @Override
  public ValueAndRmd<ByteBuffer> update(
      ValueAndRmd<ByteBuffer> oldValueAndRmd,
      Lazy<GenericRecord> writeOperation,
      Schema currValueSchema,
      long updateOperationTimestamp,
      int updateOperationColoID,
      long newValueSourceOffset,
      int newValueSourceBrokerID) {
    throw new IllegalStateException("Update request should not be handled by this class.");
  }

  @Override
  ByteBuffer compareAndReturn(ByteBuffer oldValue, ByteBuffer newValue) {
    return (ByteBuffer) MergeUtils.compareAndReturn(oldValue, newValue);
  }
}
