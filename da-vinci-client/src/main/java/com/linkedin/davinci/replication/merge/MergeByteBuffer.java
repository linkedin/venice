package com.linkedin.davinci.replication.merge;

import com.linkedin.venice.schema.merge.ValueAndReplicationMetadata;
import com.linkedin.venice.utils.lazy.Lazy;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import static com.linkedin.venice.schema.rmd.ReplicationMetadataConstants.*;


/**
 * This class handles byte-level merge. Each byte buffer has only one (value-level) timestamp. DCR happens only at the
 * whole byte buffer level. Specifically, given 2 byte buffers to merge, only one of them will win completely.
 */
public class MergeByteBuffer extends AbstractMerge<ByteBuffer> {

  @Override
  public ValueAndReplicationMetadata<ByteBuffer> put(
      ValueAndReplicationMetadata<ByteBuffer> oldValueAndReplicationMetadata,
      ByteBuffer newValue,
      long putOperationTimestamp,
      int writeOperationColoID,
      long sourceOffsetOfNewValue,
      int newValueSourceBrokerID
  ) {
    final GenericRecord oldReplicationMetadata = oldValueAndReplicationMetadata.getReplicationMetadata();
    final Object tsObject = oldReplicationMetadata.get(TIMESTAMP_FIELD_NAME);
    RmdTimestampType rmdTimestampType = MergeUtils.getReplicationMetadataType(tsObject);

    if (rmdTimestampType == RmdTimestampType.VALUE_LEVEL_TIMESTAMP) {
      return putWithRecordLevelTimestamp(
          (long) tsObject,
          oldValueAndReplicationMetadata,
          putOperationTimestamp,
          sourceOffsetOfNewValue,
          newValueSourceBrokerID,
          newValue
      );
    } else {
      throw new IllegalArgumentException("Only handle record-level timestamp. Got: " + rmdTimestampType);
    }
  }

  @Override
  public ValueAndReplicationMetadata<ByteBuffer> delete(
      ValueAndReplicationMetadata<ByteBuffer> oldValueAndReplicationMetadata,
      long deleteOperationTimestamp,
      int deleteOperationColoID,
      long newValueSourceOffset,
      int newValueSourceBrokerID
  ) {
    final GenericRecord oldReplicationMetadata = oldValueAndReplicationMetadata.getReplicationMetadata();
    final Object tsObject = oldReplicationMetadata.get(TIMESTAMP_FIELD_NAME);
    RmdTimestampType rmdTimestampType = MergeUtils.getReplicationMetadataType(tsObject);
    if (rmdTimestampType == RmdTimestampType.VALUE_LEVEL_TIMESTAMP) {
      return deleteWithValueLevelTimestamp(
          (long) tsObject,
          deleteOperationTimestamp,
          newValueSourceOffset,
          newValueSourceBrokerID,
          oldValueAndReplicationMetadata
      );
    } else {
      throw new IllegalArgumentException("Only handle record-level timestamp. Got: " + rmdTimestampType);
    }
  }

  @Override
  public ValueAndReplicationMetadata<ByteBuffer> update(
      ValueAndReplicationMetadata<ByteBuffer> oldValueAndReplicationMetadata,
      Lazy<GenericRecord> writeOperation,
      Schema currValueSchema,
      Schema writeComputeSchema,
      long updateOperationTimestamp,
      int updateOperationColoID,
      long newValueSourceOffset,
      int newValueSourceBrokerID
  ) {
    throw new IllegalStateException("Update request should not be handled by this class.");
  }

  @Override
  ByteBuffer compareAndReturn(ByteBuffer oldValue, ByteBuffer newValue) {
    return (ByteBuffer) MergeUtils.compareAndReturn(oldValue, newValue);
  }
}
