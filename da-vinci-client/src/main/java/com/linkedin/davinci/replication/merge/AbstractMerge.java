package com.linkedin.davinci.replication.merge;

import static com.linkedin.venice.schema.rmd.ReplicationMetadataConstants.*;

import com.linkedin.venice.schema.merge.ValueAndReplicationMetadata;
import java.util.List;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;


/**
 * This abstract class contains common methods used by various implementations of the Merge interface.
 *
 * @param <T> type of value that merge happens on.
 */
abstract class AbstractMerge<T> implements Merge<T> {
  protected ValueAndReplicationMetadata<T> putWithRecordLevelTimestamp(
      final long oldTimestamp,
      ValueAndReplicationMetadata<T> oldValueAndReplicationMetadata,
      final long putOperationTimestamp,
      final long newValueSourceOffset,
      final int newValueSourceBrokerID,
      T newValue) {
    final GenericRecord oldReplicationMetadata = oldValueAndReplicationMetadata.getReplicationMetadata();

    if (oldTimestamp < putOperationTimestamp) {
      // New value wins
      oldValueAndReplicationMetadata.setValue(newValue);
      oldReplicationMetadata.put(TIMESTAMP_FIELD_NAME, putOperationTimestamp);
      updateReplicationCheckpointVector(oldReplicationMetadata, newValueSourceOffset, newValueSourceBrokerID);

    } else if (oldTimestamp == putOperationTimestamp) {
      // When timestamps tie, compare decide which one should win.
      final T oldValue = oldValueAndReplicationMetadata.getValue();
      newValue = compareAndReturn(oldValue, newValue);
      if (newValue == oldValue) { // Old value wins
        oldValueAndReplicationMetadata.setUpdateIgnored(true);
      } else {
        oldValueAndReplicationMetadata.setValue(newValue);
        updateReplicationCheckpointVector(oldReplicationMetadata, newValueSourceOffset, newValueSourceBrokerID);
      }

    } else {
      // Old value wins.
      oldValueAndReplicationMetadata.setUpdateIgnored(true);
    }
    return oldValueAndReplicationMetadata;
  }

  protected ValueAndReplicationMetadata<T> deleteWithValueLevelTimestamp(
      final long oldTimestamp,
      final long deleteOperationTimestamp,
      final long newValueSourceOffset,
      final int newValueSourceBrokerID,
      ValueAndReplicationMetadata<T> oldValueAndReplicationMetadata) {
    // Delete wins when old and new write operation timestamps are equal.
    if (oldTimestamp <= deleteOperationTimestamp) {
      oldValueAndReplicationMetadata.setValue(null);
      // Still need to track the delete timestamp in order to reject future PUT record with lower replication timestamp
      final GenericRecord oldReplicationMetadata = oldValueAndReplicationMetadata.getReplicationMetadata();
      oldReplicationMetadata.put(TIMESTAMP_FIELD_NAME, deleteOperationTimestamp);
      updateReplicationCheckpointVector(oldReplicationMetadata, newValueSourceOffset, newValueSourceBrokerID);

    } else {
      oldValueAndReplicationMetadata.setUpdateIgnored(true);
    }
    return oldValueAndReplicationMetadata;
  }

  protected void updateReplicationCheckpointVector(
      GenericRecord oldReplicationMetadata,
      long newValueSourceOffset,
      int newValueSourceBrokerID) {
    oldReplicationMetadata.put(
        REPLICATION_CHECKPOINT_VECTOR_FIELD,
        MergeUtils.mergeOffsetVectors(
            Optional.ofNullable((List<Long>) oldReplicationMetadata.get(REPLICATION_CHECKPOINT_VECTOR_FIELD)),
            newValueSourceOffset,
            newValueSourceBrokerID));
  }

  /**
   * Return the value that should be preserved. This method should be used to decide which value to preserve and which
   * value to discard when timestamps tie.
   *
   * @param oldValue current value
   * @param newValue new value
   * @return value that should be preserved.
   */
  abstract T compareAndReturn(T oldValue, T newValue);
}
