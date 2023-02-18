package com.linkedin.davinci.replication.merge;

import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;

import com.linkedin.venice.schema.merge.ValueAndRmd;
import java.util.List;
import org.apache.avro.generic.GenericRecord;


/**
 * This abstract class contains common methods used by various implementations of the Merge interface.
 *
 * @param <T> type of value that merge happens on.
 */
abstract class AbstractMerge<T> implements Merge<T> {
  protected ValueAndRmd<T> putWithRecordLevelTimestamp(
      final long oldTimestamp,
      ValueAndRmd<T> oldValueAndRmd,
      final long putOperationTimestamp,
      final long newValueSourceOffset,
      final int newValueSourceBrokerID,
      T newValue) {
    final GenericRecord oldRmd = oldValueAndRmd.getRmd();

    if (oldTimestamp < putOperationTimestamp) {
      // New value wins
      oldValueAndRmd.setValue(newValue);
      oldRmd.put(TIMESTAMP_FIELD_NAME, putOperationTimestamp);
      updateReplicationCheckpointVector(oldRmd, newValueSourceOffset, newValueSourceBrokerID);

    } else if (oldTimestamp == putOperationTimestamp) {
      // When timestamps tie, compare decide which one should win.
      final T oldValue = oldValueAndRmd.getValue();
      newValue = compareAndReturn(oldValue, newValue);
      if (newValue == oldValue) { // Old value wins
        oldValueAndRmd.setUpdateIgnored(true);
      } else {
        oldValueAndRmd.setValue(newValue);
        updateReplicationCheckpointVector(oldRmd, newValueSourceOffset, newValueSourceBrokerID);
      }

    } else {
      // Old value wins.
      oldValueAndRmd.setUpdateIgnored(true);
    }
    return oldValueAndRmd;
  }

  protected ValueAndRmd<T> deleteWithValueLevelTimestamp(
      final long oldTimestamp,
      final long deleteOperationTimestamp,
      final long newValueSourceOffset,
      final int newValueSourceBrokerID,
      ValueAndRmd<T> oldValueAndRmd) {
    // Delete wins when old and new write operation timestamps are equal.
    if (oldTimestamp <= deleteOperationTimestamp) {
      oldValueAndRmd.setValue(null);
      // Still need to track the delete timestamp in order to reject future PUT record with lower replication timestamp
      final GenericRecord oldRmd = oldValueAndRmd.getRmd();
      oldRmd.put(TIMESTAMP_FIELD_NAME, deleteOperationTimestamp);
      updateReplicationCheckpointVector(oldRmd, newValueSourceOffset, newValueSourceBrokerID);

    } else {
      oldValueAndRmd.setUpdateIgnored(true);
    }
    return oldValueAndRmd;
  }

  protected void updateReplicationCheckpointVector(
      GenericRecord oldRmd,
      long newValueSourceOffset,
      int newValueSourceBrokerID) {
    oldRmd.put(
        REPLICATION_CHECKPOINT_VECTOR_FIELD,
        MergeUtils.mergeOffsetVectors(
            (List<Long>) oldRmd.get(REPLICATION_CHECKPOINT_VECTOR_FIELD),
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
