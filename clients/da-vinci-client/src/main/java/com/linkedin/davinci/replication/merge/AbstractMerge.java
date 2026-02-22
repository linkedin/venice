package com.linkedin.davinci.replication.merge;

import static com.linkedin.venice.schema.rmd.RmdConstants.REPLICATION_CHECKPOINT_VECTOR_FIELD_POS;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_POS;

import com.linkedin.davinci.schema.merge.ValueAndRmd;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import java.util.Collections;
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
      final PubSubPosition newValueSourcePosition,
      final int newValueSourceBrokerID,
      T newValue) {
    final GenericRecord oldRmd = oldValueAndRmd.getRmd();

    if (oldTimestamp < putOperationTimestamp) {
      // New value wins
      oldValueAndRmd.setValue(newValue);
      oldRmd.put(TIMESTAMP_FIELD_POS, putOperationTimestamp);
      updateReplicationCheckpointVector(oldRmd);

    } else if (oldTimestamp == putOperationTimestamp) {
      // When timestamps tie, compare decide which one should win.
      final T oldValue = oldValueAndRmd.getValue();
      newValue = compareAndReturn(oldValue, newValue);
      if (newValue == oldValue) { // Old value wins
        oldValueAndRmd.setUpdateIgnored(true);
      } else {
        oldValueAndRmd.setValue(newValue);
        updateReplicationCheckpointVector(oldRmd);
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
      final PubSubPosition newValueSourcePosition,
      final int newValueSourceBrokerID,
      ValueAndRmd<T> oldValueAndRmd) {
    // Delete wins when old and new write operation timestamps are equal.
    if (oldTimestamp <= deleteOperationTimestamp) {
      oldValueAndRmd.setValue(null);
      // Still need to track the delete timestamp in order to reject future PUT record with lower replication timestamp
      final GenericRecord oldRmd = oldValueAndRmd.getRmd();
      oldRmd.put(TIMESTAMP_FIELD_POS, deleteOperationTimestamp);
      updateReplicationCheckpointVector(oldRmd);

    } else {
      oldValueAndRmd.setUpdateIgnored(true);
    }
    return oldValueAndRmd;
  }

  protected void updateReplicationCheckpointVector(GenericRecord oldRmd) {
    oldRmd.put(REPLICATION_CHECKPOINT_VECTOR_FIELD_POS, Collections.emptyList());
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
