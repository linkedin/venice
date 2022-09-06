package com.linkedin.davinci.replication.merge;

import com.linkedin.venice.annotation.Threadsafe;
import com.linkedin.venice.schema.merge.ValueAndRmd;
import com.linkedin.venice.utils.lazy.Lazy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * API for merging existing data with incoming writes.
 *
 * There should be at least one implementation of this interface for each version of the replication metadata protocol, and
 * possibly more if multiple competing implementations are created in an attempt to optimize performance,
 * efficiency or other considerations.
 *
 * Conceptually, the various functions merge together 4 elements relating to a given key in the store:
 *
 * 1. The old value associated with the key
 * 2. The old replication metadata associated with the old value
 * 3. The incoming write operation for that key (absent in case of deletes)
 * 4. The timestamp of the write operation
 *
 * Conflict resolution rules must be applied deterministically such that given an old value and an old replication metadata, a
 * set of write operations (and their associated timestamp) may be applied in any order and always converge to
 * the same end result (i.e. to the same final value and replication metadata). This determinism is achieved by the following
 * rules:
 *
 * 1. The fields which are set in the write operation will clobber those of the old value if and only if the write
 *    operation timestamp is greater than the timestamp associated with that field in the replication metadata.
 * 2. Each element included in a collection merging operation is applied to the corresponding collection in
 *    the old value iif the write operation timestamp is greater than the timestamp of the corresponding
 *    element, than the timestamp of the corresponding element's tombstone, and than the collection's timestamp.
 * 3. In case of equal timestamps, the value of the field or of the element is compared to that of the old field
 *    or element in order to deterministically decide which one wins the conflict resolution.
 *
 * Note: all implementation must return the reference of the same {@link ValueAndRmd} instance that was
 * passed in. The input {@link ValueAndRmd} object may be mutated or replaced its inner variables. As such,
 * a caller of this function should not expect the passed in parameter to remain unchanged.
 */
@Threadsafe
public interface Merge<T> {
  /**
   * @param oldValueAndRmd the old value and replication metadata which are persisted in the server prior
   *                                       to the write operation. Note that some implementation(s) may require the old
   *                                       value to be non-null. Please refer to the Javadoc of specific implementation.
   * @param newValue a record with all fields populated and with one of the registered value schemas
   * @param putOperationTimestamp the timestamp of the incoming write operation
   * @param putOperationColoID ID of the colo/fabric where this PUT request was originally received.
   * @param newValueSourceOffset The offset from which the new value originates in the realtime stream.  Used to build
   *                               the ReplicationMetadata for the newly inserted record.
   * @param newValueSourceBrokerID The ID of the broker from which the new value originates.  ID's should correspond
   *                                 to the kafkaClusterUrlIdMap configured in the LeaderFollowerIngestionTask.  Used to build
   *                                 the ReplicationMetadata for the newly inserted record.
   * @return the resulting {@link ValueAndRmd} after merging the old one with the incoming write operation.
   *         The returned object is guaranteed to be "==" to the input oldValueAndReplicationMetadata object and the internal
   *         members of the object are possibly mutated.
   */
  ValueAndRmd<T> put(
      ValueAndRmd<T> oldValueAndRmd,
      T newValue,
      long putOperationTimestamp,
      int putOperationColoID,
      long newValueSourceOffset,
      int newValueSourceBrokerID);

  /**
   * @param oldValueAndRmd the old value and replication metadata which are persisted in the server prior to the write operation
   * @param newValueSourceOffset The offset from which the new value originates in the realtime stream.  Used to build
   *                               the ReplicationMetadata for the newly inserted record.
   * @param deleteOperationColoID ID of the colo/fabric where this DELETE request was originally received.
   * @param newValueSourceBrokerID The ID of the broker from which the new value originates.  ID's should correspond
   *                                 to the kafkaClusterUrlIdMap configured in the LeaderFollowerIngestionTask.  Used to build
   *                                 the ReplicationMetadata for the newly inserted record.
   * @return the resulting {@link ValueAndRmd} after merging the old one with the incoming delete operation.
   *         The returned object is guaranteed to be "==" to the input oldValueAndReplicationMetadata object and the internal
   *         members of the object are possibly mutated.
   */
  ValueAndRmd<T> delete(
      ValueAndRmd<T> oldValueAndRmd,
      long deleteOperationTimestamp,
      int deleteOperationColoID,
      long newValueSourceOffset,
      int newValueSourceBrokerID);

  /**
   * @param oldValueAndRmd the old value and replication metadata which are persisted in the server prior to the write operation
   * @param writeOperation a record with a write compute schema
   * @param currValueSchema Schema of the current value that is to-be-updated here.
   * @param writeComputeSchema Schema used to generate the write compute record. This schema could be a union and that is
   *                           why this schema is needed when we already pass in the {@code writeOperation} generic record.
   * @param updateOperationTimestamp the timestamp of the incoming write operation
   * @param updateOperationColoID ID of the colo/fabric where this UPDATE request was originally received.
   * @param newValueSourceOffset The offset from which the new value originates in the realtime stream.  Used to build
   *                               the ReplicationMetadata for the newly inserted record.
   * @param newValueSourceBrokerID The ID of the broker from which the new value originates.  ID's should correspond
   *                                 to the kafkaClusterUrlIdMap configured in the LeaderFollowerIngestionTask.  Used to build
   *                                 the ReplicationMetadata for the newly inserted record.
   * @return the resulting {@link ValueAndRmd} after merging the old one with the incoming write operation.
   *         The returned object is guaranteed to be "==" to the input oldValueAndReplicationMetadata object and the internal
   *         members of the object are possibly mutated.
   */
  ValueAndRmd<T> update(
      ValueAndRmd<T> oldValueAndRmd,
      Lazy<GenericRecord> writeOperation,
      Schema currValueSchema,
      long updateOperationTimestamp,
      int updateOperationColoID,
      long newValueSourceOffset,
      int newValueSourceBrokerID);
}
