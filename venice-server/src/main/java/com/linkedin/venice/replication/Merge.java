package com.linkedin.venice.replication;

import org.apache.avro.generic.GenericRecord;


/**
 * API for merging existing data with incoming writes.
 *
 * There should be at least one implementation of this interface for each version of the TSMD protocol, and
 * possibly more if multiple competing implementations are created in an attempt to optimize performance,
 * efficiency or other considerations.
 *
 * Conceptually, the various functions merge together 4 elements relating to a given key in the store:
 *
 * 1. The old value associated with the key
 * 2. The old timestamp metadata associated with the old value
 * 3. The incoming write operation for that key (absent in case of deletes)
 * 4. The timestamp of the write operation
 *
 * Conflict resolution rules must be applied deterministically such that given an old value and an old TSMD, a
 * set of write operations (and their associated timestamp) may be applied in any order and always converge to
 * the same end result (i.e. to the same final value and TSMD). This determinism is achieved by the following
 * rules:
 *
 * 1. The fields which are set in the write operation will clobber those of the old value if and only if the write
 *    operation timestamp is greater than the timestamp associated with that field in the TSMD.
 * 2. Each element included in a collection merging operation is applied to the corresponding collection in
 *    the old value iif the write operation timestamp is greater than the timestamp of the corresponding
 *    element, than the timestamp of the corresponding element's tombstone, and than the collection's timestamp.
 * 3. In case of equal timestamps, the value of the field or of the element is compared to that of the old field
 *    or element in order to deterministically decide which one wins the conflict resolution.
 *
 * Note: an implementation is allowed to return the same {@link ValueAndTimestampMetadata} instance which was
 * passed in, and to mutate or replace its inner variables. As such, a caller of this function should not expect
 * the passed in parameter to remain unchanged. The reverse assumption is also invalid, as there may be cases
 * where an implementation cannot mutate the inner variables (such as when the write operation and old value use
 * different schemas) and it is therefore possible that new objects will be instantiated to replace the old ones,
 * rather than mutating them.
 */
public interface Merge {
  /**
   * @param oldValueAndTimestampMetadata the old value and TSMD which are persisted in the server prior to the write operation
   * @param newValue a record with all fields populated and with one of the registered value schemas
   * @param writeOperationTimestamp the timestamp of the incoming write operation
   * @return the resulting {@link ValueAndTimestampMetadata} after merging the old one with the incoming write operation
   */
  ValueAndTimestampMetadata put(ValueAndTimestampMetadata oldValueAndTimestampMetadata, GenericRecord newValue, long writeOperationTimestamp);

  /**
   * @param oldValueAndTimestampMetadata the old value and TSMD which are persisted in the server prior to the write operation
   * @param writeOperationTimestamp the timestamp of the incoming write operation
   * @return the resulting {@link ValueAndTimestampMetadata} after merging the old one with the incoming write operation
   */
  ValueAndTimestampMetadata delete(ValueAndTimestampMetadata oldValueAndTimestampMetadata, long writeOperationTimestamp);

  /**
   * @param oldValueAndTimestampMetadata the old value and TSMD which are persisted in the server prior to the write operation
   * @param writeOperation a record with a write compute schema
   * @param writeOperationTimestamp the timestamp of the incoming write operation
   * @return the resulting {@link ValueAndTimestampMetadata} after merging the old one with the incoming write operation
   */
  ValueAndTimestampMetadata update(ValueAndTimestampMetadata oldValueAndTimestampMetadata, GenericRecord writeOperation, long writeOperationTimestamp);
}
