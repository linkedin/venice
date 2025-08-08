package com.linkedin.venice.schema.writecompute;

import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * This interface provides methods to execute write-compute operation a value
 */
public interface WriteComputeHandler {
  /**
   * Execute write-compute operation on a value record.
   *
   * @param valueSchema Schema of the value record.
   * @param valueRecord Value record. Note that this object may be mutated in the implementation of this method and
   *                    the returned object may be reference equal to this input object.
   *                    When it is {@link Optional#empty}, it means that there is currently no value.
   * @param writeComputeRecord Write compute schema
   *
   * @return updated value record.
   */
  GenericRecord updateValueRecord(Schema valueSchema, GenericRecord valueRecord, GenericRecord writeComputeRecord);

  /**
   * Merge two partial update record that has the same update schema.
   * Note that the semantic of the merged update record is equivalent to two partial update applied one by one with
   * different timestamp for each field:
   * (1) If any one of the field operation is NoOp, result will be the same as the other field operation.
   * (2) If both field operation are set field, the latter one will win.
   * (2) For collection field:
   * -- If 2nd update is a set field, it will override the previous one;
   * -- If the first one is set field and the 2nd one is collection merge, they will merge into a set field. This is to
   * make sure the first set field semantic get applied correctly and fully override previous value.
   * -- If both are collection merge, they will also merge into a collection merge. Note that:
   *    (1) If an element is removed in the first update and added in the latter update, it will still present in the
   *    merged collection merge operation. This API for now does not perform de-dupe if an element present in both union
   *    and diff operation. Venice Server will be able to handle this correctly regardless.
   *    (2) The expected priority for the two collection merge operation is: (a) current update's union op < (b) current
   *    update's diff op < (c) next update's union op < (d) next update's diff op. Within the same update, diff always
   *    has higher priority than union op, which is also exercised in Venice Backend conflict resolution logic.
   * Also, note that this API may modify the content of input update records.
   * @param currUpdateRecord Current update record. If existing update record is null, return the next update record.
   * @param newUpdateRecord Next update record.
   *
   * @return Merged update record.
   */
  GenericRecord mergeUpdateRecord(Schema valueSchema, GenericRecord currUpdateRecord, GenericRecord newUpdateRecord);
}
