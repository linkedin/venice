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
   * @param valueRecord Value record. Note that the this object may be mutated in the implementation of this method and
   *                    the returned object may be reference equal to this input object.
   *                    When it is {@link Optional#empty}, it means that there is currently no value.
   * @param writeComputeRecord Write compute schema
   *
   * @return updated value record.
   */
  GenericRecord updateValueRecord(Schema valueSchema, GenericRecord valueRecord, GenericRecord writeComputeRecord);
}
