package com.linkedin.venice.schema.writecompute;

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
   * @param writeComputeSchema Write compute schema that corresponds to the value schema.
   * @param valueRecord Value record. Note that the this object may be mutated in the implementation of this method and
   *                    the returned object may be reference equal to this input object.
   * @param writeComputeRecord Write compute schema
   *
   * @return updated value record.
   */
  GenericRecord updateRecord(Schema valueSchema, Schema writeComputeSchema, GenericRecord valueRecord, GenericRecord writeComputeRecord);
}
