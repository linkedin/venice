package com.linkedin.venice.schema.writecompute;

import com.linkedin.venice.exceptions.VeniceException;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * Write compute V2 handles value records with replication metadata.
 */
@ThreadSafe
public class WriteComputeHandlerV2 extends WriteComputeHandlerV1 {

  public GenericRecord updateRecord(
      Schema valueSchema,
      Schema writeComputeSchema,
      GenericRecord replicationMetadata,
      GenericRecord originalRecord,
      GenericRecord writeComputeRecord,
      long updateOperationTimestamp
  ) {
    // TODO: to be implemented
    throw new VeniceException("Not implemented yet");
  }
}
