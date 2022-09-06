package com.linkedin.venice.partitioner;

import com.linkedin.venice.exceptions.PartitionerSchemaMismatchException;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;


/**
 * Implementation of the {@link VenicePartitioner} class for test purpose.
 *
 */
public class InvalidKeySchemaPartitioner extends VenicePartitioner {
  public InvalidKeySchemaPartitioner(VeniceProperties props, Schema schema) {
    super(props, schema);
  }

  @Override
  public int getPartitionId(byte[] keyBytes, int numPartitions) {
    return 0;
  }

  @Override
  public int getPartitionId(ByteBuffer keyByteBuffer, int numPartitions) {
    return 0;
  }

  @Override
  protected void checkSchema(Schema keySchema) throws PartitionerSchemaMismatchException {
    throw new PartitionerSchemaMismatchException("The key schema is invalid.");
  }
}
