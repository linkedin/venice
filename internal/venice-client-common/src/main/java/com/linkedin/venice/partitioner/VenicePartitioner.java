package com.linkedin.venice.partitioner;

import com.linkedin.venice.exceptions.PartitionerSchemaMismatchException;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;


/**
 * Determines partitioning, which is used for producing messages into the right
 * Kafka partitions and routing reads to the correct Venice storage nodes.
 *
 * N.B.: This is purposefully de-coupled from Kafka, so that the Router does
 *       not need to depend on Kafka.
 */
public abstract class VenicePartitioner {
  protected final VeniceProperties props; // available for sub-classes to use.

  public VenicePartitioner() {
    this(VeniceProperties.empty(), null);
  }

  public VenicePartitioner(VeniceProperties props) {
    this(props, null);
  }

  public VenicePartitioner(VeniceProperties props, Schema schema) {
    this.props = props;
    if (schema != null) {
      checkSchema(schema);
    }
  }

  /**
   * A function that returns the partitionId based on the key and partition count.
   *
   * @param keyBytes      - A key that will be mapped into a partition
   * @param numPartitions - The number of total partitions available in Kafka/storage
   * @return
   */
  public abstract int getPartitionId(byte[] keyBytes, int numPartitions);

  /**
   * Implementors of this class can optionally provide an implementation of this function,
   * which would result in eliminating an instantiation of {@link ByteBuffer} in the case
   * where the provided offset and length do not map to the boundaries of the byte[]. This
   * is just a minor optimization.
   */
  public int getPartitionId(byte[] keyBytes, int offset, int length, int numPartitions) {
    if (offset != 0 || keyBytes.length != length) {
      return getPartitionId(ByteBuffer.wrap(keyBytes, offset, length), numPartitions);
    }
    return getPartitionId(keyBytes, numPartitions);
  }

  public abstract int getPartitionId(ByteBuffer keyByteBuffer, int numPartitions);

  /**
   * Implementors of this class can optionally provide an implementation of this function,
   * which can perform validation of schemas to be certain that they are compatible with the
   * partitioner implementation.
   *
   * @param keySchema the schema to be validated
   * @throws PartitionerSchemaMismatchException should the provided schema not match the partitioner (with a message that explains why).
   */
  protected void checkSchema(@Nonnull Schema keySchema) throws PartitionerSchemaMismatchException {
  }
}
