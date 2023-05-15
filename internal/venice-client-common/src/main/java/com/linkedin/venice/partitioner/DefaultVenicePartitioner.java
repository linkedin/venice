package com.linkedin.venice.partitioner;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.avro.Schema;


/**
 * Default implementation of the {@link VenicePartitioner} class.
 *
 * It uses a hashing algorithm (MD5 by default) to determine the appropriate partition
 * for each message.
 */
public class DefaultVenicePartitioner extends VenicePartitioner {
  public static final String MD5_HASH_ALGORITHM = "MD5";
  private static final int MD5_DIGEST_SIZE = 16;

  /**
   * This class encapsulates the objects and primitives that can be re-used in order to minimize object allocation
   */
  private static class PartitionerState {
    final MessageDigest md;
    final byte[] digestOutput = new byte[MD5_DIGEST_SIZE];
    int modulo = 0;
    int digit = 0;
    int digestSize = 0;

    PartitionerState() {
      try {
        this.md = MessageDigest.getInstance(MD5_HASH_ALGORITHM);
      } catch (NoSuchAlgorithmException e) {
        throw new VeniceException("Failed to initialize MD5 hash MessageDigest");
      }
    }
  }

  private static final ThreadLocal<PartitionerState> partitionerState =
      ThreadLocal.withInitial(() -> new PartitionerState());

  public DefaultVenicePartitioner() {
    super();
  }

  public DefaultVenicePartitioner(VeniceProperties props) {
    this(props, null);
  }

  public DefaultVenicePartitioner(VeniceProperties props, Schema schema) {
    super(props, schema);
  }

  public int getPartitionId(byte[] keyBytes, int offset, int length, int numPartitions) {
    PartitionerState ps = partitionerState.get();

    ps.md.update(keyBytes, offset, length);
    try {
      ps.digestSize = ps.md.digest(ps.digestOutput, 0, ps.digestOutput.length);
    } catch (Exception e) {
      throw new VeniceException("Indigestion!", e);
    }

    // find partition value from basic modulus algorithm
    ps.modulo = 0;
    for (int i = 0; i < ps.digestSize; i++) {
      // Convert byte (-128..127) to int 0..255
      ps.digit = ps.digestOutput[i] & 0xFF;
      ps.modulo = (ps.modulo * 256 + ps.digit) % numPartitions;
    }

    int partition = Math.abs(ps.modulo % numPartitions);

    ps.md.reset();
    return partition;
  }

  @Override
  public int getPartitionId(byte[] keyBytes, int numPartitions) {
    return getPartitionId(keyBytes, 0, keyBytes.length, numPartitions);
  }

  @Override
  public int getPartitionId(ByteBuffer keyByteBuffer, int numPartitions) {
    return getPartitionId(keyByteBuffer.array(), keyByteBuffer.position(), keyByteBuffer.remaining(), numPartitions);
  }
}
