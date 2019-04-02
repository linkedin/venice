package com.linkedin.venice.partitioner;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.ByteUtils;

import com.linkedin.venice.utils.VeniceProperties;
import java.nio.ByteBuffer;
import org.apache.log4j.Logger;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Default implementation of the {@link VenicePartitioner} class.
 *
 * It uses a hashing algorithm (MD5 by default) to determine the appropriate partition
 * for each message.
 */
public class DefaultVenicePartitioner extends VenicePartitioner {

  static final Logger logger = Logger.getLogger(DefaultVenicePartitioner.class);

  public static final String MD5_HASH_ALGORITHM = "MD5";
  private static final ThreadLocal<MessageDigest> messageDigestThreadLocal = ThreadLocal.withInitial(() -> {
    try {
      return MessageDigest.getInstance(MD5_HASH_ALGORITHM);
    } catch (NoSuchAlgorithmException e) {
      throw new VeniceException("Failed to initialize MD5 hash MessageDigest");
    }
  });

  public DefaultVenicePartitioner() {
    super();
  }

  public DefaultVenicePartitioner(VeniceProperties props) {
    super(props);
  }

  private int getPartitionId(byte[] keyBytes, int offset, int length, int numPartitions) {

    MessageDigest md = messageDigestThreadLocal.get();
    md.update(keyBytes, offset, length);

    byte[] byteData = md.digest();

    // find partition value from basic modulus algorithm
    int modulo = 0;
    int digit = 0;
    for (int i = 0; i < byteData.length; i++) {
      // Convert byte (-128..127) to int 0..255
      digit = byteData[i] & 0xFF;
      modulo = (modulo * 256 + digit) % numPartitions;
    }

    int partition = Math.abs(modulo % numPartitions);

    if (logger.isDebugEnabled()) {
      logger.debug("Choose partitionId " + partition + " out of " + numPartitions);
    }

    md.reset();
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
