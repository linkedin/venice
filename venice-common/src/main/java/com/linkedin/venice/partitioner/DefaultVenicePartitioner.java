package com.linkedin.venice.partitioner;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.ByteUtils;

import com.linkedin.venice.utils.VeniceProperties;
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

  public int getPartitionId(byte[] keyBytes, int numPartitions) {

    MessageDigest md = messageDigestThreadLocal.get();
    md.update(keyBytes);

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
      logger.debug("Using hash algorithm: " + ByteUtils.toLogString(keyBytes) + " goes to partitionId " + partition
          + " out of " + numPartitions);
    }

    md.reset();
    return partition;

  }
}
