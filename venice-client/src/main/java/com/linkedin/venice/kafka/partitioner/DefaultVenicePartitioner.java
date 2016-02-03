package com.linkedin.venice.kafka.partitioner;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.utils.ByteUtils;
import java.util.Map;
import org.apache.log4j.Logger;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


/**
 * Default Kafka Partitioner which uses a hashing algorithm (MD5 by default)
 * to determine the appropriate partition for each message.
 */
public class DefaultVenicePartitioner extends VenicePartitioner {

  static final Logger logger = Logger.getLogger(DefaultVenicePartitioner.class.getName());

  private String hashAlgorithm;

  public static final String HASH_ALGORITHM_KEY = "partitioner.hash.algorithm";
  public static final String DEFAULT_HASH_ALGORITHM = "MD5";

  public DefaultVenicePartitioner() {
    hashAlgorithm = DEFAULT_HASH_ALGORITHM;
  }

  @Override
  public int getPartitionId(KafkaKey key, int numPartitions) {
    byte[] keyBytes = key.getKey();

    try {

      MessageDigest md = MessageDigest.getInstance(hashAlgorithm);
      md.update(keyBytes);

      byte[] byteData = md.digest();

      // find partition value from basic modulus algorithm
      int modulo = 0;
      int digit = 0;
      for (int i = 0; i < byteData.length; i++) {
        // Convert byte (-128..127) to int 0..255
        digit =  byteData[i] & 0xFF;
        modulo = (modulo * 256 + digit) % numPartitions;
      }

      int partition = Math.abs(modulo % numPartitions);

      if(logger.isDebugEnabled()) {
        logger.debug("Using hash algorithm: " + ByteUtils.toHexString(keyBytes) + " goes to partitionId " + partition
                + " out of " + numPartitions);
      }

      md.reset();
      return partition;
    } catch (NoSuchAlgorithmException e) {
      throw new VeniceException(" Hashing algorithm given is not recognized: "+ hashAlgorithm);
    }
  }

  @Override
  /**
   * Configure this class with the given key-value pairs.
   * // TODO: Get the properties from .properties file to the Map.
   */
  public void configure(Map<String, ?> configMap) {
    if (configMap.containsKey(HASH_ALGORITHM_KEY)) {
      hashAlgorithm = (String) configMap.get(HASH_ALGORITHM_KEY);
    }
  }
}
