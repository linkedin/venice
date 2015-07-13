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
public class DefaultKafkaPartitioner extends KafkaPartitioner {

  static final Logger logger = Logger.getLogger(DefaultKafkaPartitioner.class.getName());

  private String hashAlgorithm;

  public static final String HASH_ALGORITHM_KEY = "partitioner.hash.algorithm";
  public static final String DEFAULT_HASH_ALGORITHM = "MD5";

  public DefaultKafkaPartitioner () {
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
      int partition = 0;
      for (int i = 0; i < byteData.length; i++) {
        partition += Math.abs(byteData[i]);
      }

      partition = partition % numPartitions;

      logger.debug("Using hash algorithm: " + ByteUtils.toHexString(keyBytes) + " goes to partitionId " + partition + " out of " + numPartitions);

      md.reset();
      return partition;
    } catch (NoSuchAlgorithmException e) {
      throw new VeniceException("\"Hashing algorithm given is not recognized: \" + hashAlgorithm");
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
