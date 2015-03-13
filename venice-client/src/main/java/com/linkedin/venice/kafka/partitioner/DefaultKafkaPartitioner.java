package com.linkedin.venice.kafka.partitioner;

import com.linkedin.venice.utils.ByteUtils;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


/**
 * Default Kafka Partitioner which uses a hashing algorithm (MD5 by default)
 * to determine the appropriate partition for each message.
 */
public class DefaultKafkaPartitioner extends KafkaPartitioner {

  static final Logger logger = Logger.getLogger(DefaultKafkaPartitioner.class.getName());

  private final String hashAlgorithm;

  public static final String HASH_ALGORITHM_KEY = "partitioner.hash.algorithm";
  public static final String DEFAULT_HASH_ALGORITHM = "MD5";

  public DefaultKafkaPartitioner(VerifiableProperties props) {
    super(props);
    hashAlgorithm = props.getString(HASH_ALGORITHM_KEY, DEFAULT_HASH_ALGORITHM);
  }

  // TODO: consider how to handle partitioning if the partition count changes after startup
  @Override
  public int partition(Object key, int numPartitions) {

    byte[] keyBytes = (byte[]) key;
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

      logger.info("Using hash algorithm: " + ByteUtils.toHexString(keyBytes) + " goes to partitionId " + partition + " out of " + numPartitions);

      md.reset();
      return partition;
    } catch (NoSuchAlgorithmException e) {
      logger.error("Hashing algorithm given is not recognized: " + hashAlgorithm);
      return -1;
    }
  }
}
