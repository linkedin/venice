package com.linkedin.venice.kafka.partitioner;

import com.linkedin.venice.metadata.NodeCache;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


/**
 * Custom Partitioner Class which is jointly used by Kafka and Venice.
 * Determines the appropriate partition for each message.
 */
public class KafkaPartitioner implements Partitioner {

  static final Logger logger = Logger.getLogger(KafkaPartitioner.class.getName());
  static NodeCache cache = NodeCache.getInstance();

  private final String HASH_ALGORITHM = "MD5";

  public KafkaPartitioner (VerifiableProperties props) {

  }

  /**
  * TODO: consider how to handle partitioning if the partition count changes after startup
  * A consistent hashing algorithm that returns the partitionId based on the key
  * Note that this is based on the number of partitions
  * @param key - A string key that will be hashed into a partition
  * @param numPartitions - The number of total partitions available in Kafka/storage
  * @return The partitionId for which the given key is mapped to
  * */
  public int partition(Object key, int numPartitions) {

    String stringKey = (String) key;

    try {

      MessageDigest md = MessageDigest.getInstance(HASH_ALGORITHM);
      md.update(stringKey.getBytes());

      byte[] byteData = md.digest();

      // find partition value from basic modulus algorithm
      int partition = 0;
      for (int i = 0; i < byteData.length; i++) {
        partition += Math.abs(byteData[i]);
      }

      partition = partition % numPartitions;

      logger.info("Using hash algorithm: " + key + " goes to partitionId " + partition + " out of " + numPartitions);

      md.reset();
      return partition;

    } catch (NoSuchAlgorithmException e) {
      logger.error("Hashing algorithm given is not recognized: " + HASH_ALGORITHM);
      return -1;
    }

  }

}
