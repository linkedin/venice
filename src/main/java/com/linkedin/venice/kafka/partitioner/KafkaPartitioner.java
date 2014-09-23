package com.linkedin.venice.kafka.partitioner;

import com.linkedin.venice.metadata.NodeCache;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by clfung on 9/12/14.
 */
public class KafkaPartitioner implements Partitioner {

  static final Logger logger = Logger.getLogger(KafkaPartitioner.class.getName());
  static NodeCache cache = NodeCache.getInstance();

  public KafkaPartitioner (VerifiableProperties props) {

  }

  /*
  * TODO: consider how to handle partitioning if the partition count changes
  * A consistent hashing algorithm that returns the partitionId based on the key
  * Note that this is based on the number of partitions
  * */
  public int partition(Object key, int numPartitions) {

    String stringKey = (String) key;

    try {

      MessageDigest md = MessageDigest.getInstance("MD5");
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
      logger.error(e);
      return 0;
    }

  }

}
