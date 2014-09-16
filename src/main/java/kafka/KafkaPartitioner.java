package kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Created by clfung on 9/12/14.
 */
public class KafkaPartitioner implements Partitioner {

  public KafkaPartitioner (VerifiableProperties props) {

  }

  // TODO: Write custom partitioner
  public int partition(Object key, int a_numPartitions) {

    int partition = 0;
    String stringKey = (String) key;
    int offset = stringKey.lastIndexOf('.');

    if (offset > 0) {
      partition = Integer.parseInt( stringKey.substring(offset+1)) % a_numPartitions;
    }

    return partition;

  }

  // TODO: Design an API for the KeyCache and the Storage to use
  public int getPartition(Object key) {
      return 0;
  }

}
