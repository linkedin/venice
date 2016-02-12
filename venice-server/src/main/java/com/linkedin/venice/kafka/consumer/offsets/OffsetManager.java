package com.linkedin.venice.kafka.consumer.offsets;

import com.linkedin.venice.config.VeniceClusterConfig;
import com.linkedin.venice.exceptions.VeniceException;


/**
 * This class records the offset for every pair(topic,partition) this node is responsible for. It provides APIs that can
 * query the last consumed offset for a specific (topic,partition) pair.
 *
 * This class should not checkpoint/flush to disk the offset for every request. Rather this should be an in memory
 * operation and should be flushed to disk in a certain interval like 5 seconds or so by a background process like bdb
 * checkpointer thread.
 *
 *
 * TODO: offset manager should also be designed in case when there is a rebalance and the partition assignments to nodes
 * in the cluster are changed.
 */
public abstract class OffsetManager {

  public static final String OFFSETS_STORE_NAME="offsets_store";

  protected final VeniceClusterConfig veniceClusterConfig;

  public OffsetManager(VeniceClusterConfig veniceClusterConfig) {
    this.veniceClusterConfig = veniceClusterConfig;
  }

  /**
   * Records the offset with underlying/external storage. Persistence to disk happens in configurable time interval by a
   * background thread. For example in case of BDB the check pointer thread can be configured to do this.
   *
   * @param topicName  kafka topic to which the consumer thread is registered to.
   * @param partitionId kafka partition id for which the consumer thread is registered to.
   * @param record OffSetRecord containing last read offset for the topic and partition combination.
   */
  public abstract void recordOffset(String topicName, int partitionId, OffsetRecord record)
      throws VeniceException;


  /**
   * Gets the Last Known persisted offset of this consumer.
   *
   *
   * @param topicName  kafka topic to which the consumer thread is registered to.
   * @param partitionId  kafka partition id for which the consumer thread is registered to.
   * @return  OffsetRecord  - contains offset and time when it was recorded before the consumer thread went down.
   * consumer
   */
  public abstract OffsetRecord getLastOffset(String topicName, int partitionId)
      throws VeniceException;


  public void shutdown()
      throws Exception {

  }
}
