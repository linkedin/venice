package com.linkedin.venice.kafka.consumer;

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
public class OffsetManager {


  public OffsetManager(){
    /** TODO initialize and start BDB database.
     *
     * This is where we can decide one of the two models:
     * 1. A single metadata database per store ( the number of records in this database will be the number of
     * partitions served by this node for that store)
     * 2. (OR) one single metadata database for all stores( here the number of records in the database will be the total
     * number of partitions served by this node)
     *
     * For now we will be going with approach 1. A new environment is created and passed on to the BDBStoragePartition
     * class that will open or create a bdb database as needed. This database will be referenced as the
     * OffsetMetadataStore and is local to this node. Please note that this is not a regular Venice Store. A router or
     * an admin service would need to know the node id to query the metadata on that node.
     *
     * The config properties for this bdb environment & database need to be tweaked to support the offset storage and
     * persistence requirements.
     *
     */
  }

  public void recordOffset(String topicName, int partitionId, long offset){
    // TODO Fill code that will persist the offset for the pair ( topic, partition ) and will throw Exception if any.
  }

  public long getLastOffset(String topicName, int partitionId){
    /**
     * TODO Fill code that will fetch the last consumed offset for the (topic, partition) pair in case of a restart or
     * failure.
     *
     * It should also take care to handle if the consumer is freshly registered where there would be no last offset
     * consumed.
    */
    return -1L;
  }

}
