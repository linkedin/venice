package com.linkedin.venice.storage;

import com.linkedin.venice.kafka.consumer.KafkaConsumerPartitionManager;
import com.linkedin.venice.kafka.consumer.SimpleKafkaConsumerTask;
import com.linkedin.venice.kafka.consumer.KafkaConsumerException;
import com.linkedin.venice.server.VeniceConfig;

import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Generalized class for a storage node in Venice
 * All storage solutions need to extend this class.
 */
public abstract class VeniceStorageNode {

  static final Logger logger = Logger.getLogger(VeniceStorageNode.class.getName());

  private ExecutorService executor;

  // Map which stores each kafka partition
  private Map<Integer, SimpleKafkaConsumerTask> kafkaPartitions = new HashMap<Integer, SimpleKafkaConsumerTask>();

  /* Constructor required for successful compile */
  public VeniceStorageNode() { }

  /* Declare abstract classes, which require extension*/
  public abstract int getNodeId();

  public abstract boolean containsPartition(int partitionId);

  public abstract void put(int partitionId, String key, Object value) throws VeniceStorageException;

  public abstract Object get(int partitionId, String key) throws VeniceStorageException;

  public abstract void delete(int partitionId, String key) throws VeniceStorageException;

  public abstract void addStoragePartition(int partition_id) throws VeniceStorageException;

  public abstract VeniceStoragePartition removePartition(int partition_id) throws VeniceStorageException;

  /**
  * Adds a partition to the current node. This includes the Kafka Partition and the Storage partition
  * @param partition_id - A unique integer identifier to the current partition
  * */
  public void addPartition(int partition_id) throws VeniceStorageException, KafkaConsumerException {
    logger.info("Registering new partition: " + partition_id + " on nodeId: " + getNodeId());
    addStoragePartition(partition_id);
    addKafkaPartition(partition_id);
  }

  /**
   * Adds a Kafka partition to the current node and launches the given process
   * @param partition_id - The Kafka partition id to consume from
   * */
  public void addKafkaPartition(int partition_id) throws KafkaConsumerException {

    int numThreads = 1;
    // get partition manager
    KafkaConsumerPartitionManager manager = KafkaConsumerPartitionManager.getInstance();
    // Receive and start the consumer task
    SimpleKafkaConsumerTask task = manager.getConsumerTask(this, partition_id);
    // launch each consumer task on a new thread
    executor = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      executor.submit(task);
    }
    kafkaPartitions.put(partition_id, task);

  }

  /**
   * Cleans up the Simple Consumer Service.
   * */
  public void shutdown() {
    logger.error("Shutting down consumer service on nodeId: " + getNodeId());
    if (executor != null) {
      executor.shutdown();
    }
    logger.error("Shutting down storage service on nodeId: " + getNodeId());
    /* Add code to shut down storage services, such as BDB */
  }
}
