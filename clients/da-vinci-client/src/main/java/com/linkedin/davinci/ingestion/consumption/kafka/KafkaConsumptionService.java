package com.linkedin.davinci.ingestion.consumption.kafka;

import com.linkedin.davinci.ingestion.consumption.ConsumptionService;
import com.linkedin.davinci.kafka.consumer.StoreIngestionTask;
import com.linkedin.venice.annotation.Threadsafe;
import org.apache.kafka.clients.consumer.ConsumerRecords;


/**
 * A Kafka-specific {@link ConsumptionService}. It consumes from Kafka topic partitions. Message queue is {@link TopicPartitionWithURL}
 * and consumed data type is {@link ConsumerRecords}. It should a singleton, meaning one per storage node. It can be
 * started/stopped and its lifecycle should be sync-ed up with that of a storage node.
 *
 * Implementation of this abstract class should be what {@link StoreIngestionTask} uses (even though a {@link ConsumptionService}
 * is passed to {@link StoreIngestionTask}).
 *
 *  @param <K> Type of the key consumed from Kafka.
 *  @param <V> Type of the value consumed from Kafka.
 */
@Threadsafe
public abstract class KafkaConsumptionService<K, V>
    implements ConsumptionService<TopicPartitionWithURL, ConsumerRecords<K, V>> {
  /**
   * When it is started, it may start all internal threads, set stuff up, etc. Note that this method is intentionally NOT
   * put on the {@link ConsumptionService} interface which {@link StoreIngestionTask} interacts with directly. Because
   * {@link StoreIngestionTask} should not be able to start nor stop a {@link ConsumptionService}.
   */
  abstract void start();

  /**
   * When it is started, it shuts down all internal threads, clean stuff up, etc.
   */
  abstract void stop();
}
