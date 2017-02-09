package com.linkedin.venice.kafka;

import com.linkedin.venice.exceptions.VeniceException;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;

import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.common.TopicExistsException;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;


public class TopicManager implements Closeable {

  // Immutable state
  private final String zkConnection;
  private final int sessionTimeoutMs;
  private final int connectionTimeoutMs;

  // Mutable, lazily initialized, state
  private ZkClient zkClient;
  private KafkaConsumer<byte[], byte[]> kafkaConsumer;
  private ZkUtils zkUtils;

  private static final Logger logger = Logger.getLogger(TopicManager.class);
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final int DEFAULT_SESSION_TIMEOUT_MS = 10 * Time.MS_PER_SECOND;
  private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 8 * Time.MS_PER_SECOND;

  public TopicManager(String zkConnection, int sessionTimeoutMs, int connectionTimeoutMs) {
    this.zkConnection = zkConnection;
    this.sessionTimeoutMs = sessionTimeoutMs;
    this.connectionTimeoutMs = connectionTimeoutMs;
  }

  public TopicManager(String zkConnection) {
    this(zkConnection, DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS);
  }

  /** for tests */
  protected TopicManager(String zkConnection, KafkaConsumer<byte[], byte[]> consumer){
    this(zkConnection, DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS);
    this.kafkaConsumer = consumer;
  }

  public void createTopic(String topicName, int numPartitions, int replication) {
    logger.info("Creating topic: " + topicName + " partitions: " + numPartitions + " replication: " + replication);
    try {
      // TODO: Stop using Kafka APIs which depend on ZK.
      /**
       * TODO: consider to increase {@link kafka.server.KafkaConfig.MinInSyncReplicasProp()} to be greater than 1,
       * so Kafka broker won't miss any data when some broker is down.
       *
       *
       * RackAwareMode.Safe: Use rack information if every broker has a rack. Otherwise, fallback to Disabled mode.
       */
      AdminUtils.createTopic(getZkUtils(), topicName, numPartitions, replication, new Properties(), RackAwareMode.Safe$.MODULE$);
    } catch (TopicExistsException e) {
      logger.warn("Met error when creating kakfa topic: " + topicName, e);
    }
  }

  public void deleteTopic(String topicName) {
    if (containsTopic(topicName)) {
      // TODO: Stop using Kafka APIs which depend on ZK.
      logger.info("Deleting topic: " + topicName);
      AdminUtils.deleteTopic(getZkUtils(), topicName);
    } else {
      logger.info("Topic: " +  topicName + " to be deleted doesn't exist");
    }
  }

  /**
   * This function is used to address the following problem:
   * 1. Topic deletion is a async operation in Kafka;
   * 2. Topic deletion triggered by Venice could happen in the middle of other Kafka operation;
   * 3. Kafka operations against non-existing topic will hang;
   * By using this function, the topic deletion is a sync op, which bypasses the hanging issue of
   * non-existing topic operations.
   * Once Kafka addresses the hanging issue of non-existing topic operations, we can safely revert back
   * to use the async version: {@link #deleteTopic(String)}
   * @param topicName
   */
  public synchronized void syncDeleteTopic(String topicName) {
    deleteTopic(topicName);
    if (containsTopic(topicName)) {
      // Since topic deletion is async, we would like to poll until topic doesn't exist any more
      final int SLEEP_MS = 100;
      final int MAX_TIMES = 300; // At most, we will wait 30s (300 * 100ms)
      int current = 0;
      while (++current <= MAX_TIMES) {
        Utils.sleep(SLEEP_MS);
        if (!containsTopic(topicName)) {
          logger.info("Topic: " + topicName + " has been deleted after polling " + current + " times");
          return;
        }
      }
      throw new VeniceException("Failed to delete kafka topic: " + topicName + " after 30 seconds");
    }
  }

  public synchronized Set<String> listTopics() {
    Set<String> topics = getConsumer().listTopics().keySet();
    return topics;
  }

  public boolean containsTopic(String topic) {
    return AdminUtils.topicExists(getZkUtils(), topic);
  }

  /**
   * Generate a map from partition number to the last offset available for that partition
   * @param topic
   * @return
   */
  public synchronized Map<Integer, Long> getLatestOffsets(String topic) {
    // To be safe, check whether the topic exists or not,
    // since querying offset against non-existing topic could cause endless retrying.
    if (! containsTopic(topic)) {
      logger.warn("Topic: " + topic + " doesn't exist, returning empty map for latest offsets");
      return new HashMap<Integer, Long>();
    }
    KafkaConsumer<byte[], byte[]> consumer = getConsumer();
    List<PartitionInfo> partitions = consumer.partitionsFor(topic);
    if (null == partitions) {
      logger.warn("Topic: " + topic + " has a null partition set, returning empty map for latest offsets");
      return new HashMap<Integer, Long>();
    }
    Map<Integer, Long> offsets = new HashMap<>();

    for (PartitionInfo partitionInfo : partitions) {
      int partition = partitionInfo.partition();
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      consumer.assign(Arrays.asList(topicPartition));
      consumer.seekToEnd(Arrays.asList(topicPartition));
      offsets.put(partition, consumer.position(topicPartition));
    }
    consumer.assign(Arrays.asList());
    return offsets;

  }

  /**
   * The first time this is called, it lazily initializes {@link #kafkaConsumer}.
   *
   * @return The internal {@link KafkaConsumer} instance.
   */
  private synchronized KafkaConsumer<byte[], byte[]> getConsumer() {
    if (this.kafkaConsumer == null) {
      Properties props = new Properties();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerListFromZk());
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
      // Increase receive buffer to 1MB to check whether it can solve the metadata timing out issue
      props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
      this.kafkaConsumer = new KafkaConsumer<>(props);
    }
    return this.kafkaConsumer;
  }

  private String brokerListFromZk() {
    StringJoiner brokers = new StringJoiner(",");
    List<String> brokerIds = getZkClient().getChildren(ZkUtils.BrokerIdsPath());
    for (String brokerId : brokerIds) {
      String brokerJson = getZkClient().readData(ZkUtils.BrokerIdsPath() + "/" + brokerId);

      try {
        Map<String, Object> brokerData = mapper.readValue(brokerJson, Map.class);
        brokers.add(brokerData.get("host") + ":" + brokerData.get("port"));
      } catch (IOException e) {
        System.err.println("Cannot parse broker data: " + brokerJson);
        continue;
      }
    }
    return brokers.toString();
  }

  /**
   * The first time this is called, it lazily initializes {@link #zkClient}.
   *
   * @return The internal {@link ZkClient} instance.
   */
  private synchronized ZkClient getZkClient() {
    if (this.zkClient == null) {
      this.zkClient = new ZkClient(zkConnection, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
    }
    return this.zkClient;
  }

  /**
   * The first time this is called, it lazily initializes {@link #zkUtils}.
   *
   * @return The internal {@link ZkUtils} instance.
   */
  private synchronized ZkUtils getZkUtils() {
    if (this.zkUtils == null) {
      this.zkUtils = new ZkUtils(getZkClient(), new ZkConnection(zkConnection), false);
    }
    return this.zkUtils;
  }

  @Override
  public synchronized void close() throws IOException {
    IOUtils.closeQuietly(kafkaConsumer);
    // does not implement closeable, so we're doing it the old-school way
    if (this.zkClient != null) {
      zkClient.close();
    }
  }

}
