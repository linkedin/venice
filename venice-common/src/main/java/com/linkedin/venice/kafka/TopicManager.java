package com.linkedin.venice.kafka;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import com.linkedin.venice.utils.Time;
import kafka.admin.AdminUtils;
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
       */
      AdminUtils.createTopic(getZkUtils(), topicName, numPartitions, replication, new Properties());
    } catch (TopicExistsException e) {
      logger.warn("Met error when creating kakfa topic: " + topicName, e);
    }
  }

  public void deleteTopic(String topicName) {
    if (listTopics().contains(topicName)) {
      // TODO: Stop using Kafka APIs which depend on ZK.
      logger.info("Deleting topic: " + topicName);
      AdminUtils.deleteTopic(getZkUtils(), topicName);
    } else {
      logger.info("Topic: " +  topicName + " to be deleted doesn't exist");
    }
  }

  public synchronized Set<String> listTopics() {
    Set<String> topics = getConsumer().listTopics().keySet();
    return topics;
  }

  /**
   * Generate a map from partition number to the last offset available for that partition
   * @param topic
   * @return
   */
  public synchronized Map<Integer, Long> getLatestOffsets(String topic) {
    // To be safe, check whether the topic exists or not,
    // since querying offset against non-existing topic could cause endless retrying.
    if (! listTopics().contains(topic)) {
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
      consumer.seekToEnd(topicPartition);
      offsets.put(partition, consumer.position(topicPartition));
    }
    consumer.assign(Arrays.asList());
    return offsets;

  }

  public synchronized void deleteOldTopicsForStore(String storename, int numberOfVersionsToRetain) {
    List<Integer> versionNumbers = listTopics().stream()
        .filter(topic -> topic.startsWith(storename)) /* early cheap filter */
        .filter(topic -> Version.topicIsValidStoreVersion(topic))
        .filter(topic -> Version.parseStoreFromKafkaTopicName(topic).equals(storename))
        .map(topic -> Version.parseVersionFromKafkaTopicName(topic))
        .collect(Collectors.toList());
    Collections.sort(versionNumbers); /* ascending */
    Collections.reverse(versionNumbers); /* descending */
    for (int i=0; i<versionNumbers.size(); i++) {
      if (i < numberOfVersionsToRetain) {
        continue;
      } else {
        String topicToDelete = new Version(storename, versionNumbers.get(i)).kafkaTopicName();
        deleteTopic(topicToDelete);
      }
    }
  }

  public synchronized void deleteTopicsForStoreOlderThanVersion(String storename, int oldestVersionToKeep) {
    listTopics().stream()
        .filter(topic -> topic.startsWith(storename)) /* early cheap filter */
        .filter(topic -> Version.topicIsValidStoreVersion(topic))
        .filter(topic -> Version.parseStoreFromKafkaTopicName(topic).equals(storename))
        .map(topic -> Version.parseVersionFromKafkaTopicName(topic))
        .filter(version -> version < oldestVersionToKeep)
        .map(version -> new Version(storename, version).kafkaTopicName())
        .forEach(topic -> deleteTopic(topic));
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
