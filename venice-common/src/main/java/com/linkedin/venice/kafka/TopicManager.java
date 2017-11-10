package com.linkedin.venice.kafka;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.common.TopicAlreadyMarkedForDeletionException;
import kafka.log.LogConfig;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * Topic Manager is shared by multiple cluster's controllers running in one physical Venice controller instance.
 */
public class TopicManager implements Closeable {

  // Immutable state
  private final String zkConnection;
  private final int sessionTimeoutMs;
  private final int connectionTimeoutMs;
  private final int kafkaOperationTimeoutMs;

  // Mutable, lazily initialized, state
  private ZkClient zkClient;
  private KafkaConsumer<byte[], byte[]> kafkaConsumer;
  private ZkUtils zkUtils;

  private static final Logger logger = Logger.getLogger(TopicManager.class);
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final int DEFAULT_SESSION_TIMEOUT_MS = 10 * Time.MS_PER_SECOND;
  private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 8 * Time.MS_PER_SECOND;
  private static final int DEFAULT_KAFKA_OPERATION_TIMEOUT_MS = 30 * Time.MS_PER_SECOND;

  public TopicManager(String zkConnection, int sessionTimeoutMs, int connectionTimeoutMs, int kafkaOperationTimeoutMs) {
    this.zkConnection = zkConnection;
    this.sessionTimeoutMs = sessionTimeoutMs;
    this.connectionTimeoutMs = connectionTimeoutMs;
    this.kafkaOperationTimeoutMs = kafkaOperationTimeoutMs;
  }

  public TopicManager(String zkConnection, int kafkaOperationTimeoutMs) {
    this(zkConnection, DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS, kafkaOperationTimeoutMs);
  }

  public TopicManager(String zkConnection) {
    this(zkConnection, DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS, DEFAULT_KAFKA_OPERATION_TIMEOUT_MS);
  }

  /** for tests */
  protected TopicManager(String zkConnection, KafkaConsumer<byte[], byte[]> consumer){
    this(zkConnection, DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS, DEFAULT_KAFKA_OPERATION_TIMEOUT_MS);
    this.kafkaConsumer = consumer;
  }

  @Deprecated
  public void createTopic(String topicName, int numPartitions, int replication) {
    createTopic(topicName, numPartitions, replication, true);
  }

  /**
   * Create a topic, and block until the topic is created (with a 25 second timeout after which this function will throw a VeniceException)
   * @param topicName Name for the new topic
   * @param numPartitions number of partitions
   * @param replication replication factor
   * @param eternal whether the topic should have infinite (~250 mil years) retention
   */
  public void createTopic(String topicName, int numPartitions, int replication, boolean eternal) {
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
      Properties topicProperties = new Properties();
      if (eternal) {
        topicProperties.put(LogConfig.RetentionMsProp(), Long.toString(Long.MAX_VALUE));
      } // TODO: else apply buffer topic configured retention.
      topicProperties.put(LogConfig.MessageTimestampTypeProp(), "LogAppendTime"); // Just in case the Kafka cluster isn't configured as expected.
      AdminUtils.createTopic(getZkUtils(), topicName, numPartitions, replication, topicProperties, RackAwareMode.Safe$.MODULE$);
      long startTime = System.currentTimeMillis();
      while (!containsTopic(topicName, numPartitions)) {
        if (System.currentTimeMillis() > startTime + kafkaOperationTimeoutMs) {
          throw new VeniceOperationAgainstKafkaTimedOut("Timeout while creating topic: " + topicName + ".  Topic still does not exist after " + kafkaOperationTimeoutMs + "ms.");
        }
        Utils.sleep(200);
      }
    } catch (TopicExistsException e) {
      logger.warn("Met error when creating kakfa topic: " + topicName, e);
    }
  }

  /**
   * If the topic exists, this method sends a delete command to Kafka and immediately returns.  Deletion will
   * occur asynchronously.
   * @param topicName
   */
  public void ensureTopicIsDeletedAsync(String topicName) {
    if (containsTopic(topicName)) {
      // TODO: Stop using Kafka APIs which depend on ZK.
      logger.info("Deleting topic: " + topicName);
      try {
        AdminUtils.deleteTopic(getZkUtils(), topicName);
      } catch (TopicAlreadyMarkedForDeletionException e) {
        logger.warn("Topic delete requested, but topic already marked for deletion");
      }
    } else {
      logger.info("Topic: " +  topicName + " to be deleted doesn't exist");
    }
  }

  public int getReplicationFactor(String topicName){
    return AdminUtils.fetchTopicMetadataFromZk(topicName, getZkUtils()).partitionMetadata().get(0).replicas().size();
  }

  /**
   * Update retention for the given topic.
   * If the topic doesn't exist, this operation will throw {@link TopicDoesNotExistException}
   * @param topicName
   * @param retentionInMS
   */
  public synchronized void updateTopicRetention(String topicName, long retentionInMS) {
    Properties topicProperties = getTopicConfig(topicName);
    String retentionInMSStr = Long.toString(retentionInMS);
    if (!topicProperties.containsKey(LogConfig.RetentionMsProp()) || // config doesn't exist
        !topicProperties.getProperty(LogConfig.RetentionMsProp()).equals(retentionInMSStr)) { // config is different
      topicProperties.put(LogConfig.RetentionMsProp(), Long.toString(retentionInMS));
      AdminUtils.changeTopicConfig(getZkUtils(), topicName, topicProperties);
    }
  }

  public void updateTopicRetentionToBeZero(String topicName) {
    updateTopicRetention(topicName, 0);
  }

  /**
   * Check whether current topic retention is 0
   * @param topicName
   * @return
   */
  public synchronized boolean isTopicRetentionZero(String topicName) {
    Properties topicProperties = getTopicConfig(topicName);
    if (topicProperties.containsKey(LogConfig.RetentionMsProp())) {
      long retention = Long.parseLong(topicProperties.getProperty(LogConfig.RetentionMsProp()));
      return retention == 0;
    }
    return false;
  }

  /**
   * This operation is a little heavy, since it will pull the configs for all the topics.
   * @param topicName
   * @return
   */
  public Properties getTopicConfig(String topicName) {
    scala.collection.Map<String, Properties> allTopicConfigs = AdminUtils.fetchAllTopicConfigs(getZkUtils());
    if (allTopicConfigs.contains(topicName)) {
      Properties topicProperties = allTopicConfigs.get(topicName).get();
      return topicProperties;
    } else {
      throw new TopicDoesNotExistException("Topic: " + topicName + " doesn't exist");
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
   * to use the async version: {@link #ensureTopicIsDeletedAsync(String)}
   * @param topicName
   */
  public synchronized void ensureTopicIsDeletedAndBlock(String topicName) {
    ensureTopicIsDeletedAsync(topicName);
    if (containsTopic(topicName)) {
      // Since topic deletion is async, we would like to poll until topic doesn't exist any more
      final int SLEEP_MS = 100;
      final int MAX_TIMES = kafkaOperationTimeoutMs / SLEEP_MS;
      int current = 0;
      while (++current <= MAX_TIMES) {
        Utils.sleep(SLEEP_MS);
        if (!containsTopic(topicName)) {
          logger.info("Topic: " + topicName + " has been deleted after polling " + current + " times");
          return;
        }
      }
      throw new VeniceOperationAgainstKafkaTimedOut("Failed to delete kafka topic: " + topicName + " after " + kafkaOperationTimeoutMs + " ms (" + current + " attempts).");
    }
  }

  public synchronized Set<String> listTopics() {
    Set<String> topics = getConsumer().listTopics().keySet();
    return topics;
  }

  /**
   * @see {@link #containsTopic(String, Integer)}
   */
  public boolean containsTopic(String topic) {
    return containsTopic(topic, null);
  }

  /**
   * This is an extensive check to mitigate an edge-case where a topic is only created in ZK but not in the brokers.
   *
   * @return true if the topic exists and all its partitions have at least one in-sync replica
   */
  public boolean containsTopic(String topic, Integer expectedPartitionCount) {
    // TODO: Decide if we should get rid of this first ZK check.
    // For now, we leave it just for increased visibility into the system.
    boolean zkMetadataCreatedForTopic = AdminUtils.topicExists(getZkUtils(), topic);
    if (!zkMetadataCreatedForTopic) {
      logger.info("AdminUtils.topicExists() returned false because the ZK path doesn't exist yet for topic: " + topic);
      return false;
    }

    List<PartitionInfo> partitionInfoList = getConsumer().partitionsFor(topic);
    if (partitionInfoList == null) {
      logger.error("getConsumer().partitionsFor() returned null for topic: " + topic);
      return false;
    }

    if (expectedPartitionCount != null && partitionInfoList.size() != expectedPartitionCount) {
      // Unexpected. Should perhaps even throw...
      logger.error("getConsumer().partitionsFor() returned the wrong number of partitions for topic: " + topic +
          ", expectedPartitionCount: " + expectedPartitionCount +
          ", actual size: " + partitionInfoList.size() +
          ", partitionInfoList: " + Arrays.toString(partitionInfoList.toArray()));
      return false;
    }

    boolean allPartitionsHaveAnInSyncReplica = partitionInfoList.stream()
        .allMatch(partitionInfo -> partitionInfo.inSyncReplicas().length > 0);
    if (!allPartitionsHaveAnInSyncReplica) {
      logger.info("getConsumer().partitionsFor() returned some partitionInfo with no in-sync replica for topic: " + topic +
          ", partitionInfoList: " + Arrays.toString(partitionInfoList.toArray()));
    } else {
      logger.info("The following topic has the at least one in-sync replica for each partition: " + topic);
    }
    return allPartitionsHaveAnInSyncReplica;
  }

  /**
   * Generate a map from partition number to the last offset available for that partition
   * @param topic
   * @return a Map of partition to latest offset, or an empty map if there's any problem
   */
  public synchronized Map<Integer, Long> getLatestOffsets(String topic) {
    // To be safe, check whether the topic exists or not,
    // since querying offset against non-existing topic could cause endless retrying.
    if (!containsTopic(topic)) {
      logger.warn("Topic: " + topic + " doesn't exist, returning empty map for latest offsets");
      return new HashMap<>();
    }
    KafkaConsumer<byte[], byte[]> consumer = getConsumer();
    List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
    if (null == partitionInfoList || partitionInfoList.isEmpty()) {
      logger.warn("Unexpected! Topic: " + topic + " has a null partition set, returning empty map for latest offsets");
      return new HashMap<>();
    }

    Map<Integer, Long> latestOffsets = partitionInfoList.stream()
        .map(pi -> pi.partition())
        .collect(Collectors.toMap(p -> p, p -> getLatestOffset(consumer, topic, p, false)));

    return latestOffsets;
  }

  public synchronized long getLatestOffset(String topic, int partition) throws TopicDoesNotExistException {
    return getLatestOffset(getConsumer(), topic, partition, true);
  }

  private synchronized Long getLatestOffset(KafkaConsumer<byte[], byte[]> consumer, String topic, Integer partition, boolean doTopicCheck) throws TopicDoesNotExistException {
    if (doTopicCheck && !containsTopic(topic)) {
      throw new TopicDoesNotExistException("Topic " + topic + " does not exist!");
    }
    if (partition < 0) {
      throw new IllegalArgumentException("Cannot retrieve latest offsets for invalid partition " + partition);
    }
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    consumer.assign(Arrays.asList(topicPartition));
    consumer.seekToEnd(Arrays.asList(topicPartition));
    long latestOffset = consumer.position(topicPartition);
    consumer.assign(Arrays.asList());
    return latestOffset;
  }

  public synchronized Map<Integer, Long> getOffsetsByTime(String topic, long timestamp) {
    int remainingAttempts = 5;
    List<PartitionInfo> partitionInfoList = getConsumer().partitionsFor(topic);
    // N.B.: During unit test development, getting a null happened occasionally without apparent
    //       reason. In their current state, the tests have been run with a high invocationCount and
    //       no failures, so it may be a non-issue. If this happens again, and we find some test case
    //       that can reproduce it, we may want to try adding a short amount of retries, and reporting
    //       a bug to Kafka.
    while (remainingAttempts > 0 && (null == partitionInfoList || partitionInfoList.isEmpty())) {
      Utils.sleep(500);
      partitionInfoList = getConsumer().partitionsFor(topic);
      remainingAttempts -= 1;
    }
    if (null == partitionInfoList || partitionInfoList.isEmpty()) {
      throw new VeniceException("Cannot get partition info for topic: " + topic + ", partitionInfoList: " + partitionInfoList);
    } else {
      Map<TopicPartition, Long> timestampsToSearch = partitionInfoList.stream()
          .collect(Collectors.toMap(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()), ignoredParam -> timestamp));

      Map<Integer, Long>  result = getConsumer().offsetsForTimes(timestampsToSearch)
          .entrySet()
          .stream()
          .collect(Collectors.toMap(
              partitionToOffset ->
                  Utils.notNull(partitionToOffset.getKey(),"Got a null TopicPartition key out of the offsetsForTime API")
                      .partition(),
              partitionToOffset -> {
                Optional<Long> offsetOptional = Optional.ofNullable(partitionToOffset.getValue()).map(offset -> offset.offset());
                if (offsetOptional.isPresent()){
                  return offsetOptional.get();
                } else {
                  return getOffsetByTimeIfOutOfRange(partitionToOffset, timestamp);
                }
              }));
      // The given timestamp exceed the timestamp of the last message. So return the last offset.
      // TODO we might get partial result that the map does not have offset for some partition, we need a way to fix it.
      if (result.isEmpty()) {
        result = getConsumer().endOffsets(timestampsToSearch.keySet())
            .entrySet()
            .stream()
            .collect(Collectors.toMap(partitionToOffset -> Utils.notNull(partitionToOffset).getKey().partition(),
                partitionToOffset -> partitionToOffset.getValue()));
      }
      return result;
    }
  }

  /**
   * Kafka's get offset by time API returns null if the requested time is before the first record OR after
   * the last record.  This method queries the time of the last message and compares it to the requested
   * timestamp in order to return either offset 0 or the last offset.
   */
  private synchronized long getOffsetByTimeIfOutOfRange(Map.Entry<TopicPartition, OffsetAndTimestamp> partitionToOffset, long timestamp){
    TopicPartition topicPartition = partitionToOffset.getKey();
    long latestOffset = getLatestOffset(topicPartition.topic(), topicPartition.partition());
    if (latestOffset <= 0) {
      return 0L;
    }
    KafkaConsumer consumer = getConsumer();
    try {
      consumer.assign(Arrays.asList(topicPartition));
      consumer.seek(topicPartition, latestOffset - 1);
      ConsumerRecords records = consumer.poll(1000L);
      if (records.isEmpty()) {
        return 0L;
      }
      ConsumerRecord record = (ConsumerRecord) records.iterator().next();
      if (timestamp > record.timestamp()) {
        return latestOffset;
      } else {
        return 0L;
      }
    } finally {
      consumer.assign(Arrays.asList());
    }
  }

  /**
   * Get a list of {@link PartitionInfo} objects for the specified topic.
   * @param topic
   * @return
   */
  public synchronized List<PartitionInfo> getPartitions(String topic){
    KafkaConsumer<byte[], byte[]> consumer = getConsumer();
    return consumer.partitionsFor(topic);
  }

  /**
   * The first time this is called, it lazily initializes {@link #kafkaConsumer}.
   * Any method that uses this consumer must be synchronized.  Since the same
   * consumer is returned each time and some methods modify the consumer's state
   * we must guard against concurrent modifications.
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
