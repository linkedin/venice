package com.linkedin.venice.kafka;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.consumer.VeniceConsumerFactory;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
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
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * Topic Manager is shared by multiple cluster's controllers running in one physical Venice controller instance.
 *
 * This class contains one global {@link KafkaConsumer}, which is not thread-safe, so when you add new functions,
 * which is using this global consumer, please add 'synchronized' keyword, otherwise this {@link TopicManager}
 * won't be thread-safe, and Kafka consumer will report the following error when multiple threads are trying to
 * use the same consumer: KafkaConsumer is not safe for multi-threaded access.
 */
public class TopicManager implements Closeable {

  // Immutable state
  private final String zkConnection;
  private final int sessionTimeoutMs;
  private final int connectionTimeoutMs;
  private final int kafkaOperationTimeoutMs;
  private final VeniceConsumerFactory veniceConsumerFactory;

  // Mutable, lazily initialized, state
  private ZkClient zkClient;
  private KafkaConsumer<byte[], byte[]> kafkaConsumer;
  private ZkUtils zkUtils;

  private static final Logger logger = Logger.getLogger(TopicManager.class);
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final int DEFAULT_SESSION_TIMEOUT_MS = 10 * Time.MS_PER_SECOND;
  private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 8 * Time.MS_PER_SECOND;
  private static final int DEFAULT_KAFKA_OPERATION_TIMEOUT_MS = 30 * Time.MS_PER_SECOND;
  protected static final long UNKNOWN_TOPIC_RETENTION = Long.MIN_VALUE;
  protected static final long DEFAULT_TOPIC_RETENTION_POLICY_MS = TimeUnit.DAYS.toMillis(5); // 5 days
  protected static final long ETERNAL_TOPIC_RETENTION_POLICY_MS = Long.MAX_VALUE;


  public TopicManager(String zkConnection, int sessionTimeoutMs, int connectionTimeoutMs, int kafkaOperationTimeoutMs,
      VeniceConsumerFactory veniceConsumerFactory) {
    this.zkConnection = zkConnection;
    this.sessionTimeoutMs = sessionTimeoutMs;
    this.connectionTimeoutMs = connectionTimeoutMs;
    this.kafkaOperationTimeoutMs = kafkaOperationTimeoutMs;
    this.veniceConsumerFactory = veniceConsumerFactory;
  }

  public TopicManager(String zkConnection, int kafkaOperationTimeoutMs, VeniceConsumerFactory veniceConsumerFactory) {
    this(zkConnection, DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS, kafkaOperationTimeoutMs,
        veniceConsumerFactory);
  }

  public TopicManager(String zkConnection, VeniceConsumerFactory veniceConsumerFactory) {
    this(zkConnection, DEFAULT_SESSION_TIMEOUT_MS, DEFAULT_CONNECTION_TIMEOUT_MS, DEFAULT_KAFKA_OPERATION_TIMEOUT_MS,
        veniceConsumerFactory);
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
        topicProperties.put(LogConfig.RetentionMsProp(), Long.toString(ETERNAL_TOPIC_RETENTION_POLICY_MS));
      }  else {
        topicProperties.put(LogConfig.RetentionMsProp(), Long.toString(DEFAULT_TOPIC_RETENTION_POLICY_MS));
      }
      topicProperties.put(LogConfig.MessageTimestampTypeProp(), "LogAppendTime"); // Just in case the Kafka cluster isn't configured as expected.
      long startTime = System.currentTimeMillis();
      boolean asyncCreateOperationSucceeded = false;
      while (!asyncCreateOperationSucceeded) {
        try {
            AdminUtils.createTopic(getZkUtils(), topicName, numPartitions, replication, topicProperties, RackAwareMode.Safe$.MODULE$);
            asyncCreateOperationSucceeded = true;
        } catch (InvalidReplicationFactorException e) {
          if (System.currentTimeMillis() > startTime + kafkaOperationTimeoutMs) {
            throw new VeniceOperationAgainstKafkaTimedOut("Timeout while creating topic: " + topicName + ". Topic still does not exist after " + kafkaOperationTimeoutMs + "ms.", e);
          } else {
            logger.error("Kafka failed to kick off topic creation because it appears to be under-replicated... Will treat it as a transient error and attempt to ride over it.", e);
            Utils.sleep(200);
          }
        }
      }
      while (!containsTopic(topicName, numPartitions)) {
        if (System.currentTimeMillis() > startTime + kafkaOperationTimeoutMs) {
          throw new VeniceOperationAgainstKafkaTimedOut("Timeout while creating topic: " + topicName + ".  Topic still does not exist after " + kafkaOperationTimeoutMs + "ms.");
        }
        Utils.sleep(200);
      }
      logger.info("Successfully created " + (eternal ? "eternal " : "") + "topic: " + topicName);
    } catch (TopicExistsException e) {
      long retentionPolicyInMs = eternal ? ETERNAL_TOPIC_RETENTION_POLICY_MS : DEFAULT_TOPIC_RETENTION_POLICY_MS;
      logger.info("Topic: " + topicName + " already exists, will update retention policy.");
      updateTopicRetention(topicName, retentionPolicyInMs);
      logger.info("Updated retention policy to be " + retentionPolicyInMs + "ms for topic: " + topicName);
    }
  }

  /**
   * If the topic exists, this method sends a delete command to Kafka and immediately returns.  Deletion will
   * occur asynchronously.
   * @param topicName
   */
  private void ensureTopicIsDeletedAsync(String topicName) {
    if (!isTopicFullyDeleted(topicName, false)) {
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
   return getReplicationFactor(topicName, getZkUtils());
  }

  public static int getReplicationFactor(String topicName, ZkUtils zkUtils) {
    Set<String> topicSet = new HashSet<>();
    topicSet.add(topicName);
    scala.collection.Set metadataSet = AdminUtils.fetchTopicMetadataFromZk(new scala.collection.immutable.Set.Set1<>(topicName),
        zkUtils, ListenerName.forSecurityProtocol(SecurityProtocol.SSL));
    if (metadataSet.isEmpty()) {
      throw new VeniceException("Couldn't fetch topic metadata from Kafka Zookeeper for topic: " + topicName);
    }
    if (metadataSet.size() > 1) {
      logger.warn("More than one entry returned: " + metadataSet + " for single topic lookup: " + topicName);
    }
    MetadataResponse.TopicMetadata topicMetadata = (MetadataResponse.TopicMetadata)metadataSet.iterator().next();
    return topicMetadata.partitionMetadata().get(0).replicas().size();
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

  public Map<String, Long> getAllTopicRetentions() {
    scala.collection.Map<String, Properties> allTopicConfigs = AdminUtils.fetchAllTopicConfigs(getZkUtils());
    Map<String, Long> topicRetentions = new HashMap<>();
    Map<String, Properties> allTopicConfigsJavaMap = scala.collection.JavaConversions.asJavaMap(allTopicConfigs);
    allTopicConfigsJavaMap.forEach( (topic, topicProperties) -> {
      if (topicProperties.containsKey(LogConfig.RetentionMsProp())) {
        topicRetentions.put(topic, Long.valueOf(topicProperties.getProperty(LogConfig.RetentionMsProp())));
      } else {
        topicRetentions.put(topic, UNKNOWN_TOPIC_RETENTION);
      }
    });
    return topicRetentions;
  }

  public long getTopicRetention(String topicName) {
    Properties topicProperties = getTopicConfig(topicName);
    if (topicProperties.containsKey(LogConfig.RetentionMsProp())) {
      return Long.parseLong(topicProperties.getProperty(LogConfig.RetentionMsProp()));
    }
    return UNKNOWN_TOPIC_RETENTION;
  }

  public boolean isTopicTruncated(String topicName, long truncatedTopicMaxRetentionMs) {
    return isRetentionBelowTruncatedThreshold(getTopicRetention(topicName), truncatedTopicMaxRetentionMs);
  }

  public boolean isRetentionBelowTruncatedThreshold(long retention, long truncatedTopicMaxRetentionMs) {
    return retention != UNKNOWN_TOPIC_RETENTION && retention <= truncatedTopicMaxRetentionMs;
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
   *
   * It is intentional to make this function to be non-synchronized since it could lock
   * {@link TopicManager} for a pretty long time (up to 30 seconds) if topic deletion is slow.
   * When topic deletion slowness happens, it will cause other operations, such as {@link #getLatestOffsets(String)}
   * to be blocked for a long time, and this could cause push job failure.
   *
   * Even with non-synchronized function, Venice could still guarantee there will be only one topic
   * deletion at one time since all the topic deletions are handled by topic cleanup service serially.
   *
   * @param topicName
   */
  public void ensureTopicIsDeletedAndBlock(String topicName) {
    if (!containsTopic(topicName)) {
      // Topic doesn't exist
      return;
    }
    // This is trying to guard concurrent topic deletion in Kafka.
    if (getZkUtils().getChildrenParentMayNotExist(ZkUtils.DeleteTopicsPath()).size() > 0) {
      throw new VeniceException("Delete operation already in progress! Try again later.");
    }
    ensureTopicIsDeletedAsync(topicName);
    // Since topic deletion is async, we would like to poll until topic doesn't exist any more
    final int SLEEP_MS = 100;
    final int MAX_TIMES = kafkaOperationTimeoutMs / SLEEP_MS;
    final int MAX_CONSUMER_RECREATION_INTERVAL = 100;
    int current = 0;
    int lastConsumerRecreation = 0;
    int consumerRecreationInterval = 5;
    while (++current <= MAX_TIMES) {
      Utils.sleep(SLEEP_MS);
      // Re-create consumer every once in a while, in case it's wedged on some stale state.
      boolean closeAndRecreateConsumer = (current - lastConsumerRecreation) == consumerRecreationInterval;
      if (closeAndRecreateConsumer) {
        // Exponential back-off: 0.5s, 1s, 2s, 4s, 8s, 10s (max)
        lastConsumerRecreation = current;
        consumerRecreationInterval = Math.max(consumerRecreationInterval * 2, MAX_CONSUMER_RECREATION_INTERVAL);
      }
      if (isTopicFullyDeleted(topicName, closeAndRecreateConsumer)) {
        logger.info("Topic: " + topicName + " has been deleted after polling " + current + " times");
        return;
      }
    }
    throw new VeniceOperationAgainstKafkaTimedOut("Failed to delete kafka topic: " + topicName + " after " + kafkaOperationTimeoutMs + " ms (" + current + " attempts).");
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
   *         false if the topic does not exist at all or if it exists but isn't completely available
   */
  public synchronized boolean containsTopic(String topic, Integer expectedPartitionCount) {
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
    if (allPartitionsHaveAnInSyncReplica) {
      logger.trace("The following topic has the at least one in-sync replica for each partition: " + topic);
    } else {
      logger.info("getConsumer().partitionsFor() returned some partitionInfo with no in-sync replica for topic: " + topic +
          ", partitionInfoList: " + Arrays.toString(partitionInfoList.toArray()));
    }
    return allPartitionsHaveAnInSyncReplica;
  }

  /**
   * This is an extensive check to verify that a topic is fully cleaned up.
   *
   * @return true if the topic exists neither in ZK nor in the brokers
   *         false if the topic exists fully or partially
   */
  public synchronized boolean isTopicFullyDeleted(String topic, boolean closeAndRecreateConsumer) {
    boolean zkMetadataExistsForTopic = AdminUtils.topicExists(getZkUtils(), topic);
    if (zkMetadataExistsForTopic) {
      logger.info("AdminUtils.topicExists() returned true, meaning that the ZK path still exists for topic: " + topic);
      return false;
    }

    List<PartitionInfo> partitionInfoList = getConsumer(closeAndRecreateConsumer).partitionsFor(topic);
    if (partitionInfoList == null) {
      logger.trace("getConsumer().partitionsFor() returned null for topic: " + topic);
      return true;
    }

    boolean noPartitionStillHasAnyReplica = partitionInfoList.stream()
        .noneMatch(partitionInfo -> partitionInfo.replicas().length > 0);
    if (noPartitionStillHasAnyReplica) {
      logger.trace("getConsumer().partitionsFor() returned no partitionInfo still containing a replica for topic: " + topic);
    } else {
      logger.info("The following topic still has at least one replica in at least one partition: " + topic
          + ", partitionInfoList: " + Arrays.toString(partitionInfoList.toArray()));
    }
    return noPartitionStillHasAnyReplica;
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

  public long getLatestOffsetAndRetry(String topic, int partition, int retries) {
    int attempt = 0;
    long offset;
    VeniceOperationAgainstKafkaTimedOut lastException = new VeniceOperationAgainstKafkaTimedOut("This exception should not be thrown");
    while (attempt < retries){
      try {
        offset = getLatestOffset(topic, partition);
        return offset;
      } catch (VeniceOperationAgainstKafkaTimedOut e){// topic and partition is listed in the exception object
        logger.warn("Failed to get latest offset.  Retries remaining: " + (retries - attempt), e);
        lastException = e;
        attempt ++;
      }
    }
    throw lastException;
  }

  /**
   * This method is synchronized because it calls #getConsumer()
   */
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
    long latestOffset;
    try {
      consumer.assign(Arrays.asList(topicPartition));
      consumer.seekToEnd(Arrays.asList(topicPartition));
      latestOffset = consumer.position(topicPartition);
      consumer.assign(Arrays.asList());
    } catch (org.apache.kafka.common.errors.TimeoutException ex) {
      throw new VeniceOperationAgainstKafkaTimedOut("Timeout exception when seeking to end to get latest offset"
          + " for topic: " + topic + " and partition: " + partition, ex);
    }
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
                  return getOffsetByTimeIfOutOfRange(partitionToOffset.getKey(), timestamp);
                }
              }));
      // The given timestamp exceed the timestamp of the last message. So return the last offset.
      if (result.isEmpty()) {
        logger.warn("Offsets result is empty. Will complement with the last offsets.");
        result = getConsumer().endOffsets(timestampsToSearch.keySet())
            .entrySet()
            .stream()
            .collect(Collectors.toMap(partitionToOffset -> Utils.notNull(partitionToOffset).getKey().partition(),
                partitionToOffset -> partitionToOffset.getValue()));
      } else if (result.size() != partitionInfoList.size()) {
        Map<TopicPartition, Long>  endOffests = getConsumer().endOffsets(timestampsToSearch.keySet());
        // Get partial offsets result.
        logger.warn("Missing offsets for some partitions. Partition Number should be :" + partitionInfoList.size()
            + " but only got: " + result.size()+". Will complement with the last offsets.");

        for (PartitionInfo partitionInfo : partitionInfoList) {
          int partitionId = partitionInfo.partition();
          if (!result.containsKey(partitionId)) {
            result.put(partitionId, endOffests.get(new TopicPartition(topic, partitionId)));
          }
        }
      }
      if (result.size() < partitionInfoList.size()) {
        throw new VeniceException(
            "Failed to get offsets for all partitions. Got offsets for " + result.size() + " partitions, should be: "
                + partitionInfoList.size());
      }
      return result;
    }
  }

  /**
   * Kafka's get offset by time API returns null if the requested time is before the first record OR after
   * the last record.  This method queries the time of the last message and compares it to the requested
   * timestamp in order to return either offset 0 or the last offset.
   */
  private synchronized long getOffsetByTimeIfOutOfRange(TopicPartition topicPartition, long timestamp){
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
  private KafkaConsumer<byte[], byte[]> getConsumer() {
    return getConsumer(false);
  }

  private synchronized KafkaConsumer<byte[], byte[]> getConsumer(boolean closeAndRecreate) {
    if (this.kafkaConsumer == null) {
      this.kafkaConsumer = veniceConsumerFactory.getKafkaConsumer(getKafkaConsumerProps());
    } else if (closeAndRecreate) {
      this.kafkaConsumer.close(kafkaOperationTimeoutMs, TimeUnit.MILLISECONDS);
      this.kafkaConsumer = veniceConsumerFactory.getKafkaConsumer(getKafkaConsumerProps());
      logger.info("Closed and recreated consumer.");
    }
    return this.kafkaConsumer;
  }

  private Properties getKafkaConsumerProps() {
    Properties props = new Properties();
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    // Increase receive buffer to 1MB to check whether it can solve the metadata timing out issue
    props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    return props;
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
