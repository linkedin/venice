package com.linkedin.venice.kafka;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.admin.KafkaAdminClient;
import com.linkedin.venice.kafka.admin.KafkaAdminWrapper;
import com.linkedin.venice.kafka.admin.ScalaAdminUtils;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.log4j.Logger;

import static com.linkedin.venice.ConfigConstants.*;
import static com.linkedin.venice.offsets.OffsetRecord.*;


/**
 * Topic Manager is shared by multiple cluster's controllers running in one physical Venice controller instance.
 *
 * This class contains one global {@link KafkaConsumer}, which is not thread-safe, so when you add new functions,
 * which is using this global consumer, please add 'synchronized' keyword, otherwise this {@link TopicManager}
 * won't be thread-safe, and Kafka consumer will report the following error when multiple threads are trying to
 * use the same consumer: KafkaConsumer is not safe for multi-threaded access.
 */
public class TopicManager implements Closeable {

  public static final long DEFAULT_TOPIC_RETENTION_POLICY_MS = 5 * Time.MS_PER_DAY;

  // Immutable state
  private final int kafkaOperationTimeoutMs;
  private final int topicDeletionStatusPollIntervalMs;
  private final long topicMinLogCompactionLagMs;
  private final KafkaClientFactory kafkaClientFactory;
  private final boolean isConcurrentTopicDeleteRequestsEnabled;

  // Mutable, lazily initialized, state
  private KafkaAdminWrapper kafkaAdmin;
  private KafkaConsumer<byte[], byte[]> kafkaRawBytesConsumer;
  private KafkaConsumer<KafkaKey, KafkaMessageEnvelope> kafkaRecordConsumer;

  private static final Logger logger = Logger.getLogger(TopicManager.class);
  public static final int DEFAULT_KAFKA_OPERATION_TIMEOUT_MS = 30 * Time.MS_PER_SECOND;
  private static final int MINIMUM_TOPIC_DELETION_STATUS_POLL_TIMES = 10;
  private static final int FAST_KAFKA_OPERATION_TIMEOUT_MS = Time.MS_PER_SECOND;
  public static final long UNKNOWN_TOPIC_RETENTION = Long.MIN_VALUE;
  protected static final long ETERNAL_TOPIC_RETENTION_POLICY_MS = Long.MAX_VALUE;
  private static final int KAFKA_POLLING_RETRY_ATTEMPT = 3;
  public static final int MAX_TOPIC_DELETE_RETRIES = 3;
  /**
   * Default setting is that no log compaction should happen for hybrid store version topics
   * if the messages are produced within 24 hours; otherwise servers could encounter MISSING
   * data DIV errors for grandfathering jobs which could potentially generate lots of
   * duplicate keys.
   */
  public static final long DEFAULT_KAFKA_MIN_LOG_COMPACTION_LAG_MS = 24 * Time.MS_PER_HOUR;
  // admin tool and venice topic consumer create this class.  We'll set this policy to false by default so those paths
  // aren't necessarily compromised with potentially new bad behavior.
  public static final boolean DEFAULT_CONCURRENT_TOPIC_DELETION_REQUEST_POLICY = false;


  //TODO: Consider adding a builder for this class as the number of constructors is getting high.
  public TopicManager(
      int kafkaOperationTimeoutMs,
      int topicDeletionStatusPollIntervalMs,
      long topicMinLogCompactionLagMs,
      KafkaClientFactory kafkaClientFactory,
      boolean isConcurrentTopicDeleteRequestsEnabled) {
    this.kafkaOperationTimeoutMs = kafkaOperationTimeoutMs;
    this.topicDeletionStatusPollIntervalMs = topicDeletionStatusPollIntervalMs;
    this.topicMinLogCompactionLagMs = topicMinLogCompactionLagMs;
    this.kafkaClientFactory = kafkaClientFactory;
    this.isConcurrentTopicDeleteRequestsEnabled = isConcurrentTopicDeleteRequestsEnabled;
  }

  public TopicManager(
      int kafkaOperationTimeoutMs,
      int topicDeletionStatusPollIntervalMs,
      long topicMinLogCompactionLagMs,
      KafkaClientFactory kafkaClientFactory) {
    this(kafkaOperationTimeoutMs,
        topicDeletionStatusPollIntervalMs,
        topicMinLogCompactionLagMs,
        kafkaClientFactory,
        DEFAULT_CONCURRENT_TOPIC_DELETION_REQUEST_POLICY);
  }

  /**
   * This constructor is used in server only; server doesn't have access to controller config like
   * topic.deletion.status.poll.interval.ms, so we use default config defined in this class; besides, TopicManager
   * in server doesn't use the config mentioned above.
   *
   * @param kafkaClientFactory
   */
  public TopicManager(KafkaClientFactory kafkaClientFactory) {
    this(DEFAULT_KAFKA_OPERATION_TIMEOUT_MS,
        DEFAULT_TOPIC_DELETION_STATUS_POLL_INTERVAL_MS,
        DEFAULT_KAFKA_MIN_LOG_COMPACTION_LAG_MS,
        kafkaClientFactory);
  }

  /**
   * Create a topic, and block until the topic is created, with a default timeout of
   * {@value #DEFAULT_KAFKA_OPERATION_TIMEOUT_MS}, after which this function will throw a VeniceException.
   *
   * @see {@link #createTopic(String, int, int, boolean)}
   */
  @Deprecated
  public void createTopic(String topicName, int numPartitions, int replication) {
    createTopic(topicName, numPartitions, replication, true);
  }

  /**
   * Create a topic, and block until the topic is created, with a default timeout of
   * {@value #DEFAULT_KAFKA_OPERATION_TIMEOUT_MS}, after which this function will throw a VeniceException.
   *
   * @see {@link #createTopic(String, int, int, boolean, boolean, Optional)}
   */
  public void createTopic(String topicName, int numPartitions, int replication, boolean eternal) {
    createTopic(topicName, numPartitions, replication, eternal, false, Optional.empty(), false);
  }

  public void createTopic(String topicName, int numPartitions, int replication, boolean eternal, boolean logCompaction,
      Optional<Integer> minIsr) {
    createTopic(topicName, numPartitions, replication, eternal, logCompaction, minIsr, true);
  }

  /**
   * Create a topic, and block until the topic is created, with a default timeout of
   * {@value #DEFAULT_KAFKA_OPERATION_TIMEOUT_MS}, after which this function will throw a VeniceException.
   *
   * @param topicName Name for the new topic
   * @param numPartitions number of partitions
   * @param replication replication factor
   * @param eternal if true, the topic will have "infinite" (~250 mil years) retention
   *                if false, its retention will be set to {@link #DEFAULT_TOPIC_RETENTION_POLICY_MS} by default
   * @param logCompaction whether to enable log compaction on the topic
   * @param minIsr if present, will apply the specified min.isr to this topic,
   *               if absent, Kafka cluster defaults will be used
   * @param useFastKafkaOperationTimeout if false, normal kafka operation timeout will be used,
   *                            if true, a much shorter timeout will be used to make topic creation non-blocking.
   */
  public void createTopic(String topicName, int numPartitions, int replication, boolean eternal, boolean logCompaction,
      Optional<Integer> minIsr, boolean useFastKafkaOperationTimeout) {
    long retentionTimeMs;
    if (eternal) {
      retentionTimeMs = ETERNAL_TOPIC_RETENTION_POLICY_MS;
    }  else {
      retentionTimeMs = DEFAULT_TOPIC_RETENTION_POLICY_MS;
    }
    createTopic(topicName, numPartitions, replication, retentionTimeMs, logCompaction, minIsr, useFastKafkaOperationTimeout);
  }


  /**
   * Create a topic, and block until the topic is created, with a default timeout of
   * {@value #DEFAULT_KAFKA_OPERATION_TIMEOUT_MS}, after which this function will throw a VeniceException.
   *
   * @param topicName Name for the new topic
   * @param numPartitions number of partitions
   * @param replication replication factor
   * @param retentionTimeMs Retention time, in ms, for the topic
   * @param logCompaction whether to enable log compaction on the topic
   * @param minIsr if present, will apply the specified min.isr to this topic,
   *               if absent, Kafka cluster defaults will be used
   * @param useFastKafkaOperationTimeout if false, normal kafka operation timeout will be used,
   *                            if true, a much shorter timeout will be used to make topic creation non-blocking.
   */
  public void createTopic(
      String topicName,
      int numPartitions,
      int replication,
      long retentionTimeMs,
      boolean logCompaction,
      Optional<Integer> minIsr,
      boolean useFastKafkaOperationTimeout) {

    long startTime = System.currentTimeMillis();
    long deadlineMs = startTime + (useFastKafkaOperationTimeout ? FAST_KAFKA_OPERATION_TIMEOUT_MS : kafkaOperationTimeoutMs);

    logger.info("Creating topic: " + topicName + " partitions: " + numPartitions + " replication: " + replication);
    try {
      Properties topicProperties = new Properties();
      topicProperties.put(TopicConfig.RETENTION_MS_CONFIG, Long.toString(retentionTimeMs));
      if (logCompaction) {
        topicProperties.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        topicProperties.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, Long.toString(topicMinLogCompactionLagMs));
      } else {
        topicProperties.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
      }

      // If not set, Kafka cluster defaults will apply
      minIsr.ifPresent(minIsrConfig -> topicProperties.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minIsrConfig));

      // Just in case the Kafka cluster isn't configured as expected.
      topicProperties.put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, "LogAppendTime");

      boolean asyncCreateOperationSucceeded = false;
      while (!asyncCreateOperationSucceeded) {
        try {
          getKafkaAdmin().createTopic(topicName, numPartitions, replication, topicProperties);
          asyncCreateOperationSucceeded = true;
        } catch (InvalidReplicationFactorException e) {
          if (System.currentTimeMillis() > deadlineMs) {
            throw new VeniceOperationAgainstKafkaTimedOut("Timeout while creating topic: " + topicName + ". Topic still does not exist after " + (deadlineMs - startTime) + "ms.", e);
          } else {
            logger.info("Kafka failed to kick off topic creation because it appears to be under-replicated... Will treat it as a transient error and attempt to ride over it.", e);
            Utils.sleep(200);
          }
        }
      }

      waitUntilTopicCreated(topicName, numPartitions, deadlineMs);
      boolean eternal = retentionTimeMs == ETERNAL_TOPIC_RETENTION_POLICY_MS;
      logger.info("Successfully created " + (eternal ? "eternal " : "") + "topic: " + topicName);

    } catch (TopicExistsException e) {
      logger.info("Topic: " + topicName + " already exists, will update retention policy.");
      waitUntilTopicCreated(topicName, numPartitions, deadlineMs);
      updateTopicRetention(topicName, retentionTimeMs);
      logger.info("Updated retention policy to be " + retentionTimeMs + "ms for topic: " + topicName);
    }
  }

  protected void waitUntilTopicCreated(String topicName, int partitionCount, long deadlineMs) {
    long startTime = System.currentTimeMillis();
    while (!containsTopicAndAllPartitionsAreOnline(topicName, partitionCount)) {
      if (System.currentTimeMillis() > deadlineMs) {
        throw new VeniceOperationAgainstKafkaTimedOut(
            "Timeout while creating topic: " + topicName + ".  Topic still did not pass all the checks after " + (deadlineMs - startTime) + "ms.");
      }
      Utils.sleep(200);
    }
  }

  /**
   * This method sends a delete command to Kafka and immediately returns with a future. The future could be null if the
   * underlying Kafka admin client doesn't support it. In both cases, deletion will occur asynchronously.
   * @param topicName
   */
  private Future<Void> ensureTopicIsDeletedAsync(String topicName) {
    // TODO: Stop using Kafka APIs which depend on ZK.
    logger.info("Deleting topic: " + topicName);
    return getKafkaAdmin().deleteTopic(topicName);
  }

  public int getReplicationFactor(String topicName) {
    return getPartitions(topicName).iterator().next().replicas().length;
  }

  /**
   * Update retention for the given topic.
   * If the topic doesn't exist, this operation will throw {@link TopicDoesNotExistException}
   * @param topicName
   * @param retentionInMS
   * @return true if the retention time config of the input topic gets updated; return false if nothing gets updated
   */
  public boolean updateTopicRetention(String topicName, long retentionInMS) {
    Properties topicProperties = getTopicConfig(topicName);
    return updateTopicRetention(topicName, retentionInMS, topicProperties);
  }

  /**
   * Update rentention for the given topic given a {@link Properties}.
   * @param topicName
   * @param retentionInMS
   * @param topicProperties
   * @return true if the retention time gets updated; false if no update is needed.
   */
  public boolean updateTopicRetention(String topicName, long retentionInMS, Properties topicProperties) {
    String retentionInMSStr = Long.toString(retentionInMS);
    if (!topicProperties.containsKey(TopicConfig.RETENTION_MS_CONFIG) || // config doesn't exist
        !topicProperties.getProperty(TopicConfig.RETENTION_MS_CONFIG).equals(retentionInMSStr)) { // config is different
      topicProperties.put(TopicConfig.RETENTION_MS_CONFIG, Long.toString(retentionInMS));
      getKafkaAdmin().setTopicConfig(topicName, topicProperties);
      return true;
    }
    // Retention time has already been updated for this topic before
    return false;
  }

  /**
   * Update topic compaction policy.
   * @throws TopicDoesNotExistException, if the topic doesn't exist
   */
  public synchronized void updateTopicCompactionPolicy(String topicName, boolean logCompaction) {
    Properties topicProperties = getTopicConfig(topicName);
    // If the compaction policy doesn't exist, by default it is disabled.
    String currentCompactionPolicy = topicProperties.containsKey(TopicConfig.CLEANUP_POLICY_CONFIG) ?
        (String)topicProperties.get(TopicConfig.CLEANUP_POLICY_CONFIG) : TopicConfig.CLEANUP_POLICY_DELETE;
    String expectedCompactionPolicy = logCompaction ? TopicConfig.CLEANUP_POLICY_COMPACT : TopicConfig.CLEANUP_POLICY_DELETE;
    long currentMinLogCompactionLagMs = topicProperties.containsKey(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG) ?
        Long.valueOf((String)topicProperties.get(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG)) : 0L;
    long expectedMinLogCompactionLagMs = logCompaction ? topicMinLogCompactionLagMs : 0L;
    boolean needToUpdateTopicConfig = false;
    if (! expectedCompactionPolicy.equals(currentCompactionPolicy)) {
      // Different, then update
      needToUpdateTopicConfig = true;
      topicProperties.put(TopicConfig.CLEANUP_POLICY_CONFIG, expectedCompactionPolicy);
    }
    if (currentMinLogCompactionLagMs != expectedMinLogCompactionLagMs) {
      needToUpdateTopicConfig = true;
      topicProperties.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, Long.toString(expectedMinLogCompactionLagMs));
    }
    if (needToUpdateTopicConfig) {
      getKafkaAdmin().setTopicConfig(topicName, topicProperties);
      logger.info("Kafka compaction policy for topic: " + topicName + " has been updated from " +
          currentCompactionPolicy + " to " + expectedCompactionPolicy + ", min compaction lag updated from "
          + currentMinLogCompactionLagMs + " to " + expectedCompactionPolicy);
    }
  }

  public boolean isTopicCompactionEnabled(String topicName) {
    Properties topicProperties = getTopicConfig(topicName);
    return topicProperties.containsKey(TopicConfig.CLEANUP_POLICY_CONFIG) &&
        topicProperties.get(TopicConfig.CLEANUP_POLICY_CONFIG).equals(TopicConfig.CLEANUP_POLICY_COMPACT);
  }

  /**
   * This API is for testing only at this moment.
   */
  public long getTopicMinLogCompactionLagMs(String topicName) {
    Properties topicProperties = getTopicConfig(topicName);
    return topicProperties.containsKey(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG)
        ? Long.valueOf((String) topicProperties.get(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG)) : 0L;
  }

  public Map<String, Long> getAllTopicRetentions() {
    return getKafkaAdmin().getAllTopicRetentions();
  }

  /**
   * Return topic retention time in MS.
   */
  public long getTopicRetention(String topicName) {
    Properties topicProperties = getTopicConfig(topicName);
    if (topicProperties.containsKey(TopicConfig.RETENTION_MS_CONFIG)) {
      return Long.parseLong(topicProperties.getProperty(TopicConfig.RETENTION_MS_CONFIG));
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
    if (!containsTopic(topicName)) {
      throw new TopicDoesNotExistException("Topic: " + topicName + " doesn't exist");
    }

    return getKafkaAdmin().getTopicConfig(topicName);
  }

  public Map<String, Properties> getAllTopicConfig() {
    return getKafkaAdmin().getAllTopicConfig();
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
  public void ensureTopicIsDeletedAndBlock(String topicName) throws ExecutionException {
    if (!containsTopicAndAllPartitionsAreOnline(topicName)) {
      // Topic doesn't exist
      return;
    }

    // TODO: Remove the isConcurrentTopicDeleteRequestsEnabled flag and make topic deletion to be always blocking or
    // refactor this method to actually support concurrent topic deletion if that's something we want.
    // This is trying to guard concurrent topic deletion in Kafka.
    if (!isConcurrentTopicDeleteRequestsEnabled &&
        /**
         * TODO: Add support for this call in the {@link com.linkedin.venice.kafka.admin.KafkaAdminClient}
         * This is the last remaining call that depends on {@link kafka.utils.ZkUtils}.
         */
        getKafkaAdmin().isTopicDeletionUnderway()) {
      throw new VeniceException("Delete operation already in progress! Try again later.");
    }

    Future<Void> future = ensureTopicIsDeletedAsync(topicName);
    if (future != null) {
      // Skip additional checks for Java kafka client since the result of the future can guarantee that the topic is deleted.
      try {
        future.get(kafkaOperationTimeoutMs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        throw new VeniceException("Thread interrupted while waiting to delete topic: " + topicName);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof UnknownTopicOrPartitionException) {
          // No-op. Topic is deleted already, consider this as a successful deletion.
        } else {
          throw e;
        }
      } catch (TimeoutException e) {
        throw new VeniceOperationAgainstKafkaTimedOut("Failed to delete kafka topic: " + topicName + " after "
            + kafkaOperationTimeoutMs);
      }
      logger.info("Topic: " + topicName + " has been deleted");
      // TODO: Remove the checks below once we have fully migrated to use the Kafka admin client.
      return;
    }
    // Since topic deletion is async, we would like to poll until topic doesn't exist any more
    int MAX_TIMES = kafkaOperationTimeoutMs / topicDeletionStatusPollIntervalMs;
    /**
     * In case we have bad config, MAX_TIMES can not be smaller than {@link #MINIMUM_TOPIC_DELETION_STATUS_POLL_TIMES}.
     */
    MAX_TIMES = Math.max(MAX_TIMES, MINIMUM_TOPIC_DELETION_STATUS_POLL_TIMES);
    final int MAX_CONSUMER_RECREATION_INTERVAL = 100;
    int current = 0;
    int lastConsumerRecreation = 0;
    int consumerRecreationInterval = 5;
    while (++current <= MAX_TIMES) {
      Utils.sleep(topicDeletionStatusPollIntervalMs);
      // Re-create consumer every once in a while, in case it's wedged on some stale state.
      boolean closeAndRecreateConsumer = (current - lastConsumerRecreation) == consumerRecreationInterval;
      if (closeAndRecreateConsumer) {
        /**
         * Exponential back-off:
         * Recreate the consumer after polling status for 2 times, (2+)4 times, (2+4+)8 times... and maximum 100 times
         */
        lastConsumerRecreation = current;
        consumerRecreationInterval = Math.min(consumerRecreationInterval * 2, MAX_CONSUMER_RECREATION_INTERVAL);
        if (consumerRecreationInterval <= 0) {
          // In case it overflows
          consumerRecreationInterval = MAX_CONSUMER_RECREATION_INTERVAL;
        }
      }
      if (isTopicFullyDeleted(topicName, closeAndRecreateConsumer)) {
        logger.info("Topic: " + topicName + " has been deleted after polling " + current + " times");
        return;
      }
    }
    throw new VeniceOperationAgainstKafkaTimedOut("Failed to delete kafka topic: " + topicName + " after " + kafkaOperationTimeoutMs + " ms (" + current + " attempts).");
  }

  public void ensureTopicIsDeletedAndBlockWithRetry(String topicName) throws ExecutionException {
    // Topic deletion may time out, so go ahead and retry the operation up the max number of attempts, if we
    // simply cannot succeed, bubble the exception up.
    Integer attempts  = 0;
    while(true) {
      try {
        ensureTopicIsDeletedAndBlock(topicName);
        return;
      } catch (VeniceOperationAgainstKafkaTimedOut e) {
        attempts++;
        logger.warn(String.format("Topic deletion for topic %s timed out!  Retry attempt %d / %d", topicName, attempts, MAX_TOPIC_DELETE_RETRIES));
        if(attempts == MAX_TOPIC_DELETE_RETRIES) {
          logger.error(String.format("Topic deletion for topic %s timed out! Giving up!!", topicName));
          throw e;
        }
      } catch (ExecutionException e) {
        attempts++;
        logger.warn(String.format("Topic deletion for topic %s errored out!  Retry attempt %d / %d", topicName, attempts, MAX_TOPIC_DELETE_RETRIES));
        if(attempts == MAX_TOPIC_DELETE_RETRIES) {
          logger.error(String.format("Topic deletion for topic %s errored out! Giving up!!", topicName));
          throw e;
        }
      }
    }
  }

  public synchronized Set<String> listTopics() {
    Set<String> topics = getRawBytesConsumer().listTopics().keySet();
    return topics;
  }

  /**
   * A quick check to see whether the topic exists.
   *
   * N.B.: The behavior of the Scala and Java admin clients are different...
   */
  public boolean containsTopic(String topic) {
    return this.getKafkaAdmin().containsTopic(topic);
  }

  /**
   * @see {@link #containsTopicAndAllPartitionsAreOnline(String, Integer)}
   */
  public boolean containsTopicAndAllPartitionsAreOnline(String topic) {
    return containsTopicAndAllPartitionsAreOnline(topic, null);
  }

  /**
   * This is an extensive check to mitigate an edge-case where a topic is not yet created in all the brokers.
   *
   * @return true if the topic exists and all its partitions have at least one in-sync replica
   *         false if the topic does not exist at all or if it exists but isn't completely available
   */
  public synchronized boolean containsTopicAndAllPartitionsAreOnline(String topic, Integer expectedPartitionCount) {
    boolean zkMetadataCreatedForTopic = containsTopic(topic);
    if (!zkMetadataCreatedForTopic) {
      return false;
    }

    List<PartitionInfo> partitionInfoList = getRawBytesConsumer().partitionsFor(topic);
    if (partitionInfoList == null) {
      logger.warn("getConsumer().partitionsFor() returned null for topic: " + topic);
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
    boolean zkMetadataExistsForTopic = containsTopic(topic);
    if (zkMetadataExistsForTopic) {
      logger.info("containsTopicInKafkaZK() returned true, meaning that the ZK path still exists for topic: " + topic);
      return false;
    }

    List<PartitionInfo> partitionInfoList = getRawBytesConsumer(closeAndRecreateConsumer).partitionsFor(topic);
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
    if (!containsTopicAndAllPartitionsAreOnline(topic)) {
      logger.warn("Topic: " + topic + " doesn't exist, returning empty map for latest offsets");
      return new HashMap<>();
    }
    KafkaConsumer<byte[], byte[]> consumer = getRawBytesConsumer();
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

  public long getLatestProducerTimestampAndRetry(String topic, int partition, int retries) {
    int attempt = 0;
    long timestamp;
    VeniceOperationAgainstKafkaTimedOut lastException = new VeniceOperationAgainstKafkaTimedOut("This exception should not be thrown");
    while (attempt < retries){
      try {
        timestamp = getLatestProducerTimestamp(topic, partition);
        return timestamp;
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
   *
   * Here this function will only check whether the topic exists in Kafka Zookeeper or not.
   * If stronger check against Kafka broker is required, the caller should call {@link #containsTopicAndAllPartitionsAreOnline(String)}
   * before invoking this function. The reason of not checking topic existence by {@link #containsTopicAndAllPartitionsAreOnline(String)}
   * by default since this function will validate whether every topic partition has ISR, which could
   * fail {@link #getLatestOffset(String, int)} since some transient non-ISR could happen randomly.
   */
  public synchronized long getLatestOffset(String topic, int partition) throws TopicDoesNotExistException {
    return getLatestOffset(getRawBytesConsumer(), topic, partition, true);
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

  /**
   * If the topic is empty or all the messages are truncated (startOffset==endOffset), return -1;
   * otherwise, return the producer timestamp of the last message in the selected partition of a topic
   */
  private synchronized Long getLatestProducerTimestamp(String topic, Integer partition) throws TopicDoesNotExistException {
    if (!containsTopic(topic)) {
      throw new TopicDoesNotExistException("Topic " + topic + " does not exist!");
    }
    if (partition < 0) {
      throw new IllegalArgumentException("Cannot retrieve latest producer timestamp for invalid partition " + partition + " topic " + topic);
    }

    KafkaConsumer<KafkaKey, KafkaMessageEnvelope> consumer = getRecordConsumer();
    TopicPartition topicPartition = new TopicPartition(topic, partition);

    long latestProducerTimestamp;
    try {
      consumer.assign(Arrays.asList(topicPartition));
      consumer.seekToEnd(Arrays.asList(topicPartition));
      long latestOffset = consumer.position(topicPartition);
      if (latestOffset <= 0) {
        // empty topic
        latestProducerTimestamp = -1;
      } else {
        consumer.seekToBeginning(Arrays.asList(topicPartition));
        long earliestOffset = consumer.position(topicPartition);
        if (earliestOffset == latestOffset) {
          // empty topic
          latestProducerTimestamp = -1;
        } else {
          // poll the last message and retrieve the producer timestamp
          consumer.seek(topicPartition, latestOffset - 1);
          ConsumerRecords records = ConsumerRecords.EMPTY;
          int attempts = 0;
          while (attempts++ < KAFKA_POLLING_RETRY_ATTEMPT && records.isEmpty()) {
            logger.info("Trying to get the last record from topic: " + topicPartition.toString() + " at offset: " + (latestOffset - 1)
                + ". Attempt#" + attempts + "/" + KAFKA_POLLING_RETRY_ATTEMPT);
            records = consumer.poll(kafkaOperationTimeoutMs);
          }
          if (records.isEmpty()) {
            /**
             * Failed the job if we cannot get the last offset of the topic.
             */
            String errorMsg = "Failed to get the last record from topic: " + topicPartition.toString() +
                " after " + KAFKA_POLLING_RETRY_ATTEMPT + " attempts";
            logger.error(errorMsg);
            throw new VeniceException(errorMsg);
          }

          // Get the latest record from the poll result
          ConsumerRecord<KafkaKey, KafkaMessageEnvelope> record = (ConsumerRecord<KafkaKey, KafkaMessageEnvelope>) records.iterator().next();
          latestProducerTimestamp = record.value().producerMetadata.messageTimestamp;
        }
      }
      consumer.assign(Arrays.asList());
    } catch (org.apache.kafka.common.errors.TimeoutException ex) {
      throw new VeniceOperationAgainstKafkaTimedOut("Timeout exception when seeking to end to get latest offset"
          + " for topic: " + topic + " and partition: " + partition, ex);
    }
    return latestProducerTimestamp;
  }

  /**
   * Get offsets for all the partitions with a specific timestamp.
   */
  public synchronized Map<Integer, Long> getOffsetsByTime(String topic, long timestamp) {
    int remainingAttempts = 5;
    List<PartitionInfo> partitionInfoList = getRawBytesConsumer().partitionsFor(topic);
    // N.B.: During unit test development, getting a null happened occasionally without apparent
    //       reason. In their current state, the tests have been run with a high invocationCount and
    //       no failures, so it may be a non-issue. If this happens again, and we find some test case
    //       that can reproduce it, we may want to try adding a short amount of retries, and reporting
    //       a bug to Kafka.
    while (remainingAttempts > 0 && (null == partitionInfoList || partitionInfoList.isEmpty())) {
      Utils.sleep(500);
      partitionInfoList = getRawBytesConsumer().partitionsFor(topic);
      remainingAttempts -= 1;
    }
    if (null == partitionInfoList || partitionInfoList.isEmpty()) {
      throw new VeniceException("Cannot get partition info for topic: " + topic + ", partitionInfoList: " + partitionInfoList);
    } else {
      Map<TopicPartition, Long> timestampsToSearch = partitionInfoList.stream()
          .collect(Collectors.toMap(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()), ignoredParam -> timestamp));
      return getOffsetsByTime(topic, timestampsToSearch, timestamp);
    }
  }

  /**
   * Return the beginning offset of a topic/partition. Synchronized because it calls #getConsumer()
   *
   * @throws TopicDoesNotExistException
   */
  public synchronized long getEarliestOffset(String topic, int partition) throws TopicDoesNotExistException {
    return getEarliestOffset(getRawBytesConsumer(), topic, partition, true);
  }

  private synchronized Long getEarliestOffset(KafkaConsumer<byte[], byte[]> consumer, String topic, Integer partition, boolean doTopicCheck) throws TopicDoesNotExistException {
    if (doTopicCheck && !containsTopic(topic)) {
      throw new TopicDoesNotExistException("Topic " + topic + " does not exist!");
    }
    if (partition < 0) {
      throw new IllegalArgumentException("Cannot retrieve latest offsets for invalid partition " + partition);
    }
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    long earliestOffset;
    try {
      consumer.assign(Arrays.asList(topicPartition));
      consumer.seekToBeginning(Arrays.asList(topicPartition));
      earliestOffset = consumer.position(topicPartition);
      consumer.assign(Arrays.asList());
    } catch (org.apache.kafka.common.errors.TimeoutException ex) {
      throw new VeniceOperationAgainstKafkaTimedOut("Timeout exception when seeking to beginning to get earliest offset"
          + " for topic: " + topic + " and partition: " + partition, ex);
    }
    return earliestOffset;
  }

  /**
   * Get offsets for only one partition with a specific timestamp.
   */
  public synchronized long getOffsetByTime(String topic, int partition, long timestamp) {
    Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
    timestampsToSearch.put(new TopicPartition(topic, partition), timestamp);
    return getOffsetsByTime(topic, timestampsToSearch, timestamp).get(partition);
  }

  /**
   * Get offsets for the selected partitions in `timestampsToSearch` with a specific timestamp
   */
  public synchronized Map<Integer, Long> getOffsetsByTime(String topic, Map<TopicPartition, Long> timestampsToSearch, long timestamp) {
    int expectedPartitionNum = timestampsToSearch.size();
    Map<Integer, Long>  result = getRawBytesConsumer().offsetsForTimes(timestampsToSearch)
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
      result = getRawBytesConsumer().endOffsets(timestampsToSearch.keySet())
          .entrySet()
          .stream()
          .collect(Collectors.toMap(partitionToOffset -> Utils.notNull(partitionToOffset).getKey().partition(),
              partitionToOffset -> partitionToOffset.getValue()));
    } else if (result.size() != expectedPartitionNum) {
      Map<TopicPartition, Long>  endOffests = getRawBytesConsumer().endOffsets(timestampsToSearch.keySet());
      // Get partial offsets result.
      logger.warn("Missing offsets for some partitions. Partition Number should be :" + expectedPartitionNum
          + " but only got: " + result.size()+". Will complement with the last offsets.");

      for (TopicPartition topicPartition : timestampsToSearch.keySet()) {
        int partitionId = topicPartition.partition();
        if (!result.containsKey(partitionId)) {
          result.put(partitionId, endOffests.get(new TopicPartition(topic, partitionId)));
        }
      }
    }
    if (result.size() < expectedPartitionNum) {
      throw new VeniceException(
          "Failed to get offsets for all partitions. Got offsets for " + result.size() + " partitions, should be: "
              + expectedPartitionNum);
    }
    return result;
  }

  /**
   * Kafka's get offset by time API returns null if the requested time is before the first record OR after
   * the last record.  This method queries the time of the last message and compares it to the requested
   * timestamp in order to return either offset 0 or the last offset.
   */
  private synchronized long getOffsetByTimeIfOutOfRange(TopicPartition topicPartition, long timestamp){
    long latestOffset = getLatestOffset(topicPartition.topic(), topicPartition.partition());
    if (latestOffset <= 0) {
      logger.info("End offset for topic " + topicPartition + " is " + latestOffset + "; return offset " + LOWEST_OFFSET);
      return LOWEST_OFFSET;
    }

    long earliestOffset = getEarliestOffset(topicPartition.topic(), topicPartition.partition());
    if (earliestOffset == latestOffset) {
      /**
       * This topic/partition is empty or retention delete the entire partition
       */
      logger.info("Both beginning offest and end offset is " + latestOffset + " for topic " + topicPartition
          + "; it's empty; return offset " + latestOffset);
      return latestOffset;
    }

    KafkaConsumer consumer = getRawBytesConsumer();
    try {
      consumer.assign(Arrays.asList(topicPartition));
      consumer.seek(topicPartition, latestOffset - 1);
      ConsumerRecords records = ConsumerRecords.EMPTY;
      /**
       * We should retry to get the last record from that topic/partition, never return 0L here because 0L offset
       * will result in replaying all the messages in real-time buffer. This function is mainly used during buffer
       * replay for hybrid stores.
       */
      int attempts = 0;
      while (attempts++ < KAFKA_POLLING_RETRY_ATTEMPT && records.isEmpty()) {
        logger.info("Trying to get the last record from topic: " + topicPartition.toString() + " at offset: " + (latestOffset - 1)
            + ". Attempt#" + attempts + "/" + KAFKA_POLLING_RETRY_ATTEMPT);
        records = consumer.poll(kafkaOperationTimeoutMs);
      }
      if (records.isEmpty()) {
        /**
         * Failed the job if we cannot get the last offset of the topic.
         */
        String errorMsg = "Failed to get the last record from topic: " + topicPartition.toString() +
            " after " + KAFKA_POLLING_RETRY_ATTEMPT + " attempts";
        logger.error(errorMsg);
        throw new VeniceException(errorMsg);
      }

      // Get the latest record from the poll result
      ConsumerRecord record = (ConsumerRecord) records.iterator().next();

      /**
       * 1. If the required timestamp is bigger than the timestamp of last record, return the offset of last record
       * 2. Otherwise, return (earliestOffset - 1) to consume from the beginning; decrease 1 because when subscribing,
       *    we seek to the next offset; besides, safeguard the edge case that we earliest offset is already -1.
       */
      long resultOffset = (timestamp > record.timestamp()) ? latestOffset : Math.max(earliestOffset - 1, LOWEST_OFFSET);
      logger.info("Successfully return offset: " + resultOffset + " for topic: " + topicPartition.toString()
          + " for timestamp: " + timestamp);
      return resultOffset;
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
    KafkaConsumer<byte[], byte[]> consumer = getRawBytesConsumer();
    return consumer.partitionsFor(topic);
  }

  /**
   * The first time this is called, it lazily initializes {@link #kafkaRawBytesConsumer}.
   * Any method that uses this consumer must be synchronized.  Since the same
   * consumer is returned each time and some methods modify the consumer's state
   * we must guard against concurrent modifications.
   *
   * @return The internal {@link KafkaConsumer} instance.
   */
  private KafkaConsumer<byte[], byte[]> getRawBytesConsumer() {
    return getRawBytesConsumer(false);
  }

  private synchronized KafkaConsumer<byte[], byte[]> getRawBytesConsumer(boolean closeAndRecreate) {
    if (this.kafkaRawBytesConsumer == null) {
      this.kafkaRawBytesConsumer = kafkaClientFactory.getKafkaConsumer(getKafkaRawBytesConsumerProps());
    } else if (closeAndRecreate) {
      this.kafkaRawBytesConsumer.close(kafkaOperationTimeoutMs, TimeUnit.MILLISECONDS);
      this.kafkaRawBytesConsumer = kafkaClientFactory.getKafkaConsumer(getKafkaRawBytesConsumerProps());
      logger.info("Closed and recreated consumer.");
    }
    return this.kafkaRawBytesConsumer;
  }

  private Properties getKafkaRawBytesConsumerProps() {
    Properties props = new Properties();
    //This is a temporary fix for the issue described here
    //https://stackoverflow.com/questions/37363119/kafka-producer-org-apache-kafka-common-serialization-stringserializer-could-no
    //In our case "org.apache.kafka.common.serialization.ByteArrayDeserializer" class can not be found
    //because class loader has no venice-common in class path. This can be only reproduced on JDK11
    //Trying to avoid class loading via Kafka's ConfigDef class
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    // Increase receive buffer to 1MB to check whether it can solve the metadata timing out issue
    props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    return props;
  }

  private KafkaConsumer<KafkaKey, KafkaMessageEnvelope> getRecordConsumer() {
    if (this.kafkaRecordConsumer == null) {
      synchronized (this) {
        if (this.kafkaRecordConsumer == null) {
          this.kafkaRecordConsumer = kafkaClientFactory.getKafkaConsumer(getKafkaRecordConsumerProps());
        }
      }
    }
    return this.kafkaRecordConsumer;
  }

  private Properties getKafkaRecordConsumerProps() {
    Properties props = new Properties();
    //This is a temporary fix for the issue described here
    //https://stackoverflow.com/questions/37363119/kafka-producer-org-apache-kafka-common-serialization-stringserializer-could-no
    //In our case "org.apache.kafka.common.serialization.ByteArrayDeserializer" class can not be found
    //because class loader has no venice-common in class path. This can be only reproduced on JDK11
    //Trying to avoid class loading via Kafka's ConfigDef class
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaKeySerializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OptimizedKafkaValueSerializer.class);
    // Increase receive buffer to 1MB to check whether it can solve the metadata timing out issue
    props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024);
    return props;
  }

  private synchronized KafkaAdminWrapper getKafkaAdmin() {
    if (null == kafkaAdmin) {
      String kafkaAdminName = "Unknown";
      kafkaAdmin = kafkaClientFactory.getKafkaAdminClient();
      if (kafkaAdmin instanceof KafkaAdminClient) {
        kafkaAdminName = KafkaAdminClient.class.getName();
      } else if (kafkaAdmin instanceof ScalaAdminUtils) {
        kafkaAdminName = ScalaAdminUtils.class.getName();
      }
      logger.info(this.getClass().getSimpleName() + " is using kafka admin client: " + kafkaAdminName);
    }
    return kafkaAdmin;
  }

  @Override
  public synchronized void close() throws IOException {
    IOUtils.closeQuietly(kafkaRawBytesConsumer);
    IOUtils.closeQuietly(kafkaAdmin);
  }
}
