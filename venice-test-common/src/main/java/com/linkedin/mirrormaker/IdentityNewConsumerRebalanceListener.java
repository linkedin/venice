/**
 * N.B.: This class comes from LiKafka. It ought to be part of Kafka proper but it is not yet open-sourced.
 * This class is necessary in order to spin-up MirrorMaker in tests.
 **/

package com.linkedin.mirrormaker;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.common.TopicAlreadyMarkedForDeletionException;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.PoisonPill;
import org.apache.kafka.common.utils.Utils;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;

import static java.lang.Integer.parseInt;


public class IdentityNewConsumerRebalanceListener implements org.apache.kafka.clients.consumer.ConsumerRebalanceListener {
  private Properties _sourceConfig;
  private Properties _targetConfig;

  private String _targetBootstrapServers;
  private String _targetZk;
  private String _targetClientId;
  private int _targetReplicationFactor;
  private String _sourceClientId;

  private ZkUtils _zkUtilsTarget;

  private Map<String, Integer> _targetPartitions = null;

  private Consumer _targetMetadataClient;
  private Consumer _sourceMetadataClient;

  private ScheduledExecutorService _topicDeletionExecutor = null;
  private Random _random = new Random();

  private int _initialCheckTimeoutMs;
  private long _topicCacheExpiration;
  private long _lastTargetTopicCacheClear = 0;
  private boolean _enableTopicDeletionPropagation;
  private long _topicDeletionPropagationDelayMs;
  private int _maxTopicCheckRetries;

  private static final int RETRY_SLEEP_TIME_MS               = 3000;
  private static final int MAX_TOPIC_CHECK_RETRIES           = 20;
  private static final int DEFAULT_TOPIC_CACHE_EXPIRATION    = 6000;
  private static final int DEFAULT_ZK_SESSION_TIMEOUT        = 30000;
  private static final int DEFAULT_ZK_CONNECTION_TIMEOUT     = 30000;
  private static final int DEFAULT_TARGET_REPLICATION_FACTOR = 2;
  private static final boolean DEFAULT_ENABLE_TOPIC_DELETION_PROPAGATION = false;
  private static final int DEFAULT_TOPIC_DELETION_PROPAGATION_DELAY_MS = 300000;

  private static final int EXIT_PARTITIONS_DONT_MATCH     = -250;
  private static final int EXIT_INCONSISTENT_STATE        = -251;
  private static final int EXIT_CANT_CREATE_TOPIC         = -252;
  private static final int EXIT_CANT_CREATE_AFTER_RETRY   = -253;
  private static final int EXIT_CANT_OPEN_FILE            = -254;

  public static final String CONFIG_DELIMITER = "#";
  private static final int DEFAULT_MAX_HEAP_DUMP_WAIT_MS = 10000;

  static final Logger logger = Logger.getLogger(IdentityNewConsumerRebalanceListener.class.getName());
  private static final File HEAP_DUMP_FOLDER = new File(System.getProperty("user.dir"));

  public IdentityNewConsumerRebalanceListener(String parameters) {
    parseListenerConfig(parameters);
    logger.info("Initializing IdentityNewConsumerRebalanceListener:" + " _targetZk=" + _targetZk
        + " targetBootstrapServers=" + _targetBootstrapServers + " _initialCheckTimeoutMs=" + _initialCheckTimeoutMs);
  }

  /**
   * This creates a serialized configuration string to pass to the constructor.  This is the call used to
   * encode a string to pass to the constructor for version 2 or above. It expects 2 config filenames to be passed
   * on. From these files it'll extract config information. See parseFileConfig() and the class documentation for
   * more information.
   *
   * @param sourceConfigFileName The filename of the consumer config file (source Cluster)
   * @param targetConfigFileName The filename of the producer config file (target Cluster)
   * @return a serialized configuration string
   */
  public static String getConfigString(String sourceConfigFileName, String targetConfigFileName) {
    return "file" + CONFIG_DELIMITER + sourceConfigFileName + CONFIG_DELIMITER + targetConfigFileName;
  }

  /**
   * Deserialize the configuration string and parse the parameters for this class. Throw an exception if we can't parse
   * the data correctly.
   *
   * @param serializedConfig a string serialized config
   */
  private void parseListenerConfig(String serializedConfig) {
    String[] configTokens = serializedConfig.split(CONFIG_DELIMITER);

    if (configTokens[0].equals("file")) {
      parseFileConfig(configTokens);
    } else {
      String errorMessage = "Could not parse config for Identity rebalancing: " + serializedConfig + " "
          + configTokens[0];
      logger.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }

  }

  /**
   * Parse a config from a set of files. We're expecting to receive 2 filenames. One for a consumer (source) config,
   * and one for a producer (target) config. We're going to load those documents and get the data required from them.
   * Specifically, we're going to pull data from the source config (for our source Zookeeper connector) and from the
   * target config for our target zookeeper connector as well as the bootstrap servers for the helper consumer (which
   * connects to the target cluster).
   *
   * @param configTokens This expects 3 total elements. The first one denotes this mode of parsing (file)
   *                     the second is the sourceConfig file name and the third is the targetConfig file name.
   */
  private void parseFileConfig(String[] configTokens) {
    if (configTokens.length != 3) {
      String errorMessage = "Could not parse config for Identity rebalancing FILE: " + Arrays.toString(configTokens);
      logger.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }
    // We received 2 config files

    try {
      _sourceConfig = Utils.loadProps(configTokens[1]);
      _targetConfig = Utils.loadProps(configTokens[2]);
    } catch (IOException e) {
      errorAndExit("Could not read config files " + configTokens[1] + "," + configTokens[2],
          EXIT_CANT_OPEN_FILE, e);
    }

    String identityMirrorVersion = _targetConfig.getProperty("identityMirror.Version");
    if ((identityMirrorVersion == null)  || (!identityMirrorVersion.equals("3"))) {
      if (identityMirrorVersion == null) identityMirrorVersion = "<missing>";
      String errorMessage = "Unknown identity mirroring version: " + identityMirrorVersion;
      logger.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }

    _targetZk = _targetConfig.getProperty("identityMirror.TargetZookeeper.connect");
    _targetBootstrapServers = _targetConfig.getProperty("bootstrap.servers");
    _targetClientId = _targetConfig.getProperty("client.id");
    _sourceClientId = _sourceConfig.getProperty("client.id");

    String checkRetries = _targetConfig.getProperty("identityMirror.MaxTopicCheckRetries");
    if ((checkRetries == null) || (checkRetries.equals(""))) {
      _maxTopicCheckRetries = MAX_TOPIC_CHECK_RETRIES;
    } else {
      _maxTopicCheckRetries = parseInt(_targetConfig.getProperty("identityMirror.MaxTopicCheckRetries"));
    }

    String checkTimeout = _targetConfig.getProperty("identityMirror.InitialCheckTimeout.ms");
    if ((checkTimeout == null) || (checkTimeout.equals(""))) {
      _initialCheckTimeoutMs = RETRY_SLEEP_TIME_MS;
    } else {
      _initialCheckTimeoutMs = parseInt(_targetConfig.getProperty("identityMirror.InitialCheckTimeout.ms"));
    }

    String cacheRefresh = _targetConfig.getProperty("identityMirror.TargetTopicCacheRefresh.ms");
    if ((cacheRefresh == null) || (cacheRefresh.equals(""))) {
      _topicCacheExpiration = DEFAULT_TOPIC_CACHE_EXPIRATION;
    } else {
      _topicCacheExpiration = parseInt(cacheRefresh);
    }

    String replicationFactor = _targetConfig.getProperty("identityMirror.TargetReplicationFactor");
    if ((replicationFactor == null) || (replicationFactor.equals(""))) {
      _targetReplicationFactor = DEFAULT_TARGET_REPLICATION_FACTOR;
    } else {
      _targetReplicationFactor = parseInt(replicationFactor);
    }

    String enableTopicDeletionPropagation = _targetConfig.getProperty("identityMirror.EnableTopicDeletionPropagation");
    if (enableTopicDeletionPropagation == null || enableTopicDeletionPropagation.isEmpty()) {
      _enableTopicDeletionPropagation = DEFAULT_ENABLE_TOPIC_DELETION_PROPAGATION;
    } else {
      _enableTopicDeletionPropagation = Boolean.parseBoolean(enableTopicDeletionPropagation);
    }

    String deletionPropagationDelay = _targetConfig.getProperty("identityMirror.TopicDeletionPropagationDelayMs");
    if (deletionPropagationDelay == null || deletionPropagationDelay.isEmpty()) {
      _topicDeletionPropagationDelayMs = DEFAULT_TOPIC_DELETION_PROPAGATION_DELAY_MS;
    } else {
      _topicDeletionPropagationDelayMs = parseInt(deletionPropagationDelay);
    }

    logger.info("Identity MirrorMaker connecting to Target Zookeeper: " + _targetZk);
    _zkUtilsTarget = ZkUtils.apply(_targetZk, DEFAULT_ZK_SESSION_TIMEOUT, DEFAULT_ZK_CONNECTION_TIMEOUT,
        false);

  }

  /**
   * Send an error to the logs and exit the program.
   *
   * @param errorMessage The error to be sent to the logs
   * @param errorCode The error code to send back when we exit
   */
  private void errorAndExit(String errorMessage, int errorCode) {
    errorAndExit(errorMessage, errorCode, null);
  }
    /**
     * Send an error to the logs, along with the exception (if any) and exit the program.
     *
     * @param errorMessage The error to be sent to the logs
     * @param errorCode The error code to send back when we exit
     */
  private void errorAndExit(String errorMessage, int errorCode, Throwable t) {
    try {
      if (t != null) {
        logger.error(errorMessage, t);
      } else {
        logger.error(errorMessage);
      }
      logger.error("Cannot recover from this error, must exit");
      LogManager.shutdown();
    } finally {
        PoisonPill.die(HEAP_DUMP_FOLDER, DEFAULT_MAX_HEAP_DUMP_WAIT_MS, errorCode);
    }
  }

  /**
   * Create a KafkaConsumer helper to retrieve the topic information from the target cluster.
   * @return a Consumer pointing at the target cluster.
   */
  private Consumer createTargetHelperConsumer() {
    logger.info("Identity MirrorMaker creating helper consumer for target cluster");
    _targetConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, _targetClientId + "-IMM-MetadataClient");
    return new KafkaConsumer<>(_targetConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer());
  }

  /**
   * Create a KafkaConsumer helper to retrieve the topic information from the source cluster.
   * @return a Consumer pointing at the source cluster.
   */
  private Consumer createSourceHelperConsumer() {
    logger.info("Identity MirrorMaker creating helper consumer for source cluster");
    _sourceConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, _sourceClientId + "-IMM-MetadataClient");
    return new KafkaConsumer<>(_sourceConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer());
  }

  /**
   * Get the partition map out of the specified cluster.
   * The current implementation uses a KafkaConsumer.
   * This function is synchronized since the clients are shared between threads.
   * @return A map of Partitions and their partition count.
   */
  private synchronized Map<String, Integer> getPartitionMap(Consumer client) {
    Map<String, List<PartitionInfo>> topicPartitionInfoMap = client.listTopics();
    Map<String, Integer> partitionMap = new HashMap<>();

    topicPartitionInfoMap.forEach((topic, partitionInfos) -> partitionMap.put(topic, partitionInfos.size()));

    return partitionMap;
  }

  /**
   * Get the partition map out of the target cluster.
   * The current implementation uses a KafkaConsumer.
   * This function is synchronized since _targetMetadataClient is shared between threads.
   * @return A map of Partitions and their partition count.
   */
  private synchronized Map<String, Integer> getTargetPartitionMap() {
    if (_targetMetadataClient == null) {
      _targetMetadataClient = createTargetHelperConsumer();
    }
    return getPartitionMap(_targetMetadataClient);
  }

  /**
   * Get the partition map out of the source cluster.
   * The current implementation uses a KafkaConsumer.
   * This function is synchronized since _sourceMetadataClient is shared between threads.
   * @return A map of Partitions and their partition count.
   */
  private synchronized Map<String, Integer> getSourcePartitionMap() {
    if (_sourceMetadataClient == null) {
      _sourceMetadataClient = createSourceHelperConsumer();
    }
    return getPartitionMap(_sourceMetadataClient);
  }

  /**
   * Check that a topic has the right number of partitions.
   * Error out if we find a topic that it has the wrong number of partitions
   * If it doesn't find the right number of partitions this will throw an exception to shut down the consumer
   *
   * @param topic The topic we are checking
   * @param desiredPartitions The desired partitions on that topic
   * @param actualPartitions The actual partitions on that topic
   */
  private void checkTopicPartitionsOrError(String topic, int desiredPartitions, int actualPartitions) {
    if (desiredPartitions <= actualPartitions) {
      logger.info("Topic " + topic + " exists already with partitionCount " + actualPartitions);
      return;
    }
    String errorMessage = "Topic: " + topic + " exists with "
        + actualPartitions + " partitions.  MirrorMaker was expecting at least "
        + desiredPartitions + " partitions.";
    errorAndExit(errorMessage, EXIT_PARTITIONS_DONT_MATCH);
  }

  /**
   * Determine which topics need to be mirrored by this node.
   * Right now this node will only mirror topics for which it's the owner of partition 0.
   *
   * @param partitions The list of partitions that are now assigned to the consumer
   * @param targetPartitionMap The partitions at the target
   * @return a Map of the topics to be mirrored and their partition count
   */
  private Map<String, Integer> getNewTopicsToBeMirrored(Collection<TopicPartition> partitions,
                                                        Map<String, Integer> sourcePartitionMap,
                                                        Map<String, Integer> targetPartitionMap,
                                                        Set<String> topicsToIgnore) {
    Map<String, Integer> newTopicsToBeMirrored = new HashMap<>();

    for (TopicPartition tp : partitions) {
      if (tp.partition() == 0 && !topicsToIgnore.contains(tp.topic())) {
        // We are the owner of partition 0 for the topic, we will have to do some work
        String topic = tp.topic();
        logger.info("Checking topic: " + topic);
        int desiredPartitions = sourcePartitionMap.get(topic);

        if (targetPartitionMap.containsKey(topic)) {
          checkTopicPartitionsOrError(topic, sourcePartitionMap.get(topic), targetPartitionMap.get(topic));
        } else {
          // topic does not exist in target cluster, we must create it, add it to newTopicsToBeMirrored
          logger.info("Topic " + topic + " not reflected in target yet. Needs " + desiredPartitions + " partitions");
          newTopicsToBeMirrored.put(topic, desiredPartitions);
        }
      }
    }
    return newTopicsToBeMirrored;
  }

  /**
   * Create a topic at a cluster defined by the zookeeper instance.
   * Create the topic with the set partition count and replication factor.
   * If the topic already exists and has a different number of partitions, throw an exception.
   * If the topic already exists but is not healthy, throw an exception
   * This function is synchronized since it may modify the partitions
   *
   * @param topicsToCreate The topic to partition number mapping to be created by this thread.
   * @param zkInstance The zookeeper instance for the creation
   */
  private synchronized void createTopics(Map<String, Integer> topicsToCreate, ZkUtils zkInstance) {
    logger.info("Creating " + topicsToCreate + " in target cluster");
    try {
      Set<String> topicsToDoubleCheck = new HashSet<>();
      for (Map.Entry<String, Integer> entry : topicsToCreate.entrySet()) {
        String topic = entry.getKey();
        Integer numPartitions = entry.getValue();
        logger.info("Creating topic " + topic + " with partitionCount " + numPartitions + " and replication "
                        + _targetReplicationFactor);
        try {
          AdminUtils.createTopic(zkInstance, topic, numPartitions, _targetReplicationFactor, new Properties(),
                                 RackAwareMode.Safe$.MODULE$);
        } catch (TopicExistsException e) {
          topicsToDoubleCheck.add(topic);
        }
      }
      if (topicsToDoubleCheck.isEmpty()) {
        return;
      }
      // The topic exists, must have been created after the last time we got our target state.
      // Let's update the state and check if we have the right number of partitions
      if (ensurePartitionNumberMatches(topicsToDoubleCheck, topicsToCreate)) {
        return;
      }

      // We don't have the partition at the target? Why did the createTopic return this error then?
      // Something is wrong, maybe we need more time to do the right thing.
      errorAndExit("Inconsistent state for " + topicsToDoubleCheck + " on " + zkInstance, EXIT_INCONSISTENT_STATE);
    } catch (Exception e) {
      errorAndExit("Could not create topic " + topicsToCreate + " on " + zkInstance,
          EXIT_CANT_CREATE_TOPIC, e);
    }
  }

  /**
   * Checks the partition number and exit if it doesn't match.
   */
  private boolean ensurePartitionNumberMatches(Set<String> toCheck, Map<String, Integer> topicsToCreate) {
    Set<String> topicsToCheck = new HashSet<>(toCheck);
    int retries = 0;
    while (true) {
      _targetPartitions = getTargetPartitionMap();
      Iterator<String> it = topicsToCheck.iterator();
      while (it.hasNext()) {
        String topic = it.next();
        int numPartitions = topicsToCreate.get(topic);
        if (_targetPartitions.keySet().contains(topic)) {
          checkTopicPartitionsOrError(topic, numPartitions, _targetPartitions.get(topic));
          it.remove();
        }
      }
      if (topicsToCheck.isEmpty()) {
        return true;
      }
      retries++;
      if (retries < _maxTopicCheckRetries) {
        logger.warn("Backing off to wait for " + topicsToCheck + " to show up");
        try {
          Thread.sleep(RETRY_SLEEP_TIME_MS);
        } catch (InterruptedException e1) {
          // let it go.
        }
      } else {
        return false;
      }
    }
  }

  /**
   * Schedule the topic deletion in the destination cluster.
   * @param topicsToDelete the set of topics to be deleted.
   */
  private void scheduleTopicDeletion(Set<String> topicsToDelete) {
    if (_topicDeletionExecutor == null) {
      _topicDeletionExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread t = new Thread(r);
          t.setDaemon(true);
          t.setUncaughtExceptionHandler((t1, e) -> logger.error("Received uncaught exception when deleting topic", e));
          return t;
        }
      });
    }
    long delayMs = _topicDeletionPropagationDelayMs + _random.nextInt(60000);
    logger.info("Scheduling deleting topics " + topicsToDelete + " with " + delayMs + " ms");
    _topicDeletionExecutor.schedule(new Runnable() {
      @Override
      public void run() {
        for (String topic : topicsToDelete) {
          // Someone else has deleted it.
          if (!_targetPartitions.containsKey(topic)) {
            continue;
          }
          try {
            logger.info("Deleting topic " + topic);
            AdminUtils.deleteTopic(_zkUtilsTarget, topic);
            while (_targetMetadataClient.listTopics().containsKey(topic)) {
              logger.debug("Waiting for " + topic + " to be deleted.");
              try {
                Thread.sleep(RETRY_SLEEP_TIME_MS);
              } catch (InterruptedException e) {
                logger.warn("Interrupted when waiting for deleting topic " + topic);
              }
            }
            logger.info("Successfully deleted topic " + topic);
          } catch (TopicAlreadyMarkedForDeletionException | UnknownTopicOrPartitionException e) {
            // let it go.
            logger.info("Topic " + topic + " has already been deleted.");
          } catch (Throwable t) {
            logger.error("Received exception when deleting topic " + topic, t);
          }
        }
      }
    }, delayMs, TimeUnit.MILLISECONDS);
  }

  /**
   * This is a helper function to get the targetPartitionMap and check if all the topics specified for creation
   * exist with the desired number of partitions.
   * This function is synchronized since _targetPartitions is shared between threads.
   *
   * @param topics A map of topics that were created and desired partition counts
   * @return Returns true if all topics specified have been created
   */
  private synchronized boolean topicsToBeMirroredMatchTarget(Map<String, Integer> topics) {
    _targetPartitions = getTargetPartitionMap();

    for (Map.Entry<String, Integer> entry : topics.entrySet()) {
      String topic = entry.getKey();
      if (_targetPartitions.containsKey(topic)) {
        checkTopicPartitionsOrError(topic, entry.getValue(), _targetPartitions.get(topic));
      } else {
        return false;
      }
    }
    return true;
  }

  /**
   * This function will mirror the new topics using a Shared target TopicPartition map.  This map is shared by all
   * threads.
   * This function is synchronized since the _targetPartitions map is shared between the rebalanceListener threads.
   * - First, check that we have a cached version of the target topic partition, if not, get one
   * - Using the new target partition map, check if given the new assignments we need to mirror a new topic
   * - If we do, mirror the topics needed, then get a new version of the target partition map (that reflects the new
   *   topics mirrored)
   *
   * This function returns a map of topics that are to be created, with their partition counts as values. This allows
   * the caller to monitor until all the topic creations are complete.
   *
   * @param partitions The list of partitions that are now assigned to the consumer
   * @return A map of topic names that are being created to desired partition counts
   */
  private synchronized Map<String, Integer> mirrorTopicsWithSharedCache(Collection<TopicPartition> partitions) {
    long currentTime = System.currentTimeMillis();
    if (_targetPartitions == null || (currentTime - _lastTargetTopicCacheClear) > _topicCacheExpiration) {
      // We don't have a cache of the _targetPartitions, we should get one.
      _targetPartitions = getTargetPartitionMap();
      _lastTargetTopicCacheClear = currentTime;
    }

    // We always get a source partition list, as it always needs to be fresh to reflect the current consumer state
    // We need the retry logic because the source cluster may have some metadata propagation lag.
    Set<String> topicsOfAssignedPartitions = new HashSet<>();
    partitions.forEach(tp -> topicsOfAssignedPartitions.add(tp.topic()));
    Map<String, Integer> sourcePartitions;
    Set<String> topicsToIgnore = new HashSet<>();
    int retries = 0;
    while (true) {
      sourcePartitions = getSourcePartitionMap();
      if (sourcePartitions.keySet().containsAll(topicsOfAssignedPartitions)) {
        break;
      } else {
        retries++;
        if (retries >= _maxTopicCheckRetries) {
          _targetPartitions = getTargetPartitionMap();
          _lastTargetTopicCacheClear = currentTime;
          topicsOfAssignedPartitions.removeAll(sourcePartitions.keySet());
          if (!_targetPartitions.keySet().containsAll(topicsOfAssignedPartitions)) {
            // See LIKAFKA-18823
            Set<String> missingTopics = new HashSet<>(topicsOfAssignedPartitions);
            missingTopics.removeAll(_targetPartitions.keySet());
            logger.warn("Cannot find topics " + missingTopics + " in the source cluster. These topics probably have been deleted soon after their creation");
          }
          topicsToIgnore.addAll(topicsOfAssignedPartitions);
          break;
        }
        // Sleep for some time and retry.
        try {
          Thread.sleep(RETRY_SLEEP_TIME_MS);
        } catch (InterruptedException e) {
          logger.warn("Received interrupted exception when waiting for metadata refresh");
        }
      }
    }

    Map<String, Integer> newTopicsToBeMirrored = getNewTopicsToBeMirrored(partitions, sourcePartitions,
        _targetPartitions, topicsToIgnore);
    logger.info("Done collecting " + newTopicsToBeMirrored.size() + " topics to be created");

    if (!newTopicsToBeMirrored.isEmpty()) {
      createTopics(newTopicsToBeMirrored, _zkUtilsTarget);
    }

    if (_enableTopicDeletionPropagation) {
      Set<String> topicsToBeDeleted = getTopicsToBeDeleted(sourcePartitions);
      logger.info("Done collecting " + topicsToBeDeleted.size() + " topics to be deleted");
      if (!topicsToBeDeleted.isEmpty()) {
        scheduleTopicDeletion(topicsToBeDeleted);
      }
    }

    return newTopicsToBeMirrored;
  }

  private Set<String> getTopicsToBeDeleted(Map<String, Integer> newSourcePartitions) {
    Set<String> topicsToBeDeleted = new HashSet<>();
    for (String topic : _targetPartitions.keySet()) {
      if (!newSourcePartitions.containsKey(topic)) {
        topicsToBeDeleted.add(topic);
      }
    }
    return topicsToBeDeleted;
  }

  /**
   * @param partitions The list of partitions that are now assigned to the consumer
   */
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    int tries = 0;
    long sleepTime = _initialCheckTimeoutMs;
    Map<String, Integer> topicsToBeMirrored = mirrorTopicsWithSharedCache(partitions);

    if (topicsToBeMirrored.size() != 0) {
      while (!topicsToBeMirroredMatchTarget(topicsToBeMirrored)) {
        logger.warn("Cannot see my mirrored topics reflected yet, will check again in " + sleepTime + " ms");
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          logger.warn("Sleep interrupted");
          logger.warn(e);
        }
        tries = tries + 1;
        sleepTime = sleepTime * 2;
        if (tries > _maxTopicCheckRetries) {
          // We tried too many times and our updates did not make it through, there is something going on.
          errorAndExit("Could not mirror required topics after " + _maxTopicCheckRetries + " retries",
              EXIT_CANT_CREATE_AFTER_RETRY);
        }
      }
    }
  }

  /**
   * Not used at this time
   *
   * @param partitions The list of partitions that were assigned to the consumer on the last rebalance
   */
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
  }
}
