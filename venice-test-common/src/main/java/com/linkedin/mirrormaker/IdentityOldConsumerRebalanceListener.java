package com.linkedin.mirrormaker;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.consumer.ConsumerThreadId;
import kafka.utils.ZkUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.log4j.Logger;

import static java.lang.Integer.parseInt;


/**
 * N.B.: This class comes from LiKafka. It ought to be part of Kafka proper but it is not yet open-sourced.
 * This class is necessary in order to spin-up MirrorMaker in tests.
 *
 * This is a class used for identity mirroring.  It is loaded by MirrorMaker (which in turn passes it on to Consumer).
 * It gets called whenever topics change. Using this effect, it creates topics in the target cluster that mirror the
 * source cluster.  At this time it mirrors partitioning as well as replication factor.
 *
 * Currently it uses zookeeper to create the topics on the target cluster. It also uses zookeeper to detect the topic
 * metadata on the source cluster. We expect this to eventually be moved to direct communication with the broker (see KIP4)
 *
 * Note that for correct behavior, the (target) cluster is expected to be configured with auto-topic create OFF.
 * Only the mirrorMaker thread which owns partition 0 of a topic will try to create that topic in the target cluster.
 * The other producer threads may continue and try to produce data to the topic. Due to timing this production may happen
 * before the topic is created. The producers are expected to block (waiting for the topic metadata) until the topic gets
 * created.
 *
 * The thread in charge of creating a topic will ensure that the topic is created by looping until it receives positive
 * confirmation that the topic has been created. This may take some time since the creation currently happens via zookeeper
 * (and is considered an async create) and the check for existence happens directly with the brokers.
 * The check loop is based on an exponential backoff with a starting value of 100ms (configurable via the
 * kafkaMirrorMaker.identityMirroringInitialCheckTimeoutMs variable). The code will try MAX_TOPIC_CHECK_RETRIES tries
 * (current default set at 10), after which it will fail.
 *
 * Side effects:
 *   When a topic already exists in the destination cluster, this class will check that it has the same partition count
 *   as the source topic. If it doesn't, this class will throw an exception.
 *
 **************
 * Version 1
 *   Required parameters:
 *       <property name="kafkaMirrorMaker.identityMirroringVersion"         value="1" />
 *       <property name="kafkaMirrorMaker.identityMirroringTargetZookeeper" value="zookeeperURL_with_port" />
 *       <property name="kafkaMirrorMaker.identityMirroringTargetBootstrapServers" value="brokerURL_with_port" />
 *   Optional parameters:
 *       <property name="kafkaMirrorMaker.identityMirroringInitialCheckTimeoutMs" value="100" />
 *
 **************
 * Version 2
 *   Required parameters:
 *       <property name="kafkaMirrorMaker.identityMirroringVersion"         value="2" />
 *       <property name="kafkaMirrorMaker.identityMirroringTargetZookeeper" value="zookeeperURL_with_port" />
 *   Optional parameters:
 *       <property name="kafkaMirrorMaker.identityMirroringInitialCheckTimeoutMs" value="100" />
 *
 **************
 *
 * These configuration parameters are passed to this class via different ways. Version 1 expects an encoded string with
 * the parameters see getConfigString(ClusterProtocol, String, String, String, int). Version 2 expects an encoded string
 * that points to 2 config files. See getConfigString(String, String).
 */
public class IdentityOldConsumerRebalanceListener implements kafka.javaapi.consumer.ConsumerRebalanceListener {

  private Properties _sourceConfig;
  private Properties _targetConfig;

  private String _targetBootstrapServers;
  private String _targetZk;
  private String _sourceZk;

  private ZkUtils _zkUtilsTarget;
  private ZkUtils _zkutilsSource;

  private Map<String, Integer> _targetPartitions = null;

  private Consumer _metadataClient;

  private int _initialCheckTimeoutMs;
  private long _topicCacheExpiration;
  private long _lastTargetTopicCacheClear = 0;

  private static final int RETRY_SLEEP_TIME_MS            = 100;
  private static final int MAX_TOPIC_CHECK_RETRIES        = 10;
  private static final int DEFAULT_TOPIC_CACHE_EXPIRATION = 600000;
  private static final int DEFAULT_ZK_SESSION_TIMEOUT     = 30000;
  private static final int DEFAULT_ZK_CONNECTION_TIMEOUT  = 30000;

  private static final int EXIT_PARTITIONS_DONT_MATCH     = -250;
  private static final int EXIT_INCONSISTENT_STATE        = -251;
  private static final int EXIT_CANT_CREATE_TOPIC         = -252;
  private static final int EXIT_CANT_CREATE_AFTER_RETRY   = -253;
  private static final int EXIT_CANT_OPEN_FILE            = -254;

  public  static final int EXIT_UNKNOWN_IDENTITY_MIRRORING_VERSION = -500;

  public  static final String CONFIG_DELIMITER = "#";

  static final Logger logger = Logger.getLogger(IdentityOldConsumerRebalanceListener.class.getName());

  /**
   * Constructor for the identity rebalance listener. It expects a configuration string. The format of this string
   * can be created using the configSerializer* functions.
   *
   * @param parameters The config parameters, encoded as a string
   */
  public IdentityOldConsumerRebalanceListener(String parameters) {
    parseListenerConfig(parameters);
    IdentityOldConsumerRebalanceListener.logger.info("Initializing IdentityOldConsumerRebalanceListener:"
        + " _targetZk=" + _targetZk + " _sourceZk=" + _sourceZk
        + " targetBootstrapServers=" + _targetBootstrapServers + " _initialCheckTimeoutMs=" + _initialCheckTimeoutMs);
  }

  /**
   * Not used at this time
   *
   * @param partitionOwnership The partition ownership map
   */
  public void beforeReleasingPartitions(Map<String, Set<Integer>> partitionOwnership) {
  }

  /**
   * This callback will tell us when topics have changed.
   *
   * It will mirrorTopics and make sure that all work that was done is actually reflected by the target cluster.
   * It uses an exponential backoff of retries with a sleep in between.  This is done _outside_ the object lock.
   * This function calls synchronized functions.
   *
   * It retires for MAX_TOPIC_CHECK_RETRIES. The timer will be doubled every time starting with initialCheckoutTimeoutMs.
   * This initial timer can be set via configuration variables.
   *
   * @param consumerId  The id of this rebalanceListener thread.
   * @param globalPartitionAssignment  The partition assignment from the source cluster
   */
  public void beforeStartingFetchers(String consumerId, Map<String, Map<Integer, ConsumerThreadId>> globalPartitionAssignment) {
    int tries = 0;
    long sleepTime = _initialCheckTimeoutMs;
    boolean doneMirroringTopics = mirrorTopicsWithSharedCache(consumerId, globalPartitionAssignment);

    while (!doneMirroringTopics) {
      IdentityOldConsumerRebalanceListener.logger.warn("Cannot see my mirrored topics reflected yet, will check again in " + sleepTime + " ms");
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        IdentityOldConsumerRebalanceListener.logger.warn("Sleep interrupted for " + consumerId);
        IdentityOldConsumerRebalanceListener.logger.warn(e);
      }
      tries = tries + 1;
      sleepTime = sleepTime * 2;
      doneMirroringTopics = updateCacheAndCheckTopicsToMirror(consumerId, globalPartitionAssignment);
      if (tries > MAX_TOPIC_CHECK_RETRIES) {
        // We tried too many times and our updates did not make it through, there is something going on.
        errorAndExit("Could not mirror required topics after " + MAX_TOPIC_CHECK_RETRIES + " retries", EXIT_CANT_CREATE_AFTER_RETRY);
      }
    }
  }

  /**
   * Get the partition map out of the target cluster.
   * The current implementation uses a kafkaConsumer.
   * This function is synchronized since _metadataClient is shared between threads.
   * @return A map of Partitions and their partition count.
   */
  private synchronized Map<String, Integer> getTargetPartitionMap(String myConsumerId) {
    if (_metadataClient == null) {
      _metadataClient = createTargetHelperConsumer(myConsumerId);
    }
    Map<String, List<PartitionInfo>> topicPartitionInfoMap = _metadataClient.listTopics();
    Map<String, Integer> partitionMap = new HashMap<>();

    for (Map.Entry<String, List<PartitionInfo>> topicPartitionEntry : topicPartitionInfoMap.entrySet()) {
      partitionMap.put(topicPartitionEntry.getKey(), topicPartitionEntry.getValue().size());
    }

    return partitionMap;
  }

  /**
   * Create a KafkaConsumer helper to retrieve the topic information from the target cluster.
   * @return a KafkaConsumer pointing at the target cluster.
   */
  private Consumer createTargetHelperConsumer(String myConsumerId) {
    IdentityOldConsumerRebalanceListener.logger.info("Identity MirrorMaker creating helper consumer");
    _targetConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, myConsumerId + "-IMM-MetadataClient");
    return new KafkaConsumer<>(_targetConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer());
  }

  /**
   * This is a helper function to get the targetPartitionMap and check if we have any work to be done. It is used for
   * the check loop after we create the topics we are in charge of.
   * This function is synchronized since _targetPartitions is shared between threads.
   *
   * @param myConsumerId  This thread identifier
   * @param globalPartitionAssignment The partition assignment at the source cluster.
   * @return Returns true if we still have topics to mirror
   */
  private synchronized boolean updateCacheAndCheckTopicsToMirror(String myConsumerId, Map<String, Map<Integer, ConsumerThreadId>> globalPartitionAssignment) {
    _targetPartitions = getTargetPartitionMap(myConsumerId);
    Map<String, Integer> newTopicsToBeMirrored = getNewTopicsToBeMirrored(myConsumerId, globalPartitionAssignment,
        _targetPartitions);
    return (newTopicsToBeMirrored.size() == 0);
  }

  /**
   * This function will mirror the new topics using a Shared target TopicPartition map.  This map is shared by all threads.
   * This function is synchronized since the _targetPartitions map is shared between the rebalanceListener threads.
   * - First, check that we have a cached version of the target topic partition, if not, get one
   * - Using the new target partition map, check if given the new assignments we need to mirror a new topic
   * - If we do, mirror the topics needed, then get a new version of the target partition map (that reflects the new topics mirrored)
   *
   * This function will return true if we are done mirroring topics.  That is, if all our work is already reflected at the brokers.
   * If we did no work then this will also be true. If we did work (created a topic) but it's not yet reflected, then we will
   * return false. This allows the caller to monitor until our work is actually reflected.
   *
   * @param consumerId The identifier of this consumer
   * @param globalPartitionAssignment The source partition assignments
   * @return whether we are done mirroring topics
   */
  private synchronized boolean mirrorTopicsWithSharedCache(String consumerId, Map<String, Map<Integer, ConsumerThreadId>> globalPartitionAssignment) {
    long currentTime = System.currentTimeMillis();
    if (_targetPartitions == null || (currentTime - _lastTargetTopicCacheClear) > _topicCacheExpiration) {
      // We don't have a cache of the _targetPartitions, we should get one.
      _targetPartitions = getTargetPartitionMap(consumerId);
      _lastTargetTopicCacheClear = currentTime;
    }
    Map<String, Integer> newTopicsToBeMirrored = getNewTopicsToBeMirrored(consumerId, globalPartitionAssignment,
        _targetPartitions);

    if (newTopicsToBeMirrored.size() != 0) {
      mirrorTopics(consumerId, newTopicsToBeMirrored);
      // We mirrored some topics, hence we need to get the new state from the broker.
      // Note that this may have also changed from another mirror maker creating topics
      return updateCacheAndCheckTopicsToMirror(consumerId, globalPartitionAssignment);
    }
    return true;
  }

  /**
   * Check that a topic has the right number of partitions.
   * Error out if we find a topic that it has the wrong number of partitions
   * If it doesn't find the right number of partitions this will throw an exception to shut down the consumer
   *
   * @param consumerId  The consumerId of the thread that is doing the work
   * @param topic The topic we are checking
   * @param desiredPartitions The desired partitions on that topic
   * @param actualPartitions The actual partitions on that topic
   */
  private void checkTopicPartitionsOrError(String consumerId, String topic, int desiredPartitions, int actualPartitions) {
    if (desiredPartitions == actualPartitions) {
      IdentityOldConsumerRebalanceListener.logger.trace("Topic " + topic + " exists already with the desired partitionCount " + actualPartitions);
      return;
    } else if (desiredPartitions < actualPartitions) {
      IdentityOldConsumerRebalanceListener.logger.warn("Topic " + topic + " exists already with more partitionCount " + actualPartitions);
      return;
    }
    String errorMessage = "[" + consumerId + " ] Topic: " + topic + " exists with "
        + actualPartitions + " partitions.  MirrorMaker was expecting at least "
        + desiredPartitions + " partitions.";
    errorAndExit(errorMessage, EXIT_PARTITIONS_DONT_MATCH);
  }

  /**
   * Send an error to the logs and exit the program.
   *
   * @param errorMessage The error to be sent to the logs
   * @param errorCode The error code to send back when we exit
   */
  private void errorAndExit(String errorMessage, int errorCode) {
    IdentityOldConsumerRebalanceListener.logger.error(errorMessage);
    IdentityOldConsumerRebalanceListener.logger.error("Cannot recover from this error, must exit");
    Runtime.getRuntime().halt(errorCode);
  }


  /**
   * Determine which topics need to be mirrored by this node.
   * Right now this node will only mirror topics for which it's the owner of partition 0.
   *
   * @param consumerId The identifier of this consumer
   * @param globalPartitionAssignment The partition mirroring assignment, including the consumerIds per partition
   * @param targetPartitionMap The partitions at the target
   * @return a Map of the topics to be mirrored and their partition count
   */
  private Map<String, Integer> getNewTopicsToBeMirrored(String consumerId,
      Map<String, Map<Integer, ConsumerThreadId>> globalPartitionAssignment,
      Map<String, Integer> targetPartitionMap) {
    Map<String, Integer> newTopicsToBeMirrored = new HashMap<>();

    for (Map.Entry<String, Map<Integer, ConsumerThreadId>> topicPartitionEntry : globalPartitionAssignment.entrySet()) {
      int desiredPartitions = topicPartitionEntry.getValue().size();
      String topic = topicPartitionEntry.getKey();
      if (topicPartitionEntry.getValue().get(0).toString().startsWith(consumerId)) {
        // We are the owner of partition 0 for the topic, we will have to do some work
        if (targetPartitionMap.containsKey(topic)) {
          checkTopicPartitionsOrError(consumerId, topic, desiredPartitions, targetPartitionMap.get(topic));
        } else {
          // topic does not exist in target cluster, we must create it, add it to newTopicsToBeMirrored
          IdentityOldConsumerRebalanceListener.logger.info("Topic " + topic + " not reflected in target yet. Needs " + desiredPartitions + " partitions (" + consumerId + ")");
          newTopicsToBeMirrored.put(topic, desiredPartitions);
        }
      }
    }
    return newTopicsToBeMirrored;
  }

  /**
   * mirror the required topics.  Create the topics on the target cluster using the partition count and replication factor from the source cluster.
   * @param newTopicsToBeMirrored The topics to be mirrored by this thread
   */
  private void mirrorTopics(String consumerId, Map<String, Integer> newTopicsToBeMirrored) {
    for (Map.Entry<String, Integer> topicEntry : newTopicsToBeMirrored.entrySet()) {
      createTopic(consumerId, topicEntry.getKey(), topicEntry.getValue(), getTopicReplicationFactor(topicEntry.getKey(), _zkutilsSource),
          _zkUtilsTarget);
    }
  }



  /**
   * Get the topic replication factor from a zookeeper cluster.  If we can't get the information (for example, the topic doesn't exist)
   * we throw an exception.
   *
   * @param topic topic to get the factor of
   * @param zkInstance the zookeeper instance to communicate to
   * @return the repliaction factor of topic according to zkInstance
   */
  private static int getTopicReplicationFactor(String topic, ZkUtils zkInstance) {
    MetadataResponse.TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkInstance);
    if (topicMetadata.error() != Errors.NONE) {
      String errorMessage = "Could not retrieve replication factor for topic: " + topic + " from  " + zkInstance;
      IdentityOldConsumerRebalanceListener.logger.error(errorMessage);
      throw new IllegalStateException(errorMessage);
    }
    List<MetadataResponse.PartitionMetadata> partitionMetadatas = topicMetadata.partitionMetadata();
    return partitionMetadatas.get(0).replicas().size();
  }

  /**
   * Create a topic at a cluster defined by the zookeeper instance.
   * Create the topic with the set partition count and replication factor.
   * If the topic already exists and has a different number of partitions, throw an exception.
   * If the topic already exists but is not healthy, throw an exception
   * This function is synchronized since it may modify the partitions
   *
   * @param topic The topic to be created
   * @param partitions The number of partitions for the topic
   * @param replicationFactor The replication factor for the topic
   * @param zkInstance The zookeeper instance for the creation
   */
  private synchronized void createTopic(String consumerId, String topic, int partitions, int replicationFactor, ZkUtils zkInstance) {
    IdentityOldConsumerRebalanceListener.logger.info("Creating topic " + topic + " with partitionCount " + partitions + " and replication " + replicationFactor);
    try {
      AdminUtils.createTopic(zkInstance, topic, partitions, replicationFactor, new Properties(), RackAwareMode.Safe$.MODULE$);
    } catch (TopicExistsException e) {
      // The topic exists, must have been created after the last time we got our target state.
      // Let's update the state and check if we have the right number of partitions
      _targetPartitions = getTargetPartitionMap(consumerId);
      if (_targetPartitions.containsKey(topic)) {
        checkTopicPartitionsOrError(consumerId, topic, partitions, _targetPartitions.get(topic));
        return;
      }
      // We don't have the partition at the target? Why did the createTopic return this error then?
      // Something is wrong, maybe we need more time to do the right thing.
      errorAndExit("Inconsistent state for " + topic + " on " + zkInstance, EXIT_INCONSISTENT_STATE);
    } catch (Exception e) {
      errorAndExit("Could not create topic " + topic + " on " + zkInstance + " " + e, EXIT_CANT_CREATE_TOPIC);
    }
  }

  /**
   * Protocols that can be used to connect to the clusters.
   * For now we support Zookeeper but eventually we expect to support native mode (KIP-4)
   * This gets passed to the config serializer which will generate a string to pass to the custom RebalanceListener
   */
  enum ClusterProtocol {
    ZOOKEEPER
  }

  /**
   * Create a serialized configuration string that uses zookeeper to mirror between source and destination clusters.
   * This configuration string can be passed to the constructor of this class.
   *
   * @param protocol Protocol used to connect to the source and destination clusters
   * @param sourceClusterZk source zookeeper
   * @param targetClusterZk target zookeeper
   * @param targetBootstrapServers target broker bootstrap servers
   * @param initialCheckTimeoutMs The initial timeout to use when checking for our work to be reflected by the brokers
   * @return a serialized configuration string
   */
  static String getConfigString(ClusterProtocol protocol, String sourceClusterZk, String targetClusterZk,
      String targetBootstrapServers, int initialCheckTimeoutMs) {
    return getConfigString(protocol, sourceClusterZk, targetClusterZk, targetBootstrapServers) + CONFIG_DELIMITER + initialCheckTimeoutMs;
  }

  /**
   * This creates a serialized configuration string to pass to the constructor. It uses the default value for
   * _initialCheckTimeoutMs
   *
   * @param protocol Protocol used to connect to the source and destination clusters
   * @param sourceClusterZk source zookeeper
   * @param targetClusterZk target zookeeper
   * @param targetBootstrapServers target broker bootstrap servers
   * @return a serialized configuration string
   */
  static String getConfigString(ClusterProtocol protocol, String sourceClusterZk, String targetClusterZk, String targetBootstrapServers) {
    String serializedConfig;

    if (!protocol.equals(ClusterProtocol.ZOOKEEPER)) {
      throw new IllegalArgumentException("Unsupported protocol for config serialization in Identity Mirror Rebalancer");
    }
    serializedConfig = "zk" + CONFIG_DELIMITER + sourceClusterZk + CONFIG_DELIMITER + targetClusterZk + CONFIG_DELIMITER
        + targetBootstrapServers;
    return serializedConfig;
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
  static String getConfigString(String sourceConfigFileName, String targetConfigFileName) {
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

    if (configTokens[0].equals("zk")) {
      parseZkConfig(configTokens);
    } else if (configTokens[0].equals("file")) {
      parseFileConfig(configTokens);
    } else {
      String errorMessage = "Could not parse config for Identity rebalancing: " + serializedConfig + " " + configTokens[0];
      IdentityOldConsumerRebalanceListener.logger.error(errorMessage);
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
      IdentityOldConsumerRebalanceListener.logger.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }
    // We received 2 config files

    try {
      _sourceConfig = Utils.loadProps(configTokens[1]);
      _targetConfig = Utils.loadProps(configTokens[2]);

      IdentityOldConsumerRebalanceListener.logger.info("source config:" + _sourceConfig.toString());
      IdentityOldConsumerRebalanceListener.logger.info("target config:" + _targetConfig.toString());
    } catch (IOException e) {
      errorAndExit("Could not read config files " + configTokens[1] + "," + configTokens[2], EXIT_CANT_OPEN_FILE);
    }

    String identityMirrorVersion = _targetConfig.getProperty("identityMirror.Version");
    if ((identityMirrorVersion == null)  || (!identityMirrorVersion.equals("2"))) {
      if (identityMirrorVersion == null) identityMirrorVersion = "<missing>";
      String errorMessage = "Unknown identity mirroring version: " + identityMirrorVersion;
      IdentityOldConsumerRebalanceListener.logger.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }

    _sourceZk = _sourceConfig.getProperty("zookeeper.connect");
    _targetZk = _targetConfig.getProperty("identityMirror.TargetZookeeper.connect");
    _targetBootstrapServers = _targetConfig.getProperty("bootstrap.servers");

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


    IdentityOldConsumerRebalanceListener.logger.info("Identity MirrorMaker connecting to Source zookeeper: " + _sourceZk);
    _zkutilsSource = ZkUtils.apply(_sourceZk, DEFAULT_ZK_SESSION_TIMEOUT, DEFAULT_ZK_CONNECTION_TIMEOUT, false);
    IdentityOldConsumerRebalanceListener.logger.info("Identity MirrorMaker connecting to Target Zookeeper: " + _targetZk);
    _zkUtilsTarget = ZkUtils.apply(_targetZk, DEFAULT_ZK_SESSION_TIMEOUT, DEFAULT_ZK_CONNECTION_TIMEOUT, false);

  }


  /**
   * Parse a zookeeper tokenized config string.
   * @param configTokens The tokens of the config string. The first token is the selector for this mode of configuration
   *                     The second is the source zookeeper URL
   *                     The third is the target zookeeper URL
   *                     The fourth is the target bootstrap server URL
   *                     The fifth, if present, is the initialCheckTimeoutMs.
   */
  private void parseZkConfig(String[] configTokens) {
    if ((configTokens.length != 4) && (configTokens.length != 5)) {
      String errorMessage = "Could not parse config for Identity rebalancing ZK: " + Arrays.toString(configTokens);
      IdentityOldConsumerRebalanceListener.logger.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }
    // Old style config. We should remove this soon...
    _sourceZk = configTokens[1];
    _targetZk = configTokens[2];
    _targetBootstrapServers = configTokens[3];

    if (configTokens.length >= 5) {
      _initialCheckTimeoutMs = parseInt(configTokens[4]);
    } else {
      _initialCheckTimeoutMs = RETRY_SLEEP_TIME_MS;
    }

    _zkutilsSource = ZkUtils.apply(_sourceZk, DEFAULT_ZK_SESSION_TIMEOUT, DEFAULT_ZK_CONNECTION_TIMEOUT, false);
    _zkUtilsTarget = ZkUtils.apply(_targetZk, DEFAULT_ZK_SESSION_TIMEOUT, DEFAULT_ZK_CONNECTION_TIMEOUT, false);
  }
}