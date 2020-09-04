package com.linkedin.venice.replication;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.kafka.TopicException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.ReflectUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.Optional;
import org.apache.log4j.Logger;


/**
 * Extend this class with an implementation that can start and end replication of data between two kafka topics
 * within the same kafka cluster.
 */
public abstract class TopicReplicator {

  private static final Logger LOGGER = Logger.getLogger(TopicReplicator.class);

  public static final String TOPIC_REPLICATOR_CONFIG_PREFIX = "topic.replicator.";
  public static final String TOPIC_REPLICATOR_CLASS_NAME = TOPIC_REPLICATOR_CONFIG_PREFIX + "class.name";
  public static final String TOPIC_REPLICATOR_SOURCE_KAFKA_CLUSTER = TOPIC_REPLICATOR_CONFIG_PREFIX + "source.kafka.cluster";
  public static final String TOPIC_REPLICATOR_SOURCE_SSL_KAFKA_CLUSTER = TOPIC_REPLICATOR_CONFIG_PREFIX + "source.ssl.kafka.cluster";

  private final TopicManager topicManager;
  protected final String destKafkaBootstrapServers;
  private final VeniceWriterFactory veniceWriterFactory;
  private final Time timer;

  /**
   * Intended to allow mocking by tests. Visibility package-private on purpose, but could be changed to
   * protected if child classes need it.
   */
  Time getTimer() {
    return timer;
  }

  /**
   * Intended to allow mocking by tests. Visibility package-private on purpose, but could be changed to
   * protected if child classes need it.
   */
  VeniceWriterFactory getVeniceWriterFactory() {
    return this.veniceWriterFactory;
  }

  private TopicReplicator(TopicManager topicManager, VeniceWriterFactory veniceWriterFactory, Time timer,
      VeniceProperties veniceProperties) {
    this.topicManager = topicManager;
    this.veniceWriterFactory = veniceWriterFactory;
    this.timer = timer;
    if (veniceProperties.getBoolean(ConfigKeys.ENABLE_TOPIC_REPLICATOR_SSL, false)) {
      destKafkaBootstrapServers = veniceProperties.getString(TopicReplicator.TOPIC_REPLICATOR_SOURCE_SSL_KAFKA_CLUSTER,
          () -> veniceProperties.getString(ConfigKeys.SSL_KAFKA_BOOTSTRAP_SERVERS)); // fallback
    } else {
      destKafkaBootstrapServers = veniceProperties.getString(TopicReplicator.TOPIC_REPLICATOR_SOURCE_KAFKA_CLUSTER,
          () -> veniceProperties.getString(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS)); // fallback
    }
  }

  public TopicReplicator(TopicManager topicManager, VeniceWriterFactory veniceWriterFactory,
      VeniceProperties veniceProperties) {
    this(topicManager, veniceWriterFactory, new SystemTime(), veniceProperties);
  }

  /**
   * Performs some topic related validation and call internal replication methods to start replication between source
   * and destination topic.
   * @param rewindStartTimestamp to indicate the rewind start time for underlying replicators to start replicating
   *                             records from this timestamp.
   * @throws TopicException
   */
  public void beginReplication(String sourceTopic, String destinationTopic, long rewindStartTimestamp) throws TopicException {
    if (doesReplicationExist(sourceTopic, destinationTopic)) {
      LOGGER.info("Replication already exists from src: " + sourceTopic + " to dest: " + destinationTopic
          + ". Skip starting replication.");
      return;
    }
    LOGGER.info("Starting topic replication from: " + sourceTopic + " to " + destinationTopic);
    String errorPrefix = "Cannot begin replication from " + sourceTopic + " to " + destinationTopic + " because";
    if (sourceTopic.equals(destinationTopic)){
      throw new DuplicateTopicException(errorPrefix + " they are the same topic.");
    }
    if (!getTopicManager().containsTopicAndAllPartitionsAreOnline(sourceTopic)){
      throw new TopicDoesNotExistException(errorPrefix + " topic " + sourceTopic + " does not exist.");
    }
    if (!getTopicManager().containsTopicAndAllPartitionsAreOnline(destinationTopic)){
      throw new TopicDoesNotExistException(errorPrefix + " topic " + destinationTopic + " does not exist.");
    }
    int sourcePartitionCount = getTopicManager().getPartitions(sourceTopic).size();
    int destinationPartitionCount = getTopicManager().getPartitions(destinationTopic).size();
    if (sourcePartitionCount != destinationPartitionCount){
      throw new PartitionMismatchException(errorPrefix + " topic " + sourceTopic + " has " + sourcePartitionCount + " partitions"
          + " and topic " + destinationTopic + " has " + destinationPartitionCount + " partitions."  );
    }
    beginReplicationInternal(sourceTopic, destinationTopic, sourcePartitionCount, rewindStartTimestamp);
    LOGGER.info("Successfully started topic replication from: " + sourceTopic + " to " + destinationTopic);
  }

  public void terminateReplication(String sourceTopic, String destinationTopic) {
    terminateReplicationInternal(sourceTopic, destinationTopic);
  }

  /**
   * General verification and topic creation for any {@link TopicReplicator} implementation used for hybrid stores.
   */
  protected void checkPreconditions(String srcTopicName, String destTopicName, Store store) {
    // Carrying on assuming that there needs to be only one and only TopicManager
    if (!store.isHybrid()) {
      throw new VeniceException("Topic replication is only supported for Hybrid Stores.");
    }
    /**
     * TopicReplicator is used in child fabrics to create real-time (RT) topic when a child fabric
     * is ready to start buffer replay but RT topic doesn't exist. This scenario could happen for a
     * hybrid store when users haven't started any Samza job yet. In this case, RT topic should be
     * created with proper retention time instead of the default 5 days retention.
     *
     * Potential race condition: If both rewind-time update operation and buffer-replay
     * start at the same time, RT topic might not be created with the expected retention time,
     * which can be fixed by sending another rewind-time update command.
     *
     * TODO: RT topic should be created in both parent and child fabrics when the store is converted to
     *       hybrid (update store command handling). However, if a store is converted to hybrid when it
     *       doesn't have any existing version or a correct storage quota, we cannot decide the partition
     *       number for it.
     */
    if (!getTopicManager().containsTopicAndAllPartitionsAreOnline(srcTopicName)) {
      int partitionCount = getTopicManager().getPartitions(destTopicName).size();
      int replicationFactor = getTopicManager().getReplicationFactor(destTopicName);
      getTopicManager().createTopic(srcTopicName,
                                    partitionCount,
                                    replicationFactor,
                                    store.getHybridStoreConfig().getRetentionTimeInMs(),
                                    // Disable RT compaction. Because for hybrid stores in Online/Offline mode,
                                    // with RT compaction, lag will be mis-calculated.
                                    false,
                                    Optional.empty(),
                                    false);
    } else {
      /**
       * If real-time topic already exists, check whether its retention time is correct.
       */
      long topicRetentionTimeInMs = getTopicManager().getTopicRetention(srcTopicName);
      if (topicRetentionTimeInMs != store.getHybridStoreConfig().getRetentionTimeInMs()) {
        getTopicManager().updateTopicRetention(srcTopicName, store.getHybridStoreConfig().getRetentionTimeInMs());
      }
    }
  }

  protected long getRewindStartTime(Store store) {
    // TODO to get a more deterministic timestamp across colo we could use the timestamp from the EOP control message.
    return getTimer().getMilliseconds() - store.getHybridStoreConfig().getRewindTimeInSeconds() * Time.MS_PER_SECOND;
  }

  abstract public void prepareAndStartReplication(String srcTopicName, String destTopicName, Store store);
  abstract void beginReplicationInternal(String sourceTopic, String destinationTopic, int partitionCount,
      long rewindStartTimestamp);
  abstract void terminateReplicationInternal(String sourceTopic, String destinationTopic);

  /**
   * Only used by tests
   */
  abstract public boolean doesReplicationExist(String sourceTopic, String destinationTopic);

  /**
   * Reflectively instantiates a {@link TopicManager} based on the passed in {@param veniceProperties}.
   *
   * The properties must contain {@value #TOPIC_REPLICATOR_CLASS_NAME}, otherwise, instantiation is not
   * attempted an instance of {@link Optional#empty()} is returned instead. If the class name is specified
   * but other properties required by the concrete implementation are missing, the implementation is
   * allowed to throw exceptions.
   *
   * @param topicManager to be used by the {@link TopicReplicator}
   * @param veniceProperties containing the class name and other implementation-specific configs
   * @return an instance of {@link Optional<TopicReplicator>}, or empty if {@param veniceProperties} is empty.
   */
  public static Optional<TopicReplicator> getTopicReplicator(TopicManager topicManager,
      VeniceProperties veniceProperties, VeniceWriterFactory veniceWriterFactory) {
    boolean enableTopicReplicator = veniceProperties.getBoolean(ConfigKeys.ENABLE_TOPIC_REPLICATOR, false);
    if (!enableTopicReplicator) {
      return Optional.empty();
    }
    String className = veniceProperties.getString(TOPIC_REPLICATOR_CLASS_NAME); // Will throw if absent
    return getTopicReplicator(className, topicManager, veniceProperties, veniceWriterFactory);
  }

  public static Optional<TopicReplicator> getTopicReplicator(String className,
                                                             TopicManager topicManager,
                                                             VeniceProperties veniceProperties,
                                                             VeniceWriterFactory veniceWriterFactory) {
    boolean enableTopicReplicator = veniceProperties.getBoolean(ConfigKeys.ENABLE_TOPIC_REPLICATOR, false);
    if (!enableTopicReplicator) {
      return Optional.empty();
    }

    try {
      Class<? extends TopicReplicator> topicReplicatorClass = ReflectUtils.loadClass(className);
      Class<TopicManager> param1Class = ReflectUtils.loadClass(TopicManager.class.getName());
      Class<VeniceWriterFactory> param2Class = ReflectUtils.loadClass(VeniceWriterFactory.class.getName());
      Class<VeniceProperties> param3Class = ReflectUtils.loadClass(VeniceProperties.class.getName());
      TopicReplicator topicReplicator = ReflectUtils.callConstructor(
          topicReplicatorClass,
          new Class[]{param1Class, param2Class, param3Class},
          new Object[]{topicManager, veniceWriterFactory, veniceProperties});

      return Optional.of(topicReplicator);
    } catch (Exception e) {
      throw new VeniceException("Failed to construct a TopicReplicator!", e);
    }
  }

  /**
   * The source and destination topic for the replication are the same topic
   */
  public static class DuplicateTopicException extends TopicException {
    public DuplicateTopicException(String message){
      super(message);
    }
  }

  /**
   * The source and destination topics for replication have different numbers of partitions.
   */
  public static class PartitionMismatchException extends TopicException {
    public PartitionMismatchException(String message){
      super(message);
    }
  }

  /**
   * Package-private visibility intended for mocking in tests.
   */
  TopicManager getTopicManager() {
    return this.topicManager;
  }
}
