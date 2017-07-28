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
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;

import java.io.Closeable;
import java.util.*;
import static java.util.concurrent.TimeUnit.*;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;


/**
 * Extend this class with an implementation that can start and end replication of data between two kafka topics
 * within the same kafka cluster.
 */
public abstract class TopicReplicator implements Closeable {

  private static final Logger LOGGER = Logger.getLogger(TopicReplicator.class);

  public static final String TOPIC_REPLICATOR_CONFIG_PREFIX = "topic.replicator.";
  public static final String TOPIC_REPLICATOR_CLASS_NAME = TOPIC_REPLICATOR_CONFIG_PREFIX + "class.name";
  public static final String TOPIC_REPLICATOR_SOURCE_KAFKA_CLUSTER = TOPIC_REPLICATOR_CONFIG_PREFIX + "source.kafka.cluster";

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

  private TopicReplicator(TopicManager topicManager, String destKafkaBootstrapServers, VeniceWriterFactory veniceWriterFactory, Time timer) {
    this.topicManager = topicManager;
    this.destKafkaBootstrapServers = destKafkaBootstrapServers;
    this.veniceWriterFactory = veniceWriterFactory;
    this.timer = timer;
  }

  public TopicReplicator(TopicManager topicManager, String destKafkaBootstrapServers) {
    this(topicManager, destKafkaBootstrapServers, VeniceWriterFactory.get(), new SystemTime());
  }

  public void beginReplication(String sourceTopic, String destinationTopic, Optional<Map<Integer, Long>> startingOffsets)
      throws TopicException {
    LOGGER.info("Starting topic replication from: " + sourceTopic + " to " + destinationTopic);
    String errorPrefix = "Cannot create replication datastream from " + sourceTopic + " to " + destinationTopic + " because";
    if (sourceTopic.equals(destinationTopic)){
      throw new DuplicateTopicException(errorPrefix + " they are the same topic.");
    }
    if (!getTopicManager().containsTopic(sourceTopic)){
      throw new TopicDoesNotExistException(errorPrefix + " topic " + sourceTopic + " does not exist.");
    }
    if (!getTopicManager().containsTopic(destinationTopic)){
      throw new TopicDoesNotExistException(errorPrefix + " topic " + destinationTopic + " does not exist.");
    }
    int sourcePartitionCount = getTopicManager().getPartitions(sourceTopic).size();
    int destinationPartitionCount = getTopicManager().getPartitions(destinationTopic).size();
    if (sourcePartitionCount != destinationPartitionCount){
      throw new PartitionMismatchException(errorPrefix + " topic " + sourceTopic + " has " + sourcePartitionCount + " partitions"
          + " and topic " + destinationTopic + " has " + destinationPartitionCount + " partitions."  );
    }
    beginReplicationInternal(sourceTopic, destinationTopic, sourcePartitionCount, startingOffsets);
    LOGGER.info("Successfully started topic replication from: " + sourceTopic + " to " + destinationTopic);
  }

  public void terminateReplication(String sourceTopic, String destinationTopic){
    terminateReplicationInternal(sourceTopic, destinationTopic);
  }

  public Map<Integer, Long> startBufferReplay(String srcTopicName,
                                              String destTopicName,
                                              Store store) throws TopicException {
    String srcKafkaBootstrapServers = destKafkaBootstrapServers; // TODO: Add this as a function parameter
    // Carrying on assuming that there needs to be only one and only TopicManager
    if (!store.isHybrid()) {
      throw new VeniceException("Buffer replay is only supported for Hybrid Stores.");
    }
    if (!getTopicManager().containsTopic(srcTopicName)) {
      int partitionCount = getTopicManager().getPartitions(destTopicName).size();
      int replicationFactor = getTopicManager().getReplicationFactor(destTopicName);
      getTopicManager().createTopic(srcTopicName, partitionCount, replicationFactor, false);
    }
    long bufferReplayStartTime = getTimer().getMilliseconds()
        - MILLISECONDS.convert(store.getHybridStoreConfig().getRewindTimeInSeconds(), SECONDS);
    Map<Integer, Long> startingOffsetsMap = getTopicManager().getOffsetsByTime(srcTopicName, bufferReplayStartTime);
    List<Long> startingOffsets = startingOffsetsMap.entrySet().stream()
        .sorted((o1, o2) -> o1.getKey().compareTo(o2.getKey()))
        .map(entry -> entry.getValue())
        .collect(Collectors.toList());
    VeniceWriter<byte[], byte[]> veniceWriter = getVeniceWriterFactory().getBasicVeniceWriter(destKafkaBootstrapServers, destTopicName, getTimer());
    veniceWriter.broadcastStartOfBufferReplay(startingOffsets, srcKafkaBootstrapServers, srcTopicName, new HashMap<>());
    beginReplication(srcTopicName, destTopicName, Optional.of(startingOffsetsMap));
    return startingOffsetsMap;
  }

  abstract void beginReplicationInternal(String sourceTopic, String destinationTopic, int partitionCount, Optional<Map<Integer, Long>> startingOffsets);
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
                                                             VeniceProperties veniceProperties) {
    boolean enableTopicReplicator = veniceProperties.getBoolean(ConfigKeys.ENABLE_TOPIC_REPLICATOR, false);
    if (!enableTopicReplicator) {
      return Optional.empty();
    }

    try {
      String className = veniceProperties.getString(TOPIC_REPLICATOR_CLASS_NAME); // Will throw if absent

      Class<? extends TopicReplicator> topicReplicatorClass = ReflectUtils.loadClass(className);
      Class<TopicManager> param1Class = ReflectUtils.loadClass(TopicManager.class.getName());
      Class<VeniceProperties> param2Class = ReflectUtils.loadClass(VeniceProperties.class.getName());
      TopicReplicator topicReplicator = ReflectUtils.callConstructor(
          topicReplicatorClass,
          new Class[]{param1Class, param2Class},
          new Object[]{topicManager, veniceProperties});

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
