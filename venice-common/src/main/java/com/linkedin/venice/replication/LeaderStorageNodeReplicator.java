package com.linkedin.venice.replication;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import org.apache.log4j.Logger;


public class LeaderStorageNodeReplicator extends TopicReplicator {

  private static final Logger logger = Logger.getLogger(LeaderStorageNodeReplicator.class);

  /**
   * Constructor used by reflection.
   */
  public LeaderStorageNodeReplicator(TopicManager topicManager, VeniceWriterFactory veniceWriterFactory,
      VeniceProperties veniceProperties) {
    super(topicManager, veniceWriterFactory, veniceProperties);
  }

  /**
   * The meaning of destination topic (destTopicName) is slightly different in the leader follower world. The
   * destination topic is where we send the topic switch message instead of the destination topic for the replication.
   * TODO Refactor/remove the {@link TopicReplicator} interface to fix this convoluted parameter.
   */
  @Override
  public void prepareAndStartReplication(String srcTopicName, String destTopicName, Store store) {
    checkPreconditions(srcTopicName, destTopicName, store);
    long bufferReplayStartTime = getRewindStartTime(store);
    Optional<Version> version = store.getVersion(Version.parseVersionFromKafkaTopicName(destTopicName));
    if (!version.isPresent()) {
      throw new VeniceException("Corresponding version does not exist for topic: " + destTopicName + " in store: "
          + store.getName());
    }
    String finalDestTopicName = version.get().getPushType().isStreamReprocessing() ?
        Version.composeStreamReprocessingTopic(store.getName(), version.get().getNumber()) : destTopicName;
    logger.info("Starting buffer replay for topic: " + finalDestTopicName
        + " with buffer replay start timestamp: " + bufferReplayStartTime);
    beginReplication(srcTopicName, finalDestTopicName, bufferReplayStartTime);
  }

  @Override
  void beginReplicationInternal(String sourceTopic, String destinationTopic, int partitionCount,
      long rewindStartTimestamp) {
    List<CharSequence> sourceClusters = new ArrayList<>();
    // Currently we only have intra-cluster replication, therefore source cluster bootstrap servers equals to destination
    sourceClusters.add(destKafkaBootstrapServers);
    getVeniceWriterFactory().useVeniceWriter(
        () -> getVeniceWriterFactory().createBasicVeniceWriter(destinationTopic, getTimer()),
        veniceWriter -> veniceWriter.broadcastTopicSwitch(sourceClusters, sourceTopic, rewindStartTimestamp, new HashMap<>())
    );
  }

  @Override
  void terminateReplicationInternal(String sourceTopic, String destinationTopic) {
    // No op because deleting the L/F helix resource will automatically terminate the leader replication stream and
    // currently there is no scenario that we would terminate the stream while keeping the L/F helix resources running.
  }

  @Override
  public boolean doesReplicationExist(String sourceTopic, String destinationTopic) {
    // Always return false unless there is a problem with emitting duplicate topic switch control messages.
    return false;
  }
}
