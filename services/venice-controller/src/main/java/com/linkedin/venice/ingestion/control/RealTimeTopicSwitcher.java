package com.linkedin.venice.ingestion.control;

import static com.linkedin.venice.VeniceConstants.REWIND_TIME_DECIDED_BY_SERVER;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.kafka.TopicException;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Class which implements the process of switching from a batch topic (e.g. version-topic or stream reprocessing topic)
 * to a real-time topic, including:
 *
 * 1. Ensuring the pre-conditions are met,
 * 2. Determining the start timestamp to rewind to,
 * 3. Writing the actual {@link com.linkedin.venice.kafka.protocol.TopicSwitch} control message.
 */
public class RealTimeTopicSwitcher {
  private static final Logger LOGGER = LogManager.getLogger(RealTimeTopicSwitcher.class);

  private final TopicManager topicManager;
  private final String destKafkaBootstrapServers;
  private final VeniceWriterFactory veniceWriterFactory;
  private final Time timer;

  public RealTimeTopicSwitcher(
      TopicManager topicManager,
      VeniceWriterFactory veniceWriterFactory,
      VeniceProperties veniceProperties) {
    this.topicManager = topicManager;
    this.veniceWriterFactory = veniceWriterFactory;
    this.timer = new SystemTime();
    this.destKafkaBootstrapServers = veniceProperties.getBoolean(ConfigKeys.SSL_TO_KAFKA, false)
        ? veniceProperties.getString(ConfigKeys.SSL_KAFKA_BOOTSTRAP_SERVERS)
        : veniceProperties.getString(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS);
  }

  /**
   * Performs some topic related validation and call internal replication methods to start replication between source
   * and destination topic.
   * @param rewindStartTimestamp to indicate the rewind start time for underlying replicators to start replicating
   *                             records from this timestamp.
   * @param remoteKafkaUrls URLs of Kafka clusters which are sources of remote replication (either native replication
   *                        is enabled or A/A is enabled)
   * @throws TopicException
   */
  void sendTopicSwitch(
      String realTimeTopicName,
      String topicWhereToSendTheTopicSwitch,
      long rewindStartTimestamp,
      List<String> remoteKafkaUrls) throws TopicException {
    String errorPrefix = "Cannot send TopicSwitch into '" + topicWhereToSendTheTopicSwitch
        + "' instructing to switch to '" + realTimeTopicName + "' because";
    if (realTimeTopicName.equals(topicWhereToSendTheTopicSwitch)) {
      throw new DuplicateTopicException(errorPrefix + " they are the same topic.");
    }
    if (!getTopicManager().containsTopicAndAllPartitionsAreOnline(realTimeTopicName)) {
      throw new TopicDoesNotExistException(errorPrefix + " topic " + realTimeTopicName + " does not exist.");
    }
    if (!getTopicManager().containsTopicAndAllPartitionsAreOnline(topicWhereToSendTheTopicSwitch)) {
      throw new TopicDoesNotExistException(
          errorPrefix + " topic " + topicWhereToSendTheTopicSwitch + " does not exist.");
    }
    int destinationPartitionCount = getTopicManager().partitionsFor(topicWhereToSendTheTopicSwitch).size();
    List<CharSequence> sourceClusters = new ArrayList<>();
    if (!remoteKafkaUrls.isEmpty()) {
      sourceClusters.addAll(remoteKafkaUrls);
    } else {
      sourceClusters.add(destKafkaBootstrapServers);
    }
    try (VeniceWriter veniceWriter = getVeniceWriterFactory().createBasicVeniceWriter(
        topicWhereToSendTheTopicSwitch,
        getTimer(),
        new DefaultVenicePartitioner(),
        destinationPartitionCount)) {
      veniceWriter
          .broadcastTopicSwitch(sourceClusters, realTimeTopicName, rewindStartTimestamp, Collections.emptyMap());
    }
    LOGGER.info(
        "Successfully sent TopicSwitch into '{}' instructing to switch to '{}' with a rewindStartTimestamp of {}.",
        topicWhereToSendTheTopicSwitch,
        realTimeTopicName,
        rewindStartTimestamp);
  }

  /**
   * General verification and topic creation for hybrid stores.
   */
  void ensurePreconditions(
      String srcTopicName,
      String topicWhereToSendTheTopicSwitch,
      Store store,
      Optional<HybridStoreConfig> hybridStoreConfig) {
    // Carrying on assuming that there needs to be only one and only TopicManager
    if (!hybridStoreConfig.isPresent()) {
      throw new VeniceException("Topic switching is only supported for Hybrid Stores.");
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
      int partitionCount;
      Optional<Version> version =
          store.getVersion(Version.parseVersionFromKafkaTopicName(topicWhereToSendTheTopicSwitch));
      if (version.isPresent()) {
        partitionCount = version.get().getPartitionCount();
      } else {
        partitionCount = store.getPartitionCount();
      }
      int replicationFactor = getTopicManager().getReplicationFactor(topicWhereToSendTheTopicSwitch);
      getTopicManager().createTopic(
          srcTopicName,
          partitionCount,
          replicationFactor,
          TopicManager.getExpectedRetentionTimeInMs(store, hybridStoreConfig.get()),
          false, // Note: do not enable RT compaction! Might make jobs in Online/Offline model stuck
          Optional.empty(),
          false);
    } else {
      /**
       * If real-time topic already exists, check whether its retention time is correct.
       */
      long topicRetentionTimeInMs = getTopicManager().getTopicRetention(srcTopicName);
      long expectedRetentionTimeMs = TopicManager.getExpectedRetentionTimeInMs(store, hybridStoreConfig.get());
      if (topicRetentionTimeInMs != expectedRetentionTimeMs) {
        getTopicManager().updateTopicRetention(srcTopicName, expectedRetentionTimeMs);
      }
    }
  }

  long getRewindStartTime(
      Version version,
      Optional<HybridStoreConfig> hybridStoreConfig,
      long versionCreationTimeInMs) {
    /**
     * For A/A mode rewindStartTime should be consistent across each colo. Setting a sentinel value here will let the Leader
     * calculate a deterministic and consistent rewindStartTimestamp in all colos.
     */
    if (version.isActiveActiveReplicationEnabled()) {
      return REWIND_TIME_DECIDED_BY_SERVER;
    }
    long rewindTimeInMs = hybridStoreConfig.get().getRewindTimeInSeconds() * Time.MS_PER_SECOND;
    if (version.getDataRecoveryVersionConfig() != null) {
      // Override the user rewind if the version is under data recovery to avoid data loss when user have short rewind.
      rewindTimeInMs = Math.min(TopicManager.BUFFER_REPLAY_MINIMAL_SAFETY_MARGIN, rewindTimeInMs);
    }
    switch (hybridStoreConfig.get().getBufferReplayPolicy()) {
      // TODO to get a more deterministic timestamp across colo we could use the timestamp from the SOP/EOP control
      // message.
      case REWIND_FROM_SOP:
        return versionCreationTimeInMs - rewindTimeInMs;
      case REWIND_FROM_EOP:
      default:
        return getTimer().getMilliseconds() - rewindTimeInMs;
    }
  }

  public void switchToRealTimeTopic(
      String realTimeTopicName,
      String topicWhereToSendTheTopicSwitch,
      Store store,
      String aggregateRealTimeSourceKafkaUrl,
      List<String> activeActiveRealTimeSourceKafkaURLs) {
    if (!Version.isRealTimeTopic(realTimeTopicName)) {
      throw new IllegalArgumentException("The realTimeTopicName param is invalid: " + realTimeTopicName);
    }
    Version version = store.getVersion(Version.parseVersionFromKafkaTopicName(topicWhereToSendTheTopicSwitch))
        .orElseThrow(
            () -> new VeniceException(
                "Corresponding version does not exist for topic: " + topicWhereToSendTheTopicSwitch + " in store: "
                    + store.getName()));

    Optional<HybridStoreConfig> hybridStoreConfig;
    if (version.isUseVersionLevelHybridConfig()) {
      hybridStoreConfig = Optional.ofNullable(version.getHybridStoreConfig());
    } else {
      hybridStoreConfig = Optional.ofNullable(store.getHybridStoreConfig());
    }
    ensurePreconditions(realTimeTopicName, topicWhereToSendTheTopicSwitch, store, hybridStoreConfig);
    long rewindStartTimestamp = getRewindStartTime(version, hybridStoreConfig, version.getCreatedTime());
    String finalTopicWhereToSendTheTopicSwitch = version.getPushType().isStreamReprocessing()
        ? Version.composeStreamReprocessingTopic(store.getName(), version.getNumber())
        : topicWhereToSendTheTopicSwitch;
    List<String> remoteKafkaUrls = new ArrayList<>(Math.max(1, activeActiveRealTimeSourceKafkaURLs.size()));

    if (version.isActiveActiveReplicationEnabled()) {
      remoteKafkaUrls.addAll(activeActiveRealTimeSourceKafkaURLs);
    } else if (version.isNativeReplicationEnabled() && (isAggregate(store) || (isIncrementalPush(version)))) {
      remoteKafkaUrls.add(aggregateRealTimeSourceKafkaUrl);
    }
    LOGGER.info(
        "Will send TopicSwitch into '{}' instructing to switch to '{}' with a rewindStartTimestamp of {}.",
        topicWhereToSendTheTopicSwitch,
        realTimeTopicName,
        rewindStartTimestamp);
    sendTopicSwitch(realTimeTopicName, finalTopicWhereToSendTheTopicSwitch, rewindStartTimestamp, remoteKafkaUrls);
  }

  private static boolean isAggregate(Store store) {
    return store.getHybridStoreConfig().getDataReplicationPolicy() == DataReplicationPolicy.AGGREGATE;
  }

  private static boolean isIncrementalPush(Version version) {
    return version.isIncrementalPushEnabled();
  }

  /**
   * Package-private visibility intended for mocking in tests.
   */
  TopicManager getTopicManager() {
    return this.topicManager;
  }

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
}
