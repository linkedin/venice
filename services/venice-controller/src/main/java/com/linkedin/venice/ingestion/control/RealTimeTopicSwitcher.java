package com.linkedin.venice.ingestion.control;

import static com.linkedin.venice.ConfigKeys.KAFKA_MIN_IN_SYNC_REPLICAS_RT_TOPICS;
import static com.linkedin.venice.ConfigKeys.KAFKA_REPLICATION_FACTOR;
import static com.linkedin.venice.ConfigKeys.KAFKA_REPLICATION_FACTOR_RT_TOPICS;
import static com.linkedin.venice.VeniceConstants.REWIND_TIME_DECIDED_BY_SERVER;
import static com.linkedin.venice.pubsub.PubSubConstants.DEFAULT_KAFKA_REPLICATION_FACTOR;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.DataReplicationPolicy;
import com.linkedin.venice.meta.HybridStoreConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.StoreUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
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
  private final int kafkaReplicationFactorForRTTopics;
  private final int kafkaReplicationFactor;
  private final Optional<Integer> minSyncReplicasForRTTopics;

  private final PubSubTopicRepository pubSubTopicRepository;

  public RealTimeTopicSwitcher(
      TopicManager topicManager,
      VeniceWriterFactory veniceWriterFactory,
      VeniceProperties veniceProperties,
      PubSubTopicRepository pubSubTopicRepository) {
    this.topicManager = topicManager;
    this.veniceWriterFactory = veniceWriterFactory;
    this.pubSubTopicRepository = pubSubTopicRepository;
    this.timer = new SystemTime();
    this.destKafkaBootstrapServers =
        veniceProperties.getBooleanWithAlternative(ConfigKeys.KAFKA_OVER_SSL, ConfigKeys.SSL_TO_KAFKA_LEGACY, false)
            ? veniceProperties.getString(ConfigKeys.SSL_KAFKA_BOOTSTRAP_SERVERS)
            : veniceProperties.getString(ConfigKeys.KAFKA_BOOTSTRAP_SERVERS);
    this.kafkaReplicationFactor = veniceProperties.getInt(KAFKA_REPLICATION_FACTOR, DEFAULT_KAFKA_REPLICATION_FACTOR);
    this.kafkaReplicationFactorForRTTopics =
        veniceProperties.getInt(KAFKA_REPLICATION_FACTOR_RT_TOPICS, kafkaReplicationFactor);
    this.minSyncReplicasForRTTopics = veniceProperties.getOptionalInt(KAFKA_MIN_IN_SYNC_REPLICAS_RT_TOPICS);
  }

  /**
   * Performs some topic related validation and call internal replication methods to start replication between source
   * and destination topic.
   * @param rewindStartTimestamp to indicate the rewind start time for underlying replicators to start replicating
   *                             records from this timestamp.
   * @param remoteKafkaUrls URLs of Kafka clusters which are sources of remote replication (either native replication
   *                        is enabled or A/A is enabled)
   */
  void sendTopicSwitch(
      PubSubTopic realTimeTopic,
      PubSubTopic topicWhereToSendTheTopicSwitch,
      long rewindStartTimestamp,
      List<String> remoteKafkaUrls) {
    String errorPrefix = "Cannot send TopicSwitch into '" + topicWhereToSendTheTopicSwitch
        + "' instructing to switch to '" + realTimeTopic + "' because";
    if (realTimeTopic.equals(topicWhereToSendTheTopicSwitch)) {
      throw new DuplicateTopicException(errorPrefix + " they are the same topic.");
    }
    if (!getTopicManager().containsTopicAndAllPartitionsAreOnline(realTimeTopic)) {
      throw new PubSubTopicDoesNotExistException(errorPrefix + " topic " + realTimeTopic + " does not exist.");
    }
    if (!getTopicManager().containsTopicAndAllPartitionsAreOnline(topicWhereToSendTheTopicSwitch)) {
      throw new PubSubTopicDoesNotExistException(
          errorPrefix + " topic " + topicWhereToSendTheTopicSwitch + " does not exist.");
    }
    int destinationPartitionCount = getTopicManager().getPartitionCount(topicWhereToSendTheTopicSwitch);
    List<CharSequence> sourceClusters = new ArrayList<>();
    if (!remoteKafkaUrls.isEmpty()) {
      sourceClusters.addAll(remoteKafkaUrls);
    } else {
      sourceClusters.add(destKafkaBootstrapServers);
    }

    try (VeniceWriter<byte[], byte[], byte[]> veniceWriter = getVeniceWriterFactory().createVeniceWriter(
        new VeniceWriterOptions.Builder(topicWhereToSendTheTopicSwitch.getName()).setTime(getTimer())
            .setPartitionCount(destinationPartitionCount)
            .build())) {
      veniceWriter
          .broadcastTopicSwitch(sourceClusters, realTimeTopic.getName(), rewindStartTimestamp, Collections.emptyMap());
    }
    LOGGER.info(
        "Successfully sent TopicSwitch into '{}' instructing to switch to '{}' with a rewindStartTimestamp of {}.",
        topicWhereToSendTheTopicSwitch,
        realTimeTopic,
        rewindStartTimestamp);
  }

  /**
   * General verification and topic creation for hybrid stores.
   */
  void ensurePreconditions(
      PubSubTopic srcTopicName,
      PubSubTopic topicWhereToSendTheTopicSwitch,
      Store store,
      Optional<HybridStoreConfig> hybridStoreConfig) {
    // Carrying on assuming that there needs to be only one and only TopicManager
    if (!hybridStoreConfig.isPresent()) {
      throw new VeniceException("Topic switching is only supported for Hybrid Stores.");
    }
    Version version =
        store.getVersion(Version.parseVersionFromKafkaTopicName(topicWhereToSendTheTopicSwitch.getName()));
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
    createRealTimeTopicIfNeeded(store, version, srcTopicName, hybridStoreConfig.get());
    if (version != null && version.isSeparateRealTimeTopicEnabled()) {
      PubSubTopic separateRealTimeTopic =
          pubSubTopicRepository.getTopic(Version.composeSeparateRealTimeTopic(store.getName()));
      createRealTimeTopicIfNeeded(store, version, separateRealTimeTopic, hybridStoreConfig.get());
    }
  }

  void createRealTimeTopicIfNeeded(
      Store store,
      Version version,
      PubSubTopic realTimeTopic,
      HybridStoreConfig hybridStoreConfig) {
    if (!getTopicManager().containsTopicAndAllPartitionsAreOnline(realTimeTopic)) {
      int partitionCount;
      if (version != null) {
        partitionCount = version.getPartitionCount();
      } else {
        partitionCount = store.getPartitionCount();
      }
      int replicationFactor = realTimeTopic.isRealTime() ? kafkaReplicationFactorForRTTopics : kafkaReplicationFactor;
      Optional<Integer> minISR = realTimeTopic.isRealTime() ? minSyncReplicasForRTTopics : Optional.empty();
      getTopicManager().createTopic(
          realTimeTopic,
          partitionCount,
          replicationFactor,
          StoreUtils.getExpectedRetentionTimeInMs(store, hybridStoreConfig),
          false,
          minISR,
          false);
    } else {
      /**
       * If real-time topic already exists, check whether its retention time is correct.
       */
      long topicRetentionTimeInMs = getTopicManager().getTopicRetention(realTimeTopic);
      long expectedRetentionTimeMs = StoreUtils.getExpectedRetentionTimeInMs(store, hybridStoreConfig);
      if (topicRetentionTimeInMs != expectedRetentionTimeMs) {
        getTopicManager().updateTopicRetention(realTimeTopic, expectedRetentionTimeMs);
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
      rewindTimeInMs = Math.min(PubSubConstants.BUFFER_REPLAY_MINIMAL_SAFETY_MARGIN, rewindTimeInMs);
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

  public void transmitVersionSwapMessage(Store store, int previousVersion, int nextVersion) {

    if (previousVersion == Store.NON_EXISTING_VERSION || nextVersion == Store.NON_EXISTING_VERSION) {
      // NoOp
      return;
    }

    Version previousStoreVersion = store.getVersionOrThrow(previousVersion);
    Version nextStoreVersion = store.getVersionOrThrow(nextVersion);

    // Only transmit version swap message to RT's if there is a view config (temporary check)
    if (!hasViewConfigs(nextStoreVersion, previousStoreVersion)) {
      // NoOp for now
      return;
    }

    // Only transmit version swap for stores which have an RT.
    // if a previous version didn't have an RT, then there will be no
    // version consuming the topic switch message. We'll transmit the version switch
    // message so long as there exists some RT
    if (!topicManager.containsTopic(pubSubTopicRepository.getTopic(Version.composeRealTimeTopic(store.getName())))) {
      // NoOp
      return;
    }
    // Write the thing!
    try (VeniceWriter veniceWriter = getVeniceWriterFactory().createVeniceWriter(
        new VeniceWriterOptions.Builder(Version.composeRealTimeTopic(store.getName())).setTime(getTimer())
            .setPartitionCount(previousStoreVersion.getPartitionCount())
            .build())) {
      veniceWriter.broadcastVersionSwap(
          previousStoreVersion.kafkaTopicName(),
          nextStoreVersion.kafkaTopicName(),
          Collections.emptyMap());
    }
    LOGGER.info(
        "Successfully sent VersionTopicSwitch for store {} from version {} to version {}",
        store.getName(),
        previousVersion,
        nextVersion);
  }

  // TODO: Delete this function once we have confidence in version swap to not stipulate views as a precondition for
  // transmitting version swap messages on RT.
  public boolean hasViewConfigs(Version nextStoreVersion, Version previousStoreVersion) {
    return ((previousStoreVersion.getViewConfigs() != null) && !previousStoreVersion.getViewConfigs().isEmpty())
        || ((nextStoreVersion.getViewConfigs() != null) && !nextStoreVersion.getViewConfigs().isEmpty());
  }

  public void switchToRealTimeTopic(
      String realTimeTopicName,
      String topicNameWhereToSendTheTopicSwitch,
      Store store,
      String aggregateRealTimeSourceKafkaUrl,
      List<String> activeActiveRealTimeSourceKafkaURLs) {
    PubSubTopic realTimeTopic = pubSubTopicRepository.getTopic(realTimeTopicName);
    PubSubTopic topicWhereToSendTheTopicSwitch = pubSubTopicRepository.getTopic(topicNameWhereToSendTheTopicSwitch);
    if (!realTimeTopic.isRealTime()) {
      throw new IllegalArgumentException("The realTimeTopicName param is invalid: " + realTimeTopic);
    }

    Version version =
        store.getVersionOrThrow(Version.parseVersionFromKafkaTopicName(topicWhereToSendTheTopicSwitch.getName()));

    Optional<HybridStoreConfig> hybridStoreConfig;
    if (version.isUseVersionLevelHybridConfig()) {
      hybridStoreConfig = Optional.ofNullable(version.getHybridStoreConfig());
    } else {
      hybridStoreConfig = Optional.ofNullable(store.getHybridStoreConfig());
    }
    ensurePreconditions(realTimeTopic, topicWhereToSendTheTopicSwitch, store, hybridStoreConfig);
    long rewindStartTimestamp = getRewindStartTime(version, hybridStoreConfig, version.getCreatedTime());
    PubSubTopic finalTopicWhereToSendTheTopicSwitch = version.getPushType().isStreamReprocessing()
        ? pubSubTopicRepository.getTopic(Version.composeStreamReprocessingTopic(store.getName(), version.getNumber()))
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
        realTimeTopic,
        rewindStartTimestamp);
    sendTopicSwitch(realTimeTopic, finalTopicWhereToSendTheTopicSwitch, rewindStartTimestamp, remoteKafkaUrls);
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
