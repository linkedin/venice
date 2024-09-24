package com.linkedin.venice.meta;

import static java.lang.Character.isDigit;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.systemstore.schemas.StoreVersion;
import com.linkedin.venice.views.VeniceView;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 * This interface defines all the public APIs, and if you need to add accessors to
 * some new fields, this interface needs to be changed accordingly.
 */
@JsonDeserialize(as = VersionImpl.class)
public interface Version extends Comparable<Version>, DataModelBackedStructure<StoreVersion> {
  String VERSION_SEPARATOR = "_v";
  String REAL_TIME_TOPIC_SUFFIX = "_rt";
  String STREAM_REPROCESSING_TOPIC_SUFFIX = "_sr";
  String SEPARATE_REAL_TIME_TOPIC_SUFFIX = "_rt_sep";
  /**
   * Special number indicating no replication metadata version is set.
   */
  int REPLICATION_METADATA_VERSION_ID_UNSET = -1;

  /**
   * Prefix used in push id to indicate the version's data source is coming from an existing version topic.
   */
  String VENICE_RE_PUSH_PUSH_ID_PREFIX = "venice_re_push_";

  /**
   * Producer type for writing data to Venice
   */
  enum PushType {
    BATCH(0), // Batch jobs will create a new version topic and write to it in a batch manner.
    STREAM_REPROCESSING(1), // reprocessing jobs will create a new version topic and a reprocessing topic.
    STREAM(2), // Stream jobs will write to a buffer or RT topic.
    INCREMENTAL(3); // Incremental jobs will re-use an existing version topic and write on top of it.

    private final int value;

    PushType(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }

    public boolean isBatch() {
      return this == BATCH;
    }

    public boolean isIncremental() {
      return this == INCREMENTAL;
    }

    public boolean isStreamReprocessing() {
      return this == STREAM_REPROCESSING;
    }

    public boolean isBatchOrStreamReprocessing() {
      return isBatch() || isStreamReprocessing();
    }

    public static PushType valueOf(int value) {
      Optional<PushType> pushType = Arrays.stream(values()).filter(p -> p.value == value).findFirst();
      if (!pushType.isPresent()) {
        throw new VeniceException("Invalid push type with int value: " + value);
      }
      return pushType.get();
    }
  }

  int getNumber();

  void setNumber(int number);

  long getCreatedTime();

  @JsonIgnore
  Duration getAge();

  @JsonIgnore
  void setAge(Duration age);

  VersionStatus getStatus();

  void setStatus(VersionStatus status);

  CompressionStrategy getCompressionStrategy();

  void setCompressionStrategy(CompressionStrategy compressionStrategy);

  default boolean isLeaderFollowerModelEnabled() {
    return true;
  }

  boolean isNativeReplicationEnabled();

  @Deprecated
  default void setLeaderFollowerModelEnabled(boolean leaderFollowerModelEnabled) {
  }

  void setNativeReplicationEnabled(boolean nativeReplicationEnabled);

  String getPushStreamSourceAddress();

  void setPushStreamSourceAddress(String address);

  void setBufferReplayEnabledForHybrid(boolean bufferReplayEnabledForHybrid);

  boolean isChunkingEnabled();

  void setChunkingEnabled(boolean chunkingEnabled);

  boolean isRmdChunkingEnabled();

  void setRmdChunkingEnabled(boolean rmdChunkingEnabled);

  String getStoreName();

  String getPushJobId();

  void setPushJobId(String pushJobId);

  PushType getPushType();

  void setPushType(PushType pushType);

  void setPartitionCount(int partitionCount);

  int getPartitionCount();

  PartitionerConfig getPartitionerConfig();

  void setPartitionerConfig(PartitionerConfig partitionerConfig);

  boolean isVersionSwapDeferred();

  void setVersionSwapDeferred(boolean versionSwapDeferred);

  int getReplicationFactor();

  void setReplicationFactor(int replicationFactor);

  int getMinActiveReplicas();

  String getNativeReplicationSourceFabric();

  void setNativeReplicationSourceFabric(String nativeReplicationSourceFabric);

  boolean isIncrementalPushEnabled();

  void setIncrementalPushEnabled(boolean incrementalPushEnabled);

  boolean isSeparateRealTimeTopicEnabled();

  void setSeparateRealTimeTopicEnabled(boolean separateRealTimeTopicEnabled);

  boolean isBlobTransferEnabled();

  void setBlobTransferEnabled(boolean blobTransferEnabled);

  boolean isUseVersionLevelIncrementalPushEnabled();

  void setUseVersionLevelIncrementalPushEnabled(boolean versionLevelIncrementalPushEnabled);

  HybridStoreConfig getHybridStoreConfig();

  void setHybridStoreConfig(HybridStoreConfig hybridConfig);

  Map<String, ViewConfig> getViewConfigs();

  void setViewConfigs(Map<String, ViewConfig> viewConfigMap);

  boolean isUseVersionLevelHybridConfig();

  void setUseVersionLevelHybridConfig(boolean versionLevelHybridConfig);

  boolean isActiveActiveReplicationEnabled();

  void setActiveActiveReplicationEnabled(boolean activeActiveReplicationEnabled);

  DataRecoveryVersionConfig getDataRecoveryVersionConfig();

  void setDataRecoveryVersionConfig(DataRecoveryVersionConfig dataRecoveryVersionConfig);

  /**
   * Get the replication metadata version id.
   * @deprecated
   * Use {@link Version#getRmdVersionId} instead
   *
   * @return the replication metadata version id
   */
  @Deprecated
  default int getTimestampMetadataVersionId() {
    return getRmdVersionId();
  }

  /**
   * Set the replication metadata version id.
   * @deprecated
   * Use {@link Version#setRmdVersionId(int)} instead
   */
  @Deprecated
  default void setTimestampMetadataVersionId(int replicationMetadataVersionId) {
    setRmdVersionId(replicationMetadataVersionId);
  }

  Version cloneVersion();

  void setRepushSourceVersion(int version);

  int getRepushSourceVersion();

  @JsonIgnore
  int getRmdVersionId();

  @JsonIgnore
  void setRmdVersionId(int replicationMetadataVersionId);

  /**
   * Kafka topic name is composed by store name and version.
   * <p>
   * The Json deserializer will think it should be a field called kafkaTopicName if we use "getKafkaTopicName" here. So
   * use "kafkaTopicName" directly here to avoid error when deserialize.
   *
   * @return kafka topic name.
   */
  @JsonIgnore
  String kafkaTopicName();

  static String parseStoreFromVersionTopic(String kafkaTopic) {
    return kafkaTopic.substring(0, getLastIndexOfVersionSeparator(kafkaTopic));
  }

  /**
   * This API works for both version topic and stream-reprocessing topics; other topic names will fail
   * with IllegalArgumentException.
   */
  static int parseVersionFromKafkaTopicName(String kafkaTopic) {
    int versionStartIndex = getLastIndexOfVersionSeparator(kafkaTopic) + VERSION_SEPARATOR.length();
    if (kafkaTopic.endsWith(STREAM_REPROCESSING_TOPIC_SUFFIX)) {
      return Integer
          .parseInt(kafkaTopic.substring(versionStartIndex, kafkaTopic.lastIndexOf(STREAM_REPROCESSING_TOPIC_SUFFIX)));
    } else if (VeniceView.isViewTopic(kafkaTopic)) {
      return VeniceView.parseVersionFromViewTopic(kafkaTopic);
    } else {
      return Integer.parseInt(kafkaTopic.substring(versionStartIndex));
    }
  }

  /**
   * This API only works for version topic; other topic names will fail with IllegalArgumentException.
   */
  static int parseVersionFromVersionTopicName(String kafkaTopic) {
    int versionStartIndex = getLastIndexOfVersionSeparator(kafkaTopic) + VERSION_SEPARATOR.length();
    return Integer.parseInt(kafkaTopic.substring(versionStartIndex));
  }

  static int getLastIndexOfVersionSeparator(String kafkaTopic) {
    int lastIndexOfVersionSeparator = kafkaTopic.lastIndexOf(VERSION_SEPARATOR);
    if (lastIndexOfVersionSeparator == -1) {
      throw new IllegalArgumentException(
          "The version separator '" + VERSION_SEPARATOR + "' is not present in the provided topic name: '" + kafkaTopic
              + "'");
    } else if (lastIndexOfVersionSeparator == 0) {
      /**
       * Empty string "" is not a valid store name, therefore the topic name cannot
       * start with the {@link VERSION_SEPARATOR}
       */
      throw new IllegalArgumentException(
          "There is nothing prior to the version separator '" + VERSION_SEPARATOR + "' in the provided topic name: '"
              + kafkaTopic + "'");
    }
    return lastIndexOfVersionSeparator;
  }

  static String composeKafkaTopic(String storeName, int versionNumber) {
    return storeName + VERSION_SEPARATOR + versionNumber;
  }

  static String composeRealTimeTopic(String storeName) {
    return storeName + REAL_TIME_TOPIC_SUFFIX;
  }

  static String composeSeparateRealTimeTopic(String storeName) {
    return storeName + SEPARATE_REAL_TIME_TOPIC_SUFFIX;
  }

  static String composeStreamReprocessingTopic(String storeName, int versionNumber) {
    return composeKafkaTopic(storeName, versionNumber) + STREAM_REPROCESSING_TOPIC_SUFFIX;
  }

  static String composeStreamReprocessingTopicFromVersionTopic(String versionTopic) {
    return versionTopic + STREAM_REPROCESSING_TOPIC_SUFFIX;
  }

  static String composeVersionTopicFromStreamReprocessingTopic(String kafkaTopic) {
    if (!isStreamReprocessingTopic(kafkaTopic)) {
      throw new VeniceException("Kafka topic: " + kafkaTopic + " is not a stream-reprocessing topic");
    }
    return kafkaTopic.substring(0, kafkaTopic.lastIndexOf(STREAM_REPROCESSING_TOPIC_SUFFIX));
  }

  static String parseStoreFromRealTimeTopic(String kafkaTopic) {
    if (!isRealTimeTopic(kafkaTopic)) {
      throw new VeniceException("Kafka topic: " + kafkaTopic + " is not a real-time topic");
    }
    if (kafkaTopic.endsWith(REAL_TIME_TOPIC_SUFFIX)) {
      return kafkaTopic.substring(0, kafkaTopic.length() - REAL_TIME_TOPIC_SUFFIX.length());
    }
    return kafkaTopic.substring(0, kafkaTopic.length() - SEPARATE_REAL_TIME_TOPIC_SUFFIX.length());
  }

  static String parseStoreFromStreamReprocessingTopic(String kafkaTopic) {
    if (!isStreamReprocessingTopic(kafkaTopic)) {
      throw new VeniceException("Kafka topic: " + kafkaTopic + " is not a stream reprocessing topic");
    }
    return kafkaTopic.substring(0, kafkaTopic.length() - STREAM_REPROCESSING_TOPIC_SUFFIX.length());
  }

  /**
   * Parse the store name of the given topic accordingly depending on the type of the kafka topic.
   * @param kafkaTopic to parse.
   * @return the store name or an empty string if the topic format doesn't match any of the known Venice topic formats.
   */
  static String parseStoreFromKafkaTopicName(String kafkaTopic) {
    if (isRealTimeTopic(kafkaTopic)) {
      return parseStoreFromRealTimeTopic(kafkaTopic);
    } else if (isStreamReprocessingTopic(kafkaTopic)) {
      return parseStoreFromStreamReprocessingTopic(kafkaTopic);
    } else if (isVersionTopic(kafkaTopic)) {
      return parseStoreFromVersionTopic(kafkaTopic);
    } else if (VeniceView.isViewTopic(kafkaTopic)) {
      return VeniceView.parseStoreFromViewTopic(kafkaTopic);
    }
    return "";
  }

  static boolean isRealTimeTopic(String kafkaTopic) {
    return kafkaTopic.endsWith(REAL_TIME_TOPIC_SUFFIX) || kafkaTopic.endsWith(SEPARATE_REAL_TIME_TOPIC_SUFFIX);
  }

  static boolean isStreamReprocessingTopic(String kafkaTopic) {
    return checkVersionSRTopic(kafkaTopic, true);
  }

  /**
   * Return true if the input topic name is a version topic or stream-reprocessing topic.
   */
  static boolean isVersionTopicOrStreamReprocessingTopic(String kafkaTopic) {
    return checkVersionSRTopic(kafkaTopic, false) || checkVersionSRTopic(kafkaTopic, true);
  }

  /**
   * Determines if the the inputted topic is a topic which is versioned. Today that includes reprocessing topics, version
   * topics, and view topics. This method is named this way in order to avoid confusion with the isVersionTopic (where the
   * alternative would be isVersionedTopic).
   *
   * @param kafkaTopic
   * @return
   */
  static boolean isATopicThatIsVersioned(String kafkaTopic) {
    return checkVersionSRTopic(kafkaTopic, false) || checkVersionSRTopic(kafkaTopic, true)
        || VeniceView.isViewTopic(kafkaTopic);
  }

  static boolean checkVersionSRTopic(String kafkaTopic, boolean checkSR) {
    int lastIndexOfVersionSeparator = kafkaTopic.lastIndexOf(VERSION_SEPARATOR);
    if (checkSR && !kafkaTopic.endsWith(STREAM_REPROCESSING_TOPIC_SUFFIX)) {
      return false;
    }

    int start = lastIndexOfVersionSeparator + VERSION_SEPARATOR.length();
    int end = kafkaTopic.length() - (checkSR ? STREAM_REPROCESSING_TOPIC_SUFFIX.length() : 0);
    if (end == start) { // no version info
      return false;
    }
    for (int i = start; i < end; i++) {
      if (!isDigit(kafkaTopic.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Only return true if the input topic name is a version topic.
   */
  static boolean isVersionTopic(String kafkaTopic) {
    return checkVersionSRTopic(kafkaTopic, false);
  }

  static String guidBasedDummyPushId() {
    return "guid_id_" + GuidUtils.getGUIDString();
  }

  static String numberBasedDummyPushId(int number) {
    return "push_for_version_" + number;
  }

  static String generateRePushId(String pushId) {
    return VENICE_RE_PUSH_PUSH_ID_PREFIX + pushId;
  }

  static boolean isPushIdRePush(String pushId) {
    if (pushId == null || pushId.isEmpty()) {
      return false;
    }
    return pushId.startsWith(VENICE_RE_PUSH_PUSH_ID_PREFIX);
  }

  static boolean containsHybridVersion(List<Version> versions) {
    for (Version version: versions) {
      if (version.getHybridStoreConfig() != null) {
        return true;
      }
    }
    return false;
  }
}
