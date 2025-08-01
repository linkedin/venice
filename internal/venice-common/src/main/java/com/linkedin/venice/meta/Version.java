package com.linkedin.venice.meta;

import static com.linkedin.venice.utils.Utils.SEPARATE_TOPIC_SUFFIX;
import static java.lang.Character.isDigit;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.systemstore.schemas.StoreVersion;
import com.linkedin.venice.views.VeniceView;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * This interface defines all the public APIs, and if you need to add accessors to
 * some new fields, this interface needs to be changed accordingly.
 */
@JsonDeserialize(as = VersionImpl.class)
public interface Version extends Comparable<Version>, DataModelBackedStructure<StoreVersion> {
  String VERSION_SEPARATOR = "_v";
  String REAL_TIME_TOPIC_SUFFIX = "_rt";
  String REAL_TIME_TOPIC_TEMPLATE = "%s_rt_v%d";
  String STREAM_REPROCESSING_TOPIC_SUFFIX = "_sr";
  /**
   * Special number indicating no replication metadata version is set.
   */
  int REPLICATION_METADATA_VERSION_ID_UNSET = -1;

  /**
   * Prefix used in push id to indicate the version's data source is coming from an existing version topic.
   */
  String VENICE_RE_PUSH_PUSH_ID_PREFIX = "venice_re_push_";

  String VENICE_TTL_RE_PUSH_PUSH_ID_PREFIX = "venice_ttl_re_push_";

  /**
   * Prefix used in push id to indicate a regular batch push is made to a store with TTL re-push enabled. This disables
   * the TTL re-push enabled flag for the corresponding store.
   */
  String VENICE_REGULAR_PUSH_WITH_TTL_RE_PUSH_PREFIX = "venice_regular_push_with_ttl_re_push_";
  int DEFAULT_RT_VERSION_NUMBER = 0;

  /**
   * Producer type for writing data to Venice
   */
  enum PushType {
    BATCH(0), // Batch jobs will create a new version topic and write to it in a batch manner.
    STREAM_REPROCESSING(1), // Reprocessing jobs will create a new version topic and a reprocessing topic.
    STREAM(2), // Stream jobs will write to a buffer or RT topic.
    INCREMENTAL(3); // Incremental jobs will re-use an existing version topic and write on top of it.

    private final int value;
    private static final Map<Integer, PushType> VALUE_TO_TYPE_MAP = new HashMap<>(4);
    private static final Map<String, PushType> NAME_TO_TYPE_MAP = new HashMap<>(4);

    // Static initializer for map population
    static {
      for (PushType type: PushType.values()) {
        VALUE_TO_TYPE_MAP.put(type.value, type);
        NAME_TO_TYPE_MAP.put(type.name(), type);
      }
    }

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
      return this == BATCH || this == STREAM_REPROCESSING;
    }

    /**
     * Retrieve the PushType based on its integer value.
     *
     * @param value the integer value of the PushType
     * @return the corresponding PushType
     * @throws VeniceException if the value is invalid
     */
    public static PushType valueOf(int value) {
      PushType pushType = VALUE_TO_TYPE_MAP.get(value);
      if (pushType == null) {
        throw new VeniceException("Invalid push type with int value: " + value);
      }
      return pushType;
    }

    /**
     * Extracts the PushType from its string name.
     *
     * @param pushTypeString the string representation of the PushType
     * @return the corresponding PushType
     * @throws IllegalArgumentException if the string is invalid
     */
    public static PushType extractPushType(String pushTypeString) {
      PushType pushType = NAME_TO_TYPE_MAP.get(pushTypeString);
      if (pushType == null) {
        throw new IllegalArgumentException(
            String.format(
                "%s is an invalid push type. Valid push types are: %s",
                pushTypeString,
                String.join(", ", NAME_TO_TYPE_MAP.keySet())));
      }
      return pushType;
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

  boolean isHybrid();

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

  void setTargetSwapRegion(String targetRegion);

  String getTargetSwapRegion();

  void setTargetSwapRegionWaitTime(int waitTime);

  int getTargetSwapRegionWaitTime();

  void setIsDavinciHeartbeatReported(boolean isReported);

  boolean getIsDavinciHeartbeatReported();

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

  boolean isGlobalRtDivEnabled();

  void setGlobalRtDivEnabled(boolean globalRtDivEnabled);

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

  static String removeRTVersionSuffix(String kafkaTopic) {
    int lastIndexOfVersionSeparator = kafkaTopic.lastIndexOf(VERSION_SEPARATOR);

    if (lastIndexOfVersionSeparator == 0) {
      throw new VeniceException(
          "There is nothing prior to the version separator '" + VERSION_SEPARATOR + "' in the provided topic name: '"
              + kafkaTopic + "'");
    } else if (lastIndexOfVersionSeparator == -1) {
      return kafkaTopic;
    }

    int start = lastIndexOfVersionSeparator + VERSION_SEPARATOR.length();
    int end = kafkaTopic.length();

    for (int i = start; i < end; i++) {
      if (!isDigit(kafkaTopic.charAt(i))) {
        return kafkaTopic;
      }
    }
    return kafkaTopic.substring(0, lastIndexOfVersionSeparator);
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

  static int parseVersionFromVersionTopicPartition(String kafkaTopic) {
    int versionStartIndex = getLastIndexOfVersionSeparator(kafkaTopic) + VERSION_SEPARATOR.length();

    // Remove partition number if present
    // e.g. store_89c1b5c06764_75ba3e03_v1-0
    String versionString = kafkaTopic.substring(versionStartIndex);
    versionString =
        versionString.contains("-") ? versionString.substring(0, versionString.indexOf('-')) : versionString;

    try {
      return Integer.parseInt(versionString);
    } catch (NumberFormatException e) {
      return 1;
    }
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

    boolean isSeparateRT = kafkaTopic.endsWith(SEPARATE_TOPIC_SUFFIX);
    if (isSeparateRT) {
      kafkaTopic = kafkaTopic.substring(0, kafkaTopic.length() - SEPARATE_TOPIC_SUFFIX.length());
    }
    String topicWithoutRTVersionSuffix = removeRTVersionSuffix(kafkaTopic);
    return topicWithoutRTVersionSuffix
        .substring(0, topicWithoutRTVersionSuffix.length() - REAL_TIME_TOPIC_SUFFIX.length());
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

  static boolean isRealTimeTopic(String topicName) {
    // valid rt topics are - abc_rt, abc_rt_v1, abc_rt_sep, abc_rt_v1_sep
    if (topicName.endsWith(SEPARATE_TOPIC_SUFFIX)) {
      topicName = topicName.substring(0, topicName.length() - SEPARATE_TOPIC_SUFFIX.length());
    }
    String topicWithoutRTVersionSuffix = removeRTVersionSuffix(topicName);
    return topicWithoutRTVersionSuffix.endsWith(REAL_TIME_TOPIC_SUFFIX);
  }

  static boolean isIncrementalPushTopic(String topicName) {
    String topicWithoutVersionSuffix = removeRTVersionSuffix(topicName);
    return topicWithoutVersionSuffix.endsWith(SEPARATE_TOPIC_SUFFIX);
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
    if (lastIndexOfVersionSeparator != -1
        && kafkaTopic.substring(0, lastIndexOfVersionSeparator).endsWith(REAL_TIME_TOPIC_SUFFIX)) {
      return false;
    }
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

  static String generateTTLRePushId(String pushId) {
    return VENICE_TTL_RE_PUSH_PUSH_ID_PREFIX + pushId;
  }

  static String generateRegularPushWithTTLRePushId(String pushId) {
    return VENICE_REGULAR_PUSH_WITH_TTL_RE_PUSH_PREFIX + pushId;
  }

  static boolean isPushIdTTLRePush(String pushId) {
    if (pushId == null || pushId.isEmpty()) {
      return false;
    }
    return pushId.startsWith(VENICE_TTL_RE_PUSH_PUSH_ID_PREFIX);
  }

  static boolean isPushIdRePush(String pushId) {
    if (pushId == null || pushId.isEmpty()) {
      return false;
    }
    return pushId.startsWith(VENICE_RE_PUSH_PUSH_ID_PREFIX);
  }

  static boolean isPushIdRegularPushWithTTLRePush(String pushId) {
    if (pushId == null || pushId.isEmpty()) {
      return false;
    }
    return pushId.startsWith(VENICE_REGULAR_PUSH_WITH_TTL_RE_PUSH_PREFIX);
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
