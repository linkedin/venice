package com.linkedin.venice.meta;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.systemstore.schemas.StoreVersion;
import com.linkedin.venice.utils.AvroCompatibilityUtils;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import org.codehaus.jackson.map.annotate.JsonDeserialize;

/**
 * This interface defines all the public APIs, and if you need to add accessors to
 * some new fields, this interface needs to be changed accordingly.
 */
@JsonDeserialize(as = VersionImpl.class)
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(as = VersionImpl.class)
public interface Version extends Comparable<Version>, DataModelBackedStructure<StoreVersion> {
  String VERSION_SEPARATOR = "_v";
  String REAL_TIME_TOPIC_SUFFIX = "_rt";
  String STREAM_REPROCESSING_TOPIC_SUFFIX = "_sr";

  /**
   * Producer type for writing data to Venice
   */
  enum PushType {
    BATCH(0), //Batch jobs will create a new version topic and write to it in a batch manner.
    STREAM_REPROCESSING(1), // Grandfathering jobs will create a new version topic and a grandfathering topic.
    STREAM(2), // Stream jobs will write to a buffer or RT topic.
    INCREMENTAL(3); // Incremental jobs will re-use an existing version topic and write on top of it.

    private final int value;

    PushType(int value) { this.value = value; }

    public int getValue() { return value; }

    public boolean isIncremental() {
      return this == INCREMENTAL;
    }

    public boolean isStreamReprocessing() {
      return this == STREAM_REPROCESSING;
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

  long getCreatedTime();

  @JsonIgnore
  Duration getAge();

  @JsonIgnore
  void setAge(Duration age);

  VersionStatus getStatus();

  void setStatus(VersionStatus status);

  CompressionStrategy getCompressionStrategy();

  void setCompressionStrategy(CompressionStrategy compressionStrategy);

  boolean isLeaderFollowerModelEnabled();

  boolean isNativeReplicationEnabled();

  void setLeaderFollowerModelEnabled(boolean leaderFollowerModelEnabled);

  void setNativeReplicationEnabled(boolean nativeReplicationEnabled);

  String getPushStreamSourceAddress();

  void setPushStreamSourceAddress(String address);

  boolean isBufferReplayEnabledForHybrid();

  void setBufferReplayEnabledForHybrid(boolean bufferReplayEnabledForHybrid);

  boolean isChunkingEnabled();

  void setChunkingEnabled(boolean chunkingEnabled);

  String getStoreName();

  String getPushJobId();

  PushType getPushType();

  void setPushType(PushType pushType);

  void setPartitionCount(int partitionCount);

  int getPartitionCount();

  PartitionerConfig getPartitionerConfig();

  void setPartitionerConfig(PartitionerConfig partitionerConfig);

  IncrementalPushPolicy getIncrementalPushPolicy();

  void setIncrementalPushPolicy(IncrementalPushPolicy incrementalPushPolicy);

  int getReplicationFactor();

  void setReplicationFactor(int replicationFactor);

  int getMinActiveReplicas();

  String getNativeReplicationSourceFabric();

  void setNativeReplicationSourceFabric(String nativeReplicationSourceFabric);

  boolean isIncrementalPushEnabled();

  void setIncrementalPushEnabled(boolean incrementalPushEnabled);

  boolean isUseVersionLevelIncrementalPushEnabled();

  void setUseVersionLevelIncrementalPushEnabled(boolean versionLevelIncrementalPushEnabled);

  HybridStoreConfig getHybridStoreConfig();

  void setHybridStoreConfig(HybridStoreConfig hybridConfig);

  boolean isUseVersionLevelHybridConfig();

  void setUseVersionLevelHybridConfig(boolean versionLevelHybridConfig);


  Version cloneVersion();

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

  static String parseStoreFromKafkaTopicName(String kafkaTopic) {
    return kafkaTopic.substring(0, getLastIndexOfVersionSeparator(kafkaTopic));
  }

  /**
   * This API works for both version topic and stream-reprocessing topics; other topic names will fail
   * with IllegalArgumentException.
   */
  static int parseVersionFromKafkaTopicName(String kafkaTopic) {
    int versionStartIndex = getLastIndexOfVersionSeparator(kafkaTopic) + VERSION_SEPARATOR.length();
    if (kafkaTopic.endsWith(STREAM_REPROCESSING_TOPIC_SUFFIX)) {
      return Integer.parseInt(kafkaTopic.substring(versionStartIndex, kafkaTopic.lastIndexOf(
          STREAM_REPROCESSING_TOPIC_SUFFIX)));
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
      throw new IllegalArgumentException("The version separator '" + VERSION_SEPARATOR
          + "' is not present in the provided topic name: '" + kafkaTopic + "'");
    } else if (lastIndexOfVersionSeparator == 0) {
      /**
       * Empty string "" is not a valid store name, therefore the topic name cannot
       * start with the {@link VERSION_SEPARATOR}
       */
      throw new IllegalArgumentException("There is nothing prior to the version separator '"
          + VERSION_SEPARATOR + "' in the provided topic name: '" + kafkaTopic + "'");
    }
    return lastIndexOfVersionSeparator;
  }

  static String composeKafkaTopic(String storeName,int versionNumber){
    return storeName + VERSION_SEPARATOR + versionNumber;
  }

  static String composeRealTimeTopic(String storeName){
    return storeName + REAL_TIME_TOPIC_SUFFIX;
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
    return kafkaTopic.substring(0, kafkaTopic.length() - REAL_TIME_TOPIC_SUFFIX.length());
  }

  static boolean isRealTimeTopic(String kafkaTopic) {
    return kafkaTopic.endsWith(REAL_TIME_TOPIC_SUFFIX);
  }

  static boolean isStreamReprocessingTopic(String kafkaTopic) {
    return kafkaTopic.endsWith(STREAM_REPROCESSING_TOPIC_SUFFIX);
  }

  /**
   * Return true if the input topic name is a version topic or stream-reprocessing topic.
   */
  static boolean isVersionTopicOrStreamReprocessingTopic(String kafkaTopic){
    try{
      parseVersionFromKafkaTopicName(kafkaTopic);
    } catch (IllegalArgumentException e){
      return false;
    }
    return true;
  }

  /**
   * Only return true if the input topic name is a version topic.
   */
  static boolean isVersionTopic(String kafkaTopic) {
    try {
      parseVersionFromVersionTopicName(kafkaTopic);
    } catch (IllegalArgumentException e) {
      return false;
    }
    return true;
  }

  static String guidBasedDummyPushId(){
    return "guid_id_" + GuidUtils.getGUIDString();
  }

  static String numberBasedDummyPushId(int number){
    return "push_for_version_" + number;
  }
}
