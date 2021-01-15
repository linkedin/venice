package com.linkedin.venice.meta;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.systemstore.schemas.StoreVersion;
import com.linkedin.venice.utils.AvroCompatibilityUtils;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Class defines the version of Venice store.
 */
@com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
@org.codehaus.jackson.annotate.JsonIgnoreProperties(ignoreUnknown = true)
public class Version implements Comparable<Version>, DataModelBackedStructure<StoreVersion> {
  private static final String VERSION_SEPARATOR = "_v";
  private static final String REAL_TIME_TOPIC_SUFFIX = "_rt";
  private static final String STREAM_REPROCESSING_TOPIC_SUFFIX = "_sr";

  /**
   * Producer type for writing data to Venice
   */
  public enum PushType {
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

  @JsonIgnore
  private final String kafkaTopicName;

  // The internal data model
  private final StoreVersion storeVersion;

  /**
   * Use the constructor that specifies a pushJobId instead
   */
  @Deprecated
  public Version(String storeName, int number) {
    this(storeName , number, System.currentTimeMillis(), numberBasedDummyPushId(number), 0, new PartitionerConfig());
  }

  public Version(String storeName, int number, String pushJobId) {
    this(storeName, number, System.currentTimeMillis(), pushJobId, 0, new PartitionerConfig());
  }

  public Version(String storeName, int number, String pushJobId, int partitionCount) {
    this(storeName, number, System.currentTimeMillis(), pushJobId, partitionCount, new PartitionerConfig());
  }

  public Version(
      @JsonProperty("storeName") @com.fasterxml.jackson.annotation.JsonProperty("storeName") String storeName,
      @JsonProperty("number") @com.fasterxml.jackson.annotation.JsonProperty("number") int number,
      @JsonProperty("createdTime")  @com.fasterxml.jackson.annotation.JsonProperty("createdTime") long createdTime,
      @JsonProperty("pushJobId") @com.fasterxml.jackson.annotation.JsonProperty("pushJobId") String pushJobId,
      @JsonProperty("partitionCount") @com.fasterxml.jackson.annotation.JsonProperty("partitionCount") int partitionCount,
      @JsonProperty("partitionerConfig") @com.fasterxml.jackson.annotation.JsonProperty("partitionerConfig") PartitionerConfig partitionerConfig) {
    this.storeVersion = Store.prefillAvroRecordWithDefaultValue(new StoreVersion());
    this.storeVersion.storeName = storeName;
    this.storeVersion.number = number;
    this.storeVersion.createdTime = createdTime;
    this.storeVersion.pushJobId = pushJobId == null ? numberBasedDummyPushId(number) : pushJobId; // for deserializing old Versions that didn't get an pushJobId
    this.storeVersion.partitionCount = partitionCount;
    if (partitionerConfig != null) {
      this.storeVersion.partitionerConfig = partitionerConfig.dataModel();
    }

    this.kafkaTopicName = composeKafkaTopic(storeName, number);
  }

  Version(StoreVersion storeVersion) {
    this.storeVersion = storeVersion;
    this.kafkaTopicName = composeKafkaTopic(getStoreName(), getNumber());
  }

  public int getNumber() {
    return this.storeVersion.number;
  }

  public long getCreatedTime() {
    return this.storeVersion.createdTime;
  }

  public VersionStatus getStatus() {
    return VersionStatus.getVersionStatusFromInt(this.storeVersion.status);
  }

  public void setStatus(VersionStatus status) {
    this.storeVersion.status = status.ordinal();
  }

  public CompressionStrategy getCompressionStrategy() {
    return CompressionStrategy.valueOf(this.storeVersion.compressionStrategy);
  }

  public void setCompressionStrategy(CompressionStrategy compressionStrategy) {
    this.storeVersion.compressionStrategy = compressionStrategy.getValue();
  }

  public boolean isLeaderFollowerModelEnabled() {
    return this.storeVersion.leaderFollowerModelEnabled;
  }

  public boolean isNativeReplicationEnabled() {
    return this.storeVersion.nativeReplicationEnabled;
  }

  public void setLeaderFollowerModelEnabled(boolean leaderFollowerModelEnabled) {
    this.storeVersion.leaderFollowerModelEnabled = leaderFollowerModelEnabled;
  }

  public void setNativeReplicationEnabled(boolean nativeReplicationEnabled) {
    this.storeVersion.nativeReplicationEnabled = nativeReplicationEnabled;
  }

  public String getPushStreamSourceAddress() {
    return this.storeVersion.pushStreamSourceAddress.toString();
  }

  public void setPushStreamSourceAddress(String address) {
    this.storeVersion.pushStreamSourceAddress = address;
  }

  public boolean isBufferReplayEnabledForHybrid() {
    return this.storeVersion.bufferReplayEnabledForHybrid;
  }

  public void setBufferReplayEnabledForHybrid(boolean bufferReplayEnabledForHybrid) {
    this.storeVersion.bufferReplayEnabledForHybrid = bufferReplayEnabledForHybrid;
  }

  public boolean isChunkingEnabled() {
    return this.storeVersion.chunkingEnabled;
  }

  public void setChunkingEnabled(boolean chunkingEnabled) {
    this.storeVersion.chunkingEnabled = chunkingEnabled;
  }

  public String getStoreName() {
    return this.storeVersion.storeName.toString();
  }

  public String getPushJobId() {
    return this.storeVersion.pushJobId.toString();
  }

  public PushType getPushType() {
    return PushType.valueOf(this.storeVersion.pushType);
  }

  public void setPushType(PushType pushType) {
    this.storeVersion.pushType = pushType.getValue();
  }

  public void setPartitionCount(int partitionCount) {
    this.storeVersion.partitionCount = partitionCount;
  }

  public int getPartitionCount() {
    return this.storeVersion.partitionCount;
  }

  public PartitionerConfig getPartitionerConfig() {
    if (null == this.storeVersion.partitionerConfig) {
      return null;
    }
    return new PartitionerConfig(this.storeVersion.partitionerConfig);
  }

  public void setPartitionerConfig(PartitionerConfig partitionerConfig) {
    if (partitionerConfig != null) {
      this.storeVersion.partitionerConfig = partitionerConfig.dataModel();
    }
  }

  public IncrementalPushPolicy getIncrementalPushPolicy() {
    return IncrementalPushPolicy.valueOf(this.storeVersion.incrementalPushPolicy);
  }

  public void setIncrementalPushPolicy(IncrementalPushPolicy incrementalPushPolicy) {
    this.storeVersion.incrementalPushPolicy = incrementalPushPolicy.getValue();
  }

  public int getReplicationFactor() {
    return this.storeVersion.replicationFactor;
  }

  public void setReplicationFactor(int replicationFactor) {
    this.storeVersion.replicationFactor = replicationFactor;
  }

  public int getMinActiveReplicas() {
    return this.storeVersion.replicationFactor - 1;
  }

  public String getNativeReplicationSourceFabric() {
    return this.storeVersion.nativeReplicationSourceFabric.toString();
  }

  public void setNativeReplicationSourceFabric(String nativeReplicationSourceFabric) {
    this.storeVersion.nativeReplicationSourceFabric = nativeReplicationSourceFabric;
  }

  public boolean isIncrementalPushEnabled() {
    return this.storeVersion.incrementalPushEnabled;
  }

  public void setIncrementalPushEnabled(boolean incrementalPushEnabled) {
    this.storeVersion.incrementalPushEnabled = incrementalPushEnabled;
  }

  public boolean isUseVersionLevelIncrementalPushEnabled() {
    return this.storeVersion.useVersionLevelIncrementalPushEnabled;
  }

  public void setUseVersionLevelIncrementalPushEnabled(boolean versionLevelIncrementalPushEnabled) {
    this.storeVersion.useVersionLevelIncrementalPushEnabled = versionLevelIncrementalPushEnabled;
  }

  @Override
  public StoreVersion dataModel() {
    return this.storeVersion;
  }


  @Override
  public String toString() {
    return "Version{" +
        "storeName='" + getStoreName() + '\'' +
        ", number=" + getNumber() +
        ", createdTime=" + getCreatedTime() +
        ", status=" + getStatus() +
        ", pushJobId='" + getPushJobId() + '\'' +
        ", compressionStrategy='" + getCompressionStrategy() + '\'' +
        ", leaderFollowerModelEnabled=" + isLeaderFollowerModelEnabled() +
        ", bufferReplayEnabledForHybrid=" + isBufferReplayEnabledForHybrid() +
        ", pushType=" + getPushType() +
        ", partitionCount=" + getPartitionCount() +
        ", partitionerConfig=" + getPartitionerConfig() +
        ", nativeReplicationEnabled=" + isNativeReplicationEnabled() +
        ", pushStreamSourceAddress=" + getPushStreamSourceAddress() +
        ", replicationFactor=" + getReplicationFactor() +
        ", nativeReplicationSourceFabric=" + getNativeReplicationSourceFabric() +
        ", incrementalPushEnabled=" + isIncrementalPushEnabled() +
        ", useVersionLevelIncrementalPushEnabled=" + isUseVersionLevelIncrementalPushEnabled() +
        '}';
  }

  @Override
  public int compareTo(Version o) {
    if(o == null) {
      throw new IllegalArgumentException("Input argument is null");
    }

    Integer num = this.storeVersion.number;
    return num.compareTo(o.storeVersion.number);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Version version = (Version) o;
    return AvroCompatibilityUtils.compare(storeVersion, version.storeVersion);
  }

  @Override
  public int hashCode() {
    return Objects.hash(storeVersion);
  }

  /**
   * Clone a new version based on current data in this version.
   *
   * @return cloned version.
   */
  @JsonIgnore
  public Version cloneVersion() {
    Version clonedVersion = new Version(getStoreName(), getNumber(), getCreatedTime(), getPushJobId(), getPartitionCount(), getPartitionerConfig());
    clonedVersion.setStatus(getStatus());
    clonedVersion.setCompressionStrategy(getCompressionStrategy());
    clonedVersion.setLeaderFollowerModelEnabled(isLeaderFollowerModelEnabled());
    clonedVersion.setBufferReplayEnabledForHybrid(isBufferReplayEnabledForHybrid());
    clonedVersion.setChunkingEnabled(isChunkingEnabled());
    clonedVersion.setPushType(getPushType());
    clonedVersion.setNativeReplicationEnabled(isNativeReplicationEnabled());
    clonedVersion.setPushStreamSourceAddress(getPushStreamSourceAddress());
    clonedVersion.setIncrementalPushPolicy(getIncrementalPushPolicy());
    clonedVersion.setReplicationFactor(getReplicationFactor());
    clonedVersion.setNativeReplicationSourceFabric(getNativeReplicationSourceFabric());
    clonedVersion.setIncrementalPushEnabled(isIncrementalPushEnabled());
    clonedVersion.setUseVersionLevelIncrementalPushEnabled(isUseVersionLevelIncrementalPushEnabled());
    return clonedVersion;
  }

  /**
   * Kafka topic name is composed by store name and version.
   * <p>
   * The Json deserializer will think it should be a field called kafkaTopicName if we use "getKafkaTopicName" here. So
   * use "kafkaTopicName" directly here to avoid error when deserialize.
   *
   * @return kafka topic name.
   */
  @JsonIgnore
  public String kafkaTopicName() {
    return kafkaTopicName;
  }

  public static String parseStoreFromKafkaTopicName(String kafkaTopic) {
    return kafkaTopic.substring(0, getLastIndexOfVersionSeparator(kafkaTopic));
  }

  /**
   * This API works for both version topic and stream-reprocessing topics; other topic names will fail
   * with IllegalArgumentException.
   */
  public static int parseVersionFromKafkaTopicName(String kafkaTopic) {
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
  public static int parseVersionFromVersionTopicName(String kafkaTopic) {
    int versionStartIndex = getLastIndexOfVersionSeparator(kafkaTopic) + VERSION_SEPARATOR.length();
    return Integer.parseInt(kafkaTopic.substring(versionStartIndex));
  }

  private static int getLastIndexOfVersionSeparator(String kafkaTopic) {
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

  public static String composeKafkaTopic(String storeName,int versionNumber){
    return storeName + VERSION_SEPARATOR + versionNumber;
  }

  public static String composeRealTimeTopic(String storeName){
    return storeName + REAL_TIME_TOPIC_SUFFIX;
  }

  public static String composeStreamReprocessingTopic(String storeName, int versionNumber) {
    return composeKafkaTopic(storeName, versionNumber) + STREAM_REPROCESSING_TOPIC_SUFFIX;
  }

  public static String composeStreamReprocessingTopicFromVersionTopic(String versionTopic) {
    return versionTopic + STREAM_REPROCESSING_TOPIC_SUFFIX;
  }

  public static String composeVersionTopicFromStreamReprocessingTopic(String kafkaTopic) {
    if (!isStreamReprocessingTopic(kafkaTopic)) {
      throw new VeniceException("Kafka topic: " + kafkaTopic + " is not a stream-reprocessing topic");
    }
    return kafkaTopic.substring(0, kafkaTopic.lastIndexOf(STREAM_REPROCESSING_TOPIC_SUFFIX));
  }

  public static String parseStoreFromRealTimeTopic(String kafkaTopic) {
    if (!isRealTimeTopic(kafkaTopic)) {
      throw new VeniceException("Kafka topic: " + kafkaTopic + " is not a real-time topic");
    }
    return kafkaTopic.substring(0, kafkaTopic.length() - REAL_TIME_TOPIC_SUFFIX.length());
  }

  public static boolean isRealTimeTopic(String kafkaTopic) {
    return kafkaTopic.endsWith(REAL_TIME_TOPIC_SUFFIX);
  }

  public static boolean isStreamReprocessingTopic(String kafkaTopic) {
    return kafkaTopic.endsWith(STREAM_REPROCESSING_TOPIC_SUFFIX);
  }

  /**
   * Return true if the input topic name is a version topic or stream-reprocessing topic.
   */
  public static boolean isVersionTopicOrStreamReprocessingTopic(String kafkaTopic){
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
  public static boolean isVersionTopic(String kafkaTopic) {
    try {
      parseVersionFromVersionTopicName(kafkaTopic);
    } catch (IllegalArgumentException e) {
      return false;
    }
    return true;
  }

  public static String guidBasedDummyPushId(){
    return "guid_id_" + GuidUtils.getGUIDString();
  }

  static String numberBasedDummyPushId(int number){
    return "push_for_version_" + number;
  }
}
