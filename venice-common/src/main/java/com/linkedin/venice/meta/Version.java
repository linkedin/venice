package com.linkedin.venice.meta;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.guid.GuidUtils;
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
public class Version implements Comparable<Version> {
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

  /**
   * Name of the store which this version belong to.
   */
  private final String storeName;
  /**
   * Version number.
   */
  private final int number;

  @JsonIgnore
  private final String kafkaTopicName;
  /**
   * Time when this version was created.
   */
  private final long createdTime;
  /**
   * Status of version.
   */
  private VersionStatus status = VersionStatus.STARTED;

  private final String pushJobId;
  /**
   * strategies used to compress/decompress Record's value
   */
  private CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;

  /**
   * Whether or not to use leader follower state transition model
   * for upcoming version.
   */
  private boolean leaderFollowerModelEnabled = false;

  /**
   * Whether or not native replication is enabled
   */
  private boolean nativeReplicationEnabled = false;

  /**
   * Address to the kafka broker which holds the source of truth topic for this store version.
   */
  private String pushStreamSourceAddress = "";

  /**
   * Whether or not to enable buffer replay for hybrid.
   */
  private boolean bufferReplayEnabledForHybrid = true;

  /**
   * Whether or not large values are supported (via chunking).
   */
  private boolean chunkingEnabled = false;

  /**
   * Producer type for this version.
   */
  private PushType pushType = PushType.BATCH;

  /**
   * Default partition count of this version.
   * assigned.
   */
  private int partitionCount = 0;

  /**
   * Config for custom partitioning.
   */
  private PartitionerConfig partitionerConfig;

  /**
   * Incremental Push Policy to reconcile with real time pushes.
   */
  private IncrementalPushPolicy incrementalPushPolicy = IncrementalPushPolicy.PUSH_TO_VERSION_TOPIC;

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
    this.storeName = storeName;
    this.number = number;
    this.createdTime = createdTime;
    this.pushJobId = pushJobId == null ? numberBasedDummyPushId(number) : pushJobId; // for deserializing old Versions that didn't get an pushJobId
    this.partitionCount = partitionCount;
    this.partitionerConfig = partitionerConfig;
    this.kafkaTopicName = composeKafkaTopic(storeName, number);
  }

  public int getNumber() {
    return number;
  }

  public long getCreatedTime() {
    return createdTime;
  }

  public VersionStatus getStatus() {
    return status;
  }

  public void setStatus(VersionStatus status) {
    this.status = status;
  }

  public CompressionStrategy getCompressionStrategy() {
    return compressionStrategy;
  }

  public void setCompressionStrategy(CompressionStrategy compressionStrategy) {
    this.compressionStrategy = compressionStrategy;
  }

  public boolean isLeaderFollowerModelEnabled() {
    return leaderFollowerModelEnabled;
  }

  public boolean isNativeReplicationEnabled() {
    return nativeReplicationEnabled;
  }

  public void setLeaderFollowerModelEnabled(boolean leaderFollowerModelEnabled) {
    this.leaderFollowerModelEnabled = leaderFollowerModelEnabled;
  }

  public void setNativeReplicationEnabled(boolean nativeReplicationEnabled) {
    this.nativeReplicationEnabled = nativeReplicationEnabled;
  }

  public String getPushStreamSourceAddress() {
    return this.pushStreamSourceAddress;
  }

  public void setPushStreamSourceAddress(String address) {
    pushStreamSourceAddress = address;
  }

  public boolean isBufferReplayEnabledForHybrid() {
    return bufferReplayEnabledForHybrid;
  }

  public void setBufferReplayEnabledForHybrid(boolean bufferReplayEnabledForHybrid) {
    this.bufferReplayEnabledForHybrid = bufferReplayEnabledForHybrid;
  }

  public boolean isChunkingEnabled() {
    return chunkingEnabled;
  }

  public void setChunkingEnabled(boolean chunkingEnabled) {
    this.chunkingEnabled = chunkingEnabled;
  }

  public String getStoreName() {
    return storeName;
  }

  public String getPushJobId() {
    return pushJobId;
  }

  public PushType getPushType() {
    return pushType;
  }

  public void setPushType(PushType pushType) {
    this.pushType = pushType;
  }

  public void setPartitionCount(int partitionCount) {
    this.partitionCount = partitionCount;
  }

  public int getPartitionCount() {
    return partitionCount;
  }

  public PartitionerConfig getPartitionerConfig() {
    return partitionerConfig;
  }

  public void setPartitionerConfig(PartitionerConfig partitionerConfig) {
    this.partitionerConfig = partitionerConfig;
  }

  public IncrementalPushPolicy getIncrementalPushPolicy() {
    return incrementalPushPolicy;
  }

  public void setIncrementalPushPolicy(IncrementalPushPolicy incrementalPushPolicy) {
    this.incrementalPushPolicy = incrementalPushPolicy;
  }

  @Override
  public String toString() {
    return "Version{" +
        "storeName='" + storeName + '\'' +
        ", number=" + number +
        ", createdTime=" + createdTime +
        ", status=" + status +
        ", pushJobId='" + pushJobId + '\'' +
        ", compressionStrategy='" + compressionStrategy + '\'' +
        ", leaderFollowerModelEnabled=" + leaderFollowerModelEnabled +
        ", bufferReplayEnabledForHybrid=" + bufferReplayEnabledForHybrid +
        ", pushType=" + pushType +
        ", partitionCount=" + partitionCount +
        ", partitionerConfig=" + partitionerConfig +
        ", nativeReplicationEnabled=" + nativeReplicationEnabled +
        ", pushStreamSourceAddress=" + pushStreamSourceAddress +
        '}';
  }

  @Override
  public int compareTo(Version o) {
    if(o == null) {
      throw new IllegalArgumentException("Input argument is null");
    }

    Integer num = this.number;
    return num.compareTo(o.number);
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

    if (number != version.number) {
      return false;
    }

    if (createdTime != version.createdTime) {
      return false;
    }

    // NPE proof comparison
    if (!Objects.equals(storeName, version.storeName)) {
      return false;
    }

    if (status != version.status) {
      return false;
    }

    if (compressionStrategy != version.compressionStrategy) {
      return false;
    }

    if (leaderFollowerModelEnabled != version.leaderFollowerModelEnabled) {
      return false;
    }

    if (nativeReplicationEnabled != version.nativeReplicationEnabled) {
      return false;
    }

    if(!pushStreamSourceAddress.equals(version.pushStreamSourceAddress)) {
      return false;
    }

    if (bufferReplayEnabledForHybrid != version.bufferReplayEnabledForHybrid) {
      return false;
    }

    if (chunkingEnabled != version.chunkingEnabled) {
      return false;
    }

    if (partitionCount != version.partitionCount) {
      return false;
    }

    if (!Objects.equals(partitionerConfig, version.partitionerConfig)) {
      return false;
    }

    if (incrementalPushPolicy != version.incrementalPushPolicy) {
      return false;
    }

    return pushJobId.equals(version.pushJobId);
  }

  @Override
  public int hashCode() {
    int result = storeName.hashCode();
    result = 31 * result + number;
    result = 31 * result + (int) (createdTime ^ (createdTime >>> 32));
    result = 31 * result + status.hashCode();
    result = 31 * result + pushJobId.hashCode();
    result = 31 * result + compressionStrategy.hashCode();
    result = 31 * result + (leaderFollowerModelEnabled ? 1: 0);
    result = 31 * result + (bufferReplayEnabledForHybrid ? 1: 0);
    result = 31 * result + (nativeReplicationEnabled ? 1 : 0);
    result = 31 * result + pushStreamSourceAddress.hashCode();
    result = 31 * result + partitionCount;
    result = 31 * result + incrementalPushPolicy.hashCode();
    return result;
  }



  /**
   * Clone a new version based on current data in this version.
   *
   * @return cloned version.
   */
  @JsonIgnore
  public Version cloneVersion() {
    Version clonedVersion = new Version(storeName, number, createdTime, pushJobId, partitionCount, partitionerConfig);
    clonedVersion.setStatus(status);
    clonedVersion.setCompressionStrategy(compressionStrategy);
    clonedVersion.setLeaderFollowerModelEnabled(leaderFollowerModelEnabled);
    clonedVersion.setBufferReplayEnabledForHybrid(bufferReplayEnabledForHybrid);
    clonedVersion.setChunkingEnabled(chunkingEnabled);
    clonedVersion.setPushType(pushType);
    clonedVersion.setNativeReplicationEnabled(nativeReplicationEnabled);
    clonedVersion.setPushStreamSourceAddress(pushStreamSourceAddress);
    clonedVersion.setIncrementalPushPolicy(incrementalPushPolicy);
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
