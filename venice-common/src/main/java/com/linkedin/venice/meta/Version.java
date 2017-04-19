package com.linkedin.venice.meta;

import com.linkedin.venice.guid.GuidUtils;
import javax.validation.constraints.NotNull;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;


/**
 * Class defines the version of Venice store.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Version implements Comparable<Version> {
  private static final String SEPARATOR = "_v";
  /**
   * Name of the store which this version belong to.
   */
  private final String storeName;
  /**
   * Version number.
   */
  private final int number;
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
   * Use the constructor that specifies a pushJobId instead
   */
  @Deprecated
  public Version(String storeName, int number) {
    this(storeName , number, System.currentTimeMillis(), numberBasedDummyPushId(number));
  }

  public Version(String storeName, int number, String pushJobId){
    this(storeName, number, System.currentTimeMillis(), pushJobId);
  }

  public Version(@JsonProperty("storeName") @NotNull String storeName, @JsonProperty("number") int number, @JsonProperty("createdTime")  long createdTime, @JsonProperty("pushJobId") String pushJobId) {
    this.storeName = storeName;
    this.number = number;
    this.createdTime = createdTime;
    this.pushJobId = pushJobId == null ? numberBasedDummyPushId(number) : pushJobId; // for deserializing old Versions that didn't get an pushJobId
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

  public void setStatus(@NotNull VersionStatus status) {
    this.status = status;
  }

  public String getStoreName() {
    return storeName;
  }

  public String getPushJobId() {
    return pushJobId;
  }

  @Override
  public String toString() {
    return "Version{" +
        "storeName='" + storeName + '\'' +
        ", number=" + number +
        ", createdTime=" + createdTime +
        ", status=" + status +
        ", pushJobId='" + pushJobId + '\'' +
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
    if (!storeName.equals(version.storeName)) {
      return false;
    }
    if (status != version.status) {
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
    return result;
  }



  /**
   * Clone a new version based on current data in this version.
   *
   * @return cloned version.
   */
  @JsonIgnore
  public Version cloneVersion() {
    Version clonedVersion = new Version(storeName, number, createdTime, pushJobId);
    clonedVersion.setStatus(status);
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
    return composeKafkaTopic(storeName,number);
  }

  public static String parseStoreFromKafkaTopicName(@NotNull String kafkaTopic) {
    return kafkaTopic.substring(0, kafkaTopic.lastIndexOf(SEPARATOR));
  }

  public static int parseVersionFromKafkaTopicName(@NotNull String kafkaTopic) {
    return Integer.valueOf(kafkaTopic.substring(kafkaTopic.lastIndexOf(SEPARATOR) + SEPARATOR.length()));
  }

  public static String composeKafkaTopic(String storeName,int versionNumber){
    return storeName + SEPARATOR + versionNumber;
  }

  public static boolean topicIsValidStoreVersion(String kafkaTopic){
    try{
      parseVersionFromKafkaTopicName(kafkaTopic);
    } catch (NumberFormatException e){
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
