package com.linkedin.venice.meta;

import javax.validation.constraints.NotNull;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;


/**
 * Class defines the version of Venice store.
 */
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

  public Version(String storeName, int number) {
    this(storeName , number, System.currentTimeMillis());
  }


  public Version(@JsonProperty("storeName") @NotNull String storeName, @JsonProperty("number") int number,@JsonProperty("createdTime")  long createdTime) {
    this.storeName = storeName;
    this.number = number;
    this.createdTime = createdTime;
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

  @Override
  public String toString() {
    return "Version{" +
        "storeName='" + storeName + '\'' +
        ", number=" + number +
        ", createdTime=" + createdTime +
        ", status=" + status +
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
    return status.equals(version.status);
  }

  @Override
  public int hashCode() {
    int result = storeName.hashCode();
    result = 31 * result + number;
    result = 31 * result + (int) (createdTime ^ (createdTime >>> 32));
    result = 31 * result + status.hashCode();
    return result;
  }

  /**
   * Clone a new version based on current data in this version.
   *
   * @return cloned version.
   */
  @JsonIgnore
  public Version cloneVersion() {
    Version clonedVersion = new Version(storeName, number, createdTime);
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

}
