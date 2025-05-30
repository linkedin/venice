package com.linkedin.davinci.kafka.consumer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


public class TopicPartitionIngestionInfo {
  private long latestOffset;
  private long offsetLag;
  private double msgRate;
  private double byteRate;
  private String consumerIdStr;
  /**
   * Tracks how much time has passed since the last Kafka consumer poll operation, regardless of whether any records were received.
   * This field helps monitor the overall consumer health and polling activity.
   * For example, if this value is too high, it might indicate that the consumer is not actively polling or is experiencing issues.
   */
  private long elapsedTimeSinceLastConsumerPollInMs;
  /**
   * Tracks how much time has passed since the last time any records were actually received for this specific topic partition.
   * Unlike elapsedTimeSinceLastConsumerPollInMs which tracks any poll attempt, this field only considers polls that returned data
   * for this particular partition.
   * This helps identify partitions that haven't received new data for a while, which could indicate upstream issues or partition-specific delays.
   */
  private long elapsedTimeSinceLastRecordForPartitionInMs;
  private String versionTopicName;

  @JsonCreator
  public TopicPartitionIngestionInfo(
      @JsonProperty("latestOffset") long latestOffset,
      @JsonProperty("offsetLag") long offsetLag,
      @JsonProperty("msgRate") double msgRate,
      @JsonProperty("byteRate") double byteRate,
      @JsonProperty("consumerIdStr") String consumerIdStr,
      @JsonProperty("elapsedTimeSinceLastPollInMs") long elapsedTimeSinceLastConsumerPollInMs,
      @JsonProperty("elapsedTimeSinceLastPolledRecordsInMs") long elapsedTimeSinceLastRecordForPartitionInMs,
      @JsonProperty("versionTopicName") String versionTopicName) {
    this.latestOffset = latestOffset;
    this.offsetLag = offsetLag;
    this.msgRate = msgRate;
    this.byteRate = byteRate;
    this.consumerIdStr = consumerIdStr;
    this.elapsedTimeSinceLastConsumerPollInMs = elapsedTimeSinceLastConsumerPollInMs;
    this.elapsedTimeSinceLastRecordForPartitionInMs = elapsedTimeSinceLastRecordForPartitionInMs;
    this.versionTopicName = versionTopicName;
  }

  public long getLatestOffset() {
    return latestOffset;
  }

  public long getOffsetLag() {
    return offsetLag;
  }

  public void setOffsetLag(long offsetLag) {
    this.offsetLag = offsetLag;
  }

  public double getMsgRate() {
    return msgRate;
  }

  public void setMsgRate(double msgRate) {
    this.msgRate = msgRate;
  }

  public double getByteRate() {
    return byteRate;
  }

  public void setByteRate(double byteRate) {
    this.byteRate = byteRate;
  }

  public String getConsumerIdStr() {
    return consumerIdStr;
  }

  public void setConsumerIdStr(String consumerIdStr) {
    this.consumerIdStr = consumerIdStr;
  }

  public long getElapsedTimeSinceLastConsumerPollInMs() {
    return elapsedTimeSinceLastConsumerPollInMs;
  }

  public void setElapsedTimeSinceLastConsumerPollInMs(long elapsedTimeSinceLastConsumerPollInMs) {
    this.elapsedTimeSinceLastConsumerPollInMs = elapsedTimeSinceLastConsumerPollInMs;
  }

  public long getElapsedTimeSinceLastRecordForPartitionInMs() {
    return elapsedTimeSinceLastRecordForPartitionInMs;
  }

  public void setElapsedTimeSinceLastRecordForPartitionInMs(long elapsedTimeSinceLastRecordForPartitionInMs) {
    this.elapsedTimeSinceLastRecordForPartitionInMs = elapsedTimeSinceLastRecordForPartitionInMs;
  }

  public String getVersionTopicName() {
    return versionTopicName;
  }

  public void setVersionTopicName(String versionTopicName) {
    this.versionTopicName = versionTopicName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TopicPartitionIngestionInfo topicPartitionIngestionInfo = (TopicPartitionIngestionInfo) o;
    return this.latestOffset == topicPartitionIngestionInfo.getLatestOffset()
        && this.offsetLag == topicPartitionIngestionInfo.getOffsetLag()
        && Double.doubleToLongBits(this.msgRate) == Double.doubleToLongBits(topicPartitionIngestionInfo.getMsgRate())
        && Double.doubleToLongBits(this.byteRate) == Double.doubleToLongBits(topicPartitionIngestionInfo.getByteRate())
        && this.consumerIdStr.equals(topicPartitionIngestionInfo.getConsumerIdStr())
        && this.elapsedTimeSinceLastConsumerPollInMs == topicPartitionIngestionInfo
            .getElapsedTimeSinceLastConsumerPollInMs()
        && this.elapsedTimeSinceLastRecordForPartitionInMs == topicPartitionIngestionInfo
            .getElapsedTimeSinceLastRecordForPartitionInMs()
        && this.versionTopicName.equals(topicPartitionIngestionInfo.getVersionTopicName());
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(latestOffset);
    result = 31 * result + Long.hashCode(offsetLag);
    result = 31 * result + Double.hashCode(msgRate);
    result = 31 * result + Double.hashCode(byteRate);
    result = 31 * result + consumerIdStr.hashCode();
    result = 31 * result + Long.hashCode(elapsedTimeSinceLastConsumerPollInMs);
    result = 31 * result + Long.hashCode(elapsedTimeSinceLastRecordForPartitionInMs);
    result = 31 * result + versionTopicName.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "{" + "latestOffset:" + latestOffset + ", offsetLag:" + offsetLag + ", msgRate:" + msgRate + ", byteRate:"
        + byteRate + ", consumerIdStr:" + consumerIdStr + ", elapsedTimeSinceLastConsumerPollInMs:"
        + elapsedTimeSinceLastConsumerPollInMs + ", elapsedTimeSinceLastRecordForPartitionInMs:"
        + elapsedTimeSinceLastRecordForPartitionInMs + ", versionTopicName:" + versionTopicName + '}';
  }
}
