package com.linkedin.davinci.kafka.consumer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


public class TopicPartitionIngestionInfo {
  private long latestOffset;
  private long offsetLag;
  private double msgRate;
  private double byteRate;
  private double pollRate;
  private String consumerIdStr;
  private long elapsedTimeSinceLastPollInMs;

  private String versionTopicName;

  @JsonCreator
  public TopicPartitionIngestionInfo(
      @JsonProperty("latestOffset") long latestOffset,
      @JsonProperty("offsetLag") long offsetLag,
      @JsonProperty("msgRate") double msgRate,
      @JsonProperty("byteRate") double byteRate,
      @JsonProperty("pollRate") double pollRate,
      @JsonProperty("consumerIdStr") String consumerIdStr,
      @JsonProperty("elapsedTimeSinceLastPollInMs") long elapsedTimeSinceLastPollInMs,
      @JsonProperty("versionTopicName") String versionTopicName) {
    this.latestOffset = latestOffset;
    this.offsetLag = offsetLag;
    this.msgRate = msgRate;
    this.byteRate = byteRate;
    this.pollRate = pollRate;
    this.consumerIdStr = consumerIdStr;
    this.elapsedTimeSinceLastPollInMs = elapsedTimeSinceLastPollInMs;
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

  public double getPollRate() {
    return pollRate;
  }

  public void setPollRate(double pollRate) {
    this.pollRate = pollRate;
  }

  public String getConsumerIdStr() {
    return consumerIdStr;
  }

  public void setConsumerIdStr(String consumerIdStr) {
    this.consumerIdStr = consumerIdStr;
  }

  public long getElapsedTimeSinceLastPollInMs() {
    return elapsedTimeSinceLastPollInMs;
  }

  public void setElapsedTimeSinceLastPollInMs(long elapsedTimeSinceLastPollInMs) {
    this.elapsedTimeSinceLastPollInMs = elapsedTimeSinceLastPollInMs;
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
        && Double.doubleToLongBits(this.pollRate) == Double.doubleToLongBits(topicPartitionIngestionInfo.getPollRate())
        && this.consumerIdStr.equals(topicPartitionIngestionInfo.getConsumerIdStr())
        && this.elapsedTimeSinceLastPollInMs == topicPartitionIngestionInfo.getElapsedTimeSinceLastPollInMs()
        && this.versionTopicName.equals(topicPartitionIngestionInfo.getVersionTopicName());
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(latestOffset);
    result = 31 * result + Long.hashCode(offsetLag);
    result = 31 * result + Double.hashCode(msgRate);
    result = 31 * result + Double.hashCode(byteRate);
    result = 31 * result + Double.hashCode(pollRate);
    result = 31 * result + consumerIdStr.hashCode();
    result = 31 * result + Long.hashCode(elapsedTimeSinceLastPollInMs);
    result = 31 * result + versionTopicName.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "{" + "latestOffset:" + latestOffset + ", offsetLag:" + offsetLag + ", msgRate:" + msgRate + ", byteRate:"
        + byteRate + ", pollRate:" + pollRate + ", consumerIdStr:" + consumerIdStr + ", elapsedTimeSinceLastPollInMs:"
        + elapsedTimeSinceLastPollInMs + ", versionTopicName:" + versionTopicName + '}';
  }
}
