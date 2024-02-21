package com.linkedin.davinci.kafka.consumer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


public class TopicPartitionIngestionInfo {
  private long latestOffset;
  private long offsetLag;
  private double msgRate;
  private double byteRate;
  private int consumerIdx;
  private long elapsedTimeSinceLastPollInMs;

  @JsonCreator
  public TopicPartitionIngestionInfo(
      @JsonProperty("latestOffset") long latestOffset,
      @JsonProperty("offsetLag") long offsetLag,
      @JsonProperty("msgRate") double msgRate,
      @JsonProperty("byteRate") double byteRate,
      @JsonProperty("consumerIdx") int consumerIdx,
      @JsonProperty("elapsedTimeSinceLastPollInMs") long elapsedTimeSinceLastPollInMs) {
    this.latestOffset = latestOffset;
    this.offsetLag = offsetLag;
    this.msgRate = msgRate;
    this.byteRate = byteRate;
    this.consumerIdx = consumerIdx;
    this.elapsedTimeSinceLastPollInMs = elapsedTimeSinceLastPollInMs;
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

  public int getConsumerIdx() {
    return consumerIdx;
  }

  public void setConsumerIdx(int consumerIdx) {
    this.consumerIdx = consumerIdx;
  }

  public long getElapsedTimeSinceLastPollInMs() {
    return elapsedTimeSinceLastPollInMs;
  }

  public void setElapsedTimeSinceLastPollInMs(long elapsedTimeSinceLastPollInMs) {
    this.elapsedTimeSinceLastPollInMs = elapsedTimeSinceLastPollInMs;
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
        && this.consumerIdx == topicPartitionIngestionInfo.getConsumerIdx()
        && this.elapsedTimeSinceLastPollInMs == topicPartitionIngestionInfo.getElapsedTimeSinceLastPollInMs();
  }

  @Override
  public int hashCode() {
    int result = Long.hashCode(latestOffset);
    result = 31 * result + Long.hashCode(offsetLag);
    result = 31 * result + Double.hashCode(msgRate);
    result = 31 * result + Double.hashCode(byteRate);
    result = 31 * result + consumerIdx;
    result = 31 * result + Long.hashCode(elapsedTimeSinceLastPollInMs);
    return result;
  }
}
