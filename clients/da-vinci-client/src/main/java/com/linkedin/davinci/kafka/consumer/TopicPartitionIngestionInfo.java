package com.linkedin.davinci.kafka.consumer;

class TopicPartitionIngestionInfo {
  protected long latestOffset;
  protected long offsetLag;
  protected double msgRate;
  protected double byteRate;
  protected int consumerIdx;
  protected long elapsedTimeSinceLastPollInMs;

  TopicPartitionIngestionInfo(
      long latestOffset,
      long offsetLag,
      double msgRate,
      double byteRate,
      int consumerIdx,
      long elapsedTimeSinceLastPollInMs) {
    this.latestOffset = latestOffset;
    this.offsetLag = offsetLag;
    this.msgRate = msgRate;
    this.byteRate = byteRate;
    this.consumerIdx = consumerIdx;
    this.elapsedTimeSinceLastPollInMs = elapsedTimeSinceLastPollInMs;
  }
}
