package com.linkedin.davinci.blobtransfer;

import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * This class is the metadata of a partition in the blob transfer client
 */
public class BlobTransferPartitionMetadata {
  public String topicName;
  public int partitionId;
  public java.nio.ByteBuffer offsetRecord;
  public java.nio.ByteBuffer storeVersionState;

  public BlobTransferPartitionMetadata() {
  }

  public BlobTransferPartitionMetadata(
      @JsonProperty("topicName") String topicName,
      @JsonProperty("partitionId") int partitionId,
      @JsonProperty("offsetRecord") java.nio.ByteBuffer offsetRecord,
      @JsonProperty("storeVersionState") java.nio.ByteBuffer storeVersionState) {
    this.topicName = topicName;
    this.partitionId = partitionId;
    this.offsetRecord = offsetRecord;
    this.storeVersionState = storeVersionState;
  }

  public String getTopicName() {
    return topicName;
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }

  public java.nio.ByteBuffer getOffsetRecord() {
    return offsetRecord;
  }

  public void setOffsetRecord(java.nio.ByteBuffer offsetRecord) {
    this.offsetRecord = offsetRecord;
  }

  public void setStoreVersionState(java.nio.ByteBuffer storeVersionState) {
    this.storeVersionState = storeVersionState;
  }

  public java.nio.ByteBuffer getStoreVersionState() {
    return storeVersionState;
  }

  @Override
  public String toString() {
    return "BlobTransferPartitionMetadata {" + " topicName='" + topicName + ", partitionId=" + partitionId
        + ", offsetRecord=" + offsetRecord + ", storeVersionState=" + storeVersionState + " }";
  }
}
