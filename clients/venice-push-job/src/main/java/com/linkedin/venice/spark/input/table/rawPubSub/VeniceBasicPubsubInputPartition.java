package com.linkedin.venice.spark.input.table.rawPubSub;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import org.apache.spark.sql.connector.read.InputPartition;

/*
Basic input partition is a 1 to one mapping between a partition in the topic and a partition in the spark task
that populates the data into the spark dataframe.
  */


public class VeniceBasicPubsubInputPartition implements InputPartition {
  //
  private static final long serialVersionUID = 1L;
  private final String region;
  private final String topicName;
  private final int partitionNumber;
  private final long startOffset;
  private final long endOffset;

  public VeniceBasicPubsubInputPartition(
      String region,
      PubSubTopicPartition topicPartition,
      PubSubPosition beginningPosition,
      PubSubPosition endPosition) {
    // this needs to be serializable for all data stored here.
    this.region = region;
    this.topicName = topicPartition.getTopicName();
    this.partitionNumber = topicPartition.getPartitionNumber();
    this.startOffset = beginningPosition.getNumericOffset();
    this.endOffset = endPosition.getNumericOffset();
  }

  public String getRegion() {
    return region;
  }

  public String getTopicName() {
    return topicName;
  }

  public int getPartitionNumber() {
    return partitionNumber;
  }

  public long getStartOffset() {
    return startOffset;
  }

  public long getEndOffset() {
    return endOffset;
  }

}
