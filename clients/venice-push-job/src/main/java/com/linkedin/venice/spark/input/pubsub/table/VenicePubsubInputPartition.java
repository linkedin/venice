package com.linkedin.venice.spark.input.pubsub.table;

import org.apache.spark.sql.connector.read.InputPartition;

/*
 This split can be a whole partition or sub part of a pubsub partition, hence the name segment.
 This is intentional not to mix up the Kafka partition and spark idea of a split
 the equivalent class for hdfs is VeniceHdfsInputPartition
 */


public class VenicePubsubInputPartition implements InputPartition {
  //
  private static final long serialVersionUID = 1L;

  private final String region;
  private final String TopicName;
  private final int partitionNumber;
  private final long segmentStartOffset;
  private final long segmentEndOffset;

  public VenicePubsubInputPartition(
      String region,
      String topicName,
      int partitionNumber,
      long startOffset,
      long endOffset) {
    this.region = region;
    this.TopicName = topicName;
    this.partitionNumber = partitionNumber;
    this.segmentStartOffset = startOffset;
    this.segmentEndOffset = endOffset;
  }

  public String getRegion() {
    return region;
  }

  public String getTopicName() {
    return TopicName;
  }

  public int getPartitionNumber() {
    return partitionNumber;
  }

  public long getSegmentStartOffset() {
    return segmentStartOffset;
  }

  public long getSegmentEndOffset() {
    return segmentEndOffset;
  }
}
