package com.linkedin.venice.spark.input.table.rawPubSub;

import org.apache.spark.sql.connector.read.InputPartition;

/*
Basic input partition is a 1 to one mapping between a partition in the topic and a partition in the spark task
that populates the data into the spark dataframe.
  */


public class VeniceBasicPubsubInputPartition implements InputPartition {
  //
  private static final long serialVersionUID = 1L;

  private final String region;
  private final String TopicName;
  private final int partitionNumber;

  public VeniceBasicPubsubInputPartition(String region, String topicName, int partitionNumber) {
    this.region = region;
    this.TopicName = topicName;
    this.partitionNumber = partitionNumber;
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
}
