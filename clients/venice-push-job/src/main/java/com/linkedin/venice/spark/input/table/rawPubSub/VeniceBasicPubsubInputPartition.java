package com.linkedin.venice.spark.input.table.rawPubSub;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
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
  private final PubSubTopic topic;
  private final PubSubTopicPartition topicPartition;
  private final PubSubPosition beginningPosition;
  private final PubSubPosition endPosition;

  public VeniceBasicPubsubInputPartition(
      String region,
      PubSubTopic topic,
      PubSubTopicPartition topicPartition,
      PubSubPosition beginningPosition,
      PubSubPosition endPosition) {
    this.region = region;
    this.topic = topic;
    this.topicPartition = topicPartition;
    this.beginningPosition = beginningPosition;
    this.endPosition = endPosition;
  }

  public String getRegion() {
    return region;
  }

  public PubSubTopic getTopic() {
    return topic;
  }

  public int getPartitionNumber() {
    return topicPartition.getPartitionNumber();
  }

  public PubSubTopicPartition getTopicPartition() {
    return topicPartition;
  }

  public PubSubPosition getBeginningPosition() {
    return beginningPosition;
  }

  public long getBeginningOffset() {
    return beginningPosition.getNumericOffset();
  }

  public PubSubPosition getEndPosition() {
    return endPosition;
  }

  public long getEndOffset() {
    return beginningPosition.getNumericOffset();
  }

  public long getOffsetLength() {
    return (endPosition.getNumericOffset() - beginningPosition.getNumericOffset());
  }

}
