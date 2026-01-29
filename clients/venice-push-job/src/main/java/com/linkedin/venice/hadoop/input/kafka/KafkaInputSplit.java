package com.linkedin.venice.hadoop.input.kafka;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.vpj.pubsub.input.PubSubPartitionSplit;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.mapred.InputSplit;


/**
 * We borrowed some idea from the open-sourced attic-crunch lib:
 * https://github.com/apache/attic-crunch/blob/master/crunch-kafka/src/main/java/org/apache/crunch/kafka/record/KafkaInputSplit.java
 *
 * InputSplit that represent retrieving data from a single {@link PubSubTopicPartition} between the specified start
 * and end offsets.
 */
public class KafkaInputSplit implements InputSplit {
  private PubSubPartitionSplit split;

  /** Nullary constructor for Hadoop to instantiate reflectively. */
  public KafkaInputSplit() {
  }

  /**
   * Original constructor signature, now wiring through to PubSubPartitionSplit.
   */
  public KafkaInputSplit(PubSubPartitionSplit pubSubPartitionSplit) {
    this.split = pubSubPartitionSplit;
  }

  public PubSubTopicPartition getTopicPartition() {
    return split.getPubSubTopicPartition();
  }

  public PubSubPosition getStartingOffset() {
    return split.getStartPubSubPosition();
  }

  public PubSubPosition getEndingOffset() {
    return split.getEndPubSubPosition();
  }

  public long getNumberOfRecords() {
    return split.getNumberOfRecords();
  }

  @Override
  public long getLength() {
    return split.getNumberOfRecords();
  }

  @Override
  public String[] getLocations() {
    // Leave empty since data locality not really an issue.
    return new String[0];
  }

  @Override
  public void write(DataOutput out) throws IOException {
    split.writeTo(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.split = PubSubPartitionSplit.readFrom(in);
  }

  PubSubPartitionSplit getSplit() {
    return split;
  }

  @Override
  public String toString() {
    return getTopicPartition() + " Start: " + getStartingOffset() + " End: " + getEndingOffset();
  }
}
