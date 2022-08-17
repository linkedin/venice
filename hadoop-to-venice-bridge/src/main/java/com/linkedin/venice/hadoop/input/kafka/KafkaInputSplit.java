package com.linkedin.venice.hadoop.input.kafka;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.kafka.common.TopicPartition;


/**
 * We borrowed some idea from the open-sourced attic-crunch lib:
 * https://github.com/apache/attic-crunch/blob/master/crunch-kafka/src/main/java/org/apache/crunch/kafka/record/KafkaInputSplit.java
 *
 * InputSplit that represent retrieving data from a single {@link TopicPartition} between the specified start
 * and end offsets.
 */
public class KafkaInputSplit implements InputSplit {
  private long startingOffset;
  private long endingOffset;
  private TopicPartition topicPartition;

  /**
   * Nullary Constructor for creating the instance inside the Mapper instance.
   */
  public KafkaInputSplit() {
  }

  /**
   * Constructs an input split for the provided {@param topic} and {@param partition} restricting data to be between
   * the {@param startingOffset} and {@param endingOffset}
   *
   * @param topic          the topic for the split
   * @param partition      the partition for the topic
   * @param startingOffset the start of the split
   * @param endingOffset   the end of the split
   */
  public KafkaInputSplit(String topic, int partition, long startingOffset, long endingOffset) {
    this.startingOffset = startingOffset;
    this.endingOffset = endingOffset;
    topicPartition = new TopicPartition(topic, partition);
  }

  @Override
  public long getLength() throws IOException {
    // This is just used as a hint for size of bytes so it is already inaccurate.
    return startingOffset > 0 ? endingOffset - startingOffset : endingOffset;
  }

  @Override
  public String[] getLocations() throws IOException {
    // Leave empty since data locality not really an issue.
    return new String[0];
  }

  /**
   * Returns the topic and partition for the split
   *
   * @return the topic and partition for the split
   */
  public TopicPartition getTopicPartition() {
    return topicPartition;
  }

  /**
   * Returns the starting offset for the split
   *
   * @return the starting offset for the split
   */
  public long getStartingOffset() {
    return startingOffset;
  }

  /**
   * Returns the ending offset for the split
   *
   * @return the ending offset for the split
   */
  public long getEndingOffset() {
    return endingOffset;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(topicPartition.topic());
    dataOutput.writeInt(topicPartition.partition());
    dataOutput.writeLong(startingOffset);
    dataOutput.writeLong(endingOffset);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    String topic = dataInput.readUTF();
    int partition = dataInput.readInt();
    startingOffset = dataInput.readLong();
    endingOffset = dataInput.readLong();

    topicPartition = new TopicPartition(topic, partition);
  }

  @Override
  public String toString() {
    return getTopicPartition() + " Start: " + startingOffset + " End: " + endingOffset;
  }
}
