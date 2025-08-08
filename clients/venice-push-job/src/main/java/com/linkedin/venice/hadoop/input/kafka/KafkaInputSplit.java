package com.linkedin.venice.hadoop.input.kafka;

import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
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
  private PubSubTopicPartition topicPartition;
  private PubSubPosition startingOffset;
  private PubSubPosition endingOffset;
  private long numberOfRecords = 0L;
  private final PubSubTopicRepository topicRepository;

  /**
   * Nullary Constructor for creating the instance inside the Mapper instance.
   */
  public KafkaInputSplit() {
    topicRepository = new PubSubTopicRepository();
  }

  /**
   * Constructs an input split for the provided {@param topic} and {@param partition} restricting data to be between
   * the {@param startingOffset} and {@param endingOffset}
   *
   * @param topicPartition  the topic-partition for the split
   * @param startingOffset the start of the split
   * @param endingOffset   the end of the split
   */
  public KafkaInputSplit(
      PubSubTopicRepository topicRepository,
      PubSubTopicPartition topicPartition,
      PubSubPosition startingOffset,
      PubSubPosition endingOffset,
      long numberOfRecords) {
    this.topicRepository = topicRepository;
    this.startingOffset = startingOffset;
    this.endingOffset = endingOffset;
    this.topicPartition = topicPartition;
    this.numberOfRecords = numberOfRecords;
  }

  @Override
  public long getLength() throws IOException {
    // This is just used as a hint for size of bytes so it is already inaccurate.
    return numberOfRecords;
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
  public PubSubTopicPartition getTopicPartition() {
    return topicPartition;
  }

  /**
   * Returns the starting offset for the split
   *
   * @return the starting offset for the split
   */
  public PubSubPosition getStartingOffset() {
    return startingOffset;
  }

  /**
   * Returns the ending offset for the split
   *
   * @return the ending offset for the split
   */
  public PubSubPosition getEndingOffset() {
    return endingOffset;
  }

  /**
   * Returns the number of records in this split.
   *
   * @return the number of records in this split
   */
  public long getNumberOfRecords() {
    return numberOfRecords;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    // Write topic, partition, and number of records
    dataOutput.writeUTF(topicPartition.getTopicName());
    dataOutput.writeInt(topicPartition.getPartitionNumber());
    dataOutput.writeLong(numberOfRecords);

    // Write startingOffset
    byte[] startBytes = startingOffset.toWireFormatBytes();
    dataOutput.writeInt(startBytes.length);
    dataOutput.write(startBytes);
    dataOutput.writeUTF(startingOffset.getFactoryClassName());

    // Write endingOffset
    byte[] endBytes = endingOffset.toWireFormatBytes();
    dataOutput.writeInt(endBytes.length);
    dataOutput.write(endBytes);
    dataOutput.writeUTF(endingOffset.getFactoryClassName());
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    // Read topic, partition, and number of records
    String topic = dataInput.readUTF();
    int partition = dataInput.readInt();
    topicPartition = new PubSubTopicPartitionImpl(topicRepository.getTopic(topic), partition);
    numberOfRecords = dataInput.readLong();

    // Read and reconstruct startingOffset
    int startLength = dataInput.readInt();
    byte[] startBytes = new byte[startLength];
    dataInput.readFully(startBytes);
    String startFactoryClass = dataInput.readUTF();
    startingOffset = PubSubPositionDeserializer.deserializePubSubPosition(startBytes, startFactoryClass);

    // Read and reconstruct endingOffset
    int endLength = dataInput.readInt();
    byte[] endBytes = new byte[endLength];
    dataInput.readFully(endBytes);
    String endFactoryClass = dataInput.readUTF();
    endingOffset = PubSubPositionDeserializer.deserializePubSubPosition(endBytes, endFactoryClass);
  }

  @Override
  public String toString() {
    return getTopicPartition() + " Start: " + startingOffset + " End: " + endingOffset;
  }
}
