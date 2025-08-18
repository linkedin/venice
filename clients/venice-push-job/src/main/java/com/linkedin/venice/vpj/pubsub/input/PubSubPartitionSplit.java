package com.linkedin.venice.vpj.pubsub.input;

import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;


/**
 * Describes a contiguous range of records within a single {@link PubSubTopicPartition}.
 * The range is bounded by a start position (inclusive) and an end position (exclusive).
 * It is used to enable parallel reading during repush jobs, where data is read from
 * pub-sub topics and written into a new version of a Venice store.
 *
 * <p><b>Scope and constraints:</b>
 * <ul>
 *   <li>One {@code PubSubPartitionSplit} covers exactly one topic partition.</li>
 *   <li>{@code startPubSubPosition} must be <= {@code endPubSubPosition}.</li>
 *   <li>Instances should be treated as immutable once constructed.</li>
 * </ul>
 *
 * <p><b>Engine independence:</b>
 * This is a framework-agnostic descriptor. Adapters can translate it to a
 * Hadoop {@code InputSplit} or a Spark DataSource V2 {@code InputPartition}.
 */
public class PubSubPartitionSplit implements Serializable {
  private static final long serialVersionUID = 1L;

  /** Serializable identity for the partition (avoid holding non-serializable PubSubTopicPartition). */
  private String topicName;
  private int partitionNumber;

  /** Positions are stored as wire payloads for serialization, lazily reconstructed when accessed. */
  private byte[] startBytes;
  private String startFactoryClass;
  private byte[] endBytes;
  private String endFactoryClass;

  /** Total records in the range. */
  private long numberOfRecords;

  /** Monotonically increasing index within the same topic-partition, starting from 0. */
  private int splitIndex;

  /** Locally built starting index of the first record in this split. */
  private long startIndex;

  /** Non-serializable helpers, reconstructed lazily when needed. */
  private transient PubSubTopicRepository topicRepository;
  private transient PubSubTopicPartition pubSubTopicPartition;
  private transient PubSubPosition startPubSubPosition;
  private transient PubSubPosition endPubSubPosition;

  /** Nullary constructor for deserialization. */
  public PubSubPartitionSplit() {
    this.topicRepository = new PubSubTopicRepository();
  }

  public PubSubPartitionSplit(
      PubSubTopicRepository topicRepository,
      PubSubTopicPartition pubSubTopicPartition,
      PubSubPosition startPubSubPosition,
      PubSubPosition endPubSubPosition,
      long numberOfRecords,
      int splitIndex,
      long startIndex) {

    this.topicRepository = Objects.requireNonNull(topicRepository, "topicRepository");
    Objects.requireNonNull(pubSubTopicPartition, "pubSubTopicPartition");
    Objects.requireNonNull(startPubSubPosition, "startPubSubPosition");
    Objects.requireNonNull(endPubSubPosition, "endPubSubPosition");

    this.topicName = pubSubTopicPartition.getTopicName();
    this.partitionNumber = pubSubTopicPartition.getPartitionNumber();

    this.startBytes = startPubSubPosition.toWireFormatBytes();
    this.startFactoryClass = startPubSubPosition.getFactoryClassName();
    this.endBytes = endPubSubPosition.toWireFormatBytes();
    this.endFactoryClass = endPubSubPosition.getFactoryClassName();

    this.numberOfRecords = numberOfRecords;
    this.splitIndex = splitIndex;
    this.startIndex = startIndex;

    // Cache transient objects for in-process use
    this.pubSubTopicPartition = pubSubTopicPartition;
    this.startPubSubPosition = startPubSubPosition;
    this.endPubSubPosition = endPubSubPosition;
  }

  public PubSubTopicPartition getPubSubTopicPartition() {
    if (pubSubTopicPartition == null) {
      pubSubTopicPartition = new PubSubTopicPartitionImpl(topicRepository.getTopic(topicName), partitionNumber);
    }
    return pubSubTopicPartition;
  }

  public PubSubPosition getStartPubSubPosition() {
    if (startPubSubPosition == null) {
      startPubSubPosition = PubSubPositionDeserializer.deserializePubSubPosition(startBytes, startFactoryClass);
    }
    return startPubSubPosition;
  }

  public PubSubPosition getEndPubSubPosition() {
    if (endPubSubPosition == null) {
      endPubSubPosition = PubSubPositionDeserializer.deserializePubSubPosition(endBytes, endFactoryClass);
    }
    return endPubSubPosition;
  }

  public String getTopicName() {
    return topicName;
  }

  public int getPartitionNumber() {
    return partitionNumber;
  }

  public long getNumberOfRecords() {
    return numberOfRecords;
  }

  public int getSplitIndex() {
    return splitIndex;
  }

  public long getStartIndex() {
    return startIndex;
  }

  public void writeTo(DataOutput out) throws IOException {
    // meta
    out.writeInt(splitIndex);
    out.writeLong(startIndex);
    out.writeUTF(topicName);
    out.writeInt(partitionNumber);
    out.writeLong(numberOfRecords);

    // start position
    out.writeInt(startBytes.length);
    out.write(startBytes);
    out.writeUTF(startFactoryClass);

    // end position
    out.writeInt(endBytes.length);
    out.write(endBytes);
    out.writeUTF(endFactoryClass);
  }

  public void readFields(DataInput in) throws IOException {
    // meta
    this.splitIndex = in.readInt();
    this.startIndex = in.readLong();
    this.topicName = in.readUTF();
    this.partitionNumber = in.readInt();
    this.numberOfRecords = in.readLong();

    // start position
    int sLen = in.readInt();
    this.startBytes = new byte[sLen];
    in.readFully(this.startBytes);
    this.startFactoryClass = in.readUTF();

    // end position
    int eLen = in.readInt();
    this.endBytes = new byte[eLen];
    in.readFully(this.endBytes);
    this.endFactoryClass = in.readUTF();

    // clear transient caches
    this.pubSubTopicPartition = null;
    this.startPubSubPosition = null;
    this.endPubSubPosition = null;
  }

  public static PubSubPartitionSplit readFrom(DataInput in) throws IOException {
    PubSubPartitionSplit s = new PubSubPartitionSplit();
    s.readFields(in);
    return s;
  }

  public void write(DataOutput out) throws IOException {
    writeTo(out);
  }

  @Override
  public String toString() {
    return "PubSubPartitionSplit{tp=" + topicName + "-" + partitionNumber + ", startIndex=" + startIndex + ", idx="
        + splitIndex + ", n=" + numberOfRecords + ", start=" + getStartPubSubPosition() + ", end="
        + getEndPubSubPosition() + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof PubSubPartitionSplit))
      return false;
    PubSubPartitionSplit that = (PubSubPartitionSplit) o;
    return partitionNumber == that.partitionNumber && numberOfRecords == that.numberOfRecords
        && splitIndex == that.splitIndex && startIndex == that.startIndex && topicName.equals(that.topicName)
        && startFactoryClass.equals(that.startFactoryClass) && endFactoryClass.equals(that.endFactoryClass)
        && java.util.Arrays.equals(startBytes, that.startBytes) && java.util.Arrays.equals(endBytes, that.endBytes);
  }

  @Override
  public int hashCode() {
    int result = Objects
        .hash(topicName, partitionNumber, numberOfRecords, splitIndex, startIndex, startFactoryClass, endFactoryClass);
    result = 31 * result + java.util.Arrays.hashCode(startBytes);
    result = 31 * result + java.util.Arrays.hashCode(endBytes);
    return result;
  }

  public PubSubTopicRepository getTopicRepository() {
    return topicRepository;
  }
}
