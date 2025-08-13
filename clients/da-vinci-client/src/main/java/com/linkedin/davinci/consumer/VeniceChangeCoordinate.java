package com.linkedin.davinci.consumer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.PubSubUtil;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import com.linkedin.venice.utils.RedundantExceptionFilter;
import com.linkedin.venice.utils.Utils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class represents a change coordinate in Venice. It contains the topic name, partition number, and
 * the pubsub position.
 */
public class VeniceChangeCoordinate implements Externalizable {
  private static final Logger LOGGER = LogManager.getLogger(VeniceChangeCoordinate.class);
  private static final RedundantExceptionFilter REDUNDANT_LOGGING_FILTER =
      RedundantExceptionFilter.getRedundantExceptionFilter();
  /**
   * Value for undefined consumer sequence id. This can happen when we decode a serialized VeniceChangeCoordinate before
   * the consumer sequence id field was introduced. This can also happen when the VeniceChangeCoordinate was created
   * without a valid consumer sequence id. e.g. Created not by consuming from the change log but instead from an
   * arbitrary {@link PubSubPosition}.
   */
  public static final long UNDEFINED_CONSUMER_SEQUENCE_ID = -1;
  private static final long serialVersionUID = 1L;
  private static final short VERSION_V2 = 2;
  private static final short VERSION_V3 = 3;
  private static final short CURRENT_VERSION = VERSION_V3;

  private String topic;
  private Integer partition;
  private PubSubPosition pubSubPosition;
  private transient PubSubPositionDeserializer pubSubPositionDeserializer;

  /**
   * A sequence id that's unique and monotonically increasing per record polled from the same consumer instance and
   * partition. This can be used to reason about the order of events consumed from the same consumer and partition.
   * e.g. event A and B are both consumed from partition 0 from the same consumer instance. If B's sequence id is
   * greater than A then it means B happened after A. The sequence id is mostly contiguous, but it is not guaranteed
   * because there can be messages that are consumed internally (sequence id is already incremented) but not processed
   * by the external consumer yet. In most cases it's good enough to provide some heuristic about the rate of
   * consumption or number of events consumed, but it's not meant for precise measurement.
   */
  private long consumerSequenceId;

  public VeniceChangeCoordinate() {
    // Empty constructor is public for Externalizable
  }

  /**
   * Serializes the VeniceChangeCoordinate in a backward-compatible format.
   *
   * <p>The serialization format is structured as follows:</p>
   * <ol>
   *   <li>Core fields: topic, partition, pubSubPositionWireFormat</li>
   *   <li>Version tag: a UTF string that identifies the version of the serialized data</li>
   *   <li>Version-specific fields: additional fields added in newer versions</li>
   * </ol>
   *
   * <p>This format ensures that older readers (pre-v2) can still deserialize the first three fields,
   * while newer readers will detect and parse the version tag and the extra fields.</p>
   *
   * @param out the output stream to write to
   * @throws IOException if writing fails
   */
  @Override
  /**
   * @param out to write to
   * @throws IOException
   * Writes the fields to the provided output in the order of
   * 1. topic
   * 2. partition
   * 3. pubSubPosition
   * 4. consumerSequenceId
   */
  public void writeExternal(ObjectOutput out) throws IOException {
    // Defensive checks to avoid corrupting serialized data
    if (topic == null || partition == null || pubSubPosition == null) {
      throw new IllegalStateException("Cannot serialize VeniceChangeCoordinate with null required fields");
    }

    // Write core fields — legacy readers will stop reading after this
    out.writeUTF(topic);
    out.writeInt(partition);
    out.writeObject(pubSubPosition.getPositionWireFormat());

    // Begin versioned block
    out.writeShort(CURRENT_VERSION); // Write version marker after core fields

    // Version-specific fields for v2
    out.writeUTF(pubSubPosition.getFactoryClassName());
    // Version-specific fields for v3
    out.writeLong(consumerSequenceId);
  }

  /**
   * Deserializes VeniceChangeCoordinate while supporting both legacy (v1) and newer (v2+) formats.
   *
   * <p>The method first reads the core fields (topic, partition, position wire format),
   * which are common across versions. It then attempts to read a version tag.</p>
   *
   * <ul>
   *   <li>If the version tag is present and recognized, version-specific fields are read accordingly.</li>
   *   <li>If the tag is missing or reading fails, the method falls back to v1 format.</li>
   * </ul>
   *
   * <p>This approach is robust against partial data and ensures forward and backward compatibility.</p>
   *
   * @param in the input stream to read from
   * @throws IOException if an I/O error occurs
   * @throws VeniceException if the class of a serialized object cannot be found
   */
  @Override
  /**
   * Reads the fields from the provided input in the order of
   * 1. topic
   * 2. partition
   * 3. pubSubPosition
   * 4. consumerSequenceId
   * @param in to read from
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    // Read shared core fields
    this.topic = in.readUTF();
    this.partition = in.readInt();
    PubSubPositionWireFormat positionWf = (PubSubPositionWireFormat) in.readObject();

    try {
      // Attempt to read version field — this will only succeed for v2+ writers
      int version = in.readShort();

      switch (version) {
        case VERSION_V2:
          // v2: pubSubPosition factory class follows the version tag
          String factoryClassName = in.readUTF();
          this.pubSubPosition = PubSubPositionDeserializer.deserializePubSubPosition(positionWf, factoryClassName);
          this.consumerSequenceId = UNDEFINED_CONSUMER_SEQUENCE_ID;
          break;
        case VERSION_V3:
          factoryClassName = in.readUTF();
          this.pubSubPosition = PubSubPositionDeserializer.deserializePubSubPosition(positionWf, factoryClassName);
          // v3: consumerSequenceId follows the pubSubPosition factory class
          this.consumerSequenceId = in.readLong();
          break;

        default:
          // Future version not supported
          throw new VeniceException("Unsupported VeniceChangeCoordinate version: " + version);
      }
      LOGGER.debug(
          "Deserialized VeniceChangeCoordinate from {} format: topic-partition: {}, position: {}",
          version,
          Utils.getReplicaId(topic, partition),
          pubSubPosition);
    } catch (IOException e) {
      if (!REDUNDANT_LOGGING_FILTER.isRedundantException(e.getMessage())) {
        LOGGER.warn("Falling back to v1 deserialization due to error: {}", e.toString());
      }
      // Legacy fallback path: version field was not present (v1 format)
      this.pubSubPosition = (pubSubPositionDeserializer != null)
          ? pubSubPositionDeserializer.toPosition(positionWf)
          : PubSubPositionDeserializer.DEFAULT_DESERIALIZER.toPosition(positionWf);
      this.consumerSequenceId = UNDEFINED_CONSUMER_SEQUENCE_ID;
      LOGGER.debug(
          "Deserialized VeniceChangeCoordinate from v1 format: topic-partition: {}, position: {}",
          Utils.getReplicaId(topic, partition),
          pubSubPosition);
    }
  }

  // Partition and store name can be publicly accessible
  public Integer getPartition() {
    return partition;
  }

  public String getStoreName() {
    return Version.parseStoreFromKafkaTopicName(topic);
  }

  /**
   * Returns the consumer sequence id for this {@link VeniceChangeCoordinate}.
   * The sequence id is unique and monotonically increasing per record polled
   * from the same consumer instance and partition. This can be used to reason
   * about the order of events consumed from the same consumer and partition.
   * The sequence id is mostly contiguous, but it is not guaranteed because there
   * can be messages that are consumed internally (sequence id is already incremented)
   * but not processed by the external consumer yet. In most cases it's good enough to
   * provide some heuristic about the rate of consumption or number of events consumed,
   * but it's not meant for precise measurement.
   *
   * @return the consumer sequence id, or {@link #UNDEFINED_CONSUMER_SEQUENCE_ID} if not available.
   */
  public long getConsumerSequenceId() {
    return consumerSequenceId;
  }

  /**
   * @param other the other position to compare to
   * @return returns 0 if the positions are equal,
   *         -1 if this position is less than the other position,
   *          and 1 if this position is greater than the other position.
   *          You should only compare positions from the same partition and consumer instance.
   */
  public int comparePosition(VeniceChangeCoordinate other) {
    if (!Objects.equals(other.partition, partition)) {
      throw new VeniceException("Coordinates from different partitions are not comparable!");
    }
    if (consumerSequenceId != UNDEFINED_CONSUMER_SEQUENCE_ID
        && other.consumerSequenceId != UNDEFINED_CONSUMER_SEQUENCE_ID) {
      return Long.compare(consumerSequenceId, other.consumerSequenceId);
    }
    if (topic.compareTo(other.topic) != 0) {
      // TODO: This works for cases where the version number increases and we traverse between CC and version topics
      // but it DOESNT account for version rollbacks. This should be removed once consumer sequence id is everywhere.
      return topic.compareTo(other.topic);
    }
    return PubSubUtil.comparePubSubPositions(pubSubPosition, other.pubSubPosition);
  }

  // These methods contain 'need to know' information and expose underlying details.
  protected String getTopic() {
    return topic;
  }

  protected PubSubPosition getPosition() {
    return pubSubPosition;
  }

  protected void setPubSubPositionDeserializer(PubSubPositionDeserializer pubSubPositionDeserializer) {
    this.pubSubPositionDeserializer = pubSubPositionDeserializer;
  }

  protected VeniceChangeCoordinate(String topic, PubSubPosition pubSubPosition, Integer partition) {
    this(topic, pubSubPosition, partition, VeniceChangeCoordinate.UNDEFINED_CONSUMER_SEQUENCE_ID);
  }

  protected VeniceChangeCoordinate(
      String topic,
      PubSubPosition pubSubPosition,
      Integer partition,
      long consumerSequenceId) {
    this.partition = partition;
    this.topic = topic;
    this.pubSubPosition = pubSubPosition;
    this.consumerSequenceId = consumerSequenceId;
  }

  public static String convertVeniceChangeCoordinateToStringAndEncode(VeniceChangeCoordinate veniceChangeCoordinate)
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream)) {
      veniceChangeCoordinate.writeExternal(outputStream);
      outputStream.flush();
      byte[] data = byteArrayOutputStream.toByteArray();
      return Base64.getEncoder().encodeToString(data);
    }
  }

  public static VeniceChangeCoordinate decodeStringAndConvertToVeniceChangeCoordinate(
      PubSubPositionDeserializer deserializer,
      String offsetString) throws IOException, ClassNotFoundException {
    byte[] newData = Base64.getDecoder().decode(offsetString);
    ByteArrayInputStream inMemoryInputStream = new ByteArrayInputStream(newData);
    ObjectInputStream objectInputStream = new ObjectInputStream(inMemoryInputStream);
    VeniceChangeCoordinate restoredCoordinate = new VeniceChangeCoordinate();
    restoredCoordinate.setPubSubPositionDeserializer(deserializer);
    restoredCoordinate.readExternal(objectInputStream);
    return restoredCoordinate;
  }

  @Override
  public String toString() {
    return "VeniceChangeCoordinate{topic-partition=" + Utils.getReplicaId(topic, partition) + ", pubSubPosition="
        + pubSubPosition + '}';
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    VeniceChangeCoordinate that = (VeniceChangeCoordinate) obj;
    return Objects.equals(topic, that.topic) && Objects.equals(partition, that.partition)
        && Objects.equals(pubSubPosition, that.pubSubPosition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, partition, pubSubPosition);
  }
}
