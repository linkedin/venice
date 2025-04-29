package com.linkedin.davinci.consumer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
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


public class VeniceChangeCoordinate implements Externalizable {
  private static final long serialVersionUID = 1L;

  private String topic;
  private Integer partition;
  private PubSubPosition pubSubPosition;
  private transient PubSubPositionDeserializer pubSubPositionDeserializer;

  public VeniceChangeCoordinate() {
    // Empty constructor is public for Externalizable
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeUTF(topic);
    out.writeInt(partition);
    out.writeObject(pubSubPosition.getPositionWireFormat());
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.topic = in.readUTF();
    this.partition = in.readInt();
    this.pubSubPosition = PubSubPosition.getPositionFromWireFormat((PubSubPositionWireFormat) in.readObject());
  }

  // Partition and store name can be publicly accessible
  public Integer getPartition() {
    return partition;
  }

  public String getStoreName() {
    return Version.parseStoreFromKafkaTopicName(topic);
  }

  /**
   * @param other the other position to compare to
   * @return returns 0 if the positions are equal,
   *         -1 if this position is less than the other position,
   *          and 1 if this position is greater than the other position
   */
  public int comparePosition(VeniceChangeCoordinate other) {
    if (!Objects.equals(other.partition, partition)) {
      throw new VeniceException("Coordinates from different partitions are not comparable!");
    }
    if (topic.compareTo(other.topic) != 0) {
      // TODO: This works for cases where the version number increases and we traverse between CC and version topics
      // but it DOESNT account for version rollbacks.
      return topic.compareTo(other.topic);
    }
    return pubSubPosition.comparePosition(other.pubSubPosition);
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
    this.partition = partition;
    this.topic = topic;
    this.pubSubPosition = pubSubPosition;
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
}
