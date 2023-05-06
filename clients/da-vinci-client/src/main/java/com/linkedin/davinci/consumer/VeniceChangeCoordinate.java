package com.linkedin.davinci.consumer;

import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class VeniceChangeCoordinate implements Externalizable {
  private static final long serialVersionUID = 1L;

  private String topic;
  private Integer partition;
  private PubSubPosition pubSubPosition;

  public VeniceChangeCoordinate() {
    // Empty constructor is public
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

  // These methods contain 'need to know' information and expose underlying details.
  protected String getTopic() {
    return topic;
  }

  protected PubSubPosition getPosition() {
    return pubSubPosition;
  }

  protected VeniceChangeCoordinate(String topic, PubSubPosition pubSubPosition, Integer partition) {
    this.partition = partition;
    this.topic = topic;
    this.pubSubPosition = pubSubPosition;
  }
}
