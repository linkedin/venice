package com.linkedin.venice.pubsub;

import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicType;
import java.util.Objects;


public class PubSubTopicImpl implements PubSubTopic {
  private final String name;
  private final PubSubTopicType pubSubTopicType;
  private final String storeName;

  /**
   * N.B. leaving this constructor package-private, as the intent is not necessarily the instantiate this object
   * from anywhere in the code. Most likely, it would be preferable to have some form of singleton topic-registry where
   * instances can be gotten from, so that the internal state is computed just once, and then reused for the lifetime
   * of the topic.
   */
  PubSubTopicImpl(String name) {
    this.name = Objects.requireNonNull(name, "Topic name cannot be null");
    this.pubSubTopicType = PubSubTopicType.getPubSubTopicType(name);
    this.storeName = Version.parseStoreFromKafkaTopicName(name);
  }

  @Override
  public String getName() {
    return name;
  }

  public PubSubTopicType getPubSubTopicType() {
    return pubSubTopicType;
  }

  @Override
  public String getStoreName() {
    return storeName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PubSubTopic)) {
      return false;
    }
    PubSubTopic that = (PubSubTopic) o;
    return name.equals(that.getName());
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public String toString() {
    return this.name;
  }
}
