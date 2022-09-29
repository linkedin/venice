package com.linkedin.venice.pubsub.kafka;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicType;


public class KafkaPubSubTopic implements PubSubTopic {
  private final String name;
  private final PubSubTopicType pubSubTopicType;
  private final String storeName;

  /**
   * N.B. leaving this constructor package-private, as the intent is not necessarily the instantiate this object
   * from anywhere in the code. Most likely, it would be preferable to have some form of singleton topic-registry where
   * instances can be gotten from, so that the internal state is computed just once, and then reused for the lifetime
   * of the topic.
   */
  KafkaPubSubTopic(String name) {
    this.name = name;
    if (Version.isRealTimeTopic(name)) {
      pubSubTopicType = PubSubTopicType.REALTIME_TOPIC;
    } else if (Version.isStreamReprocessingTopic(name)) {
      pubSubTopicType = PubSubTopicType.REPROCESSING_TOPIC;
    } else if (Version.isVersionTopic(name)) {
      pubSubTopicType = PubSubTopicType.VERSION_TOPIC;
    } else {
      throw new VeniceException("Unsupported topic type for: " + name);
    }
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
}
