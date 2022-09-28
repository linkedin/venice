package com.linkedin.venice.pubsub.kafka;

import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubTopic;


public class KafkaPubSubTopic implements PubSubTopic {
  private final String name;
  private final boolean isRealTimeTopic;
  private final boolean isVersionTopic;
  private final boolean isReprocessingTopic;
  private final String storeName;

  /**
   * N.B. leaving this constructor package-private, as the intent is not necessarily the instantiate this object
   * from anywhere in the code. Most likely, it would be preferable to have some form of singleton topic-registry where
   * instances can be gotten from, so that the internal state is computed just once, and then reused for the lifetime
   * of the topic.
   */
  KafkaPubSubTopic(String name) {
    this.name = name;
    this.isRealTimeTopic = Version.isRealTimeTopic(name);
    this.isVersionTopic = Version.isVersionTopic(name);
    this.isReprocessingTopic = Version.isStreamReprocessingTopic(name);
    this.storeName = Version.parseStoreFromKafkaTopicName(name);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean isRealTimeTopic() {
    return isRealTimeTopic;
  }

  @Override
  public boolean isVersionTopic() {
    return isVersionTopic;
  }

  @Override
  public boolean isReprocessingTopic() {
    return isReprocessingTopic;
  }

  @Override
  public String getStoreName() {
    return storeName;
  }
}
