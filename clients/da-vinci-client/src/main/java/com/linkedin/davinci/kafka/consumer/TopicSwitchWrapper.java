package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.kafka.protocol.TopicSwitch;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import io.tehuti.utils.Utils;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * This class precomputes the conversion from Avro's {@link CharSequence} to String, and also hangs on to the
 * correct instance of {@link PubSubTopic}, so that the lookup need not be performed in many locations.
 */
public class TopicSwitchWrapper {
  private final TopicSwitch topicSwitch;
  private final PubSubTopic newSourceTopic;
  private final Set<String> sourceServers;

  public TopicSwitchWrapper(TopicSwitch topicSwitch, PubSubTopic newSourceTopic) {
    this.topicSwitch = Utils.notNull(topicSwitch);
    this.newSourceTopic = newSourceTopic;
    this.sourceServers = (topicSwitch.sourceKafkaServers == null || topicSwitch.sourceKafkaServers.isEmpty())
        ? Collections.emptySet()
        : topicSwitch.sourceKafkaServers.stream().map(CharSequence::toString).collect(Collectors.toSet());
  }

  public TopicSwitch getTopicSwitch() {
    return topicSwitch;
  }

  public PubSubTopic getNewSourceTopic() {
    return newSourceTopic;
  }

  public Set<String> getSourceServers() {
    return sourceServers;
  }
}
