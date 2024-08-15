package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.Objects;


/**
 * This class is a wrapper of pair of <Version Topic, Version /Real time topic partition>. As for a unique topic partition
 * to be ingested, We need both version topic and pub-sub topic partition to identify. For example, the server host may
 * ingestion same {#linkPubSubTopicType.REALTIME_TOPIC} partition from different {#link PubSubTopicType.VERSION_TOPIC}s,
 * and we should avoid sharing the same consumer for them.
 * TODO： Modify the Consumer Service to use this class instead of using version topic and pub-sub topic partition separately.
 */
public class TopicPartitionForIngestion {
  private final PubSubTopic versionTopic;
  private final PubSubTopicPartition pubSubTopicPartition;

  public TopicPartitionForIngestion(PubSubTopic versionTopic, PubSubTopicPartition pubSubTopicPartition) {
    this.versionTopic = versionTopic;
    this.pubSubTopicPartition = pubSubTopicPartition;
  }

  public PubSubTopic getVersionTopic() {
    return versionTopic;
  }

  public PubSubTopicPartition getPubSubTopicPartition() {
    return pubSubTopicPartition;
  }

  @Override
  public final int hashCode() {
    return Objects.hash(versionTopic, pubSubTopicPartition);
  }

  @Override
  public final boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (!(obj instanceof TopicPartitionForIngestion)) {
      return false;
    }

    final TopicPartitionForIngestion other = (TopicPartitionForIngestion) (obj);
    return Objects.equals(versionTopic, other.versionTopic)
        && Objects.equals(pubSubTopicPartition, other.pubSubTopicPartition);
  }
}
