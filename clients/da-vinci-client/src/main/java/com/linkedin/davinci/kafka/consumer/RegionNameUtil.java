package com.linkedin.davinci.kafka.consumer;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;


public class RegionNameUtil {
  public static String getRegionName(PubSubMessage message, Int2ObjectMap<String> regionIdToNameMap) {
    if (message == null || message.getValue() == null || regionIdToNameMap == null || regionIdToNameMap.isEmpty()) {
      return "";
    }
    KafkaMessageEnvelope envelope = (KafkaMessageEnvelope) message.getValue();
    if (envelope.getLeaderMetadataFooter() == null
        || envelope.getLeaderMetadataFooter().getUpstreamKafkaClusterId() == -1) {
      return "";
    }
    return regionIdToNameMap.getOrDefault(envelope.getLeaderMetadataFooter().getUpstreamKafkaClusterId(), "");
  }
}
