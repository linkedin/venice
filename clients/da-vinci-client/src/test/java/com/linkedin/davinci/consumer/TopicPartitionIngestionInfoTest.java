package com.linkedin.davinci.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.linkedin.davinci.kafka.consumer.TopicPartitionIngestionInfo;
import com.linkedin.venice.helix.VeniceJsonSerializer;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TopicPartitionIngestionInfoTest {
  PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @Test
  public void testJsonParse() throws Exception {
    TopicPartitionIngestionInfo topicPartitionIngestionInfo = new TopicPartitionIngestionInfo(0, 1, 2.0, 4.0, 5, 7);
    PubSubTopic pubSubTopic = pubSubTopicRepository.getTopic("test_store_v1");
    String kafkaUrl = "localhost:1234";
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(pubSubTopic, 0);
    Map<String, Map<String, TopicPartitionIngestionInfo>> topicPartitionIngestionContext = new HashMap<>();
    topicPartitionIngestionContext.computeIfAbsent(kafkaUrl, k -> new HashMap<>())
        .put(pubSubTopicPartition.toString(), topicPartitionIngestionInfo);
    VeniceJsonSerializer<Map<String, Map<String, TopicPartitionIngestionInfo>>> veniceJsonSerializer =
        new VeniceJsonSerializer<>(new TypeReference<Map<String, Map<String, TopicPartitionIngestionInfo>>>() {
        });
    byte[] jsonOutput = veniceJsonSerializer.serialize(topicPartitionIngestionContext, "");
    Map<String, Map<String, TopicPartitionIngestionInfo>> topicPartitionIngestionContexts =
        veniceJsonSerializer.deserialize(jsonOutput, "");
    Assert.assertEquals(
        topicPartitionIngestionContexts.get(kafkaUrl).get(pubSubTopicPartition.toString()),
        topicPartitionIngestionInfo);
  }
}
