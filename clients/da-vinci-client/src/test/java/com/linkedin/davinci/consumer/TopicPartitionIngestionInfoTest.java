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
    PubSubTopic versionTopic = pubSubTopicRepository.getTopic("test_store_v1");
    TopicPartitionIngestionInfo topicPartitionIngestionInfo =
        new TopicPartitionIngestionInfo(0, 1, 2.0, 4.0, "consumerIdStr", 7, 8, versionTopic.getName());
    String kafkaUrl = "localhost:1234";
    PubSubTopicPartition pubSubTopicPartition = new PubSubTopicPartitionImpl(versionTopic, 0);
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

  @Test
  public void testElapsedTimeSinceLastPolledRecordsInMs() {
    long elapsedTimeSinceLastPollInMs = 100;
    long elapsedTimeSinceLastPolledRecordsInMs = 200;
    TopicPartitionIngestionInfo info = new TopicPartitionIngestionInfo(
        0,
        1,
        2.0,
        4.0,
        "testConsumer",
        elapsedTimeSinceLastPollInMs,
        elapsedTimeSinceLastPolledRecordsInMs,
        "test_store_v1");

    Assert.assertEquals(info.getElapsedTimeSinceLastPollInMs(), elapsedTimeSinceLastPollInMs);
    Assert.assertEquals(info.getElapsedTimeSinceLastPolledRecordsInMs(), elapsedTimeSinceLastPolledRecordsInMs);

    String infoString = info.toString();
    Assert.assertTrue(
        infoString.contains("elapsedTimeSinceLastPolledRecordsInMs:" + elapsedTimeSinceLastPolledRecordsInMs));

    TopicPartitionIngestionInfo sameInfo = new TopicPartitionIngestionInfo(
        0,
        1,
        2.0,
        4.0,
        "testConsumer",
        elapsedTimeSinceLastPollInMs,
        elapsedTimeSinceLastPolledRecordsInMs,
        "test_store_v1");
    Assert.assertEquals(info, sameInfo);

    TopicPartitionIngestionInfo differentInfo = new TopicPartitionIngestionInfo(
        0,
        1,
        2.0,
        4.0,
        "testConsumer",
        elapsedTimeSinceLastPollInMs,
        300,
        "test_store_v1");
    Assert.assertNotEquals(info, differentInfo);
  }
}
