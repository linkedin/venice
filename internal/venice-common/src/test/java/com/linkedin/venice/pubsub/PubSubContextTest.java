package com.linkedin.venice.pubsub;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;

import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class PubSubContextTest {
  @Test
  public void testBuilderAndGetters() {
    TopicManagerRepository mockRepo = Mockito.mock(TopicManagerRepository.class);
    PubSubPositionTypeRegistry mockRegistry = Mockito.mock(PubSubPositionTypeRegistry.class);
    PubSubPositionDeserializer mockDeserializer = Mockito.mock(PubSubPositionDeserializer.class);
    PubSubTopicRepository mockTopicRepo = Mockito.mock(PubSubTopicRepository.class);

    PubSubContext context = new PubSubContext.Builder().setTopicManagerRepository(mockRepo)
        .setPubSubPositionTypeRegistry(mockRegistry)
        .setPubSubPositionDeserializer(mockDeserializer)
        .setPubSubTopicRepository(mockTopicRepo)
        .build();

    assertSame(context.getTopicManagerRepository(), mockRepo);
    assertSame(context.getPubSubPositionTypeRegistry(), mockRegistry);
    assertSame(context.getPubSubPositionDeserializer(), mockDeserializer);
    assertSame(context.getPubSubTopicRepository(), mockTopicRepo);
  }

  @Test
  public void testGetTopicManager() {
    String topicName = "test-topic";
    TopicManager mockTopicManager = Mockito.mock(TopicManager.class);
    TopicManagerRepository mockRepo = Mockito.mock(TopicManagerRepository.class);
    Mockito.when(mockRepo.getTopicManager(topicName)).thenReturn(mockTopicManager);

    PubSubContext context = new PubSubContext.Builder().setTopicManagerRepository(mockRepo).build();

    TopicManager result = context.getTopicManager(topicName);
    assertNotNull(result);
    assertEquals(result, mockTopicManager);
    Mockito.verify(mockRepo, Mockito.times(1)).getTopicManager(topicName);
  }
}
