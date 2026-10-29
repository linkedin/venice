package com.linkedin.venice.pubsub;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class PubSubContextTest {
  @Test
  public void testBuilderAndGetters() {
    TopicManagerRepository mockRepo = Mockito.mock(TopicManagerRepository.class);
    PubSubPositionTypeRegistry mockRegistry = Mockito.mock(PubSubPositionTypeRegistry.class);
    PubSubPositionDeserializer mockDeserializer = Mockito.mock(PubSubPositionDeserializer.class);
    PubSubTopicRepository mockTopicRepo = Mockito.mock(PubSubTopicRepository.class);
    AtomicReference<String> keyUrn = new AtomicReference<>();
    Function<String, String> keyLookup = storeName -> keyUrn.get();

    PubSubContext context = new PubSubContext.Builder().setTopicManagerRepository(mockRepo)
        .setPubSubPositionTypeRegistry(mockRegistry)
        .setPubSubPositionDeserializer(mockDeserializer)
        .setPubSubTopicRepository(mockTopicRepo)
        .setPubSubEncryptionKeyUrnLookup(keyLookup)
        .build();

    assertSame(context.getTopicManagerRepository(), mockRepo);
    assertSame(context.getPubSubPositionTypeRegistry(), mockRegistry);
    assertSame(context.getPubSubPositionDeserializer(), mockDeserializer);
    assertSame(context.getPubSubTopicRepository(), mockTopicRepo);
    assertSame(context.getPubSubEncryptionKeyUrnLookup(), keyLookup);
    assertNull(context.getPubSubEncryptionKeyUrnLookup().apply("store"));
    keyUrn.set("urn:test:key:1");
    assertEquals(context.getPubSubEncryptionKeyUrnLookup().apply("store"), "urn:test:key:1");
  }

  @Test
  public void testEncryptionKeyLookupIsOptional() {
    assertNull(new PubSubContext.Builder().build().getPubSubEncryptionKeyUrnLookup());
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
