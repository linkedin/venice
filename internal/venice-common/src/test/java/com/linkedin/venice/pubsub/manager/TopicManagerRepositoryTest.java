package com.linkedin.venice.pubsub.manager;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.CompletableFuture;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link TopicManagerRepository}.
 */
public class TopicManagerRepositoryTest {
  @Test
  public void testGetTopicManagerIsCreatedIfNotExists() {
    // Use spy to return dummy TopicManager form createTopicManager
    TopicManagerContext topicManagerContext = mock(TopicManagerContext.class);
    String localPubSubAddress = "local.example.com:9092";
    String remotePubSubAddress = "remote.example.com:9092";

    TopicManager localTopicManager = mock(TopicManager.class);
    TopicManager remoteTopicManager = mock(TopicManager.class);

    TopicManagerRepository topicManagerRepositorySpy =
        spy(new TopicManagerRepository(topicManagerContext, localPubSubAddress));

    doReturn(localTopicManager).when(topicManagerRepositorySpy).createTopicManager(localPubSubAddress);
    doReturn(remoteTopicManager).when(topicManagerRepositorySpy).createTopicManager(remotePubSubAddress);

    // get all topic managers should return empty list
    assertEquals(topicManagerRepositorySpy.getAllTopicManagers().size(), 0);

    // let's call getTopicManager several times for each address
    for (int i = 0; i < 5; i++) {
      assertEquals(topicManagerRepositorySpy.getLocalTopicManager(), localTopicManager);
      assertEquals(topicManagerRepositorySpy.getTopicManager(localPubSubAddress), localTopicManager);
      assertEquals(topicManagerRepositorySpy.getTopicManager(remotePubSubAddress), remoteTopicManager);
    }

    // verify that createTopicManager was called only once for each address
    verify(topicManagerRepositorySpy).createTopicManager(localPubSubAddress);
    verify(topicManagerRepositorySpy).createTopicManager(remotePubSubAddress);
    verify(topicManagerRepositorySpy, never()).createTopicManager("some.other.address:9092");

    // getAllTopicManagers should return both topic managers
    assertEquals(topicManagerRepositorySpy.getAllTopicManagers().size(), 2);
    assertTrue(topicManagerRepositorySpy.getAllTopicManagers().contains(localTopicManager));
    assertTrue(topicManagerRepositorySpy.getAllTopicManagers().contains(remoteTopicManager));

    // test close
    when(localTopicManager.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
    when(remoteTopicManager.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
    topicManagerRepositorySpy.close();
    verify(localTopicManager).closeAsync();
    verify(remoteTopicManager).closeAsync();
  }
}
