package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.VersionImpl;
import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TopicExistenceCheckerTest {
  @Test
  public void testMetadataBasedTopicExistenceChecker() {
    String exitingTopic1 = "existingTopic_v123";
    String exitingTopic2 = "existingTopic_rt";
    String exitingTopic3 = "existingTopic_v123_sr";

    String nontExitingTopic1 = "non-existingTopic_v123";
    String nontExitingTopic2 = "non-existingTopic_rt";

    ReadOnlyStoreRepository repository = mock(ReadOnlyStoreRepository.class);
    Store store = mock(Store.class);
    doReturn(Optional.of(new VersionImpl("existingTopic", 123))).when(store).getVersion(123);
    doReturn(store).when(repository).getStoreOrThrow("existingTopic");
    doThrow(new VeniceNoStoreException(nontExitingTopic1)).when(repository).getStoreOrThrow("non-existingTopic");
    doReturn(true).when(store).isHybrid();
    MetadataRepoBasedTopicExistingCheckerImpl topicExistingChecker =
        new MetadataRepoBasedTopicExistingCheckerImpl(repository);

    Assert.assertTrue(topicExistingChecker.checkTopicExists(exitingTopic1));
    Assert.assertTrue(topicExistingChecker.checkTopicExists(exitingTopic2));
    Assert.assertTrue(topicExistingChecker.checkTopicExists(exitingTopic3));
    Assert.assertFalse(topicExistingChecker.checkTopicExists(nontExitingTopic1));
    Assert.assertFalse(topicExistingChecker.checkTopicExists(nontExitingTopic2));
  }
}
