package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import java.util.ArrayList;
import java.util.List;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class StaleTopicCheckerTest {
  @Test
  public void testMetadataBasedTopicExistenceChecker() {
    String existingTopic1 = "existingTopic_v123";
    String existingTopic2 = "existingTopic_v123_sr";

    String nonExistingTopic = "non-existingTopic_v123";

    ReadOnlyStoreRepository repository = mock(ReadOnlyStoreRepository.class);
    Store store = mock(Store.class);
    doReturn(new VersionImpl("existingTopic", 123, "existingTopic" + Version.REAL_TIME_TOPIC_SUFFIX)).when(store)
        .getVersion(123);
    doReturn(store).when(repository).getStoreOrThrow("existingTopic");
    doThrow(new VeniceNoStoreException(nonExistingTopic)).when(repository).getStoreOrThrow("non-existingTopic");
    doReturn(true).when(store).isHybrid();
    MetadataRepoBasedStaleTopicCheckerImpl staleTopicChecker = new MetadataRepoBasedStaleTopicCheckerImpl(repository);

    Assert.assertTrue(staleTopicChecker.shouldTopicExist(existingTopic1));
    Assert.assertTrue(staleTopicChecker.shouldTopicExist(existingTopic2));
    Assert.assertFalse(staleTopicChecker.shouldTopicExist(nonExistingTopic));
  }

  @Test
  public void testRealtimeTopicChecker() {
    String realtimeTopic = "storeA_rt";
    ReadOnlyStoreRepository repository = mock(ReadOnlyStoreRepository.class);
    Store store = mock(Store.class);
    doReturn(store).when(repository).getStoreOrThrow("storeA");
    MetadataRepoBasedStaleTopicCheckerImpl staleTopicChecker = new MetadataRepoBasedStaleTopicCheckerImpl(repository);

    try (MockedStatic<Version> versionMockedStatic = Mockito.mockStatic(Version.class)) {
      versionMockedStatic.when(() -> Version.isRealTimeTopic(realtimeTopic)).thenReturn(true);
      versionMockedStatic.when(() -> Version.parseStoreFromKafkaTopicName(realtimeTopic)).thenReturn("storeA");
      doReturn(false).when(store).isHybrid();
      List<Version> versions = new ArrayList<>();
      Version version = mock(Version.class);
      versions.add(version);
      doReturn(versions).when(store).getVersions();

      // When hybrid version is not present, the topic should not exist - for both hybrid and non-hybrid stores
      versionMockedStatic.when(() -> Version.containsHybridVersion(versions)).thenReturn(false);
      Assert.assertFalse(staleTopicChecker.shouldTopicExist(realtimeTopic));

      // When hybrid version is present, the topic should exist
      versionMockedStatic.when(() -> Version.containsHybridVersion(versions)).thenReturn(true);
      Assert.assertTrue(staleTopicChecker.shouldTopicExist(realtimeTopic));
    }
  }
}
