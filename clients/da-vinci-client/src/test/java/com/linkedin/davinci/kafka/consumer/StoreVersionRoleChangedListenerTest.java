package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.utils.Utils;
import org.testng.annotations.Test;


public class StoreVersionRoleChangedListenerTest {
  @Test
  public void testForVersionRoleChangeToTriggerResubscribe() {

    PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
    StoreIngestionTask storeIngestionTask = mock(StoreIngestionTask.class);
    String storeName = Utils.getUniqueString("storeName");
    int versionNumber = 1;
    PubSubTopic versionTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, versionNumber));
    Store store = mock(Store.class);
    doReturn(versionTopic).when(storeIngestionTask).getVersionTopic();
    doReturn(versionNumber).when(store).getCurrentVersion();
    StoreVersionRoleChangedListener storeVersionRoleChangedListener =
        new StoreVersionRoleChangedListener(storeIngestionTask, store, versionNumber);

    doReturn(storeName).when(store).getName();
    doReturn(2).when(store).getCurrentVersion();
    storeVersionRoleChangedListener.handleStoreChanged(store);
    verify(storeIngestionTask).versionRoleChangeToTriggerResubscribe(TopicPartitionReplicaRole.VersionRole.BACKUP);

  }
}
