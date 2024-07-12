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
    doReturn(TopicPartitionReplicaRole.VersionRole.FUTURE).when(storeIngestionTask).getStoreVersionRole();
    String storeName = Utils.getUniqueString("storeName");
    PubSubTopic versionTopic = pubSubTopicRepository.getTopic(Version.composeKafkaTopic(storeName, 1));
    Store store = mock(Store.class);
    doReturn(versionTopic).when(storeIngestionTask).getVersionTopic();
    StoreVersionRoleChangedListener storeVersionRoleChangedListener =
        new StoreVersionRoleChangedListener(storeIngestionTask);

    doReturn(storeName).when(store).getName();
    doReturn(TopicPartitionReplicaRole.VersionRole.CURRENT).when(storeIngestionTask).getStoreVersionRole();
    storeVersionRoleChangedListener.handleStoreChanged(store);
    verify(storeIngestionTask).versionRoleChangeToTriggerResubscribe();

  }
}
