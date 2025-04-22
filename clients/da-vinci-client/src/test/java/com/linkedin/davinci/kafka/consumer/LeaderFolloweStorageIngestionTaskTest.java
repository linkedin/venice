package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.davinci.kafka.consumer.LeaderFollowerStateType.LEADER;
import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.utils.Utils;
import java.lang.reflect.Field;
import org.mockito.Answers;
import org.testng.Assert;
import org.testng.annotations.Test;


public class LeaderFolloweStorageIngestionTaskTest {
  PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();
  String store = "test_store";

  @Test
  public void test() throws NoSuchFieldException, IllegalAccessException {
    PubSubTopic realTimeTopic = pubSubTopicRepository.getTopic(Utils.composeRealTimeTopic(store));
    PubSubTopic versionTopic = pubSubTopicRepository.getTopic(store + Version.VERSION_SEPARATOR + "1");
    PartitionConsumptionState partitionConsumptionState =
        mock(PartitionConsumptionState.class, Answers.RETURNS_DEEP_STUBS);
    doReturn(LEADER).when(partitionConsumptionState).getLeaderFollowerState();

    LeaderFollowerStoreIngestionTask leaderFollowerStoreIngestionTask =
        mock(LeaderFollowerStoreIngestionTask.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    doCallRealMethod().when(leaderFollowerStoreIngestionTask).shouldCompressData(any());
    Field field = leaderFollowerStoreIngestionTask.getClass().getSuperclass().getDeclaredField("compressionStrategy");
    field.setAccessible(true);
    field.set(leaderFollowerStoreIngestionTask, CompressionStrategy.ZSTD_WITH_DICT);

    // test 1 - do compress for real time topic
    field = leaderFollowerStoreIngestionTask.getClass().getSuperclass().getDeclaredField("realTimeTopic");
    field.setAccessible(true);
    field.set(leaderFollowerStoreIngestionTask, realTimeTopic);
    when(partitionConsumptionState.getOffsetRecord().getLeaderTopic(any())).thenReturn(realTimeTopic);
    Assert.assertTrue(leaderFollowerStoreIngestionTask.shouldCompressData(partitionConsumptionState));

    // test 2 - do not compress for version topic
    field = leaderFollowerStoreIngestionTask.getClass().getSuperclass().getDeclaredField("realTimeTopic");
    field.setAccessible(true);
    field.set(leaderFollowerStoreIngestionTask, null);
    when(partitionConsumptionState.getOffsetRecord().getLeaderTopic(any())).thenReturn(versionTopic);
    Assert.assertFalse(leaderFollowerStoreIngestionTask.shouldCompressData(partitionConsumptionState));
  }
}
