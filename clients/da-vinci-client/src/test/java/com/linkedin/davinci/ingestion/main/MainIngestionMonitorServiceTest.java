package com.linkedin.davinci.ingestion.main;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MainIngestionMonitorServiceTest {
  @Test
  public void testRecoverOngoingIngestionTask() {
    Map<String, MainTopicIngestionStatus> topicIngestionStatusMap = new VeniceConcurrentHashMap<>();
    String topic = "topic1";
    MainTopicIngestionStatus mainTopicIngestionStatus = new MainTopicIngestionStatus(topic);
    mainTopicIngestionStatus.setPartitionIngestionStatusToLocalIngestion(0);
    mainTopicIngestionStatus.setPartitionIngestionStatusToIsolatedIngestion(1);
    mainTopicIngestionStatus.setPartitionIngestionStatusToIsolatedIngestion(2);
    mainTopicIngestionStatus.setPartitionIngestionStatusToIsolatedIngestion(3);
    topicIngestionStatusMap.put(topic, mainTopicIngestionStatus);
    Map<String, Map<Integer, Boolean>> topicPartitionLeaderStatusMap = new VeniceConcurrentHashMap<>();
    topicPartitionLeaderStatusMap.put(topic, Collections.singletonMap(3, Boolean.TRUE));
    MainIngestionRequestClient client = mock(MainIngestionRequestClient.class);
    when(client.startConsumption(topic, 0))
        .thenThrow(new VeniceException("Not expected to start remote consumption on local resource"));
    when(client.startConsumption(topic, 1))
        .thenThrow(new VeniceException("Simulate zombie resource ingestion failure"));
    when(client.startConsumption(topic, 2)).thenReturn(Boolean.TRUE);
    when(client.startConsumption(topic, 3)).thenReturn(Boolean.TRUE);
    when(client.promoteToLeader(topic, 3)).thenReturn(Boolean.TRUE);

    MainIngestionMonitorService monitorService = mock(MainIngestionMonitorService.class);
    when(monitorService.getTopicIngestionStatusMap()).thenReturn(topicIngestionStatusMap);
    when(monitorService.getTopicPartitionLeaderStatusMap()).thenReturn(topicPartitionLeaderStatusMap);

    ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    when(monitorService.createClient()).thenReturn(client);
    when(monitorService.resumeOngoingIngestionTasks()).thenCallRealMethod();
    when(monitorService.isTopicPartitionInLeaderState(anyString(), anyInt())).thenCallRealMethod();
    when(monitorService.getForkProcessActionLock()).thenReturn(readWriteLock);
    Assert.assertTrue(monitorService.isTopicPartitionInLeaderState(topic, 3));
    Assert.assertEquals(monitorService.resumeOngoingIngestionTasks(), 2);
    verify(client, times(0)).promoteToLeader(topic, 2);
    verify(client, times(1)).promoteToLeader(topic, 3);

  }
}
