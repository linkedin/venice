package com.linkedin.davinci.ingestion.isolated;

import com.linkedin.davinci.ingestion.utils.IsolatedIngestionUtils;
import com.linkedin.venice.ingestion.protocol.IngestionTaskReport;
import com.linkedin.venice.ingestion.protocol.enums.IngestionCommandType;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IsolatedIngestionServerHandlerTest {
  @Test
  public void testHandlerCheckCommandWithSubscription() {
    String topic = "testTopic_v1";
    int partitionId = 2;

    Map<String, Map<Integer, AtomicBoolean>> topicPartitionSubscriptionMap = new VeniceConcurrentHashMap<>();
    IsolatedIngestionServer mockedServer = Mockito.mock(IsolatedIngestionServer.class);
    Mockito.when(mockedServer.isResourceSubscribed(topic, partitionId)).thenCallRealMethod();
    Mockito.when(mockedServer.getTopicPartitionSubscriptionMap()).thenReturn(topicPartitionSubscriptionMap);
    IsolatedIngestionServerHandler isolatedIngestionServerHandler = new IsolatedIngestionServerHandler(mockedServer);
    IngestionTaskReport ingestionTaskReport = IsolatedIngestionUtils.createIngestionTaskReport(topic, partitionId);
    AtomicInteger executionCount = new AtomicInteger(0);
    IngestionCommandType command = IngestionCommandType.REMOVE_PARTITION;
    // Validate that a non-subscribed topic partition should reject the command.
    isolatedIngestionServerHandler
        .validateAndExecuteCommand(command, ingestionTaskReport, () -> executionCount.addAndGet(1));
    Assert.assertEquals(executionCount.get(), 0);
    Assert.assertFalse(ingestionTaskReport.isPositive);

    // Validate that a ready-to-serve topic partition should reject the command.
    ingestionTaskReport.setIsPositive(true);
    Map<Integer, AtomicBoolean> topicMap = new VeniceConcurrentHashMap<>();
    topicMap.put(partitionId, new AtomicBoolean(false));
    topicPartitionSubscriptionMap.put(topic, topicMap);
    isolatedIngestionServerHandler
        .validateAndExecuteCommand(command, ingestionTaskReport, () -> executionCount.addAndGet(1));
    Assert.assertEquals(executionCount.get(), 0);
    Assert.assertFalse(ingestionTaskReport.isPositive);

    // Validate that an active topic partition subscription should execute the command.
    ingestionTaskReport.setIsPositive(true);
    topicPartitionSubscriptionMap.get(topic).get(partitionId).set(true);
    isolatedIngestionServerHandler
        .validateAndExecuteCommand(command, ingestionTaskReport, () -> executionCount.addAndGet(1));
    Assert.assertEquals(executionCount.get(), 1);
    Assert.assertTrue(ingestionTaskReport.isPositive);
  }
}
