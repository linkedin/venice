package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.fail;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.Collections;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class StorePartitionDataReceiverTest {
  @DataProvider
  public Object[][] receiverFailures() {
    return new Object[][] { { new StoreBufferService.StalledDrainerException("stalled") },
        { new VeniceException(new StoreBufferService.StalledDrainerException("stalled")) },
        { new VeniceException("ordinary consumer failure") } };
  }

  @Test(dataProvider = "receiverFailures")
  public void testReceiverDelegatesExceptionClassificationToIngestionTask(Exception failure) throws Exception {
    StoreIngestionTask task = mock(StoreIngestionTask.class);
    PubSubTopicPartition partition =
        new PubSubTopicPartitionImpl(new PubSubTopicRepository().getTopic("receiver_v1"), 3);
    doThrow(failure).when(task).produceToStoreBufferServiceOrKafka(any(), eq(partition), anyString(), anyInt());
    new StorePartitionDataReceiver(task, partition, "kafka", 0).write(Collections.emptyList());
    verify(task).setLastConsumerException(failure);
    // The failed QueueNode owns registration; the receiver must not re-register it after a reset.
    verify(task, never()).setIngestionException(anyInt(), any());
  }

  @Test
  public void testInterruptedReceiverStillPropagates() throws Exception {
    StoreIngestionTask task = mock(StoreIngestionTask.class);
    PubSubTopicPartition partition =
        new PubSubTopicPartitionImpl(new PubSubTopicRepository().getTopic("receiver_v1"), 3);
    InterruptedException failure = new InterruptedException("shutdown");
    doThrow(failure).when(task).produceToStoreBufferServiceOrKafka(any(), eq(partition), anyString(), anyInt());
    try {
      new StorePartitionDataReceiver(task, partition, "kafka", 0).write(Collections.emptyList());
      fail("Interruption must propagate");
    } catch (InterruptedException actual) {
      assertSame(actual, failure);
    }
    verify(task, never()).setLastConsumerException(any());
  }
}
