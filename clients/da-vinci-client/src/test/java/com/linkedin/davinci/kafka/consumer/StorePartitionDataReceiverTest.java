package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;


public class StorePartitionDataReceiverTest {
  private final PubSubTopicPartition topicPartition =
      new PubSubTopicPartitionImpl(new PubSubTopicRepository().getTopic("test_store_v1"), 0);

  @Test
  public void testDisabledHasNoDeadlineOrPollWork() throws Exception {
    StoreIngestionTask task = mock(StoreIngestionTask.class);
    LongSupplier clock = mock(LongSupplier.class);
    StorePartitionDataReceiver receiver = new StorePartitionDataReceiver(task, topicPartition, "broker", 0, clock);
    assertFalse(receiver.requiresPollCallback());
    receiver.onPoll();
    verify(clock, never()).getAsLong();
    verify(task, never()).checkpointOnTime(any(), anyString(), anyInt(), any(), any());
  }

  @Test
  public void testIdleTimeTriggerAndIndependentByteTrigger() throws Exception {
    StoreIngestionTask task = mock(StoreIngestionTask.class);
    VeniceServerConfig config = mock(VeniceServerConfig.class);
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    when(task.isTimeBasedCheckpointEnabled()).thenReturn(true);
    when(task.getServerConfig()).thenReturn(config);
    when(config.getDatabaseSyncIntervalMs()).thenReturn(100L);
    when(task.isRunning()).thenReturn(true);
    when(task.getPartitionConsumptionState(0)).thenReturn(pcs);
    CompletableFuture<Void> pending = new CompletableFuture<>();
    when(task.checkpointOnTime(any(), anyString(), anyInt(), any(), any())).thenReturn(pending);
    AtomicLong clock = new AtomicLong();
    StorePartitionDataReceiver receiver = new StorePartitionDataReceiver(task, topicPartition, "broker", 0, clock::get);
    assertTrue(receiver.requiresPollCallback());

    DefaultPubSubMessage message = mock(DefaultPubSubMessage.class);
    when(message.getKey()).thenReturn(new KafkaKey(MessageType.PUT, new byte[0]));
    doCallRealMethod().when(task).shouldSyncOffset(any(), any(), any());
    when(task.getSyncBytesInterval(pcs)).thenReturn(100L);
    when(pcs.getProcessedRecordSizeSinceLastSync()).thenReturn(100L);
    assertTrue(task.shouldSyncOffset(pcs, message, null), "Bytes must trigger before the time deadline");
    when(pcs.getProcessedRecordSizeSinceLastSync()).thenReturn(1L);
    assertFalse(task.shouldSyncOffset(pcs, message, null));

    clock.set(TimeUnit.MILLISECONDS.toNanos(99));
    receiver.onPoll();
    verify(task, never()).checkpointOnTime(any(), anyString(), anyInt(), any(), any());
    clock.set(TimeUnit.MILLISECONDS.toNanos(100));
    receiver.onPoll();
    clock.set(TimeUnit.MILLISECONDS.toNanos(1000));
    receiver.onPoll();
    ArgumentCaptor<BooleanSupplier> validity = ArgumentCaptor.forClass(BooleanSupplier.class);
    verify(task).checkpointOnTime(eq(topicPartition), eq("broker"), eq(0), eq(pcs), validity.capture());
    assertTrue(validity.getValue().getAsBoolean());
    verify(task, never()).produceToStoreBufferServiceOrKafka(any(), any(), anyString(), anyInt());

    when(task.isRunning()).thenReturn(false);
    assertFalse(validity.getValue().getAsBoolean());
    receiver.onPoll();
    when(task.isRunning()).thenReturn(true);
    when(task.getPartitionConsumptionState(0)).thenReturn(mock(PartitionConsumptionState.class));
    assertFalse(validity.getValue().getAsBoolean());
    receiver.onPoll();
    when(task.getPartitionConsumptionState(0)).thenReturn(pcs);
    receiver.onUnsubscription();
    assertFalse(validity.getValue().getAsBoolean());
    pending.complete(null);
    receiver.onPoll();
    verify(task, times(1)).checkpointOnTime(any(), anyString(), anyInt(), any(), any());
  }
}
