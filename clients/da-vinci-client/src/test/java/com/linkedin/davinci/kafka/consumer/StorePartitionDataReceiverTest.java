package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
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
import java.util.concurrent.CountDownLatch;
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
  public void testUnsubscribeFencesNotYetEnqueuedWorkWithoutWaitingOnConsumerLocks() throws Exception {
    StoreIngestionTask task = mock(StoreIngestionTask.class);
    VeniceServerConfig config = mock(VeniceServerConfig.class);
    when(task.isTimeBasedCheckpointEnabled()).thenReturn(true);
    when(task.isRunning()).thenReturn(true);
    when(task.getServerConfig()).thenReturn(config);
    when(config.getDatabaseSyncIntervalSeconds()).thenReturn(1L);
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    CheckpointProgress progress = new CheckpointProgress();
    when(task.getPartitionConsumptionState(0)).thenReturn(pcs);
    when(pcs.initializeCheckpointProgress()).thenReturn(progress);
    when(pcs.getCheckpointProgress()).thenReturn(progress);
    AtomicLong clock = new AtomicLong();
    StorePartitionDataReceiver receiver = new StorePartitionDataReceiver(task, topicPartition, "broker", 0, clock::get);
    receiver.onPoll();
    progress.source(StoreIngestionTask.checkpointSource(topicPartition, "broker")).recordUserData();
    CountDownLatch preparing = new CountDownLatch(1);
    CountDownLatch enqueue = new CountDownLatch(1);
    CompletableFuture<Void> executed = new CompletableFuture<>();
    doAnswer(invocation -> {
      preparing.countDown();
      assertTrue(enqueue.await(5, TimeUnit.SECONDS));
      return executed;
    }).when(task).checkpointOnTime(any(), anyString(), anyInt(), any(), any());
    clock.set(TimeUnit.SECONDS.toNanos(1));
    CompletableFuture<Void> poll = CompletableFuture.runAsync(() -> {
      try {
        receiver.onPoll();
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    });
    try {
      assertTrue(preparing.await(5, TimeUnit.SECONDS));
      receiver.onUnsubscription();
      assertFalse(poll.isDone(), "Unsubscription must not wait for a bounded-queue put under consumer locks");
      CompletableFuture<Void> barrier = progress.checkpoints();
      assertFalse(barrier.isDone(), "Teardown must already see work not yet enqueued");
      enqueue.countDown();
      poll.get(5, TimeUnit.SECONDS);
      assertFalse(barrier.isDone());
      executed.complete(null);
      barrier.get(5, TimeUnit.SECONDS);
    } finally {
      enqueue.countDown();
      executed.complete(null);
      poll.get(5, TimeUnit.SECONDS);
    }
  }

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
    when(config.getDatabaseSyncIntervalSeconds()).thenReturn(100L);
    when(task.isRunning()).thenReturn(true);
    when(task.getPartitionConsumptionState(0)).thenReturn(pcs);
    CheckpointProgress progress = new CheckpointProgress();
    when(pcs.initializeCheckpointProgress()).thenReturn(progress);
    when(pcs.getCheckpointProgress()).thenReturn(progress);
    CompletableFuture<Void> pending = new CompletableFuture<>();
    when(task.checkpointOnTime(any(), anyString(), anyInt(), any(), any())).thenReturn(pending);
    AtomicLong clock = new AtomicLong();
    StorePartitionDataReceiver receiver = new StorePartitionDataReceiver(task, topicPartition, "broker", 0, clock::get);
    assertTrue(receiver.requiresPollCallback());
    receiver.onPoll();
    // Heartbeat/control traffic alone does not advance this user-data generation.
    clock.set(TimeUnit.SECONDS.toNanos(100));
    receiver.onPoll();
    verify(task, never()).checkpointOnTime(any(), anyString(), anyInt(), any(), any());
    progress.source(StoreIngestionTask.checkpointSource(topicPartition, "broker")).recordUserData();

    DefaultPubSubMessage message = mock(DefaultPubSubMessage.class);
    when(message.getKey()).thenReturn(new KafkaKey(MessageType.PUT, new byte[0]));
    doCallRealMethod().when(task).shouldSyncOffset(any(), any(), any());
    when(task.getSyncBytesInterval(pcs)).thenReturn(100L);
    when(pcs.getProcessedRecordSizeSinceLastSync()).thenReturn(100L);
    assertTrue(task.shouldSyncOffset(pcs, message, null), "Bytes must trigger before the time deadline");
    when(pcs.getProcessedRecordSizeSinceLastSync()).thenReturn(1L);
    assertFalse(task.shouldSyncOffset(pcs, message, null));

    clock.set(TimeUnit.SECONDS.toNanos(99));
    receiver.onPoll();
    verify(task, never()).checkpointOnTime(any(), anyString(), anyInt(), any(), any());
    clock.set(TimeUnit.SECONDS.toNanos(100));
    receiver.onPoll();
    clock.set(TimeUnit.SECONDS.toNanos(1000));
    receiver.onPoll();
    ArgumentCaptor<BooleanSupplier> validity = ArgumentCaptor.forClass(BooleanSupplier.class);
    verify(task).checkpointOnTime(eq(topicPartition), eq("broker"), eq(0), eq(pcs), validity.capture());
    assertTrue(validity.getValue().getAsBoolean());
    verify(task, never()).produceToStoreBufferServiceOrKafka(any(), any(), anyString(), anyInt());

    when(task.isRunning()).thenReturn(false);
    assertFalse(validity.getValue().getAsBoolean());
    receiver.onPoll();
    when(task.isRunning()).thenReturn(true);
    PartitionConsumptionState replacement = mock(PartitionConsumptionState.class);
    when(task.getPartitionConsumptionState(0)).thenReturn(replacement);
    CheckpointProgress replacementProgress = new CheckpointProgress();
    when(replacement.initializeCheckpointProgress()).thenReturn(replacementProgress);
    when(replacement.getCheckpointProgress()).thenReturn(replacementProgress);
    assertFalse(validity.getValue().getAsBoolean());
    receiver.onPoll();
    when(task.getPartitionConsumptionState(0)).thenReturn(pcs);
    receiver.onUnsubscription();
    assertFalse(validity.getValue().getAsBoolean());
    pending.complete(null);
    receiver.onPoll();
    verify(task, times(1)).checkpointOnTime(any(), anyString(), anyInt(), any(), any());
  }

  @Test
  public void testReceiverRebindsAfterPartitionStateReplacementAndIsolatesAsyncInterruption() throws Exception {
    StoreIngestionTask task = mock(StoreIngestionTask.class);
    VeniceServerConfig config = mock(VeniceServerConfig.class);
    when(task.isTimeBasedCheckpointEnabled()).thenReturn(true);
    when(task.isRunning()).thenReturn(true);
    when(task.getServerConfig()).thenReturn(config);
    when(config.getDatabaseSyncIntervalSeconds()).thenReturn(1L);
    AtomicLong clock = new AtomicLong();
    StorePartitionDataReceiver receiver = new StorePartitionDataReceiver(task, topicPartition, "broker", 0, clock::get);
    PartitionConsumptionState old = mock(PartitionConsumptionState.class);
    CheckpointProgress oldProgress = new CheckpointProgress();
    when(old.initializeCheckpointProgress()).thenReturn(oldProgress);
    when(old.getCheckpointProgress()).thenReturn(oldProgress);
    when(task.getPartitionConsumptionState(0)).thenReturn(old);
    receiver.onPoll();

    PartitionConsumptionState current = mock(PartitionConsumptionState.class);
    CheckpointProgress progress = new CheckpointProgress();
    when(current.initializeCheckpointProgress()).thenReturn(progress);
    when(current.getCheckpointProgress()).thenReturn(progress);
    when(task.getPartitionConsumptionState(0)).thenReturn(current);
    receiver.onPoll();
    progress.source(StoreIngestionTask.checkpointSource(topicPartition, "broker")).recordUserData();
    CompletableFuture<Void> failed = new CompletableFuture<>();
    failed.completeExceptionally(new InterruptedException("producer callback interrupted"));
    when(task.checkpointOnTime(any(), anyString(), anyInt(), any(), any())).thenReturn(failed);
    clock.set(TimeUnit.SECONDS.toNanos(1));
    receiver.onPoll();
    receiver.onPoll();
    verify(task).checkpointOnTime(eq(topicPartition), eq("broker"), eq(0), eq(current), any());
    verify(task).setIngestionException(eq(0), any());
    verify(task, never()).setLastConsumerException(any());
    assertFalse(Thread.currentThread().isInterrupted());
  }
}
