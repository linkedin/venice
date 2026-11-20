package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.validation.DataIntegrityValidator;
import com.linkedin.davinci.validation.PartitionTracker;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class RecordDrivenCheckpointTest {
  @Test
  public void testDisabledProducerFailureRetainsOriginalCallbackAndPersistenceFuture() throws Exception {
    LeaderFollowerStoreIngestionTask task = mock(LeaderFollowerStoreIngestionTask.class);
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    PubSubTopicPartition tp = new PubSubTopicPartitionImpl(new PubSubTopicRepository().getTopic("store_v1"), 0);
    DefaultPubSubMessage record = heartbeat(tp);
    when(record.getKey()).thenReturn(new KafkaKey(MessageType.PUT, new byte[0]));
    when(task.isGlobalRtDivEnabled()).thenReturn(true);
    when(task.getVersionTopic()).thenReturn(tp.getPubSubTopic());
    when(task.shouldSendGlobalRtDiv(record, pcs, "store_v1")).thenReturn(true);
    when(pcs.getReplicaTopicPartition()).thenReturn(tp);
    DataIntegrityValidator div = mock(DataIntegrityValidator.class);
    when(task.getConsumerDiv()).thenReturn(div);
    when(div.cloneVtProducerStates(anyInt(), anyBoolean(), anyLong())).thenReturn(mock(PartitionTracker.class));
    LeaderProducerCallback callback = mock(LeaderProducerCallback.class);
    when(task.createProducerCallback(any(), any(), any(), anyInt(), any(), anyLong())).thenReturn(callback);
    LeaderProducedRecordContext context = mock(LeaderProducedRecordContext.class);
    CompletableFuture<Void> persisted = new CompletableFuture<>();
    when(context.getPersistedToDBFuture()).thenReturn(persisted);
    doCallRealMethod().when(task).produceToLocalKafka(any(), any(), any(), any(), anyInt(), any(), anyInt(), anyLong());
    VeniceException failure = new VeniceException("produce failed");

    org.testng.Assert.assertSame(
        org.testng.Assert.expectThrows(
            VeniceException.class,
            () -> task.produceToLocalKafka(record, pcs, context, (installedCallback, metadata) -> {
              org.testng.Assert.assertSame(installedCallback, callback);
              verify(callback).setOnCompletionCallback(any());
              throw failure;
            }, 0, "remote-broker", 0, 0)),
        failure);

    assertFalse(persisted.isDone(), "Byte-only mode must not add failure completion of the original persist future");
    verify(pcs).setLastLeaderPersistFuture(persisted);
    verify(pcs, never()).initializeCheckpointProgress();
    verify(pcs, never()).getCheckpointProgress();
    verify(task, never()).enqueueCheckpointSnapshot(any(), any(), any(), any(), any(), anyBoolean());
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = com.linkedin.venice.utils.DataProviderUtils.class)
  public void testHeartbeatCreatesCallbackBeforeMetadataPreparationFailure(boolean periodic) {
    LeaderFollowerStoreIngestionTask task = mock(LeaderFollowerStoreIngestionTask.class);
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    DefaultPubSubMessage record = mock(DefaultPubSubMessage.class);
    LeaderProducedRecordContext context = mock(LeaderProducedRecordContext.class);
    when(task.isTimeBasedCheckpointEnabled()).thenReturn(periodic);
    VeniceException failure = new VeniceException("metadata preparation failed");
    when(record.getPosition()).thenThrow(failure);
    doCallRealMethod().when(task)
        .propagateHeartbeatFromUpstreamTopicToLocalVersionTopic(
            any(),
            any(),
            any(),
            anyInt(),
            any(),
            anyInt(),
            anyLong());

    org.testng.Assert
        .assertSame(
            org.testng.Assert.expectThrows(
                VeniceException.class,
                () -> task
                    .propagateHeartbeatFromUpstreamTopicToLocalVersionTopic(pcs, record, context, 0, "broker", 0, 0)),
            failure);

    org.mockito.InOrder order = inOrder(task, record);
    order.verify(task).propagateHeartbeatFromUpstreamTopicToLocalVersionTopic(pcs, record, context, 0, "broker", 0, 0);
    order.verify(task).createProducerCallback(record, pcs, context, 0, "broker", 0);
    order.verify(record).getPosition();
    verify(pcs, never()).initializeCheckpointProgress();
  }

  @Test
  public void testDisabledGlobalControlPredicateSkipsByteAndTimeState() {
    LeaderFollowerStoreIngestionTask task = mock(LeaderFollowerStoreIngestionTask.class);
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    DefaultPubSubMessage record =
        heartbeat(new PubSubTopicPartitionImpl(new PubSubTopicRepository().getTopic("store_rt"), 0));
    when(task.isGlobalRtDivEnabled()).thenReturn(true);
    doCallRealMethod().when(task).shouldSendGlobalRtDiv(any(), any(), any());

    assertFalse(task.shouldSendGlobalRtDiv(record, pcs, "broker"));

    verify(task, never()).getSyncBytesInterval(any());
    verify(task, never()).isCheckpointDue(any(), any(), any());
    org.mockito.Mockito.verifyNoInteractions(pcs);
  }

  @DataProvider
  public Object[][] heartbeatPendingData() {
    return new Object[][] { { false }, { true } };
  }

  @Test(dataProvider = "heartbeatPendingData")
  public void testRtHeartbeatAdapterUsesSourcePredicateWithoutChangingPersistenceBookkeeping(boolean pendingData) {
    LeaderFollowerStoreIngestionTask task = mock(LeaderFollowerStoreIngestionTask.class);
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    PubSubTopicRepository topics = new PubSubTopicRepository();
    DefaultPubSubMessage record = heartbeat(new PubSubTopicPartitionImpl(topics.getTopic("store_rt"), 0));
    when(task.getVersionTopic()).thenReturn(topics.getTopic("store_v1"));
    when(task.isTimeBasedCheckpointEnabled()).thenReturn(true);
    when(task.isGlobalRtDivEnabled()).thenReturn(true);
    when(task.checkpointTimeNs()).thenReturn(100L);
    CheckpointProgress progress = new CheckpointProgress();
    PeriodicCheckpoint checkpoint = progress.source(
        StoreIngestionTask.checkpointSource(record.getTopicPartition(), "broker"),
        new PeriodicCheckpoint(100, 0, 0));
    if (pendingData) {
      checkpoint.recordUserData();
    }
    when(pcs.initializeCheckpointProgress()).thenReturn(progress);
    when(pcs.getCheckpointProgress()).thenReturn(progress);
    when(pcs.isSubscribed()).thenReturn(true);
    VeniceWriter<byte[], byte[], byte[]> writer = mock(VeniceWriter.class);
    when(pcs.getVeniceWriterLazyRef()).thenReturn(Lazy.of(() -> writer));
    when(writer.sendHeartbeat(any(), any(), any(), anyBoolean(), any(), anyLong()))
        .thenReturn(CompletableFuture.completedFuture(null));
    doCallRealMethod().when(task).recordCheckpointUserData(any(), any(), any());
    doCallRealMethod().when(task).isCheckpointDue(any(), any(), any());
    doCallRealMethod().when(task).shouldSendGlobalRtDiv(any(), any(), any());
    doCallRealMethod().when(task)
        .propagateHeartbeatFromUpstreamTopicToLocalVersionTopic(
            any(),
            any(),
            any(),
            anyInt(),
            any(),
            anyInt(),
            anyLong());

    task.propagateHeartbeatFromUpstreamTopicToLocalVersionTopic(
        pcs,
        record,
        mock(LeaderProducedRecordContext.class),
        0,
        "broker",
        0,
        0);

    verify(writer).sendHeartbeat(any(), any(), any(), anyBoolean(), any(), anyLong());
    verify(task, org.mockito.Mockito.times(pendingData ? 1 : 0))
        .sendGlobalRtDivMessage(any(), any(), any(), anyInt(), any(), anyLong(), anyInt(), anyBoolean());
    verify(pcs, never()).setLastLeaderPersistFuture(any());
    assertEquals(checkpoint.generation(), pendingData ? 1L : 0L);
  }

  @DataProvider
  public Object[][] recordRoutes() {
    return new Object[][] { { "drainer" }, { "rt-leader" }, { "vt-consumer" } };
  }

  @Test(dataProvider = "recordRoutes")
  public void testNewHeartbeatUsesTimeOnlyForPendingDataAndByteThresholdRemainsIndependent(String route) {
    LeaderFollowerStoreIngestionTask task = mock(LeaderFollowerStoreIngestionTask.class);
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    PubSubTopicRepository topics = new PubSubTopicRepository();
    PubSubTopicPartition tp =
        new PubSubTopicPartitionImpl(topics.getTopic(route.equals("rt-leader") ? "store_rt" : "store_v1"), 0);
    DefaultPubSubMessage record = heartbeat(tp);
    CheckpointProgress progress = new CheckpointProgress();
    PeriodicCheckpoint checkpoint = progress.source(
        StoreIngestionTask.checkpointSource(tp, route.equals("rt-leader") ? "broker" : task.localKafkaServer),
        new PeriodicCheckpoint(100, 20, 0));
    when(task.isTimeBasedCheckpointEnabled()).thenReturn(true);
    when(task.isGlobalRtDivEnabled()).thenReturn(!route.equals("drainer"));
    when(task.getSyncBytesInterval(pcs)).thenReturn(100L);
    when(task.getVersionTopic()).thenReturn(topics.getTopic("store_v1"));
    when(pcs.getCheckpointProgress()).thenReturn(progress);
    when(pcs.isSubscribed()).thenReturn(true);
    doCallRealMethod().when(task).isCheckpointDue(any(), any(), any());
    doCallRealMethod().when(task).shouldSyncOffset(any(), any(), any());
    doCallRealMethod().when(task).shouldSendGlobalRtDiv(any(), any(), any());
    doCallRealMethod().when(task).shouldSendVtDivSnapshot(any(), any());
    BooleanSupplier predicate = route.equals("drainer")
        ? () -> task.shouldSyncOffset(pcs, record, null)
        : route.equals("rt-leader")
            ? () -> task.shouldSendGlobalRtDiv(record, pcs, "broker")
            : () -> task.shouldSendVtDivSnapshot(record, pcs);

    when(task.checkpointTimeNs()).thenReturn(120L);
    assertFalse(predicate.getAsBoolean(), "Heartbeat-only traffic must not dirty a source");
    checkpoint.recordUserData();
    when(task.checkpointTimeNs()).thenReturn(119L);
    assertFalse(predicate.getAsBoolean(), "The stable spread is part of the effective interval");
    when(task.checkpointTimeNs()).thenReturn(120L);
    assertTrue(predicate.getAsBoolean(), "A new heartbeat can checkpoint earlier pending user data");
    when(task.isTimeBasedCheckpointEnabled()).thenReturn(false);
    assertFalse(predicate.getAsBoolean(), "Startup-loaded kill switch disables only the time branch");

    when(record.getKey()).thenReturn(new KafkaKey(MessageType.PUT, new byte[0]));
    when(pcs.getProcessedRecordSizeSinceLastSync()).thenReturn(100L);
    when(pcs.getConsumedBytesSinceLastGlobalRtDivSync("broker")).thenReturn(100L);
    when(pcs.getConsumedBytesSinceLastGlobalRtDivSync("store_v1")).thenReturn(199L);
    assertEquals(predicate.getAsBoolean(), !route.equals("vt-consumer"));
    when(pcs.getConsumedBytesSinceLastGlobalRtDivSync("store_v1")).thenReturn(200L);
    assertTrue(predicate.getAsBoolean(), "Existing byte thresholds must work with time checkpoints disabled");
  }

  @DataProvider
  public Object[][] nonRtSources() {
    return new Object[][] { { "store_v1" }, { "store_v1_sr" } };
  }

  @Test(dataProvider = "nonRtSources")
  public void testNonRtTimeTriggerUsesExistingDataProduceCallback(String source) throws Exception {
    LeaderFollowerStoreIngestionTask task = mock(LeaderFollowerStoreIngestionTask.class);
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    PubSubTopicPartition tp = new PubSubTopicPartitionImpl(new PubSubTopicRepository().getTopic(source), 0);
    DefaultPubSubMessage record = heartbeat(tp);
    CheckpointProgress progress = new CheckpointProgress();
    PeriodicCheckpoint checkpoint = progress
        .source(StoreIngestionTask.checkpointSource(tp, task.localKafkaServer), new PeriodicCheckpoint(100, 0, 0));
    checkpoint.recordUserData();
    when(task.isTimeBasedCheckpointEnabled()).thenReturn(true);
    when(task.isGlobalRtDivEnabled()).thenReturn(true);
    when(task.checkpointTimeNs()).thenReturn(100L);
    when(pcs.getCheckpointProgress()).thenReturn(progress);
    when(pcs.isSubscribed()).thenReturn(true);
    PubSubTopicPartition local = new PubSubTopicPartitionImpl(new PubSubTopicRepository().getTopic("store_v1"), 0);
    when(pcs.getReplicaTopicPartition()).thenReturn(local);
    com.linkedin.venice.offsets.OffsetRecord offsetRecord = mock(com.linkedin.venice.offsets.OffsetRecord.class);
    when(offsetRecord.getLeaderTopic(any())).thenReturn(tp.getPubSubTopic());
    when(pcs.getOffsetRecord()).thenReturn(offsetRecord);
    when(task.getVersionTopic()).thenReturn(tp.getPubSubTopic());
    DataIntegrityValidator div = mock(DataIntegrityValidator.class);
    when(task.getConsumerDiv()).thenReturn(div);
    when(div.cloneVtProducerStates(anyInt(), anyBoolean(), anyLong())).thenReturn(mock(PartitionTracker.class));
    doCallRealMethod().when(task).isCheckpointDue(any(), any(), any());
    doCallRealMethod().when(task).addVtDivToProducerCallbackIfNeeded(any(), any(), any(), any());
    LeaderProducerCallback callback = mock(LeaderProducerCallback.class);
    when(task.enqueueCheckpointSnapshot(any(), any(), any(), any(), any(), anyBoolean()))
        .thenReturn(CompletableFuture.completedFuture(null));

    task.addVtDivToProducerCallbackIfNeeded(record, callback, pcs, new CompletableFuture<>());

    verify(callback).setOnCompletionCallback(any());
    verify(pcs).resetConsumedBytesSinceLastGlobalRtDivSync(source);
    verify(task, never()).sendGlobalRtDivMessage(any(), any(), any(), anyInt(), any(), anyLong(), anyInt());
    assertFalse(progress.checkpoints().isDone(), "The existing producer callback owns checkpoint completion");
    assertEquals(checkpoint.generation(), 1L, "Enqueue must not acknowledge pending data");
    org.mockito.ArgumentCaptor<java.util.function.Consumer<com.linkedin.venice.pubsub.api.PubSubProduceResult>> completion =
        org.mockito.ArgumentCaptor.forClass(java.util.function.Consumer.class);
    verify(callback).setOnCompletionCallback(completion.capture());
    completion.getValue().accept(mock(com.linkedin.venice.pubsub.api.PubSubProduceResult.class));
    verify(task).enqueueCheckpointSnapshot(
        org.mockito.ArgumentMatchers.eq(tp),
        org.mockito.ArgumentMatchers.eq(pcs),
        any(),
        any(),
        any(),
        org.mockito.ArgumentMatchers.eq(true));
  }

  private DefaultPubSubMessage heartbeat(PubSubTopicPartition tp) {
    DefaultPubSubMessage record = mock(DefaultPubSubMessage.class);
    when(record.getTopicPartition()).thenReturn(tp);
    when(record.getKey()).thenReturn(new KafkaKey(MessageType.CONTROL_MESSAGE, new byte[0]));
    KafkaMessageEnvelope envelope = new KafkaMessageEnvelope();
    envelope.producerMetadata = new ProducerMetadata();
    ControlMessage control = new ControlMessage();
    control.controlMessageType = ControlMessageType.START_OF_SEGMENT.getValue();
    envelope.payloadUnion = control;
    when(record.getValue()).thenReturn(envelope);
    return record;
  }
}
