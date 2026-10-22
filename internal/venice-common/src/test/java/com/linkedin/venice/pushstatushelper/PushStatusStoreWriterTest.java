package com.linkedin.venice.pushstatushelper;

import static com.linkedin.venice.common.PushStatusStoreUtils.SERVER_INCREMENTAL_PUSH_PREFIX;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.common.PushStatusStoreUtils;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.logger.TestLogAppender;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushstatus.PushStatusKey;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class PushStatusStoreWriterTest {
  private PushStatusStoreVeniceWriterCache veniceWriterCacheMock;
  private VeniceWriter veniceWriterMock;
  private VeniceWriter clusterWriter;
  private PushStatusStoreWriter pushStatusStoreWriter;
  private final List<PushStatusStoreWriter> clusterWriters = new ArrayList<>();
  private static final String instanceName = "instanceX";
  private static final String storeName = "venice-test-push-status-store";
  private static final int storeVersion = 42;
  private static final String incPushVersion = "inc_push_test_version_1";
  private static final int derivedSchemaId = 1;
  private static final int valueSchemaId =
      AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersion();
  private static final Schema valueSchema =
      AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersionSchema();
  private static final Schema updateSchema =
      WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);

  @BeforeMethod
  public void setUp() {
    veniceWriterCacheMock = mock(PushStatusStoreVeniceWriterCache.class);
    veniceWriterMock = mock(VeniceWriter.class);
    clusterWriter = mock(VeniceWriter.class);
    when(veniceWriterCacheMock.prepareClusterVeniceWriter("cluster")).thenReturn(clusterWriter);
    when(clusterWriter.update(any(), any(), eq(valueSchemaId), eq(derivedSchemaId), any()))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(veniceWriterMock.delete(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
    when(clusterWriter.delete(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
    pushStatusStoreWriter =
        new PushStatusStoreWriter(veniceWriterCacheMock, instanceName, valueSchemaId, derivedSchemaId, updateSchema);
    when(veniceWriterCacheMock.prepareVeniceWriter(storeName)).thenReturn(veniceWriterMock);
  }

  @AfterMethod
  public void closeWriters() {
    clusterWriters.forEach(PushStatusStoreWriter::close);
    clusterWriters.clear();
    pushStatusStoreWriter.close();
  }

  private GenericRecord getAddIncrementalPushUpdateRecord() {
    UpdateBuilder updateBuilder = new UpdateBuilderImpl(updateSchema);
    updateBuilder.setEntriesToAddToMapField(
        "instances",
        Collections.singletonMap(incPushVersion, START_OF_INCREMENTAL_PUSH_RECEIVED.getValue()));
    return updateBuilder.build();
  }

  private GenericRecord getRemoveIncrementalPushUpdateRecord() {
    UpdateBuilder updateBuilder = new UpdateBuilderImpl(updateSchema);
    updateBuilder.setKeysToRemoveFromMapField("instances", Collections.singletonList(incPushVersion));
    return updateBuilder.build();
  }

  private GenericRecord getHeartbeatRecord(long heartbeatTimestamp) {
    UpdateBuilder updateBuilder = new UpdateBuilderImpl(updateSchema);
    updateBuilder.setNewFieldValue("reportTimestamp", heartbeatTimestamp);
    return updateBuilder.build();

  }

  @Test(description = "Expect an update call for adding current inc-push to ongoing-inc-pushes when status is SOIP")
  public void testWritePushStatusWhenStatusIsSOIP() {
    PushStatusKey serverPushStatusKey = PushStatusStoreUtils
        .getServerIncrementalPushKey(storeVersion, 1, incPushVersion, SERVER_INCREMENTAL_PUSH_PREFIX);
    PushStatusKey ongoPushStatusKey = PushStatusStoreUtils.getOngoingIncrementalPushStatusesKey(storeVersion);

    pushStatusStoreWriter.writePushStatus(
        storeName,
        storeVersion,
        1,
        START_OF_INCREMENTAL_PUSH_RECEIVED,
        Optional.of(incPushVersion),
        Optional.of(SERVER_INCREMENTAL_PUSH_PREFIX));

    verify(veniceWriterMock).update(eq(serverPushStatusKey), any(), eq(valueSchemaId), eq(derivedSchemaId), eq(null));
    verify(veniceWriterCacheMock, times(2)).prepareVeniceWriter(storeName);
    verify(veniceWriterMock).update(
        eq(ongoPushStatusKey),
        eq(getAddIncrementalPushUpdateRecord()),
        eq(valueSchemaId),
        eq(derivedSchemaId),
        eq(null));
  }

  @Test
  public void testAddToSupposedlyOngoingIncrementalPushVersions() {
    PushStatusKey statusKey = PushStatusStoreUtils.getOngoingIncrementalPushStatusesKey(storeVersion);
    pushStatusStoreWriter.addToSupposedlyOngoingIncrementalPushVersions(
        storeName,
        storeVersion,
        incPushVersion,
        START_OF_INCREMENTAL_PUSH_RECEIVED);
    verify(veniceWriterCacheMock).prepareVeniceWriter(storeName);
    verify(veniceWriterMock).update(
        eq(statusKey),
        eq(getAddIncrementalPushUpdateRecord()),
        eq(valueSchemaId),
        eq(derivedSchemaId),
        eq(null));
  }

  @Test
  public void testRemoveFromOngoingIncrementalPushVersions() {
    PushStatusKey statusKey = PushStatusStoreUtils.getOngoingIncrementalPushStatusesKey(storeVersion);
    pushStatusStoreWriter.removeFromSupposedlyOngoingIncrementalPushVersions(storeName, storeVersion, incPushVersion);
    verify(veniceWriterCacheMock).prepareVeniceWriter(storeName);
    verify(veniceWriterMock).update(
        eq(statusKey),
        eq(getRemoveIncrementalPushUpdateRecord()),
        eq(valueSchemaId),
        eq(derivedSchemaId),
        eq(null));
  }

  @Test
  public void testDeletePushStatus() {
    int partitionCount = 4;
    pushStatusStoreWriter.deletePushStatus(storeName, storeVersion, Optional.empty(), partitionCount);
    verify(veniceWriterCacheMock, times(partitionCount + 1)).prepareVeniceWriter(storeName);
    verify(veniceWriterMock).delete(eq(PushStatusStoreUtils.getPushKey(storeVersion)), eq(null));
    for (int i = 0; i < partitionCount; i++) {
      PushStatusKey statusKey = PushStatusStoreUtils.getPushKey(storeVersion, i, Optional.empty());
      verify(veniceWriterMock).delete(eq(statusKey), eq(null));
    }
  }

  @Test
  public void testDualWritesAndDeletesUseStoreScopedClusterKeys() throws Exception {
    CountDownLatch clusterUpdates = new CountDownLatch(6);
    when(clusterWriter.update(any(), any(), eq(valueSchemaId), eq(derivedSchemaId), any())).thenAnswer(invocation -> {
      clusterUpdates.countDown();
      return CompletableFuture.completedFuture(null);
    });
    PushStatusStoreWriter writer = dualWriter();
    writer.writeHeartbeatForBootstrappingInstance(storeName);
    writer.writeVersionLevelPushStatus(
        storeName,
        storeVersion,
        ExecutionStatus.COMPLETED,
        Collections.singleton(0),
        Optional.empty());
    writer.writePushStatus(storeName, storeVersion, 0, ExecutionStatus.COMPLETED, Optional.empty());
    writer.writePushStatus(
        storeName,
        storeVersion,
        0,
        START_OF_INCREMENTAL_PUSH_RECEIVED,
        Optional.of(incPushVersion),
        Optional.of(SERVER_INCREMENTAL_PUSH_PREFIX));
    writer.removeFromSupposedlyOngoingIncrementalPushVersions(storeName, storeVersion, incPushVersion);
    Assert.assertTrue(clusterUpdates.await(10, TimeUnit.SECONDS));
    ArgumentCaptor<PushStatusKey> keys = ArgumentCaptor.forClass(PushStatusKey.class);
    ArgumentCaptor<GenericRecord> updates = ArgumentCaptor.forClass(GenericRecord.class);
    verify(veniceWriterMock, times(6))
        .update(keys.capture(), updates.capture(), eq(valueSchemaId), eq(derivedSchemaId), any());
    for (int i = 0; i < keys.getAllValues().size(); i++) {
      verify(clusterWriter).update(
          eq(PushStatusStoreUtils.getClusterPushStatusKey(storeName, keys.getAllValues().get(i))),
          eq(updates.getAllValues().get(i)),
          eq(valueSchemaId),
          eq(derivedSchemaId),
          any(PubSubProducerCallback.class));
    }
    writer.deletePushStatus("cluster", storeName, storeVersion, Optional.empty(), 1).get(10, TimeUnit.SECONDS);
    for (PushStatusKey key: Arrays.asList(
        PushStatusStoreUtils.getPushKey(storeVersion),
        PushStatusStoreUtils.getPushKey(storeVersion, 0, Optional.empty()))) {
      verify(veniceWriterMock).delete(eq(key), eq(null));
      verify(clusterWriter)
          .delete(eq(PushStatusStoreUtils.getClusterPushStatusKey(storeName, key)), any(PubSubProducerCallback.class));
    }
    writer.removePushStatusStoreVeniceWriter(storeName);
    verify(veniceWriterCacheMock).removeVeniceWriter(storeName);
    verify(clusterWriter, never()).close();
  }

  @Test(dataProvider = "clusterUpdateFailureStages")
  @SuppressWarnings("unchecked")
  public void testClusterUpdateFailureDoesNotInterruptLegacyReports(String failureStage) throws Exception {
    Function<String, String> resolver = mock(Function.class);
    when(resolver.apply(storeName)).thenReturn("cluster");
    RuntimeException failure = new IllegalStateException("cluster " + failureStage + " failed");
    CountDownLatch failures = new CountDownLatch(2);
    if (failureStage.equals("resolution")) {
      when(resolver.apply(storeName)).thenThrow(failure);
    } else if (failureStage.equals("creation")) {
      when(veniceWriterCacheMock.prepareClusterVeniceWriter("cluster")).thenAnswer(invocation -> {
        failures.countDown();
        throw failure;
      });
    } else {
      doAnswer(invocation -> {
        failures.countDown();
        throw failure;
      }).when(clusterWriter).update(any(), any(), eq(valueSchemaId), eq(derivedSchemaId), any());
    }
    PushStatusStoreWriter writer = writer(resolver, 1024);
    for (Optional<String> incrementalVersion: Arrays.asList(Optional.<String>empty(), Optional.of(incPushVersion))) {
      writer.writeVersionLevelPushStatus(
          storeName,
          storeVersion,
          ExecutionStatus.COMPLETED,
          Collections.singleton(0),
          incrementalVersion);
      verify(veniceWriterMock).update(
          eq(PushStatusStoreUtils.getPushKey(storeVersion, incrementalVersion)),
          any(),
          eq(valueSchemaId),
          eq(derivedSchemaId),
          any(PubSubProducerCallback.class));
    }
    verify(resolver, times(2)).apply(storeName);
    if (!failureStage.equals("resolution")) {
      Assert.assertTrue(failures.await(10, TimeUnit.SECONDS));
    }
  }

  @DataProvider
  public Object[][] clusterUpdateFailureStages() {
    return new Object[][] { { "resolution" }, { "creation" }, { "update" } };
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testBlockedClusterWorkDoesNotDelayAnotherStoresLegacyHeartbeat(boolean blockCreation) throws Exception {
    when(veniceWriterCacheMock.prepareVeniceWriter("another-store")).thenReturn(veniceWriterMock);
    PushStatusStoreWriter writer = dualWriter();
    ExecutorService heartbeatScheduler = Executors.newSingleThreadExecutor();
    try (BlockedClusterWork work = new BlockedClusterWork(blockCreation, 2)) {
      heartbeatScheduler.submit(() -> writer.writeHeartbeat(storeName, 1)).get(10, TimeUnit.SECONDS);
      Assert.assertTrue(work.started.await(10, TimeUnit.SECONDS));
      heartbeatScheduler.submit(() -> writer.writeHeartbeat("another-store", 2)).get(10, TimeUnit.SECONDS);
      verify(veniceWriterMock).update(any(), eq(getHeartbeatRecord(2)), eq(valueSchemaId), eq(derivedSchemaId), any());
      work.close();
      Assert.assertTrue(work.updates.await(10, TimeUnit.SECONDS));
    } finally {
      heartbeatScheduler.shutdownNow();
    }
  }

  @Test
  public void testClusterQueueSaturationAndUpdateDeleteOrdering() throws Exception {
    TestLogAppender appender = new TestLogAppender("ClusterQueueSaturation", PatternLayout.createDefaultLayout());
    appender.start();
    Logger logger = (Logger) LogManager.getLogger(PushStatusStoreWriter.class);
    logger.addAppender(appender);
    CompletableFuture<PubSubProduceResult> acknowledgement = new CompletableFuture<>();
    when(clusterWriter.delete(any(), any())).thenReturn(acknowledgement);
    PushStatusStoreWriter writer = dualWriter(1);
    try (BlockedClusterWork work = new BlockedClusterWork(false, 2)) {
      writer.writeHeartbeat(storeName, 1);
      Assert.assertTrue(work.started.await(10, TimeUnit.SECONDS));
      writer.writeHeartbeat(storeName, 2);
      writer.writeHeartbeat(storeName, 3);
      Assert.assertTrue(appender.getLog().contains("Dropped cluster push status update"));
      Future<PubSubProduceResult> rejected =
          writer.deletePartitionIncrementalPushStatus(storeName, storeVersion, incPushVersion, 0);
      assertRejected(rejected);
      verify(veniceWriterMock, times(3)).update(any(), any(), eq(valueSchemaId), eq(derivedSchemaId), any());
      work.close();
      Assert.assertTrue(work.updates.await(10, TimeUnit.SECONDS));
      Future<PubSubProduceResult> deletion =
          writer.deletePartitionIncrementalPushStatus(storeName, storeVersion, incPushVersion, 0);
      Assert.assertFalse(deletion.isDone());
      acknowledgement.complete(null);
      deletion.get(10, TimeUnit.SECONDS);
      org.mockito.InOrder order = Mockito.inOrder(clusterWriter);
      order.verify(clusterWriter)
          .update(any(), eq(getHeartbeatRecord(1)), eq(valueSchemaId), eq(derivedSchemaId), any());
      order.verify(clusterWriter)
          .update(any(), eq(getHeartbeatRecord(2)), eq(valueSchemaId), eq(derivedSchemaId), any());
      order.verify(clusterWriter).delete(any(), any());
      verify(clusterWriter, times(2)).update(any(), any(), eq(valueSchemaId), eq(derivedSchemaId), any());
    } finally {
      logger.removeAppender(appender);
      appender.stop();
    }
  }

  @Test(dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testCloseCancelsQueuedWorkAndClosesCacheAfterLateClusterWork(boolean blockCreation) throws Exception {
    CountDownLatch cacheClosed = new CountDownLatch(1);
    doAnswer(invocation -> {
      cacheClosed.countDown();
      return null;
    }).when(veniceWriterCacheMock).close();
    PushStatusStoreWriter writer = dualWriter();
    try (BlockedClusterWork work = new BlockedClusterWork(blockCreation, 1)) {
      writer.writeHeartbeat(storeName);
      Assert.assertTrue(work.started.await(10, TimeUnit.SECONDS));
      Future<PubSubProduceResult> queued =
          writer.deletePartitionIncrementalPushStatus(storeName, storeVersion, incPushVersion, 0);
      writer.close();
      assertRejected(queued);
      verify(veniceWriterCacheMock, never()).close();
      Assert.expectThrows(IllegalStateException.class, () -> writer.writeHeartbeat(storeName));
      Assert.expectThrows(
          IllegalStateException.class,
          () -> writer.deletePartitionIncrementalPushStatus(storeName, storeVersion, incPushVersion, 0));
      work.close();
      Assert.assertTrue(cacheClosed.await(10, TimeUnit.SECONDS));
      verify(veniceWriterCacheMock).prepareClusterVeniceWriter("cluster");
      verify(clusterWriter, never()).delete(any(), any());
      if (blockCreation) {
        verify(clusterWriter, never()).update(any(), any(), eq(valueSchemaId), eq(derivedSchemaId), any());
      }
      writer.close();
      verify(veniceWriterCacheMock).close();
    }
  }

  @Test
  public void testCloseFailsUnacknowledgedClusterDeletion() throws Exception {
    CountDownLatch submitted = new CountDownLatch(1);
    CompletableFuture<PubSubProduceResult> acknowledgement = new CompletableFuture<>();
    when(clusterWriter.delete(any(), any())).thenAnswer(invocation -> {
      submitted.countDown();
      return acknowledgement;
    });
    PushStatusStoreWriter writer = dualWriter();
    Future<PubSubProduceResult> deletion =
        writer.deletePartitionIncrementalPushStatus(storeName, storeVersion, incPushVersion, 0);
    Assert.assertTrue(submitted.await(10, TimeUnit.SECONDS));
    writer.close();
    assertRejected(deletion);
    acknowledgement.complete(null);
    assertRejected(deletion);
  }

  @Test
  public void testLegacyUpdateFailurePropagates() {
    RuntimeException failure = new IllegalStateException("legacy update failed");
    doThrow(failure).when(veniceWriterMock).update(any(), any(), eq(valueSchemaId), eq(derivedSchemaId), any());
    Assert.assertSame(
        Assert.expectThrows(IllegalStateException.class, () -> dualWriter().writeHeartbeat(storeName)),
        failure);
    verify(veniceWriterCacheMock, never()).prepareClusterVeniceWriter(any());
  }

  @Test
  public void testDualDeleteWaitsForBothStoresAndPropagatesFailure() throws Exception {
    CompletableFuture<PubSubProduceResult> legacyDelete = new CompletableFuture<>();
    CompletableFuture<PubSubProduceResult> clusterDelete = new CompletableFuture<>();
    when(veniceWriterMock.delete(any(), any())).thenReturn(legacyDelete);
    when(clusterWriter.delete(any(), any())).thenReturn(clusterDelete);
    Future<PubSubProduceResult> deletion =
        dualWriter().deletePartitionIncrementalPushStatus(storeName, storeVersion, incPushVersion, 0);
    legacyDelete.complete(null);
    Assert.assertFalse(deletion.isDone());
    clusterDelete.completeExceptionally(new IllegalStateException("cluster delete failed"));
    try {
      deletion.get(10, TimeUnit.SECONDS);
      Assert.fail("Cluster deletion failure must propagate");
    } catch (ExecutionException e) {
      Assert.assertEquals(e.getCause().getMessage(), "cluster delete failed");
    }
  }

  @Test(dataProvider = "Three-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testDeletePushStatusWaitsForEveryTombstone(
      boolean failDeletion,
      boolean versionKey,
      boolean migrationCleanup) throws Exception {
    PushStatusKey pendingKey = versionKey
        ? PushStatusStoreUtils.getPushKey(storeVersion)
        : PushStatusStoreUtils.getPushKey(storeVersion, 1, Optional.empty());
    CompletableFuture<PubSubProduceResult> legacyDelete = new CompletableFuture<>();
    CompletableFuture<PubSubProduceResult> clusterDelete = new CompletableFuture<>();
    when(veniceWriterMock.delete(eq(pendingKey), any())).thenReturn(legacyDelete);
    when(clusterWriter.delete(eq(PushStatusStoreUtils.getClusterPushStatusKey(storeName, pendingKey)), any()))
        .thenReturn(clusterDelete);
    try (PushStatusStoreWriter writer = dualWriter()) {
      CompletableFuture<Void> deletion =
          writer.deletePushStatus("cluster", storeName, storeVersion, Optional.empty(), 2, migrationCleanup);
      Assert.assertFalse(deletion.isDone());
      legacyDelete.complete(null);
      Assert.assertFalse(deletion.isDone(), "Cluster acknowledgements must be included");
      if (failDeletion) {
        clusterDelete.completeExceptionally(new IllegalStateException("cluster tombstone failed"));
        ExecutionException failure =
            Assert.expectThrows(ExecutionException.class, () -> deletion.get(10, TimeUnit.SECONDS));
        Assert.assertEquals(failure.getCause().getMessage(), "cluster tombstone failed");
      } else {
        clusterDelete.complete(null);
        deletion.get(10, TimeUnit.SECONDS);
      }
      if (migrationCleanup) {
        verify(veniceWriterCacheMock, never()).prepareVeniceWriter(any());
        verify(veniceWriterMock, never()).delete(any(), any());
        verify(clusterWriter, times(3)).delete(any(), any());
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExplicitDeleteUsesOwningClusterInsteadOfDiscovery() throws Exception {
    Function<String, String> resolver = mock(Function.class);
    when(resolver.apply(storeName)).thenReturn("surviving-cluster");
    try (PushStatusStoreWriter writer = writer(resolver, 1024)) {
      writer.deletePushStatus("cluster", storeName, storeVersion, Optional.empty(), 1).get(10, TimeUnit.SECONDS);
      verify(veniceWriterCacheMock, times(2)).prepareClusterVeniceWriter("cluster");
      writer.deletePushStatus(null, storeName, storeVersion, Optional.empty(), 1).get(10, TimeUnit.SECONDS);
      verify(veniceWriterCacheMock, times(2)).prepareClusterVeniceWriter(any());
      verify(resolver, never()).apply(any());
    }
  }

  @Test
  public void testMigrationCleanupWithoutClusterTargetPreservesAllRecords() throws Exception {
    try (PushStatusStoreWriter writer = dualWriter()) {
      writer.deletePushStatus(null, storeName, storeVersion, Optional.empty(), 2, true).get(10, TimeUnit.SECONDS);
      verify(veniceWriterCacheMock, never()).prepareVeniceWriter(any());
      verify(veniceWriterCacheMock, never()).prepareClusterVeniceWriter(any());
      verify(veniceWriterMock, never()).delete(any(), any());
      verify(clusterWriter, never()).delete(any(), any());
    }
  }

  private PushStatusStoreWriter dualWriter() {
    return dualWriter(1024);
  }

  private PushStatusStoreWriter dualWriter(int queueCapacity) {
    return writer(store -> "cluster", queueCapacity);
  }

  private PushStatusStoreWriter writer(Function<String, String> resolver, int queueCapacity) {
    PushStatusStoreWriter writer = new PushStatusStoreWriter(
        veniceWriterCacheMock,
        instanceName,
        valueSchemaId,
        derivedSchemaId,
        updateSchema,
        resolver,
        queueCapacity);
    clusterWriters.add(writer);
    return writer;
  }

  private void assertRejected(Future<?> operation) {
    Assert.assertTrue(
        Assert.expectThrows(ExecutionException.class, () -> operation.get(10, TimeUnit.SECONDS))
            .getCause() instanceof RejectedExecutionException);
  }

  private class BlockedClusterWork implements AutoCloseable {
    private final CountDownLatch started = new CountDownLatch(1);
    private final CompletableFuture<Void> release = new CompletableFuture<>();
    private final CountDownLatch updates;

    private BlockedClusterWork(boolean blockCreation, int expectedUpdates) {
      updates = new CountDownLatch(expectedUpdates);
      if (blockCreation) {
        when(veniceWriterCacheMock.prepareClusterVeniceWriter("cluster")).thenAnswer(invocation -> {
          awaitRelease();
          return clusterWriter;
        });
      }
      when(clusterWriter.update(any(), any(), eq(valueSchemaId), eq(derivedSchemaId), any())).thenAnswer(invocation -> {
        if (!blockCreation) {
          awaitRelease();
        }
        updates.countDown();
        return CompletableFuture.completedFuture(null);
      });
    }

    private void awaitRelease() {
      started.countDown();
      release.join();
    }

    @Override
    public void close() {
      release.complete(null);
    }
  }

  @Test
  public void testClusterWriterIsSharedAndSurvivesUserStoreRemoval() {
    VeniceWriterFactory factory = mock(VeniceWriterFactory.class);
    when(factory.createVeniceWriter(any())).thenReturn(veniceWriterMock, clusterWriter);
    try (PushStatusStoreVeniceWriterCache cache =
        new PushStatusStoreVeniceWriterCache(factory, valueSchema, updateSchema)) {
      Assert.assertSame(cache.prepareVeniceWriter(storeName), veniceWriterMock);
      Assert.assertSame(cache.prepareClusterVeniceWriter("cluster"), clusterWriter);
      Assert.assertSame(cache.prepareClusterVeniceWriter("cluster"), clusterWriter);
      ArgumentCaptor<VeniceWriterOptions> options = ArgumentCaptor.forClass(VeniceWriterOptions.class);
      verify(factory, times(2)).createVeniceWriter(options.capture());
      Assert.assertEquals(
          options.getAllValues().get(1).getTopicName(),
          Utils.composeRealTimeTopic(VeniceSystemStoreUtils.getDaVinciPushStatusStoreNameForCluster("cluster")));
      Assert.assertEquals(options.getAllValues().get(0).getPartitionCount().intValue(), 1);
      Assert.assertEquals(options.getAllValues().get(1).getPartitionCount().intValue(), 200);
      cache.removeVeniceWriter(storeName);
      verify(veniceWriterMock).close();
      verify(clusterWriter, never()).close();
    }
    verify(clusterWriter).close();
    verify(veniceWriterMock).close();
  }

  @Test
  public void testWriteHeartbeat() {
    long heartbeat = 12345L;
    PushStatusKey statusKey = PushStatusStoreUtils.getHeartbeatKey(instanceName);
    pushStatusStoreWriter.writeHeartbeat(storeName, heartbeat);
    verify(veniceWriterCacheMock).prepareVeniceWriter(storeName);
    verify(veniceWriterMock)
        .update(eq(statusKey), eq(getHeartbeatRecord(heartbeat)), eq(valueSchemaId), eq(derivedSchemaId), eq(null));
  }

  @Test
  public void testWriteHeartbeatForBootstrappingInstance() {
    PushStatusKey statusKey = PushStatusStoreUtils.getHeartbeatKey(instanceName);
    pushStatusStoreWriter.writeHeartbeatForBootstrappingInstance(storeName);
    verify(veniceWriterCacheMock).prepareVeniceWriter(storeName);
    verify(veniceWriterMock)
        .update(eq(statusKey), eq(getHeartbeatRecord(-1)), eq(valueSchemaId), eq(derivedSchemaId), eq(null));
  }

  @Test
  public void testPushStatusStoreWriterLogging() throws Exception {
    // Add the test appender to the class
    TestLogAppender testLogAppender = new TestLogAppender("TestLogAppender", PatternLayout.createDefaultLayout());
    testLogAppender.start();

    // Add appender to the logger
    Logger logger = (Logger) LogManager.getLogger(PushStatusStoreWriter.class);
    logger.addAppender(testLogAppender);

    // Mock the produce callback behavior
    Field callbackField = PushStatusStoreWriter.class.getDeclaredField("PUSH_STATUS_UPDATE_LOGGER_CALLBACK");
    callbackField.setAccessible(true);
    PubSubProducerCallback callback = (PubSubProducerCallback) callbackField.get(null);

    // Mock the produce result
    PubSubProduceResult produceResult = Mockito.mock(PubSubProduceResult.class);
    Mockito.when(produceResult.getTopic()).thenReturn("test-topic");
    Mockito.when(produceResult.getPubSubPosition()).thenReturn(ApacheKafkaOffsetPosition.of(12345L));

    // Test successful completion path
    callback.onCompletion(produceResult, null);

    // Verify logging
    String capturedLog = testLogAppender.getLog();
    Assert.assertTrue(
        capturedLog.contains("Updated push status into topic: test-topic-0 at position:"),
        "Got: " + capturedLog);
    Assert.assertTrue(capturedLog.contains("12345"));

    // Test failure path
    Exception exception = new Exception("Test error");
    callback.onCompletion(produceResult, exception);

    // Verify logging
    capturedLog = testLogAppender.getLog();
    Assert.assertTrue(capturedLog.contains("Failed to update push status"));
  }
}
