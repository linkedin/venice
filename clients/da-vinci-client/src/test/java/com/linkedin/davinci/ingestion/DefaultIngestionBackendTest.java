package com.linkedin.davinci.ingestion;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreVersionInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit test for {@link DefaultIngestionBackend}
 *
 * Blob transfer orchestration tests have been moved to StoreIngestionTask tests,
 * since blob transfer is now handled inside SIT's processCommonConsumerAction and
 * checkLongRunningTaskState.
 */
public class DefaultIngestionBackendTest {
  @Mock
  private StorageMetadataService storageMetadataService;
  @Mock
  private KafkaStoreIngestionService storeIngestionService;
  @Mock
  private StorageService storageService;
  @Mock
  private VeniceStoreVersionConfig storeConfig;
  @Mock
  private StorageEngine storageEngine;
  @Mock
  private ReadOnlyStoreRepository metadataRepo;
  @Mock
  private VeniceServerConfig veniceServerConfig;
  @Mock
  private Store store;
  @Mock
  private Version version;
  @Mock
  private StoreVersionState storeVersionState;

  private DefaultIngestionBackend ingestionBackend;

  private static final int VERSION_NUMBER = 1;
  private static final int PARTITION = 1;
  private static final String STORE_NAME = "testStore";
  private static final String STORE_VERSION = "store_v1";
  private static final String REPLICA_ID = Utils.getReplicaId(STORE_VERSION, PARTITION);

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    when(store.getName()).thenReturn(STORE_NAME);
    when(version.getNumber()).thenReturn(VERSION_NUMBER);
    StoreVersionInfo storeAndVersion = new StoreVersionInfo(store, version);

    when(storeConfig.getStoreVersionName()).thenReturn(STORE_VERSION);
    when(storeIngestionService.getMetadataRepo()).thenReturn(metadataRepo);
    doNothing().when(storeIngestionService).startConsumption(any(VeniceStoreVersionConfig.class), anyInt(), any());
    when(metadataRepo.waitVersion(anyString(), anyInt(), any(Duration.class))).thenReturn(storeAndVersion);
    when(storageMetadataService.getStoreVersionState(STORE_VERSION)).thenReturn(storeVersionState);
    when(storageService.openStoreForNewPartition(eq(storeConfig), eq(PARTITION), any())).thenReturn(storageEngine);

    ingestionBackend =
        new DefaultIngestionBackend(storageMetadataService, storeIngestionService, storageService, veniceServerConfig);
  }

  @Test
  public void testStartConsumption() {
    ingestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty(), REPLICA_ID);

    // Verify store is opened and consumption is started via ingestion service
    verify(storageService).openStoreForNewPartition(eq(storeConfig), eq(PARTITION), any());
    verify(storeIngestionService).startConsumption(eq(storeConfig), eq(PARTITION), any());
  }

  @Test
  public void testStartConsumptionSyncsBlobTransferConfig() {
    when(store.isBlobTransferEnabled()).thenReturn(true);

    ingestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty(), REPLICA_ID);

    // Verify blob transfer flag is synced from store metadata to store config
    verify(storeIngestionService).startConsumption(eq(storeConfig), eq(PARTITION), any());

    // Reset replica state for next iteration
    ingestionBackend.shutdownIngestionTask(storeConfig.getStoreVersionName());
  }

  @Test
  public void testStartConsumptionDoesNotSyncBlobTransferWhenDisabled() {
    when(store.isBlobTransferEnabled()).thenReturn(false);

    ingestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty(), REPLICA_ID);

    // When store doesn't have blob transfer enabled, setBlobTransferEnabled should not be called
    verify(storeConfig, org.mockito.Mockito.never()).setBlobTransferEnabled(true);
    verify(storeIngestionService).startConsumption(eq(storeConfig), eq(PARTITION), any());
  }

  @Test
  public void testStopConsumption() {
    when(storeIngestionService.stopConsumption(any(), anyInt())).thenReturn(CompletableFuture.completedFuture(null));

    // Start consumption first to set state to RUNNING
    ingestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty(), REPLICA_ID);

    ingestionBackend.stopConsumption(storeConfig, PARTITION, REPLICA_ID);

    verify(storeIngestionService).stopConsumption(storeConfig, PARTITION);
  }

  @Test
  public void testStopConsumption_MinimalBackend() {
    DefaultIngestionBackend backend =
        new DefaultIngestionBackend(storageMetadataService, storeIngestionService, storageService, veniceServerConfig);

    backend.startConsumption(storeConfig, PARTITION, Optional.empty(), REPLICA_ID);

    backend.stopConsumption(storeConfig, PARTITION, REPLICA_ID);
    verify(storeIngestionService).stopConsumption(storeConfig, PARTITION);
  }

  @Test
  public void testDropStoragePartitionGracefully() {
    doNothing().when(storeIngestionService).stopConsumptionAndWait(any(), anyInt(), anyInt(), anyInt(), eq(true));
    when(storeIngestionService.dropStoragePartitionGracefully(any(), anyInt()))
        .thenReturn(CompletableFuture.completedFuture(null));

    ingestionBackend.dropStoragePartitionGracefully(storeConfig, PARTITION, 5, true, REPLICA_ID);

    verify(storeIngestionService).stopConsumptionAndWait(eq(storeConfig), eq(PARTITION), eq(1), eq(5), eq(true));
    verify(storeIngestionService).dropStoragePartitionGracefully(storeConfig, PARTITION);
  }

  @Test
  public void testHasCurrentVersionBootstrapping() {
    KafkaStoreIngestionService mockIngestionService = mock(KafkaStoreIngestionService.class);
    DefaultIngestionBackend ingestionBackend = new DefaultIngestionBackend(null, mockIngestionService, null, null);
    doReturn(true).when(mockIngestionService).hasCurrentVersionBootstrapping();
    assertTrue(ingestionBackend.hasCurrentVersionBootstrapping());

    doReturn(false).when(mockIngestionService).hasCurrentVersionBootstrapping();
    assertFalse(ingestionBackend.hasCurrentVersionBootstrapping());
  }

  @Test
  public void testKillConsumptionTask() {
    String topicName = Version.composeKafkaTopic(STORE_NAME, VERSION_NUMBER);
    ingestionBackend.killConsumptionTask(topicName);
    verify(storeIngestionService).killConsumptionTask(topicName);
  }

  @Test
  public void testShutdownIngestionTask() {
    String topicName = Version.composeKafkaTopic(STORE_NAME, VERSION_NUMBER);
    ingestionBackend.shutdownIngestionTask(topicName);
    verify(storeIngestionService).shutdownStoreIngestionTask(topicName);
  }

  @Test
  public void testRemoveStorageEngine() {
    String topicName = Version.composeKafkaTopic(STORE_NAME, VERSION_NUMBER);
    ingestionBackend.removeStorageEngine(topicName);
    verify(storageService).removeStorageEngine(topicName);
  }

  @Test
  public void testShutdownIngestionTaskClearsReplicaState() {
    String topicName = storeConfig.getStoreVersionName();
    String replicaId = Utils.getReplicaId(topicName, PARTITION);

    // Start consumption to set state to RUNNING
    ingestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty(), REPLICA_ID);
    assertEquals(
        ingestionBackend.getReplicaIntendedState(replicaId),
        DefaultIngestionBackend.ReplicaIntendedState.RUNNING);

    // Shutdown ingestion task should clean up replica state
    ingestionBackend.shutdownIngestionTask(topicName);

    // State should be NOT_EXIST, allowing re-subscription
    assertEquals(
        ingestionBackend.getReplicaIntendedState(replicaId),
        DefaultIngestionBackend.ReplicaIntendedState.NOT_EXIST);
  }

  // --- ReplicaIntendedState tests ---

  @Test
  public void testReplicaIntendedState_NormalStartSetsRunning() {
    assertEquals(
        ingestionBackend.getReplicaIntendedState(REPLICA_ID),
        DefaultIngestionBackend.ReplicaIntendedState.NOT_EXIST);

    ingestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty(), REPLICA_ID);

    assertEquals(
        ingestionBackend.getReplicaIntendedState(REPLICA_ID),
        DefaultIngestionBackend.ReplicaIntendedState.RUNNING);
  }

  @Test
  public void testReplicaIntendedState_DuplicateStartIgnored() {
    ingestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty(), REPLICA_ID);
    assertEquals(
        ingestionBackend.getReplicaIntendedState(REPLICA_ID),
        DefaultIngestionBackend.ReplicaIntendedState.RUNNING);

    // Second start should be ignored (no extra openStoreForNewPartition call)
    ingestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty(), REPLICA_ID);
    assertEquals(
        ingestionBackend.getReplicaIntendedState(REPLICA_ID),
        DefaultIngestionBackend.ReplicaIntendedState.RUNNING);

    // openStoreForNewPartition should only be called once
    verify(storageService).openStoreForNewPartition(eq(storeConfig), eq(PARTITION), any());
  }

  @Test
  public void testReplicaIntendedState_StopSetsStoppedFromRunning() {
    when(storeIngestionService.stopConsumption(any(), anyInt())).thenReturn(CompletableFuture.completedFuture(null));

    ingestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty(), REPLICA_ID);
    ingestionBackend.stopConsumption(storeConfig, PARTITION, REPLICA_ID);

    assertEquals(
        ingestionBackend.getReplicaIntendedState(REPLICA_ID),
        DefaultIngestionBackend.ReplicaIntendedState.STOPPED);
  }

  @Test
  public void testReplicaIntendedState_StopWithoutStartIsNoop() {
    when(storeIngestionService.stopConsumption(any(), anyInt())).thenReturn(CompletableFuture.completedFuture(null));

    ingestionBackend.stopConsumption(storeConfig, PARTITION, REPLICA_ID);

    // State should remain NOT_EXIST since stop was a no-op
    assertEquals(
        ingestionBackend.getReplicaIntendedState(REPLICA_ID),
        DefaultIngestionBackend.ReplicaIntendedState.NOT_EXIST);
    // stopConsumption on the ingestion service should NOT be called
    verify(storeIngestionService, never()).stopConsumption(any(), anyInt());
  }

  @Test
  public void testReplicaIntendedState_DuplicateStopIsNoop() {
    when(storeIngestionService.stopConsumption(any(), anyInt())).thenReturn(CompletableFuture.completedFuture(null));

    ingestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty(), REPLICA_ID);
    ingestionBackend.stopConsumption(storeConfig, PARTITION, REPLICA_ID);
    ingestionBackend.stopConsumption(storeConfig, PARTITION, REPLICA_ID);

    assertEquals(
        ingestionBackend.getReplicaIntendedState(REPLICA_ID),
        DefaultIngestionBackend.ReplicaIntendedState.STOPPED);
    // stopConsumption on the ingestion service should only be called once
    verify(storeIngestionService).stopConsumption(any(), anyInt());
  }

  @Test
  public void testReplicaIntendedState_DropFromRunningSetsNotExist() {
    doNothing().when(storeIngestionService).stopConsumptionAndWait(any(), anyInt(), anyInt(), anyInt(), anyBoolean());
    when(storeIngestionService.dropStoragePartitionGracefully(any(), anyInt()))
        .thenReturn(CompletableFuture.completedFuture(null));

    ingestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty(), REPLICA_ID);
    ingestionBackend.dropStoragePartitionGracefully(storeConfig, PARTITION, 5, true, REPLICA_ID);

    assertEquals(
        ingestionBackend.getReplicaIntendedState(REPLICA_ID),
        DefaultIngestionBackend.ReplicaIntendedState.NOT_EXIST);
  }

  @Test
  public void testReplicaIntendedState_DropFromStoppedSetsNotExist() {
    when(storeIngestionService.stopConsumption(any(), anyInt())).thenReturn(CompletableFuture.completedFuture(null));
    doNothing().when(storeIngestionService).stopConsumptionAndWait(any(), anyInt(), anyInt(), anyInt(), anyBoolean());
    when(storeIngestionService.dropStoragePartitionGracefully(any(), anyInt()))
        .thenReturn(CompletableFuture.completedFuture(null));

    ingestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty(), REPLICA_ID);
    ingestionBackend.stopConsumption(storeConfig, PARTITION, REPLICA_ID);
    ingestionBackend.dropStoragePartitionGracefully(storeConfig, PARTITION, 5, true, REPLICA_ID);

    assertEquals(
        ingestionBackend.getReplicaIntendedState(REPLICA_ID),
        DefaultIngestionBackend.ReplicaIntendedState.NOT_EXIST);
  }

  @Test
  public void testReplicaIntendedState_DropFromNotExistSetsNotExist() {
    doNothing().when(storeIngestionService).stopConsumptionAndWait(any(), anyInt(), anyInt(), anyInt(), anyBoolean());
    when(storeIngestionService.dropStoragePartitionGracefully(any(), anyInt()))
        .thenReturn(CompletableFuture.completedFuture(null));

    ingestionBackend.dropStoragePartitionGracefully(storeConfig, PARTITION, 5, true, REPLICA_ID);

    assertEquals(
        ingestionBackend.getReplicaIntendedState(REPLICA_ID),
        DefaultIngestionBackend.ReplicaIntendedState.NOT_EXIST);
  }

  @Test
  public void testReplicaIntendedState_StartAfterStopWaitsAndSetsRunning() {
    when(storeIngestionService.stopConsumption(any(), anyInt())).thenReturn(CompletableFuture.completedFuture(null));
    doNothing().when(storeIngestionService).stopConsumptionAndWait(any(), anyInt(), anyInt(), anyInt(), anyBoolean());

    ingestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty(), REPLICA_ID);
    ingestionBackend.stopConsumption(storeConfig, PARTITION, REPLICA_ID);

    assertEquals(
        ingestionBackend.getReplicaIntendedState(REPLICA_ID),
        DefaultIngestionBackend.ReplicaIntendedState.STOPPED);

    // Start again after stop should wait for stop to complete, then set to RUNNING
    ingestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty(), REPLICA_ID);

    assertEquals(
        ingestionBackend.getReplicaIntendedState(REPLICA_ID),
        DefaultIngestionBackend.ReplicaIntendedState.RUNNING);

    // stopConsumptionAndWait should have been called to wait for the previous stop
    verify(storeIngestionService).stopConsumptionAndWait(eq(storeConfig), eq(PARTITION), anyInt(), anyInt(), eq(false));
  }

  @Test
  public void testReplicaIntendedState_FullLifecycle() {
    when(storeIngestionService.stopConsumption(any(), anyInt())).thenReturn(CompletableFuture.completedFuture(null));
    doNothing().when(storeIngestionService).stopConsumptionAndWait(any(), anyInt(), anyInt(), anyInt(), anyBoolean());
    when(storeIngestionService.dropStoragePartitionGracefully(any(), anyInt()))
        .thenReturn(CompletableFuture.completedFuture(null));

    // NOT_EXIST -> start -> RUNNING
    assertEquals(
        ingestionBackend.getReplicaIntendedState(REPLICA_ID),
        DefaultIngestionBackend.ReplicaIntendedState.NOT_EXIST);
    ingestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty(), REPLICA_ID);
    assertEquals(
        ingestionBackend.getReplicaIntendedState(REPLICA_ID),
        DefaultIngestionBackend.ReplicaIntendedState.RUNNING);

    // RUNNING -> stop -> STOPPED
    ingestionBackend.stopConsumption(storeConfig, PARTITION, REPLICA_ID);
    assertEquals(
        ingestionBackend.getReplicaIntendedState(REPLICA_ID),
        DefaultIngestionBackend.ReplicaIntendedState.STOPPED);

    // STOPPED -> start -> RUNNING (with wait)
    ingestionBackend.startConsumption(storeConfig, PARTITION, Optional.empty(), REPLICA_ID);
    assertEquals(
        ingestionBackend.getReplicaIntendedState(REPLICA_ID),
        DefaultIngestionBackend.ReplicaIntendedState.RUNNING);

    // RUNNING -> drop -> NOT_EXIST
    ingestionBackend.dropStoragePartitionGracefully(storeConfig, PARTITION, 5, true, REPLICA_ID);
    assertEquals(
        ingestionBackend.getReplicaIntendedState(REPLICA_ID),
        DefaultIngestionBackend.ReplicaIntendedState.NOT_EXIST);
  }
}
