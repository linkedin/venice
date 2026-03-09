package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.blobtransfer.BlobTransferManager;
import com.linkedin.davinci.blobtransfer.BlobTransferStatusTrackingManager;
import com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferTableFormat;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.stats.AggVersionedBlobTransferStats;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.davinci.store.StoragePartitionAdjustmentTrigger;
import com.linkedin.davinci.store.rocksdb.RocksDBServerConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubContext;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.utils.Utils;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class BlobTransferIngestionHelperTest {
  private static final String STORE_NAME = "testStore";
  private static final int VERSION_NUMBER = 1;
  private static final int PARTITION = 0;
  private static final String STORE_VERSION = Version.composeKafkaTopic(STORE_NAME, VERSION_NUMBER);
  private static final String REPLICA_ID = Utils.getReplicaId(STORE_VERSION, PARTITION);

  @Mock
  private BlobTransferManager blobTransferManager;
  @Mock
  private StorageService storageService;
  @Mock
  private StorageMetadataService storageMetadataService;
  @Mock
  private VeniceServerConfig serverConfig;
  @Mock
  private BlobTransferStatusTrackingManager statusTrackingManager;
  @Mock
  private AggVersionedBlobTransferStats blobTransferStats;
  @Mock
  private OffsetRecord offsetRecord;
  @Mock
  private PubSubContext pubSubContext;

  private BlobTransferIngestionHelper helper;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    when(blobTransferManager.getTransferStatusTrackingManager()).thenReturn(statusTrackingManager);
    when(blobTransferManager.getAggVersionedBlobTransferStats()).thenReturn(blobTransferStats);
    helper = new BlobTransferIngestionHelper(blobTransferManager, storageService, storageMetadataService, serverConfig);
  }

  @Test
  public void testIsReplicaLaggedWhenOffsetRecordIsNull() {
    when(storageMetadataService.getLastOffset(anyString(), anyInt(), any())).thenReturn(null);
    assertTrue(
        helper.isReplicaLaggedAndNeedBlobTransfer(
            STORE_NAME,
            VERSION_NUMBER,
            PARTITION,
            1000,
            0,
            true,
            REPLICA_ID,
            pubSubContext));
  }

  @Test
  public void testIsReplicaLaggedNegativeThreshold() {
    when(storageMetadataService.getLastOffset(anyString(), anyInt(), any())).thenReturn(offsetRecord);
    assertTrue(
        helper.isReplicaLaggedAndNeedBlobTransfer(
            STORE_NAME,
            VERSION_NUMBER,
            PARTITION,
            -1,
            0,
            true,
            REPLICA_ID,
            pubSubContext));
  }

  @Test
  public void testIsReplicaNotLaggedBatchStoreWithEOP() {
    when(storageMetadataService.getLastOffset(anyString(), anyInt(), any())).thenReturn(offsetRecord);
    when(offsetRecord.isEndOfPushReceived()).thenReturn(true);
    assertFalse(
        helper.isReplicaLaggedAndNeedBlobTransfer(
            STORE_NAME,
            VERSION_NUMBER,
            PARTITION,
            1000,
            0,
            false,
            REPLICA_ID,
            pubSubContext));
  }

  @Test
  public void testIsReplicaLaggedBatchStoreWithoutEOP() {
    when(storageMetadataService.getLastOffset(anyString(), anyInt(), any())).thenReturn(offsetRecord);
    when(offsetRecord.isEndOfPushReceived()).thenReturn(false);
    assertTrue(
        helper.isReplicaLaggedAndNeedBlobTransfer(
            STORE_NAME,
            VERSION_NUMBER,
            PARTITION,
            1000,
            0,
            false,
            REPLICA_ID,
            pubSubContext));
  }

  @Test
  public void testIsReplicaNotLaggedHybridStoreWithinOffsetThreshold() {
    when(storageMetadataService.getLastOffset(anyString(), anyInt(), any())).thenReturn(offsetRecord);
    when(offsetRecord.getOffsetLag()).thenReturn(10L);
    when(offsetRecord.getCheckpointedLocalVtPosition())
        .thenReturn(com.linkedin.venice.pubsub.PubSubUtil.fromKafkaOffset(10L));
    assertFalse(
        helper.isReplicaLaggedAndNeedBlobTransfer(
            STORE_NAME,
            VERSION_NUMBER,
            PARTITION,
            1000,
            0,
            true,
            REPLICA_ID,
            pubSubContext));
  }

  @Test
  public void testIsReplicaLaggedHybridStoreExceedsOffsetThreshold() {
    when(storageMetadataService.getLastOffset(anyString(), anyInt(), any())).thenReturn(offsetRecord);
    when(offsetRecord.getOffsetLag()).thenReturn(2000L);
    when(offsetRecord.getCheckpointedLocalVtPosition())
        .thenReturn(com.linkedin.venice.pubsub.PubSubUtil.fromKafkaOffset(10L));
    assertTrue(
        helper.isReplicaLaggedAndNeedBlobTransfer(
            STORE_NAME,
            VERSION_NUMBER,
            PARTITION,
            1000,
            0,
            true,
            REPLICA_ID,
            pubSubContext));
  }

  @Test
  public void testIsReplicaLaggedHybridStoreZeroOffsetWithEarliest() {
    when(storageMetadataService.getLastOffset(anyString(), anyInt(), any())).thenReturn(offsetRecord);
    when(offsetRecord.getOffsetLag()).thenReturn(0L);
    when(offsetRecord.getCheckpointedLocalVtPosition()).thenReturn(PubSubSymbolicPosition.EARLIEST);
    assertTrue(
        helper.isReplicaLaggedAndNeedBlobTransfer(
            STORE_NAME,
            VERSION_NUMBER,
            PARTITION,
            0,
            0,
            true,
            REPLICA_ID,
            pubSubContext));
  }

  @Test
  public void testIsReplicaLaggedTimeLagThreshold() {
    when(storageMetadataService.getLastOffset(anyString(), anyInt(), any())).thenReturn(offsetRecord);
    // Heartbeat 2 minutes ago
    when(offsetRecord.getHeartbeatTimestamp()).thenReturn(System.currentTimeMillis() - 120000);
    // Time lag threshold is 1 minute → lagged
    assertTrue(
        helper.isReplicaLaggedAndNeedBlobTransfer(
            STORE_NAME,
            VERSION_NUMBER,
            PARTITION,
            1000,
            1,
            true,
            REPLICA_ID,
            pubSubContext));
    // Time lag threshold is 3 minutes → not lagged
    assertFalse(
        helper.isReplicaLaggedAndNeedBlobTransfer(
            STORE_NAME,
            VERSION_NUMBER,
            PARTITION,
            1000,
            3,
            true,
            REPLICA_ID,
            pubSubContext));
  }

  @Test
  public void testBootstrapFromBlobsNotLagged() {
    when(storageMetadataService.getLastOffset(anyString(), anyInt(), any())).thenReturn(offsetRecord);
    when(offsetRecord.getOffsetLag()).thenReturn(10L);
    when(offsetRecord.getCheckpointedLocalVtPosition())
        .thenReturn(com.linkedin.venice.pubsub.PubSubUtil.fromKafkaOffset(10L));
    when(serverConfig.getBlobTransferDisabledOffsetLagThreshold()).thenReturn(1000L);
    when(serverConfig.getBlobTransferDisabledTimeLagThresholdInMinutes()).thenReturn(0);

    VeniceStoreVersionConfig storeConfig = mock(VeniceStoreVersionConfig.class);
    when(storeConfig.getStoreVersionName()).thenReturn(STORE_VERSION);

    CompletionStage<Void> result = helper.bootstrapFromBlobs(
        STORE_NAME,
        VERSION_NUMBER,
        PARTITION,
        BlobTransferTableFormat.BLOCK_BASED_TABLE,
        true,
        storeConfig,
        () -> null,
        REPLICA_ID,
        pubSubContext);

    assertTrue(result.toCompletableFuture().isDone());
    // Should not call blob transfer since replica is not lagged
    verify(blobTransferManager, never()).get(anyString(), anyInt(), anyInt(), any());
  }

  @Test
  public void testBootstrapFromBlobsLaggedAndSuccessful() {
    when(storageMetadataService.getLastOffset(anyString(), anyInt(), any())).thenReturn(offsetRecord);
    when(offsetRecord.getOffsetLag()).thenReturn(0L);
    when(offsetRecord.getCheckpointedLocalVtPosition()).thenReturn(PubSubSymbolicPosition.EARLIEST);
    when(serverConfig.getBlobTransferDisabledOffsetLagThreshold()).thenReturn(0L);
    when(serverConfig.getBlobTransferDisabledTimeLagThresholdInMinutes()).thenReturn(0);
    when(serverConfig.getRocksDBPath()).thenReturn("/tmp/test-rocksdb");

    StorageEngine storageEngine = mock(StorageEngine.class);
    when(storageEngine.getStoreVersionName()).thenReturn(STORE_VERSION);
    when(storageService.getStorageEngine(STORE_VERSION)).thenReturn(storageEngine);
    when(storageEngine.containsPartition(PARTITION)).thenReturn(false);

    VeniceStoreVersionConfig storeConfig = mock(VeniceStoreVersionConfig.class);
    when(storeConfig.getStoreVersionName()).thenReturn(STORE_VERSION);

    CompletableFuture<InputStream> transferFuture = CompletableFuture.completedFuture(null);
    when(blobTransferManager.get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION), any())).thenReturn(transferFuture);

    doNothing().when(storageEngine)
        .adjustStoragePartition(eq(PARTITION), eq(StoragePartitionAdjustmentTrigger.END_BLOB_TRANSFER), any());

    CompletionStage<Void> result = helper.bootstrapFromBlobs(
        STORE_NAME,
        VERSION_NUMBER,
        PARTITION,
        BlobTransferTableFormat.BLOCK_BASED_TABLE,
        true,
        storeConfig,
        () -> null,
        REPLICA_ID,
        pubSubContext);

    assertTrue(result.toCompletableFuture().isDone());
    verify(blobTransferManager).get(eq(STORE_NAME), eq(VERSION_NUMBER), eq(PARTITION), any());
    verify(statusTrackingManager).markTransferCompleted(eq(REPLICA_ID));
  }

  @Test
  public void testCancelTransfer() {
    helper.cancelTransfer(REPLICA_ID);
    verify(statusTrackingManager).cancelTransfer(eq(REPLICA_ID));
  }

  @Test
  public void testGetRequestTableFormat() {
    RocksDBServerConfig rocksDBServerConfig = mock(RocksDBServerConfig.class);
    when(serverConfig.getRocksDBServerConfig()).thenReturn(rocksDBServerConfig);

    when(rocksDBServerConfig.isRocksDBPlainTableFormatEnabled()).thenReturn(false);
    assertTrue(helper.getRequestTableFormat() == BlobTransferTableFormat.BLOCK_BASED_TABLE);

    when(rocksDBServerConfig.isRocksDBPlainTableFormatEnabled()).thenReturn(true);
    assertTrue(helper.getRequestTableFormat() == BlobTransferTableFormat.PLAIN_TABLE);
  }

  @Test
  public void testAdjustStoragePartitionWhenBlobTransferComplete() {
    StorageEngine storageEngine = mock(StorageEngine.class);
    when(storageEngine.getStoreVersionName()).thenReturn(STORE_VERSION);
    doNothing().when(storageEngine)
        .adjustStoragePartition(eq(PARTITION), eq(StoragePartitionAdjustmentTrigger.END_BLOB_TRANSFER), any());

    helper.adjustStoragePartitionWhenBlobTransferComplete(storageEngine, PARTITION, REPLICA_ID);

    verify(storageEngine)
        .adjustStoragePartition(eq(PARTITION), eq(StoragePartitionAdjustmentTrigger.END_BLOB_TRANSFER), any());
  }

  @Test
  public void testAdjustStoragePartitionFallsBackToDropOnException() {
    StorageEngine storageEngine = mock(StorageEngine.class);
    when(storageEngine.getStoreVersionName()).thenThrow(new RuntimeException("test error"));

    helper.adjustStoragePartitionWhenBlobTransferComplete(storageEngine, PARTITION, REPLICA_ID);

    verify(storageEngine).dropPartition(PARTITION, false);
  }
}
