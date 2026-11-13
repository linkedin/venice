package com.linkedin.davinci.kafka.consumer;

import static com.linkedin.venice.utils.TestUtils.DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.offsets.OffsetRecord;
import java.util.Collections;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class BlobTransferCheckpointDiagnosticsTest {
  @DataProvider
  public Object[][] enabled() {
    return new Object[][] { { false }, { true } };
  }

  @Test(dataProvider = "enabled")
  public void testOriginalMetadataLoggedBeforePartitionReplacementOnlyWhenEnabled(boolean enabled) {
    VeniceServerConfig config = mock(VeniceServerConfig.class);
    when(config.isIngestionCheckpointDiagnosticsEnabled()).thenReturn(enabled);
    when(config.getBlobTransferDisabledTimeLagThresholdInMinutes()).thenReturn(60);
    StorageMetadataService metadata = mock(StorageMetadataService.class);
    OffsetRecord original = mock(OffsetRecord.class);
    when(original.getHeartbeatTimestamp()).thenReturn(123L);
    when(original.getLastCheckpointTimestamp()).thenReturn(456L);
    when(original.getOffsetLag()).thenReturn(789L);
    when(metadata.getLastOffset("test_v1", 8, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING)).thenReturn(original);
    BlobTransferIngestionHelper helper =
        new BlobTransferIngestionHelper(null, mock(StorageService.class), metadata, config, Collections.emptySet());

    try (ShutdownCheckpointDiagnosticsTest.Logs logs =
        new ShutdownCheckpointDiagnosticsTest.Logs(BlobTransferIngestionHelper.class)) {
      assertTrue(
          helper.isReplicaLaggedAndNeedBlobTransfer(
              "test",
              1,
              8,
              "test_v1_8",
              true,
              "test_v1",
              DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING));
      assertEquals(logs.diagnostics().size(), enabled ? 1 : 0);
      assertTrue(logs.appender.getLogs().stream().anyMatch(line -> line.startsWith("Time lag check for replica")));
      verify(metadata, times(1)).getLastOffset("test_v1", 8, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
      if (!enabled) {
        verify(original, never()).getLastCheckpointTimestamp();
      }

      StorageEngine engine = mock(StorageEngine.class);
      when(engine.containsPartition(8)).thenReturn(true);
      PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
      when(pcs.getReplicaId()).thenReturn("test_v1_8");
      doAnswer(invocation -> {
        if (enabled) {
          String saved = logs.diagnostics().get(0);
          assertTrue(saved.contains("savedHeartbeat=123, savedCheckpoint=456"));
          assertTrue(saved.contains("thresholdMs=3600000, offsetLag=789, isLagged=true"));
        }
        // Stop at the replacement boundary; no directories or actual storage are touched.
        throw new IllegalStateException("replacement boundary");
      }).when(engine).dropPartition(8, false);
      assertThrows(
          IllegalStateException.class,
          () -> helper.startBlobTransferAsyncForPartition(8, pcs, engine, "test", 1, null, "test_v1"));
    }
  }
}
