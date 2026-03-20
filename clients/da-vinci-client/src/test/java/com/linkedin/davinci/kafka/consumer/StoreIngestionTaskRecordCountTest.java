package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.stats.HostLevelIngestionStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubMessageHeader;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StoreIngestionTaskRecordCountTest {
  private StoreIngestionTask task;
  private HostLevelIngestionStats hostLevelIngestionStats;
  private VeniceServerConfig serverConfig;

  @BeforeMethod
  public void setUp() throws Exception {
    task = mock(StoreIngestionTask.class);
    hostLevelIngestionStats = mock(HostLevelIngestionStats.class);
    serverConfig = mock(VeniceServerConfig.class);

    // Set the fields on the mock so the real method can access them
    Field statsField = StoreIngestionTask.class.getDeclaredField("hostLevelIngestionStats");
    statsField.setAccessible(true);
    statsField.set(task, hostLevelIngestionStats);

    Field configField = StoreIngestionTask.class.getDeclaredField("serverConfig");
    configField.setAccessible(true);
    configField.set(task, serverConfig);
  }

  private void invokeVerifyBatchPushRecordCount(PartitionConsumptionState pcs, PubSubMessageHeaders headers)
      throws Exception {
    Method method = StoreIngestionTask.class
        .getDeclaredMethod("verifyBatchPushRecordCount", PartitionConsumptionState.class, PubSubMessageHeaders.class);
    method.setAccessible(true);
    try {
      method.invoke(task, pcs, headers);
    } catch (java.lang.reflect.InvocationTargetException e) {
      if (e.getCause() instanceof VeniceException) {
        throw (VeniceException) e.getCause();
      }
      throw e;
    }
  }

  private PubSubMessageHeaders createHeadersWithRecordCount(long count) {
    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    byte[] countBytes = ByteBuffer.allocate(Long.BYTES).putLong(count).array();
    headers.add(PubSubMessageHeaders.VENICE_PARTITION_RECORD_COUNT_HEADER, countBytes);
    return headers;
  }

  @Test
  public void testNullHeaders() throws Exception {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    // Should return early without any interaction
    invokeVerifyBatchPushRecordCount(pcs, null);
    verify(pcs, never()).getBatchPushRecordCount();
    verify(hostLevelIngestionStats, never()).recordBatchPushRecordCountMismatch();
  }

  @Test
  public void testMissingRecordCountHeader() throws Exception {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    // No record count header added - should return early
    invokeVerifyBatchPushRecordCount(pcs, headers);
    verify(pcs, never()).getBatchPushRecordCount();
    verify(hostLevelIngestionStats, never()).recordBatchPushRecordCountMismatch();
  }

  @Test
  public void testNullHeaderValue() throws Exception {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(new PubSubMessageHeader(PubSubMessageHeaders.VENICE_PARTITION_RECORD_COUNT_HEADER, null));
    // Null value - should return early
    invokeVerifyBatchPushRecordCount(pcs, headers);
    verify(pcs, never()).getBatchPushRecordCount();
    verify(hostLevelIngestionStats, never()).recordBatchPushRecordCountMismatch();
  }

  @Test
  public void testWrongHeaderSize() throws Exception {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    // Add header with wrong size (4 bytes instead of 8)
    headers.add(PubSubMessageHeaders.VENICE_PARTITION_RECORD_COUNT_HEADER, new byte[] { 1, 2, 3, 4 });
    invokeVerifyBatchPushRecordCount(pcs, headers);
    verify(pcs, never()).getBatchPushRecordCount();
    verify(hostLevelIngestionStats, never()).recordBatchPushRecordCountMismatch();
  }

  @Test
  public void testSentinelValueMinusOne() throws Exception {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    PubSubMessageHeaders headers = createHeadersWithRecordCount(-1);
    // Sentinel value -1 should return early
    invokeVerifyBatchPushRecordCount(pcs, headers);
    verify(pcs, never()).getBatchPushRecordCount();
    verify(hostLevelIngestionStats, never()).recordBatchPushRecordCountMismatch();
  }

  @Test
  public void testRecordCountMatch() throws Exception {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    when(pcs.getBatchPushRecordCount()).thenReturn(100L);
    when(pcs.getReplicaId()).thenReturn("test-replica");
    PubSubMessageHeaders headers = createHeadersWithRecordCount(100);

    invokeVerifyBatchPushRecordCount(pcs, headers);
    // Should not record mismatch
    verify(hostLevelIngestionStats, never()).recordBatchPushRecordCountMismatch();
  }

  @Test
  public void testRecordCountMismatchWithVerificationDisabled() throws Exception {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    when(pcs.getBatchPushRecordCount()).thenReturn(50L);
    when(pcs.getReplicaId()).thenReturn("test-replica");
    when(serverConfig.isBatchPushRecordCountVerificationEnabled()).thenReturn(false);
    PubSubMessageHeaders headers = createHeadersWithRecordCount(100);

    // Should log error and record metric but NOT throw
    invokeVerifyBatchPushRecordCount(pcs, headers);
    verify(hostLevelIngestionStats).recordBatchPushRecordCountMismatch();
  }

  @Test
  public void testRecordCountMismatchWithVerificationEnabled() throws Exception {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    when(pcs.getBatchPushRecordCount()).thenReturn(50L);
    when(pcs.getReplicaId()).thenReturn("test-replica");
    when(serverConfig.isBatchPushRecordCountVerificationEnabled()).thenReturn(true);
    PubSubMessageHeaders headers = createHeadersWithRecordCount(100);

    try {
      invokeVerifyBatchPushRecordCount(pcs, headers);
      fail("Expected VeniceException to be thrown");
    } catch (VeniceException e) {
      assertTrue(e.getMessage().contains("Record count mismatch"));
      assertTrue(e.getMessage().contains("Expected: 100"));
      assertTrue(e.getMessage().contains("Actual: 50"));
    }
    verify(hostLevelIngestionStats).recordBatchPushRecordCountMismatch();
  }

  @Test
  public void testRecordCountMatchWithZero() throws Exception {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    when(pcs.getBatchPushRecordCount()).thenReturn(0L);
    when(pcs.getReplicaId()).thenReturn("test-replica");
    PubSubMessageHeaders headers = createHeadersWithRecordCount(0);

    invokeVerifyBatchPushRecordCount(pcs, headers);
    verify(hostLevelIngestionStats, never()).recordBatchPushRecordCountMismatch();
  }
}
