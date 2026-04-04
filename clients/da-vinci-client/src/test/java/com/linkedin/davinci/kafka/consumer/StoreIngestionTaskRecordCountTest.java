package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.stats.HostLevelIngestionStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubMessageHeader;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StoreIngestionTaskRecordCountTest {
  private StoreIngestionTask task;
  private HostLevelIngestionStats hostLevelIngestionStats;
  private VeniceServerConfig serverConfig;

  @BeforeMethod
  public void setUp() {
    hostLevelIngestionStats = mock(HostLevelIngestionStats.class);
    serverConfig = mock(VeniceServerConfig.class);

    task = mock(StoreIngestionTask.class, org.mockito.Mockito.CALLS_REAL_METHODS);
    AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
      try {
        Field statsField = StoreIngestionTask.class.getDeclaredField("hostLevelIngestionStats");
        statsField.setAccessible(true);
        statsField.set(task, hostLevelIngestionStats);

        Field configField = StoreIngestionTask.class.getDeclaredField("serverConfig");
        configField.setAccessible(true);
        configField.set(task, serverConfig);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return null;
    });
  }

  private PubSubMessageHeaders createHeadersWithRecordCount(long count) {
    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    byte[] countBytes = ByteBuffer.allocate(Long.BYTES).putLong(count).array();
    headers.add(PubSubMessageHeaders.VENICE_PARTITION_RECORD_COUNT_HEADER, countBytes);
    return headers;
  }

  @Test
  public void testNullHeaders() {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    task.verifyBatchPushRecordCount(pcs, null);
    verify(pcs, never()).getBatchPushRecordCount();
    verify(hostLevelIngestionStats, never()).recordBatchPushRecordCountMismatch();
  }

  @Test
  public void testMissingRecordCountHeader() {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    task.verifyBatchPushRecordCount(pcs, headers);
    verify(pcs, never()).getBatchPushRecordCount();
    verify(hostLevelIngestionStats, never()).recordBatchPushRecordCountMismatch();
  }

  @Test
  public void testNullHeaderValue() {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(new PubSubMessageHeader(PubSubMessageHeaders.VENICE_PARTITION_RECORD_COUNT_HEADER, null));
    task.verifyBatchPushRecordCount(pcs, headers);
    verify(pcs, never()).getBatchPushRecordCount();
    verify(hostLevelIngestionStats, never()).recordBatchPushRecordCountMismatch();
  }

  @Test
  public void testWrongHeaderSize() {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    PubSubMessageHeaders headers = new PubSubMessageHeaders();
    headers.add(PubSubMessageHeaders.VENICE_PARTITION_RECORD_COUNT_HEADER, new byte[] { 1, 2, 3, 4 });
    task.verifyBatchPushRecordCount(pcs, headers);
    verify(pcs, never()).getBatchPushRecordCount();
    verify(hostLevelIngestionStats, never()).recordBatchPushRecordCountMismatch();
  }

  @Test
  public void testSentinelValueMinusOne() {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    PubSubMessageHeaders headers = createHeadersWithRecordCount(-1);
    task.verifyBatchPushRecordCount(pcs, headers);
    verify(pcs, never()).getBatchPushRecordCount();
    verify(hostLevelIngestionStats, never()).recordBatchPushRecordCountMismatch();
  }

  @Test
  public void testRecordCountMatch() {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    when(pcs.getBatchPushRecordCount()).thenReturn(100L);
    when(pcs.getReplicaId()).thenReturn("test-replica");
    PubSubMessageHeaders headers = createHeadersWithRecordCount(100);

    task.verifyBatchPushRecordCount(pcs, headers);
    verify(hostLevelIngestionStats, never()).recordBatchPushRecordCountMismatch();
  }

  @Test
  public void testRecordCountMismatchWithVerificationDisabled() {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    when(pcs.getBatchPushRecordCount()).thenReturn(50L);
    when(pcs.getReplicaId()).thenReturn("test-replica");
    when(serverConfig.isBatchPushRecordCountVerificationEnabled()).thenReturn(false);
    PubSubMessageHeaders headers = createHeadersWithRecordCount(100);

    task.verifyBatchPushRecordCount(pcs, headers);
    verify(hostLevelIngestionStats).recordBatchPushRecordCountMismatch();
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*Record count mismatch.*")
  public void testRecordCountMismatchWithVerificationEnabled() {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    when(pcs.getBatchPushRecordCount()).thenReturn(50L);
    when(pcs.getReplicaId()).thenReturn("test-replica");
    when(serverConfig.isBatchPushRecordCountVerificationEnabled()).thenReturn(true);
    PubSubMessageHeaders headers = createHeadersWithRecordCount(100);

    task.verifyBatchPushRecordCount(pcs, headers);
  }

  @Test
  public void testRecordCountMatchWithZero() {
    PartitionConsumptionState pcs = mock(PartitionConsumptionState.class);
    when(pcs.getBatchPushRecordCount()).thenReturn(0L);
    when(pcs.getReplicaId()).thenReturn("test-replica");
    PubSubMessageHeaders headers = createHeadersWithRecordCount(0);

    task.verifyBatchPushRecordCount(pcs, headers);
    verify(hostLevelIngestionStats, never()).recordBatchPushRecordCountMismatch();
  }
}
