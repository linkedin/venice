package com.linkedin.davinci.blobtransfer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferStatus;
import com.linkedin.davinci.blobtransfer.client.NettyFileTransferClient;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class BlobTransferStatusTrackingManagerTest {
  private BlobTransferStatusTrackingManager statusTrackingManager;
  private NettyFileTransferClient mockNettyClient;

  @BeforeMethod
  public void setUp() {
    mockNettyClient = mock(NettyFileTransferClient.class);
    statusTrackingManager = new BlobTransferStatusTrackingManager(mockNettyClient);
  }

  @Test
  public void testInitialTransfer() {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);

    statusTrackingManager.initialTransfer(replicaId);

    assertFalse(statusTrackingManager.isBlobTransferInProgress(replicaId));
    assertFalse(statusTrackingManager.isBlobTransferCancelRequested(replicaId));
    assertEquals(statusTrackingManager.getTransferStatus(replicaId), BlobTransferStatus.TRANSFER_NOT_STARTED);
  }

  @Test
  public void testStartedTransfer() {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);

    statusTrackingManager.startedTransfer(replicaId);

    assertTrue(statusTrackingManager.isBlobTransferInProgress(replicaId));
    assertFalse(statusTrackingManager.isBlobTransferCancelRequested(replicaId));
    assertEquals(statusTrackingManager.getTransferStatus(replicaId), BlobTransferStatus.TRANSFER_STARTED);
  }

  @Test
  public void testCancelTransferWhenInFinalState() {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);

    // Test canceling when status is TRANSFER_COMPLETED (final state)
    statusTrackingManager.startedTransfer(replicaId);
    statusTrackingManager.markTransferCompleted(replicaId);

    assertTrue(statusTrackingManager.isTransferInFinalState(replicaId));

    // Cancel should be skipped
    statusTrackingManager.cancelTransfer(replicaId);

    // Status should remain TRANSFER_COMPLETED
    assertEquals(statusTrackingManager.getTransferStatus(replicaId), BlobTransferStatus.TRANSFER_COMPLETED);
    assertFalse(statusTrackingManager.isBlobTransferCancelRequested(replicaId));
  }

  @Test
  public void testCancelTransferWithOngoingTransfer() {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);
    statusTrackingManager.startedTransfer(replicaId);

    // Mock channel
    Channel mockChannel = mock(Channel.class);
    ChannelFuture mockChannelFuture = mock(ChannelFuture.class);
    when(mockNettyClient.getActiveChannel(replicaId)).thenReturn(mockChannel);
    when(mockChannel.isActive()).thenReturn(true);
    when(mockChannel.close()).thenReturn(mockChannelFuture);
    when(mockChannelFuture.syncUninterruptibly()).thenReturn(mockChannelFuture);

    // Cancel the transfer (non-blocking)
    statusTrackingManager.cancelTransfer(replicaId);

    // Verify channel was closed and status is TRANSFER_CANCEL_REQUESTED
    verify(mockChannel).close();
    assertTrue(statusTrackingManager.isBlobTransferCancelRequested(replicaId));
    assertTrue(statusTrackingManager.isBlobTransferCancelRequestSentBefore(replicaId));
    assertEquals(statusTrackingManager.getTransferStatus(replicaId), BlobTransferStatus.TRANSFER_CANCEL_REQUESTED);
    assertTrue(statusTrackingManager.isBlobTransferInProgress(replicaId)); // Still in progress
  }

  @Test
  public void testIsBlobTransferCancelling() {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);
    assertFalse(statusTrackingManager.isBlobTransferCancelRequested(replicaId));

    statusTrackingManager.startedTransfer(replicaId);
    assertFalse(statusTrackingManager.isBlobTransferCancelRequested(replicaId));

    // Cancel the transfer (non-blocking)
    statusTrackingManager.cancelTransfer(replicaId);
    assertTrue(statusTrackingManager.isBlobTransferCancelRequested(replicaId));

    // After marking as cancelled
    statusTrackingManager.markTransferCancelled(replicaId);
    assertFalse(statusTrackingManager.isBlobTransferCancelRequested(replicaId)); // Now in TRANSFER_CANCELLED state
  }

  @Test
  public void testIsBlobTransferCancelRequestSentBefore() {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);
    assertFalse(statusTrackingManager.isBlobTransferCancelRequestSentBefore(replicaId));

    statusTrackingManager.startedTransfer(replicaId);
    assertFalse(statusTrackingManager.isBlobTransferCancelRequestSentBefore(replicaId));

    // Cancel the transfer
    statusTrackingManager.cancelTransfer(replicaId);
    assertTrue(statusTrackingManager.isBlobTransferCancelRequestSentBefore(replicaId));

    // After marking as cancelled
    statusTrackingManager.markTransferCancelled(replicaId);
    assertTrue(statusTrackingManager.isBlobTransferCancelRequestSentBefore(replicaId)); // Still true
  }

  @Test
  public void testIsBlobTransferInProgress() {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);
    assertFalse(statusTrackingManager.isBlobTransferInProgress(replicaId));

    statusTrackingManager.startedTransfer(replicaId);

    // In progress when status is TRANSFER_STARTED
    assertTrue(statusTrackingManager.isBlobTransferInProgress(replicaId));
    assertEquals(statusTrackingManager.getTransferStatus(replicaId), BlobTransferStatus.TRANSFER_STARTED);

    // Mark as completed
    statusTrackingManager.markTransferCompleted(replicaId);

    // Not in progress when status is TRANSFER_COMPLETED (final state)
    assertFalse(statusTrackingManager.isBlobTransferInProgress(replicaId));
    assertEquals(statusTrackingManager.getTransferStatus(replicaId), BlobTransferStatus.TRANSFER_COMPLETED);
  }

  @Test
  public void testGetTransferStatus() {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);
    assertNull(statusTrackingManager.getTransferStatus(replicaId));

    statusTrackingManager.startedTransfer(replicaId);
    assertEquals(statusTrackingManager.getTransferStatus(replicaId), BlobTransferStatus.TRANSFER_STARTED);

    statusTrackingManager.cancelTransfer(replicaId);
    assertEquals(statusTrackingManager.getTransferStatus(replicaId), BlobTransferStatus.TRANSFER_CANCEL_REQUESTED);
  }

  @Test
  public void testClearTransferStatusEnum() {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);
    statusTrackingManager.startedTransfer(replicaId);

    statusTrackingManager.cancelTransfer(replicaId);
    assertTrue(statusTrackingManager.isBlobTransferCancelRequested(replicaId));

    statusTrackingManager.clearTransferStatusEnum(replicaId);
    assertFalse(statusTrackingManager.isBlobTransferCancelRequested(replicaId));
    assertNull(statusTrackingManager.getTransferStatus(replicaId));
  }

  @Test
  public void testCancelTransferWithNoActiveChannel() {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);
    statusTrackingManager.startedTransfer(replicaId);

    when(mockNettyClient.getActiveChannel(replicaId)).thenReturn(null);

    // Cancel returns immediately (non-blocking)
    statusTrackingManager.cancelTransfer(replicaId);

    // Status is set to TRANSFER_CANCEL_REQUESTED
    assertTrue(statusTrackingManager.isBlobTransferCancelRequested(replicaId));
    verify(mockNettyClient).getActiveChannel(replicaId);
  }

  @Test
  public void testMultiplePartitions() {
    String replicaId1 = Utils.getReplicaId(Version.composeKafkaTopic("store1", 1), 0);
    String replicaId2 = Utils.getReplicaId(Version.composeKafkaTopic("store2", 2), 1);

    statusTrackingManager.startedTransfer(replicaId1);
    statusTrackingManager.startedTransfer(replicaId2);

    assertTrue(statusTrackingManager.isBlobTransferInProgress(replicaId1));
    assertTrue(statusTrackingManager.isBlobTransferInProgress(replicaId2));

    assertFalse(statusTrackingManager.isBlobTransferCancelRequested(replicaId1));
    assertFalse(statusTrackingManager.isBlobTransferCancelRequested(replicaId2));

    assertEquals(statusTrackingManager.getTransferStatus(replicaId1), BlobTransferStatus.TRANSFER_STARTED);
    assertEquals(statusTrackingManager.getTransferStatus(replicaId2), BlobTransferStatus.TRANSFER_STARTED);

    statusTrackingManager.clearTransferStatusEnum(replicaId1);

    assertFalse(statusTrackingManager.isBlobTransferCancelRequested(replicaId1));
    assertTrue(statusTrackingManager.isBlobTransferInProgress(replicaId2));
    assertNull(statusTrackingManager.getTransferStatus(replicaId1));
  }

  @Test
  public void testMarkTransferCompleted() {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);
    statusTrackingManager.startedTransfer(replicaId);

    assertEquals(statusTrackingManager.getTransferStatus(replicaId), BlobTransferStatus.TRANSFER_STARTED);

    statusTrackingManager.markTransferCompleted(replicaId);
    assertEquals(statusTrackingManager.getTransferStatus(replicaId), BlobTransferStatus.TRANSFER_COMPLETED);
    assertFalse(statusTrackingManager.isBlobTransferCancelRequested(replicaId));
  }

  @Test
  public void testMarkTransferCancelled() {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);
    statusTrackingManager.startedTransfer(replicaId);

    statusTrackingManager.cancelTransfer(replicaId);
    assertEquals(statusTrackingManager.getTransferStatus(replicaId), BlobTransferStatus.TRANSFER_CANCEL_REQUESTED);

    statusTrackingManager.markTransferCancelled(replicaId);
    assertEquals(statusTrackingManager.getTransferStatus(replicaId), BlobTransferStatus.TRANSFER_CANCELLED);
    assertTrue(statusTrackingManager.isBlobTransferCancelRequestSentBefore(replicaId));
  }

  @Test
  public void testFullCancellationLifecycle() {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);

    // Step 1: Initialize transfer → TRANSFER_NOT_STARTED
    statusTrackingManager.initialTransfer(replicaId);
    assertEquals(statusTrackingManager.getTransferStatus(replicaId), BlobTransferStatus.TRANSFER_NOT_STARTED);
    assertFalse(statusTrackingManager.isBlobTransferCancelRequested(replicaId));
    assertFalse(statusTrackingManager.isBlobTransferInProgress(replicaId));

    // Step 2: Start transfer → TRANSFER_STARTED
    statusTrackingManager.startedTransfer(replicaId);
    assertEquals(statusTrackingManager.getTransferStatus(replicaId), BlobTransferStatus.TRANSFER_STARTED);
    assertFalse(statusTrackingManager.isBlobTransferCancelRequested(replicaId));
    assertTrue(statusTrackingManager.isBlobTransferInProgress(replicaId));

    // Step 3: Initiate cancellation → TRANSFER_CANCEL_REQUESTED
    statusTrackingManager.cancelTransfer(replicaId);
    assertEquals(statusTrackingManager.getTransferStatus(replicaId), BlobTransferStatus.TRANSFER_CANCEL_REQUESTED);
    assertTrue(statusTrackingManager.isBlobTransferCancelRequested(replicaId));
    assertTrue(statusTrackingManager.isBlobTransferInProgress(replicaId));

    // Step 4: Mark cancellation complete → TRANSFER_CANCELLED
    statusTrackingManager.markTransferCancelled(replicaId);
    assertEquals(statusTrackingManager.getTransferStatus(replicaId), BlobTransferStatus.TRANSFER_CANCELLED);
    assertFalse(statusTrackingManager.isBlobTransferCancelRequested(replicaId)); // No longer cancelling
    assertTrue(statusTrackingManager.isBlobTransferCancelRequestSentBefore(replicaId)); // But was cancelled
    assertFalse(statusTrackingManager.isBlobTransferInProgress(replicaId)); // Final state

    // Step 5: Cleanup
    statusTrackingManager.clearTransferStatusEnum(replicaId);
    assertNull(statusTrackingManager.getTransferStatus(replicaId));
    assertFalse(statusTrackingManager.isBlobTransferCancelRequested(replicaId));
  }

  @Test
  public void testFullSuccessfulLifecycle() {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);

    // Step 1: Initialize transfer → TRANSFER_NOT_STARTED
    statusTrackingManager.initialTransfer(replicaId);
    assertEquals(statusTrackingManager.getTransferStatus(replicaId), BlobTransferStatus.TRANSFER_NOT_STARTED);
    assertFalse(statusTrackingManager.isBlobTransferCancelRequested(replicaId));
    assertFalse(statusTrackingManager.isBlobTransferInProgress(replicaId));

    // Step 2: Start transfer → TRANSFER_STARTED
    statusTrackingManager.startedTransfer(replicaId);
    assertEquals(statusTrackingManager.getTransferStatus(replicaId), BlobTransferStatus.TRANSFER_STARTED);
    assertFalse(statusTrackingManager.isBlobTransferCancelRequested(replicaId));
    assertTrue(statusTrackingManager.isBlobTransferInProgress(replicaId));

    // Step 3: Mark as completed → TRANSFER_COMPLETED
    statusTrackingManager.markTransferCompleted(replicaId);
    assertEquals(statusTrackingManager.getTransferStatus(replicaId), BlobTransferStatus.TRANSFER_COMPLETED);
    assertFalse(statusTrackingManager.isBlobTransferCancelRequested(replicaId));
    assertFalse(statusTrackingManager.isBlobTransferInProgress(replicaId)); // Final state

    // Step 4: Cleanup
    statusTrackingManager.clearTransferStatusEnum(replicaId);
    assertNull(statusTrackingManager.getTransferStatus(replicaId));
  }

  @Test
  public void testIsTransferInFinalState() {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);

    // No status - in final state (null counts as final)
    assertTrue(statusTrackingManager.isTransferInFinalState(replicaId));

    // TRANSFER_NOT_STARTED - not in final state
    statusTrackingManager.initialTransfer(replicaId);
    assertFalse(statusTrackingManager.isTransferInFinalState(replicaId));

    // TRANSFER_STARTED - not in final state
    statusTrackingManager.startedTransfer(replicaId);
    assertFalse(statusTrackingManager.isTransferInFinalState(replicaId));

    // TRANSFER_CANCEL_REQUESTED - not in final state
    statusTrackingManager.cancelTransfer(replicaId);
    assertFalse(statusTrackingManager.isTransferInFinalState(replicaId));

    // TRANSFER_CANCELLED - in final state
    statusTrackingManager.markTransferCancelled(replicaId);
    assertTrue(statusTrackingManager.isTransferInFinalState(replicaId));

    statusTrackingManager.clearTransferStatusEnum(replicaId);

    // TRANSFER_COMPLETED - in final state
    statusTrackingManager.startedTransfer(replicaId);
    statusTrackingManager.markTransferCompleted(replicaId);
    assertTrue(statusTrackingManager.isTransferInFinalState(replicaId));
  }

  @Test
  public void testIsBlobTransferInProgressBasedOnStatus() {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);

    // Test 1: TRANSFER_STARTED → in progress
    statusTrackingManager.startedTransfer(replicaId);
    assertTrue(statusTrackingManager.isBlobTransferInProgress(replicaId));

    // Test 2: TRANSFER_CANCEL_REQUESTED → still in progress
    statusTrackingManager.cancelTransfer(replicaId);
    assertTrue(statusTrackingManager.isBlobTransferInProgress(replicaId));

    // Test 3: TRANSFER_CANCELLED → not in progress (final state)
    statusTrackingManager.markTransferCancelled(replicaId);
    assertFalse(statusTrackingManager.isBlobTransferInProgress(replicaId));

    statusTrackingManager.clearTransferStatusEnum(replicaId);

    // Test 4: TRANSFER_COMPLETED → not in progress (final state)
    statusTrackingManager.startedTransfer(replicaId);
    statusTrackingManager.markTransferCompleted(replicaId);
    assertFalse(statusTrackingManager.isBlobTransferInProgress(replicaId));
  }

  @Test
  public void testCancelTransferWhenAlreadyCancelling() {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);

    statusTrackingManager.startedTransfer(replicaId);
    statusTrackingManager.cancelTransfer(replicaId);

    assertEquals(statusTrackingManager.getTransferStatus(replicaId), BlobTransferStatus.TRANSFER_CANCEL_REQUESTED);

    statusTrackingManager.cancelTransfer(replicaId);
    assertEquals(statusTrackingManager.getTransferStatus(replicaId), BlobTransferStatus.TRANSFER_CANCEL_REQUESTED);
  }
}
