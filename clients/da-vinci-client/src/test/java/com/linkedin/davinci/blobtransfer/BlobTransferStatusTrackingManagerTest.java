package com.linkedin.davinci.blobtransfer;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.davinci.blobtransfer.client.NettyFileTransferClient;
import com.linkedin.venice.exceptions.VeniceBlobTransferCancelledException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
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
  public void testRegisterTransfer() {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);
    CompletableFuture<InputStream> transferFuture = new CompletableFuture<>();

    statusTrackingManager.registerTransfer(replicaId, transferFuture);

    assertTrue(statusTrackingManager.isBlobTransferInProgress(replicaId));
    assertFalse(statusTrackingManager.isBlobTransferCancelled(replicaId));
  }

  @Test
  public void testCancelTransferWithNoOngoingTransfer() throws Exception {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);
    // Test canceling when no transfer is in progress
    statusTrackingManager.cancelTransfer(replicaId, 10);

    // flag should be empty.
    assertFalse(statusTrackingManager.isBlobTransferCancelled(replicaId));
    assertFalse(statusTrackingManager.isBlobTransferInProgress(replicaId));
  }

  @Test
  public void testCancelTransferWithOngoingTransfer() throws Exception {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);
    CompletableFuture<InputStream> transferFuture = new CompletableFuture<>();
    statusTrackingManager.registerTransfer(replicaId, transferFuture);

    // Mock channel
    Channel mockChannel = mock(Channel.class);
    ChannelFuture mockChannelFuture = mock(ChannelFuture.class);
    when(mockNettyClient.getActiveChannel(replicaId)).thenReturn(mockChannel);
    when(mockChannel.isActive()).thenReturn(true);
    when(mockChannel.close()).thenReturn(mockChannelFuture);
    when(mockChannelFuture.syncUninterruptibly()).thenReturn(mockChannelFuture);

    // Complete the future with cancellation exception in a separate thread
    new Thread(() -> {
      try {
        Thread.sleep(100);
        transferFuture.completeExceptionally(new VeniceBlobTransferCancelledException("Transfer cancelled for test"));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }).start();

    // Cancel the transfer
    statusTrackingManager.cancelTransfer(replicaId, 5);

    // Verify channel was closed and flag REMAINS SET (caller will clear it)
    verify(mockChannel).close();
    assertTrue(statusTrackingManager.isBlobTransferCancelled(replicaId));
  }

  @Test
  public void testCancelTransferTimeout() {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);
    CompletableFuture<InputStream> transferFuture = new CompletableFuture<>();
    statusTrackingManager.registerTransfer(replicaId, transferFuture);

    // Don't complete the future - should timeout
    assertThrows(TimeoutException.class, () -> {
      statusTrackingManager.cancelTransfer(replicaId, 1);
    });

    // Flag should still be set
    assertTrue(statusTrackingManager.isBlobTransferCancelled(replicaId));
  }

  @Test
  public void testCancelTransferWithSuccessfulCompletion() throws Exception {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);
    CompletableFuture<InputStream> transferFuture = new CompletableFuture<>();
    statusTrackingManager.registerTransfer(replicaId, transferFuture);

    // Complete successfully in a separate thread
    new Thread(() -> {
      try {
        Thread.sleep(100);
        transferFuture.complete(mock(InputStream.class));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }).start();

    // Cancel should complete without exception (transfer finished before cancellation took effect)
    statusTrackingManager.cancelTransfer(replicaId, 5);

    // Flag REMAINS SET (caller will clear it)
    assertTrue(statusTrackingManager.isBlobTransferCancelled(replicaId));
  }

  @Test
  public void testIsBlobTransferCancelled() {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);
    assertFalse(statusTrackingManager.isBlobTransferCancelled(replicaId));

    CompletableFuture<InputStream> transferFuture = new CompletableFuture<>();
    statusTrackingManager.registerTransfer(replicaId, transferFuture);

    assertFalse(statusTrackingManager.isBlobTransferCancelled(replicaId));

    try {
      statusTrackingManager.cancelTransfer(replicaId, 1);
    } catch (Exception e) {
      // Ignore timeout
    }

    assertTrue(statusTrackingManager.isBlobTransferCancelled(replicaId));
  }

  @Test
  public void testIsBlobTransferInProgress() {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);
    assertFalse(statusTrackingManager.isBlobTransferInProgress(replicaId));

    CompletableFuture<InputStream> transferFuture = new CompletableFuture<>();
    statusTrackingManager.registerTransfer(replicaId, transferFuture);

    assertTrue(statusTrackingManager.isBlobTransferInProgress(replicaId));

    transferFuture.complete(mock(InputStream.class));

    assertFalse(statusTrackingManager.isBlobTransferInProgress(replicaId));
  }

  @Test
  public void testGetCancellationFlag() {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);
    assertNull(statusTrackingManager.getCancellationFlag(replicaId));

    CompletableFuture<InputStream> transferFuture = new CompletableFuture<>();
    statusTrackingManager.registerTransfer(replicaId, transferFuture);

    assertNotNull(statusTrackingManager.getCancellationFlag(replicaId));
    assertFalse(statusTrackingManager.getCancellationFlag(replicaId).get());
  }

  @Test
  public void testClearCancellationRequest() throws Exception {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);
    CompletableFuture<InputStream> transferFuture = new CompletableFuture<>();
    statusTrackingManager.registerTransfer(replicaId, transferFuture);

    try {
      statusTrackingManager.cancelTransfer(replicaId, 1);
    } catch (TimeoutException e) {
      // Expected
    }

    assertTrue(statusTrackingManager.isBlobTransferCancelled(replicaId));

    statusTrackingManager.clearCancellationRequest(replicaId);

    assertFalse(statusTrackingManager.isBlobTransferCancelled(replicaId));
  }

  @Test
  public void testClearCancellationRequestAfterTransferComplete() throws InterruptedException, TimeoutException {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);
    CompletableFuture<InputStream> transferFuture = new CompletableFuture<>();
    statusTrackingManager.registerTransfer(replicaId, transferFuture);

    assertTrue(statusTrackingManager.isBlobTransferInProgress(replicaId));

    // Complete the transfer
    transferFuture.complete(mock(InputStream.class));

    // Transfer is no longer in progress
    assertFalse(statusTrackingManager.isBlobTransferInProgress(replicaId));

    // But we can still set and clear the cancellation flag
    statusTrackingManager.cancelTransfer(replicaId, 5);
    assertFalse(statusTrackingManager.isBlobTransferCancelled(replicaId));
  }

  @Test
  public void testCancelTransferWithNoActiveChannel() throws Exception {
    String replicaId = Utils.getReplicaId(Version.composeKafkaTopic("testStore", 1), 0);
    CompletableFuture<InputStream> transferFuture = new CompletableFuture<>();
    statusTrackingManager.registerTransfer(replicaId, transferFuture);

    when(mockNettyClient.getActiveChannel(replicaId)).thenReturn(null);

    // Complete the future in a separate thread
    new Thread(() -> {
      try {
        Thread.sleep(100);
        transferFuture.completeExceptionally(new VeniceBlobTransferCancelledException("Transfer cancelled"));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }).start();

    statusTrackingManager.cancelTransfer(replicaId, 5);

    // Flag REMAINS SET (caller will clear it)
    assertTrue(statusTrackingManager.isBlobTransferCancelled(replicaId));
    verify(mockNettyClient).getActiveChannel(replicaId);
  }

  @Test
  public void testMultiplePartitions() {
    String replicaId1 = Utils.getReplicaId(Version.composeKafkaTopic("store1", 1), 0);
    String replicaId2 = Utils.getReplicaId(Version.composeKafkaTopic("store2", 2), 1);
    CompletableFuture<InputStream> transfer1 = new CompletableFuture<>();
    CompletableFuture<InputStream> transfer2 = new CompletableFuture<>();

    statusTrackingManager.registerTransfer(replicaId1, transfer1);
    statusTrackingManager.registerTransfer(replicaId2, transfer2);

    assertTrue(statusTrackingManager.isBlobTransferInProgress(replicaId1));
    assertTrue(statusTrackingManager.isBlobTransferInProgress(replicaId2));

    assertFalse(statusTrackingManager.isBlobTransferCancelled(replicaId1));
    assertFalse(statusTrackingManager.isBlobTransferCancelled(replicaId2));

    statusTrackingManager.clearCancellationRequest(replicaId1);

    assertFalse(statusTrackingManager.isBlobTransferCancelled(replicaId1));
    assertTrue(statusTrackingManager.isBlobTransferInProgress(replicaId2));
  }
}
