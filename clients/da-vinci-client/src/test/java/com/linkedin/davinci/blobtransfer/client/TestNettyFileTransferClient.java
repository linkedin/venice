package com.linkedin.davinci.blobtransfer.client;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.stats.AggBlobTransferStats;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.Utils;
import io.netty.util.concurrent.EventExecutor;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;
import org.testng.annotations.Test;


/**
 * Targeted tests for the Netty event-loop pool sizing and thread priority configured in
 * {@link NettyFileTransferClient}. Lives in the same package as the client so it can read the
 * package-private {@code workerGroup}.
 */
public class TestNettyFileTransferClient {
  // Reuse the production floor so this test tracks it instead of hard-coding a second copy.
  private static final int MIN_NETTY_WORKER_THREADS = NettyFileTransferClient.MIN_NETTY_WORKER_THREADS;
  // Mirrors NettyFileTransferClient#BLOB_TRANSFER_CLIENT_THREAD_PRIORITY.
  private static final int EXPECTED_THREAD_PRIORITY = 4;

  private NettyFileTransferClient createClient(int nettyWorkerThreadCount) throws Exception {
    return new NettyFileTransferClient(
        0, // serverPort
        Utils.getTempDataDirectory().getAbsolutePath(), // baseDir (auto-registered for deletion on JVM exit)
        mock(StorageMetadataService.class),
        30, // peersConnectivityFreshnessInSeconds
        30, // blobReceiveTimeoutInMin
        60, // blobReceiveReaderIdleTimeInSeconds
        nettyWorkerThreadCount,
        null, // globalChannelTrafficShapingHandler: only referenced when a channel is initialized, never in this test
        mock(AggBlobTransferStats.class),
        Optional.<SSLFactory>empty(),
        () -> null, // notifierSupplier
        LogContext.forTests("test"));
  }

  private static int countWorkerThreads(NettyFileTransferClient client) {
    return (int) StreamSupport.stream(client.workerGroup.spliterator(), false).count();
  }

  @Test
  public void testWorkerThreadCountAboveFloorIsHonored() throws Exception {
    int requested = MIN_NETTY_WORKER_THREADS + 6;
    NettyFileTransferClient client = createClient(requested);
    try {
      assertEquals(countWorkerThreads(client), requested, "A worker thread count above the floor should be used as-is");
    } finally {
      client.close();
    }
  }

  @Test
  public void testWorkerThreadCountIsFlooredAtMinimum() throws Exception {
    // Any value at or below the floor (including a non-positive value that would otherwise make Netty fall back to its
    // 2 * cores default) clamps up to MIN_NETTY_WORKER_THREADS.
    for (int requested: new int[] { 1, MIN_NETTY_WORKER_THREADS, 0, -1 }) {
      NettyFileTransferClient client = createClient(requested);
      try {
        assertEquals(
            countWorkerThreads(client),
            MIN_NETTY_WORKER_THREADS,
            "A worker thread count of " + requested + " should be floored at " + MIN_NETTY_WORKER_THREADS);
      } finally {
        client.close();
      }
    }
  }

  @Test
  public void testWorkerThreadsRunBelowNormalPriorityAsNamedDaemons() throws Exception {
    NettyFileTransferClient client = createClient(MIN_NETTY_WORKER_THREADS);
    try {
      for (EventExecutor executor: client.workerGroup) {
        // Submitting a task forces the event-loop thread to be created, then we inspect that thread.
        Thread thread = executor.submit(Thread::currentThread).get(5, TimeUnit.SECONDS);
        assertEquals(
            thread.getPriority(),
            EXPECTED_THREAD_PRIORITY,
            "Blob transfer event-loop threads should run below normal priority");
        assertTrue(
            thread.getName().startsWith("Venice-BlobTransfer-Client-Netty"),
            "Unexpected event-loop thread name: " + thread.getName());
        assertTrue(thread.isDaemon(), "Blob transfer event-loop threads should be daemon threads");
      }
    } finally {
      client.close();
    }
  }
}
