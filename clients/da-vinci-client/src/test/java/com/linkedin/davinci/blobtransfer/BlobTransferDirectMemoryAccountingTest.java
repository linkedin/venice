package com.linkedin.davinci.blobtransfer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.blobtransfer.client.NettyFileTransferClient;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;


/**
 * Pins how much direct memory the receiver's allocator reports, because those numbers are what the admission gate's
 * ceiling has to be sized against.
 * <p>
 * The allocator is pinned to production's shape rather than inheriting the test JVM's defaults: the servers run with
 * {@code -Dio.netty.allocator.maxOrder=4}, which narrows the chunk to 128KB, and every conclusion here depends on the
 * receiver's reads being larger than that chunk.
 */
public class BlobTransferDirectMemoryAccountingTest {
  /** preferDirect, 2 heap arenas, 2 direct arenas, 8KB page, maxOrder 4, which is what the servers run with. */
  private static PooledByteBufAllocator productionShapedAllocator() {
    return new PooledByteBufAllocator(true, 2, 2, 8192, 4);
  }

  @Test
  public void testChunkIsSmallerThanASingleReceiverRead() {
    // The premise for everything below: at the servers' maxOrder a socket read does not fit in a chunk, so it is
    // served outside the pool instead of from it.
    PooledByteBufAllocator allocator = productionShapedAllocator();
    assertEquals(allocator.metric().chunkSize(), 128 * 1024);
    assertTrue(
        NettyFileTransferClient.MAX_RECEIVE_BUFFER_BYTES > allocator.metric().chunkSize(),
        "A receiver read is expected to exceed the chunk, otherwise these allocations would be pooled");
  }

  @Test
  public void testPooledAllocationIsChargedAsAWholeChunk() {
    // Usage is counted in whole chunks, so a buffer well inside one still reserves all of it. That over-reports what
    // is in use but is the right figure to budget against, since the reserved chunk occupies the process limit
    // whether or not it is full.
    PooledByteBufAllocator allocator = productionShapedAllocator();
    assertEquals(allocator.metric().usedDirectMemory(), 0);

    ByteBuf small = allocator.directBuffer(64 * 1024);
    try {
      assertEquals(allocator.metric().usedDirectMemory(), allocator.metric().chunkSize());
    } finally {
      small.release();
    }
    assertEquals(allocator.metric().usedDirectMemory(), 0);
  }

  @Test
  public void testLargerThanChunkAllocationIsCountedWhileItIsHeld() {
    // This is what makes reading the allocator at admission time meaningful. A receiver read bypasses the pool, and
    // a bypassed allocation is still counted while it is in flight, so the reading does reflect work in progress.
    PooledByteBufAllocator allocator = productionShapedAllocator();
    ByteBuf read = allocator.directBuffer(1024 * 1024);
    try {
      assertEquals(allocator.metric().usedDirectMemory(), 1024 * 1024);
      assertEquals(allocator.metric().directArenas().stream().mapToLong(a -> a.numHugeAllocations()).sum(), 1);
    } finally {
      read.release();
    }
  }

  @Test
  public void testUsageReturnsToZeroOnceBuffersAreReleased() {
    // This is why the per-transfer logs cannot size a threshold. Both ends sample once a transfer has settled and
    // released its buffers, so the sample reads close to the baseline no matter how much the transfer held while it
    // was running. Collecting more of those logs does not make them a peak.
    PooledByteBufAllocator allocator = productionShapedAllocator();
    ByteBuf read = allocator.directBuffer(1024 * 1024);
    assertTrue(allocator.metric().usedDirectMemory() > 0);
    read.release();
    assertEquals(
        allocator.metric().usedDirectMemory(),
        0,
        "A sample taken after a transfer released its buffers carries no information about its peak");
  }

  @Test
  public void testAllReceiverReadsTogetherStayFarBelowAProcessLimit() {
    // The receiver's whole direct footprint is its concurrent reads, and at the configured concurrency that comes to
    // tens of megabytes. This is the figure the ceiling has to be sized against: hundreds of megabytes bounds the
    // pathological case, while a gigabyte-scale ceiling could never be reached. It also bounds what the gate can
    // promise, since the process can fail an allocation for someone else's memory while this reading is still low.
    int concurrentReads = 30;
    PooledByteBufAllocator allocator = productionShapedAllocator();
    List<ByteBuf> held = new ArrayList<>(concurrentReads);
    try {
      for (int i = 0; i < concurrentReads; i++) {
        held.add(allocator.directBuffer(NettyFileTransferClient.MAX_RECEIVE_BUFFER_BYTES));
      }
      long usedBytes = allocator.metric().usedDirectMemory();
      assertEquals(usedBytes, (long) concurrentReads * NettyFileTransferClient.MAX_RECEIVE_BUFFER_BYTES);
      assertTrue(
          usedBytes < 64L * 1024 * 1024,
          "The receiver's own ceiling is expected to be tens of MB, got " + usedBytes + " B");
    } finally {
      held.forEach(ByteBuf::release);
    }
    assertEquals(allocator.metric().usedDirectMemory(), 0);
  }
}
